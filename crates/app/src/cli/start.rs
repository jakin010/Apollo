//! `start`: load config, apply run-only overrides, optionally daemonize, serve.

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::Args;
use tokio::sync::{mpsc, watch};

use apollo_config::{Allowlist, Config, Overrides, TlsConfig, load, validate};
use apollo_engine::{Engine, HAND_BACK_LIMIT, Lifecycle, Phase, Step, WebhookSink};

use crate::allow::{self, Bound, Published};

#[derive(Args)]
pub struct StartArgs {
    /// Config file path (default: the per-user config dir, e.g.
    /// ~/.config/apollo/config.toml on Linux).
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Override the bind host/IP for this run only.
    #[arg(long)]
    pub endpoint: Option<String>,
    /// Override the port for this run only.
    #[arg(long)]
    pub port: Option<u16>,
    /// Override the webhook receiver URL (e.g. http://127.0.0.1:9090); enables
    /// delivery for this run even without a [webhook] section, or with its
    /// `enabled = false`.
    #[arg(long = "webhook-url")]
    pub webhook_url: Option<String>,
    /// Override the health check port ([health].port) for this run only.
    #[arg(long = "health-port")]
    pub health_port: Option<u16>,
    /// Override the metrics port ([metrics].port) for this run only.
    #[arg(long = "metrics-port")]
    pub metrics_port: Option<u16>,
    /// Detach and run in the background, writing a PID file.
    #[arg(long)]
    pub daemon: bool,
}

/// Where the config comes from, so a reload reads it the same way.
#[derive(Clone)]
struct ConfigSource {
    /// `--config`, or `None` for the default location.
    path: Option<PathBuf>,
    /// The run-only command-line overrides, re-applied on every reload.
    overrides: Overrides,
}

impl ConfigSource {
    /// Load, apply the overrides, and validate.
    fn load(&self) -> anyhow::Result<Config> {
        let mut config = load::load(self.path.as_deref()).context("loading config")?;
        config.apply_overrides(&self.overrides);
        config.validate().context("config is invalid")?;
        if !config.has_models() {
            return Err(anyhow!(
                "no models configured — nothing to serve; add a [models.<label>] section"
            ));
        }
        Ok(config)
    }
}

pub fn run(args: StartArgs) -> anyhow::Result<()> {
    let source = ConfigSource {
        path: args.config.clone(),
        overrides: Overrides {
            endpoint: args.endpoint.clone(),
            port: args.port,
            webhook_url: args.webhook_url.clone(),
            health_port: args.health_port,
            metrics_port: args.metrics_port,
        },
    };
    let config = source.load()?;
    check_ports(&config)?;

    if args.daemon {
        return spawn_detached(&args);
    }

    // Foreground: own the runtime so daemonization (above) happens before any
    // runtime threads exist.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building the Tokio runtime")?;
    let config_file = args
        .config
        .clone()
        .unwrap_or_else(load::default_path)
        .canonicalize()
        .ok();
    runtime.block_on(serve(config, config_file, source))
}

async fn serve(
    config: Config,
    config_file: Option<PathBuf>,
    source: ConfigSource,
) -> anyhow::Result<()> {
    let log_filter = init_tracing(&config.app.log_level);
    let lc = Lifecycle::new();
    stop_on_signals(lc.clone());
    // First, so probes answer while the steps below run.
    let health = crate::health::start(&config, &lc).await?;
    let run = Run {
        config_file,
        source,
        log_filter,
        health,
        pid_file: None,
        status_socket: None,
        hangup: true,
    };
    serve_with(config, run, lc).await
}

/// The listeners' ports checked on the addresses their hosts resolve to,
/// before anything binds: validation compares the hosts as written, so a
/// hostname that is another listener's IP passes it. A host that doesn't
/// resolve is left to its listener, which reports it.
fn check_ports(config: &Config) -> anyhow::Result<()> {
    let resolved: Vec<(&str, SocketAddr)> = config
        .listeners()
        .into_iter()
        .filter_map(|(field, host, port)| {
            let addr = (host, port).to_socket_addrs().ok()?.next()?;
            Some((field, addr))
        })
        .collect();
    let clashes = port_clashes(&resolved);
    if clashes.is_empty() {
        return Ok(());
    }
    Err(apollo_config::ConfigError::Validation(clashes)).context("config is invalid")
}

/// A message for each two of `listeners` (field, address) that share a port
/// where their addresses overlap.
fn port_clashes(listeners: &[(&str, SocketAddr)]) -> Vec<String> {
    let mut clashes = Vec::new();
    for (i, &(field, addr)) in listeners.iter().enumerate() {
        for &(other, other_addr) in &listeners[..i] {
            if addr.port() == other_addr.port()
                && validate::addrs_overlap(addr.ip(), other_addr.ip())
            {
                clashes.push(format!(
                    "{field} {addr} is also {other} {other_addr}: give each listener its own port"
                ));
            }
        }
    }
    clashes
}

/// What [`serve_with`] needs besides the config: where the config comes
/// from, the health listener started before it, and where it leaves what
/// other commands find it by. Tests point those at a directory of their own.
struct Run {
    /// The config file, as `apollo status` shows it.
    config_file: Option<PathBuf>,
    source: ConfigSource,
    log_filter: Option<LogFilter>,
    /// The health listener's socket, when `[health]` is on.
    health: Option<Bound>,
    /// The PID file; `None` for the default location.
    pid_file: Option<PathBuf>,
    /// The status socket; `None` for the default location.
    status_socket: Option<PathBuf>,
    /// Reload the config on SIGHUP.
    hangup: bool,
}

/// Start, following `lc`, and serve until it stops. A stop during startup
/// ends the start, handing back what recovery took, and returns `Ok`.
async fn serve_with(config: Config, run: Run, lc: Lifecycle) -> anyhow::Result<()> {
    log_stop_budget(&config.app);

    let addr = (config.app.endpoint.as_str(), config.app.port)
        .to_socket_addrs()
        .with_context(|| format!("resolving {}:{}", config.app.endpoint, config.app.port))?
        .next()
        .ok_or_else(|| anyhow!("no address for {}:{}", config.app.endpoint, config.app.port))?;
    let metrics_allow = metrics_allowlist(&config)?;

    // open() runs migrations internally.
    let Some(storage) = unless_stopped(&lc, apollo_storage::open(&config.database)).await else {
        return stopped_starting(&lc, None).await;
    };
    let storage = storage.context("opening storage")?;

    let webhook = webhook_sink(&config)?;

    // Engine::with_lifecycle spawns background tasks, so it must run inside
    // the runtime.
    let node = node_id(&config).context("reading the node id")?;
    let config = Arc::new(config);
    // Kept for the shared auth key and revocations.
    let db = Arc::clone(&storage);
    let engine = Engine::with_lifecycle(config.clone(), storage, webhook, node, lc.clone());
    // The allow lists in force, replaced by each reload.
    let (published, rules) = watch::channel(Published {
        generation: engine.config_generation().0,
        metrics: metrics_allow,
    });
    let metrics = start_metrics(&config.metrics, engine.clone(), rules).await?;
    let reloader = Reloader {
        engine: engine.clone(),
        source: run.source,
        log_filter: run.log_filter,
        published,
        metrics,
        health: run.health,
    };
    if run.hangup {
        reload_on_hangup(reloader.clone());
    }

    // Owns the PID file for this process; removed on graceful shutdown.
    let _pidfile = run.pid_file.map_or_else(
        crate::daemon::PidFile::create,
        crate::daemon::PidFile::create_at,
    )?;

    lc.starting(Step::ConfiguringAuth);
    let Some(auth) = unless_stopped(&lc, configure_auth(&config, &db, engine.node())).await else {
        return stopped_starting(&lc, Some(&engine)).await;
    };
    let auth = auth?;
    auth.watch_revocations(REVOCATION_POLL);
    let tls = load_tls(config.tls.as_ref())?;

    // Take the port before any task: a second start on the same database
    // (and port) must fail before it claims anything.
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr}"))?;

    // Take tasks over only once nothing above can fail the start: a failed
    // start would leave its claims to lapse, which the next start counts as a
    // crash of each task.
    lc.starting(Step::RecoveringTasks);
    // A stop drops only this wait: recovery runs in a task of its own to its
    // next safe point, and the hand-back waits for it.
    let resumed = match unless_stopped(&lc, engine.recover()).await {
        None => return stopped_starting(&lc, Some(&engine)).await,
        Some(Ok(n)) => n,
        Some(Err(e)) => {
            // Hand back whatever it took before failing.
            engine.hand_back().await;
            return Err(e).context("recovering tasks");
        }
    };
    if resumed > 0 {
        tracing::info!(resumed, "re-queued in-flight tasks from a previous run");
    }
    if tls.is_none() && !addr.ip().is_loopback() {
        tracing::warn!(
            %addr,
            "serving plaintext gRPC on a non-loopback address: tokens, media and results \
             cross the network unencrypted — configure [tls] or put a TLS proxy in front"
        );
    }

    // Answer `apollo status` on a local socket for as long as the server runs.
    let info = ServerInfo {
        started_at: std::time::SystemTime::now(),
        listen: addr.to_string(),
        tls: tls.is_some(),
        mutual_tls: tls.as_ref().is_some_and(|t| t.client_ca_pem.is_some()),
        auth: auth.is_enabled(),
        config_file: run.config_file.map(|p| p.display().to_string()),
        database: match config.database.backend {
            // Instances sharing the database tell each other apart by node.
            apollo_config::Backend::Surrealdb => format!(
                "{} (node {})",
                describe_database(&config.database),
                engine.node()
            ),
            _ => describe_database(&config.database),
        },
        log_file: std::env::var(DAEMON_LOG_ENV).ok(),
    };
    let _status_socket = start_status_socket(engine.clone(), info, reloader, run.status_socket);

    stop_after_drain_delay(engine.clone());
    lc.starting(Step::LoadingModels);
    ready_after_preloads(engine.clone());
    let health = apollo_server::Health::new(&lc).await;
    tracing::info!(%addr, models = config.model_count(), "apollo listening");
    // Stopping closes the port; the drain timeout (as the config says when
    // the stop begins) or a second signal cuts the calls still open.
    let timeout = {
        let engine = engine.clone();
        move || Duration::from_secs(engine.config().app.drain_timeout_secs.into())
    };
    let served = apollo_server::serve_until_stopped(
        apollo_server::serve_on(engine.clone(), listener, health, auth, tls),
        &lc,
        timeout,
    )
    .await;
    // Also when serving failed: probes say so while the tasks go back.
    lc.stopping();

    // Hand unfinished tasks back, also when serving failed: a clean stop must
    // not count toward the poison-task cap for whatever was still in flight,
    // and the next start (or another instance) takes them at once.
    engine.hand_back().await;
    served.context("gRPC server error")?;

    tracing::info!("apollo shut down cleanly");
    Ok(())
}

/// A Ctrl-C or SIGTERM drains; one while draining (or later) stops at once.
fn stop_on_signals(lc: Lifecycle) {
    let (tx, rx) = mpsc::unbounded_channel();
    // Listening from here on, so no signal is lost while one is handled.
    match crate::daemon::shutdown_signals() {
        Ok(mut signals) => {
            tokio::spawn(async move {
                loop {
                    signals.recv().await;
                    if tx.send(()).is_err() {
                        return;
                    }
                }
            });
        }
        Err(e) => tracing::warn!(
            error = %e,
            "can't listen for SIGTERM and Ctrl-C: they won't drain the server"
        ),
    }
    stop_on(lc, rx);
}

/// Each signal on `signals`: the first drains (drained by hand or not), any
/// later one stops at once.
fn stop_on(lc: Lifecycle, mut signals: mpsc::UnboundedReceiver<()>) {
    tokio::spawn(async move {
        while signals.recv().await.is_some() {
            if lc.drain() {
                tracing::info!("shutdown signal received; draining (a second one stops at once)");
            } else {
                tracing::warn!("another shutdown signal: stopping at once");
                lc.force();
            }
        }
    });
}

/// `step`, unless a stop is asked for first: then it is dropped, and `None`.
async fn unless_stopped<T>(lc: &Lifecycle, step: impl Future<Output = T>) -> Option<T> {
    tokio::select! {
        biased;
        () = lc.reached(Phase::Draining) => None,
        done = step => Some(done),
    }
}

/// End a start that a stop interrupted: hand back what recovery took
/// (within the hand-back limit), unless a second signal says to stop at once.
async fn stopped_starting(lc: &Lifecycle, engine: Option<&Engine>) -> anyhow::Result<()> {
    lc.stopping();
    tracing::info!("stopped while starting");
    if let Some(engine) = engine {
        tokio::select! {
            _ = engine.hand_back() => {}
            () = lc.until_forced() => {
                tracing::warn!("stopping at once, without handing the tasks back")
            }
        }
    }
    Ok(())
}

/// Stop serving `[app].drain_delay_secs` after the drain begins (as the
/// config says then), or at once when forced.
fn stop_after_drain_delay(engine: Engine) {
    tokio::spawn(async move {
        let lc = engine.lifecycle().clone();
        lc.reached(Phase::Draining).await;
        let delay = Duration::from_secs(engine.config().app.drain_delay_secs.into());
        if !delay.is_zero() {
            tracing::info!(
                ?delay,
                "unready, and still serving while load balancers stop sending work here"
            );
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = lc.until_forced() => {}
            }
        }
        lc.stopping();
    });
}

/// Serving, once every pinned model has tried its first load (loaded or
/// not), unless a stop comes first.
fn ready_after_preloads(engine: Engine) {
    tokio::spawn(async move {
        let lc = engine.lifecycle().clone();
        tokio::select! {
            () = engine.preloads_attempted() => {}
            () = lc.reached(Phase::Draining) => return,
        }
        lc.serving();
        if lc.ready() {
            tracing::info!("apollo ready");
        } else if lc.drained() && lc.phase() == Phase::Serving {
            tracing::info!("apollo serving, but drained: `apollo drain --undo` makes it ready");
        }
    });
}

/// `apollo drain` (`drain`), or `apollo drain --undo`: take this instance out
/// of rotation, or back in.
#[cfg(unix)]
fn drain_by_hand(lc: &Lifecycle, drain: bool) -> crate::status::DrainOutcome {
    let changed = lc.set_drained(drain);
    if changed && drain {
        tracing::info!(
            "drained (apollo drain): unready with weight 0, and taking no work over; \
             still serving what it is sent"
        );
    } else if changed {
        tracing::info!(ready = lc.ready(), "undrained (apollo drain --undo)");
    }
    crate::status::DrainOutcome {
        drained: lc.drained(),
        changed,
        phase: lc.phase().name().to_string(),
        ready: lc.ready(),
    }
}

/// Log the longest a stop takes, for the process manager's grace period.
fn log_stop_budget(app: &apollo_config::AppConfig) {
    let hand_back = HAND_BACK_LIMIT.as_secs();
    let delay = app.drain_delay_secs;
    match app.drain_timeout_secs {
        0 => tracing::info!(
            drain_delay_secs = delay,
            "a stop waits for the open calls with no time limit ([app].drain_timeout_secs = 0)"
        ),
        timeout => {
            let budget = u64::from(delay) + u64::from(timeout) + hand_back;
            tracing::info!(
                "a stop takes at most {budget}s (drain delay {delay}s + drain timeout {timeout}s \
                 + {hand_back}s to hand tasks back): give it that long, e.g. \
                 terminationGracePeriodSeconds >= {budget}"
            );
        }
    }
}

/// This instance's node id: `[app].node_id`, or one kept in the state directory
/// for its upload directory (two instances on one machine with their own cache
/// directories are two nodes), made on first use.
pub(super) fn node_id(config: &Config) -> anyhow::Result<String> {
    if let Some(node) = &config.app.node_id {
        return Ok(node.clone());
    }
    let uploads = match &config.app.cache_dir {
        Some(dir) => PathBuf::from(dir),
        None => dirs::cache_dir()
            .unwrap_or_else(std::env::temp_dir)
            .join("apollo"),
    };
    // The same string whether or not the directory exists yet (it's made by
    // the first upload), so the id doesn't change once it does.
    let uploads = std::path::absolute(&uploads).unwrap_or(uploads);
    let uploads = uploads
        .to_string_lossy()
        .trim_end_matches(std::path::MAIN_SEPARATOR)
        .to_string();
    // FNV-1a: a stable name for the directory, not a security boundary.
    let key = uploads.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    let path = crate::daemon::state_dir()?.join(format!("node-{key:016x}"));
    let read = |path: &std::path::Path| -> Option<String> {
        let id = std::fs::read_to_string(path).ok()?.trim().to_string();
        (!id.is_empty()).then_some(id)
    };
    if let Some(id) = read(&path) {
        return Ok(id);
    }
    // Written aside, then linked into place: two starts racing agree on one.
    let id = format!("n-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&tmp, &id).with_context(|| format!("writing {}", tmp.display()))?;
    let linked = std::fs::hard_link(&tmp, &path);
    let _ = std::fs::remove_file(&tmp);
    match linked {
        Ok(()) => Ok(id),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            read(&path).ok_or_else(|| anyhow!("{} is empty", path.display()))
        }
        Err(e) => Err(e).with_context(|| format!("writing {}", path.display())),
    }
}

/// Set by `start --daemon` for the detached child: the log file it writes to.
const DAEMON_LOG_ENV: &str = "APOLLO_DAEMON_LOG";

/// Static facts about this server, reported by `apollo status`.
#[derive(Clone)]
struct ServerInfo {
    started_at: std::time::SystemTime,
    listen: String,
    tls: bool,
    mutual_tls: bool,
    auth: bool,
    config_file: Option<String>,
    database: String,
    log_file: Option<String>,
}

/// The `[webhook]` sink for `config`, if it has one and it's enabled.
fn webhook_sink(config: &Config) -> anyhow::Result<Option<Arc<dyn WebhookSink>>> {
    match &config.webhook {
        Some(w) if w.enabled => {
            let sink = apollo_server::GrpcWebhookSink::new(&w.url, w.secret.clone())
                .context("configuring webhook client")?;
            Ok(Some(Arc::new(sink) as Arc<dyn WebhookSink>))
        }
        _ => Ok(None),
    }
}

/// Re-reads the config and applies it to the running server (`apollo reload`,
/// SIGHUP).
#[derive(Clone)]
struct Reloader {
    engine: Engine,
    source: ConfigSource,
    log_filter: Option<LogFilter>,
    /// The allow lists the listeners check.
    published: watch::Sender<Published>,
    /// The `/metrics` listener's socket, when it runs.
    metrics: Option<Bound>,
    /// The health listener's socket, when it runs.
    health: Option<Bound>,
}

impl Reloader {
    /// Load, validate and apply the config. On any error the running config
    /// stays as it was.
    async fn reload(&self) -> anyhow::Result<crate::status::ReloadOutcome> {
        let config = self.source.load()?;
        // A list that can't match the running listener keeps the running config.
        let metrics_allow = metrics_allowlist(&config)?;
        let warnings = match (&self.metrics, &metrics_allow) {
            (Some(bound), Some(list)) => bound.check("metrics.allow", list)?,
            _ => Vec::new(),
        };
        let next_start = self.check_health_allow(&config);
        let webhook = webhook_sink(&config)?;
        let level = config.app.log_level.clone();
        let report = self.engine.reload(config, webhook).await?;
        // `allow` is reloadable, so the list read is the one applied.
        let next = Published {
            generation: report.generation,
            metrics: metrics_allow.clone(),
        };
        if allow::publish(&self.published, next)
            && let Some(bound) = &self.metrics
        {
            for warning in warnings {
                tracing::warn!("{warning}");
            }
            tracing::info!("{}", bound.describe("metrics", metrics_allow.as_deref()));
        }
        if let Some(filter) = &self.log_filter {
            set_log_level(filter, &level);
        }
        let mut problems = Vec::new();
        match next_start {
            Some(Ok(warnings)) => {
                for warning in warnings {
                    tracing::warn!("{warning} (from the next start on)");
                }
            }
            Some(Err(e)) => {
                let problem = format!("the next start will fail: {e}");
                tracing::warn!("{problem}");
                problems.push(problem);
            }
            None => {}
        }
        let m = report.models;
        Ok(crate::status::ReloadOutcome {
            generation: report.generation,
            added: m.added,
            reloaded: m.reloaded,
            kept: m.kept,
            removed: m.removed,
            restart_required: report.restart_required,
            warnings: problems,
        })
    }

    /// A changed `[health].allow` in `config`, which applies at the next
    /// start, checked as that start will check it: against the health
    /// listener, while the next start binds the same address. `None` when
    /// there is nothing to check.
    fn check_health_allow(&self, config: &Config) -> Option<anyhow::Result<Vec<String>>> {
        let bound = self.health.as_ref()?;
        let running = self.engine.config();
        let (next, now) = (&config.health, &running.health);
        let same_address = next.enabled
            && next.port == now.port
            && next.bind_host(&config.app) == now.bind_host(&running.app);
        if !same_address || next.allow == now.allow {
            return None;
        }
        // The config is valid, so the list parses.
        let list = next.allowlist().ok().flatten()?;
        Some(bound.check("health.allow", &list))
    }
}

/// Reload the config on SIGHUP.
fn reload_on_hangup(reloader: Reloader) {
    #[cfg(unix)]
    tokio::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};
        let mut hangup = match signal(SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "SIGHUP reload disabled");
                return;
            }
        };
        while hangup.recv().await.is_some() {
            tracing::info!("SIGHUP received; reloading the config");
            if let Err(e) = reloader.reload().await {
                tracing::error!(error = %format!("{e:#}"), "config reload failed; the running config stays");
            }
        }
    });
    #[cfg(not(unix))]
    let _ = reloader;
}

/// `[metrics].allow` parsed: `None` admits everyone.
fn metrics_allowlist(config: &Config) -> anyhow::Result<Option<Arc<Allowlist>>> {
    allow::parsed("metrics.allow", config.metrics.allowlist())
}

/// Serve `GET /metrics` when `[metrics]` is enabled, to the peers the list
/// in `rules` admits. The listener runs until the process exits. Returns its
/// socket.
async fn start_metrics(
    config: &apollo_config::MetricsConfig,
    engine: Engine,
    rules: watch::Receiver<Published>,
) -> anyhow::Result<Option<Bound>> {
    if !config.enabled {
        return Ok(None);
    }
    let addr = (config.endpoint.as_str(), config.port)
        .to_socket_addrs()
        .with_context(|| format!("resolving [metrics] {}:{}", config.endpoint, config.port))?
        .next()
        .ok_or_else(|| {
            anyhow!(
                "no address for [metrics] {}:{}",
                config.endpoint,
                config.port
            )
        })?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding the metrics listener on {addr}"))?;
    let bound = Bound::of(&listener).context("reading the metrics listener's socket")?;
    let list = rules.borrow().metrics.clone();
    match &list {
        Some(list) => {
            for warning in bound.check("metrics.allow", list)? {
                tracing::warn!("{warning}");
            }
        }
        None if !addr.ip().is_loopback() => tracing::warn!(
            %addr,
            "serving unauthenticated metrics on a non-loopback address to anyone who can \
             reach it; set [metrics].allow to choose who may scrape"
        ),
        None => {}
    }
    let counter = engine.clone();
    let guard = allow::Guard::new(
        "metrics",
        "[metrics].allow",
        bound.local,
        allow::metrics_rules(rules),
        Arc::new(move |listener| counter.note_refused(listener)),
    );
    let app = axum::Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .with_state(engine);
    tracing::info!(%addr, "serving Prometheus metrics on /metrics");
    tracing::info!("{}", bound.describe("metrics", list.as_deref()));
    tokio::spawn(async move {
        if let Err(e) = allow::serve(listener, app, guard).await {
            tracing::error!(error = %e, "metrics listener stopped");
        }
    });
    Ok(Some(bound))
}

async fn metrics_handler(
    axum::extract::State(engine): axum::extract::State<Engine>,
) -> impl axum::response::IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        engine.metrics_text().await,
    )
}

/// The configured database, without credentials.
fn describe_database(db: &apollo_config::DatabaseConfig) -> String {
    use apollo_config::Backend;
    match db.backend {
        Backend::Sqlite => format!("sqlite {}", db.sqlite.clone().unwrap_or_default().path),
        Backend::Surrealdb => match &db.surrealdb {
            Some(s) => format!("surrealdb {} ({}/{})", s.url, s.namespace, s.database),
            None => "surrealdb".into(),
        },
        Backend::Postgres => "postgres".into(),
    }
}

/// Serve `apollo status`, `reload` and `drain` on the local status socket.
/// Failure is logged, not fatal: the server works without it.
#[cfg(unix)]
fn start_status_socket(
    engine: Engine,
    info: ServerInfo,
    reloader: Reloader,
    path: Option<PathBuf>,
) -> Option<crate::status::SocketGuard> {
    let path = match path.map_or_else(crate::status::socket_path, Ok) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "status socket disabled");
            return None;
        }
    };
    match serve_status(&path, engine, info, reloader) {
        Ok(guard) => {
            tracing::info!(socket = %path.display(), "status socket ready (`apollo status`)");
            Some(guard)
        }
        Err(e) => {
            tracing::warn!(socket = %path.display(), error = %e, "status socket disabled");
            None
        }
    }
}

/// Serve the status socket on `path`.
#[cfg(unix)]
fn serve_status(
    path: &std::path::Path,
    engine: Engine,
    info: ServerInfo,
    reloader: Reloader,
) -> std::io::Result<crate::status::SocketGuard> {
    let lc = engine.lifecycle().clone();
    let report = move || {
        let engine = engine.clone();
        let info = info.clone();
        async move { Ok(build_report(&engine, &info).await) }
    };
    let reload = move || {
        let reloader = reloader.clone();
        async move { reloader.reload().await }
    };
    crate::status::serve_socket(path, report, reload, move |drain| drain_by_hand(&lc, drain))
}

#[cfg(not(unix))]
fn start_status_socket(
    _engine: Engine,
    _info: ServerInfo,
    _reloader: Reloader,
    _path: Option<PathBuf>,
) -> Option<()> {
    None
}

/// Combine the static server facts with the engine's live state.
#[cfg(unix)]
async fn build_report(engine: &Engine, info: &ServerInfo) -> crate::status::StatusReport {
    use crate::status::{self as st, unix_secs};
    let s = engine.status().await;
    let config = engine.config();
    let (generation, loaded_at) = engine.config_generation();
    let lc = engine.lifecycle();
    let phase = lc.phase();
    let load = lc.load();
    st::StatusReport {
        version: env!("CARGO_PKG_VERSION").to_string(),
        pid: std::process::id(),
        started_at: unix_secs(info.started_at),
        phase: Some(phase.name().to_string()),
        step: match phase {
            apollo_engine::Phase::Starting(step) => Some(step.name().to_string()),
            _ => None,
        },
        drained: Some(lc.drained()),
        listen: info.listen.clone(),
        tls: info.tls,
        mutual_tls: info.mutual_tls,
        auth: info.auth,
        config_file: info.config_file.clone(),
        config_generation: Some(generation),
        config_loaded_at: Some(unix_secs(loaded_at)),
        database: info.database.clone(),
        log_file: info.log_file.clone(),
        device: s.device,
        memory: st::Memory {
            used_bytes: s.memory_bytes,
            limit_bytes: s.max_memory_bytes,
        },
        queue: st::Queue {
            items_in_flight: s.items_in_flight,
            max_pending: s.max_pending,
            slots_total: s.slots_total,
            slots_busy: s.slots_busy,
            slots_waiting: s.slots_waiting,
        },
        load: Some(st::Load {
            utilization: load.utilization,
            weight: load.weight(lc.ready()),
            degraded: load.degraded().into_iter().map(String::from).collect(),
            database: load.database.name().to_string(),
        }),
        tasks: s.stored.map(|c| st::Tasks {
            queued: c.tasks.queued,
            processing: c.tasks.processing,
            completed: c.tasks.completed,
            failed: c.tasks.failed,
            cancelled: c.tasks.cancelled,
        }),
        webhook: config
            .webhook
            .as_ref()
            .filter(|w| w.enabled)
            .map(|w| st::Webhook {
                url: w.url.clone(),
                pending: s.stored.and_then(|c| c.webhooks_pending),
            }),
        models: s
            .models
            .into_iter()
            .map(|m| st::Model {
                label: m.label,
                repo: m.repo,
                architecture: m.architecture,
                enabled: m.enabled,
                loaded: m.loaded,
                keep_in_memory: m.keep_in_memory,
                batch: m.batch,
                input_size: m.input_size,
                queued_jobs: m.queued_jobs,
                loads: m.loads,
                classified: m.classified,
                last_used: m.last_used.map(unix_secs),
            })
            .collect(),
    }
}

/// How often a running server re-checks the token revocation file.
const REVOCATION_POLL: std::time::Duration = std::time::Duration::from_secs(10);

/// Read the `[tls]` PEM files, if configured.
fn load_tls(tls: Option<&TlsConfig>) -> anyhow::Result<Option<apollo_server::TlsSettings>> {
    let Some(tls) = tls else {
        return Ok(None);
    };
    let read = |what: &str, path: &str| {
        std::fs::read(path).with_context(|| format!("reading [tls].{what} {path}"))
    };
    Ok(Some(apollo_server::TlsSettings {
        cert_pem: read("cert_file", &tls.cert_file)?,
        key_pem: read("key_file", &tls.key_file)?,
        client_ca_pem: tls
            .client_ca_file
            .as_deref()
            .map(|p| read("client_ca_file", p))
            .transpose()?,
    }))
}

/// Build the auth interceptor. Authentication is on unless the config says
/// `[auth] enabled = false`; without a configured `public_key` the server uses
/// (creating it on first start) the local keypair that `apollo token` reads.
async fn configure_auth(
    config: &Config,
    storage: &Arc<dyn apollo_storage::Storage>,
    node: &str,
) -> anyhow::Result<apollo_server::AuthInterceptor> {
    use super::keys::{KeyChoice, within_db_timeout};
    let auth = &config.auth;
    if !auth.enabled {
        tracing::warn!(
            "authentication is DISABLED ([auth] enabled = false): anyone who can reach this \
             port can submit work and read results"
        );
        return Ok(apollo_server::AuthInterceptor::disabled());
    }
    let shared = storage.shared().then(|| storage.as_ref());
    let local_path = super::keys::local_key_path()?;
    let secs = config.database.timeout;
    let (public_key, choice) = within_db_timeout(
        secs,
        super::keys::resolve_public_key(auth, shared, node, &local_path),
    )
    .await
    .context("resolving the auth public key")?;
    let path = local_path.display();
    match &choice {
        KeyChoice::Local { created } => {
            if *created {
                tracing::info!(%path, "generated a new signing keypair");
            }
            tracing::info!(
                %path,
                "PASETO authentication enabled (local keypair); mint client tokens with \
                 `apollo token --subject NAME`"
            );
        }
        KeyChoice::Configured { published: false } if shared.is_none() => {
            tracing::info!("PASETO authentication enabled (configured public key)")
        }
        KeyChoice::Configured { published } => tracing::info!(
            key = %public_key,
            "PASETO authentication enabled (configured public key, {})",
            if *published {
                "published to the database"
            } else {
                "matches the database's"
            }
        ),
        KeyChoice::Published { created } => {
            if *created {
                tracing::info!(%path, "generated a new signing keypair");
            }
            tracing::info!(
                %path,
                key = %public_key,
                "PASETO authentication enabled: published this host's public key to the \
                 database; every instance sharing it verifies tokens with it — mint tokens on \
                 this host"
            );
        }
        KeyChoice::SharedLocal => tracing::info!(
            %path,
            key = %public_key,
            "PASETO authentication enabled (local keypair, which is the database's shared key)"
        ),
        KeyChoice::Database { node: from, local } => {
            let from = from.as_deref().unwrap_or("(unknown)");
            match local {
                None => tracing::info!(
                    key = %public_key,
                    node = from,
                    "PASETO authentication enabled, with the database's shared public key; mint \
                     tokens on that node or with its secret key"
                ),
                Some((_, true)) => tracing::info!(
                    key = %public_key,
                    node = from,
                    "PASETO authentication enabled: another instance published its key first; \
                     using it"
                ),
                Some((_, false)) => tracing::warn!(
                    %path,
                    key = %public_key,
                    node = from,
                    "this host's local keypair is not the database's shared key: tokens signed \
                     with it are rejected here and on every instance sharing the database"
                ),
            }
        }
    }
    let revocation_file = super::keys::revocation_file(auth)?;
    if !auth.admin_subjects.is_empty() {
        tracing::info!(admins = ?auth.admin_subjects, "admin subjects may access every task");
    }
    let shared_revocations = shared.map(|_| {
        Arc::new(super::keys::DbRevocations(Arc::clone(storage)))
            as Arc<dyn apollo_server::RevocationSource>
    });
    let interceptor = apollo_server::AuthInterceptor::new(apollo_server::AuthSettings {
        public_key,
        admin_subjects: auth.admin_subjects.clone(),
        revocation_file: Some(revocation_file.clone()),
        shared_revocations,
    })
    .context("configuring authentication")?;
    if shared.is_some() {
        within_db_timeout(secs, async {
            interceptor
                .load_shared_revocations()
                .await
                .map_err(|e| anyhow!(e))
        })
        .await
        .context("reading token revocations from the database")?;
        tracing::info!(
            file = %revocation_file.display(),
            "token revocations: the file and the database (polled every 10 s)"
        );
    } else {
        tracing::info!(file = %revocation_file.display(), "token revocation list (watched for changes)");
    }
    Ok(interceptor)
}

/// Changes the log filter of the running subscriber.
type LogFilter =
    tracing_subscriber::reload::Handle<tracing_subscriber::EnvFilter, tracing_subscriber::Registry>;

/// Set up logging. Returns a handle to change the level on reload, or `None`
/// when `RUST_LOG` is set (it wins over the config, then and on reload) or a
/// subscriber already existed.
fn init_tracing(level: &str) -> Option<LogFilter> {
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{EnvFilter, fmt, reload};
    let from_env = EnvFilter::try_from_default_env().ok();
    let pinned = from_env.is_some();
    let filter = from_env.unwrap_or_else(|| EnvFilter::new(level));
    let (filter, handle) = reload::Layer::new(filter);
    // try_init fails if a subscriber is already set (e.g. in tests).
    let installed = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .try_init()
        .is_ok();
    (installed && !pinned).then_some(handle)
}

fn set_log_level(filter: &LogFilter, level: &str) {
    match tracing_subscriber::EnvFilter::try_new(level) {
        Ok(new) => {
            if let Err(e) = filter.reload(new) {
                tracing::warn!(error = %e, "could not change the log level");
            }
        }
        Err(e) => tracing::warn!(level, error = %e, "invalid log level; unchanged"),
    }
}

/// Re-exec this binary as a detached background `start` (without `--daemon`).
/// The child process writes and owns the PID file once it is serving.
fn spawn_detached(args: &StartArgs) -> anyhow::Result<()> {
    let exe = std::env::current_exe().context("locating current executable")?;
    let log_path = crate::daemon::log_path()?;
    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let log = options
        .open(&log_path)
        .with_context(|| format!("opening log file {}", log_path.display()))?;
    let log_err = log.try_clone().context("cloning log handle")?;

    let mut cmd = Command::new(exe);
    // Lets the detached server report its log file in `apollo status`.
    cmd.env(DAEMON_LOG_ENV, &log_path);
    cmd.arg("start");
    if let Some(c) = &args.config {
        cmd.arg("--config").arg(c);
    }
    if let Some(e) = &args.endpoint {
        cmd.arg("--endpoint").arg(e);
    }
    if let Some(p) = args.port {
        cmd.arg("--port").arg(p.to_string());
    }
    if let Some(u) = &args.webhook_url {
        cmd.arg("--webhook-url").arg(u);
    }
    if let Some(p) = args.health_port {
        cmd.arg("--health-port").arg(p.to_string());
    }
    if let Some(p) = args.metrics_port {
        cmd.arg("--metrics-port").arg(p.to_string());
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // New process group so it survives the parent shell.
        cmd.process_group(0);
    }

    let child = cmd.spawn().context("spawning background process")?;
    println!(
        "apollo started in background (pid {}); logging to {}",
        child.id(),
        log_path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::tests::{PROMPT, assert_in, get, start_health};

    /// A config for [`serve_with`] that keeps to `dir`: its database and
    /// cache there, its node id given and no auth, so nothing outside `dir`
    /// is read or written. `app` are more `[app]` lines, `rest` more tables.
    fn config_in(dir: &tempfile::TempDir, app: &str, rest: &str) -> String {
        format!(
            "[app]\nnode_id = \"n\"\ncache_dir = \"{}\"\n{app}[auth]\nenabled = false\n\
             [database.sqlite]\npath = \"{}\"\n{rest}",
            dir.path().display(),
            dir.path().join("apollo.db").display()
        )
    }

    /// A model that needs no weights, for a config that must have one.
    fn model_in(dir: &tempfile::TempDir) -> String {
        let list = dir.path().join("known.txt");
        std::fs::write(&list, format!("{} 1\n", "ab".repeat(32))).unwrap();
        format!(
            "[models.known]\narchitecture = \"pdq\"\nhash_list = \"{}\"\n\
             categories = {{ 1 = \"known\" }}\n",
            list.display()
        )
    }

    #[test]
    fn listeners_clash_where_their_resolved_addresses_overlap() {
        let at = |s: &str| s.parse::<SocketAddr>().unwrap();
        let clashes = |listeners: &[(&str, &str)]| {
            let listeners: Vec<(&str, SocketAddr)> =
                listeners.iter().map(|&(f, a)| (f, at(a))).collect();
            port_clashes(&listeners)
        };
        assert_eq!(
            clashes(&[
                ("app.port", "127.0.0.1:8080"),
                ("metrics.port", "127.0.0.1:8080")
            ]),
            [
                "metrics.port 127.0.0.1:8080 is also app.port 127.0.0.1:8080: \
              give each listener its own port"
            ]
        );
        assert_eq!(
            clashes(&[
                ("app.port", "0.0.0.0:8080"),
                ("metrics.port", "127.0.0.1:8080")
            ])
            .len(),
            1
        );
        assert_eq!(
            clashes(&[("app.port", "[::]:8080"), ("metrics.port", "10.0.0.5:8080")]).len(),
            1
        );
        let apart: &[&[(&str, &str)]] = &[
            &[
                ("app.port", "[::1]:8080"),
                ("metrics.port", "127.0.0.1:8080"),
            ],
            &[
                ("app.port", "127.0.0.1:8080"),
                ("metrics.port", "127.0.0.1:8081"),
            ],
            &[
                ("app.port", "10.0.0.5:8080"),
                ("metrics.port", "10.0.0.6:8080"),
            ],
        ];
        for listeners in apart {
            assert_eq!(clashes(listeners), Vec::<String>::new(), "{listeners:?}");
        }
    }

    #[test]
    fn a_hostname_that_is_another_listeners_address_fails_the_start() {
        // Resolving `localhost` reads the hosts file; nothing binds.
        let local = ("localhost", 0)
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap()
            .ip();
        let config = |metrics: &str| {
            let toml = format!(
                "[app]\nendpoint = \"localhost\"\nport = 9464\n\
                 [metrics]\nenabled = true\nendpoint = \"{metrics}\"\n"
            );
            let config = load::from_str(&toml).unwrap();
            // Validation compares the hosts as written: it can't tell.
            config.validate().expect("valid as written");
            config
        };
        let err = check_ports(&config(&local.to_string())).unwrap_err();
        let addr = SocketAddr::new(local, 9464);
        assert!(
            format!("{err:#}").contains(&format!(
                "metrics.port {addr} is also app.port {addr}: give each listener its own port"
            )),
            "{err:#}"
        );
        // The other loopback address is apart.
        let other = if local.is_ipv4() { "::1" } else { "127.0.0.1" };
        check_ports(&config(other)).unwrap();
    }

    /// A [`Run`] whose PID file and status socket are in `dir`, which reads
    /// `dir/apollo.toml` on reload, and which leaves SIGHUP alone.
    fn run_in(dir: &tempfile::TempDir, health: Option<Bound>) -> Run {
        Run {
            config_file: None,
            source: ConfigSource {
                path: Some(dir.path().join("apollo.toml")),
                overrides: Overrides::default(),
            },
            log_filter: None,
            health,
            pid_file: Some(dir.path().join("apollo.pid")),
            status_socket: Some(dir.path().join("s.sock")),
            hangup: false,
        }
    }

    /// Wait until `done`, failing with `what` after `limit`.
    async fn until(what: &str, limit: Duration, done: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + limit;
        while !done() {
            assert!(std::time::Instant::now() < deadline, "{what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// An engine on `toml` (in `dir`), and a [`Reloader`] for it that reads
    /// `dir/apollo.toml`, with `[metrics]` started as `toml` says and the
    /// health listener `health`.
    async fn reloader_in(dir: &tempfile::TempDir, toml: &str, health: Option<Bound>) -> Reloader {
        let config = load::from_str(toml).unwrap();
        let storage = apollo_storage::open(&config.database).await.unwrap();
        let engine = Engine::with_lifecycle(
            Arc::new(config.clone()),
            storage,
            None,
            "n".into(),
            Lifecycle::new(),
        );
        assert_in(&engine, dir);
        let (published, rules) = watch::channel(Published {
            generation: engine.config_generation().0,
            metrics: metrics_allowlist(&config).unwrap(),
        });
        let metrics = start_metrics(&config.metrics, engine.clone(), rules)
            .await
            .unwrap();
        Reloader {
            engine,
            source: ConfigSource {
                path: Some(dir.path().join("apollo.toml")),
                overrides: Overrides::default(),
            },
            log_filter: None,
            published,
            metrics,
            health,
        }
    }

    #[tokio::test]
    async fn a_disabled_webhook_builds_no_sink() {
        let config = |toml: &str| apollo_config::load::from_str(toml).unwrap();
        let on = "[webhook]\nurl = \"http://127.0.0.1:1\"\n";
        assert!(webhook_sink(&config(on)).unwrap().is_some());
        let off = format!("{on}enabled = false\n");
        assert!(webhook_sink(&config(&off)).unwrap().is_none());
        assert!(webhook_sink(&config("")).unwrap().is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn metrics_and_status_answer_while_the_database_hangs() {
        use crate::status::{Webhook, render};
        use std::time::{Instant, SystemTime};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let dir = tempfile::tempdir().unwrap();
        let toml = format!(
            "[app]\ncache_dir = \"{}\"\n[database]\ntimeout = 1\n[database.sqlite]\npath = \"{}\"\n\
             [metrics]\nenabled = true\nport = 0\n\
             [webhook]\nurl = \"http://127.0.0.1:1\"\n",
            dir.path().display(),
            dir.path().join("apollo.db").display()
        );
        let config = load::from_str(&toml).unwrap();
        let storage = apollo_storage::open(&config.database).await.unwrap();
        let (stalled, gate) = apollo_engine::testing::Stalled::new(storage);
        let sink = webhook_sink(&config).unwrap();
        let engine = Engine::new(Arc::new(config.clone()), Arc::new(stalled), sink);
        assert_in(&engine, &dir);
        let (_tx, rx) = watch::channel(Published {
            generation: 1,
            metrics: None,
        });
        let metrics = start_metrics(&config.metrics, engine.clone(), rx)
            .await
            .unwrap()
            .unwrap();
        let info = ServerInfo {
            started_at: SystemTime::now(),
            listen: "127.0.0.1:0".into(),
            tls: false,
            mutual_tls: false,
            auth: false,
            config_file: None,
            database: describe_database(&config.database),
            log_file: None,
        };
        let scrape = || async move {
            let mut stream = tokio::net::TcpStream::connect(metrics.local).await.unwrap();
            stream
                .write_all(b"GET /metrics HTTP/1.1\r\nhost: test\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut answer = String::new();
            stream.read_to_string(&mut answer).await.unwrap();
            answer
        };

        gate.shut();
        let at = Instant::now();
        let (answer, report) = tokio::join!(scrape(), build_report(&engine, &info));
        // They wait [database].timeout, 1 s here.
        assert!(at.elapsed() < Duration::from_secs(2), "{:?}", at.elapsed());
        assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
        assert!(
            answer.contains("\napollo_metrics_database_ok 0\n"),
            "{answer}"
        );
        assert_eq!(report.tasks, None);
        assert_eq!(
            report.webhook,
            Some(Webhook {
                url: "http://127.0.0.1:1".into(),
                pending: None
            })
        );
        let text = render(&report, SystemTime::now(), None);
        assert!(
            text.contains("\ntasks      database unavailable\n"),
            "{text}"
        );

        gate.open();
        let (answer, report) = tokio::join!(scrape(), build_report(&engine, &info));
        assert!(
            answer.contains("\napollo_metrics_database_ok 1\n"),
            "{answer}"
        );
        assert!(report.tasks.is_some());
        assert_eq!(report.webhook.unwrap().pending, Some(0));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_server_drained_by_hand_then_signalled_drains_and_stops() {
        use crate::health::tests::start_with_agent;
        use crate::status::{DrainOutcome, drain, fetch};
        use tokio::io::AsyncReadExt;

        let dir = tempfile::tempdir().unwrap();
        let lc = Lifecycle::new();
        let (signal, signals) = mpsc::unbounded_channel();
        stop_on(lc.clone(), signals);
        let (health, agent) = start_with_agent("", &lc).await;
        let agent_line = || async move {
            let mut stream = tokio::net::TcpStream::connect(agent).await.unwrap();
            let mut line = String::new();
            stream.read_to_string(&mut line).await.unwrap();
            line
        };

        // The real start, with a drain delay of 2 s.
        let app = "endpoint = \"127.0.0.1\"\nport = 0\ndrain_delay_secs = 2\n";
        let config = load::from_str(&config_in(&dir, app, "")).unwrap();
        let served = tokio::spawn(serve_with(config, run_in(&dir, None), lc.clone()));
        until("never ready", Duration::from_secs(5), || lc.ready()).await;

        // Found where the other commands look: the PID file and the socket.
        let pid_file = dir.path().join("apollo.pid");
        let socket = dir.path().join("s.sock");
        let pid = std::fs::read_to_string(&pid_file).unwrap();
        assert_eq!(pid, std::process::id().to_string());
        let ask = |on: bool| {
            let path = socket.clone();
            tokio::task::spawn_blocking(move || drain(&path, on))
        };
        let report = || {
            let path = socket.clone();
            tokio::task::spawn_blocking(move || fetch(&path))
        };
        let outcome = |drained, changed, ready| DrainOutcome {
            drained,
            changed,
            phase: "serving".into(),
            ready,
        };
        let status = report().await.unwrap().unwrap();
        assert_eq!(
            (status.phase.as_deref(), status.step, status.drained),
            (Some("serving"), None, Some(false))
        );

        // Drained: unready everywhere, weight 0, still serving.
        assert_eq!(
            ask(true).await.unwrap().unwrap(),
            outcome(true, true, false)
        );
        for path in ["/readyz", "/", "/readyz/strict"] {
            assert_eq!(
                get(health, path).await,
                (503, "not ready: drained\n".into()),
                "{path}"
            );
        }
        assert_eq!(get(health, "/livez").await, (200, "ok\n".into()));
        assert_eq!(agent_line().await, "drain\n");
        assert_eq!(lc.weight(), 0);
        assert!(!lc.taking_work());
        let status = report().await.unwrap().unwrap();
        assert_eq!(
            (status.phase.as_deref(), status.drained),
            (Some("serving"), Some(true))
        );
        assert_eq!(status.load.unwrap().weight, 0);
        assert_eq!(
            ask(true).await.unwrap().unwrap(),
            outcome(true, false, false)
        );

        // Undone: ready again.
        assert_eq!(
            ask(false).await.unwrap().unwrap(),
            outcome(false, true, true)
        );
        assert_eq!(get(health, "/readyz").await, (200, "ok\n".into()));
        assert!(agent_line().await.starts_with("up ready "));
        assert!(lc.weight() > 0 && lc.taking_work());
        assert_eq!(report().await.unwrap().unwrap().drained, Some(false));

        // A signal after the undrain drains, then stops, as usual.
        signal.send(()).unwrap();
        until("no drain", Duration::from_secs(1), || {
            lc.phase() != Phase::Serving
        })
        .await;
        assert_eq!(
            get(health, "/readyz").await,
            (503, "not ready: draining\n".into())
        );
        assert_eq!(agent_line().await, "drain\n");
        // The start ends by itself once the delay has passed, and cleans up.
        let ended = tokio::time::timeout(Duration::from_secs(5), served)
            .await
            .expect("stopped after the drain delay")
            .unwrap();
        assert!(ended.is_ok(), "{ended:?}");
        assert_eq!(lc.phase(), Phase::Stopping);
        assert_eq!(
            get(health, "/readyz").await,
            (503, "not ready: stopping\n".into())
        );
        assert!(!lc.forced(), "one signal is no forced stop");
        assert!(!pid_file.exists(), "the PID file stayed");
        assert!(!socket.exists(), "the status socket stayed");
    }

    #[tokio::test]
    async fn a_second_signal_stops_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let lc = Lifecycle::new();
        let (signal, signals) = mpsc::unbounded_channel();
        stop_on(lc.clone(), signals);
        // A drain delay the test never waits out.
        let app = "endpoint = \"127.0.0.1\"\nport = 0\ndrain_delay_secs = 600\n";
        let config = load::from_str(&config_in(&dir, app, "")).unwrap();
        let served = tokio::spawn(serve_with(config, run_in(&dir, None), lc.clone()));
        until("never ready", Duration::from_secs(5), || lc.ready()).await;

        signal.send(()).unwrap();
        until("no drain", Duration::from_secs(1), || {
            lc.phase() == Phase::Draining
        })
        .await;
        assert!(!lc.forced());
        signal.send(()).unwrap();
        let ended = tokio::time::timeout(Duration::from_secs(2), served)
            .await
            .expect("stopped at once")
            .unwrap();
        assert!(ended.is_ok(), "{ended:?}");
        assert!(lc.forced());
        assert_eq!(lc.phase(), Phase::Stopping);
    }

    #[tokio::test]
    async fn a_stop_hands_back_the_tasks_still_running() {
        use apollo_domain::{Input, Item, ItemState, Task, TaskState, Url};

        let dir = tempfile::tempdir().unwrap();
        // An input whose download never gets an answer, so its run is still
        // going when the server stops.
        let source = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/a.png", source.local_addr().unwrap());
        let app = "endpoint = \"127.0.0.1\"\nport = 0\n";
        let rest = format!("{}[limits]\nblock_private_ips = false\n", model_in(&dir));
        let config = load::from_str(&config_in(&dir, app, &rest)).unwrap();
        // Left in flight by an earlier run, so the start takes it over.
        let storage = apollo_storage::open(&config.database).await.unwrap();
        let input = Input::Image(Url {
            main: url,
            fallback: None,
        });
        let mut item = Item::new(input, vec!["known".into()], None);
        item.state = ItemState::Processing;
        let mut task = Task::new("t", vec![item], None);
        task.state = TaskState::Processing;
        storage.create_task(&task, None, None).await.unwrap();

        let lc = Lifecycle::new();
        let (signal, signals) = mpsc::unbounded_channel();
        stop_on(lc.clone(), signals);
        let served = tokio::spawn(serve_with(config, run_in(&dir, None), lc.clone()));
        let _download = tokio::time::timeout(Duration::from_secs(5), source.accept())
            .await
            .expect("the task never ran")
            .unwrap();

        signal.send(()).unwrap();
        let ended = tokio::time::timeout(Duration::from_secs(10), served)
            .await
            .expect("never stopped")
            .unwrap();
        assert!(ended.is_ok(), "{ended:?}");
        assert!(!lc.forced());
        // Handed back: the next claim counts no resume attempt.
        let next = apollo_storage::Lease {
            token: "next",
            node: "other",
            process: "p",
            ttl: Duration::from_secs(30),
        };
        assert_eq!(
            storage
                .claim_task("t", &next, apollo_storage::ClaimMode::Force, Duration::ZERO)
                .await
                .unwrap(),
            Some(0),
            "the stop left its claim to lapse"
        );
    }

    #[tokio::test]
    async fn a_second_signal_while_starting_skips_the_hand_back() {
        use crate::health::tests::engine_in;

        let dir = tempfile::tempdir().unwrap();
        let lc = Lifecycle::new();
        let (engine, gate) = engine_in(&dir, &lc, false).await;
        let (signal, signals) = mpsc::unbounded_channel();
        stop_on(lc.clone(), signals);
        lc.starting(Step::RecoveringTasks);

        // The first signal ends the start, which hands the tasks back to a
        // store that doesn't answer...
        gate.shut();
        signal.send(()).unwrap();
        until("no drain", Duration::from_secs(1), || {
            lc.phase() == Phase::Draining
        })
        .await;
        let ended = tokio::spawn({
            let lc = lc.clone();
            async move { stopped_starting(&lc, Some(&engine)).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!ended.is_finished(), "the store answered");

        // ...and a second one ends it at once, well within the hand-back
        // limit.
        signal.send(()).unwrap();
        let ended = tokio::time::timeout(Duration::from_secs(1), ended)
            .await
            .expect("ended at once")
            .unwrap();
        assert!(ended.is_ok(), "{ended:?}");
        assert!(lc.forced());
    }

    #[tokio::test]
    async fn signals_that_come_together_are_each_handled() {
        let lc = Lifecycle::new();
        lc.serving();
        let (signal, signals) = mpsc::unbounded_channel();
        // Both arrive before the loop has handled the first.
        signal.send(()).unwrap();
        signal.send(()).unwrap();
        stop_on(lc.clone(), signals);
        tokio::time::timeout(Duration::from_secs(1), lc.until_forced())
            .await
            .expect("the second one stops at once");
        assert_eq!(lc.phase(), Phase::Stopping);
    }

    #[tokio::test]
    async fn a_reload_publishes_metrics_allow_once_it_applies() {
        use crate::health::tests::ask;
        use tokio::net::TcpStream;

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("apollo.toml");
        let model = model_in(&dir);
        let toml = |port: u16, allow: &str| {
            let metrics = format!("[metrics]\nenabled = true\nport = {port}\n{allow}{model}");
            config_in(&dir, "port = 1\n", &metrics)
        };
        // Loopback may scrape. Port 0: one of its own.
        let reloader = reloader_in(&dir, &toml(0, "allow = [\"127.0.0.1\"]\n"), None).await;
        let addr = reloader.metrics.unwrap().local;
        let first = reloader.published.borrow().generation;
        let mut open = TcpStream::connect(addr).await.unwrap();
        assert_eq!(ask(&mut open, "GET", "/metrics").await.0, 200);

        // A list that can't match on loopback fails the reload, and the
        // running one stays.
        std::fs::write(&file, toml(addr.port(), "allow = [\"10.0.0.0/8\"]\n")).unwrap();
        let err = reloader.reload().await.unwrap_err().to_string();
        assert!(
            err.starts_with("metrics.allow: no entry can match on 127.0.0.1:"),
            "{err}"
        );
        assert_eq!(reloader.published.borrow().generation, first);
        assert_eq!(reloader.engine.config_generation().0, first);
        assert_eq!(ask(&mut open, "GET", "/metrics").await.0, 200);

        // One that can match there but leaves this peer out applies, with
        // the reload's generation: the open connection's next request is
        // refused.
        std::fs::write(&file, toml(addr.port(), "allow = [\"127.0.0.2\"]\n")).unwrap();
        let outcome = reloader.reload().await.unwrap();
        assert!(outcome.generation > first);
        assert_eq!(reloader.published.borrow().generation, outcome.generation);
        let (status, head, _) = ask(&mut open, "GET", "/metrics").await;
        assert_eq!(status, 403);
        assert!(head.contains("\r\nconnection: close\r\n"), "{head}");
    }

    #[tokio::test]
    async fn a_reload_warns_of_a_health_allow_the_next_start_refuses() {
        use crate::health::tests::health_config;

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("apollo.toml");
        let model = model_in(&dir);
        let lc = Lifecycle::new();
        let health = crate::health::start(&health_config(""), &lc)
            .await
            .unwrap()
            .unwrap();
        let port = health.local.port();
        let toml = |endpoint: &str, allow: &str| {
            let health = format!(
                "[health]\nenabled = true\nendpoint = \"{endpoint}\"\nport = {port}\n{allow}{model}"
            );
            config_in(&dir, "port = 1\n", &health)
        };
        let reloader = reloader_in(&dir, &toml("127.0.0.1", ""), Some(health)).await;

        // On the same address, a list that can't match there: the reload
        // goes through, and says the next start will fail.
        std::fs::write(&file, toml("127.0.0.1", "allow = [\"10.20.0.0/16\"]\n")).unwrap();
        let outcome = reloader.reload().await.unwrap();
        assert_eq!(outcome.restart_required, ["health.allow"]);
        let fails = format!(
            "the next start will fail: health.allow: no entry can match on 127.0.0.1:{port}: "
        );
        assert!(
            outcome.warnings.len() == 1 && outcome.warnings[0].starts_with(&fails),
            "{:?}",
            outcome.warnings
        );
        // One that can match: nothing to say.
        std::fs::write(&file, toml("127.0.0.1", "allow = [\"127.0.0.1\"]\n")).unwrap();
        assert_eq!(
            reloader.reload().await.unwrap().warnings,
            Vec::<String>::new()
        );
        // On another address, the next start checks it there.
        std::fs::write(&file, toml("0.0.0.0", "allow = [\"10.20.0.0/16\"]\n")).unwrap();
        let outcome = reloader.reload().await.unwrap();
        assert_eq!(
            outcome.restart_required,
            ["health.endpoint", "health.allow"]
        );
        assert_eq!(outcome.warnings, Vec::<String>::new());
    }

    #[tokio::test]
    async fn the_listener_answers_while_storage_opens() {
        let dir = tempfile::tempdir().unwrap();
        // A database host that takes connections and never answers.
        let db = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let db_addr = db.local_addr().unwrap();
        let (reached, reached_db) = tokio::sync::oneshot::channel();
        let held = tokio::spawn(async move {
            let mut reached = Some(reached);
            let mut held = Vec::new();
            while let Ok((stream, _)) = db.accept().await {
                held.push(stream);
                if let Some(reached) = reached.take() {
                    let _ = reached.send(());
                }
            }
        });
        let lc = Lifecycle::new();
        // First, as `serve` starts it.
        let addr = start_health("", &lc).await;
        let toml = format!(
            "[app]\nnode_id = \"n\"\ncache_dir = \"{}\"\n[auth]\nenabled = false\n\
             [database]\nbackend = \"surrealdb\"\n\
             [database.surrealdb]\nurl = \"ws://{db_addr}\"\nnamespace = \"n\"\ndatabase = \"d\"\n",
            dir.path().display()
        );
        let config = load::from_str(&toml).unwrap();
        let started = tokio::spawn(serve_with(config, run_in(&dir, None), lc.clone()));

        tokio::time::timeout(Duration::from_secs(5), reached_db)
            .await
            .expect("storage never connected")
            .unwrap();
        let at = std::time::Instant::now();
        assert_eq!(get(addr, "/livez").await, (200, "ok\n".into()));
        let (status, body) = get(addr, "/readyz").await;
        assert!(at.elapsed() < PROMPT, "{:?}", at.elapsed());
        assert_eq!(status, 503);
        assert!(
            body.starts_with("not ready: starting (opening database, "),
            "{body}"
        );
        assert!(!started.is_finished(), "storage opened");

        // A stop while it opens ends the start at once, cleanly; the probes
        // still answer.
        assert!(lc.drain());
        let ended = tokio::time::timeout(Duration::from_secs(1), started)
            .await
            .expect("the start ended")
            .unwrap();
        assert!(ended.is_ok(), "{ended:?}");
        assert_eq!(
            get(addr, "/readyz").await,
            (503, "not ready: stopping\n".into())
        );
        held.abort();
    }

    #[tokio::test]
    async fn readiness_waits_for_a_pinned_model_to_load() {
        use apollo_engine::testing::{HeldLoad, engine_with_load_hook};

        let dir = tempfile::tempdir().unwrap();
        let lc = Lifecycle::new();
        let addr = start_health("", &lc).await;

        // A server as `serve_with` starts it, with a model kept in memory
        // whose weights take as long as the test says.
        let list = dir.path().join("known.txt");
        std::fs::write(&list, format!("{} 1\n", "ab".repeat(32))).unwrap();
        let toml = format!(
            "[app]\ncache_dir = \"{}\"\n[database.sqlite]\npath = \"{}\"\n\
             [models.known]\narchitecture = \"pdq\"\nhash_list = \"{}\"\n\
             categories = {{ 1 = \"known\" }}\nkeep_in_memory = true\n",
            dir.path().display(),
            dir.path().join("apollo.db").display(),
            list.display()
        );
        let config = load::from_str(&toml).unwrap();
        config.validate().unwrap();
        let storage = apollo_storage::open(&config.database).await.unwrap();
        let held = HeldLoad::new("known");
        let engine = engine_with_load_hook(
            Arc::new(config),
            storage,
            None,
            "n".into(),
            lc.clone(),
            held.hook(),
        );
        assert_in(&engine, &dir);
        lc.starting(Step::LoadingModels);
        ready_after_preloads(engine.clone());

        // Not ready while it loads...
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while held.held() == 0 {
            assert!(std::time::Instant::now() < deadline, "never loaded");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!lc.ready());
        let (status, body) = get(addr, "/readyz").await;
        assert_eq!(status, 503);
        assert!(
            body.starts_with("not ready: starting (loading models, "),
            "{body}"
        );

        // ...and ready once it has.
        held.release();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !lc.ready() {
            assert!(std::time::Instant::now() < deadline, "never ready");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(get(addr, "/readyz").await, (200, "ok\n".into()));
        lc.stopping();
    }
}
