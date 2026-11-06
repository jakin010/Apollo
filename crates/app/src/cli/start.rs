//! `start`: load config, apply run-only overrides, optionally daemonize, serve.

use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::Args;
use tokio::sync::watch;

use apollo_config::{Allowlist, Config, Overrides, TlsConfig, load};
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
        },
    };
    let config = source.load()?;

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
    serve_with(config, config_file, source, log_filter, lc).await
}

/// Start, following `lc`, and serve until it stops. A stop during startup
/// ends the start, handing back what recovery took, and returns `Ok`.
async fn serve_with(
    config: Config,
    config_file: Option<PathBuf>,
    source: ConfigSource,
    log_filter: Option<LogFilter>,
    lc: Lifecycle,
) -> anyhow::Result<()> {
    // First, so probes answer while the steps below run.
    crate::health::start(&config, &lc).await?;
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
        source,
        log_filter,
        published,
        metrics,
    };
    reload_on_hangup(reloader.clone());

    // Owns the PID file for this process; removed on graceful shutdown.
    let _pidfile = crate::daemon::PidFile::create()?;

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
        config_file: config_file.map(|p| p.display().to_string()),
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
    let _status_socket = start_status_socket(engine.clone(), info, reloader);

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
    tokio::spawn(async move {
        loop {
            crate::daemon::shutdown_signal().await;
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
        }
    });
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
        let m = report.models;
        Ok(crate::status::ReloadOutcome {
            generation: report.generation,
            added: m.added,
            reloaded: m.reloaded,
            kept: m.kept,
            removed: m.removed,
            restart_required: report.restart_required,
        })
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
    let list = config
        .metrics
        .allowlist()
        .map_err(|errs| anyhow!("metrics.allow {}", errs.join("; ")))?;
    Ok(list.map(Arc::new))
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

/// Serve `apollo status` on the local status socket. Failure is logged, not
/// fatal: the server works without it.
#[cfg(unix)]
fn start_status_socket(
    engine: Engine,
    info: ServerInfo,
    reloader: Reloader,
) -> Option<crate::status::SocketGuard> {
    let path = match crate::status::socket_path() {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "status socket disabled");
            return None;
        }
    };
    let report = move || {
        let engine = engine.clone();
        let info = info.clone();
        async move { Ok(build_report(&engine, &info).await) }
    };
    let reload = move || {
        let reloader = reloader.clone();
        async move { reloader.reload().await }
    };
    match crate::status::serve_socket(&path, report, reload) {
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

#[cfg(not(unix))]
fn start_status_socket(_engine: Engine, _info: ServerInfo, _reloader: Reloader) -> Option<()> {
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
        let port = crate::health::tests::free_port();
        let toml = format!(
            "[database]\ntimeout = 1\n[database.sqlite]\npath = \"{}\"\n\
             [metrics]\nenabled = true\nport = {port}\n\
             [webhook]\nurl = \"http://127.0.0.1:1\"\n",
            dir.path().join("apollo.db").display()
        );
        let config = load::from_str(&toml).unwrap();
        let storage = apollo_storage::open(&config.database).await.unwrap();
        let (stalled, gate) = apollo_engine::testing::Stalled::new(storage);
        let sink = webhook_sink(&config).unwrap();
        let engine = Engine::new(Arc::new(config.clone()), Arc::new(stalled), sink);
        let (_tx, rx) = watch::channel(Published {
            generation: 1,
            metrics: None,
        });
        start_metrics(&config.metrics, engine.clone(), rx)
            .await
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
            let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .unwrap();
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

    #[tokio::test]
    async fn the_listener_answers_while_storage_opens() {
        use crate::health::tests::{free_port, get};
        // A database host that takes connections and never answers.
        let db = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let db_addr = db.local_addr().unwrap();
        let held = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = db.accept().await {
                held.push(stream);
            }
        });
        let port = free_port();
        let toml = format!(
            "[health]\nenabled = true\nendpoint = \"127.0.0.1\"\nport = {port}\n\
             [database]\nbackend = \"surrealdb\"\n\
             [database.surrealdb]\nurl = \"ws://{db_addr}\"\nnamespace = \"n\"\ndatabase = \"d\"\n"
        );
        let config = load::from_str(&toml).unwrap();
        let source = ConfigSource {
            path: None,
            overrides: Overrides::default(),
        };
        let lc = Lifecycle::new();
        let started = tokio::spawn(serve_with(config, None, source, None, lc.clone()));

        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while tokio::net::TcpStream::connect(addr).await.is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "the health port never opened"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let at = std::time::Instant::now();
        assert_eq!(get(addr, "/livez").await, (200, "ok\n".into()));
        let (status, body) = get(addr, "/readyz").await;
        assert!(
            at.elapsed() < Duration::from_millis(100),
            "{:?}",
            at.elapsed()
        );
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
}
