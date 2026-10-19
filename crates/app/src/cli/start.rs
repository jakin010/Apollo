//! `start`: load config, apply run-only overrides, optionally daemonize, serve.

use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use clap::Args;

use apollo_config::{AuthConfig, Config, Overrides, TlsConfig, load};
use apollo_engine::{Engine, WebhookSink};

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
    /// delivery for this run even without a [webhook] section.
    #[arg(long = "webhook-url")]
    pub webhook_url: Option<String>,
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

    let addr = (config.app.endpoint.as_str(), config.app.port)
        .to_socket_addrs()
        .with_context(|| format!("resolving {}:{}", config.app.endpoint, config.app.port))?
        .next()
        .ok_or_else(|| anyhow!("no address for {}:{}", config.app.endpoint, config.app.port))?;

    // open() runs migrations internally.
    let storage = apollo_storage::open(&config.database)
        .await
        .context("opening storage")?;

    let webhook = webhook_sink(&config)?;

    // Engine::new spawns background tasks, so it must run inside the runtime.
    let config = Arc::new(config);
    let engine = Engine::new(config.clone(), storage, webhook);
    let reloader = Reloader {
        engine: engine.clone(),
        source,
        log_filter,
    };
    reload_on_hangup(reloader.clone());
    let _metrics = start_metrics(&config.metrics, engine.clone()).await?;

    let resumed = engine.recover().await.context("recovering tasks")?;
    if resumed > 0 {
        tracing::info!(resumed, "re-queued in-flight tasks from a previous run");
    }

    // Owns the PID file for this process; removed on graceful shutdown.
    let _pidfile = crate::daemon::PidFile::create()?;

    let auth = configure_auth(&config.auth)?;
    auth.watch_revocations(REVOCATION_POLL);
    let tls = load_tls(config.tls.as_ref())?;
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
        database: describe_database(&config.database),
        log_file: std::env::var(DAEMON_LOG_ENV).ok(),
    };
    let _status_socket = start_status_socket(engine.clone(), info, reloader);

    tracing::info!(%addr, models = config.model_count(), "apollo listening");
    apollo_server::serve_with_shutdown(
        engine.clone(),
        addr,
        crate::daemon::shutdown_signal(),
        auth,
        tls,
    )
    .await
    .context("gRPC server error")?;

    // A clean stop must not count toward the poison-task cap for whatever was
    // still in flight; those tasks resume on the next start.
    engine.shutdown().await;

    tracing::info!("apollo shut down cleanly");
    Ok(())
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

/// The `[webhook]` sink for `config`, if it has one.
fn webhook_sink(config: &Config) -> anyhow::Result<Option<Arc<dyn WebhookSink>>> {
    match &config.webhook {
        Some(w) => {
            let sink = apollo_server::GrpcWebhookSink::new(&w.url, w.secret.clone())
                .context("configuring webhook client")?;
            Ok(Some(Arc::new(sink) as Arc<dyn WebhookSink>))
        }
        None => Ok(None),
    }
}

/// Re-reads the config and applies it to the running server (`apollo reload`,
/// SIGHUP).
#[derive(Clone)]
struct Reloader {
    engine: Engine,
    source: ConfigSource,
    log_filter: Option<LogFilter>,
}

impl Reloader {
    /// Load, validate and apply the config. On any error the running config
    /// stays as it was.
    async fn reload(&self) -> anyhow::Result<crate::status::ReloadOutcome> {
        let config = self.source.load()?;
        let webhook = webhook_sink(&config)?;
        let level = config.app.log_level.clone();
        let report = self.engine.reload(config, webhook).await?;
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

/// Serve `GET /metrics` when `[metrics]` is enabled. The listener runs until
/// the process exits.
async fn start_metrics(
    config: &apollo_config::MetricsConfig,
    engine: Engine,
) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
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
    if !addr.ip().is_loopback() {
        tracing::warn!(
            %addr,
            "serving unauthenticated metrics on a non-loopback address"
        );
    }
    let app = axum::Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .with_state(engine);
    tracing::info!(%addr, "serving Prometheus metrics on /metrics");
    Ok(Some(tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "metrics listener stopped");
        }
    })))
}

async fn metrics_handler(
    axum::extract::State(engine): axum::extract::State<Engine>,
) -> axum::response::Response {
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;
    match engine.metrics_text().await {
        Ok(text) => (
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            text,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
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
        async move { build_report(&engine, &info).await }
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
async fn build_report(
    engine: &Engine,
    info: &ServerInfo,
) -> anyhow::Result<crate::status::StatusReport> {
    use crate::status::{self as st, unix_secs};
    let s = engine.status().await?;
    let config = engine.config();
    let (generation, loaded_at) = engine.config_generation();
    Ok(st::StatusReport {
        version: env!("CARGO_PKG_VERSION").to_string(),
        pid: std::process::id(),
        started_at: unix_secs(info.started_at),
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
        tasks: st::Tasks {
            queued: s.tasks.queued,
            processing: s.tasks.processing,
            completed: s.tasks.completed,
            failed: s.tasks.failed,
            cancelled: s.tasks.cancelled,
        },
        webhook: match (&config.webhook, s.webhooks_pending) {
            (Some(w), Some(pending)) => Some(st::Webhook {
                url: w.url.clone(),
                pending,
            }),
            _ => None,
        },
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
    })
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
fn configure_auth(auth: &AuthConfig) -> anyhow::Result<apollo_server::AuthInterceptor> {
    if !auth.enabled {
        tracing::warn!(
            "authentication is DISABLED ([auth] enabled = false): anyone who can reach this \
             port can submit work and read results"
        );
        return Ok(apollo_server::AuthInterceptor::disabled());
    }
    let public_key = match &auth.public_key {
        Some(key) => {
            tracing::info!("PASETO authentication enabled (configured public key)");
            key.clone()
        }
        None => {
            let (key, path, created) = super::keys::load_or_create_local_keypair()
                .context("preparing the local signing keypair")?;
            if created {
                tracing::info!(path = %path.display(), "generated a new signing keypair");
            }
            tracing::info!(
                path = %path.display(),
                "PASETO authentication enabled (local keypair); mint client tokens with \
                 `apollo token --subject NAME`"
            );
            key
        }
    };
    let revocation_file = super::keys::revocation_file(auth)?;
    if !auth.admin_subjects.is_empty() {
        tracing::info!(admins = ?auth.admin_subjects, "admin subjects may access every task");
    }
    let interceptor = apollo_server::AuthInterceptor::new(apollo_server::AuthSettings {
        public_key,
        admin_subjects: auth.admin_subjects.clone(),
        revocation_file: Some(revocation_file.clone()),
    })
    .context("configuring authentication")?;
    tracing::info!(file = %revocation_file.display(), "token revocation list (watched for changes)");
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
