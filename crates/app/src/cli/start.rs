//! `start`: load config, apply run-only overrides, optionally daemonize, serve.

use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use clap::Args;

use apollo_config::{Config, Overrides, load};
use apollo_engine::{Engine, WebhookSink};

#[derive(Args)]
pub struct StartArgs {
    /// Config file path (default: /etc/apollo/config.toml).
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Override the bind host/IP for this run only.
    #[arg(long)]
    pub endpoint: Option<String>,
    /// Override the port for this run only.
    #[arg(long)]
    pub port: Option<u16>,
    /// Override the webhook receiver URL (e.g. http://127.0.0.1:9090).
    #[arg(long = "webhook-url")]
    pub webhook_url: Option<String>,
    /// Detach and run in the background, writing a PID file.
    #[arg(long)]
    pub daemon: bool,
}

pub fn run(args: StartArgs) -> anyhow::Result<()> {
    let mut config = load::load(args.config.as_deref()).context("loading config")?;
    config.apply_overrides(&Overrides {
        endpoint: args.endpoint.clone(),
        port: args.port,
        webhook_url: args.webhook_url.clone(),
    });
    config.validate().context("config is invalid")?;
    if !config.has_models() {
        return Err(anyhow!(
            "no models configured — nothing to serve; add a [models.<label>] section"
        ));
    }

    if args.daemon {
        return spawn_detached(&args);
    }

    // Foreground: own the runtime so daemonization (above) happens before any
    // runtime threads exist.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building the Tokio runtime")?;
    runtime.block_on(serve(config))
}

async fn serve(config: Config) -> anyhow::Result<()> {
    init_tracing(&config.app.log_level);

    let addr = (config.app.endpoint.as_str(), config.app.port)
        .to_socket_addrs()
        .with_context(|| format!("resolving {}:{}", config.app.endpoint, config.app.port))?
        .next()
        .ok_or_else(|| anyhow!("no address for {}:{}", config.app.endpoint, config.app.port))?;

    // open() runs migrations internally.
    let storage = apollo_storage::open(&config.database)
        .await
        .context("opening storage")?;

    let webhook = match &config.webhook {
        Some(w) => {
            let sink = apollo_server::GrpcWebhookSink::new(&w.url, w.secret.clone())
                .context("configuring webhook client")?;
            Some(Arc::new(sink) as Arc<dyn WebhookSink>)
        }
        None => None,
    };

    // Engine::new spawns background tasks, so it must run inside the runtime.
    let config = Arc::new(config);
    let engine = Engine::new(config.clone(), storage, webhook);

    let resumed = engine.recover().await.context("recovering tasks")?;
    if resumed > 0 {
        tracing::info!(resumed, "re-queued in-flight tasks from a previous run");
    }

    // Owns the PID file for this process; removed on graceful shutdown.
    let _pidfile = crate::daemon::PidFile::create()?;

    tracing::info!(%addr, models = config.model_count(), "apollo listening");
    apollo_server::serve_with_shutdown(engine, addr, crate::daemon::shutdown_signal())
        .await
        .context("gRPC server error")?;

    tracing::info!("apollo shut down cleanly");
    Ok(())
}

fn init_tracing(level: &str) {
    use tracing_subscriber::{EnvFilter, fmt};
    // RUST_LOG wins; otherwise fall back to the configured level.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    // try_init is a no-op if a subscriber is already set (e.g. in tests).
    let _ = fmt().with_env_filter(filter).try_init();
}

/// Re-exec this binary as a detached background `start` (without `--daemon`).
/// The child process writes and owns the PID file once it is serving.
fn spawn_detached(args: &StartArgs) -> anyhow::Result<()> {
    let exe = std::env::current_exe().context("locating current executable")?;
    let log_path = std::env::temp_dir().join("apollo.log");
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("opening log file {}", log_path.display()))?;
    let log_err = log.try_clone().context("cloning log handle")?;

    let mut cmd = Command::new(exe);
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
