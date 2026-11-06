//! The HAProxy agent (`[health].agent_port`), for `agent-check`: on each
//! connection it sends one line and closes.
//!
//! - Ready: `up ready NN%`. `up` undoes a down state (if HAProxy's own health
//!   checks pass too), `ready` ends a drain, and `NN%` is the
//!   [weight](Lifecycle::weight), as a share of the server's configured
//!   weight.
//! - Not ready (starting, draining): `drain`, so HAProxy sends no new
//!   connections.
//! - Stopping: the port is closed. HAProxy keeps the state an agent it can't
//!   reach reported last.
//!
//! It never sends `down` or `maint`: HAProxy's own health checks decide that.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use apollo_config::Allowlist;
use apollo_engine::{Lifecycle, Phase};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use crate::allow::{self, Bound, Guard};

/// How long a connection stays open after its line, for the checker to read
/// it and close.
const LINGER: Duration = Duration::from_secs(2);
/// Connections open at once. More wait, unaccepted, until one closes.
const MAX_CONNECTIONS: usize = 64;

/// Serve the agent on `port` (0: not at all) of the health listener's
/// address `health`, to the peers `list` admits, until `lc` stops. Call it
/// inside the runtime. Returns the address it serves on.
pub(crate) async fn start(
    health: SocketAddr,
    port: u16,
    list: Option<Arc<Allowlist>>,
    lc: &Lifecycle,
) -> anyhow::Result<Option<SocketAddr>> {
    if port == 0 {
        return Ok(None);
    }
    let addr = SocketAddr::new(health.ip(), port);
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding the HAProxy agent on {addr}"))?;
    let bound = Bound::of(&listener).context("reading the agent's socket")?;
    if let Some(list) = &list {
        // Its warnings were logged for the health port, on the same address.
        bound.check("health.allow", list)?;
    }
    tracing::info!(%addr, "serving the HAProxy agent (agent-check)");
    tracing::info!("{}", bound.describe("agent", list.as_deref()));
    let counter = lc.clone();
    let guard = Guard::new(
        "agent",
        "[health].allow",
        bound.local,
        Arc::new(move || list.clone()),
        Arc::new(move |listener| counter.note_refused(listener)),
    );
    tokio::spawn(serve(listener, guard, lc.clone()));
    Ok(Some(bound.local))
}

/// Answer the agent's checks on `listener`, from the peers `guard` admits,
/// until `lc` is stopping; then close it.
async fn serve(mut listener: TcpListener, guard: Guard, lc: Lifecycle) {
    let open = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let stopping = lc.reached(Phase::Stopping);
    tokio::pin!(stopping);
    loop {
        let next = async {
            // At the cap, accept nothing until a connection closes.
            let slot = Arc::clone(&open).acquire_owned().await;
            // Admitted by `[health].allow`, which holds until a restart.
            (slot, allow::accept_allowed(&mut listener, &guard).await.0)
        };
        let (slot, stream) = tokio::select! {
            biased;
            () = &mut stopping => return,
            next = next => next,
        };
        let Ok(slot) = slot else {
            return;
        };
        let line = line(&lc);
        tokio::spawn(async move {
            answer(stream, &line).await;
            drop(slot);
        });
    }
}

/// The agent's line for `lc` as it stands now: `up ready NN%` while ready,
/// with NN its weight (1 to 100), otherwise `drain`.
fn line(lc: &Lifecycle) -> String {
    let ready = lc.ready();
    if ready {
        format!("up ready {}%\n", lc.load().weight(ready))
    } else {
        "drain\n".into()
    }
}

/// Send `line`, then wait for the checker to close, reading whatever it
/// sends (`agent-send`): closing with unread data would reset the
/// connection, which can cost the checker the line.
async fn answer(mut stream: TcpStream, line: &str) {
    let _ = tokio::time::timeout(LINGER, async {
        stream.write_all(line.as_bytes()).await?;
        stream.shutdown().await?;
        let mut sink = [0u8; 256];
        while stream.read(&mut sink).await? > 0 {}
        std::io::Result::Ok(())
    })
    .await;
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::time::Instant;

    use apollo_engine::testing::publish_load;
    use apollo_engine::{Database, LoadSnapshot, Step};

    use super::*;
    use crate::health::tests::{closed, engine_in, free_port, health_config};

    /// Serve the agent for `lc` on a loopback port, to everyone.
    async fn spawn(lc: &Lifecycle) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let guard = Guard::new(
            "agent",
            "[health].allow",
            addr,
            Arc::new(|| None),
            Arc::new(|_| {}),
        );
        tokio::spawn(serve(listener, guard, lc.clone()));
        addr
    }

    /// What the agent at `addr` says, after `send` (as `agent-send` would),
    /// up to its close (an orderly one: a reset fails).
    async fn ask(addr: SocketAddr, send: &[u8]) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(send).await.unwrap();
        let mut answer = String::new();
        tokio::time::timeout(Duration::from_secs(1), stream.read_to_string(&mut answer))
            .await
            .expect("closed after the line")
            .unwrap();
        answer
    }

    #[test]
    fn the_line_follows_the_lifecycle() {
        let lc = Lifecycle::new();
        for step in [
            Step::OpeningDatabase,
            Step::ConfiguringAuth,
            Step::RecoveringTasks,
            Step::LoadingModels,
        ] {
            lc.starting(step);
            assert_eq!(line(&lc), "drain\n", "{step:?}");
        }
        lc.serving();
        // No sample yet: idle.
        assert_eq!(line(&lc), "up ready 100%\n");
        let busy = LoadSnapshot {
            utilization: 0.42,
            ..LoadSnapshot::default()
        };
        publish_load(&lc, busy.clone());
        assert_eq!(line(&lc), "up ready 60%\n");
        // Full, it keeps the lowest weight; it is never 0% while ready.
        publish_load(
            &lc,
            LoadSnapshot {
                utilization: 1.0,
                ..LoadSnapshot::default()
            },
        );
        assert_eq!(line(&lc), "up ready 1%\n");
        // Out of touch with a shared database: weight 1.
        publish_load(
            &lc,
            LoadSnapshot {
                database: Database::Stale {
                    last_contact: Instant::now(),
                },
                ..busy
            },
        );
        assert_eq!(line(&lc), "up ready 1%\n");
        assert!(lc.drain());
        assert_eq!(line(&lc), "drain\n");
        lc.stopping();
        assert_eq!(line(&lc), "drain\n");
    }

    #[tokio::test]
    async fn each_connection_gets_one_line_then_a_close() {
        let lc = Lifecycle::new();
        let addr = spawn(&lc).await;
        assert_eq!(ask(addr, b"").await, "drain\n");
        lc.serving();
        assert_eq!(ask(addr, b"").await, "up ready 100%\n");
        publish_load(
            &lc,
            LoadSnapshot {
                utilization: 0.42,
                ..LoadSnapshot::default()
            },
        );
        // What the checker sends is read and ignored.
        assert_eq!(ask(addr, b"hello\n").await, "up ready 60%\n");
        assert!(lc.drain());
        assert_eq!(ask(addr, b"").await, "drain\n");
    }

    #[tokio::test]
    async fn the_port_closes_once_stopping() {
        let lc = Lifecycle::new();
        lc.serving();
        let addr = spawn(&lc).await;
        assert!(lc.drain());
        assert_eq!(ask(addr, b"").await, "drain\n");
        lc.stopping();
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            match TcpStream::connect(addr).await {
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => break,
                // Still open, or queued just as it closed (then reset).
                other => assert!(Instant::now() < deadline, "still open: {other:?}"),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn a_peer_not_in_health_allow_is_reset_at_the_agent_port() {
        let dir = tempfile::tempdir().unwrap();
        let lc = Lifecycle::new();
        lc.serving();
        let agent = free_port();
        let (config, _) =
            health_config(&format!("allow = [\"127.0.0.2\"]\nagent_port = {agent}\n"));
        crate::health::start(&config, &lc).await.unwrap();
        let stream = TcpStream::connect(("127.0.0.1", agent)).await.unwrap();
        // Reset at accept: no line.
        assert!(closed(stream).await);

        let (engine, _) = engine_in(&dir, &lc, false).await;
        let text = engine.metrics_text().await;
        let refused: Vec<&str> = text
            .lines()
            .filter(|l| l.starts_with("apollo_listener_refused_total"))
            .collect();
        assert_eq!(
            refused,
            ["apollo_listener_refused_total{listener=\"agent\"} 1"]
        );
    }

    #[tokio::test]
    async fn a_peer_in_health_allow_gets_the_line() {
        let lc = Lifecycle::new();
        let agent = free_port();
        let (config, _) =
            health_config(&format!("allow = [\"127.0.0.1\"]\nagent_port = {agent}\n"));
        crate::health::start(&config, &lc).await.unwrap();
        let addr = SocketAddr::from(([127, 0, 0, 1], agent));
        assert_eq!(ask(addr, b"").await, "drain\n");
    }

    #[tokio::test]
    async fn agent_port_0_starts_nothing() {
        let lc = Lifecycle::new();
        let health = SocketAddr::from(([127, 0, 0, 1], free_port()));
        assert_eq!(start(health, 0, None, &lc).await.unwrap(), None);

        // Nor does an agent_port without [health].
        let agent = free_port();
        let (mut config, _) = health_config(&format!("agent_port = {agent}\n"));
        config.health.enabled = false;
        crate::health::start(&config, &lc).await.unwrap();
        assert!(TcpStream::connect(("127.0.0.1", agent)).await.is_err());
    }
}
