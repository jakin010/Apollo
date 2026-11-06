//! The health checks load balancers and orchestrators probe (`[health]`):
//! plaintext HTTP/1.1 on a port of its own, bound before storage opens and
//! open until the process exits. Answers come from the process's
//! [`Lifecycle`] only, so a probe never waits on the database or a model.
//!
//! | Path | 200 | 503 |
//! |---|---|---|
//! | `/livez` | always | never |
//! | `/readyz`, `/` | ready for work | starting, draining or stopping, with why |

use std::convert::Infallible;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use axum::body::Body;
use axum::http::{HeaderValue, Method, Response, StatusCode, header};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use apollo_config::Config;
use apollo_engine::{Lifecycle, Phase};

use crate::allow::{self, Bound, Guard};

/// How long a connection may take to send a request's headers, and may
/// stay idle between requests.
pub(crate) const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Connections open at once. More wait, unaccepted, until one closes.
pub(crate) const MAX_CONNECTIONS: usize = 256;

/// The methods every path answers.
const ALLOW: &str = "GET, HEAD, OPTIONS";

/// What one connection may hold, and how many may be open.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub(crate) header_read: Duration,
    pub(crate) connections: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            header_read: HEADER_READ_TIMEOUT,
            connections: MAX_CONNECTIONS,
        }
    }
}

/// Serve the health checks when `[health]` is enabled, following `lc`, to
/// the peers `[health].allow` admits. Call it inside the runtime.
pub(crate) async fn start(config: &Config, lc: &Lifecycle) -> anyhow::Result<()> {
    let health = &config.health;
    if !health.enabled {
        return Ok(());
    }
    let host = health.bind_host(&config.app);
    let addr = (host, health.port)
        .to_socket_addrs()
        .with_context(|| format!("resolving [health] {host}:{}", health.port))?
        .next()
        .ok_or_else(|| anyhow!("no address for [health] {host}:{}", health.port))?;
    let list = health
        .allowlist()
        .map_err(|errs| anyhow!("health.allow {}", errs.join("; ")))?
        .map(Arc::new);
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding the health listener on {addr}"))?;
    let bound = Bound::of(&listener).context("reading the health listener's socket")?;
    if let Some(list) = &list {
        for warning in bound.check("health.allow", list)? {
            tracing::warn!("{warning}");
        }
    }
    tracing::info!(%addr, "serving health checks on /livez and /readyz");
    tracing::info!("{}", bound.describe("health", list.as_deref()));
    // Counted in the lifecycle, which exists before the engine does.
    let counter = lc.clone();
    let guard = Guard::new(
        "health",
        "[health].allow",
        bound.local,
        Arc::new(move || list.clone()),
        Arc::new(move |listener| counter.note_refused(listener)),
    );
    tokio::spawn(serve(listener, guard, lc.clone(), Limits::default()));
    Ok(())
}

/// Answer probes on `listener`, from the peers `guard` admits, for as long
/// as the runtime runs.
pub(crate) async fn serve(mut listener: TcpListener, guard: Guard, lc: Lifecycle, limits: Limits) {
    let open = Arc::new(Semaphore::new(limits.connections));
    loop {
        // At the cap, accept nothing until a connection closes.
        let Ok(slot) = Arc::clone(&open).acquire_owned().await else {
            return;
        };
        let (stream, _) = allow::accept_allowed(&mut listener, &guard).await;
        let _ = stream.set_nodelay(true);
        let lc = lc.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |request| {
                let response = respond(&lc, request.method(), request.uri().path());
                async move { Ok::<_, Infallible>(response) }
            });
            // A connection that fails or times out just ends.
            let _ = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(limits.header_read)
                .serve_connection(TokioIo::new(stream), service)
                .await;
            drop(slot);
        });
    }
}

/// The answer to `method` on `path`, as `lc` stands now. The `Host` header
/// and the query are ignored.
fn respond(lc: &Lifecycle, method: &Method, path: &str) -> Response<Body> {
    let readiness = match path {
        "/livez" => false,
        "/readyz" | "/" => true,
        _ => return reply(StatusCode::NOT_FOUND, "not found\n".into()),
    };
    let allowed = matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS);
    let mut response = if !allowed {
        reply(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed\n".into(),
        )
    } else if let Some(reason) = readiness
        .then(|| not_ready(lc.phase(), lc.since().elapsed()))
        .flatten()
    {
        reply(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not ready: {reason}\n"),
        )
    } else {
        reply(StatusCode::OK, "ok\n".into())
    };
    if !allowed || *method == Method::OPTIONS {
        let allow = HeaderValue::from_static(ALLOW);
        response.headers_mut().insert(header::ALLOW, allow);
    }
    response
}

/// Why a lifecycle in `phase` (for `elapsed`) isn't ready for work, as
/// `/readyz` says it: `starting (recovering tasks, 41s)`, `draining`,
/// `stopping`. `None` when it is ready.
fn not_ready(phase: Phase, elapsed: Duration) -> Option<String> {
    match phase {
        Phase::Serving => None,
        Phase::Starting(step) => Some(format!(
            "starting ({}, {}s)",
            step.name(),
            elapsed.as_secs()
        )),
        Phase::Draining | Phase::Stopping => Some(phase.name().to_string()),
    }
}

/// A plain-text answer that no cache keeps.
fn reply(status: StatusCode, body: String) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io;
    use std::net::SocketAddr;
    use std::time::Instant;

    use apollo_engine::{Engine, Step};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use super::*;

    /// Serve the probes for `lc` on a loopback port, to everyone.
    async fn spawn(lc: &Lifecycle, limits: Limits) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let guard = Guard::new(
            "health",
            "[health].allow",
            addr,
            Arc::new(|| None),
            Arc::new(|_| {}),
        );
        tokio::spawn(serve(listener, guard, lc.clone(), limits));
        addr
    }

    /// A loopback port free a moment ago.
    pub(crate) fn free_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    /// One answer read from `stream`: the status, the head (lowercase) and,
    /// unless it answers a HEAD, the body.
    async fn read_answer(stream: &mut TcpStream, head_only: bool) -> (u16, String, String) {
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            assert_eq!(stream.read(&mut byte).await.unwrap(), 1, "closed early");
            head.push(byte[0]);
        }
        let head = String::from_utf8(head).unwrap().to_ascii_lowercase();
        let status = head[9..12].parse().unwrap();
        let length = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length: "))
            .map_or(0, |n| n.trim().parse().unwrap());
        let mut body = vec![0; if head_only { 0 } else { length }];
        stream.read_exact(&mut body).await.unwrap();
        (status, head, String::from_utf8(body).unwrap())
    }

    /// `method path` over HTTP/1.1 on `stream`, kept open.
    async fn ask(stream: &mut TcpStream, method: &str, path: &str) -> (u16, String, String) {
        let request = format!("{method} {path} HTTP/1.1\r\nhost: test\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        read_answer(stream, method == "HEAD").await
    }

    /// GET `path` on a new connection: the status and the body.
    pub(crate) async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let (status, head, body) = ask(&mut stream, "GET", path).await;
        assert_plain(&head);
        (status, body)
    }

    /// Every answer: plain text that no cache keeps.
    fn assert_plain(head: &str) {
        assert!(
            head.contains("\r\ncontent-type: text/plain; charset=utf-8\r\n"),
            "{head}"
        );
        assert!(head.contains("\r\ncache-control: no-store\r\n"), "{head}");
    }

    /// Whether the server closed `stream` without an answer (EOF or a
    /// reset) within a second.
    async fn closed(mut stream: TcpStream) -> bool {
        let mut buf = [0u8; 64];
        match tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf)).await {
            Ok(Ok(n)) => n == 0,
            Ok(Err(e)) => e.kind() == io::ErrorKind::ConnectionReset,
            Err(_) => false,
        }
    }

    #[test]
    fn the_reason_names_the_step_and_how_long_it_has_run() {
        let secs = Duration::from_secs(41);
        assert_eq!(
            not_ready(Phase::Starting(Step::RecoveringTasks), secs).as_deref(),
            Some("starting (recovering tasks, 41s)")
        );
        assert_eq!(not_ready(Phase::Serving, secs), None);
        assert_eq!(
            not_ready(Phase::Draining, secs).as_deref(),
            Some("draining")
        );
        assert_eq!(
            not_ready(Phase::Stopping, secs).as_deref(),
            Some("stopping")
        );
    }

    #[tokio::test]
    async fn livez_is_ok_in_every_phase_and_readyz_follows_the_phase() {
        let lc = Lifecycle::new();
        let addr = spawn(&lc, Limits::default()).await;
        let phases: [(&dyn Fn(), u16, &str); 7] = [
            (&|| {}, 503, "not ready: starting (opening database, 0s)\n"),
            (
                &|| lc.starting(Step::ConfiguringAuth),
                503,
                "not ready: starting (configuring auth, 0s)\n",
            ),
            (
                &|| lc.starting(Step::RecoveringTasks),
                503,
                "not ready: starting (recovering tasks, 0s)\n",
            ),
            (
                &|| lc.starting(Step::LoadingModels),
                503,
                "not ready: starting (loading models, 0s)\n",
            ),
            (&|| lc.serving(), 200, "ok\n"),
            (
                &|| {
                    lc.drain();
                },
                503,
                "not ready: draining\n",
            ),
            (&|| lc.stopping(), 503, "not ready: stopping\n"),
        ];
        for (enter, status, body) in phases {
            enter();
            let phase = lc.phase();
            assert_eq!(get(addr, "/livez").await, (200, "ok\n".into()), "{phase:?}");
            for path in ["/readyz", "/", "/readyz?verbose"] {
                assert_eq!(
                    get(addr, path).await,
                    (status, body.into()),
                    "{phase:?} {path}"
                );
            }
        }
    }

    #[tokio::test]
    async fn head_options_http_1_0_and_the_errors() {
        let lc = Lifecycle::new();
        lc.serving();
        let addr = spawn(&lc, Limits::default()).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        // HEAD: the GET answer's head, with no body (the next answer on the
        // connection follows at once).
        let (status, head, _) = ask(&mut stream, "HEAD", "/readyz").await;
        assert_eq!(status, 200);
        assert!(head.contains("\r\ncontent-length: 3\r\n"), "{head}");
        assert_plain(&head);
        let (status, head, body) = ask(&mut stream, "OPTIONS", "/livez").await;
        assert_eq!((status, body.as_str()), (200, "ok\n"));
        assert!(head.contains("\r\nallow: get, head, options\r\n"), "{head}");
        assert_plain(&head);
        for method in ["POST", "PUT", "DELETE"] {
            let (status, head, body) = ask(&mut stream, method, "/readyz").await;
            assert_eq!(
                (status, body.as_str()),
                (405, "method not allowed\n"),
                "{method}"
            );
            assert!(head.contains("\r\nallow: get, head, options\r\n"), "{head}");
            assert_plain(&head);
        }
        for (method, path) in [("GET", "/nope"), ("POST", "/nope"), ("GET", "/readyz/")] {
            let (status, head, body) = ask(&mut stream, method, path).await;
            assert_eq!(
                (status, body.as_str()),
                (404, "not found\n"),
                "{method} {path}"
            );
            assert_plain(&head);
        }

        // HTTP/1.0 with no Host: answered, then closed.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /readyz HTTP/1.0\r\n\r\n")
            .await
            .unwrap();
        let mut answer = String::new();
        tokio::time::timeout(Duration::from_secs(1), stream.read_to_string(&mut answer))
            .await
            .expect("closed after the answer")
            .unwrap();
        assert!(answer.starts_with("HTTP/1.0 200 OK\r\n"), "{answer}");
        assert!(answer.ends_with("\r\n\r\nok\n"), "{answer}");
    }

    #[tokio::test]
    async fn slow_and_idle_connections_are_closed() {
        let lc = Lifecycle::new();
        let limits = Limits {
            header_read: Duration::from_millis(200),
            ..Limits::default()
        };
        let addr = spawn(&lc, limits).await;
        // Nothing sent.
        assert!(closed(TcpStream::connect(addr).await.unwrap()).await);
        // Half a request head.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"GET /livez HTTP/1.1\r\n").await.unwrap();
        assert!(closed(stream).await);
        // Idle after an answer.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        assert_eq!(ask(&mut stream, "GET", "/livez").await.0, 200);
        assert!(closed(stream).await);
    }

    #[tokio::test]
    async fn connections_past_the_cap_wait_for_one_to_close() {
        let lc = Lifecycle::new();
        let limits = Limits {
            connections: 1,
            ..Limits::default()
        };
        let addr = spawn(&lc, limits).await;
        let mut first = TcpStream::connect(addr).await.unwrap();
        assert_eq!(ask(&mut first, "GET", "/livez").await.0, 200);

        // The second waits while the first stays open...
        let mut second = TcpStream::connect(addr).await.unwrap();
        second
            .write_all(b"GET /livez HTTP/1.1\r\nhost: test\r\n\r\n")
            .await
            .unwrap();
        let mut byte = [0u8; 1];
        let early = tokio::time::timeout(Duration::from_millis(300), second.peek(&mut byte)).await;
        assert!(early.is_err(), "answered past the cap: {early:?}");
        // ...and is answered once it closes.
        drop(first);
        let answer = tokio::time::timeout(Duration::from_secs(1), read_answer(&mut second, false))
            .await
            .expect("answered once a slot is free");
        assert_eq!((answer.0, answer.2.as_str()), (200, "ok\n"));
    }

    /// An engine on a SQLite database in `dir`, following `lc`; with
    /// `stall`, every storage call waits.
    async fn engine_in(
        dir: &tempfile::TempDir,
        lc: &Lifecycle,
        stall: bool,
    ) -> (Engine, Option<apollo_engine::testing::StallGate>) {
        let toml = format!(
            "[database.sqlite]\npath = \"{}\"\n",
            dir.path().join("apollo.db").display()
        );
        let config = apollo_config::load::from_str(&toml).unwrap();
        let storage = apollo_storage::open(&config.database).await.unwrap();
        let (storage, gate) = if stall {
            let (stalled, gate) = apollo_engine::testing::Stalled::new(storage);
            gate.shut();
            (
                Arc::new(stalled) as Arc<dyn apollo_storage::Storage>,
                Some(gate),
            )
        } else {
            (storage, None)
        };
        let engine =
            Engine::with_lifecycle(Arc::new(config), storage, None, "n".into(), lc.clone());
        (engine, gate)
    }

    #[tokio::test]
    async fn probes_answer_while_storage_is_stalled() {
        let dir = tempfile::tempdir().unwrap();
        let lc = Lifecycle::new();
        let (engine, _gate) = engine_in(&dir, &lc, true).await;
        // The engine waits on the store...
        let status = tokio::spawn(async move { engine.status().await.is_ok() });
        lc.serving();
        let addr = spawn(&lc, Limits::default()).await;
        // ...and the probes don't.
        for (path, status, body) in [
            ("/livez", 200, "ok\n"),
            ("/readyz", 200, "ok\n"),
            ("/", 200, "ok\n"),
        ] {
            let at = Instant::now();
            assert_eq!(get(addr, path).await, (status, body.into()), "{path}");
            assert!(
                at.elapsed() < Duration::from_millis(100),
                "{path}: {:?}",
                at.elapsed()
            );
        }
        assert!(lc.drain());
        let at = Instant::now();
        assert_eq!(get(addr, "/readyz").await.0, 503);
        assert!(
            at.elapsed() < Duration::from_millis(100),
            "{:?}",
            at.elapsed()
        );
        assert!(!status.is_finished(), "the store answered");
    }

    /// `[health]` on a free loopback port with `allow`, as the config gives it.
    fn health_config(allow: &str) -> (Config, SocketAddr) {
        let port = free_port();
        let toml =
            format!("[health]\nenabled = true\nendpoint = \"127.0.0.1\"\nport = {port}\n{allow}");
        let config = apollo_config::load::from_str(&toml).unwrap();
        (config, SocketAddr::from(([127, 0, 0, 1], port)))
    }

    #[tokio::test]
    async fn a_peer_not_in_health_allow_is_reset_and_counted() {
        let dir = tempfile::tempdir().unwrap();
        let lc = Lifecycle::new();
        // Before the engine exists, as at startup.
        let (config, addr) = health_config("allow = [\"127.0.0.2\"]\n");
        start(&config, &lc).await.unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        assert!(closed(stream).await);

        // One family with the metrics listener's refusals.
        let (engine, _) = engine_in(&dir, &lc, false).await;
        engine.note_refused("metrics");
        let text = engine.metrics_text().await.unwrap();
        let refused: Vec<&str> = text
            .lines()
            .filter(|l| l.starts_with("apollo_listener_refused_total"))
            .collect();
        assert_eq!(
            refused,
            [
                "apollo_listener_refused_total{listener=\"health\"} 1",
                "apollo_listener_refused_total{listener=\"metrics\"} 1"
            ]
        );
    }

    #[tokio::test]
    async fn a_peer_in_health_allow_is_answered() {
        let lc = Lifecycle::new();
        let (config, addr) = health_config("allow = [\"127.0.0.1\"]\n");
        start(&config, &lc).await.unwrap();
        let at = Instant::now();
        assert_eq!(get(addr, "/livez").await, (200, "ok\n".into()));
        assert!(
            at.elapsed() < Duration::from_millis(100),
            "{:?}",
            at.elapsed()
        );
    }

    #[tokio::test]
    async fn a_health_allow_that_cannot_match_fails_the_start() {
        let lc = Lifecycle::new();
        let (config, _) = health_config("allow = [\"10.0.0.0/8\"]\n");
        let err = start(&config, &lc).await.unwrap_err().to_string();
        assert!(
            err.starts_with("health.allow: no entry can match on 127.0.0.1:"),
            "{err}"
        );
        // Off: nothing is bound.
        let (mut config, addr) = health_config("");
        config.health.enabled = false;
        start(&config, &lc).await.unwrap();
        assert!(TcpStream::connect(addr).await.is_err());
    }
}
