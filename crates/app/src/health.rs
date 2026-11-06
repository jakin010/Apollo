//! The health checks load balancers and orchestrators probe (`[health]`):
//! plaintext HTTP/1.1 on a port of its own, bound before storage opens and
//! open until the process exits. Answers come from the process's
//! [`Lifecycle`] and the load sample it holds only, so a probe never waits on
//! the database or a model.
//!
//! | Path | 200 | 503 |
//! |---|---|---|
//! | `/livez` | always | never |
//! | `/readyz`, `/` | ready for work | starting, draining or stopping, with why |
//! | `/readyz/strict` | ready, and the database isn't stale | otherwise, with why |
//! | `/load` | always: the load figure as JSON | never |
//!
//! A ready answer while degraded (load, database) says so, with
//! `x-envoy-degraded: 1`. `?verbose` on a readiness path answers with the
//! details as JSON, only to a local peer that no proxy forwarded for.

use std::convert::Infallible;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, Response, StatusCode, Uri, header};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use apollo_config::Config;
use apollo_engine::{Database, Lifecycle, LoadSnapshot, Phase};

use crate::allow::{self, Bound, Guard, PeerAddr};

/// How long a connection may take to send a request's headers, and may
/// stay idle between requests.
pub(crate) const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Connections open at once. More wait, unaccepted, until one closes.
pub(crate) const MAX_CONNECTIONS: usize = 256;

/// The methods every path answers.
const ALLOW: &str = "GET, HEAD, OPTIONS";

/// Headers a proxy adds: a request with one may come from anywhere, whatever
/// its TCP peer.
const FORWARDING: [&str; 4] = ["forwarded", "x-forwarded-for", "x-real-ip", "via"];

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
    tracing::info!(
        %addr,
        "serving health checks on /livez, /readyz, /readyz/strict and /load"
    );
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
        // Admitted by `[health].allow`, which holds until a restart.
        let (stream, peer) = allow::accept_allowed(&mut listener, &guard).await;
        let peer = PeerAddr(peer).ip();
        let _ = stream.set_nodelay(true);
        let lc = lc.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |request| {
                let response = respond(
                    &lc,
                    peer,
                    request.method(),
                    request.uri(),
                    request.headers(),
                );
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

/// What a path asks for.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Probe {
    Live,
    Ready,
    Strict,
    Load,
}

/// The answer to `method` on `uri` from `peer`, as `lc` stands now. The
/// `Host` header is ignored, and the query but for `verbose`.
fn respond(
    lc: &Lifecycle,
    peer: IpAddr,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Response<Body> {
    let probe = match uri.path() {
        "/livez" => Probe::Live,
        "/readyz" | "/" => Probe::Ready,
        "/readyz/strict" => Probe::Strict,
        "/load" => Probe::Load,
        _ => return reply(StatusCode::NOT_FOUND, "not found\n".into()),
    };
    let allowed = matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS);
    let mut response = if !allowed {
        reply(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed\n".into(),
        )
    } else {
        match probe {
            Probe::Live => reply(StatusCode::OK, "ok\n".into()),
            Probe::Load => reply_json(StatusCode::OK, &load_body(lc, &lc.load())),
            Probe::Ready | Probe::Strict => {
                let verbose = wants_verbose(uri) && may_see_details(peer, headers);
                readiness(lc, probe == Probe::Strict, verbose)
            }
        }
    };
    if !allowed || *method == Method::OPTIONS {
        let allow = HeaderValue::from_static(ALLOW);
        response.headers_mut().insert(header::ALLOW, allow);
    }
    response
}

/// `/readyz` (or with `strict`, `/readyz/strict`): `ok`, `ok degraded:
/// load,database`, or `not ready: …`; with `verbose`, the details as JSON.
fn readiness(lc: &Lifecycle, strict: bool, verbose: bool) -> Response<Body> {
    let load = lc.load();
    let phase_reason = not_ready(lc.phase(), lc.since().elapsed());
    let database_reason = stale(load.database);
    let reason = match &phase_reason {
        Some(reason) => Some(reason),
        None if strict => database_reason.as_ref(),
        None => None,
    };
    let status = match reason {
        Some(_) => StatusCode::SERVICE_UNAVAILABLE,
        None => StatusCode::OK,
    };
    let degraded = load.degraded();
    let mut response = if verbose {
        let reasons: Vec<&String> = phase_reason.iter().chain(&database_reason).collect();
        reply_json(status, &verbose_body(lc, &load, &reasons))
    } else {
        let body = match reason {
            Some(reason) => format!("not ready: {reason}\n"),
            None if degraded.is_empty() => "ok\n".into(),
            None => format!("ok degraded: {}\n", degraded.join(",")),
        };
        reply(status, body)
    };
    if status == StatusCode::OK && !degraded.is_empty() {
        let flag = HeaderValue::from_static("1");
        response.headers_mut().insert("x-envoy-degraded", flag);
    }
    response
}

/// Why `/readyz/strict` fails for `database`: `database (no contact 47s)`.
fn stale(database: Database) -> Option<String> {
    match database {
        Database::Stale { last_contact } => Some(format!(
            "database (no contact {}s)",
            last_contact.elapsed().as_secs()
        )),
        _ => None,
    }
}

/// Whether the query asks for `verbose`.
fn wants_verbose(uri: &Uri) -> bool {
    uri.query().is_some_and(|query| {
        query
            .split('&')
            .any(|pair| pair.split('=').next() == Some("verbose"))
    })
}

/// Whether `peer` may see the details: exactly 127.0.0.1 or ::1, with no
/// header a proxy adds.
fn may_see_details(peer: IpAddr, headers: &HeaderMap) -> bool {
    let local = peer == IpAddr::V4(Ipv4Addr::LOCALHOST) || peer == IpAddr::V6(Ipv6Addr::LOCALHOST);
    local && !FORWARDING.iter().any(|name| headers.contains_key(*name))
}

/// The `/load` body: the instance's load figure, without model names or the
/// node id.
fn load_body(lc: &Lifecycle, load: &LoadSnapshot) -> Value {
    let ready = lc.ready();
    json!({
        "phase": lc.phase().name(),
        "ready": ready,
        "ready_strict": ready && !load.database.is_stale(),
        "drained": false,
        "degraded": load.degraded(),
        "utilization": rounded(load.utilization),
        "weight": load.weight(ready),
        "items": {
            "in_flight": load.in_flight,
            "max_pending": load.max_pending,
            "free": load.free(),
        },
        "slots": {
            "total": load.slots_total,
            "busy": load.slots_busy,
            "waiting": load.slots_waiting,
        },
        "memory": { "utilization": load.memory.map(rounded) },
        "database": load.database.name(),
        "sampled_ms_ago": load.sampled_at.map(|at| at.elapsed().as_millis() as u64),
    })
}

/// The `?verbose` body: why it isn't ready (`reasons`), each model's load,
/// the database, the load figure and the config in force.
fn verbose_body(lc: &Lifecycle, load: &LoadSnapshot, reasons: &[&String]) -> Value {
    let phase = lc.phase();
    let models: Vec<Value> = load
        .models
        .iter()
        .map(|m| {
            json!({
                "model": m.label,
                "pinned": m.pinned,
                "preload": m.preload.name(),
                "admitted": m.admitted,
                "max_pending": (m.max_pending > 0).then_some(m.max_pending),
            })
        })
        .collect();
    let sampled = load.sampled_at.is_some();
    json!({
        "phase": phase.name(),
        "step": match phase {
            Phase::Starting(step) => Some(step.name()),
            _ => None,
        },
        "since_secs": lc.since().elapsed().as_secs(),
        "reasons": reasons,
        "models": models,
        "database": {
            "state": load.database.name(),
            "last_contact_secs": load.database.last_contact().map(|at| at.elapsed().as_secs()),
        },
        "load": load_body(lc, load),
        "config_generation": sampled.then_some(load.generation),
        "restart_required": load.restart_required,
    })
}

/// `v` to 3 decimals.
fn rounded(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
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
    answer(status, body, "text/plain; charset=utf-8")
}

/// A JSON answer that no cache keeps.
fn reply_json(status: StatusCode, body: &Value) -> Response<Body> {
    answer(status, format!("{body}\n"), "application/json")
}

fn answer(status: StatusCode, body: String, content_type: &'static str) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io;
    use std::net::SocketAddr;
    use std::time::Instant;

    use apollo_engine::testing::{StallGate, Stalled, publish_load};
    use apollo_engine::{Engine, ModelLoad, Preload, Step};
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
        ask_with(stream, method, path, "").await
    }

    /// [`ask`], with the header lines `extra`.
    async fn ask_with(
        stream: &mut TcpStream,
        method: &str,
        path: &str,
        extra: &str,
    ) -> (u16, String, String) {
        let request = format!("{method} {path} HTTP/1.1\r\nhost: test\r\n{extra}\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        read_answer(stream, method == "HEAD").await
    }

    /// GET `path` with the header lines `extra` on a new connection: the
    /// status, the head (lowercase) and the body.
    async fn fetch(addr: SocketAddr, path: &str, extra: &str) -> (u16, String, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        ask_with(&mut stream, "GET", path, extra).await
    }

    /// GET `path`: the status, the plain-text body, and whether it says
    /// degraded in `x-envoy-degraded`.
    async fn probe(addr: SocketAddr, path: &str) -> (u16, String, bool) {
        let (status, head, body) = fetch(addr, path, "").await;
        assert_plain(&head);
        (status, body, head.contains("\r\nx-envoy-degraded: 1\r\n"))
    }

    /// GET `path` from `addr`, answered as JSON: the status and the body.
    async fn json_at(addr: SocketAddr, path: &str) -> (u16, Value) {
        let (status, head, body) = fetch(addr, path, "").await;
        assert!(
            head.contains("\r\ncontent-type: application/json\r\n"),
            "{head}"
        );
        assert!(head.contains("\r\ncache-control: no-store\r\n"), "{head}");
        (status, serde_json::from_str(&body).unwrap())
    }

    /// When the database last answered: `secs` ago.
    fn ago(secs: u64) -> std::time::Instant {
        std::time::Instant::now()
            .checked_sub(Duration::from_secs(secs))
            .unwrap()
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
            for path in ["/readyz", "/", "/readyz/strict"] {
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

    /// An engine on a SQLite database in `dir`, following `lc`, through a
    /// store that counts its calls; with `stall`, every storage call waits.
    async fn engine_in(
        dir: &tempfile::TempDir,
        lc: &Lifecycle,
        stall: bool,
    ) -> (Engine, StallGate) {
        let toml = format!(
            "[database.sqlite]\npath = \"{}\"\n",
            dir.path().join("apollo.db").display()
        );
        let config = apollo_config::load::from_str(&toml).unwrap();
        let storage = apollo_storage::open(&config.database).await.unwrap();
        let (stalled, gate) = Stalled::new(storage);
        if stall {
            gate.shut();
        }
        let engine = Engine::with_lifecycle(
            Arc::new(config),
            Arc::new(stalled),
            None,
            "n".into(),
            lc.clone(),
        );
        (engine, gate)
    }

    #[tokio::test]
    async fn probes_answer_while_storage_is_stalled() {
        let dir = tempfile::tempdir().unwrap();
        let lc = Lifecycle::new();
        let (engine, _gate) = engine_in(&dir, &lc, true).await;
        // The engine waits on the store...
        let status = tokio::spawn(async move { engine.status().await });
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
        let text = engine.metrics_text().await;
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

    #[tokio::test]
    async fn ready_answers_say_degraded_and_strict_readiness_follows_the_database() {
        let lc = Lifecycle::new();
        lc.serving();
        let addr = spawn(&lc, Limits::default()).await;
        // Not shared, not busy.
        for path in ["/readyz", "/readyz/strict"] {
            assert_eq!(
                probe(addr, path).await,
                (200, "ok\n".into(), false),
                "{path}"
            );
        }

        let busy = LoadSnapshot {
            overloaded: true,
            ..LoadSnapshot::default()
        };
        publish_load(&lc, busy.clone());
        for path in ["/readyz", "/", "/readyz/strict"] {
            assert_eq!(
                probe(addr, path).await,
                (200, "ok degraded: load\n".into(), true),
                "{path}"
            );
        }

        // A stale database: still ready, but not strictly.
        let stale = LoadSnapshot {
            database: Database::Stale {
                last_contact: ago(47),
            },
            ..busy
        };
        publish_load(&lc, stale);
        assert_eq!(
            probe(addr, "/readyz").await,
            (200, "ok degraded: load,database\n".into(), true)
        );
        assert_eq!(
            probe(addr, "/readyz/strict").await,
            (503, "not ready: database (no contact 47s)\n".into(), false)
        );
        // HEAD says the same in its head.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let (status, head, _) = ask(&mut stream, "HEAD", "/readyz").await;
        assert_eq!(status, 200);
        assert!(head.contains("\r\nx-envoy-degraded: 1\r\n"), "{head}");

        // The phase comes first.
        assert!(lc.drain());
        for path in ["/readyz", "/readyz/strict"] {
            assert_eq!(
                probe(addr, path).await,
                (503, "not ready: draining\n".into(), false),
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn load_answers_200_in_every_phase_with_only_its_fields() {
        let lc = Lifecycle::new();
        let addr = spawn(&lc, Limits::default()).await;
        let fields = |v: &Value| {
            let mut keys: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
            keys.sort();
            keys
        };
        let expected = [
            "database",
            "degraded",
            "drained",
            "items",
            "memory",
            "phase",
            "ready",
            "ready_strict",
            "sampled_ms_ago",
            "slots",
            "utilization",
            "weight",
        ];

        // Before the engine's first sample.
        let (status, body) = json_at(addr, "/load").await;
        assert_eq!(status, 200);
        assert_eq!(fields(&body), expected);
        assert_eq!(
            (&body["phase"], &body["ready"], &body["weight"]),
            (&json!("starting"), &json!(false), &json!(0))
        );
        assert_eq!(
            (&body["database"], &body["sampled_ms_ago"]),
            (&json!("n/a"), &Value::Null)
        );

        let sample = LoadSnapshot {
            sampled_at: Some(tokio::time::Instant::now()),
            utilization: 0.42,
            in_flight: 3,
            max_pending: 10,
            slots_total: 4,
            slots_busy: 2,
            slots_waiting: 1,
            memory: Some(0.5),
            models: vec![ModelLoad {
                label: "nsfw".into(),
                pinned: true,
                preload: Preload::Loaded,
                admitted: 3,
                max_pending: 4,
            }],
            ..LoadSnapshot::default()
        };
        publish_load(&lc, sample.clone());
        lc.serving();
        let (status, mut body) = json_at(addr, "/load").await;
        assert_eq!(status, 200);
        let sampled = body["sampled_ms_ago"].take();
        assert!(sampled.as_u64().unwrap() < 1000, "{sampled}");
        assert_eq!(
            body,
            json!({
                "phase": "serving",
                "ready": true,
                "ready_strict": true,
                "drained": false,
                "degraded": [],
                "utilization": 0.42,
                "weight": 60,
                "items": { "in_flight": 3, "max_pending": 10, "free": 7 },
                "slots": { "total": 4, "busy": 2, "waiting": 1 },
                "memory": { "utilization": 0.5 },
                "database": "n/a",
                "sampled_ms_ago": null,
            })
        );
        // No model names, no node id.
        assert!(!body.to_string().contains("nsfw"), "{body}");

        // A stale database: ready but not strictly, weight 1.
        publish_load(
            &lc,
            LoadSnapshot {
                database: Database::Stale {
                    last_contact: ago(3),
                },
                ..sample
            },
        );
        let (status, body) = json_at(addr, "/load").await;
        assert_eq!(status, 200);
        assert_eq!(
            (
                &body["ready"],
                &body["ready_strict"],
                &body["weight"],
                &body["degraded"],
                &body["database"]
            ),
            (
                &json!(true),
                &json!(false),
                &json!(1),
                &json!(["database"]),
                &json!("stale")
            )
        );
        let stops: [(&str, &dyn Fn()); 2] = [
            ("draining", &|| {
                lc.drain();
            }),
            ("stopping", &|| lc.stopping()),
        ];
        for (phase, enter) in stops {
            enter();
            let (status, body) = json_at(addr, "/load").await;
            assert_eq!(status, 200, "{phase}");
            assert_eq!(fields(&body), expected);
            assert_eq!(
                (&body["phase"], &body["weight"]),
                (&json!(phase), &json!(0))
            );
        }
    }

    #[test]
    fn verbose_details_go_only_to_a_local_peer_no_proxy_forwarded_for() {
        let lc = Lifecycle::new();
        lc.serving();
        let answer = |peer: IpAddr, path: &str, headers: &[&'static str]| {
            let mut map = HeaderMap::new();
            for name in headers {
                map.insert(*name, HeaderValue::from_static("192.0.2.7"));
            }
            let uri: Uri = path.parse().unwrap();
            let response = respond(&lc, peer, &Method::GET, &uri, &map);
            response.headers()[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .to_string()
        };
        let mapped = PeerAddr("[::ffff:127.0.0.1]:1".parse().unwrap()).ip();
        for peer in ["127.0.0.1".parse().unwrap(), "::1".parse().unwrap(), mapped] {
            for path in [
                "/readyz?verbose",
                "/?verbose=1",
                "/readyz/strict?x=1&verbose",
            ] {
                assert_eq!(answer(peer, path, &[]), "application/json", "{peer} {path}");
            }
            assert_eq!(
                answer(peer, "/readyz?verbosely", &[]),
                "text/plain; charset=utf-8"
            );
            for header in FORWARDING {
                assert_eq!(
                    answer(peer, "/readyz?verbose", &[header]),
                    "text/plain; charset=utf-8",
                    "{peer} {header}"
                );
            }
        }
        // Not exactly 127.0.0.1 or ::1: the query is ignored.
        for peer in ["127.0.0.2", "::2", "192.0.2.7", "10.0.0.1"] {
            let peer = peer.parse().unwrap();
            assert_eq!(
                answer(peer, "/readyz?verbose", &[]),
                "text/plain; charset=utf-8",
                "{peer}"
            );
        }
    }

    #[tokio::test]
    async fn verbose_details_over_a_connection() {
        let lc = Lifecycle::new();
        lc.serving();
        publish_load(
            &lc,
            LoadSnapshot {
                sampled_at: Some(tokio::time::Instant::now()),
                database: Database::Stale {
                    last_contact: ago(47),
                },
                models: vec![ModelLoad {
                    label: "nsfw".into(),
                    pinned: true,
                    preload: Preload::Failed,
                    admitted: 1,
                    max_pending: 0,
                }],
                generation: 3,
                restart_required: vec!["app.port".into()],
                ..LoadSnapshot::default()
            },
        );
        let addr = spawn(&lc, Limits::default()).await;
        let (status, body) = json_at(addr, "/readyz/strict?verbose").await;
        assert_eq!(status, 503);
        assert_eq!(body["phase"], "serving");
        assert_eq!(body["step"], Value::Null);
        assert_eq!(body["reasons"], json!(["database (no contact 47s)"]));
        assert_eq!(
            body["models"],
            json!([{
                "model": "nsfw",
                "pinned": true,
                "preload": "failed",
                "admitted": 1,
                "max_pending": null,
            }])
        );
        assert_eq!(body["database"]["state"], "stale");
        assert_eq!(body["database"]["last_contact_secs"], 47);
        assert_eq!(body["load"]["weight"], 1);
        assert_eq!(body["config_generation"], 3);
        assert_eq!(body["restart_required"], json!(["app.port"]));
        let (status, body) = json_at(addr, "/readyz?verbose").await;
        assert_eq!(
            (status, &body["reasons"]),
            (200, &json!(["database (no contact 47s)"]))
        );

        // Through a proxy: the plain answer.
        let (status, head, body) =
            fetch(addr, "/readyz?verbose", "x-forwarded-for: 192.0.2.7\r\n").await;
        assert_plain(&head);
        assert_eq!((status, body.as_str()), (200, "ok degraded: database\n"));
    }

    #[tokio::test]
    async fn probes_never_touch_storage() {
        let dir = tempfile::tempdir().unwrap();
        let lc = Lifecycle::new();
        let (engine, gate) = engine_in(&dir, &lc, false).await;
        lc.serving();
        let addr = spawn(&lc, Limits::default()).await;
        let first = lc.load().sampled_at;
        let before = gate.calls();
        let paths = [
            "/livez",
            "/readyz",
            "/",
            "/readyz/strict",
            "/load",
            "/readyz?verbose",
            "/readyz/strict?verbose",
        ];
        for _ in 0..3 {
            for path in paths {
                assert_eq!(fetch(addr, path, "").await.0, 200, "{path}");
            }
        }
        // The engine's load samples meanwhile, without the store either.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_ne!(lc.load().sampled_at, first);
        assert_eq!(gate.calls(), before);
        // The store does count calls.
        engine.status().await;
        assert!(gate.calls() > before);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_database_out_for_every_instance_keeps_readyz_and_fails_strict_everywhere() {
        let mut fleet = Vec::new();
        for _ in 0..2 {
            let dir = tempfile::tempdir().unwrap();
            let toml = format!(
                "[database]\nlease_secs = 1\n[database.sqlite]\npath = \"{}\"\n",
                dir.path().join("apollo.db").display()
            );
            let config = apollo_config::load::from_str(&toml).unwrap();
            let storage = apollo_storage::open(&config.database).await.unwrap();
            // As if shared: the engine keeps in touch, and reports it.
            let (stalled, gate) = Stalled::new_shared(storage);
            let lc = Lifecycle::new();
            let engine = Engine::with_lifecycle(
                Arc::new(config),
                Arc::new(stalled),
                None,
                "n".into(),
                lc.clone(),
            );
            engine.recover().await.unwrap();
            lc.serving();
            let addr = spawn(&lc, Limits::default()).await;
            assert_eq!(
                probe(addr, "/readyz/strict").await,
                (200, "ok\n".into(), false)
            );
            fleet.push((dir, engine, gate, lc, addr));
        }

        for (_, _, gate, ..) in &fleet {
            gate.shut();
        }
        let deadline = Instant::now() + Duration::from_secs(8);
        while !fleet
            .iter()
            .all(|(.., lc, _)| lc.load().database.is_stale())
        {
            assert!(Instant::now() < deadline, "not stale everywhere");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        for (.., lc, addr) in &fleet {
            assert_eq!(
                probe(*addr, "/readyz").await,
                (200, "ok degraded: database\n".into(), true)
            );
            let (status, body, _) = probe(*addr, "/readyz/strict").await;
            assert_eq!(status, 503);
            assert!(
                body.starts_with("not ready: database (no contact "),
                "{body}"
            );
            assert_eq!(lc.weight(), 1);
        }
        for (_, _, gate, ..) in &fleet {
            gate.open();
        }
    }
}
