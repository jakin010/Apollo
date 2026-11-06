//! Who may connect to a monitoring listener, by TCP peer address.
//!
//! A refused peer is reset at accept, before hyper reads a byte. A reload
//! that drops a peer from `[metrics].allow` answers its next request on an
//! open connection with 403 and `connection: close`. Each refusal is counted;
//! a bounded number of them are logged.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use apollo_config::Allowlist;
use axum::Router;
use axum::extract::connect_info::{ConnectInfo, Connected};
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::serve::IncomingStream;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

/// Counts one refusal for a listener (`apollo_listener_refused_total`).
pub(crate) type CountHook = Arc<dyn Fn(&'static str) + Send + Sync>;

/// Reads the list in force; `None` admits everyone.
pub(crate) type Rules = Arc<dyn Fn() -> Option<Arc<Allowlist>> + Send + Sync>;

/// The reloadable lists, and the config generation they come from.
#[derive(Debug, Clone)]
pub(crate) struct Published {
    pub(crate) generation: u64,
    /// `[metrics].allow`.
    pub(crate) metrics: Option<Arc<Allowlist>>,
}

/// Replace the published lists with `next`, unless a newer generation is
/// out already (reloads can finish in any order). Whether it replaced them.
pub(crate) fn publish(tx: &watch::Sender<Published>, next: Published) -> bool {
    tx.send_if_modified(|current| {
        let newer = next.generation > current.generation;
        if newer {
            *current = next;
        }
        newer
    })
}

/// A config section's `allow` list, as its `allowlist()` parsed it, with
/// the errors under `field` (`metrics.allow`). `None` admits everyone.
pub(crate) fn parsed(
    field: &str,
    list: Result<Option<Allowlist>, Vec<String>>,
) -> anyhow::Result<Option<Arc<Allowlist>>> {
    let list = list.map_err(|errs| anyhow!("{field} {}", errs.join("; ")))?;
    Ok(list.map(Arc::new))
}

/// `[metrics].allow`, as last published.
pub(crate) fn metrics_rules(rx: watch::Receiver<Published>) -> Rules {
    Arc::new(move || rx.borrow().metrics.clone())
}

/// A listener's socket, as the allow check sees it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Bound {
    pub(crate) local: SocketAddr,
    /// An IPv6 socket that refuses IPv4 peers (`IPV6_V6ONLY`).
    pub(crate) only_v6: bool,
}

impl Bound {
    pub(crate) fn of(listener: &TcpListener) -> io::Result<Self> {
        let local = listener.local_addr()?;
        let only_v6 = local.is_ipv6() && only_v6(listener)?;
        Ok(Self { local, only_v6 })
    }

    /// `list` checked against this socket: an error when no entry can match,
    /// otherwise the warnings to log. Each message starts with `field`.
    pub(crate) fn check(&self, field: &str, list: &Allowlist) -> anyhow::Result<Vec<String>> {
        match list.check_bind(self.local, self.only_v6) {
            Ok(warnings) => Ok(warnings.iter().map(|w| format!("{field}: {w}")).collect()),
            Err(e) => Err(anyhow!("{field}: {e}")),
        }
    }

    /// The line logged at startup and on reload, e.g. `metrics
    /// 0.0.0.0:9464 allows 10.0.0.0/8 (loopback refused)`.
    pub(crate) fn describe(&self, listener: &str, list: Option<&Allowlist>) -> String {
        match list {
            None => format!("{listener} {} allows everyone", self.local),
            Some(list) => match list.loopback_note(self.local, self.only_v6) {
                Some(note) => format!("{listener} {} allows {list} ({note})", self.local),
                None => format!("{listener} {} allows {list}", self.local),
            },
        }
    }
}

/// Whether an IPv6 socket refuses IPv4 peers.
#[cfg(unix)]
fn only_v6(listener: &TcpListener) -> io::Result<bool> {
    use std::os::fd::AsRawFd;
    let mut value: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: the descriptor is an open socket, and `value` has `len` bytes.
    let rc = unsafe {
        libc::getsockopt(
            listener.as_raw_fd(),
            libc::IPPROTO_IPV6,
            libc::IPV6_V6ONLY,
            (&raw mut value).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(value != 0)
}

/// Windows sockets refuse IPv4 peers on IPv6 unless told otherwise.
#[cfg(not(unix))]
fn only_v6(_listener: &TcpListener) -> io::Result<bool> {
    Ok(true)
}

/// What a listener checks peers against, and how it reports refusals.
#[derive(Clone)]
pub(crate) struct Guard {
    listener: &'static str,
    rules: Rules,
    log: Arc<Mutex<RefusalLog>>,
    count: CountHook,
}

impl Guard {
    /// `listener` names it in logs and the counter (`metrics`), `field` is
    /// the config field (`[metrics].allow`), `local` its address. Spawns the
    /// task that logs the summary of refusals left out of the log once a
    /// minute.
    pub(crate) fn new(
        listener: &'static str,
        field: &'static str,
        local: SocketAddr,
        rules: Rules,
        count: CountHook,
    ) -> Self {
        let log = RefusalLog::new(listener, local, field, Instant::now());
        let log = Arc::new(Mutex::new(log));
        spawn_summaries(Arc::downgrade(&log));
        Self {
            listener,
            rules,
            log,
            count,
        }
    }

    fn allows(&self, peer: IpAddr) -> bool {
        (self.rules)().is_none_or(|list| list.contains(peer))
    }

    /// Count `peer`'s refusal, and log it if the log has room.
    fn refuse(&self, peer: IpAddr) {
        (self.count)(self.listener);
        let line = self.log.lock().unwrap().refused(peer, Instant::now());
        if let Some(line) = line {
            tracing::warn!("{line}");
        }
    }
}

/// Log `log`'s summary once a minute, until its guard is gone.
fn spawn_summaries(log: Weak<Mutex<RefusalLog>>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + MINUTE, MINUTE);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let Some(log) = log.upgrade() else {
                return;
            };
            let summary = log.lock().unwrap().summary();
            if let Some(summary) = summary {
                tracing::warn!("{summary}");
            }
        }
    });
}

/// The next connection from a peer `guard` admits. A refused peer is
/// counted, logged and reset (`SO_LINGER` 0: an RST, and no TIME_WAIT on
/// this side). Accept errors are retried as axum does.
pub(crate) async fn accept_allowed(
    listener: &mut TcpListener,
    guard: &Guard,
) -> (TcpStream, SocketAddr) {
    loop {
        let (stream, peer) = <TcpListener as axum::serve::Listener>::accept(listener).await;
        if guard.allows(peer.ip()) {
            return (stream, peer);
        }
        guard.refuse(peer.ip());
        let _ = stream.set_zero_linger();
    }
}

/// A listener that hands axum only the peers its guard admits.
pub(crate) struct Guarded {
    listener: TcpListener,
    guard: Guard,
}

impl Guarded {
    pub(crate) fn new(listener: TcpListener, guard: Guard) -> Self {
        Self { listener, guard }
    }
}

impl axum::serve::Listener for Guarded {
    type Io = TcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (TcpStream, SocketAddr) {
        accept_allowed(&mut self.listener, &self.guard).await
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

/// A connection's TCP peer, for [`ConnectInfo`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct PeerAddr(pub(crate) SocketAddr);

impl PeerAddr {
    /// The peer's address; an IPv4-mapped one as IPv4.
    pub(crate) fn ip(&self) -> IpAddr {
        self.0.ip().to_canonical()
    }
}

impl Connected<IncomingStream<'_, Guarded>> for PeerAddr {
    fn connect_info(stream: IncomingStream<'_, Guarded>) -> Self {
        Self(*stream.remote_addr())
    }
}

/// Serve `router` on `listener` to the peers `guard` admits.
pub(crate) async fn serve(listener: TcpListener, router: Router, guard: Guard) -> io::Result<()> {
    let router = guarded(router, guard.clone());
    let service = router.into_make_service_with_connect_info::<PeerAddr>();
    axum::serve(Guarded::new(listener, guard), service).await
}

/// `router` with every request, unknown paths too, checked against the
/// list in force.
fn guarded(router: Router, guard: Guard) -> Router {
    router.layer(axum::middleware::from_fn_with_state(guard, check))
}

/// Refuse a request from a peer the list no longer admits (after a reload):
/// 403, and `connection: close`, so the client reconnects and is refused at
/// accept.
async fn check(
    State(guard): State<Guard>,
    ConnectInfo(peer): ConnectInfo<PeerAddr>,
    request: Request,
    next: Next,
) -> Response {
    if guard.allows(peer.ip()) {
        return next.run(request).await;
    }
    guard.refuse(peer.ip());
    (
        StatusCode::FORBIDDEN,
        [(header::CONNECTION, "close")],
        "forbidden\n",
    )
        .into_response()
}

/// Sources a [`RefusalLog`] remembers, and distinct sources a summary counts.
const MAX_SOURCES: usize = 256;
/// Lines a listener logs a minute, besides the summary.
const MAX_LINES: usize = 10;
/// Peers a summary names.
const LATEST: usize = 3;
const MINUTE: Duration = Duration::from_secs(60);

/// Which refusals a listener logs. A source's first refusal is logged, then
/// at most once a minute, and at most [`MAX_LINES`] lines a minute in all;
/// the rest go into a summary, logged once a minute. IPv6 sources are
/// grouped by /64.
pub(crate) struct RefusalLog {
    listener: &'static str,
    local: SocketAddr,
    field: &'static str,
    /// When each source was last logged.
    logged: HashMap<IpAddr, Instant>,
    /// When the current minute started, and the lines logged in it.
    minute: Instant,
    lines: usize,
    /// Refusals left out since the last summary, their sources (counted up
    /// to [`MAX_SOURCES`]) and the latest peers.
    skipped: u64,
    skipped_sources: HashSet<IpAddr>,
    latest: VecDeque<IpAddr>,
}

impl RefusalLog {
    pub(crate) fn new(
        listener: &'static str,
        local: SocketAddr,
        field: &'static str,
        now: Instant,
    ) -> Self {
        Self {
            listener,
            local,
            field,
            logged: HashMap::new(),
            minute: now,
            lines: 0,
            skipped: 0,
            skipped_sources: HashSet::new(),
            latest: VecDeque::new(),
        }
    }

    /// A refusal of `peer` at `now`: the line to log, if it gets one.
    pub(crate) fn refused(&mut self, peer: IpAddr, now: Instant) -> Option<String> {
        let peer = peer.to_canonical();
        let source = source_of(peer);
        if now.saturating_duration_since(self.minute) >= MINUTE {
            self.minute = now;
            self.lines = 0;
        }
        let recent = self
            .logged
            .get(&source)
            .is_some_and(|at| now.saturating_duration_since(*at) < MINUTE);
        if !recent && self.lines < MAX_LINES && self.remember(source, now) {
            self.lines += 1;
            return Some(format!(
                "refused {peer} on the {} port {} (not in {})",
                self.listener, self.local, self.field
            ));
        }
        self.skipped += 1;
        if self.skipped_sources.len() < MAX_SOURCES {
            self.skipped_sources.insert(source);
        }
        self.latest.retain(|p| *p != peer);
        if self.latest.len() == LATEST {
            self.latest.pop_front();
        }
        self.latest.push_back(peer);
        None
    }

    /// Note that `source` was logged at `now`. False when the log is full of
    /// sources logged within the minute; then it isn't logged either.
    fn remember(&mut self, source: IpAddr, now: Instant) -> bool {
        if self.logged.len() >= MAX_SOURCES && !self.logged.contains_key(&source) {
            self.logged
                .retain(|_, at| now.saturating_duration_since(*at) < MINUTE);
            if self.logged.len() >= MAX_SOURCES {
                return false;
            }
        }
        self.logged.insert(source, now);
        true
    }

    /// The summary of the refusals left out since the last one, if any.
    pub(crate) fn summary(&mut self) -> Option<String> {
        if self.skipped == 0 {
            return None;
        }
        let sources = match self.skipped_sources.len() {
            n if n >= MAX_SOURCES => format!("{n} or more sources"),
            1 => "1 source".to_string(),
            n => format!("{n} sources"),
        };
        let latest: Vec<String> = self.latest.iter().map(ToString::to_string).collect();
        let line = format!(
            "refused {} connection{} from {sources} on the {} port {} (not in {}); latest: {}",
            self.skipped,
            if self.skipped == 1 { "" } else { "s" },
            self.listener,
            self.local,
            self.field,
            latest.join(", ")
        );
        self.skipped = 0;
        self.skipped_sources.clear();
        self.latest.clear();
        Some(line)
    }
}

/// The source a refusal is logged under: the address, or its /64 for IPv6.
fn source_of(peer: IpAddr) -> IpAddr {
    match peer {
        IpAddr::V4(_) => peer,
        IpAddr::V6(v6) => IpAddr::V6(Ipv6Addr::from(u128::from(v6) & !u128::from(u64::MAX))),
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::extract::connect_info::MockConnectInfo;
    use axum::routing::get;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::health::tests::{ask, closed};

    fn list(entries: &[&str]) -> Option<Arc<Allowlist>> {
        let entries: Vec<String> = entries.iter().map(|e| e.to_string()).collect();
        Some(Arc::new(Allowlist::parse(&entries).unwrap()))
    }

    fn fixed(list: Option<Arc<Allowlist>>) -> Rules {
        Arc::new(move || list.clone())
    }

    fn app() -> Router {
        Router::new().route("/metrics", get(|| async { "ok" }))
    }

    /// Serve [`app`] on `listener` with `rules`: its address, and the
    /// refusals counted.
    async fn server(listener: TcpListener, rules: Rules) -> (SocketAddr, Arc<AtomicUsize>) {
        let local = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&count);
        let hook: CountHook = Arc::new(move |listener| {
            assert_eq!(listener, "metrics");
            counted.fetch_add(1, Ordering::SeqCst);
        });
        let guard = Guard::new("metrics", "[metrics].allow", local, rules, hook);
        tokio::spawn(serve(listener, app(), guard));
        (local, count)
    }

    /// Whether the server closed `stream` without answering a request (EOF or
    /// a reset) within a second.
    async fn refused(mut stream: TcpStream) -> bool {
        let _ = stream
            .write_all(b"GET /metrics HTTP/1.1\r\nhost: test\r\n\r\n")
            .await;
        closed(stream).await
    }

    #[tokio::test]
    async fn a_peer_not_in_the_list_is_reset_at_accept() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (addr, count) = server(listener, fixed(list(&["192.0.2.0/24"]))).await;
        assert!(refused(TcpStream::connect(addr).await.unwrap()).await);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_listed_peer_is_served() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = Bound::of(&listener).unwrap();
        let allow = list(&["127.0.0.1"]);
        assert_eq!(
            bound
                .check("metrics.allow", allow.as_deref().unwrap())
                .unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            bound.describe("metrics", allow.as_deref()),
            format!("metrics {} allows 127.0.0.1", bound.local)
        );
        assert_eq!(
            bound.describe("metrics", None),
            format!("metrics {} allows everyone", bound.local)
        );
        let (addr, count) = server(listener, fixed(allow)).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let (status, _, body) = ask(&mut stream, "GET", "/metrics").await;
        assert_eq!((status, body.as_str()), (200, "ok"));
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_dual_stack_socket_names_the_loopback_address_it_refuses() {
        let bound = Bound {
            local: "[::]:9464".parse().unwrap(),
            only_v6: false,
        };
        assert_eq!(
            bound.describe("metrics", list(&["::1", "10.0.0.0/8"]).as_deref()),
            "metrics [::]:9464 allows ::1, 10.0.0.0/8 (127.0.0.1 refused)"
        );
        assert_eq!(
            bound.describe("metrics", list(&["127.0.0.1"]).as_deref()),
            "metrics [::]:9464 allows 127.0.0.1 (::1 refused)"
        );
        assert_eq!(
            bound.describe("metrics", list(&["10.0.0.0/8"]).as_deref()),
            "metrics [::]:9464 allows 10.0.0.0/8 (loopback refused)"
        );
        assert_eq!(
            bound.describe("metrics", list(&["127.0.0.1", "::1"]).as_deref()),
            "metrics [::]:9464 allows 127.0.0.1, ::1"
        );
    }

    #[test]
    fn a_bad_list_is_an_error_under_its_field() {
        let bad = vec!["10.0.0.1/8".to_string(), "localhost".to_string()];
        let err = parsed("health.allow", Allowlist::parse(&bad).map(Some)).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("health.allow `10.0.0.1/8`: host bits are set"),
            "{err}"
        );
        assert!(
            err.to_string().contains("; `localhost`: hostnames"),
            "{err}"
        );
        assert!(parsed("health.allow", Ok(None)).unwrap().is_none());
    }

    #[tokio::test]
    async fn a_reload_that_drops_a_peer_closes_its_open_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (tx, rx) = watch::channel(Published {
            generation: 1,
            metrics: list(&["127.0.0.1"]),
        });
        let (addr, count) = server(listener, metrics_rules(rx)).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        assert_eq!(ask(&mut stream, "GET", "/metrics").await.0, 200);

        let next = Published {
            generation: 2,
            metrics: list(&["192.0.2.0/24"]),
        };
        assert!(publish(&tx, next));
        // The same keep-alive connection: 403, then closed.
        let (status, head, _) = ask(&mut stream, "GET", "/metrics").await;
        assert_eq!(status, 403);
        assert!(head.contains("\r\nconnection: close\r\n"), "{head}");
        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf)).await;
        assert!(matches!(read, Ok(Ok(0))), "{read:?}");
        // A new connection is refused at accept.
        assert!(refused(TcpStream::connect(addr).await.unwrap()).await);
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_ipv4_peer_of_a_dual_stack_socket_matches_as_ipv4() {
        let Ok(listener) = TcpListener::bind("[::]:0").await else {
            eprintln!("skipped: no IPv6");
            return;
        };
        let bound = Bound::of(&listener).unwrap();
        if bound.only_v6 {
            eprintln!("skipped: IPv6 sockets here refuse IPv4 peers");
            return;
        }
        let (addr, _) = server(listener, fixed(list(&["127.0.0.1"]))).await;
        let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, addr.port()))
            .await
            .unwrap();
        assert_eq!(ask(&mut stream, "GET", "/metrics").await.0, 200);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn another_loopback_source_is_not_127_0_0_1() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (addr, count) = server(listener, fixed(list(&["127.0.0.1"]))).await;
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.2:0".parse().unwrap()).unwrap();
        let stream = socket.connect(addr).await.unwrap();
        assert!(refused(stream).await);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn every_path_answers_403_to_a_peer_not_in_the_list() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hook: CountHook = Arc::new(|_| {});
        let guard = Guard::new(
            "metrics",
            "[metrics].allow",
            addr,
            fixed(list(&["127.0.0.1"])),
            hook,
        );
        let peer = PeerAddr("192.0.2.7:1".parse().unwrap());
        let router = guarded(app(), guard).layer(MockConnectInfo(peer));
        tokio::spawn(async move { axum::serve(listener, router).await });
        for path in ["/metrics", "/nope"] {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let (status, head, _) = ask(&mut stream, "GET", path).await;
            assert_eq!(status, 403, "{path}");
            assert!(head.contains("\r\nconnection: close\r\n"), "{head}");
        }
    }

    #[test]
    fn the_refusal_log_is_bounded() {
        let local: SocketAddr = "0.0.0.0:9464".parse().unwrap();
        let t0 = Instant::now();
        let mut log = RefusalLog::new("metrics", local, "[metrics].allow", t0);
        let a: IpAddr = "192.0.2.7".parse().unwrap();
        let line = log.refused(a, t0).expect("the first refusal is logged");
        assert_eq!(
            line,
            "refused 192.0.2.7 on the metrics port 0.0.0.0:9464 (not in [metrics].allow)"
        );
        assert_eq!(log.refused(a, t0 + Duration::from_secs(30)), None);
        // A mapped peer is the same source.
        assert_eq!(log.refused("::ffff:192.0.2.7".parse().unwrap(), t0), None);
        // One /64 is one source.
        assert!(log.refused("2001:db8::1".parse().unwrap(), t0).is_some());
        assert_eq!(log.refused("2001:db8::2".parse().unwrap(), t0), None);
        assert_eq!(
            log.summary().as_deref(),
            Some(
                "refused 3 connections from 2 sources on the metrics port 0.0.0.0:9464 \
                 (not in [metrics].allow); latest: 192.0.2.7, 2001:db8::2"
            )
        );
        assert_eq!(log.summary(), None);
        assert!(log.refused(a, t0 + Duration::from_secs(61)).is_some());

        // 10,000 sources over 100 minutes: at most 10 lines a minute, and the
        // memory stays bounded.
        let mut lines = 0;
        for i in 0..10_000u32 {
            let minute = t0 + Duration::from_secs(180 + 60 * u64::from(i / 100));
            let peer = IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + i));
            lines += usize::from(log.refused(peer, minute).is_some());
            assert!(log.logged.len() <= MAX_SOURCES);
            assert!(log.skipped_sources.len() <= MAX_SOURCES);
            assert!(log.latest.len() <= LATEST);
        }
        assert_eq!(lines, 100 * MAX_LINES);
        let summary = log.summary().unwrap();
        assert!(
            summary.starts_with("refused 9000 connections from 256 or more sources"),
            "{summary}"
        );
    }

    #[test]
    fn an_older_generation_never_replaces_a_newer_one() {
        let (tx, rx) = watch::channel(Published {
            generation: 1,
            metrics: None,
        });
        let third = Published {
            generation: 3,
            metrics: list(&["10.0.0.0/8"]),
        };
        assert!(publish(&tx, third));
        let second = Published {
            generation: 2,
            metrics: None,
        };
        assert!(!publish(&tx, second));
        assert_eq!(rx.borrow().generation, 3);
        assert!(rx.borrow().metrics.is_some());
    }
}
