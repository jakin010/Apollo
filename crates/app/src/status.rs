//! `apollo status`, `apollo reload` and `apollo drain`: a running server
//! answers on a Unix socket, `apollo.sock` in its private per-user state
//! directory next to the PID file (or, where that can't hold a socket, in
//! `$XDG_RUNTIME_DIR/apollo`; see [`bind_socket`] and
//! [`client_socket_paths`]). A client sends one request line —
//! `status`, `reload`, `drain` or `undrain` — and gets one JSON reply: a
//! [`StatusReport`], a [`ReloadOutcome`] or a [`DrainOutcome`]. A client that
//! sends nothing (older versions) gets a status report, as does any line an
//! older server doesn't know.
//!
//! A local socket (rather than an RPC on the public port) means no token, no TLS
//! setup and no network exposure: only the OS user running the server can
//! connect (the socket is owner-only inside an owner-only directory).

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Everything `apollo status` shows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusReport {
    pub version: String,
    pub pid: u32,
    /// Unix seconds.
    pub started_at: u64,
    /// `starting`, `serving`, `draining` or `stopping`, and while starting
    /// the step (`loading models`). Unset from older servers.
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub step: Option<String>,
    /// Taken out of rotation with `apollo drain`. Unset from older servers.
    #[serde(default)]
    pub drained: Option<bool>,
    pub listen: String,
    pub tls: bool,
    pub mutual_tls: bool,
    pub auth: bool,
    pub config_file: Option<String>,
    /// Configs applied since start (1 = the startup config) and when the
    /// running one was applied (unix seconds). Unset from older servers.
    #[serde(default)]
    pub config_generation: Option<u64>,
    #[serde(default)]
    pub config_loaded_at: Option<u64>,
    pub database: String,
    /// Where a `--daemon` server logs (foreground servers log to their terminal).
    pub log_file: Option<String>,
    pub device: String,
    pub memory: Memory,
    pub queue: Queue,
    /// What load balancers see. Unset from older servers.
    #[serde(default)]
    pub load: Option<Load>,
    /// `None` when the database did not answer in time.
    pub tasks: Option<Tasks>,
    pub webhook: Option<Webhook>,
    pub models: Vec<Model>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Memory {
    /// Memory the server process is using: `phys_footprint` on macOS (includes
    /// Metal GPU buffers), resident set size elsewhere. Older servers sent it
    /// as `rss_bytes`.
    #[serde(alias = "rss_bytes")]
    pub used_bytes: Option<u64>,
    pub limit_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Queue {
    pub items_in_flight: usize,
    /// `0` = unlimited.
    pub max_pending: usize,
    pub slots_total: usize,
    pub slots_busy: usize,
    pub slots_waiting: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Load {
    /// 0 to 1: how close the server is to refusing work.
    pub utilization: f64,
    /// The weight for load balancers (0 when not ready).
    pub weight: u32,
    /// `load`, `database`, or none.
    pub degraded: Vec<String>,
    /// The shared database: `ok` or `stale`; `n/a` when not shared.
    pub database: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Tasks {
    pub queued: u64,
    pub processing: u64,
    pub completed: u64,
    pub failed: u64,
    pub cancelled: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Webhook {
    pub url: String,
    /// `None` when the database did not answer in time.
    pub pending: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Model {
    pub label: String,
    pub repo: String,
    pub architecture: String,
    pub enabled: bool,
    pub loaded: bool,
    pub keep_in_memory: bool,
    pub batch: usize,
    pub input_size: Option<u32>,
    pub queued_jobs: usize,
    pub loads: u64,
    pub classified: u64,
    /// Unix seconds.
    pub last_used: Option<u64>,
}

/// What a reload did (see `apollo_engine::ReloadReport`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReloadOutcome {
    pub generation: u64,
    pub added: Vec<String>,
    pub reloaded: Vec<String>,
    pub kept: Vec<String>,
    pub removed: Vec<String>,
    /// Changed settings that only take effect after a restart.
    pub restart_required: Vec<String>,
    /// What the next start would fail on, from those settings. An older
    /// server sends none.
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// What `apollo drain` (or `--undo`) did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainOutcome {
    /// Out of rotation now.
    pub drained: bool,
    /// Whether this request changed that.
    pub changed: bool,
    /// The phase now: `serving`, or `starting`, `draining` or `stopping`.
    pub phase: String,
    /// Ready for work now (what `/readyz` answers).
    pub ready: bool,
}

/// What a client asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    Status,
    Reload,
    Drain,
    Undrain,
}

/// What the socket sends: a report, reload or drain outcome, or why there
/// is none.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Reply {
    status: Option<StatusReport>,
    #[serde(default)]
    reload: Option<ReloadOutcome>,
    #[serde(default)]
    drain: Option<DrainOutcome>,
    error: Option<String>,
}

/// How long the server waits for a request line before treating the client
/// as an older one that only reads.
const REQUEST_WAIT: Duration = Duration::from_millis(300);

/// The status socket's file name.
const SOCKET_FILE: &str = "apollo.sock";

/// The status socket in `dir`.
fn socket_in(dir: &Path) -> PathBuf {
    dir.join(SOCKET_FILE)
}

/// The fallback directory for the socket in the runtime dir `runtime`:
/// `$XDG_RUNTIME_DIR/apollo`.
fn runtime_socket_dir(runtime: &Path) -> PathBuf {
    runtime.join("apollo")
}

/// Fails when `path` is too long for a Unix socket address, which binding
/// and connecting would refuse with a vaguer error.
#[cfg(unix)]
fn check_socket_len(path: &Path) -> anyhow::Result<()> {
    // `sun_path` less its terminating NUL: 107 bytes on Linux, 103 on macOS.
    let max = std::mem::size_of::<libc::sockaddr_un>()
        - std::mem::offset_of!(libc::sockaddr_un, sun_path)
        - 1;
    let len = path.as_os_str().len();
    if len > max {
        anyhow::bail!(
            "the status socket path {} is {len} bytes, more than the {max} a Unix \
             socket allows",
            path.display()
        );
    }
    Ok(())
}

/// Where the server bound its status socket.
#[cfg(unix)]
#[derive(Debug)]
pub struct Bound<G> {
    pub path: PathBuf,
    pub guard: G,
    /// Why it isn't in the state directory, when it is in the runtime
    /// directory instead.
    pub fallback_reason: Option<anyhow::Error>,
}

/// Bind the status socket with `serve`: `apollo.sock` in the state directory,
/// next to the PID file. It follows `$HOME` (and `$XDG_STATE_HOME` on Linux),
/// not the login session, so the commands find it whether the server and they
/// run under systemd, SSH, `su -`, `sudo -H` or cron, given the same `$HOME`
/// and `$XDG_STATE_HOME`, and logging out doesn't remove it.
///
/// Where the state directory can't hold a socket (a path too long for a
/// socket address, or a home on a filesystem without sockets, such as sshfs)
/// and the server has a runtime directory (`$XDG_RUNTIME_DIR`, which Linux
/// login sessions have), it binds `$XDG_RUNTIME_DIR/apollo/apollo.sock`, the
/// second place the commands look ([`client_socket_paths`]).
#[cfg(unix)]
pub fn bind_socket<G>(serve: impl Fn(&Path) -> std::io::Result<G>) -> anyhow::Result<Bound<G>> {
    bind_in(
        crate::daemon::state_dir(),
        dirs::runtime_dir().as_deref(),
        serve,
    )
}

/// [`bind_socket`] with the state directory `state` and the runtime
/// directory `runtime`.
#[cfg(unix)]
fn bind_in<G>(
    state: anyhow::Result<PathBuf>,
    runtime: Option<&Path>,
    serve: impl Fn(&Path) -> std::io::Result<G>,
) -> anyhow::Result<Bound<G>> {
    use anyhow::Context;

    let bind = |dir: &Path| -> anyhow::Result<(PathBuf, G)> {
        let path = socket_in(dir);
        check_socket_len(&path)?;
        let guard = serve(&path).with_context(|| format!("binding {}", path.display()))?;
        Ok((path, guard))
    };
    let reason = match state.and_then(|dir| bind(&dir)) {
        Ok((path, guard)) => {
            return Ok(Bound {
                path,
                guard,
                fallback_reason: None,
            });
        }
        Err(e) => e,
    };
    let Some(runtime) = runtime else {
        return Err(reason);
    };
    let dir = runtime_socket_dir(runtime);
    match crate::daemon::create_private_dir(&dir).and_then(|()| bind(&dir)) {
        Ok((path, guard)) => Ok(Bound {
            path,
            guard,
            fallback_reason: Some(reason),
        }),
        Err(e) => Err(anyhow::anyhow!(
            "{reason:#}; and in the runtime directory: {e:#}"
        )),
    }
}

/// Where a client looks for the status socket, in order: in the state
/// directory, then `$XDG_RUNTIME_DIR/apollo/apollo.sock` when the client has a
/// runtime directory. A server binds the second when the state directory
/// can't hold a socket (see [`bind_socket`]), as did earlier builds whenever
/// the server had `$XDG_RUNTIME_DIR`.
pub fn client_socket_paths() -> anyhow::Result<Vec<PathBuf>> {
    Ok(socket_candidates(
        &crate::daemon::state_dir()?,
        dirs::runtime_dir().as_deref(),
    ))
}

/// The socket in the state directory `state`, then the one in the runtime
/// directory `runtime`.
fn socket_candidates(state: &Path, runtime: Option<&Path>) -> Vec<PathBuf> {
    let mut paths = vec![socket_in(state)];
    if let Some(runtime) = runtime {
        let fallback = socket_in(&runtime_socket_dir(runtime));
        if fallback != paths[0] {
            paths.push(fallback);
        }
    }
    paths
}

pub fn unix_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Removes the socket file when the server stops.
pub struct SocketGuard(PathBuf);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Serve status reports, reloads and drains on `path` until the returned
/// guard drops. Each connection sends at most one request line and receives
/// one JSON [`Reply`], from `report`, `reload` or `drain` (`true` to drain,
/// `false` to undo it). Must be called within a Tokio runtime.
#[cfg(unix)]
pub fn serve_socket<F, Fut, G, GFut, D>(
    path: &Path,
    report: F,
    reload: G,
    drain: D,
) -> std::io::Result<SocketGuard>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = anyhow::Result<StatusReport>> + Send + 'static,
    G: Fn() -> GFut + Send + Sync + 'static,
    GFut: std::future::Future<Output = anyhow::Result<ReloadOutcome>> + Send + 'static,
    D: Fn(bool) -> DrainOutcome + Send + Sync + 'static,
{
    use std::os::unix::fs::PermissionsExt;
    use tokio::io::AsyncWriteExt;

    // A socket left by a server that did not shut down cleanly would make bind
    // fail; it is ours to replace.
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let listener = tokio::net::UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    let guard = SocketGuard(path.to_path_buf());

    let handlers = std::sync::Arc::new((report, reload, drain));
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                continue;
            };
            let handlers = std::sync::Arc::clone(&handlers);
            tokio::spawn(async move {
                let request = read_request(&mut stream).await;
                let reply = match request {
                    Request::Status => match (handlers.0)().await {
                        Ok(status) => Reply {
                            status: Some(status),
                            ..Default::default()
                        },
                        Err(e) => Reply {
                            error: Some(format!("{e:#}")),
                            ..Default::default()
                        },
                    },
                    Request::Reload => match (handlers.1)().await {
                        Ok(outcome) => Reply {
                            reload: Some(outcome),
                            ..Default::default()
                        },
                        Err(e) => Reply {
                            error: Some(format!("{e:#}")),
                            ..Default::default()
                        },
                    },
                    Request::Drain | Request::Undrain => Reply {
                        drain: Some((handlers.2)(request == Request::Drain)),
                        ..Default::default()
                    },
                };
                if let Ok(body) = serde_json::to_vec(&reply) {
                    let _ = stream.write_all(&body).await;
                }
                let _ = stream.shutdown().await;
            });
        }
    });
    Ok(guard)
}

/// The client's request line: `reload`, `drain` or `undrain`, or anything
/// else (including nothing within [`REQUEST_WAIT`]) for status.
#[cfg(unix)]
async fn read_request(stream: &mut tokio::net::UnixStream) -> Request {
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 64];
    let mut len = 0;
    let read = async {
        while len < buf.len() {
            match stream.read(&mut buf[len..]).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    len += n;
                    if buf[..len].contains(&b'\n') {
                        break;
                    }
                }
            }
        }
    };
    let _ = tokio::time::timeout(REQUEST_WAIT, read).await;
    match std::str::from_utf8(&buf[..len]).map(str::trim) {
        Ok("reload") => Request::Reload,
        Ok("drain") => Request::Drain,
        Ok("undrain") => Request::Undrain,
        _ => Request::Status,
    }
}

/// Connect to the first of `paths` a server listens on. A path too long for
/// a socket, a socket that is missing, or one that nothing listens on (left
/// by a crash) moves on to the next; any other error stops there. When none
/// answers, the error is the first path's.
#[cfg(unix)]
fn connect<P: AsRef<Path>>(paths: &[P]) -> anyhow::Result<std::os::unix::net::UnixStream> {
    use std::io::ErrorKind;

    let mut first: Option<anyhow::Error> = None;
    for path in paths {
        let path = path.as_ref();
        if let Err(e) = check_socket_len(path) {
            first.get_or_insert(e);
            continue;
        }
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                let next = matches!(e.kind(), ErrorKind::NotFound | ErrorKind::ConnectionRefused);
                if first.is_none() {
                    first = Some(
                        anyhow::Error::new(e).context(format!("connecting to {}", path.display())),
                    );
                }
                if !next {
                    break;
                }
            }
        }
    }
    Err(first.unwrap_or_else(|| anyhow::anyhow!("no status socket to connect to")))
}

/// Send `request` to the server listening on the first of `paths` that
/// answers (see [`connect`]) and read its reply.
#[cfg(unix)]
fn exchange<P: AsRef<Path>>(
    paths: &[P],
    request: Request,
    timeout: Duration,
) -> anyhow::Result<Reply> {
    use anyhow::Context;
    use std::io::{Read, Write};

    let mut stream = connect(paths)?;
    stream.set_read_timeout(Some(timeout))?;
    let line: &[u8] = match request {
        Request::Status => b"status\n",
        Request::Reload => b"reload\n",
        Request::Drain => b"drain\n",
        Request::Undrain => b"undrain\n",
    };
    stream.write_all(line).context("sending the request")?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    let mut body = Vec::new();
    stream.read_to_end(&mut body).context("reading the reply")?;
    serde_json::from_slice(&body).context("parsing the reply")
}

/// Ask the server listening on the first of `paths` that answers for its
/// status.
#[cfg(unix)]
pub fn fetch<P: AsRef<Path>>(paths: &[P]) -> anyhow::Result<StatusReport> {
    use anyhow::anyhow;
    let reply = exchange(paths, Request::Status, Duration::from_secs(10))?;
    match (reply.status, reply.error) {
        (Some(status), _) => Ok(status),
        (None, Some(error)) => Err(anyhow!("the server could not report its status: {error}")),
        (None, None) => Err(anyhow!("empty status reply")),
    }
}

/// Ask the server listening on the first of `paths` that answers to reload
/// its config.
#[cfg(unix)]
pub fn reload<P: AsRef<Path>>(paths: &[P]) -> anyhow::Result<ReloadOutcome> {
    use anyhow::anyhow;
    // Loading a new model is lazy, so a reload is quick; allow for a slow disk.
    let reply = exchange(paths, Request::Reload, Duration::from_secs(60))?;
    match reply {
        Reply {
            reload: Some(outcome),
            ..
        } => Ok(outcome),
        Reply {
            error: Some(error), ..
        } => Err(anyhow!("the config was not reloaded: {error}")),
        Reply {
            status: Some(_), ..
        } => Err(anyhow!(
            "this server is too old to reload its config; restart it instead"
        )),
        _ => Err(anyhow!("empty reload reply")),
    }
}

/// Ask the server listening on the first of `paths` that answers to take
/// itself out of rotation (`drain`), or back in.
#[cfg(unix)]
pub fn drain<P: AsRef<Path>>(paths: &[P], drain: bool) -> anyhow::Result<DrainOutcome> {
    use anyhow::anyhow;
    let request = if drain {
        Request::Drain
    } else {
        Request::Undrain
    };
    let reply = exchange(paths, request, Duration::from_secs(10))?;
    match reply {
        Reply {
            drain: Some(outcome),
            ..
        } => Ok(outcome),
        Reply {
            error: Some(error), ..
        } => Err(anyhow!("the server answered with an error: {error}")),
        // An older server answers any line it doesn't know with a report.
        Reply {
            status: Some(_), ..
        } => Err(anyhow!(
            "this server is too old for `apollo drain`; nothing changed"
        )),
        _ => Err(anyhow!("empty drain reply")),
    }
}

// ------------------------------- rendering -------------------------------

/// Images classified per second, per model label, between two reports.
pub type Rates = std::collections::BTreeMap<String, f64>;

/// Per-model throughput between `prev` and `cur`, taken `elapsed` apart. Empty
/// when the server restarted in between (its counters started over).
pub fn rates(prev: &StatusReport, cur: &StatusReport, elapsed: Duration) -> Rates {
    let secs = elapsed.as_secs_f64();
    if secs <= 0.0 || prev.pid != cur.pid {
        return Rates::new();
    }
    cur.models
        .iter()
        .filter_map(|m| {
            let before = prev.models.iter().find(|p| p.label == m.label)?;
            let done = m.classified.checked_sub(before.classified)?;
            Some((m.label.clone(), done as f64 / secs))
        })
        .collect()
}

/// Human-readable status, as printed by `apollo status`. With `rates` (watch
/// mode) it adds live throughput.
pub fn render(r: &StatusReport, now: SystemTime, rates: Option<&Rates>) -> String {
    let now_secs = unix_secs(now);
    let mut out = String::new();
    let mut line = |s: String| {
        out.push_str(&s);
        out.push('\n');
    };

    line(format!(
        "apollo {} — running (pid {}), up {}",
        r.version,
        r.pid,
        duration(now_secs.saturating_sub(r.started_at))
    ));
    let drained = if r.drained == Some(true) {
        ", drained (undo with apollo drain --undo)"
    } else {
        ""
    };
    match (&r.phase, &r.step) {
        (Some(phase), Some(step)) => line(format!("  phase      {phase} ({step}){drained}")),
        (Some(phase), None) => line(format!("  phase      {phase}{drained}")),
        _ => {}
    }
    let on = |b: bool| if b { "on" } else { "off" };
    line(format!(
        "  listening  {}   TLS {}{}, auth {}",
        r.listen,
        on(r.tls),
        if r.mutual_tls { " (mutual)" } else { "" },
        on(r.auth)
    ));
    line(format!("  device     {}", r.device));
    let generation = match (r.config_generation, r.config_loaded_at) {
        (Some(g), Some(at)) if g > 1 => format!(
            " (generation {g}, reloaded {} ago)",
            duration(now_secs.saturating_sub(at))
        ),
        _ => String::new(),
    };
    if let Some(c) = &r.config_file {
        line(format!("  config     {c}{generation}"));
    } else if !generation.is_empty() {
        line(format!("  config    {generation}"));
    }
    match r.load.as_ref().map(|l| l.database.as_str()) {
        Some(state @ ("ok" | "stale")) => {
            line(format!("  database   {} — contact {state}", r.database))
        }
        _ => line(format!("  database   {}", r.database)),
    }
    if let Some(l) = &r.log_file {
        line(format!("  log        {l}"));
    }
    line(format!(
        "  memory     {}{}",
        r.memory
            .used_bytes
            .map_or("unknown".into(), |b| format!("{} used", bytes(b))),
        r.memory
            .limit_bytes
            .map_or(String::new(), |l| format!(" (limit {})", bytes(l)))
    ));

    line(String::new());
    let q = &r.queue;
    let limit = if q.max_pending == 0 {
        "unlimited".to_string()
    } else {
        format!("limit {}", thousands(q.max_pending as u64))
    };
    line(format!(
        "queue      {} items in flight ({limit}); {} of {} slots busy, {} waiting",
        thousands(q.items_in_flight as u64),
        q.slots_busy,
        q.slots_total,
        q.slots_waiting
    ));
    if let Some(l) = &r.load {
        let degraded = if l.degraded.is_empty() {
            String::new()
        } else {
            format!("; degraded: {}", l.degraded.join(", "))
        };
        line(format!(
            "load       {:.0}% utilization, weight {}{degraded}",
            l.utilization * 100.0,
            l.weight
        ));
    }
    match &r.tasks {
        Some(t) => line(format!(
            "tasks      {} queued · {} processing · {} completed · {} failed · {} cancelled",
            thousands(t.queued),
            thousands(t.processing),
            thousands(t.completed),
            thousands(t.failed),
            thousands(t.cancelled)
        )),
        None => line(format!("tasks      {UNAVAILABLE}")),
    }
    if let Some(w) = &r.webhook {
        match w.pending {
            Some(n) => line(format!(
                "webhook    {} — {} pending deliveries",
                w.url,
                thousands(n as u64)
            )),
            None => line(format!("webhook    {} — {UNAVAILABLE}", w.url)),
        }
    }
    if let Some(rates) = rates {
        let total: f64 = rates.values().sum();
        line(format!("throughput {} images/s", rate(total)));
    }

    line(String::new());
    line(format!("models ({})", r.models.len()));
    let rows: Vec<Vec<String>> = r
        .models
        .iter()
        .map(|m| {
            let state = if !m.enabled {
                "disabled".to_string()
            } else if m.loaded {
                if m.keep_in_memory {
                    "loaded (pinned)".to_string()
                } else {
                    "loaded".to_string()
                }
            } else {
                "unloaded".to_string()
            };
            let mut row = vec![
                m.label.clone(),
                m.architecture.clone(),
                m.repo.clone(),
                state,
                m.input_size.map_or("-".into(), |s| format!("{s}px")),
                format!("{}/{}", m.queued_jobs, m.batch),
                thousands(m.classified),
            ];
            if let Some(rates) = rates {
                row.push(
                    rates
                        .get(&m.label)
                        .map_or("-".into(), |r| format!("{}/s", rate(*r))),
                );
            }
            row.push(m.last_used.map_or("never".into(), |t| {
                format!("{} ago", duration(now_secs.saturating_sub(t)))
            }));
            row
        })
        .collect();
    let mut header = vec![
        "LABEL",
        "ARCH",
        "REPO",
        "STATE",
        "INPUT",
        "QUEUE/BATCH",
        "CLASSIFIED",
    ];
    if rates.is_some() {
        header.push("RATE");
    }
    header.push("LAST USED");
    let mut widths: Vec<usize> = header.iter().map(|h| h.chars().count()).collect();
    for row in &rows {
        for (w, cell) in widths.iter_mut().zip(row) {
            *w = (*w).max(cell.chars().count());
        }
    }
    let fmt_row = |cells: Vec<&str>| {
        let mut s = String::from("  ");
        for (i, (cell, w)) in cells.iter().zip(&widths).enumerate() {
            s.push_str(cell);
            if i + 1 < cells.len() {
                s.push_str(&" ".repeat(w - cell.chars().count() + 2));
            }
        }
        s
    };
    line(fmt_row(header.clone()));
    for row in &rows {
        line(fmt_row(row.iter().map(String::as_str).collect()));
    }
    out
}

/// What `apollo status` shows for counts the database did not answer in time.
const UNAVAILABLE: &str = "database unavailable";

/// Images per second: one decimal below 100, whole numbers above.
fn rate(r: f64) -> String {
    if r < 100.0 {
        format!("{r:.1}")
    } else {
        format!("{r:.0}")
    }
}

/// `1h 2m`, `3m 4s`, `5s`, `2d 3h`.
pub fn duration(secs: u64) -> String {
    let (d, h, m, s) = (secs / 86_400, secs / 3600 % 24, secs / 60 % 60, secs % 60);
    match (d, h, m) {
        (0, 0, 0) => format!("{s}s"),
        (0, 0, _) => format!("{m}m {s}s"),
        (0, _, _) => format!("{h}h {m}m"),
        _ => format!("{d}d {h}h"),
    }
}

/// `512 B`, `1.5 KiB`, `1.2 GiB`.
pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit + 1 < UNITS.len() {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

/// `1234567` -> `1,234,567`.
pub fn thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> StatusReport {
        StatusReport {
            version: "1.0.0".into(),
            pid: 4242,
            started_at: 1_000_000,
            phase: Some("serving".into()),
            step: None,
            drained: Some(false),
            listen: "127.0.0.1:8080".into(),
            tls: true,
            mutual_tls: false,
            auth: true,
            config_file: Some("/etc/apollo/config.toml".into()),
            config_generation: Some(3),
            config_loaded_at: Some(1_000_000 + 7_880),
            database: "sqlite /var/lib/apollo/apollo.db".into(),
            log_file: None,
            device: "metal".into(),
            memory: Memory {
                used_bytes: Some(3 * 1024 * 1024 * 1024 / 2),
                limit_bytes: Some(4 * 1024 * 1024 * 1024),
            },
            queue: Queue {
                items_in_flight: 3,
                max_pending: 1024,
                slots_total: 20,
                slots_busy: 2,
                slots_waiting: 1,
            },
            load: Some(Load {
                utilization: 0.42,
                weight: 60,
                degraded: Vec::new(),
                database: "n/a".into(),
            }),
            tasks: Some(Tasks {
                queued: 1,
                processing: 2,
                completed: 1204,
                failed: 3,
                cancelled: 7,
            }),
            webhook: Some(Webhook {
                url: "https://hooks.example.com".into(),
                pending: Some(0),
            }),
            models: vec![Model {
                label: "general".into(),
                repo: "google/vit-base-patch16-224".into(),
                architecture: "vit".into(),
                enabled: true,
                loaded: true,
                keep_in_memory: true,
                batch: 8,
                input_size: Some(224),
                queued_jobs: 0,
                loads: 1,
                classified: 12_402,
                last_used: Some(1_000_000 + 7_997),
            }],
        }
    }

    #[test]
    fn formats_units() {
        assert_eq!(duration(5), "5s");
        assert_eq!(duration(185), "3m 5s");
        assert_eq!(duration(7_980), "2h 13m");
        assert_eq!(duration(2 * 86_400 + 3 * 3600), "2d 3h");
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(1536), "1.5 KiB");
        assert_eq!(bytes(3 * 1024 * 1024 * 1024 / 2), "1.5 GiB");
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(1204), "1,204");
        assert_eq!(thousands(1_234_567), "1,234,567");
    }

    #[test]
    fn renders_a_readable_summary() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000 + 8_000);
        let text = render(&sample(), now, None);
        assert!(
            !text.contains("RATE") && !text.contains("throughput"),
            "{text}"
        );
        assert!(
            text.starts_with("apollo 1.0.0 — running (pid 4242), up 2h 13m"),
            "{text}"
        );
        assert!(text.contains("\n  phase      serving\n"), "{text}");
        assert!(text.contains("TLS on, auth on"), "{text}");
        assert!(
            text.contains("/etc/apollo/config.toml (generation 3, reloaded 2m 0s ago)"),
            "{text}"
        );
        assert!(text.contains("1.5 GiB used (limit 4.0 GiB)"), "{text}");
        assert!(text.contains("3 items in flight (limit 1,024); 2 of 20 slots busy, 1 waiting"));
        assert!(
            text.contains("\nload       42% utilization, weight 60\n"),
            "{text}"
        );
        assert!(
            text.contains("\n  database   sqlite /var/lib/apollo/apollo.db\n"),
            "{text}"
        );
        assert!(text.contains("1,204 completed"));
        assert!(text.contains("https://hooks.example.com — 0 pending deliveries"));
        assert!(text.contains("general"));
        assert!(text.contains("loaded (pinned)"));
        assert!(text.contains("12,402"));
        assert!(text.contains("3s ago"), "{text}");
    }

    #[test]
    fn renders_database_unavailable_for_counts_it_could_not_read() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000 + 8_000);
        let mut report = sample();
        report.tasks = None;
        report.webhook.as_mut().unwrap().pending = None;
        let text = render(&report, now, None);
        assert!(
            text.contains("\ntasks      database unavailable\n"),
            "{text}"
        );
        assert!(
            text.contains("\nwebhook    https://hooks.example.com — database unavailable\n"),
            "{text}"
        );
        // The other lines are as usual.
        let usual = render(&sample(), now, None);
        let changed: Vec<&str> = usual
            .lines()
            .filter(|l| !text.lines().any(|t| t == *l))
            .collect();
        assert_eq!(
            changed,
            [
                "tasks      1 queued · 2 processing · 1,204 completed · 3 failed · 7 cancelled",
                "webhook    https://hooks.example.com — 0 pending deliveries",
            ]
        );
        assert_eq!(text.lines().count(), usual.lines().count());
        let json = serde_json::to_value(&report).unwrap();
        assert!(json["tasks"].is_null(), "{json}");
        assert!(json["webhook"]["pending"].is_null(), "{json}");
    }

    #[test]
    fn renders_degraded_reasons_and_the_database_contact() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000 + 8_000);
        let mut report = sample();
        report.database = "surrealdb ws://db:8000 (apollo/apollo) (node a)".into();
        report.load = Some(Load {
            utilization: 0.95,
            weight: 1,
            degraded: vec!["load".into(), "database".into()],
            database: "stale".into(),
        });
        let text = render(&report, now, None);
        assert!(
            text.contains("\nload       95% utilization, weight 1; degraded: load, database\n"),
            "{text}"
        );
        assert!(
            text.contains(
                "\n  database   surrealdb ws://db:8000 (apollo/apollo) (node a) — contact stale\n"
            ),
            "{text}"
        );

        // An older server sends no load.
        let mut json = serde_json::to_value(sample()).unwrap();
        json.as_object_mut().unwrap().remove("load");
        let old: StatusReport = serde_json::from_value(json).unwrap();
        assert_eq!(old.load, None);
        let text = render(&old, now, None);
        assert!(!text.contains("utilization"), "{text}");
    }

    #[test]
    fn reads_the_old_rss_bytes_field() {
        let m: Memory =
            serde_json::from_str(r#"{"rss_bytes": 1024, "limit_bytes": null}"#).unwrap();
        assert_eq!(m.used_bytes, Some(1024));
        let json = serde_json::to_value(&m).unwrap();
        assert_eq!(json["used_bytes"], 1024);
    }

    #[test]
    fn renders_the_step_while_starting_and_reads_reports_without_a_phase() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000 + 8_000);
        let mut report = sample();
        report.phase = Some("starting".into());
        report.step = Some("loading models".into());
        let text = render(&report, now, None);
        assert!(
            text.contains("\n  phase      starting (loading models)\n"),
            "{text}"
        );

        // An older server sends neither.
        let mut json = serde_json::to_value(sample()).unwrap();
        let fields = json.as_object_mut().unwrap();
        fields.remove("phase");
        fields.remove("step");
        let old: StatusReport = serde_json::from_value(json).unwrap();
        assert_eq!((old.phase, old.step), (None, None));
        let text = render(
            &StatusReport {
                phase: None,
                ..sample()
            },
            now,
            None,
        );
        assert!(!text.contains("phase"), "{text}");
    }

    #[test]
    fn rates_come_from_counter_deltas() {
        let prev = sample();
        let mut cur = sample();
        cur.models[0].classified += 90;
        let r = rates(&prev, &cur, Duration::from_secs(2));
        assert_eq!(r.get("general"), Some(&45.0));

        // A restarted server (new pid) has reset counters: no rates.
        cur.pid += 1;
        assert!(rates(&prev, &cur, Duration::from_secs(2)).is_empty());

        let now = UNIX_EPOCH + Duration::from_secs(1_000_000 + 8_000);
        let mut live = Rates::new();
        live.insert("general".into(), 45.0);
        let text = render(&sample(), now, Some(&live));
        assert!(text.contains("throughput 45.0 images/s"), "{text}");
        assert!(text.contains("RATE") && text.contains("45.0/s"), "{text}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn socket_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.sock");
        let report = sample();
        let served = report.clone();
        let drained = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let guard = serve_socket(
            &path,
            move || {
                let r = served.clone();
                async move { Ok(r) }
            },
            || async { Ok(outcome()) },
            move |on| {
                use std::sync::atomic::Ordering;
                let changed = drained.swap(on, Ordering::SeqCst) != on;
                DrainOutcome {
                    drained: on,
                    changed,
                    phase: "serving".into(),
                    ready: !on,
                }
            },
        )
        .unwrap();

        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);

        let p = path.clone();
        let got = tokio::task::spawn_blocking(move || fetch(&[&p]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, report);

        let p = path.clone();
        let reloaded = tokio::task::spawn_blocking(move || reload(&[&p]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded, outcome());

        for (on, changed) in [(true, true), (true, false), (false, true), (false, false)] {
            let p = path.clone();
            let got = tokio::task::spawn_blocking(move || drain(&[&p], on))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                got,
                DrainOutcome {
                    drained: on,
                    changed,
                    phase: "serving".into(),
                    ready: !on,
                },
                "drain {on}"
            );
        }

        // A client that sends nothing (an older `apollo status`) gets a report.
        let p = path.clone();
        let old = tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut stream = std::os::unix::net::UnixStream::connect(&p).unwrap();
            let mut body = String::new();
            stream.read_to_string(&mut body).unwrap();
            body
        })
        .await
        .unwrap();
        let reply: Reply = serde_json::from_str(&old).unwrap();
        assert_eq!(reply.status, Some(report));

        drop(guard);
        assert!(!path.exists(), "socket removed on shutdown");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn server_errors_are_reported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.sock");
        let _guard = serve_socket(
            &path,
            || async { Err(anyhow::anyhow!("database is locked")) },
            || async { Err(anyhow::anyhow!("model 'x': unknown strategy")) },
            |_| unreachable!("not asked"),
        )
        .unwrap();
        let p = path.clone();
        let err = tokio::task::spawn_blocking(move || fetch(&[&p]))
            .await
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(err.contains("database is locked"), "{err}");
        let p = path.clone();
        let err = tokio::task::spawn_blocking(move || reload(&[&p]))
            .await
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(err.contains("not reloaded: model 'x'"), "{err}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn an_older_server_says_it_cannot_drain() {
        // An older server answers every line with a report.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut line = [0u8; 64];
                let _ = stream.read(&mut line).await;
                let reply = Reply {
                    status: Some(sample()),
                    ..Default::default()
                };
                let body = serde_json::to_vec(&reply).unwrap();
                let _ = stream.write_all(&body).await;
                let _ = stream.shutdown().await;
            }
        });
        for on in [true, false] {
            let p = path.clone();
            let err = tokio::task::spawn_blocking(move || drain(&[&p], on))
                .await
                .unwrap()
                .unwrap_err()
                .to_string();
            assert_eq!(
                err, "this server is too old for `apollo drain`; nothing changed",
                "drain {on}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn drain_fails_when_no_server_answers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.sock");
        let err = format!("{:#}", drain(&[&path], true).unwrap_err());
        assert!(err.starts_with("connecting to "), "{err}");
    }

    #[test]
    fn clients_look_in_the_state_dir_then_the_runtime_dir() {
        let state = Path::new("/home/a/.local/state/apollo");
        let primary = state.join("apollo.sock");
        assert_eq!(
            socket_candidates(state, Some(Path::new("/run/user/1000"))),
            [
                primary.clone(),
                PathBuf::from("/run/user/1000/apollo/apollo.sock")
            ]
        );
        // No runtime dir (systemd, sudo, cron): the state dir alone.
        assert_eq!(socket_candidates(state, None), vec![primary.clone()]);
        // A runtime dir that is the state dir's parent: one path, tried once.
        assert_eq!(
            socket_candidates(state, Some(Path::new("/home/a/.local/state"))),
            [primary]
        );
    }

    /// A directory in `tmp` whose `apollo.sock` is `len` bytes long, or
    /// `None` when `tmp` is too long for that.
    fn dir_with_socket_len(tmp: &Path, len: usize) -> Option<PathBuf> {
        let used = tmp.as_os_str().len() + "/".len() + "/apollo.sock".len();
        let name = len.checked_sub(used).filter(|&n| n > 0)?;
        Some(tmp.join("d".repeat(name)))
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn a_socket_path_too_long_to_bind_is_caught_up_front() {
        let max = if cfg!(target_os = "linux") { 107 } else { 103 };
        let tmp = tempfile::tempdir().unwrap();
        let (Some(fits), Some(over)) = (
            dir_with_socket_len(tmp.path(), max),
            dir_with_socket_len(tmp.path(), max + 1),
        ) else {
            eprintln!("skipped: the temp dir {} is too long", tmp.path().display());
            return;
        };

        std::fs::create_dir(&fits).unwrap();
        let path = socket_in(&fits);
        assert_eq!(path.as_os_str().len(), max);
        check_socket_len(&path).unwrap();
        std::os::unix::net::UnixListener::bind(&path).expect("the longest path binds");

        std::fs::create_dir(&over).unwrap();
        let path = socket_in(&over);
        let err = check_socket_len(&path).unwrap_err().to_string();
        assert!(
            err.contains(&format!(
                "is {} bytes, more than the {max} a Unix socket allows",
                max + 1
            )),
            "{err}"
        );
        assert!(
            std::os::unix::net::UnixListener::bind(&path).is_err(),
            "binding agrees it is too long"
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_server_binds_in_the_runtime_dir_when_the_state_dir_cannot_hold_the_socket() {
        use std::os::unix::fs::PermissionsExt;

        /// A bind that hands back where it bound, and fails anywhere under
        /// `refuse`, as a home without sockets (sshfs) does.
        fn binding(refuse: Option<&Path>) -> impl Fn(&Path) -> std::io::Result<PathBuf> + '_ {
            move |path| match refuse {
                Some(dir) if path.starts_with(dir) => {
                    Err(std::io::Error::other("Operation not supported"))
                }
                _ => Ok(path.to_path_buf()),
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        let runtime = tmp.path().join("runtime");
        std::fs::create_dir(&runtime).unwrap();
        let in_state = state.join("apollo.sock");
        let in_runtime = runtime.join("apollo/apollo.sock");
        let refused = |path: &Path| format!("binding {}: Operation not supported", path.display());

        // The state dir holds it.
        let bound = bind_in(Ok(state.clone()), Some(&runtime), binding(None)).unwrap();
        assert_eq!((&bound.path, &bound.guard), (&in_state, &in_state));
        assert!(bound.fallback_reason.is_none());
        assert!(
            !runtime.join("apollo").exists(),
            "nothing made in the runtime dir"
        );

        // It can't: the runtime dir, in an owner-only directory.
        let bound = bind_in(Ok(state.clone()), Some(&runtime), binding(Some(&state))).unwrap();
        assert_eq!((&bound.path, &bound.guard), (&in_runtime, &in_runtime));
        assert_eq!(
            format!("{:#}", bound.fallback_reason.unwrap()),
            refused(&in_state)
        );
        let mode = std::fs::metadata(runtime.join("apollo"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);

        // A state dir too long for a socket: the same.
        let long = tmp.path().join("d".repeat(120));
        let bound = bind_in(Ok(long), Some(&runtime), binding(None)).unwrap();
        assert_eq!(bound.path, in_runtime);
        let reason = format!("{:#}", bound.fallback_reason.unwrap());
        assert!(reason.contains("more than the"), "{reason}");

        // No runtime dir (systemd, cron): the state dir's error.
        let err = bind_in(Ok(state.clone()), None, binding(Some(&state))).unwrap_err();
        assert_eq!(format!("{err:#}"), refused(&in_state));

        // Neither holds it: both errors.
        let err =
            bind_in(Ok(state.clone()), Some(&runtime), binding(Some(tmp.path()))).unwrap_err();
        assert_eq!(
            format!("{err:#}"),
            format!(
                "{}; and in the runtime directory: {}",
                refused(&in_state),
                refused(&in_runtime)
            )
        );
    }

    /// A status socket at `path` whose reports carry `pid`.
    #[cfg(unix)]
    fn serve_pid(path: &Path, pid: u32) -> std::io::Result<SocketGuard> {
        let report = StatusReport { pid, ..sample() };
        serve_socket(
            path,
            move || {
                let r = report.clone();
                async move { Ok(r) }
            },
            || async { Ok(outcome()) },
            |_| unreachable!("not asked"),
        )
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_client_finds_the_server_whichever_session_started_it() {
        async fn ask(paths: &[PathBuf]) -> anyhow::Result<u32> {
            let paths = paths.to_vec();
            tokio::task::spawn_blocking(move || fetch(&paths).map(|r| r.pid))
                .await
                .unwrap()
        }
        let state = tempfile::tempdir().unwrap();
        let runtime = tempfile::tempdir().unwrap();
        let paths = socket_candidates(state.path(), Some(runtime.path()));
        std::fs::create_dir(runtime.path().join("apollo")).unwrap();

        // Nothing answers: the error names the state dir's socket.
        let err = format!("{:#}", ask(&paths).await.unwrap_err());
        assert!(
            err.starts_with(&format!("connecting to {}: ", paths[0].display())),
            "{err}"
        );

        // Any other error stops there: a state dir that is a file (ENOTDIR,
        // which even root gets) isn't skipped for a server in the runtime dir.
        let file = state.path().join("file");
        std::fs::write(&file, "").unwrap();
        let blocked = [socket_in(&file), paths[1].clone()];
        let live = serve_pid(&paths[1], 2).unwrap();
        let err = format!("{:#}", ask(&blocked).await.unwrap_err());
        assert!(
            err.starts_with(&format!("connecting to {}: ", blocked[0].display())),
            "{err}"
        );
        drop(live);

        // A server bound in the state dir answers a client that has a runtime
        // dir, as when systemd started it and its user SSHes in.
        let guard = serve_pid(&paths[0], 1).unwrap();
        assert_eq!(ask(&paths).await.unwrap(), 1);
        drop(guard);

        // A server bound in the runtime dir (an older build, or a state dir
        // that can't hold a socket) still answers...
        let older = serve_pid(&paths[1], 2).unwrap();
        assert_eq!(ask(&paths).await.unwrap(), 2);
        // ...also past a socket that a crashed server left in the state dir.
        drop(std::os::unix::net::UnixListener::bind(&paths[0]).unwrap());
        assert!(paths[0].exists());
        assert_eq!(ask(&paths).await.unwrap(), 2);

        // The state dir is asked first.
        let guard = serve_pid(&paths[0], 1).unwrap();
        assert_eq!(ask(&paths).await.unwrap(), 1);
        drop((guard, older));

        // A state dir too long for a socket: the server binds in the runtime
        // dir, and the client finds it there past the long path.
        let long = state.path().join("d".repeat(120));
        let long_paths = socket_candidates(&long, Some(runtime.path()));
        let bound = bind_in(Ok(long), Some(runtime.path()), |p| serve_pid(p, 3)).unwrap();
        assert_eq!(bound.path, long_paths[1]);
        assert_eq!(ask(&long_paths).await.unwrap(), 3);
        drop(bound);
        let err = format!("{:#}", ask(&long_paths).await.unwrap_err());
        assert!(err.contains("a Unix socket allows"), "{err}");
    }

    #[test]
    fn renders_a_drained_server_and_reads_reports_without_it() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000 + 8_000);
        let report = StatusReport {
            drained: Some(true),
            ..sample()
        };
        let text = render(&report, now, None);
        assert!(
            text.contains("\n  phase      serving, drained (undo with apollo drain --undo)\n"),
            "{text}"
        );

        // An older server sends no `drained`.
        let mut json = serde_json::to_value(sample()).unwrap();
        json.as_object_mut().unwrap().remove("drained");
        let old: StatusReport = serde_json::from_value(json).unwrap();
        assert_eq!(old.drained, None);
        let text = render(&old, now, None);
        assert!(text.contains("\n  phase      serving\n"), "{text}");
    }

    fn outcome() -> ReloadOutcome {
        ReloadOutcome {
            generation: 2,
            added: vec!["ocr".into()],
            reloaded: vec![],
            kept: vec!["general".into()],
            removed: vec![],
            restart_required: vec!["app.port".into()],
            warnings: vec![],
        }
    }
}
