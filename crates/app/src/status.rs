//! `apollo status`: a running server answers on a Unix socket in its private
//! per-user directory with a JSON [`StatusReport`]; the CLI reads and renders it.
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
    pub listen: String,
    pub tls: bool,
    pub mutual_tls: bool,
    pub auth: bool,
    pub config_file: Option<String>,
    pub database: String,
    /// Where a `--daemon` server logs (foreground servers log to their terminal).
    pub log_file: Option<String>,
    pub device: String,
    pub memory: Memory,
    pub queue: Queue,
    pub tasks: Tasks,
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
    pub pending: usize,
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

/// What the socket sends: a report, or why one could not be produced.
#[derive(Debug, Serialize, Deserialize)]
struct Reply {
    status: Option<StatusReport>,
    error: Option<String>,
}

/// Where the status socket lives: the per-user runtime directory on Linux
/// (`$XDG_RUNTIME_DIR`), else the state directory next to the PID file.
pub fn socket_path() -> anyhow::Result<PathBuf> {
    let dir = match dirs::runtime_dir() {
        Some(runtime) => {
            let dir = runtime.join("apollo");
            crate::daemon::create_private_dir(&dir)?;
            dir
        }
        None => crate::daemon::state_dir()?,
    };
    Ok(dir.join("apollo.sock"))
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

/// Serve status reports on `path` until the returned guard drops. Each
/// connection receives one JSON [`Reply`] built by `report`. Must be called
/// within a Tokio runtime.
#[cfg(unix)]
pub fn serve_socket<F, Fut>(path: &Path, report: F) -> std::io::Result<SocketGuard>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = anyhow::Result<StatusReport>> + Send + 'static,
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

    let report = std::sync::Arc::new(report);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                continue;
            };
            let report = std::sync::Arc::clone(&report);
            tokio::spawn(async move {
                let reply = match report().await {
                    Ok(status) => Reply {
                        status: Some(status),
                        error: None,
                    },
                    Err(e) => Reply {
                        status: None,
                        error: Some(format!("{e:#}")),
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

/// Ask the server listening on `path` for its status.
#[cfg(unix)]
pub fn fetch(path: &Path) -> anyhow::Result<StatusReport> {
    use anyhow::{Context, anyhow};
    use std::io::Read;

    let mut stream = std::os::unix::net::UnixStream::connect(path)
        .with_context(|| format!("connecting to {}", path.display()))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut body = Vec::new();
    stream
        .read_to_end(&mut body)
        .context("reading the status reply")?;
    let reply: Reply = serde_json::from_slice(&body).context("parsing the status reply")?;
    match (reply.status, reply.error) {
        (Some(status), _) => Ok(status),
        (None, Some(error)) => Err(anyhow!("the server could not report its status: {error}")),
        (None, None) => Err(anyhow!("empty status reply")),
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
    let on = |b: bool| if b { "on" } else { "off" };
    line(format!(
        "  listening  {}   TLS {}{}, auth {}",
        r.listen,
        on(r.tls),
        if r.mutual_tls { " (mutual)" } else { "" },
        on(r.auth)
    ));
    line(format!("  device     {}", r.device));
    if let Some(c) = &r.config_file {
        line(format!("  config     {c}"));
    }
    line(format!("  database   {}", r.database));
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
    let t = &r.tasks;
    line(format!(
        "tasks      {} queued · {} processing · {} completed · {} failed · {} cancelled",
        thousands(t.queued),
        thousands(t.processing),
        thousands(t.completed),
        thousands(t.failed),
        thousands(t.cancelled)
    ));
    if let Some(w) = &r.webhook {
        line(format!(
            "webhook    {} — {} pending deliveries",
            w.url,
            thousands(w.pending as u64)
        ));
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
            listen: "127.0.0.1:8080".into(),
            tls: true,
            mutual_tls: false,
            auth: true,
            config_file: Some("/etc/apollo/config.toml".into()),
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
            tasks: Tasks {
                queued: 1,
                processing: 2,
                completed: 1204,
                failed: 3,
                cancelled: 7,
            },
            webhook: Some(Webhook {
                url: "https://hooks.example.com".into(),
                pending: 0,
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
        assert!(text.contains("TLS on, auth on"), "{text}");
        assert!(text.contains("1.5 GiB used (limit 4.0 GiB)"), "{text}");
        assert!(text.contains("3 items in flight (limit 1,024); 2 of 20 slots busy, 1 waiting"));
        assert!(text.contains("1,204 completed"));
        assert!(text.contains("https://hooks.example.com — 0 pending deliveries"));
        assert!(text.contains("general"));
        assert!(text.contains("loaded (pinned)"));
        assert!(text.contains("12,402"));
        assert!(text.contains("3s ago"), "{text}");
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
        let guard = serve_socket(&path, move || {
            let r = served.clone();
            async move { Ok(r) }
        })
        .unwrap();

        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);

        let p = path.clone();
        let got = tokio::task::spawn_blocking(move || fetch(&p))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, report);

        drop(guard);
        assert!(!path.exists(), "socket removed on shutdown");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn server_errors_are_reported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.sock");
        let _guard = serve_socket(&path, || async {
            Err(anyhow::anyhow!("database is locked"))
        })
        .unwrap();
        let p = path.clone();
        let err = tokio::task::spawn_blocking(move || fetch(&p))
            .await
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(err.contains("database is locked"), "{err}");
    }
}
