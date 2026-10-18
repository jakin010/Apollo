//! `status`: is the server running, and what is it doing.
//!
//! Exit status follows `systemctl status`: 0 when running, 3 when not. With
//! `--watch` the view refreshes in place until Ctrl-C (adding live throughput),
//! and keeps waiting if the server stops or restarts.

use std::io::{IsTerminal, Write};
use std::time::{Duration, Instant, SystemTime};

use anyhow::anyhow;
use clap::Args;
use serde::Serialize;

use crate::status::{self, Rates, StatusReport};

#[derive(Args)]
pub struct StatusArgs {
    /// Print the full status as JSON (for scripts and monitoring). With
    /// `--watch`, one JSON object per line per refresh.
    #[arg(long)]
    json: bool,
    /// Keep running and refresh the status in place until Ctrl-C, with live
    /// per-model throughput.
    #[arg(short, long)]
    watch: bool,
    /// Seconds between refreshes with `--watch` (at least 0.2).
    #[arg(short = 'n', long, default_value_t = 1.0, value_parser = parse_interval, requires = "watch")]
    interval: f64,
}

/// Exit status when the server is not running.
const NOT_RUNNING: i32 = 3;

fn parse_interval(s: &str) -> Result<f64, String> {
    let v: f64 = s.parse().map_err(|_| format!("'{s}' is not a number"))?;
    if v.is_finite() && v >= 0.2 {
        Ok(v)
    } else {
        Err("the interval must be at least 0.2 seconds".into())
    }
}

/// What a status check found.
enum Probe {
    NotRunning(String),
    /// The process is alive but did not report (old version, socket missing,
    /// non-Unix platform).
    Unreachable(String),
    Running(Box<StatusReport>),
}

fn probe() -> Probe {
    let pid = match crate::daemon::PidFile::read() {
        Ok(Some(pid)) => pid,
        Ok(None) => return Probe::NotRunning("apollo is not running".into()),
        Err(e) => return Probe::Unreachable(format!("{e:#}")),
    };
    if !crate::daemon::is_alive(pid) {
        return Probe::NotRunning(format!(
            "apollo is not running (stale PID file for pid {pid})"
        ));
    }
    fetch(pid)
}

#[cfg(unix)]
fn fetch(pid: u32) -> Probe {
    match status::socket_path().and_then(|p| status::fetch(&p)) {
        Ok(report) => Probe::Running(Box::new(report)),
        Err(e) => Probe::Unreachable(format!(
            "apollo is running (pid {pid}) but did not answer on its status socket: {e:#}"
        )),
    }
}

#[cfg(not(unix))]
fn fetch(pid: u32) -> Probe {
    Probe::Unreachable(format!(
        "apollo is running (pid {pid}); detailed status is only available on Unix"
    ))
}

pub fn run(args: StatusArgs) -> anyhow::Result<()> {
    if args.watch {
        return watch(&args);
    }
    match probe() {
        Probe::NotRunning(msg) => {
            println!("{msg}");
            std::process::exit(NOT_RUNNING);
        }
        Probe::Unreachable(msg) => Err(anyhow!(msg)),
        Probe::Running(report) => {
            if args.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print!("{}", status::render(&report, SystemTime::now(), None));
            }
            Ok(())
        }
    }
}

/// One `--watch --json` line.
#[derive(Serialize)]
struct WatchLine<'a> {
    /// Unix seconds of this sample.
    time: u64,
    running: bool,
    status: Option<&'a StatusReport>,
    /// Images per second per model since the previous sample.
    rates: Option<&'a Rates>,
    error: Option<&'a str>,
}

/// Switches a terminal to the alternate screen with the cursor hidden, and
/// restores both on drop — including on Ctrl-C, errors, and panics.
struct Screen;

impl Screen {
    fn enter() -> Self {
        print!("\x1b[?1049h\x1b[?25l");
        let _ = std::io::stdout().flush();
        Screen
    }
}

impl Drop for Screen {
    fn drop(&mut self) {
        print!("\x1b[?25h\x1b[?1049l");
        let _ = std::io::stdout().flush();
    }
}

fn watch(args: &StatusArgs) -> anyhow::Result<()> {
    let interval = Duration::from_secs_f64(args.interval);
    // Redraw in place only on a terminal; JSON or piped output is a stream.
    let redraw = !args.json && std::io::stdout().is_terminal();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        // Register for Ctrl-C / SIGTERM before anything else, so a signal
        // arriving mid-refresh still exits cleanly through the screen guard.
        let mut stop = shutdown_signals()?;
        let _screen = redraw.then(Screen::enter);
        let mut prev: Option<(StatusReport, Instant)> = None;
        loop {
            let probed = tokio::task::spawn_blocking(probe).await?;
            let now = Instant::now();
            let report = match &probed {
                Probe::Running(r) => Some(r.as_ref()),
                _ => None,
            };
            let rates = match (report, &prev) {
                (Some(cur), Some((before, at))) => Some(status::rates(before, cur, now - *at)),
                _ => None,
            };

            if args.json {
                let line = WatchLine {
                    time: status::unix_secs(SystemTime::now()),
                    running: report.is_some(),
                    status: report,
                    rates: rates.as_ref(),
                    error: match &probed {
                        Probe::NotRunning(m) | Probe::Unreachable(m) => Some(m),
                        Probe::Running(_) => None,
                    },
                };
                println!("{}", serde_json::to_string(&line)?);
            } else {
                let body = match &probed {
                    Probe::Running(r) => status::render(r, SystemTime::now(), rates.as_ref()),
                    Probe::NotRunning(m) => format!("{m} — waiting for it to start\n"),
                    Probe::Unreachable(m) => format!("{m}\n"),
                };
                let footer = format!("\nrefreshing every {}s · Ctrl-C to quit\n", args.interval);
                if redraw {
                    // Home the cursor, draw, and clear whatever the previous
                    // (possibly longer) frame left below.
                    print!("\x1b[H{body}{footer}\x1b[J");
                } else {
                    println!("{body}");
                }
            }
            std::io::stdout().flush()?;
            prev = report.map(|r| (r.clone(), now));

            tokio::select! {
                _ = stop.recv() => break,
                _ = tokio::time::sleep(interval) => {}
            }
        }
        Ok(())
    })
}

/// Resolves when Ctrl-C (or SIGTERM on Unix) arrives.
struct Signals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl Signals {
    async fn recv(&mut self) {
        #[cfg(unix)]
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn shutdown_signals() -> anyhow::Result<Signals> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Signals {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }
    #[cfg(not(unix))]
    {
        Ok(Signals {})
    }
}
