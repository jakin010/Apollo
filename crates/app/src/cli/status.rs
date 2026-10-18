//! `status`: is the server running, and what is it doing.
//!
//! Exit status follows `systemctl status`: 0 when running, 3 when not.

use std::time::SystemTime;

use anyhow::anyhow;
use clap::Args;

#[derive(Args)]
pub struct StatusArgs {
    /// Print the full status as JSON (for scripts and monitoring).
    #[arg(long)]
    json: bool,
}

/// Exit status when the server is not running.
const NOT_RUNNING: i32 = 3;

pub fn run(args: StatusArgs) -> anyhow::Result<()> {
    let Some(pid) = crate::daemon::PidFile::read()? else {
        println!("apollo is not running");
        std::process::exit(NOT_RUNNING);
    };
    if !crate::daemon::is_alive(pid) {
        println!("apollo is not running (stale PID file for pid {pid})");
        std::process::exit(NOT_RUNNING);
    }
    query(pid, args.json)
}

#[cfg(unix)]
fn query(pid: u32, json: bool) -> anyhow::Result<()> {
    let socket = crate::status::socket_path()?;
    let report = crate::status::fetch(&socket).map_err(|e| {
        anyhow!("apollo is running (pid {pid}) but did not answer on its status socket: {e:#}")
    })?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print!("{}", crate::status::render(&report, SystemTime::now()));
    }
    Ok(())
}

#[cfg(not(unix))]
fn query(pid: u32, _json: bool) -> anyhow::Result<()> {
    println!("apollo is running (pid {pid}); detailed status is only available on Unix");
    Ok(())
}
