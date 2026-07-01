//! `stop`: read the PID file and signal a graceful shutdown.

use anyhow::{Context, anyhow};
use clap::Args;

#[derive(Args)]
pub struct StopArgs {}

pub fn run(_args: StopArgs) -> anyhow::Result<()> {
    let pid = crate::daemon::PidFile::read()
        .context("reading PID file")?
        .ok_or_else(|| anyhow!("no PID file found — is apollo running?"))?;
    crate::daemon::terminate(pid)?;
    println!("requested graceful shutdown of apollo (pid {pid})");
    Ok(())
}
