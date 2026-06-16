//! Daemon lifecycle: the PID file (RAII), graceful-shutdown signal handling, and
//! sending a shutdown signal to a running instance.

use std::fs;
use std::path::PathBuf;

use anyhow::{anyhow, Context};

/// Path where a running instance records its PID.
fn pid_path() -> PathBuf {
    std::env::temp_dir().join("apollo.pid")
}

/// Writes this process's PID on creation and removes the file when dropped, so a
/// clean shutdown leaves no stale PID file behind.
pub struct PidFile {
    path: PathBuf,
}

impl PidFile {
    /// Record the current process's PID.
    pub fn create() -> anyhow::Result<Self> {
        let path = pid_path();
        fs::write(&path, std::process::id().to_string())
            .with_context(|| format!("writing PID file {}", path.display()))?;
        Ok(Self { path })
    }

    /// Read the recorded PID, if a PID file exists.
    pub fn read() -> anyhow::Result<Option<u32>> {
        let path = pid_path();
        match fs::read_to_string(&path) {
            Ok(s) => Ok(s.trim().parse::<u32>().ok()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => {
                Err(anyhow::Error::new(e).context(format!("reading {}", path.display())))
            }
        }
    }
}

impl Drop for PidFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Send a graceful-shutdown signal (SIGTERM) to `pid`.
#[cfg(unix)]
pub fn terminate(pid: u32) -> anyhow::Result<()> {
    // Safe: kill() with a valid pid and signal has no memory-safety implications.
    let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    if rc != 0 {
        return Err(anyhow!(
            "could not signal pid {pid}: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
pub fn terminate(_pid: u32) -> anyhow::Result<()> {
    Err(anyhow!("`stop` is only supported on Unix platforms"))
}

/// Resolves on Ctrl-C or SIGTERM; drives the server's graceful drain.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    tracing::info!("shutdown signal received; draining");
}
