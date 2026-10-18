//! Daemon lifecycle: the PID file (RAII), graceful-shutdown signal handling, and
//! sending a shutdown signal to a running instance.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, anyhow};

/// Per-user directory for the PID file and the daemon log: the state dir on
/// Linux (`~/.local/state/apollo`), the local data dir elsewhere
/// (`~/Library/Application Support/apollo` on macOS), created owner-only.
///
/// Deliberately not the shared system temp dir: there another local user could
/// pre-create `apollo.pid` (making `apollo stop` signal a PID of their choosing)
/// or plant an `apollo.log` symlink that the daemon would append to.
pub fn state_dir() -> anyhow::Result<PathBuf> {
    let dir = dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .ok_or_else(|| anyhow!("cannot determine a per-user state directory (is $HOME set?)"))?
        .join("apollo");
    create_private_dir(&dir)?;
    Ok(dir)
}

/// Create `dir` (and parents) if missing; a newly created directory is
/// owner-only (`0700`) on Unix.
pub fn create_private_dir(dir: &std::path::Path) -> anyhow::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    Ok(())
}

/// Path where a running instance records its PID.
fn pid_path() -> anyhow::Result<PathBuf> {
    Ok(state_dir()?.join("apollo.pid"))
}

/// Path the detached daemon logs to.
pub fn log_path() -> anyhow::Result<PathBuf> {
    Ok(state_dir()?.join("apollo.log"))
}

/// Writes this process's PID on creation and removes the file when dropped, so a
/// clean shutdown leaves no stale PID file behind.
pub struct PidFile {
    path: PathBuf,
}

impl PidFile {
    /// Record the current process's PID.
    pub fn create() -> anyhow::Result<Self> {
        let path = pid_path()?;
        fs::write(&path, std::process::id().to_string())
            .with_context(|| format!("writing PID file {}", path.display()))?;
        Ok(Self { path })
    }

    /// Read the recorded PID, if a PID file exists.
    pub fn read() -> anyhow::Result<Option<u32>> {
        let path = pid_path()?;
        match fs::read_to_string(&path) {
            Ok(s) => Ok(s.trim().parse::<u32>().ok()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(anyhow::Error::new(e).context(format!("reading {}", path.display()))),
        }
    }
}

impl Drop for PidFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Whether a process with `pid` exists (signal 0 probes without signalling).
#[cfg(unix)]
pub fn is_alive(pid: u32) -> bool {
    // Safe: kill() with signal 0 performs only the existence/permission check.
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
pub fn is_alive(_pid: u32) -> bool {
    true
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
        use tokio::signal::unix::{SignalKind, signal};
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
