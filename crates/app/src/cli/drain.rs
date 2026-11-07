//! `drain`: take the running server out of load balancers' rotation without
//! stopping it, or back in with `--undo`.

use anyhow::{Context, anyhow};
use clap::Args;

use crate::status::DrainOutcome;

#[derive(Args)]
pub struct DrainArgs {
    /// Take it back into rotation.
    #[arg(long)]
    undo: bool,
}

pub fn run(args: DrainArgs) -> anyhow::Result<()> {
    let pid = crate::daemon::PidFile::read()?
        .ok_or_else(|| anyhow!("apollo is not running (or is still opening its database)"))?;
    if !crate::daemon::is_alive(pid) {
        return Err(anyhow!(
            "apollo is not running (stale PID file for pid {pid})"
        ));
    }
    let what = if args.undo { "undo its drain" } else { "drain" };
    let outcome = request(!args.undo).with_context(|| {
        format!("asking apollo (pid {pid}) to {what} (it answers once tasks are recovered)")
    })?;
    print!("{}", render(&outcome));
    Ok(())
}

#[cfg(unix)]
fn request(drain: bool) -> anyhow::Result<DrainOutcome> {
    let paths = crate::status::client_socket_paths()?;
    crate::status::drain(&paths, drain)
}

#[cfg(not(unix))]
fn request(_drain: bool) -> anyhow::Result<DrainOutcome> {
    Err(anyhow!("`drain` is only supported on Unix platforms"))
}

fn render(o: &DrainOutcome) -> String {
    let mut out = String::from(match (o.drained, o.changed) {
        (true, true) => "drained\n",
        (true, false) => "already drained\n",
        (false, true) => "undrained\n",
        (false, false) => "not drained\n",
    });
    if o.drained {
        out.push_str(
            "  /readyz fails and the weight is 0, so load balancers send it no new work; \
             it still serves what it is sent, and takes no work over from other instances\n  \
             undo with `apollo drain --undo`\n",
        );
    } else if o.ready {
        out.push_str("  ready for work\n");
    } else {
        out.push_str(&format!("  not ready: {}\n", o.phase));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(drained: bool, changed: bool, phase: &str, ready: bool) -> DrainOutcome {
        DrainOutcome {
            drained,
            changed,
            phase: phase.into(),
            ready,
        }
    }

    #[test]
    fn renders_what_changed() {
        let drained = render(&outcome(true, true, "serving", false));
        assert!(drained.starts_with("drained\n  /readyz fails"), "{drained}");
        assert!(
            drained.ends_with("\n  undo with `apollo drain --undo`\n"),
            "{drained}"
        );
        assert!(render(&outcome(true, false, "serving", false)).starts_with("already drained\n"));
        assert_eq!(
            render(&outcome(false, true, "serving", true)),
            "undrained\n  ready for work\n"
        );
        assert_eq!(
            render(&outcome(false, false, "serving", true)),
            "not drained\n  ready for work\n"
        );
        // Undone while a stop drains it: not ready all the same.
        assert_eq!(
            render(&outcome(false, true, "draining", false)),
            "undrained\n  not ready: draining\n"
        );
    }
}
