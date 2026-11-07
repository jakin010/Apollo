//! `reload`: apply the edited config file to the running server without a
//! restart (the same as sending it SIGHUP, but with the outcome reported).

use anyhow::{Context, anyhow};
use clap::Args;

use crate::status::ReloadOutcome;

#[derive(Args)]
pub struct ReloadArgs {
    /// Print the outcome as JSON.
    #[arg(long)]
    json: bool,
}

pub fn run(args: ReloadArgs) -> anyhow::Result<()> {
    let pid = crate::daemon::PidFile::read()?
        .ok_or_else(|| anyhow!("apollo is not running (or is still opening its database)"))?;
    if !crate::daemon::is_alive(pid) {
        return Err(anyhow!(
            "apollo is not running (stale PID file for pid {pid})"
        ));
    }
    let outcome = request()?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&outcome)?);
    } else {
        print!("{}", render(&outcome));
    }
    Ok(())
}

#[cfg(unix)]
fn request() -> anyhow::Result<ReloadOutcome> {
    let paths = crate::status::client_socket_paths()?;
    crate::status::reload(&paths).context("reloading the config")
}

#[cfg(not(unix))]
fn request() -> anyhow::Result<ReloadOutcome> {
    Err(anyhow!("`reload` is only supported on Unix platforms"))
}

fn render(o: &ReloadOutcome) -> String {
    let mut out = format!("config reloaded (generation {})\n", o.generation);
    let list = |names: &[String]| names.join(", ");
    for (what, names) in [
        ("added", &o.added),
        ("reloaded (loads on next use)", &o.reloaded),
        ("removed", &o.removed),
    ] {
        if !names.is_empty() {
            out.push_str(&format!("  models {what}: {}\n", list(names)));
        }
    }
    if !o.kept.is_empty() {
        out.push_str(&format!("  models kept: {}\n", list(&o.kept)));
    }
    if !o.restart_required.is_empty() {
        out.push_str(&format!(
            "  restart to apply: {} (the running values stay until then)\n",
            list(&o.restart_required)
        ));
    }
    for warning in &o.warnings {
        out.push_str(&format!("  warning: {warning}\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_what_changed() {
        let text = render(&ReloadOutcome {
            generation: 4,
            added: vec!["ocr".into()],
            reloaded: vec!["category".into()],
            kept: vec!["general".into(), "nsfw".into()],
            removed: vec![],
            restart_required: vec!["app.port".into()],
            warnings: vec![],
        });
        assert_eq!(
            text,
            "config reloaded (generation 4)\n  models added: ocr\n  models reloaded (loads on next use): category\n  models kept: general, nsfw\n  restart to apply: app.port (the running values stay until then)\n"
        );
        let text = render(&ReloadOutcome {
            generation: 5,
            added: vec![],
            reloaded: vec![],
            kept: vec![],
            removed: vec![],
            restart_required: vec!["health.allow".into()],
            warnings: vec!["the next start will fail: health.allow: no entry can match".into()],
        });
        assert_eq!(
            text,
            "config reloaded (generation 5)\n  restart to apply: health.allow (the running values stay until then)\n  warning: the next start will fail: health.allow: no entry can match\n"
        );
    }
}
