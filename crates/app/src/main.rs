//! `apollo` — binary entrypoint. Parses the CLI and dispatches to
//! `start` / `stop` / `status` / `config` / `keygen` / `token` / `revoke`. Each subcommand owns its own setup (the server
//! builds a Tokio runtime; `config`/`stop` are synchronous).

mod cli;
mod daemon;
mod status;

use clap::Parser;

fn main() {
    if let Err(e) = cli::Cli::parse().run() {
        // `{:#}` renders the full anyhow context chain.
        eprintln!("apollo: {e:#}");
        std::process::exit(1);
    }
}
