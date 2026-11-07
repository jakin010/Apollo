//! `apollo` — binary entrypoint. Parses the CLI and dispatches to
//! `start` / `stop` / `status` / `config` / `keygen` / `token` / `revoke`. Each subcommand owns its own setup (the server
//! builds a Tokio runtime; `config`/`stop` are synchronous).

mod agent;
mod allow;
mod cli;
mod daemon;
mod health;
mod status;

use clap::Parser;

// glibc gives candle's large tensor buffers back to the kernel and faults them in again.
#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    // Before anything opens a TLS connection, e.g. storage over wss://.
    apollo_server::install_crypto_provider();
    if let Err(e) = cli::Cli::parse().run() {
        // `{:#}` renders the full anyhow context chain.
        eprintln!("apollo: {e:#}");
        std::process::exit(1);
    }
}
