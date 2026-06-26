//! clap command tree: `start` / `stop` / `config`.

mod config;
mod start;
mod stop;

use clap::{Parser, Subcommand};

/// Apollo — a gRPC service that runs ML classification models over images and video.
#[derive(Parser)]
#[command(name = "apollo", version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the inference server (foreground, or `--daemon` to detach).
    Start(start::StartArgs),
    /// Gracefully stop a running daemon.
    Stop(stop::StopArgs),
    /// Read or edit the config file.
    #[command(subcommand)]
    Config(config::ConfigCmd),
}

impl Cli {
    pub fn run(self) -> anyhow::Result<()> {
        match self.command {
            Command::Start(args) => start::run(args),
            Command::Stop(args) => stop::run(args),
            Command::Config(cmd) => config::run(cmd),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::{CommandFactory, Parser};

    #[test]
    fn cli_is_well_formed() {
        // Catches overlapping args, bad option names, etc. at test time.
        Cli::command().debug_assert();
    }

    #[test]
    fn parses_config_set() {
        assert!(Cli::try_parse_from(["apollo", "config", "set", "app.port", "9090"]).is_ok());
    }

    #[test]
    fn parses_start_with_overrides() {
        assert!(Cli::try_parse_from(["apollo", "start", "--port", "8080", "--daemon"]).is_ok());
    }

    #[test]
    fn start_rejects_non_numeric_port() {
        assert!(Cli::try_parse_from(["apollo", "start", "--port", "nope"]).is_err());
    }

    #[test]
    fn config_requires_a_subcommand() {
        assert!(Cli::try_parse_from(["apollo", "config"]).is_err());
    }
}
