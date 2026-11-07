//! clap command tree: `start` / `stop` / `status` / `reload` / `drain` /
//! `config` / keys.

mod calibrate;
mod config;
mod drain;
mod hash;
mod keys;
mod reload;
mod start;
mod status;
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
    /// Show whether the server is running, and its live state (queue, tasks,
    /// models, memory).
    Status(status::StatusArgs),
    /// Apply the edited config file to the running server without restarting
    /// it (also done on SIGHUP).
    Reload(reload::ReloadArgs),
    /// Take the running server out of load balancers' rotation without
    /// stopping it (`--undo` takes it back). It keeps serving what it is sent.
    Drain(drain::DrainArgs),
    /// Read or edit the config file.
    #[command(subcommand)]
    Config(config::ConfigCmd),
    /// Measure a model's precision and recall on labelled examples over a range
    /// of thresholds, and optionally write the recommended threshold.
    Calibrate(calibrate::CalibrateArgs),
    /// Print the PDQ hashes of images, to build block lists for `pdq` models.
    Hash(hash::HashArgs),
    /// Generate a PASETO v4 signing keypair for authentication.
    Keygen(keys::KeygenArgs),
    /// Mint an API token signed with the secret key.
    Token(keys::TokenArgs),
    /// Revoke a token (by id or the token itself) or every token of a subject.
    Revoke(keys::RevokeArgs),
    /// Store this host's public key as the shared key of a shared database (or
    /// rotate it with --replace).
    PublishKey(keys::PublishKeyArgs),
}

impl Cli {
    pub fn run(self) -> anyhow::Result<()> {
        match self.command {
            Command::Start(args) => start::run(args),
            Command::Stop(args) => stop::run(args),
            Command::Status(args) => status::run(args),
            Command::Reload(args) => reload::run(args),
            Command::Drain(args) => drain::run(args),
            Command::Config(cmd) => config::run(cmd),
            Command::Calibrate(args) => calibrate::run(args),
            Command::Hash(args) => hash::run(args),
            Command::Keygen(args) => keys::run_keygen(args),
            Command::Token(args) => keys::run_token(args),
            Command::Revoke(args) => keys::run_revoke(args),
            Command::PublishKey(args) => keys::run_publish_key(args),
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
        let cli = Cli::try_parse_from([
            "apollo",
            "start",
            "--health-port",
            "9500",
            "--metrics-port",
            "9501",
        ])
        .unwrap();
        let super::Command::Start(args) = cli.command else {
            panic!("not start");
        };
        assert_eq!(
            (args.health_port, args.metrics_port),
            (Some(9500), Some(9501))
        );
    }

    #[test]
    fn start_rejects_non_numeric_port() {
        assert!(Cli::try_parse_from(["apollo", "start", "--port", "nope"]).is_err());
    }

    #[test]
    fn revoke_takes_exactly_one_target() {
        assert!(Cli::try_parse_from(["apollo", "revoke", "--id", "abc"]).is_ok());
        assert!(Cli::try_parse_from(["apollo", "revoke", "--subject", "ci"]).is_ok());
        assert!(Cli::try_parse_from(["apollo", "revoke"]).is_err());
        assert!(Cli::try_parse_from(["apollo", "revoke", "--id", "a", "--subject", "b"]).is_err());
    }

    #[test]
    fn parses_the_shared_key_commands() {
        assert!(Cli::try_parse_from(["apollo", "publish-key", "--replace"]).is_ok());
        assert!(
            Cli::try_parse_from([
                "apollo",
                "publish-key",
                "--public-key",
                "k",
                "--secret-key-file",
                "f"
            ])
            .is_err()
        );
        assert!(Cli::try_parse_from(["apollo", "revoke", "--undo", "--id", "x"]).is_ok());
        assert!(
            Cli::try_parse_from([
                "apollo",
                "token",
                "--subject",
                "s",
                "--no-db-check",
                "--config",
                "c.toml"
            ])
            .is_ok()
        );
    }

    #[test]
    fn parses_status() {
        assert!(Cli::try_parse_from(["apollo", "status"]).is_ok());
        assert!(Cli::try_parse_from(["apollo", "status", "--json"]).is_ok());
        assert!(Cli::try_parse_from(["apollo", "status", "--watch"]).is_ok());
        assert!(Cli::try_parse_from(["apollo", "status", "-w", "-n", "0.5"]).is_ok());
        assert!(Cli::try_parse_from(["apollo", "status", "-w", "--json"]).is_ok());
        // An interval only makes sense with --watch, and must be sane.
        assert!(Cli::try_parse_from(["apollo", "status", "-n", "2"]).is_err());
        assert!(Cli::try_parse_from(["apollo", "status", "-w", "-n", "0"]).is_err());
        assert!(Cli::try_parse_from(["apollo", "status", "-w", "-n", "soon"]).is_err());
    }

    #[test]
    fn parses_drain() {
        assert!(Cli::try_parse_from(["apollo", "drain"]).is_ok());
        assert!(Cli::try_parse_from(["apollo", "drain", "--undo"]).is_ok());
        assert!(Cli::try_parse_from(["apollo", "drain", "now"]).is_err());
    }

    #[test]
    fn config_requires_a_subcommand() {
        assert!(Cli::try_parse_from(["apollo", "config"]).is_err());
    }
}
