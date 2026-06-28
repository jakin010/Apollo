//! `apollo keygen` / `apollo token` — mint the PASETO v4 signing keypair and the
//! API tokens clients present to an auth-enabled server.
//!
//! `keygen` prints a fresh Ed25519 keypair as PASERK strings: put the public key
//! in `[auth].public_key` and keep the secret key safe. `token` signs a token
//! with that secret key; the printed string is the API key a client sends in the
//! `authorization` metadata.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Args;
use pasetors::claims::Claims;
use pasetors::keys::{AsymmetricKeyPair, AsymmetricSecretKey, Generate};
use pasetors::paserk::FormatAsPaserk;
use pasetors::version4::V4;

#[derive(Args)]
pub struct KeygenArgs {}

/// Generate a PASETO v4 signing keypair and print both keys as PASERK.
pub fn run_keygen(_args: KeygenArgs) -> Result<()> {
    let kp =
        AsymmetricKeyPair::<V4>::generate().map_err(|e| anyhow!("generating keypair: {e}"))?;

    let mut public = String::new();
    kp.public
        .fmt(&mut public)
        .map_err(|e| anyhow!("encoding public key: {e}"))?;
    let mut secret = String::new();
    kp.secret
        .fmt(&mut secret)
        .map_err(|e| anyhow!("encoding secret key: {e}"))?;

    println!("# PASETO v4 signing keypair");
    println!("# Put the PUBLIC key in apollo's config under [auth].public_key.");
    println!("# Keep the SECRET key safe — it mints API tokens via `apollo token`.");
    println!();
    println!("public_key = \"{public}\"");
    println!("secret_key = \"{secret}\"");
    Ok(())
}

#[derive(Args)]
pub struct TokenArgs {
    /// Subject — a label identifying who/what the token is for (stored as `sub`).
    #[arg(long)]
    subject: String,
    /// Token lifetime, e.g. `30d`, `12h`, `90m`, `3600s`. Omit for a non-expiring
    /// key (revocable only by rotating the keypair).
    #[arg(long)]
    expires: Option<String>,
    /// Path to a file holding the PASERK secret key (`k4.secret.…`). If omitted,
    /// the key is read from the `APOLLO_SECRET_KEY` environment variable.
    #[arg(long)]
    secret_key_file: Option<String>,
}

/// Mint an API token signed with the secret key and print it to stdout.
pub fn run_token(args: TokenArgs) -> Result<()> {
    let paserk = match &args.secret_key_file {
        Some(path) => std::fs::read_to_string(path)
            .with_context(|| format!("reading secret key file '{path}'"))?,
        None => std::env::var("APOLLO_SECRET_KEY")
            .map_err(|_| anyhow!("provide --secret-key-file or set APOLLO_SECRET_KEY"))?,
    };
    let secret = AsymmetricSecretKey::<V4>::try_from(paserk.trim())
        .map_err(|e| anyhow!("parsing secret key: {e}"))?;

    let mut claims = Claims::new().map_err(|e| anyhow!("building claims: {e}"))?;
    claims
        .subject(&args.subject)
        .map_err(|e| anyhow!("setting subject: {e}"))?;
    match &args.expires {
        Some(spec) => {
            let d = parse_duration(spec)?;
            claims
                .set_expires_in(&d)
                .map_err(|e| anyhow!("setting expiry: {e}"))?;
        }
        None => claims.non_expiring(),
    }

    let token = pasetors::public::sign(&secret, &claims, None, None)
        .map_err(|e| anyhow!("signing token: {e}"))?;
    println!("{token}");
    Ok(())
}

/// Parse a compact duration like `30d`, `12h`, `90m`, `45s`.
fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| anyhow!("duration '{s}' needs a unit suffix (s/m/h/d)"))?;
    let (num, unit) = s.split_at(split);
    let n: u64 = num
        .parse()
        .with_context(|| format!("invalid number in duration '{s}'"))?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86_400,
        other => return Err(anyhow!("unknown duration unit '{other}' (use s/m/h/d)")),
    };
    Ok(Duration::from_secs(secs))
}
