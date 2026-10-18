//! `apollo keygen` / `apollo token` — mint the PASETO v4 signing keypair and the
//! API tokens clients present to the server (authentication is on by default).
//!
//! `keygen` prints a fresh Ed25519 keypair as PASERK strings: put the public key
//! in `[auth].public_key` and keep the secret key safe. `token` signs a token
//! with that secret key; the printed string is the API key a client sends in the
//! `authorization` metadata.
//!
//! When `[auth].public_key` is not configured, the server instead uses a local
//! keypair stored in `<state dir>/auth.key` (owner-only, same format `keygen`
//! prints), created on first start. `token` reads that file when no other secret
//! key is given.

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::Args;
use pasetors::claims::Claims;
use pasetors::keys::{AsymmetricKeyPair, AsymmetricPublicKey, AsymmetricSecretKey, Generate};
use pasetors::paserk::FormatAsPaserk;
use pasetors::version4::V4;

#[derive(Args)]
pub struct KeygenArgs {}

/// Generate a PASETO v4 signing keypair and print both keys as PASERK.
pub fn run_keygen(_args: KeygenArgs) -> Result<()> {
    let (public, secret) = generate_keypair()?;
    print!("{}", keypair_file(&public, &secret));
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
    /// File holding the secret key: a bare PASERK (`k4.secret.…`) or the output of
    /// `apollo keygen`. Defaults to `APOLLO_SECRET_KEY`, then to the server's
    /// local keypair file.
    #[arg(long)]
    secret_key_file: Option<String>,
}

/// Mint an API token signed with the secret key and print it to stdout.
pub fn run_token(args: TokenArgs) -> Result<()> {
    let secret = match &args.secret_key_file {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading secret key file '{path}'"))?;
            parse_secret(&text)?
        }
        None => match std::env::var("APOLLO_SECRET_KEY") {
            Ok(paserk) => parse_secret(&paserk)?,
            Err(_) => {
                let path = local_key_path()?;
                let text = std::fs::read_to_string(&path).map_err(|_| {
                    anyhow!(
                        "no secret key: pass --secret-key-file, set APOLLO_SECRET_KEY, or start \
                         the server once without [auth].public_key so it creates {}",
                        path.display()
                    )
                })?;
                parse_secret(&text)?
            }
        },
    };

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

/// Path of the server's local signing keypair (used when `[auth].public_key` is
/// not configured).
pub fn local_key_path() -> Result<PathBuf> {
    Ok(crate::daemon::state_dir()?.join("auth.key"))
}

/// The public key of the local keypair, creating the keypair (owner-only) if it
/// does not exist yet. Returns the PASERK public key, the file path, and whether
/// it was just created.
pub fn load_or_create_local_keypair() -> Result<(String, PathBuf, bool)> {
    let path = local_key_path()?;
    if !path.exists() {
        let (public, secret) = generate_keypair()?;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(mut file) => {
                file.write_all(keypair_file(&public, &secret).as_bytes())
                    .with_context(|| format!("writing {}", path.display()))?;
                return Ok((public, path, true));
            }
            // Another process created it first: use theirs.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(anyhow::Error::new(e).context(format!("creating {}", path.display())));
            }
        }
    }
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let secret = parse_secret(&text).with_context(|| format!("parsing {}", path.display()))?;
    let public = AsymmetricPublicKey::<V4>::try_from(&secret)
        .map_err(|e| anyhow!("deriving public key: {e}"))?;
    Ok((paserk(&public)?, path, false))
}

fn generate_keypair() -> Result<(String, String)> {
    let kp = AsymmetricKeyPair::<V4>::generate().map_err(|e| anyhow!("generating keypair: {e}"))?;
    Ok((paserk(&kp.public)?, paserk(&kp.secret)?))
}

fn paserk(key: &impl FormatAsPaserk) -> Result<String> {
    let mut out = String::new();
    key.fmt(&mut out)
        .map_err(|e| anyhow!("encoding key as PASERK: {e}"))?;
    Ok(out)
}

/// The keypair as `apollo keygen` prints it (also the local key file format).
fn keypair_file(public: &str, secret: &str) -> String {
    format!(
        "# PASETO v4 signing keypair\n\
         # Put the PUBLIC key in apollo's config under [auth].public_key.\n\
         # Keep the SECRET key safe — it mints API tokens via `apollo token`.\n\
         \n\
         public_key = \"{public}\"\n\
         secret_key = \"{secret}\"\n"
    )
}

/// Parse a secret key from a bare PASERK string or from `keygen`-style text
/// containing a `secret_key = "…"` line.
fn parse_secret(text: &str) -> Result<AsymmetricSecretKey<V4>> {
    let raw = text
        .lines()
        .map(str::trim)
        .find_map(|line| {
            let rest = line.strip_prefix("secret_key")?.trim_start();
            Some(rest.strip_prefix('=')?.trim().trim_matches('"'))
        })
        .unwrap_or_else(|| text.trim());
    AsymmetricSecretKey::<V4>::try_from(raw).map_err(|e| anyhow!("parsing secret key: {e}"))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_parses_from_bare_or_keygen_output() {
        let (public, secret) = generate_keypair().unwrap();
        let from_bare = parse_secret(&format!("  {secret}\n")).unwrap();
        let from_file = parse_secret(&keypair_file(&public, &secret)).unwrap();
        assert_eq!(paserk(&from_bare).unwrap(), secret);
        assert_eq!(paserk(&from_file).unwrap(), secret);
        // The public key is recoverable from the secret alone.
        let derived = AsymmetricPublicKey::<V4>::try_from(&from_file).unwrap();
        assert_eq!(paserk(&derived).unwrap(), public);
        assert!(parse_secret("not a key").is_err());
    }
}
