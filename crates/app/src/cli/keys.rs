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
//!
//! Every minted token carries a random id (`jti`), printed to stderr. `revoke`
//! appends that id — or a whole subject — to the revocation file, which running
//! servers re-read within seconds; no keypair rotation needed.

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::{ArgGroup, Args};
use pasetors::Public;
use pasetors::claims::Claims;
use pasetors::keys::{AsymmetricKeyPair, AsymmetricPublicKey, AsymmetricSecretKey, Generate};
use pasetors::paserk::FormatAsPaserk;
use pasetors::token::UntrustedToken;
use pasetors::version4::V4;

use apollo_config::AuthConfig;
use apollo_server::Revocation;

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

    let token_id = uuid::Uuid::new_v4().simple().to_string();
    let mut claims = Claims::new().map_err(|e| anyhow!("building claims: {e}"))?;
    claims
        .subject(&args.subject)
        .map_err(|e| anyhow!("setting subject: {e}"))?;
    claims
        .token_identifier(&token_id)
        .map_err(|e| anyhow!("setting token id: {e}"))?;
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
    // The id goes to stderr so `TOKEN=$(apollo token …)` captures only the token.
    eprintln!("token id: {token_id} (revoke with `apollo revoke --id {token_id}`)");
    println!("{token}");
    Ok(())
}

#[derive(Args)]
#[command(group(ArgGroup::new("target").required(true).args(["id", "token", "subject"])))]
pub struct RevokeArgs {
    /// Token id, as printed by `apollo token`.
    #[arg(long)]
    id: Option<String>,
    /// The token itself (its id is read from it).
    #[arg(long)]
    token: Option<String>,
    /// Revoke every token issued to this subject, including tokens minted before
    /// token ids existed.
    #[arg(long)]
    subject: Option<String>,
    /// Config file whose `[auth].revocation_file` to use (default: the standard
    /// config path; falls back to the state directory).
    #[arg(long)]
    config: Option<PathBuf>,
}

/// Append a revocation entry to the revocation file that running servers watch.
pub fn run_revoke(args: RevokeArgs) -> Result<()> {
    let entry = match (args.id, args.token, args.subject) {
        (Some(id), _, _) => Revocation::Id(id.trim().to_string()),
        (_, Some(token), _) => Revocation::Id(token_id_of(&token)?),
        (_, _, Some(subject)) => Revocation::Subject(subject.trim().to_string()),
        _ => unreachable!("clap requires one target"),
    };
    // An explicitly named config must load; the default path may not exist.
    let auth = match &args.config {
        Some(path) => {
            apollo_config::load::load(Some(path))
                .with_context(|| format!("loading {}", path.display()))?
                .auth
        }
        None => apollo_config::load::load(None)
            .map(|c| c.auth)
            .unwrap_or_default(),
    };
    let path = revocation_file(&auth)?;

    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    writeln!(file, "{entry}").with_context(|| format!("writing {}", path.display()))?;
    println!(
        "revoked {entry} in {} (running servers apply it within ~10s)",
        path.display()
    );
    Ok(())
}

/// The revocation file: `[auth].revocation_file`, else `revoked_tokens` in the
/// state directory.
pub fn revocation_file(auth: &AuthConfig) -> Result<PathBuf> {
    match &auth.revocation_file {
        Some(path) => Ok(PathBuf::from(path)),
        None => Ok(crate::daemon::state_dir()?.join("revoked_tokens")),
    }
}

/// Read the id (`jti`) out of a token without verifying it (revoking is safe
/// either way: at worst an unknown id is listed).
fn token_id_of(token: &str) -> Result<String> {
    let raw = token.trim();
    let raw = raw.strip_prefix("Bearer ").unwrap_or(raw);
    let untrusted = UntrustedToken::<Public, V4>::try_from(raw)
        .map_err(|_| anyhow!("not a v4.public token"))?;
    let claims = Claims::from_string(
        std::str::from_utf8(untrusted.untrusted_payload()).context("token payload is not UTF-8")?,
    )
    .map_err(|e| anyhow!("reading token claims: {e}"))?;
    match claims.get_claim("jti").and_then(|v| v.as_str()) {
        Some(id) => Ok(id.to_string()),
        None => {
            let subject = claims
                .get_claim("sub")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            Err(anyhow!(
                "this token has no id (minted before token ids existed); revoke its subject \
                 instead: apollo revoke --subject {subject}"
            ))
        }
    }
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
    fn token_id_is_read_from_the_token() {
        let (_, secret) = generate_keypair().unwrap();
        let secret = parse_secret(&secret).unwrap();
        let mint = |id: Option<&str>| {
            let mut claims = Claims::new().unwrap();
            claims.subject("ci").unwrap();
            if let Some(id) = id {
                claims.token_identifier(id).unwrap();
            }
            pasetors::public::sign(&secret, &claims, None, None).unwrap()
        };
        assert_eq!(token_id_of(&mint(Some("abc123"))).unwrap(), "abc123");
        assert_eq!(
            token_id_of(&format!("Bearer {}", mint(Some("x")))).unwrap(),
            "x"
        );
        let err = token_id_of(&mint(None)).unwrap_err().to_string();
        assert!(err.contains("--subject ci"), "{err}");
        assert!(token_id_of("garbage").is_err());
    }

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
