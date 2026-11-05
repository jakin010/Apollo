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
//!
//! Instances sharing one database (SurrealDB) share one public key, stored
//! there: the first to start publishes its own (its secret key stays on that
//! host), and the others verify with it. `token` refuses to sign with a secret
//! key whose public key isn't the shared one, `revoke` also writes the
//! database, which every instance reads, and `publish-key` stores (or with
//! `--replace` rotates) the shared key.

use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::{ArgGroup, Args};
use pasetors::Public;
use pasetors::claims::Claims;
use pasetors::keys::{AsymmetricKeyPair, AsymmetricPublicKey, AsymmetricSecretKey, Generate};
use pasetors::paserk::FormatAsPaserk;
use pasetors::token::UntrustedToken;
use pasetors::version4::V4;

use apollo_config::{AuthConfig, Config};
use apollo_server::Revocation;
use apollo_storage::{SharedKey, Storage};

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
    /// Config file whose `[database]` to check the key against (default: the
    /// standard config path, skipped when absent).
    #[arg(long)]
    config: Option<PathBuf>,
    /// Don't check that the key is the one instances sharing the database
    /// verify with.
    #[arg(long)]
    no_db_check: bool,
}

/// Mint an API token signed with the secret key and print it to stdout.
pub fn run_token(args: TokenArgs) -> Result<()> {
    let secret = load_secret(args.secret_key_file.as_deref())?;
    if !args.no_db_check
        && let Some(config) = load_config(args.config.as_deref())?
    {
        let public = public_of(&secret)?;
        let stored = with_shared_db(&config, |db| async move { Ok(db.auth_key().await?) })
            .context(
                "checking the key against the shared database; pass --no-db-check to mint \
                 without checking",
            )?;
        if let Some(stored) = stored {
            minting_key_matches(&public, stored.as_ref())?;
        }
    }

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
    /// config path; falls back to the state directory). Its `[database]`, when
    /// instances share it, gets the revocation too.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Take the revocation back: remove it from the file (and the database).
    #[arg(long)]
    undo: bool,
}

/// Append a revocation entry to the revocation file that running servers watch,
/// and store it in the database instances share, if they do.
pub fn run_revoke(args: RevokeArgs) -> Result<()> {
    let entry = match (args.id, args.token, args.subject) {
        (Some(id), _, _) => Revocation::Id(id.trim().to_string()),
        (_, Some(token), _) => Revocation::Id(token_id_of(&token)?),
        (_, _, Some(subject)) => Revocation::Subject(subject.trim().to_string()),
        _ => unreachable!("clap requires one target"),
    };
    let config = load_config(args.config.as_deref())?;
    let auth = config.as_ref().map(|c| c.auth.clone()).unwrap_or_default();
    let path = revocation_file(&auth)?;

    if args.undo {
        unrevoke_in_file(&path, &entry)?;
    } else {
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
    }
    let done = if args.undo { "un-revoked" } else { "revoked" };
    let in_db = match &config {
        Some(config) => {
            let entry = entry.clone();
            let undo = args.undo;
            with_shared_db(config, move |db| async move {
                if undo {
                    db.remove_revocation(&entry).await?;
                } else {
                    db.add_revocation(&entry).await?;
                }
                Ok(())
            })
        }
        None => Ok(None),
    };
    match in_db {
        Ok(Some(())) => println!(
            "{done} {entry} in {} and in the database (every instance sharing it applies it \
             within ~10s)",
            path.display()
        ),
        Ok(None) => println!(
            "{done} {entry} in {} (running servers apply it within ~10s)",
            path.display()
        ),
        Err(e) => {
            return Err(anyhow!(
                "{done} {entry} in {} but NOT in the database: {e:#}; other instances sharing \
                 it don't see it — run again",
                path.display()
            ));
        }
    }
    Ok(())
}

/// Remove `entry`'s lines from the revocation file at `path` (keeping
/// everything else), through a new file put in its place.
fn unrevoke_in_file(path: &Path, entry: &Revocation) -> Result<()> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(anyhow::Error::new(e).context(format!("reading {}", path.display()))),
    };
    let kept = without_entry(&text, entry);
    if kept == text {
        return Ok(());
    }
    let tmp = path.with_extension("tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&tmp)
        .with_context(|| format!("writing {}", tmp.display()))?;
    file.write_all(kept.as_bytes())
        .with_context(|| format!("writing {}", tmp.display()))?;
    drop(file);
    std::fs::rename(&tmp, path).with_context(|| format!("replacing {}", path.display()))
}

/// `text` without the lines that revoke `entry`.
fn without_entry(text: &str, entry: &Revocation) -> String {
    text.lines()
        .filter(|line| Revocation::parse_line(line).ok().flatten().as_ref() != Some(entry))
        .map(|line| format!("{line}\n"))
        .collect()
}

#[derive(Args)]
pub struct PublishKeyArgs {
    /// The public key (`k4.public.…`). Default: the public half of the secret
    /// key `apollo token` would use.
    #[arg(long)]
    public_key: Option<String>,
    /// File holding the secret key whose public half to publish (as for
    /// `apollo token`).
    #[arg(long, conflicts_with = "public_key")]
    secret_key_file: Option<String>,
    /// Replace the stored key (a rotation; restart every instance after).
    #[arg(long)]
    replace: bool,
    /// Config file whose `[database]` to use (default: the standard path).
    #[arg(long)]
    config: Option<PathBuf>,
}

/// Store a public key as the one instances sharing the database verify with.
pub fn run_publish_key(args: PublishKeyArgs) -> Result<()> {
    let public = match &args.public_key {
        Some(key) => {
            parse_public(key)?;
            key.clone()
        }
        None => public_of(&load_secret(args.secret_key_file.as_deref())?)?,
    };
    let config = match &args.config {
        Some(path) => apollo_config::load::load(Some(path))
            .with_context(|| format!("loading {}", path.display()))?,
        None => apollo_config::load::load(None).context("loading the config")?,
    };
    let node = super::start::node_id(&config).context("reading the node id")?;
    let replace = args.replace;
    let key = public.clone();
    let outcome = with_shared_db(&config, move |db| async move {
        publish(db.as_ref(), &key, &node, replace).await
    })?
    .ok_or_else(|| {
        anyhow!(
            "the database isn't shared (SQLite): servers use [auth].public_key or their local \
             keypair"
        )
    })?;
    match outcome {
        Published::Stored => println!("published {public} as the shared key"),
        Published::Already => println!("{public} is already the shared key"),
        Published::Replaced(old) => println!(
            "replaced the shared key (was {}); restart every instance to verify with it. Tokens \
             signed with the old key are rejected by each instance once it restarts; instances \
             with [auth].public_key set must have it changed too, or they refuse to start.",
            old.map_or("none".into(), |k| k.public_key)
        ),
    }
    Ok(())
}

/// What [`publish`] did.
#[derive(Debug, PartialEq, Eq)]
enum Published {
    Stored,
    Already,
    /// Replaced this key (if one was stored).
    Replaced(Option<SharedKey>),
}

/// Store `public` as the shared key (from `node`): only when none is stored,
/// or with `replace`, whatever is.
async fn publish(db: &dyn Storage, public: &str, node: &str, replace: bool) -> Result<Published> {
    let before = db.auth_key().await?;
    if before
        .as_ref()
        .is_some_and(|k| same_key(&k.public_key, public))
    {
        return Ok(Published::Already);
    }
    if replace {
        db.replace_auth_key(public, node).await?;
        return Ok(Published::Replaced(before));
    }
    let stored = db.publish_auth_key(public, node).await?;
    if same_key(&stored.public_key, public) {
        Ok(Published::Stored)
    } else {
        Err(anyhow!(
            "the database already holds a different key ({}, published by node {}); pass \
             --replace to rotate",
            stored.public_key,
            node_name(&stored)
        ))
    }
}

/// How the public key a server verifies tokens with was chosen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KeyChoice {
    /// This host's local keypair (made now, or not), on a database only this
    /// instance uses.
    Local { created: bool },
    /// `[auth].public_key`; `published`: stored as the shared key now.
    Configured { published: bool },
    /// This host's local keypair (made now, or not), published now as the
    /// shared key.
    Published { created: bool },
    /// This host's local keypair, which is the shared key already.
    SharedLocal,
    /// The shared key, published by `node`. `local`: this host's own keypair,
    /// if it has one, and whether it was made now.
    Database {
        node: Option<String>,
        local: Option<(PathBuf, bool)>,
    },
}

/// The public key a starting server verifies tokens with. On a database only
/// it uses (`shared` is `None`): `[auth].public_key`, else its local keypair
/// at `local_path` (made if missing). On a shared one, every instance uses
/// one key: the configured one, stored if none is (and refused if another
/// is), else the stored one, else this host's local key, which it publishes.
pub(crate) async fn resolve_public_key(
    auth: &AuthConfig,
    shared: Option<&dyn Storage>,
    node: &str,
    local_path: &Path,
) -> Result<(String, KeyChoice)> {
    let Some(db) = shared else {
        return match &auth.public_key {
            Some(key) => Ok((key.clone(), KeyChoice::Configured { published: false })),
            None => {
                let (key, created) = load_or_create_keypair_at(local_path)?;
                Ok((key, KeyChoice::Local { created }))
            }
        };
    };
    if let Some(configured) = &auth.public_key {
        parse_public(configured).context("[auth].public_key")?;
        let before = db
            .auth_key()
            .await
            .context("reading the shared public key")?;
        let was_empty = before.is_none();
        let stored = match before {
            Some(stored) => stored,
            None => db
                .publish_auth_key(configured, node)
                .await
                .context("publishing [auth].public_key")?,
        };
        parse_public(&stored.public_key).map_err(|_| invalid_stored(&stored))?;
        if same_key(&stored.public_key, configured) {
            let published = was_empty && stored.node.as_deref() == Some(node);
            return Ok((configured.clone(), KeyChoice::Configured { published }));
        }
        return Err(anyhow!(
            "[auth].public_key ({configured}) differs from the key instances sharing this \
             database verify with ({}, published by node {}). Instances on one database must use \
             one key: remove [auth].public_key to use the database's, or rotate the database's \
             key with `apollo publish-key --replace`.",
            stored.public_key,
            node_name(&stored)
        ));
    }
    if let Some(stored) = db
        .auth_key()
        .await
        .context("reading the shared public key")?
    {
        parse_public(&stored.public_key).map_err(|_| invalid_stored(&stored))?;
        let local = local_public_key_at(local_path)?;
        if local
            .as_deref()
            .is_some_and(|l| same_key(l, &stored.public_key))
        {
            return Ok((stored.public_key, KeyChoice::SharedLocal));
        }
        let local = local.map(|_| (local_path.to_path_buf(), false));
        return Ok((
            stored.public_key,
            KeyChoice::Database {
                node: stored.node,
                local,
            },
        ));
    }
    let (local, created) = load_or_create_keypair_at(local_path)?;
    let stored = db
        .publish_auth_key(&local, node)
        .await
        .context("publishing this host's public key")?;
    parse_public(&stored.public_key).map_err(|_| invalid_stored(&stored))?;
    if same_key(&stored.public_key, &local) {
        Ok((local, KeyChoice::Published { created }))
    } else {
        // Another instance published its key first.
        Ok((
            stored.public_key,
            KeyChoice::Database {
                node: stored.node,
                local: Some((local_path.to_path_buf(), created)),
            },
        ))
    }
}

/// The shared database's revocations, for the server to poll.
pub(crate) struct DbRevocations(pub Arc<dyn Storage>);

#[async_trait::async_trait]
impl apollo_server::RevocationSource for DbRevocations {
    async fn revocations(&self) -> Result<Vec<Revocation>, String> {
        self.0.revocations().await.map_err(|e| e.to_string())
    }
}

/// Whether a token signed with the secret key whose public half is `public`
/// is accepted by instances verifying with `stored`. With none stored yet,
/// the first instance to start publishes its own key: a note says so.
fn minting_key_matches(public: &str, stored: Option<&SharedKey>) -> Result<()> {
    match stored {
        Some(stored) if same_key(&stored.public_key, public) => Ok(()),
        Some(stored) => Err(anyhow!(
            "this secret key's public key ({public}) is not the key instances sharing the \
             database verify with ({}, published by node {}); a token signed with it would be \
             rejected by every instance. Mint on node {} (or pass that key with \
             --secret-key-file / APOLLO_SECRET_KEY), or rotate with `apollo publish-key \
             --replace`.",
            stored.public_key,
            node_name(stored),
            node_name(stored)
        )),
        None => {
            eprintln!(
                "note: no instance sharing the database has published a key yet; the first to \
                 start publishes its own: this token works only if that is this key"
            );
            Ok(())
        }
    }
}

/// `work` on the configured database, if instances share it (SurrealDB;
/// `None` for SQLite), each step within `[database].timeout`.
fn with_shared_db<T, F, Fut>(config: &Config, work: F) -> Result<Option<T>>
where
    F: FnOnce(Arc<dyn Storage>) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("starting a runtime")?;
    let secs = config.database.timeout;
    runtime.block_on(async {
        let Some(db) = apollo_storage::connect_shared(&config.database)
            .await
            .context("connecting to the database")?
        else {
            return Ok(None);
        };
        within_db_timeout(secs, work(db)).await.map(Some)
    })
}

/// `work`, failing after `secs` seconds (`0`: no limit), as `[database].timeout`
/// says.
pub(crate) async fn within_db_timeout<T>(
    secs: u32,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    match secs {
        0 => work.await,
        secs => {
            let limit = Duration::from_secs(secs.into());
            tokio::time::timeout(limit, work)
                .await
                .map_err(|_| anyhow!("the database didn't answer within {limit:?}"))?
        }
    }
}

/// The config at `path`, which must load, or at the standard path, if there
/// is one.
fn load_config(path: Option<&Path>) -> Result<Option<Config>> {
    match path {
        Some(path) => apollo_config::load::load(Some(path))
            .map(Some)
            .with_context(|| format!("loading {}", path.display())),
        None => Ok(apollo_config::load::load(None).ok()),
    }
}

/// The secret key `apollo token` signs with: from `file`, else
/// `APOLLO_SECRET_KEY`, else the local keypair.
fn load_secret(file: Option<&str>) -> Result<AsymmetricSecretKey<V4>> {
    match file {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading secret key file '{path}'"))?;
            parse_secret(&text)
        }
        None => match std::env::var("APOLLO_SECRET_KEY") {
            Ok(paserk) => parse_secret(&paserk),
            Err(_) => {
                let path = local_key_path()?;
                let text = std::fs::read_to_string(&path).map_err(|_| {
                    anyhow!(
                        "no secret key: pass --secret-key-file, set APOLLO_SECRET_KEY, or start \
                         the server once without [auth].public_key so it creates {} (with \
                         instances sharing a database, only the one that published the shared \
                         key has it: mint there)",
                        path.display()
                    )
                })?;
                parse_secret(&text)
            }
        },
    }
}

fn public_of(secret: &AsymmetricSecretKey<V4>) -> Result<String> {
    let public = AsymmetricPublicKey::<V4>::try_from(secret)
        .map_err(|e| anyhow!("deriving public key: {e}"))?;
    paserk(&public)
}

fn parse_public(key: &str) -> Result<AsymmetricPublicKey<V4>> {
    AsymmetricPublicKey::<V4>::try_from(key).map_err(|e| anyhow!("invalid public key: {e}"))
}

/// Whether two PASERK public keys are the same key.
fn same_key(a: &str, b: &str) -> bool {
    match (parse_public(a), parse_public(b)) {
        (Ok(a), Ok(b)) => a.as_bytes() == b.as_bytes(),
        _ => a.trim() == b.trim(),
    }
}

fn node_name(key: &SharedKey) -> &str {
    key.node.as_deref().unwrap_or("(unknown)")
}

fn invalid_stored(stored: &SharedKey) -> anyhow::Error {
    anyhow!(
        "the database's shared public key is invalid ({}, published by node {}); replace it \
         with `apollo publish-key --replace`",
        stored.public_key,
        node_name(stored)
    )
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

/// The public key of the keypair at `path`, if there is one (never makes one).
pub(crate) fn local_public_key_at(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            let secret =
                parse_secret(&text).with_context(|| format!("parsing {}", path.display()))?;
            public_of(&secret).map(Some)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(anyhow::Error::new(e).context(format!("reading {}", path.display()))),
    }
}

/// The public key of the keypair at `path`, made (owner-only) if missing, and
/// whether it was made now.
pub(crate) fn load_or_create_keypair_at(path: &Path) -> Result<(String, bool)> {
    if !path.exists() {
        let (public, secret) = generate_keypair()?;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(path) {
            Ok(mut file) => {
                file.write_all(keypair_file(&public, &secret).as_bytes())
                    .with_context(|| format!("writing {}", path.display()))?;
                return Ok((public, true));
            }
            // Another process created it first: use theirs.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(anyhow::Error::new(e).context(format!("creating {}", path.display())));
            }
        }
    }
    let public = local_public_key_at(path)?
        .ok_or_else(|| anyhow!("{} vanished as it was read", path.display()))?;
    Ok((public, false))
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

    use apollo_config::SqliteConfig;
    use apollo_storage::SqliteStorage;

    /// A migrated SQLite database in `dir`, used as a shared one (its auth
    /// methods are those of every backend).
    async fn db_in(dir: &Path) -> SqliteStorage {
        let store = SqliteStorage::connect(&SqliteConfig {
            path: dir.join("db.sqlite").to_string_lossy().into_owned(),
            ..Default::default()
        })
        .await
        .unwrap();
        store.migrate().await.unwrap();
        store
    }

    fn configured(key: Option<&str>) -> AuthConfig {
        AuthConfig {
            public_key: key.map(str::to_string),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn alone_the_key_is_the_configured_or_local_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.key");
        let (public, _) = generate_keypair().unwrap();
        let got = resolve_public_key(&configured(Some(&public)), None, "n", &path)
            .await
            .unwrap();
        assert_eq!(got, (public, KeyChoice::Configured { published: false }));
        assert!(!path.exists());

        let (key, choice) = resolve_public_key(&configured(None), None, "n", &path)
            .await
            .unwrap();
        assert_eq!(choice, KeyChoice::Local { created: true });
        assert_eq!(local_public_key_at(&path).unwrap(), Some(key.clone()));
        let again = resolve_public_key(&configured(None), None, "n", &path)
            .await
            .unwrap();
        assert_eq!(again, (key, KeyChoice::Local { created: false }));
    }

    #[tokio::test]
    async fn the_first_instance_publishes_its_local_key() {
        let dir = tempfile::tempdir().unwrap();
        let db = db_in(dir.path()).await;
        let path = dir.path().join("auth.key");
        let (key, choice) = resolve_public_key(&configured(None), Some(&db), "n1", &path)
            .await
            .unwrap();
        assert_eq!(choice, KeyChoice::Published { created: true });
        let stored = db.auth_key().await.unwrap().unwrap();
        assert_eq!(
            (stored.public_key, stored.node),
            (key.clone(), Some("n1".into()))
        );
        // Its next start: its own key, which is the shared one.
        let again = resolve_public_key(&configured(None), Some(&db), "n1", &path)
            .await
            .unwrap();
        assert_eq!(again, (key, KeyChoice::SharedLocal));
    }

    #[tokio::test]
    async fn others_verify_with_the_shared_key() {
        let dir = tempfile::tempdir().unwrap();
        let db = db_in(dir.path()).await;
        let (shared, _) = generate_keypair().unwrap();
        db.publish_auth_key(&shared, "n1").await.unwrap();

        // No local keypair: none is made.
        let path = dir.path().join("auth.key");
        let got = resolve_public_key(&configured(None), Some(&db), "n2", &path)
            .await
            .unwrap();
        assert_eq!(
            got,
            (
                shared.clone(),
                KeyChoice::Database {
                    node: Some("n1".into()),
                    local: None
                }
            )
        );
        assert!(!path.exists());

        // An older local keypair of its own: not used.
        let (mine, _) = load_or_create_keypair_at(&path).unwrap();
        assert_ne!(mine, shared);
        let got = resolve_public_key(&configured(None), Some(&db), "n2", &path)
            .await
            .unwrap();
        assert_eq!(
            got,
            (
                shared,
                KeyChoice::Database {
                    node: Some("n1".into()),
                    local: Some((path, false))
                }
            )
        );
    }

    #[tokio::test]
    async fn a_configured_key_must_be_the_shared_one() {
        let dir = tempfile::tempdir().unwrap();
        let db = db_in(dir.path()).await;
        let path = dir.path().join("auth.key");
        let (key, _) = generate_keypair().unwrap();
        // On an empty database it is published.
        let got = resolve_public_key(&configured(Some(&key)), Some(&db), "n1", &path)
            .await
            .unwrap();
        assert_eq!(
            got,
            (key.clone(), KeyChoice::Configured { published: true })
        );
        let got = resolve_public_key(&configured(Some(&key)), Some(&db), "n2", &path)
            .await
            .unwrap();
        assert_eq!(got, (key, KeyChoice::Configured { published: false }));
        // Another one refuses the start.
        let (other, _) = generate_keypair().unwrap();
        let err = resolve_public_key(&configured(Some(&other)), Some(&db), "n2", &path)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("differs") && err.contains("publish-key --replace"),
            "{err}"
        );
        assert!(!path.exists());

        // A stored key that isn't one.
        db.replace_auth_key("garbage", "x").await.unwrap();
        let err = resolve_public_key(&configured(None), Some(&db), "n2", &path)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid"), "{err}");
    }

    #[test]
    fn minting_needs_the_shared_key() {
        let (public, _) = generate_keypair().unwrap();
        let (other, _) = generate_keypair().unwrap();
        let stored = |key: &str| SharedKey {
            public_key: key.into(),
            node: Some("n1".into()),
        };
        assert!(minting_key_matches(&public, Some(&stored(&public))).is_ok());
        assert!(minting_key_matches(&public, None).is_ok());
        let err = minting_key_matches(&public, Some(&stored(&other)))
            .unwrap_err()
            .to_string();
        assert!(err.contains("node n1"), "{err}");
    }

    #[test]
    fn undoing_a_revocation_keeps_the_rest_of_the_file() {
        let text = "# revoked\nid a\nsubject bob\n  id a  \nid b\n";
        assert_eq!(
            without_entry(text, &Revocation::Id("a".into())),
            "# revoked\nsubject bob\nid b\n"
        );
        assert_eq!(
            without_entry(text, &Revocation::Subject("a".into())),
            text,
            "a subject named like an id is another entry"
        );
    }

    /// A token for `subject`, with id `jti`, signed with the keypair at `path`.
    fn mint(path: &Path, subject: &str, jti: &str) -> String {
        let secret = parse_secret(&std::fs::read_to_string(path).unwrap()).unwrap();
        let mut claims = Claims::new().unwrap();
        claims.subject(subject).unwrap();
        claims.token_identifier(jti).unwrap();
        pasetors::public::sign(&secret, &claims, None, None).unwrap()
    }

    fn accepts(auth: &mut apollo_server::AuthInterceptor, token: &str) -> bool {
        use tonic::service::Interceptor;
        let mut request = tonic::Request::new(());
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        auth.call(request).is_ok()
    }

    /// Two instances on one SurrealDB, each with a local keypair of its own,
    /// starting together: they settle on one key, accept the same tokens, and
    /// apply each other's revocations.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn instances_sharing_a_database_accept_the_same_tokens() {
        let Some(scratch) = apollo_storage::testing::SurrealScratch::create().await else {
            return;
        };
        let config = scratch.config();
        let outcome = tokio::spawn(async move {
            let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let paths = (
                dirs.0.path().join("auth.key"),
                dirs.1.path().join("auth.key"),
            );
            load_or_create_keypair_at(&paths.0).unwrap();
            load_or_create_keypair_at(&paths.1).unwrap();
            let connect = || async {
                let store = apollo_storage::SurrealStorage::connect(&config)
                    .await
                    .unwrap();
                Arc::new(store) as Arc<dyn Storage>
            };
            let (db_a, db_b) = (connect().await, connect().await);
            let auth = configured(None);
            let (a, b) = tokio::join!(
                resolve_public_key(&auth, Some(db_a.as_ref()), "a", &paths.0),
                resolve_public_key(&auth, Some(db_b.as_ref()), "b", &paths.1),
            );
            let ((key_a, choice_a), (key_b, choice_b)) = (a.unwrap(), b.unwrap());
            assert_eq!(key_a, key_b, "one key");
            let (winner, loser, lost) = match (&choice_a, &choice_b) {
                (KeyChoice::Published { .. }, lost) => (&paths.0, &paths.1, lost),
                (lost, KeyChoice::Published { .. }) => (&paths.1, &paths.0, lost),
                other => panic!("neither published: {other:?}"),
            };
            assert!(
                matches!(
                    lost,
                    KeyChoice::Database {
                        local: Some((_, false)),
                        ..
                    }
                ),
                "{lost:?}"
            );

            let verifier = |db: Arc<dyn Storage>| {
                apollo_server::AuthInterceptor::new(apollo_server::AuthSettings {
                    public_key: key_a.clone(),
                    admin_subjects: Vec::new(),
                    revocation_file: None,
                    shared_revocations: Some(Arc::new(DbRevocations(db))),
                })
                .unwrap()
            };
            let mut verifiers = [verifier(db_a.clone()), verifier(db_b.clone())];
            for v in &verifiers {
                v.load_shared_revocations().await.unwrap();
            }
            let good = mint(winner, "alice", "t1");
            let bad = mint(loser, "alice", "t2");
            for v in &mut verifiers {
                assert!(accepts(v, &good), "the shared key's token");
                assert!(!accepts(v, &bad), "the other local key's token");
            }

            // Revoked through one instance's database connection: both apply it.
            db_b.add_revocation(&Revocation::Id("t1".into()))
                .await
                .unwrap();
            for v in &mut verifiers {
                v.poll_once(Duration::from_secs(5)).await;
                assert!(!accepts(v, &good), "revoked");
            }

            // A third, configured with another key, refuses to start.
            let (other, _) = generate_keypair().unwrap();
            let third = dirs.0.path().join("third.key");
            assert!(
                resolve_public_key(&configured(Some(&other)), Some(db_a.as_ref()), "c", &third)
                    .await
                    .is_err()
            );
        })
        .await;
        scratch.remove().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic.into_panic());
        }
    }

    #[tokio::test]
    async fn publishing_replaces_only_when_told() {
        let dir = tempfile::tempdir().unwrap();
        let db = db_in(dir.path()).await;
        let (a, _) = generate_keypair().unwrap();
        let (b, _) = generate_keypair().unwrap();
        assert_eq!(
            publish(&db, &a, "n1", false).await.unwrap(),
            Published::Stored
        );
        assert_eq!(
            publish(&db, &a, "n2", false).await.unwrap(),
            Published::Already
        );
        let err = publish(&db, &b, "n2", false).await.unwrap_err().to_string();
        assert!(err.contains("--replace"), "{err}");
        let replaced = publish(&db, &b, "n2", true).await.unwrap();
        assert_eq!(
            replaced,
            Published::Replaced(Some(SharedKey {
                public_key: a,
                node: Some("n1".into())
            }))
        );
        assert_eq!(db.auth_key().await.unwrap().unwrap().public_key, b);
    }

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
