//! PASETO v4 (public / asymmetric) authentication for the `Inference` service.
//!
//! Tokens are minted out-of-band by the `apollo token` CLI, which signs a set of
//! claims with an Ed25519 **secret** key. The server holds only the matching
//! **public** key and verifies the token on every `Inference` RPC via a tonic
//! interceptor. Health and reflection are deliberately left unauthenticated so
//! load balancers and tooling can probe them.
//!
//! Clients present the token in the `authorization` metadata, with or without a
//! `Bearer ` prefix. A verified token must carry a subject (`sub`); the
//! interceptor attaches it to the request as a [`Caller`], which the service uses
//! to scope tasks to the subject that created them.
//!
//! Individual tokens are revoked without rotating the keypair through a
//! revocation file (see [`Revocation`]): one entry per line, either a token id
//! (`jti`, printed by `apollo token`) or a whole subject. Running servers re-read
//! the file when it changes ([`AuthInterceptor::watch_revocations`]).

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use pasetors::Public;
use pasetors::claims::ClaimsValidationRules;
use pasetors::keys::AsymmetricPublicKey;
use pasetors::token::UntrustedToken;
use pasetors::version4::V4;
use tonic::service::Interceptor;
use tonic::{Request, Status};

/// Failure parsing the configured PASERK public key.
#[derive(Debug, thiserror::Error)]
#[error("invalid PASETO public key: {0}")]
pub struct KeyError(String);

/// The authenticated caller of an `Inference` RPC, attached to the request's
/// extensions by [`AuthInterceptor`]. Absent when authentication is disabled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    /// The token's subject (`sub` claim).
    pub subject: String,
    /// Whether the subject is listed in `[auth].admin_subjects` (may access every
    /// task).
    pub admin: bool,
}

/// One entry of the revocation file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Revocation {
    /// A single token, by its id (`jti` claim).
    Id(String),
    /// Every token issued to a subject (`sub` claim), including tokens minted
    /// before token ids existed.
    Subject(String),
}

impl fmt::Display for Revocation {
    /// The line written to the revocation file.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Revocation::Id(id) => write!(f, "id {id}"),
            Revocation::Subject(subject) => write!(f, "subject {subject}"),
        }
    }
}

impl Revocation {
    /// Parse one line (`id <jti>` or `subject <name>`). Blank lines and `#`
    /// comments yield `Ok(None)`.
    pub fn parse_line(line: &str) -> Result<Option<Self>, String> {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return Ok(None);
        }
        match line.split_once(char::is_whitespace) {
            Some(("id", v)) if !v.trim().is_empty() => Ok(Some(Revocation::Id(v.trim().into()))),
            Some(("subject", v)) if !v.trim().is_empty() => {
                Ok(Some(Revocation::Subject(v.trim().into())))
            }
            _ => Err(format!("unrecognized revocation entry {line:?}")),
        }
    }
}

/// The revoked token ids and subjects currently in force.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Revocations {
    ids: HashSet<String>,
    subjects: HashSet<String>,
}

impl Revocations {
    fn parse(text: &str) -> (Self, Vec<String>) {
        let mut out = Revocations::default();
        let mut problems = Vec::new();
        for (n, line) in text.lines().enumerate() {
            match Revocation::parse_line(line) {
                Ok(Some(Revocation::Id(id))) => {
                    out.ids.insert(id);
                }
                Ok(Some(Revocation::Subject(s))) => {
                    out.subjects.insert(s);
                }
                Ok(None) => {}
                Err(e) => problems.push(format!("line {}: {e}", n + 1)),
            }
        }
        (out, problems)
    }

    fn revokes(&self, subject: &str, token_id: Option<&str>) -> bool {
        self.subjects.contains(subject) || token_id.is_some_and(|id| self.ids.contains(id))
    }
}

/// Settings for an authentication-enabled server.
pub struct AuthSettings {
    /// PASERK-encoded v4 public key (`k4.public.…`).
    pub public_key: String,
    /// Subjects allowed to access every task.
    pub admin_subjects: Vec<String>,
    /// Revocation file; a missing file means nothing is revoked.
    pub revocation_file: Option<PathBuf>,
}

struct Verifier {
    key: AsymmetricPublicKey<V4>,
    admins: HashSet<String>,
    revocations: RwLock<Revocations>,
    file: Option<PathBuf>,
    /// Modification time of the revocation file when last loaded.
    loaded_mtime: Mutex<Option<SystemTime>>,
}

impl Verifier {
    /// Re-read the revocation file if it changed since the last load. A file
    /// that cannot be read keeps the previous list (fail closed); a deleted file
    /// clears it. Returns whether the list was replaced.
    fn reload(&self) -> bool {
        let Some(path) = &self.file else {
            return false;
        };
        let mtime = std::fs::metadata(path).and_then(|m| m.modified()).ok();
        let mut loaded = self.loaded_mtime.lock().unwrap();
        if mtime == *loaded && mtime.is_some() {
            return false;
        }
        let next = match std::fs::read_to_string(path) {
            Ok(text) => {
                let (parsed, problems) = Revocations::parse(&text);
                for p in problems {
                    tracing::warn!(file = %path.display(), "revocation file {p}; ignored");
                }
                parsed
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Revocations::default(),
            Err(e) => {
                tracing::error!(file = %path.display(), error = %e, "cannot read revocation file; keeping the previous list");
                return false;
            }
        };
        *loaded = mtime;
        let mut current = self.revocations.write().unwrap();
        if *current == next {
            return false;
        }
        tracing::info!(
            ids = next.ids.len(),
            subjects = next.subjects.len(),
            "loaded token revocations"
        );
        *current = next;
        true
    }
}

/// Verifies a PASETO v4 public token on each request. Constructed with
/// [`AuthInterceptor::disabled`], it passes every request through unchanged.
#[derive(Clone)]
pub struct AuthInterceptor {
    verifier: Option<Arc<Verifier>>,
}

impl AuthInterceptor {
    /// No authentication: every request is allowed and carries no [`Caller`].
    pub fn disabled() -> Self {
        Self { verifier: None }
    }

    /// Enforce authentication with `settings`. Loads the revocation file now;
    /// call [`watch_revocations`](Self::watch_revocations) to pick up later
    /// changes.
    pub fn new(settings: AuthSettings) -> Result<Self, KeyError> {
        let key = AsymmetricPublicKey::<V4>::try_from(settings.public_key.as_str())
            .map_err(|e| KeyError(e.to_string()))?;
        let verifier = Arc::new(Verifier {
            key,
            admins: settings.admin_subjects.into_iter().collect(),
            revocations: RwLock::new(Revocations::default()),
            file: settings.revocation_file,
            loaded_mtime: Mutex::new(None),
        });
        verifier.reload();
        Ok(Self {
            verifier: Some(verifier),
        })
    }

    /// Whether authentication is enforced.
    pub fn is_enabled(&self) -> bool {
        self.verifier.is_some()
    }

    /// The revocation file in use, if any.
    pub fn revocation_file(&self) -> Option<&Path> {
        self.verifier.as_ref()?.file.as_deref()
    }

    /// Re-check the revocation file every `every` and apply changes. Must be
    /// called within a Tokio runtime. No-op when auth is disabled or no file is
    /// configured.
    pub fn watch_revocations(&self, every: Duration) {
        let Some(verifier) = self.verifier.clone() else {
            return;
        };
        if verifier.file.is_none() {
            return;
        }
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.tick().await; // already loaded at construction
            loop {
                tick.tick().await;
                let v = Arc::clone(&verifier);
                let _ = tokio::task::spawn_blocking(move || v.reload()).await;
            }
        });
    }

    /// Verify `raw` (an `authorization` value) and identify the caller.
    fn authenticate(verifier: &Verifier, raw: &str) -> Result<Caller, Status> {
        let token = raw.strip_prefix("Bearer ").unwrap_or(raw).trim();
        let untrusted = UntrustedToken::<Public, V4>::try_from(token)
            .map_err(|_| Status::unauthenticated("malformed token"))?;

        // Validate iat/nbf/exp; `allow_non_expiring` accepts long-lived API keys
        // (tokens that *do* carry an exp are still checked against it).
        let mut rules = ClaimsValidationRules::new();
        rules.allow_non_expiring();
        let trusted = pasetors::public::verify(&verifier.key, &untrusted, &rules, None, None)
            .map_err(|_| Status::unauthenticated("token verification failed"))?;

        let claims = trusted
            .payload_claims()
            .ok_or_else(|| Status::unauthenticated("token has no claims"))?;
        let subject = claims
            .get_claim("sub")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Status::unauthenticated("token has no subject"))?;
        let token_id = claims.get_claim("jti").and_then(|v| v.as_str());

        if verifier
            .revocations
            .read()
            .unwrap()
            .revokes(subject, token_id)
        {
            return Err(Status::unauthenticated("token has been revoked"));
        }
        Ok(Caller {
            subject: subject.to_string(),
            admin: verifier.admins.contains(subject),
        })
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        // Auth disabled -> allow, with no caller identity.
        let Some(verifier) = self.verifier.as_deref() else {
            return Ok(request);
        };
        let raw = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization metadata"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization is not valid ASCII"))?;
        let caller = Self::authenticate(verifier, raw)?;
        request.extensions_mut().insert(caller);
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pasetors::claims::Claims;
    use pasetors::keys::{AsymmetricKeyPair, Generate};
    use pasetors::paserk::FormatAsPaserk;

    struct Keys {
        public: String,
        pair: AsymmetricKeyPair<V4>,
    }

    fn keys() -> Keys {
        let pair = AsymmetricKeyPair::<V4>::generate().unwrap();
        let mut public = String::new();
        pair.public.fmt(&mut public).unwrap();
        Keys { public, pair }
    }

    fn token(keys: &Keys, subject: Option<&str>, id: Option<&str>) -> String {
        let mut claims = Claims::new().unwrap();
        if let Some(s) = subject {
            claims.subject(s).unwrap();
        }
        if let Some(id) = id {
            claims.token_identifier(id).unwrap();
        }
        pasetors::public::sign(&keys.pair.secret, &claims, None, None).unwrap()
    }

    fn call(auth: &mut AuthInterceptor, token: &str) -> Result<Option<Caller>, Status> {
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        auth.call(req)
            .map(|r| r.extensions().get::<Caller>().cloned())
    }

    fn interceptor(keys: &Keys, file: Option<PathBuf>) -> AuthInterceptor {
        AuthInterceptor::new(AuthSettings {
            public_key: keys.public.clone(),
            admin_subjects: vec!["ops".into()],
            revocation_file: file,
        })
        .unwrap()
    }

    #[test]
    fn identifies_the_caller() {
        let k = keys();
        let mut auth = interceptor(&k, None);
        let alice = call(&mut auth, &token(&k, Some("alice"), None)).unwrap();
        assert_eq!(
            alice,
            Some(Caller {
                subject: "alice".into(),
                admin: false
            })
        );
        let ops = call(&mut auth, &token(&k, Some("ops"), None)).unwrap();
        assert!(ops.unwrap().admin);
    }

    #[test]
    fn rejects_tokens_without_a_subject_or_from_another_key() {
        let k = keys();
        let mut auth = interceptor(&k, None);
        let err = call(&mut auth, &token(&k, None, None)).unwrap_err();
        assert!(err.message().contains("no subject"), "{}", err.message());
        let other = keys();
        assert!(call(&mut auth, &token(&other, Some("alice"), None)).is_err());
    }

    #[test]
    fn revocation_by_id_and_subject_applies_on_reload() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("revoked_tokens");
        let k = keys();
        let mut auth = interceptor(&k, Some(file.clone()));
        let t1 = token(&k, Some("alice"), Some("id-1"));
        let t2 = token(&k, Some("alice"), Some("id-2"));
        let bob = token(&k, Some("bob"), None);
        assert!(call(&mut auth, &t1).is_ok());

        std::fs::write(
            &file,
            format!("# revoked\n{}\n", Revocation::Id("id-1".into())),
        )
        .unwrap();
        assert!(auth.verifier.as_ref().unwrap().reload());
        let err = call(&mut auth, &t1).unwrap_err();
        assert!(err.message().contains("revoked"));
        assert!(call(&mut auth, &t2).is_ok(), "other tokens stay valid");

        // Revoking a subject covers its tokens with or without an id.
        let mut text = std::fs::read_to_string(&file).unwrap();
        text.push_str(&format!("{}\n", Revocation::Subject("bob".into())));
        std::fs::write(&file, text).unwrap();
        // Make sure the mtime differs even on coarse-grained filesystems.
        let later = SystemTime::now() + Duration::from_secs(5);
        std::fs::File::options()
            .write(true)
            .open(&file)
            .unwrap()
            .set_modified(later)
            .unwrap();
        assert!(auth.verifier.as_ref().unwrap().reload());
        assert!(call(&mut auth, &bob).is_err());
        assert!(call(&mut auth, &t2).is_ok());

        // Deleting the file clears the list.
        std::fs::remove_file(&file).unwrap();
        assert!(auth.verifier.as_ref().unwrap().reload());
        assert!(call(&mut auth, &t1).is_ok());
    }

    #[test]
    fn revocation_lines_round_trip() {
        for r in [
            Revocation::Id("abc".into()),
            Revocation::Subject("ci".into()),
        ] {
            assert_eq!(Revocation::parse_line(&r.to_string()), Ok(Some(r)));
        }
        assert_eq!(Revocation::parse_line("  # note"), Ok(None));
        assert!(Revocation::parse_line("token abc").is_err());
        assert!(Revocation::parse_line("id   ").is_err());
    }

    #[test]
    fn disabled_auth_allows_everything_without_a_caller() {
        let mut auth = AuthInterceptor::disabled();
        assert_eq!(
            auth.call(Request::new(()))
                .unwrap()
                .extensions()
                .get::<Caller>(),
            None
        );
    }
}
