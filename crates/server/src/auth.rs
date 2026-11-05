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
//! (`jti`, printed by `apollo token`) or a whole subject. On a database
//! instances share, revocations are also stored there
//! ([`AuthSettings::shared_revocations`]), and a token is refused when either
//! list revokes it. Running servers re-read both every few seconds
//! ([`AuthInterceptor::watch_revocations`]).

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

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

pub use apollo_domain::Revocation;

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

    fn from_list(list: Vec<Revocation>) -> Self {
        let mut out = Revocations::default();
        for r in list {
            match r {
                Revocation::Id(id) => out.ids.insert(id),
                Revocation::Subject(s) => out.subjects.insert(s),
            };
        }
        out
    }

    fn union(&self, other: &Revocations) -> Self {
        Revocations {
            ids: self.ids.union(&other.ids).cloned().collect(),
            subjects: self.subjects.union(&other.subjects).cloned().collect(),
        }
    }

    fn len(&self) -> usize {
        self.ids.len() + self.subjects.len()
    }
}

/// Where the revocations of instances sharing a database are read from.
// `async_trait` marks the boxed-future methods `#[must_use]`; clippy >= 1.99
// reports that as `double_must_use`.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait RevocationSource: Send + Sync + 'static {
    /// Every revocation stored now.
    async fn revocations(&self) -> Result<Vec<Revocation>, String>;
}

/// Settings for an authentication-enabled server.
pub struct AuthSettings {
    /// PASERK-encoded v4 public key (`k4.public.…`).
    pub public_key: String,
    /// Subjects allowed to access every task.
    pub admin_subjects: Vec<String>,
    /// Revocation file; a missing file means nothing is revoked.
    pub revocation_file: Option<PathBuf>,
    /// The revocations of instances sharing a database, applied along with the
    /// file's.
    pub shared_revocations: Option<Arc<dyn RevocationSource>>,
}

struct Verifier {
    key: AsymmetricPublicKey<V4>,
    admins: HashSet<String>,
    /// In force: the file's and the database's.
    revocations: RwLock<Revocations>,
    file: Option<PathBuf>,
    /// Modification time of the revocation file when last loaded.
    loaded_mtime: Mutex<Option<SystemTime>>,
    from_file: Mutex<Revocations>,
    from_db: Mutex<Revocations>,
    shared: Option<Arc<dyn RevocationSource>>,
    /// The last database read failed.
    db_failing: AtomicBool,
}

impl Verifier {
    /// Put the file's and the database's revocations in force. Returns
    /// whether that changed what is.
    fn apply(&self) -> bool {
        let file = self.from_file.lock().unwrap().clone();
        let db = self.from_db.lock().unwrap().clone();
        let next = file.union(&db);
        let mut current = self.revocations.write().unwrap();
        if *current == next {
            return false;
        }
        tracing::info!(
            ids = next.ids.len(),
            subjects = next.subjects.len(),
            file = file.len(),
            database = db.len(),
            "loaded token revocations"
        );
        *current = next;
        true
    }

    /// Read the database's revocations, waiting `limit` at most. One that
    /// can't be read keeps the previous list (fail closed). Returns whether
    /// what is in force changed.
    async fn reload_db(&self, limit: Duration) -> bool {
        let Some(source) = &self.shared else {
            return false;
        };
        match tokio::time::timeout(limit, source.revocations()).await {
            Ok(Ok(list)) => {
                *self.from_db.lock().unwrap() = Revocations::from_list(list);
                if self.db_failing.swap(false, Ordering::SeqCst) {
                    tracing::info!("token revocations readable from the database again");
                }
                self.apply()
            }
            failed => {
                if !self.db_failing.swap(true, Ordering::SeqCst) {
                    let error = match failed {
                        Ok(Err(e)) => e,
                        _ => format!("no answer within {limit:?}"),
                    };
                    tracing::warn!(%error, "cannot read token revocations from the database; keeping the previous list");
                }
                false
            }
        }
    }

    /// Re-read the revocation file if it changed since the last load. A file
    /// that cannot be read keeps the previous list (fail closed); a deleted file
    /// clears it. Returns whether what is in force changed.
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
        drop(loaded);
        *self.from_file.lock().unwrap() = next;
        self.apply()
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
    /// with shared revocations, call
    /// [`load_shared_revocations`](Self::load_shared_revocations) before
    /// serving, and [`watch_revocations`](Self::watch_revocations) to pick up
    /// later changes.
    pub fn new(settings: AuthSettings) -> Result<Self, KeyError> {
        let key = AsymmetricPublicKey::<V4>::try_from(settings.public_key.as_str())
            .map_err(|e| KeyError(e.to_string()))?;
        let verifier = Arc::new(Verifier {
            key,
            admins: settings.admin_subjects.into_iter().collect(),
            revocations: RwLock::new(Revocations::default()),
            file: settings.revocation_file,
            loaded_mtime: Mutex::new(None),
            from_file: Mutex::new(Revocations::default()),
            from_db: Mutex::new(Revocations::default()),
            shared: settings.shared_revocations,
            db_failing: AtomicBool::new(false),
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

    /// Whether revocations are also read from a shared database.
    pub fn has_shared_revocations(&self) -> bool {
        self.verifier.as_ref().is_some_and(|v| v.shared.is_some())
    }

    /// The first read of the shared revocations, whose failure is returned
    /// (a server shouldn't start without them).
    pub async fn load_shared_revocations(&self) -> Result<(), String> {
        let Some(verifier) = &self.verifier else {
            return Ok(());
        };
        let Some(source) = &verifier.shared else {
            return Ok(());
        };
        let list = source.revocations().await?;
        *verifier.from_db.lock().unwrap() = Revocations::from_list(list);
        verifier.apply();
        Ok(())
    }

    /// Re-read the revocation file, and the shared revocations within
    /// `limit`, applying changes. Returns whether what is in force changed.
    pub async fn poll_once(&self, limit: Duration) -> bool {
        let Some(verifier) = self.verifier.clone() else {
            return false;
        };
        let v = Arc::clone(&verifier);
        let file = tokio::task::spawn_blocking(move || v.reload())
            .await
            .unwrap_or(false);
        let db = verifier.reload_db(limit).await;
        file || db
    }

    /// Re-check the revocation file and the shared revocations every `every`
    /// and apply changes. Must be called within a Tokio runtime. No-op when
    /// auth is disabled, or there is neither a file nor shared revocations.
    pub fn watch_revocations(&self, every: Duration) {
        let Some(verifier) = &self.verifier else {
            return;
        };
        if verifier.file.is_none() && verifier.shared.is_none() {
            return;
        }
        let auth = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await; // already loaded
            loop {
                tick.tick().await;
                auth.poll_once(every).await;
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
        with_source(keys, file, None)
    }

    fn with_source(
        keys: &Keys,
        file: Option<PathBuf>,
        source: Option<Arc<FakeSource>>,
    ) -> AuthInterceptor {
        AuthInterceptor::new(AuthSettings {
            public_key: keys.public.clone(),
            admin_subjects: vec!["ops".into()],
            revocation_file: file,
            shared_revocations: source.map(|s| s as Arc<dyn RevocationSource>),
        })
        .unwrap()
    }

    /// A database's revocations: what the next read gives.
    struct FakeSource(Mutex<Result<Vec<Revocation>, String>>);

    impl FakeSource {
        fn new(next: Result<Vec<Revocation>, String>) -> Arc<Self> {
            Arc::new(Self(Mutex::new(next)))
        }

        fn set(&self, next: Result<Vec<Revocation>, String>) {
            *self.0.lock().unwrap() = next;
        }
    }

    #[async_trait]
    impl RevocationSource for FakeSource {
        async fn revocations(&self) -> Result<Vec<Revocation>, String> {
            self.0.lock().unwrap().clone()
        }
    }

    const LIMIT: Duration = Duration::from_secs(1);

    #[tokio::test]
    async fn db_and_file_revocations_merge() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("revoked_tokens");
        std::fs::write(&file, "id id-1\n").unwrap();
        let k = keys();
        let source = FakeSource::new(Ok(vec![Revocation::Subject("bob".into())]));
        let mut auth = with_source(&k, Some(file), Some(source));
        auth.poll_once(LIMIT).await;
        assert!(call(&mut auth, &token(&k, Some("alice"), Some("id-1"))).is_err());
        assert!(call(&mut auth, &token(&k, Some("bob"), Some("id-9"))).is_err());
        assert!(call(&mut auth, &token(&k, Some("alice"), Some("id-2"))).is_ok());
    }

    #[tokio::test]
    async fn db_read_failure_keeps_previous_list() {
        let k = keys();
        let source = FakeSource::new(Ok(vec![Revocation::Id("id-1".into())]));
        let mut auth = with_source(&k, None, Some(source.clone()));
        auth.load_shared_revocations().await.unwrap();
        let t1 = token(&k, Some("alice"), Some("id-1"));
        assert!(call(&mut auth, &t1).is_err());
        source.set(Err("connection reset".into()));
        assert!(!auth.poll_once(LIMIT).await);
        assert!(call(&mut auth, &t1).is_err(), "still revoked");
        source.set(Ok(vec![]));
        assert!(auth.poll_once(LIMIT).await);
        assert!(call(&mut auth, &t1).is_ok(), "un-revoked in the database");
    }

    #[tokio::test]
    async fn deleting_the_file_keeps_db_revocations() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("revoked_tokens");
        std::fs::write(&file, "subject bob\n").unwrap();
        let k = keys();
        let source = FakeSource::new(Ok(vec![Revocation::Subject("bob".into())]));
        let mut auth = with_source(&k, Some(file.clone()), Some(source));
        auth.poll_once(LIMIT).await;
        let bob = token(&k, Some("bob"), None);
        assert!(call(&mut auth, &bob).is_err());
        std::fs::remove_file(&file).unwrap();
        auth.poll_once(LIMIT).await;
        assert!(
            call(&mut auth, &bob).is_err(),
            "still revoked in the database"
        );
    }

    #[tokio::test]
    async fn load_shared_revocations_error_is_returned() {
        let k = keys();
        let auth = with_source(&k, None, Some(FakeSource::new(Err("down".into()))));
        assert_eq!(auth.load_shared_revocations().await, Err("down".into()));
    }

    #[tokio::test]
    async fn watch_polls_the_database_without_a_file() {
        let k = keys();
        let source = FakeSource::new(Ok(vec![]));
        let mut auth = with_source(&k, None, Some(source.clone()));
        auth.load_shared_revocations().await.unwrap();
        auth.watch_revocations(Duration::from_millis(50));
        let t1 = token(&k, Some("alice"), Some("id-1"));
        assert!(call(&mut auth, &t1).is_ok());
        source.set(Ok(vec![Revocation::Id("id-1".into())]));
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while call(&mut auth, &t1).is_ok() {
            assert!(std::time::Instant::now() < deadline, "never applied");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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
