//! gRPC `Inference` service: `Classify` (single input -> one task id), `GetTask`
//! (state + results, backed by storage), and `CancelTask`. The `serve` helpers
//! also expose gRPC server reflection and health, optionally over TLS.
//!
//! With authentication enabled, a task belongs to the token subject that
//! submitted it: `GetTask` / `CancelTask` from any other subject (unless it is an
//! admin) answer `NOT_FOUND`, exactly as for an unknown id, so task ids reveal
//! nothing to other callers.

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;

use tokio::io::AsyncWriteExt;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic::{Request, Response, Status, transport::Server};

use apollo_domain::Input;
use apollo_engine::{Engine, EngineError, Submission};
use apollo_proto::classify_chunk::Payload;
use apollo_proto::inference_server::{Inference, InferenceServer};
use apollo_proto::{
    CancelRequest, ClassifyChunk, ClassifyRequest, GetTaskRequest, Task, TaskCreated,
};

use crate::auth::{AuthInterceptor, Caller};
use crate::convert::{submission_from_proto, task_to_proto};

/// The `Inference` service implementation, wrapping the engine.
#[derive(Clone)]
pub struct InferenceService {
    engine: Engine,
}

impl InferenceService {
    pub fn new(engine: Engine) -> Self {
        Self { engine }
    }
}

#[tonic::async_trait]
impl Inference for InferenceService {
    async fn classify(
        &self,
        request: Request<ClassifyRequest>,
    ) -> Result<Response<TaskCreated>, Status> {
        let owner = caller(&request).map(|c| c.subject);
        let item = request
            .into_inner()
            .item
            .ok_or_else(|| Status::invalid_argument("request has no item"))?;
        let model_count = item.models.len();
        let submission = submission_from_proto(item).map_err(Status::invalid_argument)?;
        let task_id = self
            .engine
            .submit(vec![submission], owner)
            .await
            .map_err(engine_to_status)?;
        tracing::info!(task = %task_id, models = model_count, "accepted Classify");
        Ok(Response::new(TaskCreated { task_id }))
    }

    async fn get_task(&self, request: Request<GetTaskRequest>) -> Result<Response<Task>, Status> {
        let caller = caller(&request);
        let id = request.into_inner().task_id;
        tracing::debug!(task = %id, "GetTask");
        let task = self.visible_task(&id, caller.as_ref()).await?;
        Ok(Response::new(task_to_proto(task)))
    }

    async fn cancel_task(&self, request: Request<CancelRequest>) -> Result<Response<Task>, Status> {
        let caller = caller(&request);
        let id = request.into_inner().task_id;
        tracing::info!(task = %id, "CancelTask");
        // Authorize before acting: a caller may only cancel tasks it can see.
        self.visible_task(&id, caller.as_ref()).await?;
        self.engine.cancel(&id).await.map_err(engine_to_status)?;
        let task = self.visible_task(&id, caller.as_ref()).await?;
        Ok(Response::new(task_to_proto(task)))
    }

    async fn classify_stream(
        &self,
        request: Request<tonic::Streaming<ClassifyChunk>>,
    ) -> Result<Response<TaskCreated>, Status> {
        let owner = caller(&request).map(|c| c.subject);
        let mut stream = request.into_inner();

        // The opening frame must be `init`.
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty stream"))?;
        let init = match first.payload {
            Some(Payload::Init(init)) => init,
            _ => {
                return Err(Status::invalid_argument(
                    "first message must be the init frame",
                ));
            }
        };
        if init.models.is_empty() && init.pipeline.is_none() {
            return Err(Status::invalid_argument(
                "init frame must set `models` or a `pipeline`",
            ));
        }

        // Validate models / pipeline and capacity before accepting a single byte,
        // so a bad or unservable request cannot make the server stage a large
        // upload first. `submit` re-checks everything below.
        let probe = Submission {
            input: Input::Bytes {
                path: PathBuf::new(),
                video: init.video,
            },
            models: init.models.clone(),
            pipeline: init.pipeline.clone(),
        };
        self.engine
            .check_submission(probe)
            .map_err(engine_to_status)?;

        // Stage the streamed bytes to an owner-only file in the private upload dir.
        let engine = self.engine.clone();
        let dir = tokio::task::spawn_blocking(move || engine.ensure_upload_dir())
            .await
            .map_err(|e| Status::internal(format!("creating upload dir: {e}")))?
            .map_err(|e| Status::internal(format!("creating upload dir: {e}")))?;
        let path = dir.join(format!("{}.bin", uuid::Uuid::new_v4()));
        let cap = self.engine.max_upload_bytes();

        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(&path)
            .await
            .map_err(|e| Status::internal(format!("creating upload file: {e}")))?;
        let mut total: u64 = 0;
        let mut saw_data = false;
        let result = async {
            while let Some(chunk) = stream.message().await? {
                match chunk.payload {
                    Some(Payload::Data(bytes)) => {
                        total += bytes.len() as u64;
                        if let Some(max) = cap
                            && total > max
                        {
                            return Err(Status::resource_exhausted(format!(
                                "upload exceeds the {max}-byte limit"
                            )));
                        }
                        saw_data = true;
                        file.write_all(&bytes)
                            .await
                            .map_err(|e| Status::internal(format!("writing upload: {e}")))?;
                    }
                    Some(Payload::Init(_)) => {
                        return Err(Status::invalid_argument("unexpected second init frame"));
                    }
                    None => {} // empty message — ignore
                }
            }
            file.flush()
                .await
                .map_err(|e| Status::internal(format!("flushing upload: {e}")))?;
            Ok(())
        }
        .await;

        if let Err(status) = result {
            let _ = tokio::fs::remove_file(&path).await;
            return Err(status);
        }
        if !saw_data {
            let _ = tokio::fs::remove_file(&path).await;
            return Err(Status::invalid_argument("no content bytes received"));
        }

        let submission = Submission {
            input: Input::Bytes {
                path: path.clone(),
                video: init.video,
            },
            models: init.models,
            pipeline: init.pipeline,
        };
        let model_count = submission.models.len();
        let task_id = match self.engine.submit(vec![submission], owner).await {
            Ok(id) => id,
            Err(e) => {
                // Validation failed before the task existed, so the task lifecycle
                // will never clean this upload — remove it now.
                let _ = tokio::fs::remove_file(&path).await;
                return Err(engine_to_status(e));
            }
        };
        tracing::info!(task = %task_id, models = model_count, video = init.video, "accepted ClassifyStream");
        Ok(Response::new(TaskCreated { task_id }))
    }
}

impl InferenceService {
    /// Load task `id` if `caller` may see it. A task that exists but belongs to
    /// someone else yields the same `NOT_FOUND` as a missing one.
    async fn visible_task(
        &self,
        id: &str,
        caller: Option<&Caller>,
    ) -> Result<apollo_domain::Task, Status> {
        match self.engine.get_task(id).await.map_err(engine_to_status)? {
            Some(task) if may_access(caller, &task) => Ok(task),
            _ => Err(Status::not_found(format!("task '{id}' not found"))),
        }
    }
}

/// The authenticated caller, or `None` when authentication is disabled.
fn caller<T>(request: &Request<T>) -> Option<Caller> {
    request.extensions().get::<Caller>().cloned()
}

/// Whether `caller` may read / cancel `task`: always without authentication;
/// otherwise admins, and the subject that submitted it. Tasks submitted while
/// authentication was disabled have no owner and are admin-only.
fn may_access(caller: Option<&Caller>, task: &apollo_domain::Task) -> bool {
    match caller {
        None => true,
        Some(c) if c.admin => true,
        Some(c) => task.owner.as_deref() == Some(c.subject.as_str()),
    }
}

/// Make `ring` the process-wide rustls crypto provider (unless one is already
/// set). rustls only picks a default automatically when exactly one backend is
/// compiled in; this binary links both `ring` and `aws-lc-rs`, and tonic's server
/// TLS setup relies on the process default, so without this it panics.
fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// PEM material for serving TLS.
#[derive(Clone)]
pub struct TlsSettings {
    /// Certificate chain presented to clients (leaf first).
    pub cert_pem: Vec<u8>,
    /// Private key for `cert_pem`.
    pub key_pem: Vec<u8>,
    /// When set, clients must present a certificate signed by this CA bundle
    /// (mutual TLS).
    pub client_ca_pem: Option<Vec<u8>>,
}

impl TlsSettings {
    fn server_config(&self) -> ServerTlsConfig {
        let mut config =
            ServerTlsConfig::new().identity(Identity::from_pem(&self.cert_pem, &self.key_pem));
        if let Some(ca) = &self.client_ca_pem {
            config = config
                .client_ca_root(Certificate::from_pem(ca))
                .client_auth_optional(false);
        }
        config
    }
}

/// Map engine errors to gRPC status codes. Submit-time validation failures are
/// client errors; everything else is internal.
fn engine_to_status(e: EngineError) -> Status {
    match e {
        EngineError::UnknownModel(_) | EngineError::Incompatible(_) | EngineError::Config(_) => {
            Status::invalid_argument(e.to_string())
        }
        EngineError::UnknownTask(_) => Status::not_found(e.to_string()),
        EngineError::Overloaded(_) => Status::resource_exhausted(e.to_string()),
        other => Status::internal(other.to_string()),
    }
}

/// Wrap an [`Engine`] as a tonic service with the PASETO auth interceptor applied,
/// ready to `add_service` to a `Server`.
pub fn inference_service(
    engine: Engine,
    auth: AuthInterceptor,
) -> InterceptedService<InferenceServer<InferenceService>, AuthInterceptor> {
    InferenceServer::with_interceptor(InferenceService::new(engine), auth)
}

/// Build the gRPC reflection service advertising the `Inference` service (and the
/// shared messages) plus the standard health service, so tools like grpcurl can
/// call both without local `.proto` files. Panics only if a compiled-in
/// descriptor is malformed.
fn reflection_service() -> tonic_reflection::server::v1::ServerReflectionServer<
    impl tonic_reflection::server::v1::ServerReflection,
> {
    tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(apollo_proto::INFERENCE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("embedded gRPC reflection descriptor must be valid")
}

/// Serve the `Inference` API (plus reflection and health) on `addr` until
/// terminated, over TLS when `tls` is given.
pub async fn serve(
    engine: Engine,
    addr: SocketAddr,
    auth: AuthInterceptor,
    tls: Option<TlsSettings>,
) -> Result<(), tonic::transport::Error> {
    serve_with_shutdown(engine, addr, std::future::pending(), auth, tls).await
}

/// Serve until `shutdown` resolves, draining in-flight RPCs (for graceful stop),
/// over TLS when `tls` is given.
pub async fn serve_with_shutdown<F>(
    engine: Engine,
    addr: SocketAddr,
    shutdown: F,
    auth: AuthInterceptor,
    tls: Option<TlsSettings>,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()>,
{
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<InferenceServer<InferenceService>>()
        .await;
    let mut builder = Server::builder();
    if let Some(tls) = &tls {
        install_crypto_provider();
        builder = builder.tls_config(tls.server_config())?;
    }
    tracing::info!(
        %addr,
        tls = tls.is_some(),
        mutual_tls = tls.as_ref().is_some_and(|t| t.client_ca_pem.is_some()),
        "serving Inference gRPC (reflection + health enabled)"
    );
    builder
        .add_service(inference_service(engine, auth))
        .add_service(reflection_service())
        .add_service(health_service)
        .serve_with_shutdown(addr, shutdown)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;

    use apollo_config::{Config, DatabaseConfig, SqliteConfig};
    use apollo_domain::{Item, ItemState, TaskState, Url};
    use apollo_storage::Storage;
    use tokio::sync::oneshot;

    struct Fixture {
        engine: Engine,
        storage: Arc<dyn Storage>,
        _dir: tempfile::TempDir,
    }

    async fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let mut config: Config = apollo_config::load::from_str("").unwrap();
        config.database = DatabaseConfig {
            sqlite: Some(SqliteConfig {
                path: dir.path().join("t.db").to_string_lossy().into_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        config.app.cache_dir = Some(dir.path().to_string_lossy().into_owned());
        let storage = apollo_storage::open(&config.database).await.unwrap();
        let engine = Engine::new(Arc::new(config), storage.clone(), None);
        Fixture {
            engine,
            storage,
            _dir: dir,
        }
    }

    fn stored_task(id: &str, owner: Option<&str>) -> apollo_domain::Task {
        apollo_domain::Task {
            id: id.into(),
            state: TaskState::Queued,
            items: vec![Item {
                input: Input::Image(Url {
                    main: "https://example.com/a.png".into(),
                    fallback: None,
                }),
                models: vec!["m".into()],
                pipeline: None,
                state: ItemState::Queued,
                results: BTreeMap::new(),
                error: None,
                retries: 0,
            }],
            owner: owner.map(str::to_string),
        }
    }

    fn as_caller<T>(msg: T, subject: Option<&str>, admin: bool) -> Request<T> {
        let mut req = Request::new(msg);
        if let Some(subject) = subject {
            req.extensions_mut().insert(Caller {
                subject: subject.into(),
                admin,
            });
        }
        req
    }

    fn get(id: &str) -> GetTaskRequest {
        GetTaskRequest { task_id: id.into() }
    }

    #[tokio::test]
    async fn tasks_are_scoped_to_their_owner() {
        let f = fixture().await;
        let svc = InferenceService::new(f.engine.clone());
        f.storage
            .create_task(&stored_task("alice-task", Some("alice")))
            .await
            .unwrap();
        f.storage
            .create_task(&stored_task("anon-task", None))
            .await
            .unwrap();

        let ok = |r: Result<Response<Task>, Status>| r.map(|r| r.into_inner().id).unwrap();
        let denied = |r: Result<Response<Task>, Status>| r.unwrap_err();

        // Owner, admin, and an auth-disabled server may read it.
        assert_eq!(
            ok(svc
                .get_task(as_caller(get("alice-task"), Some("alice"), false))
                .await),
            "alice-task"
        );
        assert_eq!(
            ok(svc
                .get_task(as_caller(get("alice-task"), Some("ops"), true))
                .await),
            "alice-task"
        );
        assert_eq!(
            ok(svc
                .get_task(as_caller(get("alice-task"), None, false))
                .await),
            "alice-task"
        );

        // Another subject gets exactly what an unknown id gets.
        let other = denied(
            svc.get_task(as_caller(get("alice-task"), Some("bob"), false))
                .await,
        );
        let missing = denied(
            svc.get_task(as_caller(get("no-such"), Some("bob"), false))
                .await,
        );
        assert_eq!(other.code(), tonic::Code::NotFound);
        assert_eq!(
            other.message().replace("alice-task", "<id>"),
            missing.message().replace("no-such", "<id>")
        );

        // Ownerless tasks (submitted without auth) are admin-only once auth is on.
        assert_eq!(
            denied(
                svc.get_task(as_caller(get("anon-task"), Some("alice"), false))
                    .await
            )
            .code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            ok(svc
                .get_task(as_caller(get("anon-task"), Some("ops"), true))
                .await),
            "anon-task"
        );

        // A non-owner cannot cancel; the owner can.
        let cancel = |id: &str| CancelRequest { task_id: id.into() };
        let err = svc
            .cancel_task(as_caller(cancel("alice-task"), Some("bob"), false))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        let t = f.storage.get_task("alice-task").await.unwrap().unwrap();
        assert_eq!(
            t.state,
            TaskState::Queued,
            "bob's cancel must not take effect"
        );
        svc.cancel_task(as_caller(cancel("alice-task"), Some("alice"), false))
            .await
            .unwrap();
        let t = f.storage.get_task("alice-task").await.unwrap().unwrap();
        assert_eq!(t.state, TaskState::Cancelled);
    }

    // ------------------------------ TLS ------------------------------

    struct Pki {
        ca_pem: String,
        server: (String, String),
        client: (String, String),
    }

    /// A throwaway CA with a server certificate for 127.0.0.1 and a client
    /// certificate.
    fn make_pki() -> Pki {
        use rcgen::{
            BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose,
            IsCa, KeyPair,
        };
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "apollo test CA");
        let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();

        let leaf = |names: Vec<String>, usage: ExtendedKeyUsagePurpose| {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(names).unwrap();
            params.extended_key_usages = vec![usage];
            let cert = params.signed_by(&key, &ca).unwrap();
            (cert.pem(), key.serialize_pem())
        };
        Pki {
            ca_pem: ca.pem(),
            server: leaf(
                vec!["127.0.0.1".into()],
                ExtendedKeyUsagePurpose::ServerAuth,
            ),
            client: leaf(vec!["client".into()], ExtendedKeyUsagePurpose::ClientAuth),
        }
    }

    /// Serve the real API with `tls` on a free port; returns its https URL and
    /// a shutdown trigger.
    async fn serve_tls(engine: Engine, tls: TlsSettings) -> (String, oneshot::Sender<()>) {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let addr: SocketAddr = ([127, 0, 0, 1], port).into();
        let (stop, stopped) = oneshot::channel::<()>();
        tokio::spawn(serve_with_shutdown(
            engine,
            addr,
            async {
                let _ = stopped.await;
            },
            AuthInterceptor::disabled(),
            Some(tls),
        ));
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        (format!("https://{addr}"), stop)
    }

    /// GetTask for an unknown id: `NOT_FOUND` proves the TLS session and the RPC
    /// both worked. A rejected handshake surfaces as a connection error or (with
    /// TLS 1.3, where the client certificate is checked after the client's side
    /// of the handshake) as a status other than `NOT_FOUND`.
    async fn probe(builder: apollo_client::ClientBuilder) -> Result<tonic::Code, String> {
        let client = builder.build().await.map_err(|e| e.to_string())?;
        match client.get_task("no-such-task").await {
            Err(apollo_client::ClientError::Status(s)) => Ok(s.code()),
            Err(e) => Err(e.to_string()),
            Ok(_) => Ok(tonic::Code::Ok),
        }
    }

    #[tokio::test]
    async fn serves_tls() {
        let f = fixture().await;
        let pki = make_pki();
        let (url, stop) = serve_tls(
            f.engine.clone(),
            TlsSettings {
                cert_pem: pki.server.0.clone().into(),
                key_pem: pki.server.1.clone().into(),
                client_ca_pem: None,
            },
        )
        .await;

        let trusted = apollo_client::Client::builder(&url).ca_certificate(pki.ca_pem.clone());
        assert_eq!(probe(trusted).await, Ok(tonic::Code::NotFound));
        // A client that does not trust the CA refuses the connection.
        assert!(probe(apollo_client::Client::builder(&url)).await.is_err());
        // Plaintext against a TLS port fails too.
        let plain = url.replace("https://", "http://");
        let outcome = probe(apollo_client::Client::builder(&plain)).await;
        assert_ne!(outcome, Ok(tonic::Code::NotFound));
        let _ = stop.send(());
    }

    #[tokio::test]
    async fn mutual_tls_requires_a_client_certificate() {
        let f = fixture().await;
        let pki = make_pki();
        let (url, stop) = serve_tls(
            f.engine.clone(),
            TlsSettings {
                cert_pem: pki.server.0.clone().into(),
                key_pem: pki.server.1.clone().into(),
                client_ca_pem: Some(pki.ca_pem.clone().into()),
            },
        )
        .await;

        let base = || apollo_client::Client::builder(&url).ca_certificate(pki.ca_pem.clone());
        let outcome = probe(base()).await;
        assert_ne!(outcome, Ok(tonic::Code::NotFound), "no client certificate");
        let with_cert = base().client_identity(pki.client.0.clone(), pki.client.1.clone());
        assert_eq!(probe(with_cert).await, Ok(tonic::Code::NotFound));
        // A certificate from a different CA is rejected.
        let stranger = make_pki();
        let wrong = base().client_identity(stranger.client.0, stranger.client.1);
        let outcome = probe(wrong).await;
        assert_ne!(
            outcome,
            Ok(tonic::Code::NotFound),
            "foreign client certificate"
        );
        let _ = stop.send(());
    }
}
