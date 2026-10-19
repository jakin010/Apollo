//! gRPC `Inference` service: `Classify` / `ClassifyStream` (one input -> one
//! task), `GetTask`, `WatchTask`, `ListTasks`, `CancelTask` and `ListModels`.
//! The `serve` helpers also expose gRPC server reflection and health,
//! optionally over TLS.
//!
//! With authentication enabled, a task belongs to the token subject that
//! submitted it: reads and cancels from any other subject (unless it is an
//! admin) answer `NOT_FOUND`, exactly as for an unknown id, so task ids reveal
//! nothing to other callers; `ListTasks` lists only the caller's own tasks.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::Duration;

use futures::Stream;
use tokio::io::AsyncWriteExt;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic::{Request, Response, Status, transport::Server};

use apollo_domain::Input;
use apollo_engine::{Engine, EngineError, Run, Submission, Submitted};
use apollo_proto::classify_chunk::Payload;
use apollo_proto::inference_server::{Inference, InferenceServer};
use apollo_proto::{
    CancelTaskRequest, ClassifyChunk, ClassifyRequest, GetTaskRequest, ListModelsRequest,
    ListModelsResponse, ListTasksRequest, ListTasksResponse, Task, WatchTaskRequest,
};

use crate::auth::{AuthInterceptor, Caller};
use crate::convert::{self, Source, View, task_to_proto};

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

type TaskStream = Pin<Box<dyn Stream<Item = Result<Task, Status>> + Send + 'static>>;

/// Everything a submission carries besides its content.
struct Accept {
    owner: Option<String>,
    run: Run,
    metadata: BTreeMap<String, String>,
    request_id: Option<String>,
    wait_seconds: u32,
    view: View,
}

impl Accept {
    fn new(
        owner: Option<String>,
        run: Run,
        metadata: BTreeMap<String, String>,
        request_id: String,
        wait_seconds: u32,
        view: i32,
    ) -> Result<Self, Status> {
        let invalid = Status::invalid_argument;
        convert::check_metadata(&metadata).map_err(invalid)?;
        Ok(Self {
            owner,
            run,
            metadata,
            request_id: convert::request_id(request_id).map_err(invalid)?,
            wait_seconds: convert::wait_seconds(wait_seconds).map_err(invalid)?,
            view: convert::view_from_proto(view).map_err(invalid)?,
        })
    }
}

#[tonic::async_trait]
impl Inference for InferenceService {
    async fn classify(&self, request: Request<ClassifyRequest>) -> Result<Response<Task>, Status> {
        let owner = caller(&request).map(|c| c.subject);
        let req = request.into_inner();
        let invalid = Status::invalid_argument;
        let source = convert::input_from_proto(req.input).map_err(invalid)?;
        let run = convert::classify_run(req.run).map_err(invalid)?;
        let accept = Accept::new(
            owner,
            run,
            req.metadata,
            req.request_id,
            req.wait_seconds,
            req.view,
        )?;

        let (input, staged) = match source {
            Source::Input(input) => (input, None),
            Source::Data { kind, bytes } => {
                // A repeat needs no copy of the content.
                if let Some(task) = self.existing(&accept).await? {
                    return Ok(Response::new(task));
                }
                if let Some(max) = self.engine.max_upload_bytes()
                    && bytes.len() as u64 > max
                {
                    return Err(Status::resource_exhausted(format!(
                        "content exceeds the {max}-byte limit"
                    )));
                }
                let (path, mut file) = self.create_upload().await?;
                let written = async {
                    file.write_all(&bytes).await?;
                    file.flush().await
                }
                .await;
                if let Err(e) = written {
                    let _ = tokio::fs::remove_file(&path).await;
                    return Err(Status::internal(format!("writing upload: {e}")));
                }
                (
                    Input::Bytes {
                        path: path.clone(),
                        kind,
                    },
                    Some(path),
                )
            }
        };
        let task = self.accept(input, staged, accept).await?;
        tracing::info!(task = %task.id, "accepted Classify");
        Ok(Response::new(task))
    }

    async fn classify_stream(
        &self,
        request: Request<tonic::Streaming<ClassifyChunk>>,
    ) -> Result<Response<Task>, Status> {
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
        let invalid = Status::invalid_argument;
        let kind = convert::media_kind_from_proto(init.kind).map_err(invalid)?;
        let run = convert::stream_run(init.run).map_err(invalid)?;
        let accept = Accept::new(
            owner,
            run,
            init.metadata,
            init.request_id,
            init.wait_seconds,
            init.view,
        )?;
        // A repeat returns the earlier task without reading the content.
        if let Some(task) = self.existing(&accept).await? {
            return Ok(Response::new(task));
        }

        // Validate models / pipeline and capacity before accepting a single byte,
        // so a bad or unservable request cannot make the server stage a large
        // upload first. `submit` re-checks everything below.
        let probe = Input::Bytes {
            path: PathBuf::new(),
            kind,
        };
        self.engine
            .check_submission(&probe, &accept.run)
            .map_err(engine_to_status)?;

        // Stage the streamed bytes to an owner-only file in the private upload dir.
        let (path, mut file) = self.create_upload().await?;
        let cap = self.engine.max_upload_bytes();
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

        let input = Input::Bytes {
            path: path.clone(),
            kind,
        };
        let task = self.accept(input, Some(path), accept).await?;
        tracing::info!(task = %task.id, bytes = total, ?kind, "accepted ClassifyStream");
        Ok(Response::new(task))
    }

    async fn get_task(&self, request: Request<GetTaskRequest>) -> Result<Response<Task>, Status> {
        let caller = caller(&request);
        let req = request.into_inner();
        let view = convert::view_from_proto(req.view).map_err(Status::invalid_argument)?;
        tracing::debug!(task = %req.task_id, "GetTask");
        let task = self.visible_task(&req.task_id, caller.as_ref()).await?;
        Ok(Response::new(task_to_proto(task, view)))
    }

    type WatchTaskStream = TaskStream;

    async fn watch_task(
        &self,
        request: Request<WatchTaskRequest>,
    ) -> Result<Response<TaskStream>, Status> {
        let caller = caller(&request);
        let req = request.into_inner();
        let view = convert::view_from_proto(req.view).map_err(Status::invalid_argument)?;
        tracing::debug!(task = %req.task_id, "WatchTask");
        // Authorize first; the watch itself re-reads the task.
        self.visible_task(&req.task_id, caller.as_ref()).await?;
        let watch = self.engine.watch(&req.task_id);
        let stream = futures::stream::unfold(Some(watch), move |watch| async move {
            let mut watch = watch?;
            match watch.next().await {
                Ok(Some(task)) => Some((Ok(task_to_proto(task, view)), Some(watch))),
                Ok(None) => None,
                Err(e) => Some((Err(engine_to_status(e)), None)),
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_tasks(
        &self,
        request: Request<ListTasksRequest>,
    ) -> Result<Response<ListTasksResponse>, Status> {
        // Admins (and an auth-disabled server) see every task; anyone else
        // their own.
        let scope = match caller(&request) {
            Some(c) if !c.admin => Some(Some(c.subject)),
            _ => None,
        };
        let query =
            convert::task_query(request.into_inner(), scope).map_err(Status::invalid_argument)?;
        let page = self
            .engine
            .list_tasks(&query)
            .await
            .map_err(engine_to_status)?;
        Ok(Response::new(ListTasksResponse {
            tasks: page
                .tasks
                .into_iter()
                .map(|t| task_to_proto(t, View::Summary))
                .collect(),
            next_page_token: page
                .next
                .as_ref()
                .map(convert::page_token)
                .unwrap_or_default(),
        }))
    }

    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> Result<Response<Task>, Status> {
        let caller = caller(&request);
        let id = request.into_inner().task_id;
        tracing::info!(task = %id, "CancelTask");
        // Authorize before acting: a caller may only cancel tasks it can see.
        self.visible_task(&id, caller.as_ref()).await?;
        self.engine.cancel(&id).await.map_err(engine_to_status)?;
        let task = self.visible_task(&id, caller.as_ref()).await?;
        Ok(Response::new(task_to_proto(task, View::Summary)))
    }

    async fn list_models(
        &self,
        _request: Request<ListModelsRequest>,
    ) -> Result<Response<ListModelsResponse>, Status> {
        let catalog = self.engine.catalog().await.map_err(engine_to_status)?;
        Ok(Response::new(convert::catalog_to_proto(catalog)))
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

    /// The task an earlier submission with this `request_id` created, if any.
    async fn existing(&self, accept: &Accept) -> Result<Option<Task>, Status> {
        let Some(rid) = &accept.request_id else {
            return Ok(None);
        };
        let Some(id) = self
            .engine
            .find_request(accept.owner.as_deref(), rid)
            .await
            .map_err(engine_to_status)?
        else {
            return Ok(None);
        };
        Ok(Some(self.respond(&id, accept).await?))
    }

    /// Submit `input` and answer with its task (waiting if asked). `staged` is
    /// the upload file `input` reads, removed here when no task takes it over.
    async fn accept(
        &self,
        input: Input,
        staged: Option<PathBuf>,
        accept: Accept,
    ) -> Result<Task, Status> {
        let submission = Submission {
            input,
            run: accept.run.clone(),
            metadata: accept.metadata.clone(),
            request_id: accept.request_id.clone(),
        };
        let discard = |path: &Option<PathBuf>| {
            if let Some(path) = path {
                remove_upload(path);
            }
        };
        let id = match self.engine.submit(submission, accept.owner.clone()).await {
            Ok(Submitted { id, existing }) => {
                if existing {
                    discard(&staged);
                }
                id
            }
            Err(e) => {
                // Validation failed before the task existed, so the task
                // lifecycle will never clean this upload — remove it now.
                discard(&staged);
                return Err(engine_to_status(e));
            }
        };
        self.respond(&id, &accept).await
    }

    /// Task `id` as the response to a submission: once finished or after
    /// `wait_seconds`, or as it is now.
    async fn respond(&self, id: &str, accept: &Accept) -> Result<Task, Status> {
        let task = if accept.wait_seconds > 0 {
            let wait = Duration::from_secs(accept.wait_seconds.into());
            self.engine.wait_for(id, wait).await
        } else {
            self.engine.get_task(id).await
        }
        .map_err(engine_to_status)?
        .ok_or_else(|| Status::internal(format!("task '{id}' disappeared")))?;
        Ok(task_to_proto(task, accept.view))
    }

    /// A new owner-only file in the private upload dir.
    async fn create_upload(&self) -> Result<(PathBuf, tokio::fs::File), Status> {
        let engine = self.engine.clone();
        let dir = tokio::task::spawn_blocking(move || engine.ensure_upload_dir())
            .await
            .map_err(|e| Status::internal(format!("creating upload dir: {e}")))?
            .map_err(|e| Status::internal(format!("creating upload dir: {e}")))?;
        let path = dir.join(format!("{}.bin", uuid::Uuid::new_v4()));
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options
            .open(&path)
            .await
            .map_err(|e| Status::internal(format!("creating upload file: {e}")))?;
        Ok((path, file))
    }
}

/// Best-effort removal of an upload no task will read.
fn remove_upload(path: &Path) {
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(path = %path.display(), error = %e, "failed to remove upload file");
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
    use std::net::TcpListener;
    use std::sync::Arc;

    use apollo_config::{Config, DatabaseConfig, SqliteConfig};
    use apollo_domain::{Item, TaskState, Url};
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
        let input = Input::Image(Url {
            main: "https://example.com/a.png".into(),
            fallback: None,
        });
        apollo_domain::Task::new(
            id,
            vec![Item::new(input, vec!["m".into()], None)],
            owner.map(str::to_string),
        )
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
        GetTaskRequest {
            task_id: id.into(),
            view: 0,
        }
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
        let cancel = |id: &str| CancelTaskRequest { task_id: id.into() };
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

    fn classify_data(models: &[&str], request_id: &str) -> ClassifyRequest {
        use apollo_proto::{Media, ModelList, classify_request, media};
        ClassifyRequest {
            input: Some(classify_request::Input::Media(Media {
                kind: apollo_proto::MediaKind::Image as i32,
                source: Some(media::Source::Data(vec![1, 2, 3])),
                fallback_url: String::new(),
            })),
            run: Some(classify_request::Run::Models(ModelList {
                names: models.iter().map(|m| m.to_string()).collect(),
            })),
            request_id: request_id.into(),
            ..Default::default()
        }
    }

    fn uploads(engine: &Engine) -> usize {
        std::fs::read_dir(engine.upload_dir()).map_or(0, |d| d.count())
    }

    #[tokio::test]
    async fn bad_requests_are_rejected_and_leave_no_upload() {
        let f = fixture().await;
        let svc = InferenceService::new(f.engine.clone());
        let code = |r: Result<Response<Task>, Status>| r.unwrap_err().code();

        // No such model: the inline content was staged, then removed.
        let err = code(
            svc.classify(Request::new(classify_data(&["nope"], "")))
                .await,
        );
        assert_eq!(err, tonic::Code::InvalidArgument);
        assert_eq!(uploads(&f.engine), 0);

        let mut no_media = classify_data(&["m"], "");
        no_media.input = None;
        let mut no_run = classify_data(&["m"], "");
        no_run.run = None;
        let mut long_wait = classify_data(&["m"], "");
        long_wait.wait_seconds = 301;
        let mut big_metadata = classify_data(&["m"], "");
        big_metadata.metadata = (0..17).map(|i| (format!("k{i}"), String::new())).collect();
        for req in [no_media, no_run, long_wait, big_metadata] {
            assert_eq!(
                code(svc.classify(Request::new(req)).await),
                tonic::Code::InvalidArgument
            );
        }
        assert_eq!(uploads(&f.engine), 0);
    }

    #[tokio::test]
    async fn a_repeated_request_id_returns_the_callers_existing_task() {
        let f = fixture().await;
        let svc = InferenceService::new(f.engine.clone());
        let mut earlier = stored_task("earlier", Some("alice"));
        earlier.request_id = Some("upload-42".into());
        earlier.metadata = [("ref".to_string(), "42".to_string())].into();
        f.storage.create_task(&earlier).await.unwrap();

        let task = svc
            .classify(as_caller(
                classify_data(&["m"], "upload-42"),
                Some("alice"),
                false,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(task.id, "earlier");
        assert_eq!(task.request_id, "upload-42");
        assert_eq!(task.metadata["ref"], "42");
        assert_eq!(task.state(), apollo_proto::TaskState::Queued);
        assert_eq!(uploads(&f.engine), 0, "a repeat stages nothing");

        // Bob's request with the same key is his own (and fails validation).
        let err = svc
            .classify(as_caller(
                classify_data(&["m"], "upload-42"),
                Some("bob"),
                false,
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn list_tasks_shows_callers_their_own_tasks() {
        let f = fixture().await;
        let svc = InferenceService::new(f.engine.clone());
        for (id, owner) in [("a1", "alice"), ("a2", "alice"), ("b1", "bob")] {
            f.storage
                .create_task(&stored_task(id, Some(owner)))
                .await
                .unwrap();
        }
        let list = |subject: Option<&str>, admin: bool, req: ListTasksRequest| {
            let svc = svc.clone();
            let req = as_caller(req, subject, admin);
            async move {
                let page = svc.list_tasks(req).await.unwrap().into_inner();
                let mut ids: Vec<String> = page.tasks.into_iter().map(|t| t.id).collect();
                ids.sort();
                (ids, page.next_page_token)
            }
        };

        let (ids, _) = list(Some("alice"), false, Default::default()).await;
        assert_eq!(ids, ["a1", "a2"]);
        let (ids, _) = list(Some("ops"), true, Default::default()).await;
        assert_eq!(ids, ["a1", "a2", "b1"]);
        let (ids, _) = list(None, false, Default::default()).await;
        assert_eq!(ids.len(), 3);

        // One at a time: two pages, then no token.
        let mut seen = Vec::new();
        let mut token = String::new();
        loop {
            let req = ListTasksRequest {
                page_size: 1,
                page_token: token,
                ..Default::default()
            };
            let (ids, next) = list(Some("alice"), false, req).await;
            seen.extend(ids);
            if next.is_empty() {
                break;
            }
            token = next;
        }
        seen.sort();
        assert_eq!(seen, ["a1", "a2"]);

        let bad = ListTasksRequest {
            page_token: "garbage".into(),
            ..Default::default()
        };
        let err = svc.list_tasks(Request::new(bad)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn watch_task_streams_until_the_task_finishes() {
        use futures::StreamExt;
        let f = fixture().await;
        let svc = InferenceService::new(f.engine.clone());
        f.storage
            .create_task(&stored_task("w", Some("alice")))
            .await
            .unwrap();
        let watch = |subject| {
            let req = WatchTaskRequest {
                task_id: "w".into(),
                view: 0,
            };
            as_caller(req, Some(subject), false)
        };

        // Someone else's task is not found.
        let err = svc.watch_task(watch("bob")).await.err().unwrap();
        assert_eq!(err.code(), tonic::Code::NotFound);

        let mut stream = svc.watch_task(watch("alice")).await.unwrap().into_inner();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.state(), apollo_proto::TaskState::Queued);
        f.engine.cancel("w").await.unwrap();
        let last = stream.next().await.unwrap().unwrap();
        assert_eq!(last.state(), apollo_proto::TaskState::Cancelled);
        assert!(stream.next().await.is_none(), "the stream ends");
    }

    #[tokio::test]
    async fn list_models_reflects_the_config() {
        let f = fixture().await;
        let svc = InferenceService::new(f.engine.clone());
        let resp = svc
            .list_models(Request::new(ListModelsRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.models.is_empty() && resp.pipelines.is_empty());
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
