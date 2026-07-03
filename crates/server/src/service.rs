//! gRPC `Inference` service: `Classify` (single input -> one task id), `GetTask`
//! (state + results, backed by storage), and `CancelTask`. The `serve` helpers
//! also expose gRPC server reflection.

use std::future::Future;
use std::net::SocketAddr;

use tokio::io::AsyncWriteExt;
use tonic::service::interceptor::InterceptedService;
use tonic::{Request, Response, Status, transport::Server};

use apollo_domain::Input;
use apollo_engine::{Engine, EngineError, Submission};
use apollo_proto::classify_chunk::Payload;
use apollo_proto::inference_server::{Inference, InferenceServer};
use apollo_proto::{
    CancelRequest, ClassifyChunk, ClassifyRequest, GetTaskRequest, Task, TaskCreated,
};

use crate::auth::AuthInterceptor;
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
        let item = request
            .into_inner()
            .item
            .ok_or_else(|| Status::invalid_argument("request has no item"))?;
        let model_count = item.models.len();
        let submission = submission_from_proto(item).map_err(Status::invalid_argument)?;
        let task_id = self
            .engine
            .submit(vec![submission])
            .await
            .map_err(engine_to_status)?;
        tracing::info!(task = %task_id, models = model_count, "accepted Classify");
        Ok(Response::new(TaskCreated { task_id }))
    }

    async fn get_task(&self, request: Request<GetTaskRequest>) -> Result<Response<Task>, Status> {
        let id = request.into_inner().task_id;
        tracing::debug!(task = %id, "GetTask");
        match self.engine.get_task(&id).await.map_err(engine_to_status)? {
            Some(task) => Ok(Response::new(task_to_proto(task))),
            None => Err(Status::not_found(format!("task '{id}' not found"))),
        }
    }

    async fn cancel_task(&self, request: Request<CancelRequest>) -> Result<Response<Task>, Status> {
        let id = request.into_inner().task_id;
        tracing::info!(task = %id, "CancelTask");
        self.engine.cancel(&id).await.map_err(engine_to_status)?;
        match self.engine.get_task(&id).await.map_err(engine_to_status)? {
            Some(task) => Ok(Response::new(task_to_proto(task))),
            None => Err(Status::not_found(format!("task '{id}' not found"))),
        }
    }

    async fn classify_stream(
        &self,
        request: Request<tonic::Streaming<ClassifyChunk>>,
    ) -> Result<Response<TaskCreated>, Status> {
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
        if init.models.is_empty() {
            return Err(Status::invalid_argument("init frame lists no models"));
        }

        // Stage the streamed bytes to a file under the upload dir.
        let dir = self.engine.upload_dir();
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| Status::internal(format!("creating upload dir: {e}")))?;
        let path = dir.join(format!("{}.bin", uuid::Uuid::new_v4()));
        let cap = self.engine.max_upload_bytes();

        let mut file = tokio::fs::File::create(&path)
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
            pipeline: None,
        };
        let model_count = submission.models.len();
        let task_id = match self.engine.submit(vec![submission]).await {
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
/// shared messages) only. Panics only if the compiled-in descriptor is malformed.
fn reflection_service() -> tonic_reflection::server::v1::ServerReflectionServer<
    impl tonic_reflection::server::v1::ServerReflection,
> {
    tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(apollo_proto::INFERENCE_DESCRIPTOR_SET)
        .build_v1()
        .expect("embedded gRPC reflection descriptor must be valid")
}

/// Serve the `Inference` API (plus reflection) on `addr` until terminated.
pub async fn serve(
    engine: Engine,
    addr: SocketAddr,
    auth: AuthInterceptor,
) -> Result<(), tonic::transport::Error> {
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<InferenceServer<InferenceService>>()
        .await;
    tracing::info!(%addr, "serving Inference gRPC (reflection + health enabled)");
    Server::builder()
        .add_service(inference_service(engine, auth))
        .add_service(reflection_service())
        .add_service(health_service)
        .serve(addr)
        .await
}

/// Serve until `shutdown` resolves, draining in-flight RPCs (for graceful stop).
pub async fn serve_with_shutdown<F>(
    engine: Engine,
    addr: SocketAddr,
    shutdown: F,
    auth: AuthInterceptor,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()>,
{
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<InferenceServer<InferenceService>>()
        .await;
    tracing::info!(%addr, "serving Inference gRPC (graceful, reflection + health enabled)");
    Server::builder()
        .add_service(inference_service(engine, auth))
        .add_service(reflection_service())
        .add_service(health_service)
        .serve_with_shutdown(addr, shutdown)
        .await
}
