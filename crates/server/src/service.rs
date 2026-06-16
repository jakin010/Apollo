//! gRPC `Inference` service: `Classify` (single), `ClassifyBatch` (array -> one
//! task id), and `GetTask` (state + results, backed by storage). The `serve`
//! helpers also expose gRPC server reflection.

use std::future::Future;
use std::net::SocketAddr;

use tonic::{transport::Server, Request, Response, Status};

use apollo_engine::{Engine, EngineError};
use apollo_proto::inference_server::{Inference, InferenceServer};
use apollo_proto::{ClassifyBatchRequest, ClassifyRequest, GetTaskRequest, Task, TaskCreated};

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

    async fn classify_batch(
        &self,
        request: Request<ClassifyBatchRequest>,
    ) -> Result<Response<TaskCreated>, Status> {
        let items = request.into_inner().items;
        if items.is_empty() {
            return Err(Status::invalid_argument("batch contains no items"));
        }
        let item_count = items.len();
        let submissions = items
            .into_iter()
            .map(submission_from_proto)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Status::invalid_argument)?;
        let task_id = self
            .engine
            .submit(submissions)
            .await
            .map_err(engine_to_status)?;
        tracing::info!(task = %task_id, items = item_count, "accepted ClassifyBatch");
        Ok(Response::new(TaskCreated { task_id }))
    }

    async fn get_task(
        &self,
        request: Request<GetTaskRequest>,
    ) -> Result<Response<Task>, Status> {
        let id = request.into_inner().task_id;
        tracing::debug!(task = %id, "GetTask");
        match self.engine.get_task(&id).await.map_err(engine_to_status)? {
            Some(task) => Ok(Response::new(task_to_proto(task))),
            None => Err(Status::not_found(format!("task '{id}' not found"))),
        }
    }
}

/// Map engine errors to gRPC status codes. Submit-time validation failures are
/// client errors; everything else is internal.
fn engine_to_status(e: EngineError) -> Status {
    match e {
        EngineError::UnknownModel(_) | EngineError::Incompatible(_) | EngineError::Config(_) => {
            Status::invalid_argument(e.to_string())
        }
        other => Status::internal(other.to_string()),
    }
}

/// Wrap an [`Engine`] as a tonic service, ready to `add_service` to a `Server`.
pub fn inference_service(engine: Engine) -> InferenceServer<InferenceService> {
    InferenceServer::new(InferenceService::new(engine))
}

/// Build the gRPC reflection service from the descriptor set embedded in
/// `apollo-proto`. Panics only if that compiled-in descriptor is malformed.
fn reflection_service(
) -> tonic_reflection::server::v1::ServerReflectionServer<impl tonic_reflection::server::v1::ServerReflection>
{
    tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(apollo_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("embedded gRPC reflection descriptor must be valid")
}

/// Serve the `Inference` API (plus reflection) on `addr` until terminated.
pub async fn serve(engine: Engine, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
    tracing::info!(%addr, "serving Inference gRPC (reflection enabled)");
    Server::builder()
        .add_service(inference_service(engine))
        .add_service(reflection_service())
        .serve(addr)
        .await
}

/// Serve until `shutdown` resolves, draining in-flight RPCs (for graceful stop).
pub async fn serve_with_shutdown<F>(
    engine: Engine,
    addr: SocketAddr,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()>,
{
    tracing::info!(%addr, "serving Inference gRPC (graceful, reflection enabled)");
    Server::builder()
        .add_service(inference_service(engine))
        .add_service(reflection_service())
        .serve_with_shutdown(addr, shutdown)
        .await
}
