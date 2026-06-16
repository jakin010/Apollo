//! The `Inference` client wrapper.

use tonic::transport::{Channel, Endpoint};

use apollo_proto::inference_client::InferenceClient;
use apollo_proto::{ClassifyBatchRequest, ClassifyRequest, GetTaskRequest, InputItem, Task};

/// Errors talking to an apollo server.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc error: {0}")]
    Status(#[from] tonic::Status),
}

/// A connection to an apollo `Inference` server.
///
/// Cheap to clone (clones share one HTTP/2 channel) and safe to share across
/// tasks — the call methods take `&self`.
#[derive(Clone)]
pub struct Client {
    inner: InferenceClient<Channel>,
}

impl Client {
    /// Connect eagerly; errors immediately if the server is unreachable.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, ClientError> {
        let channel = Endpoint::from_shared(endpoint.into())?.connect().await?;
        Ok(Self {
            inner: InferenceClient::new(channel),
        })
    }

    /// Build a client that connects on first use and reconnects automatically.
    pub fn connect_lazy(endpoint: impl Into<String>) -> Result<Self, ClientError> {
        let channel = Endpoint::from_shared(endpoint.into())?.connect_lazy();
        Ok(Self {
            inner: InferenceClient::new(channel),
        })
    }

    /// Wrap a pre-built tonic [`Channel`] (custom TLS, interceptors, timeouts).
    pub fn with_channel(channel: Channel) -> Self {
        Self {
            inner: InferenceClient::new(channel),
        }
    }

    /// Submit a single input. Returns the new task id. Build `item` with the
    /// [`crate::item`] helpers.
    pub async fn classify(&self, item: InputItem) -> Result<String, ClientError> {
        let resp = self
            .inner
            .clone()
            .classify(ClassifyRequest { item: Some(item) })
            .await?;
        Ok(resp.into_inner().task_id)
    }

    /// Submit several inputs as one task. Returns the single task id.
    pub async fn classify_batch(&self, items: Vec<InputItem>) -> Result<String, ClientError> {
        let resp = self
            .inner
            .clone()
            .classify_batch(ClassifyBatchRequest { items })
            .await?;
        Ok(resp.into_inner().task_id)
    }

    /// Poll a task's state and per-item / per-model results.
    pub async fn get_task(&self, task_id: impl Into<String>) -> Result<Task, ClientError> {
        let resp = self
            .inner
            .clone()
            .get_task(GetTaskRequest {
                task_id: task_id.into(),
            })
            .await?;
        Ok(resp.into_inner())
    }
}
