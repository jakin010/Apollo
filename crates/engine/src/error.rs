//! Engine error type.

use apollo_domain::{ErrorKind, TaskError};

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("storage error: {0}")]
    Storage(#[from] apollo_storage::StorageError),

    #[error("media error: {0}")]
    Media(#[from] apollo_media::MediaError),

    /// A stringified inference failure. Kept as `String` rather than
    /// `#[from] InferenceError` because one failure is fanned out to every waiter
    /// in a batch (so the payload must be cheaply cloneable) and `InferenceError`
    /// is not `Clone`.
    #[error("inference error: {0}")]
    Inference(String),

    /// The model's weights could not be loaded (download, parse, or a panic
    /// while loading).
    #[error("model unavailable: {0}")]
    ModelUnavailable(String),

    #[error("unknown or disabled model '{0}'")]
    UnknownModel(String),

    #[error("incompatible input/model: {0}")]
    Incompatible(String),

    #[error("unknown task '{0}'")]
    UnknownTask(String),

    #[error("overloaded: {0}")]
    Overloaded(String),

    #[error("cancelled")]
    Cancelled,

    #[error("configuration error: {0}")]
    Config(String),

    #[error("{0}")]
    Timeout(String),

    #[error("model worker unavailable")]
    WorkerGone,

    #[error("background task failed: {0}")]
    Join(String),

    #[error("engine is shutting down")]
    ShuttingDown,
}

impl EngineError {
    /// Whether retrying the same work could plausibly succeed. Input that was
    /// refused or could not be decoded, and configuration mismatches, are final.
    pub(crate) fn is_transient(&self) -> bool {
        match self {
            EngineError::Media(e) => e.is_transient(),
            EngineError::Storage(_)
            | EngineError::Inference(_)
            | EngineError::ModelUnavailable(_)
            | EngineError::Timeout(_)
            | EngineError::WorkerGone
            | EngineError::Join(_) => true,
            EngineError::UnknownModel(_)
            | EngineError::Incompatible(_)
            | EngineError::UnknownTask(_)
            | EngineError::Overloaded(_)
            | EngineError::Cancelled
            | EngineError::Config(_)
            | EngineError::ShuttingDown => false,
        }
    }

    /// The client-facing error for a failure while fetching / decoding the input.
    pub(crate) fn to_fetch_error(&self) -> TaskError {
        let kind = match self {
            EngineError::Media(e) if e.is_content_error() => ErrorKind::Decode,
            EngineError::Media(_) => ErrorKind::Fetch,
            // Fetched, but its content was rejected (e.g. a video over the
            // duration limit or a frame over the pixel cap).
            EngineError::Incompatible(_) => ErrorKind::Decode,
            EngineError::Timeout(_) => ErrorKind::Timeout,
            EngineError::Cancelled => ErrorKind::Cancelled,
            _ => ErrorKind::Internal,
        };
        TaskError::new(kind, self.to_string())
    }

    /// The client-facing error for a failure while running a model.
    pub(crate) fn to_model_error(&self) -> TaskError {
        let kind = match self {
            EngineError::Inference(_) => ErrorKind::Inference,
            EngineError::Timeout(_) => ErrorKind::Timeout,
            EngineError::ModelUnavailable(_) | EngineError::WorkerGone => {
                ErrorKind::ModelUnavailable
            }
            EngineError::Cancelled => ErrorKind::Cancelled,
            // Frame extraction / probing failed mid-scan: the content is the issue.
            EngineError::Media(e) if e.is_content_error() => ErrorKind::Decode,
            EngineError::Media(_) => ErrorKind::Fetch,
            _ => ErrorKind::Internal,
        };
        TaskError::new(kind, self.to_string())
    }
}
