//! Engine error type.

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
