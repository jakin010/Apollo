//! Engine error type.

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("storage error: {0}")]
    Storage(#[from] apollo_storage::StorageError),

    #[error("media error: {0}")]
    Media(String),

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

pub(crate) fn media_err(e: apollo_media::MediaError) -> EngineError {
    EngineError::Media(e.to_string())
}
