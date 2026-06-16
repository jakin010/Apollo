//! Storage error type.

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("corrupt stored data: {0}")]
    Corrupt(String),

    #[error("unsupported backend '{0}' (not implemented in this version)")]
    UnsupportedBackend(String),
}
