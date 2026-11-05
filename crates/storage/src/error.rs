//! Storage error type.

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("surrealdb error: {0}")]
    Surreal(String),

    /// The database didn't answer within `[database].timeout`, so it may be
    /// unreachable: connecting at startup, or a client's call (the engine's).
    #[error("the database didn't answer within {0:?}; it may be unreachable")]
    Timeout(std::time::Duration),

    #[error("corrupt stored data: {0}")]
    Corrupt(String),

    #[error("invalid stored state: {0}")]
    State(#[from] apollo_domain::ParseStateError),

    #[error("unsupported backend '{0}' (not implemented in this version)")]
    UnsupportedBackend(String),
}
