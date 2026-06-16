//! Configuration error type.

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config '{0}': {1}")]
    Io(PathBuf, #[source] std::io::Error),

    #[error("failed to parse config: {0}")]
    Parse(String),

    #[error("failed to parse config for editing: {0}")]
    Edit(String),

    #[error("{0}")]
    EditOp(String),

    #[error("invalid config:\n  - {}", .0.join("\n  - "))]
    Validation(Vec<String>),
}
