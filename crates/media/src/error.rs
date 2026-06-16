//! Media error type.

#[derive(Debug, thiserror::Error)]
pub enum MediaError {
    #[error("all sources failed for '{input}': {}", .errors.join("; "))]
    AllSourcesFailed { input: String, errors: Vec<String> },

    #[error("input not found: {0}")]
    NotFound(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("http error: {0}")]
    Http(String),

    #[error("image decode failed: {0}")]
    Decode(String),

    #[error("{0}")]
    Ffmpeg(String),

    #[error("failed to parse {0}")]
    Parse(String),

    #[error("misconfigured sampling step: {0}")]
    MisconfiguredStep(String),
}
