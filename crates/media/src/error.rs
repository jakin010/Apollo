//! Media error type.

#[derive(Debug, thiserror::Error)]
pub enum MediaError {
    /// Every source (`main`, then `fallback`) failed. `transient` is set when at
    /// least one source failed in a way a retry could fix (network error, 5xx).
    #[error("all sources failed for '{input}': {}", .errors.join("; "))]
    AllSourcesFailed {
        input: String,
        errors: Vec<String>,
        transient: bool,
    },

    #[error("input not found: {0}")]
    NotFound(String),

    #[error("io error: {0}")]
    Io(String),

    /// A network-level failure (DNS, connect, TLS, body read) or a retryable HTTP
    /// status (5xx, 408, 429).
    #[error("http error: {0}")]
    Http(String),

    /// Refused by policy (scheme, SSRF guard, local-file gate, size cap) or a
    /// non-retryable HTTP status. Retrying the same request cannot succeed.
    #[error("refused: {0}")]
    Refused(String),

    #[error("image decode failed: {0}")]
    Decode(String),

    #[error("{0}")]
    Ffmpeg(String),

    #[error("failed to parse {0}")]
    Parse(String),

    #[error("misconfigured sampling step: {0}")]
    MisconfiguredStep(String),
}

impl MediaError {
    /// Whether retrying the same request could plausibly succeed.
    pub fn is_transient(&self) -> bool {
        match self {
            MediaError::AllSourcesFailed { transient, .. } => *transient,
            MediaError::Http(_) | MediaError::Io(_) => true,
            MediaError::NotFound(_)
            | MediaError::Refused(_)
            | MediaError::Decode(_)
            | MediaError::Ffmpeg(_)
            | MediaError::Parse(_)
            | MediaError::MisconfiguredStep(_) => false,
        }
    }

    /// Whether the input was retrieved but its content could not be decoded,
    /// probed, or accepted (as opposed to not being retrieved at all).
    pub fn is_content_error(&self) -> bool {
        matches!(
            self,
            MediaError::Decode(_) | MediaError::Ffmpeg(_) | MediaError::Parse(_)
        )
    }
}
