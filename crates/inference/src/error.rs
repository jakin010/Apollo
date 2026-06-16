//! Inference error type.

#[derive(Debug, thiserror::Error)]
pub enum InferenceError {
    #[error("candle error: {0}")]
    Candle(#[from] candle_core::Error),

    #[error("hub download error: {0}")]
    Hub(String),

    #[error("model config error: {0}")]
    Config(String),

    #[error("preprocess error: {0}")]
    Preprocess(String),

    #[error("unsupported: {0}")]
    Unsupported(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
