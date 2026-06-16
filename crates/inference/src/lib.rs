//! `apollo-inference` — load Hugging Face classification models with candle and
//! run them on the best available device.
//!
//! [`select_device`] picks CUDA → Metal → CPU (GPU backends are only attempted
//! when compiled in via the `cuda` / `metal` features). [`load`] downloads a model
//! from the Hub (cached) and returns a [`Loaded`] classifier; the `arch` module is
//! the seam where model families plug in. Image classifiers ([`ImageClassifier`])
//! cover image inputs and per-frame video scans; video classifiers
//! ([`VideoClassifier`]) take a whole clip.
//!
//! Loading and inference are synchronous (candle + safetensors mmap + Hub I/O);
//! the async engine runs them on blocking tasks.

use std::path::Path;

use candle_core::Device;

use apollo_config::ModelConfig;
use apollo_domain::{Architecture, Classification, DecodedImage};

mod arch;
mod device;
mod error;
mod loader;
mod preprocess;

pub use device::{select_device, DeviceKind};
pub use error::InferenceError;
pub use preprocess::Normalization;

/// An image classifier: independent per-image classification, batched.
pub trait ImageClassifier: Send + Sync {
    /// Classify a batch of images, one [`Classification`] per input (top-5 ∪ >0.90).
    fn classify(&self, images: &[DecodedImage]) -> Result<Vec<Classification>, InferenceError>;
    /// The model's label set, ordered by class index.
    fn labels(&self) -> &[String];
}

/// A video classifier: a clip of frames in, one classification out.
pub trait VideoClassifier: Send + Sync {
    /// Classify a single clip of [`clip_len`](Self::clip_len) frames.
    fn classify_clip(&self, frames: &[DecodedImage]) -> Result<Classification, InferenceError>;
    /// Number of frames the model expects in a clip.
    fn clip_len(&self) -> usize;
    /// The model's label set, ordered by class index.
    fn labels(&self) -> &[String];
}

/// A loaded model, dispatched by architecture kind.
pub enum Loaded {
    /// Image-classifier (e.g. ViT) — also used for per-frame video scans.
    Image(Box<dyn ImageClassifier>),
    /// Whole-clip video-classifier (e.g. VideoMAE).
    Video(Box<dyn VideoClassifier>),
}

/// Download (if not cached) and load the model named by `cfg` onto `device`.
/// `cache_dir` overrides the Hugging Face cache location when set.
pub fn load(
    cfg: &ModelConfig,
    device: &Device,
    cache_dir: Option<&Path>,
) -> Result<Loaded, InferenceError> {
    let hub = loader::Hub::open(cfg, cache_dir)?;
    match cfg.architecture {
        Architecture::Vit => Ok(Loaded::Image(Box::new(arch::vit::load(&hub, device)?))),
        Architecture::VideoMae => Ok(Loaded::Video(Box::new(arch::videomae::load(&hub, device)?))),
    }
}
