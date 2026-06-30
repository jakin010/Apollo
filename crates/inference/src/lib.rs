//! `apollo-inference` — load Hugging Face image-classification models with candle
//! and run them on the best available device.
//!
//! [`select_device`] picks CUDA → Metal → CPU (Metal is compiled in automatically
//! on macOS; CUDA is opt-in via the `cuda` feature). [`load`] downloads a model
//! from the Hub (cached) and returns a boxed [`ImageClassifier`]; the `arch`
//! module is the seam where model families plug in. Image classifiers cover both
//! image inputs and per-frame video scans.
//!
//! Loading and inference are synchronous (candle + safetensors mmap + Hub I/O);
//! the async engine runs them on blocking threads.

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
    /// Classify a batch of images, one [`Classification`] per input. Label
    /// selection is architecture-specific (vit: top-5 ∪ >0.90; siglip: a sigmoid
    /// threshold).
    fn classify(&self, images: &[DecodedImage]) -> Result<Vec<Classification>, InferenceError>;
    /// The model's label set, ordered by class index.
    fn labels(&self) -> &[String];
}

/// Download (if not cached) and load the model named by `cfg` onto `device`.
/// `cache_dir` overrides the Hugging Face cache location when set.
pub fn load(
    cfg: &ModelConfig,
    device: &Device,
    cache_dir: Option<&Path>,
) -> Result<Box<dyn ImageClassifier>, InferenceError> {
    let hub = loader::Hub::open(cfg, cache_dir)?;
    match cfg.architecture {
        Architecture::Vit => Ok(Box::new(arch::vit::load(&hub, device)?)),
        Architecture::Siglip => Ok(Box::new(arch::siglip::load(&hub, cfg, device)?)),
    }
}
