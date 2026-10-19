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
use apollo_domain::{Architecture, Classification, DecodedImage, Label};

mod arch;
mod device;
mod error;
mod loader;
mod preprocess;

pub use device::{DeviceKind, open_device, select_device};
pub use error::InferenceError;
pub use preprocess::Normalization;

/// An image classifier: independent per-image classification, batched.
pub trait ImageClassifier: Send + Sync {
    /// Classify a batch of images, one [`Classification`] per input. Label
    /// selection is architecture-specific (vit: top-5 ∪ >0.90; siglip: a sigmoid
    /// threshold).
    fn classify(&self, images: &[&DecodedImage]) -> Result<Vec<Classification>, InferenceError>;
    /// The labels the model can predict (ids as in its predictions).
    fn labels(&self) -> &[Label];
    /// Side of the square input the model is fed; images of exactly this size
    /// skip resizing (video frames are extracted at this size).
    fn input_size(&self) -> u32;
}

/// The labels of the model `cfg` names, read without loading or downloading
/// anything: from the config (siglip `labels` or `taxonomy_file`) or the
/// repo's cached `config.json` (vit). `None` when they cannot be read that way
/// (e.g. a vit model that was never downloaded).
pub fn labels(cfg: &ModelConfig, cache_dir: Option<&Path>) -> Option<Vec<Label>> {
    match cfg.architecture {
        Architecture::Siglip => match cfg.taxonomy_file.as_deref() {
            Some(path) => {
                let tax = apollo_config::Taxonomy::load(Path::new(path)).ok()?;
                Some(
                    tax.children
                        .into_iter()
                        .map(|c| Label {
                            id: c.id,
                            name: c.name,
                        })
                        .collect(),
                )
            }
            None => Some(Label::indexed(&cfg.labels)),
        },
        Architecture::Vit => {
            let path = loader::cached_file(cfg, cache_dir, "config.json")?;
            let bytes = std::fs::read(path).ok()?;
            let names = loader::labels_from_config(&bytes).ok()?;
            Some(Label::indexed(&names))
        }
    }
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
