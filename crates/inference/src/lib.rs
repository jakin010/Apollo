//! `apollo-inference` — load Hugging Face image and text classifiers, and
//! models that read the text in images, with candle and run them on the best
//! available device.
//!
//! [`select_device`] picks CUDA → Metal → CPU (Metal is compiled in automatically
//! on macOS; CUDA is opt-in via the `cuda` feature). [`load`] downloads a model
//! from the Hub (cached) and returns a [`LoadedModel`]: an [`ImageClassifier`]
//! (images, and video frame by frame), a [`TextClassifier`], or an
//! [`ImageToText`] model (OCR). The `arch` module is the seam where model
//! families plug in.
//!
//! Loading and inference are synchronous (candle + safetensors mmap + Hub I/O);
//! the async engine runs them on blocking threads.

use std::path::Path;
use std::sync::Arc;

use candle_core::Device;

use apollo_config::ModelConfig;
use apollo_domain::{Architecture, Classification, DecodedImage, Label};

mod arch;
mod device;
mod error;
mod loader;
mod preprocess;

pub use arch::pdq::{
    DEFAULT_MAX_DISTANCE as PDQ_DEFAULT_MAX_DISTANCE,
    DEFAULT_MIN_QUALITY as PDQ_DEFAULT_MIN_QUALITY, ListEntry as PdqListEntry, PdqHash,
    parse_hash_list as parse_pdq_hash_list, pdq_dihedral_hashes, pdq_hash,
};
pub use device::{DeviceKind, open_device, select_device};
pub use error::InferenceError;
pub use preprocess::{Normalization, resize_to_input};

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
    /// Whether `classify` only uses an image after resizing it to
    /// `input_size` with [`resize_to_input`], so a caller may resize ahead of
    /// time, off the model's thread, and get the same result. `false` for a
    /// model that reads the full image (`pdq`).
    fn resizes_to_input(&self) -> bool {
        false
    }
}

/// A text classifier: independent per-text classification, batched.
pub trait TextClassifier: Send + Sync {
    /// Classify a batch of texts, one [`Classification`] per input: the top 5
    /// labels plus any above 0.90.
    fn classify(&self, texts: &[&str]) -> Result<Vec<Classification>, InferenceError>;
    /// The labels the model can predict (ids as in its predictions).
    fn labels(&self) -> &[Label];
}

/// A model that reads the text in images (OCR), one image at a time.
pub trait ImageToText: Send + Sync {
    /// The text in each image, one string per input (empty when there is
    /// none). Each image is scaled by the model itself (see [`read_size`]).
    fn read(&self, images: &[&DecodedImage]) -> Result<Vec<String>, InferenceError>;
    /// A short run that gets the device ready (GPU kernels compiled, buffers
    /// allocated), so the first real image doesn't pay for it.
    fn warm_up(&self) -> Result<(), InferenceError> {
        let blank = DecodedImage {
            width: 1,
            height: 1,
            data: vec![0; 3],
        };
        self.read(&[&blank]).map(drop)
    }
}

/// A loaded model, by what it takes and gives.
pub enum LoadedModel {
    Image(Box<dyn ImageClassifier>),
    Text(Box<dyn TextClassifier>),
    /// Reads the text in images.
    ImageToText(Box<dyn ImageToText>),
}

impl LoadedModel {
    /// The labels it predicts; none for a model that reads text.
    pub fn labels(&self) -> &[Label] {
        match self {
            LoadedModel::Image(m) => m.labels(),
            LoadedModel::Text(m) => m.labels(),
            LoadedModel::ImageToText(_) => &[],
        }
    }

    /// Run a tiny input through the model: the first run compiles GPU kernels
    /// and allocates buffers, and can take seconds that would otherwise come
    /// out of the first real request's timeout.
    pub fn warm_up(&self) -> Result<(), InferenceError> {
        match self {
            LoadedModel::Image(m) => {
                let warm = DecodedImage {
                    width: 1,
                    height: 1,
                    data: vec![0; 3],
                };
                m.classify(&[&warm]).map(drop)
            }
            LoadedModel::Text(m) => m.classify(&["warm up"]).map(drop),
            LoadedModel::ImageToText(m) => m.warm_up(),
        }
    }
}

/// The size a model that reads text (`paddleocr_vl`) reads an image of
/// `width`×`height` at: its shape kept, scaled to the model's pixel range
/// (capped by the config's `max_pixels`), each side on its patch grid. Video
/// frames are extracted at this size. `None` for other architectures (they
/// take a fixed square, see [`ImageClassifier::input_size`]), or when the
/// image is too long and thin for the model.
pub fn read_size(cfg: &ModelConfig, width: u32, height: u32) -> Option<(u32, u32)> {
    match cfg.architecture {
        Architecture::PaddleocrVl => {
            arch::paddleocr_vl::fit(width, height, arch::paddleocr_vl::max_pixels(cfg))
        }
        _ => None,
    }
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
        Architecture::Pdq => arch::pdq::category_labels(cfg).ok(),
        // Reads text; no labels.
        Architecture::PaddleocrVl => Some(Vec::new()),
        Architecture::Vit
        | Architecture::Bert
        | Architecture::Distilbert
        | Architecture::XlmRoberta
        | Architecture::Modernbert
        | Architecture::DebertaV2 => {
            let path = loader::cached_file(cfg, cache_dir, "config.json")?;
            let bytes = std::fs::read(path).ok()?;
            let names = loader::labels_from_config(&bytes).ok()?;
            Some(Label::indexed(&names))
        }
    }
}

/// The contents of the files a model reads besides its weights (a taxonomy,
/// a hash list), read when the config is applied. A model loads from these,
/// not from disk, so it always loads what was validated, also after an idle
/// unload, until a reload reads the files again.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelFiles {
    pub taxonomy: Option<Arc<str>>,
    pub hash_list: Option<Arc<str>>,
}

impl ModelFiles {
    /// Read `cfg`'s files now. One that cannot be read is left out, and
    /// loading the model then fails with the reason.
    pub fn read(cfg: &ModelConfig) -> Self {
        let read = |path: &Option<String>| {
            path.as_deref()
                .and_then(|p| std::fs::read_to_string(p).ok())
                .map(Arc::from)
        };
        Self {
            taxonomy: read(&cfg.taxonomy_file),
            hash_list: read(&cfg.hash_list),
        }
    }
}

/// Download (if not cached) and load the model named by `cfg` onto `device`,
/// with its `files` as read by [`ModelFiles::read`]. `cache_dir` overrides the
/// Hugging Face cache location when set.
pub fn load(
    cfg: &ModelConfig,
    files: &ModelFiles,
    device: &Device,
    cache_dir: Option<&Path>,
) -> Result<LoadedModel, InferenceError> {
    let image = |m: Box<dyn ImageClassifier>| Ok(LoadedModel::Image(m));
    match cfg.architecture {
        Architecture::Vit => {
            let hub = loader::Hub::open(cfg, cache_dir)?;
            image(Box::new(arch::vit::load(&hub, device)?))
        }
        Architecture::Siglip => {
            let hub = loader::Hub::open(cfg, cache_dir)?;
            image(Box::new(arch::siglip::load(&hub, cfg, files, device)?))
        }
        // Runs on the CPU, with no weights to fetch.
        Architecture::Pdq => image(Box::new(arch::pdq::load(cfg, files)?)),
        Architecture::Bert
        | Architecture::Distilbert
        | Architecture::XlmRoberta
        | Architecture::Modernbert
        | Architecture::DebertaV2 => {
            let hub = loader::Hub::open(cfg, cache_dir)?;
            Ok(LoadedModel::Text(Box::new(arch::text::load(
                &hub, cfg, device,
            )?)))
        }
        Architecture::PaddleocrVl => {
            let hub = loader::Hub::open(cfg, cache_dir)?;
            Ok(LoadedModel::ImageToText(Box::new(
                arch::paddleocr_vl::load(&hub, cfg, device)?,
            )))
        }
    }
}
