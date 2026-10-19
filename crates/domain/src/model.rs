//! Shared descriptors: `Architecture` (the model family) and input `Modality`
//! (image / video / future text, audio).

use serde::{Deserialize, Serialize};

/// Concrete model architecture. Selects the candle builder and preprocessing in
/// `apollo-inference`; add a variant (plus its dispatch arm in `apollo_inference::load`)
/// to support a new family. All current architectures are image classifiers —
/// video inputs are handled by running an image classifier over sampled frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Architecture {
    Vit,
    Siglip,
    /// PDQ perceptual hashes matched against a block list.
    Pdq,
}

impl Architecture {
    /// The name used in the config (`architecture = "vit"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Architecture::Vit => "vit",
            Architecture::Siglip => "siglip",
            Architecture::Pdq => "pdq",
        }
    }
}

/// Modality of an input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Modality {
    Image,
    Video,
    Text,
    Audio,
}
