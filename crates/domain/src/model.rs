//! Shared descriptors: `Architecture` (vit, videomae, ...) and input `Modality`
//! (image / video / future text, audio).

use serde::{Deserialize, Serialize};

/// Concrete model architecture. Selects the candle builder and preprocessing in
/// `apollo-inference`; add a variant (plus its dispatch arm) to support a new family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Architecture {
    Vit,
    VideoMae,
}

/// What an architecture fundamentally classifies. Derived from `Architecture`
/// (the config `kind` field was dropped in favour of this).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelKind {
    ImageClassifier,
    VideoClassifier,
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

impl Architecture {
    /// The kind of classifier this architecture is.
    pub fn kind(self) -> ModelKind {
        match self {
            Architecture::Vit => ModelKind::ImageClassifier,
            Architecture::VideoMae => ModelKind::VideoClassifier,
        }
    }
}

impl ModelKind {
    /// Whether a model of this kind can classify an image input.
    pub fn accepts_image(self) -> bool {
        matches!(self, ModelKind::ImageClassifier)
    }

    /// Whether a model of this kind can classify a video as a whole clip.
    /// An image-classifier can also handle video, but only via a configured
    /// frame strategy — that check lives with the config/engine, since it
    /// depends on settings rather than the architecture alone.
    pub fn accepts_video_natively(self) -> bool {
        matches!(self, ModelKind::VideoClassifier)
    }
}
