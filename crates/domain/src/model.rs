//! Shared descriptors: `Architecture` (the model family) and input `Modality`
//! (image / video / text / future audio).

use serde::{Deserialize, Serialize};

/// Concrete model architecture. Selects the candle builder and preprocessing in
/// `apollo-inference`; add a variant (plus its dispatch arm in `apollo_inference::load`)
/// to support a new family. Image architectures also take video, by running
/// over sampled frames; text architectures take text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Architecture {
    Vit,
    Siglip,
    /// PDQ perceptual hashes matched against a block list.
    Pdq,
    /// BERT text classifiers (`BertForSequenceClassification`).
    Bert,
    /// DistilBERT text classifiers.
    Distilbert,
    /// XLM-RoBERTa and RoBERTa text classifiers.
    #[serde(rename = "xlm_roberta", alias = "roberta")]
    XlmRoberta,
    /// ModernBERT text classifiers.
    Modernbert,
    /// DeBERTa-v2 and -v3 text classifiers.
    #[serde(rename = "deberta_v2")]
    DebertaV2,
}

impl Architecture {
    /// The name used in the config (`architecture = "vit"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Architecture::Vit => "vit",
            Architecture::Siglip => "siglip",
            Architecture::Pdq => "pdq",
            Architecture::Bert => "bert",
            Architecture::Distilbert => "distilbert",
            Architecture::XlmRoberta => "xlm_roberta",
            Architecture::Modernbert => "modernbert",
            Architecture::DebertaV2 => "deberta_v2",
        }
    }

    /// The kind of input the architecture classifies.
    pub fn input(self) -> ModelInput {
        match self {
            Architecture::Vit | Architecture::Siglip | Architecture::Pdq => ModelInput::Image,
            Architecture::Bert
            | Architecture::Distilbert
            | Architecture::XlmRoberta
            | Architecture::Modernbert
            | Architecture::DebertaV2 => ModelInput::Text,
        }
    }
}

/// What a model classifies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInput {
    /// Images, and video frame by frame.
    Image,
    Text,
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
