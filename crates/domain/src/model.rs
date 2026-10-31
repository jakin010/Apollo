//! Shared descriptors: `Architecture` (the model family), what a model takes
//! and gives (`ModelInput`, `OutputKind`), and input `Modality` (image /
//! video / text / future audio).

use serde::{Deserialize, Serialize};

/// Concrete model architecture. Selects the candle builder and preprocessing in
/// `apollo-inference`; add a variant (plus its dispatch arm in `apollo_inference::load`)
/// to support a new family. Image architectures also take video, by running
/// over sampled frames; text architectures take text. Most classify;
/// `paddleocr_vl` reads the text in images instead.
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
    /// PaddleOCR-VL: reads the text in images (OCR).
    #[serde(rename = "paddleocr_vl")]
    PaddleocrVl,
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
            Architecture::PaddleocrVl => "paddleocr_vl",
        }
    }

    /// The kind of input the architecture takes.
    pub fn input(self) -> ModelInput {
        match self {
            Architecture::Vit
            | Architecture::Siglip
            | Architecture::Pdq
            | Architecture::PaddleocrVl => ModelInput::Image,
            Architecture::Bert
            | Architecture::Distilbert
            | Architecture::XlmRoberta
            | Architecture::Modernbert
            | Architecture::DebertaV2 => ModelInput::Text,
        }
    }

    /// What the architecture gives for an input.
    pub fn output(self) -> OutputKind {
        match self {
            Architecture::PaddleocrVl => OutputKind::Text,
            Architecture::Vit
            | Architecture::Siglip
            | Architecture::Pdq
            | Architecture::Bert
            | Architecture::Distilbert
            | Architecture::XlmRoberta
            | Architecture::Modernbert
            | Architecture::DebertaV2 => OutputKind::Classification,
        }
    }
}

/// What a model takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelInput {
    /// Images, and video frame by frame.
    Image,
    Text,
}

/// What a model gives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputKind {
    /// Scores for its labels.
    Classification,
    /// The text it reads, with no labels (OCR).
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
