//! SigLIP zero-shot image classifier (candle-transformers `siglip`).
//!
//! Unlike the ViT head (fixed classes baked into the weights), SigLIP scores an
//! image against a configured set of text prompts in a shared embedding space.
//! The prompts are tokenized and encoded once at load; classification then
//! encodes the image, takes its cosine similarity to each prompt embedding,
//! applies SigLIP's learned scale + bias and a sigmoid — yielding an INDEPENDENT
//! probability per prompt.
//!
//! Two shapes of configuration are supported:
//!   * **flat** — a `labels` list. Each label is a candidate; results are the
//!     labels scoring at/above a threshold (each `Prediction.label` is the
//!     label's index). "Nothing matches" is a real outcome, which is why a
//!     threshold beats top-k here.
//!   * **taxonomy** — a `taxonomy_file` grouping prompts into parent/child
//!     categories. Each child's prompt scores are aggregated (mean/max) into one
//!     child score; the children scoring at/above `score_threshold` are returned
//!     flat in `predictions` (each `Prediction.label` is the child id), highest
//!     first. The parent grouping is NOT duplicated into `groups` — it is just
//!     these children keyed by parent, reconstructable from the taxonomy. With no
//!     `score_threshold` set, every child is kept.

use std::path::Path;

use candle_core::{D, DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::siglip;
use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

use apollo_config::{Aggregation, ModelConfig, TaxonChild, Taxonomy};
use apollo_domain::{Classification, DecodedImage, Prediction};

use crate::ImageClassifier;
use crate::error::InferenceError;
use crate::loader::Hub;
use crate::preprocess::{self, Normalization};

/// Per-label keep threshold for flat mode when the config doesn't set one.
const DEFAULT_THRESHOLD: f32 = 0.5;
/// SigLIP's canonical fixed text sequence length.
const DEFAULT_MAX_TOKENS: usize = 64;

/// How results are selected, decided at load from the config shape.
enum Mode {
    /// Plain `labels`: threshold + optional cap; `label` is the label index.
    Flat {
        threshold: f32,
        max_results: Option<usize>,
    },
    /// `taxonomy_file`: aggregate per child, group by parent; `label` is child id.
    Taxonomy {
        threshold: f32,
        children: Vec<TaxonChild>,
    },
}

/// A loaded SigLIP classifier: the model, the precomputed (L2-normalized) prompt
/// embeddings, the learned scale/bias, and the selection mode.
pub(crate) struct SiglipClassifier {
    model: siglip::Model,
    /// L2-normalized prompt embeddings, shape `(num_prompts, dim)`.
    text_embeds: Tensor,
    logit_scale: Tensor,
    logit_bias: Tensor,
    /// For the `labels()` accessor: the flat labels, or the taxonomy's prompts.
    labels: Vec<String>,
    norm: Normalization,
    mode: Mode,
    device: Device,
}

/// Load a SigLIP classifier from a Hub repo. Expects `config.json` (HF
/// `SiglipConfig`), `model.safetensors`, and `tokenizer.json`. Candidate prompts
/// come from the model's `[models.<label>]` config (a `labels` list or a
/// `taxonomy_file`), not from the weights.
pub(crate) fn load(
    hub: &Hub,
    cfg: &ModelConfig,
    device: &Device,
) -> Result<SiglipClassifier, InferenceError> {
    let config_bytes = std::fs::read(hub.file("config.json")?)?;
    let config: siglip::Config = serde_json::from_slice(&config_bytes)
        .map_err(|e| InferenceError::Config(format!("siglip config: {e}")))?;

    let weights = hub.file("model.safetensors").map_err(|_| {
        InferenceError::Config(
            "expected 'model.safetensors' in the repo \
             (sharded or .bin weights are not supported in this build)"
                .into(),
        )
    })?;
    // SAFETY: mmap of a trusted, locally-cached safetensors file.
    let vb = unsafe { VarBuilder::from_mmaped_safetensors(&[weights], DType::F32, device)? };
    let model = siglip::Model::new(&config, vb.clone())?;
    // SigLIP's learned temperature + bias sit at the top level of the weights.
    let logit_scale = vb.get(&[1], "logit_scale")?;
    let logit_bias = vb.get(&[1], "logit_bias")?;

    // Decide the candidate prompts and the selection mode from the config shape.
    // (Config validation guarantees `labels` and `taxonomy_file` are not both
    // set; here we just dispatch on whichever is present.)
    let (text_inputs, mode, labels) = match cfg.taxonomy_file.as_deref() {
        Some(path) => {
            let tax = Taxonomy::load(Path::new(path))
                .map_err(|e| InferenceError::Config(format!("taxonomy: {e}")))?;
            // Taxonomy prompts are complete phrases — used verbatim, no template.
            (
                tax.prompts.clone(),
                Mode::Taxonomy {
                    // Unlike flat mode (where the threshold is the selection
                    // mechanism), taxonomy historically returned every child; an
                    // unset threshold preserves that (0.0 keeps everything).
                    threshold: cfg.score_threshold.unwrap_or(0.0),
                    children: tax.children,
                },
                tax.prompts,
            )
        }
        None => {
            if cfg.labels.is_empty() {
                return Err(InferenceError::Config(
                    "siglip models require a `labels` list or a `taxonomy_file`".into(),
                ));
            }
            (
                build_prompts(&cfg.labels, cfg.prompt_template.as_deref()),
                Mode::Flat {
                    threshold: cfg.score_threshold.unwrap_or(DEFAULT_THRESHOLD),
                    max_results: cfg.max_results,
                },
                cfg.labels.clone(),
            )
        }
    };

    // Encode the prompts once; reuse the embeddings for every image.
    let tokenizer = load_tokenizer(hub, config.text_config.max_position_embeddings)?;
    let input_ids = tokenize(&tokenizer, &text_inputs, device)?;
    let text_features = model.get_text_features(&input_ids)?;
    let text_embeds = l2_normalize(&text_features)?;

    let norm = preprocess::normalization_for(hub, config.vision_config.image_size);

    Ok(SiglipClassifier {
        model,
        text_embeds,
        logit_scale,
        logit_bias,
        labels,
        norm,
        mode,
        device: device.clone(),
    })
}

impl ImageClassifier for SiglipClassifier {
    fn classify(&self, images: &[DecodedImage]) -> Result<Vec<Classification>, InferenceError> {
        if images.is_empty() {
            return Ok(Vec::new());
        }
        let batch = preprocess::preprocess(images, &self.norm, &self.device)?;
        let image_features = self.model.get_image_features(&batch)?;
        let image_embeds = l2_normalize(&image_features)?; // (B, dim)

        // Cosine similarity (both sides L2-normalized): (B, dim) x (dim, P) = (B, P).
        let sims = image_embeds.matmul(&self.text_embeds.t()?.contiguous()?)?;
        // SigLIP logit = scale.exp() * sim + bias; probability = sigmoid(logit),
        // computed as 1 / (1 + exp(-logit)).
        let scale = self.logit_scale.exp()?;
        let logits = sims
            .broadcast_mul(&scale)?
            .broadcast_add(&self.logit_bias)?;
        let probs = logits.neg()?.exp()?.affine(1.0, 1.0)?.recip()?;
        let rows: Vec<Vec<f32>> = probs.to_vec2()?;

        let out = rows
            .into_iter()
            .map(|row| match &self.mode {
                Mode::Flat {
                    threshold,
                    max_results,
                } => flat_classification(&row, *threshold, *max_results),
                Mode::Taxonomy {
                    threshold,
                    children,
                } => taxonomy_classification(&row, *threshold, children),
            })
            .collect();
        Ok(out)
    }

    fn labels(&self) -> &[String] {
        &self.labels
    }
}

/// Flat mode: every label scoring at/above `threshold`, highest first, capped by
/// `max_results`. `label` is the label's index in the configured list.
fn flat_classification(row: &[f32], threshold: f32, max_results: Option<usize>) -> Classification {
    let mut preds = row
        .iter()
        .enumerate()
        .map(|(i, &score)| Prediction {
            label: i as u32,
            score,
        })
        .filter(|p| p.score >= threshold)
        .collect::<Vec<_>>();
    preds.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if let Some(k) = max_results {
        preds.truncate(k);
    }
    Classification { predictions: preds }
}

/// Taxonomy mode: aggregate each child's prompt scores (mean/max) into one child
/// score, keep the children scoring at/above `threshold`, and return them flat in
/// `predictions` (highest first; `label` is the child id). The per-parent
/// grouping is left to the caller, who can reconstruct it from the taxonomy.
fn taxonomy_classification(row: &[f32], threshold: f32, children: &[TaxonChild]) -> Classification {
    let mut predictions = children
        .iter()
        .map(|child| {
            let slice = &row[child.prompt_start..child.prompt_start + child.prompt_len];
            let score = match child.aggregation {
                Aggregation::Max => slice.iter().copied().fold(f32::MIN, f32::max),
                Aggregation::Mean => slice.iter().sum::<f32>() / slice.len() as f32,
            };
            Prediction {
                label: child.id,
                score,
            }
        })
        .filter(|p| p.score >= threshold)
        .collect::<Vec<_>>();
    predictions.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Classification { predictions }
}

/// Wrap each label in the prompt template (replacing a `{}` placeholder, or
/// prefixing if there is none), or use the labels verbatim when no template.
fn build_prompts(labels: &[String], template: Option<&str>) -> Vec<String> {
    match template {
        Some(t) if t.contains("{}") => labels.iter().map(|l| t.replace("{}", l)).collect(),
        Some(t) => labels.iter().map(|l| format!("{t} {l}")).collect(),
        None => labels.to_vec(),
    }
}

/// Load the repo's fast tokenizer and pin padding + truncation to SigLIP's fixed
/// sequence length (the text tower pools the last position, so every prompt must
/// be padded to the same length).
fn load_tokenizer(hub: &Hub, max_len: usize) -> Result<Tokenizer, InferenceError> {
    let path = hub.file("tokenizer.json").map_err(|_| {
        InferenceError::Config(
            "expected 'tokenizer.json' in the siglip repo (the fast tokenizer)".into(),
        )
    })?;
    let mut tokenizer = Tokenizer::from_file(path)
        .map_err(|e| InferenceError::Config(format!("tokenizer: {e}")))?;
    let len = if max_len == 0 {
        DEFAULT_MAX_TOKENS
    } else {
        max_len
    };
    tokenizer.with_padding(Some(PaddingParams {
        strategy: PaddingStrategy::Fixed(len),
        ..Default::default()
    }));
    tokenizer
        .with_truncation(Some(TruncationParams {
            max_length: len,
            ..Default::default()
        }))
        .map_err(|e| InferenceError::Config(format!("tokenizer truncation: {e}")))?;
    Ok(tokenizer)
}

/// Tokenize prompts into a `(num_prompts, seq_len)` U32 tensor.
fn tokenize(
    tokenizer: &Tokenizer,
    prompts: &[String],
    device: &Device,
) -> Result<Tensor, InferenceError> {
    let encodings = tokenizer
        .encode_batch(prompts.to_vec(), true)
        .map_err(|e| InferenceError::Config(format!("tokenize: {e}")))?;
    let rows = encodings.len();
    let seq = encodings.first().map(|e| e.get_ids().len()).unwrap_or(0);
    let mut ids = Vec::with_capacity(rows * seq);
    for enc in &encodings {
        ids.extend_from_slice(enc.get_ids());
    }
    Tensor::from_vec(ids, (rows, seq), device).map_err(InferenceError::from)
}

/// L2-normalize each row (last dim) of a 2-D embedding tensor.
fn l2_normalize(t: &Tensor) -> Result<Tensor, InferenceError> {
    let norm = t.sqr()?.sum_keepdim(D::Minus1)?.sqrt()?;
    Ok(t.broadcast_div(&norm)?)
}
