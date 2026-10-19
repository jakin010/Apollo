//! SigLIP zero-shot image classifier (candle-transformers `siglip`).
//!
//! Unlike the ViT head (fixed classes baked into the weights), SigLIP scores an
//! image against a configured set of text prompts in a shared embedding space.
//! The prompts are tokenized and encoded once at load (in chunks, and cached on
//! disk next to the model so later loads skip the text tower); classification then
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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use candle_core::{D, DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::siglip;
use sha2::{Digest, Sha256};
use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

use apollo_config::{Aggregation, ModelConfig, TaxonChild, Taxonomy};
use apollo_domain::{Classification, DecodedImage, Label, Prediction};

use crate::ImageClassifier;
use crate::error::InferenceError;
use crate::loader::Hub;
use crate::preprocess::{self, Normalization};

/// Per-label keep threshold for flat mode when the config doesn't set one.
const DEFAULT_THRESHOLD: f32 = 0.5;
/// SigLIP's canonical fixed text sequence length.
const DEFAULT_MAX_TOKENS: usize = 64;
/// Prompts per text-tower forward pass at load. Bounds activation memory: a
/// ~1k-prompt taxonomy in one pass peaked at ~7 GB.
const PROMPT_CHUNK: usize = 32;
/// Bump when the cached embedding format or its inputs change.
const EMBED_CACHE_VERSION: &str = "apollo-siglip-prompt-embeds-v1";

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
    /// The flat labels by index, or the taxonomy's children by id.
    labels: Vec<Label>,
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
    files: &crate::ModelFiles,
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
    let vb = unsafe { VarBuilder::from_mmaped_safetensors(&[&weights], DType::F32, device)? };
    let model = siglip::Model::new(&config, vb.clone())?;
    // SigLIP's learned temperature + bias sit at the top level of the weights.
    let logit_scale = vb.get(&[1], "logit_scale")?;
    let logit_bias = vb.get(&[1], "logit_bias")?;

    // Decide the candidate prompts and the selection mode from the config shape.
    // (Config validation guarantees `labels` and `taxonomy_file` are not both
    // set; here we just dispatch on whichever is present.)
    let (text_inputs, mode, labels) = match cfg.taxonomy_file.as_deref() {
        Some(path) => {
            let text = files.taxonomy.as_deref().ok_or_else(|| {
                InferenceError::Config(format!("taxonomy file {path} could not be read"))
            })?;
            let tax = Taxonomy::parse(text)
                .map_err(|e| InferenceError::Config(format!("taxonomy: {e}")))?;
            // Taxonomy prompts are complete phrases — used verbatim, no template.
            (
                tax.prompts.clone(),
                Mode::Taxonomy {
                    // Unlike flat mode (where the threshold is the selection
                    // mechanism), taxonomy historically returned every child; an
                    // unset threshold preserves that (0.0 keeps everything).
                    threshold: cfg.score_threshold.unwrap_or(0.0),
                    children: tax.children.clone(),
                },
                tax.children
                    .iter()
                    .map(|c| Label {
                        id: c.id,
                        name: c.name.clone(),
                    })
                    .collect(),
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
                Label::indexed(&cfg.labels),
            )
        }
    };

    // Encode the prompts once; reuse the embeddings for every image.
    let tokenizer_path = hub.file("tokenizer.json").map_err(|_| {
        InferenceError::Config(
            "expected 'tokenizer.json' in the siglip repo (the fast tokenizer)".into(),
        )
    })?;
    let max_len = config.text_config.max_position_embeddings;
    let cache = EmbedCache::new(&weights, &tokenizer_path, max_len, &text_inputs);
    let text_embeds = match cache
        .as_ref()
        .and_then(|c| c.load(text_inputs.len(), device))
    {
        Some(embeds) => embeds,
        None => {
            let started = Instant::now();
            let tokenizer = load_tokenizer(&tokenizer_path, max_len)?;
            let embeds = encode_prompts(&model, &tokenizer, &text_inputs, device)?;
            tracing::info!(
                prompts = text_inputs.len(),
                ms = started.elapsed().as_millis() as u64,
                "encoded siglip prompts"
            );
            if let Some(cache) = &cache {
                cache.store(&embeds);
            }
            embeds
        }
    };

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
    fn classify(&self, images: &[&DecodedImage]) -> Result<Vec<Classification>, InferenceError> {
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
                } => flat_classification(&row, &self.labels, *threshold, *max_results),
                Mode::Taxonomy {
                    threshold,
                    children,
                } => taxonomy_classification(&row, *threshold, children),
            })
            .collect();
        Ok(out)
    }

    fn labels(&self) -> &[Label] {
        &self.labels
    }

    fn input_size(&self) -> u32 {
        self.norm.size as u32
    }
}

/// Flat mode: every label scoring at/above `threshold`, highest first, capped by
/// `max_results`. `label` is the label's index in the configured list.
fn flat_classification(
    row: &[f32],
    labels: &[Label],
    threshold: f32,
    max_results: Option<usize>,
) -> Classification {
    let mut preds = labels
        .iter()
        .zip(row)
        .map(|(label, &score)| label.predict(score))
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
/// score, keep the children scoring at/above their own `threshold` (or the
/// model's `threshold` when they set none), and return them flat in
/// `predictions` (highest first; `label` is the child id). The per-parent
/// grouping is left to the caller, who can reconstruct it from the taxonomy.
fn taxonomy_classification(row: &[f32], threshold: f32, children: &[TaxonChild]) -> Classification {
    let mut predictions = children
        .iter()
        .filter_map(|child| {
            let slice = &row[child.prompt_start..child.prompt_start + child.prompt_len];
            let score = match child.aggregation {
                Aggregation::Max => slice.iter().copied().fold(f32::MIN, f32::max),
                Aggregation::Mean => slice.iter().sum::<f32>() / slice.len() as f32,
            };
            // A child's own threshold replaces the model's.
            (score >= child.threshold.unwrap_or(threshold))
                .then(|| Prediction::new(child.id, child.name.clone(), score))
        })
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
fn load_tokenizer(path: &Path, max_len: usize) -> Result<Tokenizer, InferenceError> {
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

/// L2-normalized text embeddings for `prompts`, shape `(num_prompts, dim)`,
/// encoded [`PROMPT_CHUNK`] prompts at a time.
fn encode_prompts(
    model: &siglip::Model,
    tokenizer: &Tokenizer,
    prompts: &[String],
    device: &Device,
) -> Result<Tensor, InferenceError> {
    if prompts.is_empty() {
        return Err(InferenceError::Config(
            "siglip: no prompts to encode".into(),
        ));
    }
    let mut parts = Vec::with_capacity(prompts.len().div_ceil(PROMPT_CHUNK));
    for chunk in prompts.chunks(PROMPT_CHUNK) {
        let input_ids = tokenize(tokenizer, chunk, device)?;
        parts.push(l2_normalize(&model.get_text_features(&input_ids)?)?);
    }
    Ok(Tensor::cat(&parts, 0)?)
}

/// On-disk cache of a model's encoded prompts, so a restart (or a reload after
/// an idle unload) skips the text tower. It lives in an `apollo-prompt-embeds`
/// folder in the model's Hub cache directory, keyed by everything the
/// embeddings depend on: the model snapshot, the tokenizer, the sequence length,
/// and the exact prompt list. Any read or write problem is logged and treated
/// as a miss — the cache never fails a load.
struct EmbedCache {
    path: PathBuf,
}

impl EmbedCache {
    /// `None` when the weights are not in a Hub snapshot directory
    /// (`<repo dir>/snapshots/<revision>/model.safetensors`).
    fn new(weights: &Path, tokenizer: &Path, max_len: usize, prompts: &[String]) -> Option<Self> {
        let snapshot = weights.parent()?;
        let snapshots = snapshot.parent()?;
        if snapshots.file_name()? != "snapshots" {
            return None;
        }
        let repo_dir = snapshots.parent()?;

        let mut h = Sha256::new();
        for part in [
            EMBED_CACHE_VERSION,
            &repo_dir.file_name()?.to_string_lossy(),
        ] {
            h.update(part.as_bytes());
            h.update([0]);
        }
        h.update(snapshot.file_name()?.as_encoded_bytes());
        h.update([0]);
        // The tokenizer may differ from the weights' snapshot only if the files
        // are swapped by hand; hashing its bytes makes that a miss too.
        h.update(std::fs::read(tokenizer).ok()?);
        h.update((max_len as u64).to_le_bytes());
        h.update((prompts.len() as u64).to_le_bytes());
        for prompt in prompts {
            h.update((prompt.len() as u64).to_le_bytes());
            h.update(prompt.as_bytes());
        }
        let key: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
        let path = repo_dir
            .join("apollo-prompt-embeds")
            .join(format!("{key}.safetensors"));
        Some(Self { path })
    }

    /// The cached embeddings, if present and shaped for `num_prompts` prompts.
    fn load(&self, num_prompts: usize, device: &Device) -> Option<Tensor> {
        if !self.path.exists() {
            return None;
        }
        let loaded = candle_core::safetensors::load(&self.path, device)
            .ok()
            .and_then(|mut m| m.remove("text_embeds"))
            .filter(|t| t.dtype() == DType::F32)
            .filter(|t| matches!(t.dims2(), Ok((n, _)) if n == num_prompts));
        match &loaded {
            Some(_) => tracing::info!(
                prompts = num_prompts,
                "loaded siglip prompt embeddings from cache"
            ),
            None => {
                tracing::warn!(path = %self.path.display(), "ignoring unreadable siglip prompt cache")
            }
        }
        loaded
    }

    /// Write `embeds` atomically (temp file + rename).
    fn store(&self, embeds: &Tensor) {
        let write = || -> Result<(), Box<dyn std::error::Error>> {
            let dir = self.path.parent().ok_or("cache path has no parent")?;
            std::fs::create_dir_all(dir)?;
            let tmp = self
                .path
                .with_extension(format!("tmp-{}", std::process::id()));
            let tensors = HashMap::from([("text_embeds".to_string(), embeds.clone())]);
            candle_core::safetensors::save(&tensors, &tmp)?;
            std::fs::rename(&tmp, &self.path)?;
            Ok(())
        };
        if let Err(e) = write() {
            tracing::warn!(error = %e, path = %self.path.display(), "could not cache siglip prompt embeddings");
        }
    }
}

/// L2-normalize each row (last dim) of a 2-D embedding tensor.
fn l2_normalize(t: &Tensor) -> Result<Tensor, InferenceError> {
    let norm = t.sqr()?.sum_keepdim(D::Minus1)?.sqrt()?;
    Ok(t.broadcast_div(&norm)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn child(id: u32, start: usize, threshold: Option<f32>) -> TaxonChild {
        TaxonChild {
            id,
            name: format!("p.c{id}"),
            parent_id: 1,
            aggregation: Aggregation::Max,
            threshold,
            prompt_start: start,
            prompt_len: 1,
        }
    }

    #[test]
    fn a_childs_own_threshold_replaces_the_models() {
        let children = [
            child(1, 0, None),
            child(2, 1, Some(0.5)),
            child(3, 2, Some(0.05)),
        ];
        let c = taxonomy_classification(&[0.2, 0.3, 0.1], 0.15, &children);
        let kept: Vec<u32> = c.predictions.iter().map(|p| p.label).collect();
        // 1 passes the model's 0.15; 2 misses its own 0.5; 3 passes its 0.05.
        assert_eq!(kept, [1, 3]);
    }
}
