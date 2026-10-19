//! Text classifiers: BERT, DistilBERT, XLM-RoBERTa (and RoBERTa), ModernBERT
//! and DeBERTa-v2/v3 checkpoints fine-tuned for sequence classification.
//!
//! Every architecture shares the same path:
//! - **Tokenize** with the repo's `tokenizer.json`, or for BERT-style repos that
//!   only ship `vocab.txt`, the standard WordPiece tokenizer built from it.
//! - **Window** text into overlapping windows (128 tokens by default),
//!   each wrapped in the model's special tokens (learned from its tokenizer,
//!   e.g. `[CLS] … [SEP]` or `<s> … </s>`).
//! - **Classify** windows in batches; the backbone gives logits.
//! - **Score** with a sigmoid per label for multi-label models (`problem_type =
//!   "multi_label_classification"`, or `multi_label = true` in the config),
//!   else a softmax across labels. A text's score for each label is its
//!   highest over its windows.
//! - **Select** the top 5 labels, plus any above 0.90, as for image models.
//!
//! Each backbone only maps a padded batch of token ids and an attention mask
//! to logits. BERT, DistilBERT and ModernBERT get their classification heads
//! here (candle ships the encoders, and ModernBERT's own head ends in a
//! softmax, which multi-label models cannot use); XLM-RoBERTa and DeBERTa use
//! candle's.

use std::path::Path;

use candle_core::{DType, Device, IndexOp, Module, Tensor};
use candle_nn::{LayerNorm, Linear, VarBuilder};
use candle_transformers::models::{bert, debertav2, distilbert, modernbert, xlm_roberta};
use tokenizers::Tokenizer;

use apollo_config::ModelConfig;
use apollo_domain::{Architecture, Classification, Label, select_top};

use crate::TextClassifier;
use crate::error::InferenceError;
use crate::loader::{self, Hub};

/// Tokens per window by default (at most the model's limit). Smaller
/// windows find a short passage in a long text better, since a window's
/// score is about the whole window: one toxic sentence in 3,000 words of
/// neutral text scored 0.03–0.66 with 512-token windows (depending on where
/// it was), 0.46–0.89 with 128, 0.80–0.96 with 64.
const DEFAULT_MAX_TOKENS: usize = 128;
/// Tokens shared by consecutive windows of a long text.
const WINDOW_OVERLAP: usize = 64;
/// Windows classified per forward pass.
const WINDOW_BATCH: usize = 16;

/// The architecture-specific part: token ids and mask to logits.
enum Backbone {
    Bert {
        model: bert::BertModel,
        pooler: Linear,
        classifier: Linear,
    },
    DistilBert {
        model: distilbert::DistilBertModel,
        pre_classifier: Linear,
        classifier: Linear,
    },
    XlmRoberta(Box<xlm_roberta::XLMRobertaForSequenceClassification>),
    ModernBert {
        model: modernbert::ModernBert,
        dense: Linear,
        norm: LayerNorm,
        classifier: Linear,
        mean_pooling: bool,
    },
    DebertaV2(Box<debertav2::DebertaV2SeqClassificationModel>),
}

impl Backbone {
    /// `ids`, `types` and `mask` are `(batch, len)` u32; `mask` is 1 for
    /// tokens and 0 for padding. Returns `(batch, labels)` logits.
    fn logits(&self, ids: &Tensor, types: &Tensor, mask: &Tensor) -> candle_core::Result<Tensor> {
        match self {
            Backbone::Bert {
                model,
                pooler,
                classifier,
            } => {
                let hidden = model.forward(ids, types, Some(mask))?;
                let cls = hidden.i((.., 0, ..))?.contiguous()?;
                classifier.forward(&pooler.forward(&cls)?.tanh()?)
            }
            Backbone::DistilBert {
                model,
                pre_classifier,
                classifier,
            } => {
                // DistilBERT's attention masks where the mask is set:
                // padding, shaped to broadcast over (batch, heads, q, k).
                let (b, len) = mask.dims2()?;
                let padding = mask.eq(0u32)?.reshape((b, 1, 1, len))?;
                let hidden = model.forward(ids, &padding)?;
                let cls = hidden.i((.., 0, ..))?.contiguous()?;
                classifier.forward(&pre_classifier.forward(&cls)?.relu()?)
            }
            Backbone::XlmRoberta(model) => model.forward(ids, mask, types),
            Backbone::ModernBert {
                model,
                dense,
                norm,
                classifier,
                mean_pooling,
            } => {
                let hidden = model.forward(ids, mask)?;
                let pooled = if *mean_pooling {
                    let m = mask.unsqueeze(2)?.to_dtype(DType::F32)?;
                    hidden
                        .broadcast_mul(&m)?
                        .sum(1)?
                        .broadcast_div(&m.sum(1)?)?
                } else {
                    hidden.i((.., 0, ..))?.contiguous()?
                };
                let head = norm.forward(&dense.forward(&pooled)?.gelu_erf()?)?;
                classifier.forward(&head)
            }
            Backbone::DebertaV2(model) => {
                model.forward(ids, None, Some(mask.to_dtype(DType::I64)?))
            }
        }
    }
}

/// A loaded text classifier.
pub(crate) struct TextModel {
    backbone: Backbone,
    tokenizer: Tokenizer,
    specials: Specials,
    /// Content tokens per window, and how many consecutive windows share.
    content_len: usize,
    overlap: usize,
    labels: Vec<Label>,
    multi_label: bool,
    pad_id: u32,
    device: Device,
}

/// The special tokens the tokenizer puts around a single sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Specials {
    prefix: Vec<u32>,
    suffix: Vec<u32>,
}

impl Specials {
    /// Learn them by encoding a probe word with and without special tokens.
    fn of(tokenizer: &Tokenizer) -> Result<Self, InferenceError> {
        let encode = |special| {
            tokenizer
                .encode("hello", special)
                .map(|e| e.get_ids().to_vec())
                .map_err(|e| InferenceError::Config(format!("tokenizer: {e}")))
        };
        let (with, without) = (encode(true)?, encode(false)?);
        let at = (0..=with.len().saturating_sub(without.len()))
            .find(|&i| with[i..].starts_with(&without))
            .ok_or_else(|| {
                InferenceError::Config(
                    "the tokenizer changes text when adding special tokens".into(),
                )
            })?;
        Ok(Self {
            prefix: with[..at].to_vec(),
            suffix: with[at + without.len()..].to_vec(),
        })
    }
}

/// Split `content` into windows of at most `len` tokens, consecutive ones
/// sharing `overlap`, each wrapped in `specials`. Empty content is one
/// window of special tokens only.
fn split_windows(
    content: &[u32],
    specials: &Specials,
    len: usize,
    overlap: usize,
) -> Vec<Vec<u32>> {
    let wrap = |piece: &[u32]| {
        let mut w = specials.prefix.clone();
        w.extend_from_slice(piece);
        w.extend_from_slice(&specials.suffix);
        w
    };
    let len = len.max(1);
    let step = len.saturating_sub(overlap).max(1);
    let mut out = Vec::new();
    let mut start = 0;
    loop {
        let end = (start + len).min(content.len());
        out.push(wrap(&content[start..end]));
        if end >= content.len() {
            return out;
        }
        start += step;
    }
}

/// Load the text classifier `cfg` names from its Hub repo.
pub(crate) fn load(
    hub: &Hub,
    cfg: &ModelConfig,
    device: &Device,
) -> Result<TextModel, InferenceError> {
    let config_bytes = std::fs::read(hub.file("config.json")?)?;
    let config: serde_json::Value = serde_json::from_slice(&config_bytes)
        .map_err(|e| InferenceError::Config(format!("config.json: {e}")))?;
    let names = loader::labels_from_config(&config_bytes)?;
    let labels = Label::indexed(&names);
    let multi_label = cfg.multi_label.unwrap_or_else(|| {
        config.get("problem_type").and_then(|v| v.as_str()) == Some("multi_label_classification")
    });

    let weights = hub.file("model.safetensors").map_err(|_| {
        InferenceError::Config(
            "expected 'model.safetensors' in the repo \
             (sharded or .bin weights are not supported in this build)"
                .into(),
        )
    })?;
    // SAFETY: mmap of a trusted, locally-cached safetensors file.
    let vb = unsafe { VarBuilder::from_mmaped_safetensors(&[weights], DType::F32, device)? };
    let parse = |what: &str| {
        let what = what.to_string();
        move |e: serde_json::Error| InferenceError::Config(format!("{what} config: {e}"))
    };
    let n = labels.len();
    let max_position = config
        .get("max_position_embeddings")
        .and_then(|v| v.as_u64())
        // BERT-style models take 512 when the config does not say.
        .unwrap_or(512) as usize;
    let (backbone, model_max) = match cfg.architecture {
        Architecture::Bert => {
            let c: bert::Config = serde_json::from_slice(&config_bytes).map_err(parse("bert"))?;
            let hidden = c.hidden_size;
            let backbone = Backbone::Bert {
                model: bert::BertModel::load(vb.pp("bert"), &c)?,
                pooler: candle_nn::linear(hidden, hidden, vb.pp("bert.pooler.dense"))?,
                classifier: candle_nn::linear(hidden, n, vb.pp("classifier"))?,
            };
            (backbone, max_position)
        }
        Architecture::Distilbert => {
            let c: distilbert::Config =
                serde_json::from_slice(&config_bytes).map_err(parse("distilbert"))?;
            let dim = c.dim;
            let backbone = Backbone::DistilBert {
                model: distilbert::DistilBertModel::load(vb.pp("distilbert"), &c)?,
                pre_classifier: candle_nn::linear(dim, dim, vb.pp("pre_classifier"))?,
                classifier: candle_nn::linear(dim, n, vb.pp("classifier"))?,
            };
            (backbone, max_position)
        }
        Architecture::XlmRoberta => {
            let c: xlm_roberta::Config =
                serde_json::from_slice(&config_bytes).map_err(parse("xlm-roberta"))?;
            let backbone = Backbone::XlmRoberta(Box::new(
                xlm_roberta::XLMRobertaForSequenceClassification::new(n, &c, vb.clone())?,
            ));
            // RoBERTa positions start after the padding index.
            (backbone, max_position.saturating_sub(2))
        }
        Architecture::Modernbert => {
            let c: modernbert::Config = serde_json::from_value(modernbert_config(config.clone()))
                .map_err(parse("modernbert"))?;
            let hidden = c.hidden_size;
            let mean_pooling = c
                .classifier_config
                .as_ref()
                .is_some_and(|cc| cc.classifier_pooling == modernbert::ClassifierPooling::MEAN);
            let backbone = Backbone::ModernBert {
                model: modernbert::ModernBert::load(vb.clone(), &c)?,
                dense: candle_nn::linear_no_bias(hidden, hidden, vb.pp("head.dense"))?,
                norm: candle_nn::layer_norm_no_bias(hidden, c.layer_norm_eps, vb.pp("head.norm"))?,
                classifier: candle_nn::linear(hidden, n, vb.pp("classifier"))?,
                mean_pooling,
            };
            (backbone, max_position)
        }
        Architecture::DebertaV2 => {
            let c: debertav2::Config =
                serde_json::from_slice(&config_bytes).map_err(parse("deberta-v2"))?;
            let backbone = Backbone::DebertaV2(Box::new(
                debertav2::DebertaV2SeqClassificationModel::load(vb.pp("deberta"), &c, None)?,
            ));
            (backbone, max_position)
        }
        other => {
            return Err(InferenceError::Unsupported(format!(
                "{} is not a text architecture",
                other.as_str()
            )));
        }
    };

    let window = cfg
        .max_tokens
        .unwrap_or(DEFAULT_MAX_TOKENS)
        .min(model_max.max(16));
    let mut tokenizer = load_tokenizer(hub)?;
    // Windows are cut here, from the whole text's tokens.
    tokenizer.with_padding(None);
    tokenizer
        .with_truncation(None)
        .map_err(|e| InferenceError::Config(format!("tokenizer: {e}")))?;
    let specials = Specials::of(&tokenizer)?;
    let content_len = window.saturating_sub(specials.prefix.len() + specials.suffix.len());
    if content_len < 8 {
        return Err(InferenceError::Config(format!(
            "max_tokens {window} leaves no room for text"
        )));
    }
    let pad_id = config
        .get("pad_token_id")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .or_else(|| {
            ["[PAD]", "<pad>"]
                .iter()
                .find_map(|t| tokenizer.token_to_id(t))
        })
        .unwrap_or(0);

    Ok(TextModel {
        backbone,
        tokenizer,
        specials,
        content_len,
        overlap: WINDOW_OVERLAP.min(content_len / 4),
        labels,
        multi_label,
        pad_id,
        device: device.clone(),
    })
}

/// A ModernBERT `config.json` in the shape candle reads. Newer transformers
/// releases write the rotary bases as `rope_parameters.{full_attention,
/// sliding_attention}.rope_theta` instead of `global_rope_theta` /
/// `local_rope_theta`; missing ones take transformers' defaults.
fn modernbert_config(mut config: serde_json::Value) -> serde_json::Value {
    let rope = |kind: &str| {
        config
            .pointer(&format!("/rope_parameters/{kind}/rope_theta"))
            .and_then(|v| v.as_f64())
    };
    let defaults = [
        (
            "global_rope_theta",
            rope("full_attention").unwrap_or(160_000.0),
        ),
        (
            "local_rope_theta",
            rope("sliding_attention").unwrap_or(10_000.0),
        ),
        ("local_attention", 128.0),
        ("global_attn_every_n_layers", 3.0),
    ];
    if let Some(map) = config.as_object_mut() {
        for (key, value) in defaults {
            if !map.contains_key(key) {
                let value = if key.ends_with("theta") {
                    serde_json::json!(value)
                } else {
                    serde_json::json!(value as u64)
                };
                map.insert(key.into(), value);
            }
        }
    }
    config
}

/// The repo's `tokenizer.json`, or a WordPiece tokenizer built from
/// `vocab.txt` (lower-casing as `tokenizer_config.json` says) for repos that
/// ship only that.
fn load_tokenizer(hub: &Hub) -> Result<Tokenizer, InferenceError> {
    let json_err = match hub.file("tokenizer.json") {
        Ok(path) => {
            return Tokenizer::from_file(&path)
                .map_err(|e| InferenceError::Config(format!("tokenizer.json: {e}")));
        }
        Err(e) => e,
    };
    let vocab = hub.file("vocab.txt").map_err(|e| {
        InferenceError::Config(format!(
            "the repo has neither a usable tokenizer.json ({json_err}) nor vocab.txt ({e})"
        ))
    })?;
    let lowercase = hub
        .file("tokenizer_config.json")
        .ok()
        .and_then(|p| std::fs::read(p).ok())
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
        .and_then(|v| v.get("do_lower_case").and_then(|v| v.as_bool()))
        .unwrap_or(true);
    wordpiece_tokenizer(&vocab, lowercase)
}

/// The standard BERT tokenizer: BERT normalisation and pre-tokenisation,
/// WordPiece over `vocab`, and `[CLS] … [SEP]` around each sequence.
pub(crate) fn wordpiece_tokenizer(
    vocab: &Path,
    lowercase: bool,
) -> Result<Tokenizer, InferenceError> {
    use tokenizers::models::wordpiece::WordPiece;
    use tokenizers::normalizers::bert::BertNormalizer;
    use tokenizers::pre_tokenizers::bert::BertPreTokenizer;
    use tokenizers::processors::bert::BertProcessing;

    let err = |e: tokenizers::Error| InferenceError::Config(format!("vocab.txt: {e}"));
    let model = WordPiece::from_file(&vocab.to_string_lossy())
        .unk_token("[UNK]".into())
        .build()
        .map_err(err)?;
    let id = |token: &str| {
        tokenizers::Model::token_to_id(&model, token)
            .ok_or_else(|| InferenceError::Config(format!("vocab.txt has no {token}")))
    };
    let (cls, sep) = (id("[CLS]")?, id("[SEP]")?);
    let mut tokenizer = Tokenizer::new(model);
    tokenizer
        .with_normalizer(Some(BertNormalizer::new(true, true, None, lowercase)))
        .map_err(err)?;
    tokenizer.with_pre_tokenizer(Some(BertPreTokenizer));
    tokenizer.with_post_processor(Some(BertProcessing::new(
        ("[SEP]".into(), sep),
        ("[CLS]".into(), cls),
    )));
    Ok(tokenizer)
}

/// One window of a text: its token ids and the owning text's index.
struct Window {
    text: usize,
    ids: Vec<u32>,
}

impl TextModel {
    /// Every text's windows.
    fn windows(&self, texts: &[&str]) -> Result<Vec<Window>, InferenceError> {
        let mut out = Vec::new();
        for (i, text) in texts.iter().enumerate() {
            let encoding = self
                .tokenizer
                .encode(*text, false)
                .map_err(|e| InferenceError::Preprocess(format!("tokenize: {e}")))?;
            for ids in split_windows(
                encoding.get_ids(),
                &self.specials,
                self.content_len,
                self.overlap,
            ) {
                out.push(Window { text: i, ids });
            }
        }
        Ok(out)
    }

    /// Logits for a batch of windows, padded to the longest.
    fn batch_logits(&self, windows: &[Window]) -> Result<Vec<Vec<f32>>, InferenceError> {
        let len = windows
            .iter()
            .map(|w| w.ids.len())
            .max()
            .unwrap_or(0)
            .max(1);
        let (mut ids, mut types, mut mask) = (Vec::new(), Vec::new(), Vec::new());
        for w in windows {
            let pad = len - w.ids.len();
            ids.extend(
                w.ids
                    .iter()
                    .copied()
                    .chain(std::iter::repeat_n(self.pad_id, pad)),
            );
            // One sequence: every token is segment 0.
            types.extend(std::iter::repeat_n(0u32, len));
            mask.extend(std::iter::repeat_n(1u32, w.ids.len()).chain(std::iter::repeat_n(0, pad)));
        }
        let shape = (windows.len(), len);
        let ids = Tensor::from_vec(ids, shape, &self.device)?;
        let types = Tensor::from_vec(types, shape, &self.device)?;
        let mask = Tensor::from_vec(mask, shape, &self.device)?;
        let logits = self.backbone.logits(&ids, &types, &mask)?;
        Ok(logits.to_dtype(DType::F32)?.to_vec2()?)
    }
}

/// Scores from logits: an independent sigmoid per label, or a softmax.
fn scores(logits: &[f32], multi_label: bool) -> Vec<f32> {
    if multi_label {
        logits.iter().map(|&x| 1.0 / (1.0 + (-x).exp())).collect()
    } else {
        let max = logits.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let exps: Vec<f32> = logits.iter().map(|&x| (x - max).exp()).collect();
        let sum: f32 = exps.iter().sum();
        exps.iter().map(|e| e / sum).collect()
    }
}

/// Each text's highest score per label over its windows.
fn combine(per_window: &[(usize, Vec<f32>)], texts: usize, labels: usize) -> Vec<Vec<f32>> {
    let mut out = vec![vec![0f32; labels]; texts];
    for (text, s) in per_window {
        for (best, &v) in out[*text].iter_mut().zip(s) {
            *best = best.max(v);
        }
    }
    out
}

impl TextClassifier for TextModel {
    fn classify(&self, texts: &[&str]) -> Result<Vec<Classification>, InferenceError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let windows = self.windows(texts)?;
        let mut per_window = Vec::with_capacity(windows.len());
        for batch in windows.chunks(WINDOW_BATCH) {
            for (w, logits) in batch.iter().zip(self.batch_logits(batch)?) {
                if logits.len() != self.labels.len() {
                    return Err(InferenceError::Config(format!(
                        "the model gives {} scores for {} labels",
                        logits.len(),
                        self.labels.len()
                    )));
                }
                per_window.push((w.text, scores(&logits, self.multi_label)));
            }
        }
        Ok(combine(&per_window, texts.len(), self.labels.len())
            .into_iter()
            .map(|s| Classification {
                predictions: select_top(
                    self.labels
                        .iter()
                        .zip(s)
                        .map(|(l, score)| l.predict(score))
                        .collect(),
                ),
            })
            .collect())
    }

    fn labels(&self) -> &[Label] {
        &self.labels
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modernbert_configs_from_newer_transformers_are_read() {
        let newer = serde_json::json!({
            "rope_parameters": {
                "full_attention": { "rope_theta": 160000.0 },
                "sliding_attention": { "rope_theta": 10000.0 }
            },
            "local_attention": 64
        });
        let c = modernbert_config(newer);
        assert_eq!(c["global_rope_theta"], 160000.0);
        assert_eq!(c["local_rope_theta"], 10000.0);
        // Values the file has are kept.
        assert_eq!(c["local_attention"], 64);
        assert_eq!(c["global_attn_every_n_layers"], 3);
        // An older config keeps its own fields.
        let older = modernbert_config(serde_json::json!({ "global_rope_theta": 5.0 }));
        assert_eq!(older["global_rope_theta"], 5.0);
    }

    #[test]
    fn softmax_and_sigmoid_scores() {
        let s = scores(&[2.0, 0.0, -2.0], false);
        assert!((s.iter().sum::<f32>() - 1.0).abs() < 1e-6);
        assert!(s[0] > s[1] && s[1] > s[2]);
        let m = scores(&[0.0, 2.0], true);
        assert!((m[0] - 0.5).abs() < 1e-6);
        assert!((m[1] - 0.880797).abs() < 1e-5);
    }

    #[test]
    fn a_texts_score_is_its_highest_window() {
        let per_window = vec![
            (0, vec![0.1, 0.9]),
            (0, vec![0.7, 0.2]),
            (1, vec![0.3, 0.3]),
        ];
        let out = combine(&per_window, 2, 2);
        assert_eq!(out, [vec![0.7, 0.9], vec![0.3, 0.3]]);
    }

    /// A tiny WordPiece vocabulary.
    fn vocab(dir: &Path) -> std::path::PathBuf {
        let path = dir.join("vocab.txt");
        let words = [
            "[PAD]", "[UNK]", "[CLS]", "[SEP]", "the", "cat", "sat", "on", "mat", "##s", "a",
        ];
        std::fs::write(&path, words.join("\n")).unwrap();
        path
    }

    #[test]
    fn the_wordpiece_fallback_tokenizes_like_bert() {
        let dir = tempfile::tempdir().unwrap();
        let tok = wordpiece_tokenizer(&vocab(dir.path()), true).unwrap();
        let e = tok.encode("The CATS sat on a dog", true).unwrap();
        assert_eq!(
            e.get_tokens(),
            [
                "[CLS]", "the", "cat", "##s", "sat", "on", "a", "[UNK]", "[SEP]"
            ]
        );
    }

    #[test]
    fn special_tokens_are_learned_from_the_tokenizer() {
        let dir = tempfile::tempdir().unwrap();
        let tok = wordpiece_tokenizer(&vocab(dir.path()), true).unwrap();
        let s = Specials::of(&tok).unwrap();
        // [CLS] = 2, [SEP] = 3 in the test vocabulary.
        assert_eq!(
            s,
            Specials {
                prefix: vec![2],
                suffix: vec![3]
            }
        );
    }

    #[test]
    fn long_text_is_split_into_overlapping_windows() {
        let specials = Specials {
            prefix: vec![100],
            suffix: vec![101, 102],
        };
        let content: Vec<u32> = (1..=9).collect();
        let windows = split_windows(&content, &specials, 4, 1);
        assert_eq!(
            windows,
            [
                vec![100, 1, 2, 3, 4, 101, 102],
                vec![100, 4, 5, 6, 7, 101, 102],
                vec![100, 7, 8, 9, 101, 102],
            ]
        );
        // Every token is in a window.
        let covered: std::collections::BTreeSet<u32> = windows
            .iter()
            .flatten()
            .copied()
            .filter(|t| *t < 100)
            .collect();
        assert_eq!(covered.len(), 9);
        // Short and empty texts are one window.
        assert_eq!(
            split_windows(&[5, 6], &specials, 4, 1),
            [vec![100, 5, 6, 101, 102]]
        );
        assert_eq!(split_windows(&[], &specials, 4, 1), [vec![100, 101, 102]]);
        // Exactly one window's worth.
        assert_eq!(split_windows(&[1, 2, 3, 4], &specials, 4, 1).len(), 1);
    }
}
