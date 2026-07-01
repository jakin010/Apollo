//! ViT image-classifier (candle-transformers `vit`).

use candle_core::{DType, Device};
use candle_nn::VarBuilder;
use candle_transformers::models::vit;

use apollo_domain::{select_top, Classification, DecodedImage, Prediction};

use crate::error::InferenceError;
use crate::loader::{self, Hub};
use crate::preprocess::{self, Normalization};
use crate::ImageClassifier;

/// A loaded ViT image classifier: weights, labels, and the input normalization.
pub(crate) struct VitClassifier {
    model: vit::Model,
    labels: Vec<String>,
    norm: Normalization,
    device: Device,
}

/// Load a ViT classifier from a Hub repo. Expects `config.json` (HF `ViTConfig`
/// plus `id2label`) and a single `model.safetensors`.
pub(crate) fn load(hub: &Hub, device: &Device) -> Result<VitClassifier, InferenceError> {
    let config_bytes = std::fs::read(hub.file("config.json")?)?;
    let config: vit::Config = serde_json::from_slice(&config_bytes)
        .map_err(|e| InferenceError::Config(format!("vit config: {e}")))?;
    let labels = loader::labels_from_config(&config_bytes)?;

    let weights = hub.file("model.safetensors").map_err(|_| {
        InferenceError::Config(
            "expected 'model.safetensors' in the repo \
             (sharded or .bin weights are not supported in this build)"
                .into(),
        )
    })?;
    // SAFETY: mmap of a trusted, locally-cached safetensors file.
    let vb = unsafe { VarBuilder::from_mmaped_safetensors(&[weights], DType::F32, device)? };
    let model = vit::Model::new(&config, labels.len(), vb)?;

    let norm = preprocess::normalization_for(hub, config.image_size);
    Ok(VitClassifier {
        model,
        labels,
        norm,
        device: device.clone(),
    })
}

impl ImageClassifier for VitClassifier {
    fn classify(&self, images: &[DecodedImage]) -> Result<Vec<Classification>, InferenceError> {
        if images.is_empty() {
            return Ok(Vec::new());
        }
        let batch = preprocess::preprocess(images, &self.norm, &self.device)?;
        let logits = self.model.forward(&batch)?;
        let probs = candle_nn::ops::softmax_last_dim(&logits)?;
        let rows: Vec<Vec<f32>> = probs.to_vec2()?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let preds = row
                .iter()
                .enumerate()
                .map(|(i, &score)| Prediction {
                    label: i as u32,
                    score,
                })
                .collect::<Vec<_>>();
            out.push(Classification {
                predictions: select_top(preds),
            });
        }
        Ok(out)
    }

    fn labels(&self) -> &[String] {
        &self.labels
    }
}
