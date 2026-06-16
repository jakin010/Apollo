//! VideoMAE video-classifier (whole-clip).
//!
//! candle-transformers (0.10) ships no VideoMAE model, so the forward pass is not
//! wired up in this build. Metadata — clip length (`num_frames`) and labels — is
//! still read from the repo's `config.json`, so the engine can plan clip sampling
//! and surface the labels. Implementing the model means adding a VideoMAE
//! (tubelet 3D patch embedding + temporal ViT encoder + classification head) here
//! and loading its safetensors into a `VarBuilder`, mirroring [`super::vit`];
//! `classify_clip` would then preprocess the clip to `(1, C, T, H, W)` and run it.

use candle_core::Device;

use apollo_domain::{Classification, DecodedImage};

use crate::error::InferenceError;
use crate::loader::{self, Hub};
use crate::VideoClassifier;

/// A VideoMAE classifier handle. Carries clip length and labels; the forward pass
/// is not implemented in this build (see module docs).
pub(crate) struct VideoMaeClassifier {
    labels: Vec<String>,
    clip_len: usize,
    _device: Device,
}

/// Read clip length and labels from the repo `config.json`. Weights are not loaded
/// until the model itself is implemented.
pub(crate) fn load(hub: &Hub, device: &Device) -> Result<VideoMaeClassifier, InferenceError> {
    let config_bytes = std::fs::read(hub.file("config.json")?)?;
    let labels = loader::labels_from_config(&config_bytes)?;
    let clip_len = serde_json::from_slice::<serde_json::Value>(&config_bytes)
        .ok()
        .and_then(|v| v.get("num_frames").and_then(|n| n.as_u64()))
        .unwrap_or(16) as usize;
    Ok(VideoMaeClassifier {
        labels,
        clip_len,
        _device: device.clone(),
    })
}

impl VideoClassifier for VideoMaeClassifier {
    fn classify_clip(&self, _frames: &[DecodedImage]) -> Result<Classification, InferenceError> {
        Err(InferenceError::Unsupported(
            "VideoMAE forward is not implemented in this build \
             (candle-transformers ships no VideoMAE model)"
                .into(),
        ))
    }

    fn clip_len(&self) -> usize {
        self.clip_len
    }

    fn labels(&self) -> &[String] {
        &self.labels
    }
}
