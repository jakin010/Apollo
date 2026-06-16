//! Loaded-model registry and submission validation.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use candle_core::Device;

use apollo_config::Config;
use apollo_domain::{Input, Modality, ModelKind};

use crate::error::EngineError;
use crate::worker::{spawn_worker, ModelHandle};

/// Holds one worker per enabled model, keyed by label.
pub(crate) struct Registry {
    workers: HashMap<String, Arc<ModelHandle>>,
}

impl Registry {
    /// Spawn a worker for every enabled model. `device` is cloned per worker.
    pub(crate) fn build(config: &Config, device: Device, cache_dir: Option<PathBuf>, idle: Duration) -> Self {
        let mut workers = HashMap::new();
        for (label, model) in &config.models {
            if !model.enabled {
                continue;
            }
            let handle = spawn_worker(model.clone(), device.clone(), cache_dir.clone(), idle);
            workers.insert(label.clone(), Arc::new(handle));
            tracing::info!(model = %label, repo = %model.repo, "model worker started");
        }
        Self { workers }
    }

    pub(crate) fn get(&self, label: &str) -> Option<Arc<ModelHandle>> {
        self.workers.get(label).cloned()
    }

    /// Validate one item's input against its requested models. Rejects unknown or
    /// disabled models and modality/architecture mismatches, so a bad request is
    /// refused synchronously before anything is queued.
    pub(crate) fn validate_item(&self, config: &Config, input: &Input, models: &[String]) -> Result<(), EngineError> {
        if models.is_empty() {
            return Err(EngineError::Incompatible("item specifies no models".into()));
        }
        let modality = input.modality();
        for label in models {
            let Some(model) = config.models.get(label) else {
                return Err(EngineError::UnknownModel(label.clone()));
            };
            if !model.enabled {
                return Err(EngineError::Incompatible(format!("model '{label}' is disabled")));
            }
            match (modality, model.architecture.kind()) {
                (Modality::Image, ModelKind::ImageClassifier) => {}
                (Modality::Video, ModelKind::VideoClassifier) => {}
                (Modality::Video, ModelKind::ImageClassifier) => match &model.video_strategy {
                    Some(s) if config.strategies.contains_key(s) => {}
                    Some(s) => {
                        return Err(EngineError::Config(format!(
                            "model '{label}' references unknown strategy '{s}'"
                        )))
                    }
                    None => {
                        return Err(EngineError::Incompatible(format!(
                            "model '{label}' is an image-classifier and needs a video_strategy to accept video"
                        )))
                    }
                },
                (Modality::Image, ModelKind::VideoClassifier) => {
                    return Err(EngineError::Incompatible(format!(
                        "video-classifier '{label}' cannot classify an image"
                    )))
                }
                (Modality::Text, _) | (Modality::Audio, _) => {
                    return Err(EngineError::Incompatible(format!(
                        "{modality:?} input is not supported yet"
                    )))
                }
            }
        }
        Ok(())
    }
}
