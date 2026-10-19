//! The configured models and pipelines, for `ListModels`.

use std::path::PathBuf;

use apollo_config::EarlyExit;
use apollo_domain::{Architecture, Label, MediaKind};

use crate::Engine;
use crate::error::EngineError;

/// Everything a client can run.
#[derive(Debug, Clone, PartialEq)]
pub struct Catalog {
    /// By name.
    pub models: Vec<ModelInfo>,
    /// By name.
    pub pipelines: Vec<PipelineInfo>,
}

/// One configured model.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelInfo {
    pub name: String,
    pub architecture: Architecture,
    pub repo: String,
    /// Input kinds it accepts: images, and video with a video strategy.
    pub accepts: Vec<MediaKind>,
    pub enabled: bool,
    /// Weights currently in memory.
    pub loaded: bool,
    /// Empty when they cannot be read without downloading the model.
    pub labels: Vec<Label>,
    /// What counts as a trigger (`[models.<l>.early_exit]`).
    pub trigger: Option<Trigger>,
    pub video_strategy: Option<String>,
}

/// One configured pipeline, its steps in run order.
#[derive(Debug, Clone, PartialEq)]
pub struct PipelineInfo {
    pub name: String,
    pub steps: Vec<PipelineStepInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PipelineStepInfo {
    pub models: Vec<String>,
    /// The step's gate.
    pub stop_if: Option<Gate>,
}

/// Any of `labels` scoring at or above `threshold`.
#[derive(Debug, Clone, PartialEq)]
pub struct Trigger {
    pub labels: Vec<u32>,
    pub threshold: f32,
}

/// A pipeline step's gate: `model`'s output reaching `trigger`.
#[derive(Debug, Clone, PartialEq)]
pub struct Gate {
    pub model: String,
    pub trigger: Trigger,
}

impl Engine {
    /// The configured models (with their labels, read without loading or
    /// downloading anything) and pipelines.
    pub async fn catalog(&self) -> Result<Catalog, EngineError> {
        let config = self.inner.config.clone();
        let cache_dir = config.app.cache_dir.clone().map(PathBuf::from);
        // Reading a taxonomy or a cached config.json is file I/O.
        let labels = tokio::task::spawn_blocking(move || {
            config
                .models
                .iter()
                .map(|(name, cfg)| {
                    let labels = apollo_inference::labels(cfg, cache_dir.as_deref());
                    (name.clone(), labels.unwrap_or_default())
                })
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|e| EngineError::Join(e.to_string()))?;

        let config = &self.inner.config;
        let models = labels
            .into_iter()
            .filter_map(|(name, labels)| {
                let cfg = config.models.get(&name)?;
                let mut accepts = vec![MediaKind::Image];
                if cfg.video_strategy.is_some() {
                    accepts.push(MediaKind::Video);
                }
                let loaded = self
                    .inner
                    .registry
                    .get(&name)
                    .is_some_and(|h| h.snapshot().loaded);
                Some(ModelInfo {
                    architecture: cfg.architecture,
                    repo: cfg.repo.clone(),
                    accepts,
                    enabled: cfg.enabled,
                    loaded,
                    labels,
                    trigger: cfg
                        .early_exit
                        .as_ref()
                        .map(|EarlyExit { labels, threshold }| Trigger {
                            labels: labels.clone(),
                            threshold: *threshold,
                        }),
                    video_strategy: cfg.video_strategy.clone(),
                    name,
                })
            })
            .collect();

        let pipelines = config
            .pipelines
            .iter()
            .map(|(name, p)| PipelineInfo {
                name: name.clone(),
                steps: p
                    .ordered_steps()
                    .into_iter()
                    .map(|step| PipelineStepInfo {
                        models: step.models().to_vec(),
                        stop_if: step.stop_if.as_ref().zip(step.gate_model()).map(
                            |(stop, model)| Gate {
                                model: model.to_string(),
                                trigger: Trigger {
                                    labels: stop.labels.clone(),
                                    threshold: stop.threshold,
                                },
                            },
                        ),
                    })
                    .collect(),
            })
            .collect();
        Ok(Catalog { models, pipelines })
    }
}
