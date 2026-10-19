//! Loaded-model registry and submission validation.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use apollo_config::{Config, ModelConfig};
use apollo_domain::{Input, Modality};

use crate::error::EngineError;
use crate::metrics::Metrics;
use crate::worker::{Loader, ModelHandle, WorkerSpec, spawn_worker};

/// Holds one worker per enabled model, keyed by label.
pub(crate) struct Registry {
    workers: HashMap<String, Worker>,
}

struct Worker {
    handle: Arc<ModelHandle>,
    /// What the worker was built from; a reload keeps the worker while this
    /// is unchanged.
    key: LoadKey,
}

/// The parts of a model's config (and the files it reads: a taxonomy, a hash
/// list) its loaded state depends on. Fields read per use — `timeout`,
/// `priority`, `video_strategy`, `early_exit` — are left out, so changing them
/// keeps the loaded model.
#[derive(PartialEq)]
struct LoadKey {
    config: ModelConfig,
    /// The worker loads from these, so it uses what was validated.
    files: apollo_inference::ModelFiles,
}

impl LoadKey {
    fn of(model: &ModelConfig) -> Self {
        let mut config = model.clone();
        config.timeout = 0;
        config.priority = 0;
        config.video_strategy = None;
        config.early_exit = None;
        Self {
            config,
            files: apollo_inference::ModelFiles::read(model),
        }
    }
}

/// How a rebuild treated each model.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegistryChanges {
    /// New (or newly enabled) models: a new worker.
    pub added: Vec<String>,
    /// Models whose weights depend on something that changed: a new worker,
    /// which loads on first use.
    pub reloaded: Vec<String>,
    /// Models kept as they were, loaded weights included.
    pub kept: Vec<String>,
    /// Removed (or disabled) models. Their workers finish the work they have,
    /// then exit.
    pub removed: Vec<String>,
}

/// What a worker is built with besides the model's config.
pub(crate) struct WorkerEnv<'a> {
    pub(crate) kind: apollo_inference::DeviceKind,
    pub(crate) cache_dir: Option<&'a PathBuf>,
    /// Idle-unload timeout in milliseconds, shared so a reload changes it for
    /// every worker.
    pub(crate) idle_ms: &'a Arc<AtomicU64>,
    pub(crate) metrics: &'a Metrics,
}

impl Registry {
    /// A worker per enabled model, each with its own `kind` device (never a
    /// shared one — see [`apollo_inference::open_device`]). With `previous`,
    /// workers whose [`LoadKey`] is unchanged are carried over.
    pub(crate) fn build(
        config: &Config,
        env: &WorkerEnv<'_>,
        previous: Option<&Registry>,
    ) -> (Self, RegistryChanges) {
        let mut changes = RegistryChanges::default();
        let mut workers = HashMap::new();
        for (label, model) in &config.models {
            if !model.enabled {
                continue;
            }
            let key = LoadKey::of(model);
            let old = previous.and_then(|p| p.workers.get(label));
            match old {
                Some(w) if w.key == key => {
                    changes.kept.push(label.clone());
                    workers.insert(
                        label.clone(),
                        Worker {
                            handle: Arc::clone(&w.handle),
                            key,
                        },
                    );
                    continue;
                }
                Some(_) => changes.reloaded.push(label.clone()),
                None => changes.added.push(label.clone()),
            }
            let handle = spawn(label, model, &key.files, env);
            workers.insert(
                label.clone(),
                Worker {
                    handle: Arc::new(handle),
                    key,
                },
            );
        }
        if let Some(previous) = previous {
            changes.removed = previous
                .workers
                .keys()
                .filter(|label| !workers.contains_key(*label))
                .cloned()
                .collect();
            changes.removed.sort();
        }
        (Self { workers }, changes)
    }

    pub(crate) fn get(&self, label: &str) -> Option<Arc<ModelHandle>> {
        self.workers.get(label).map(|w| Arc::clone(&w.handle))
    }

    /// Validate one item's input against its requested models. Rejects unknown or
    /// disabled models and modality/architecture mismatches, so a bad request is
    /// refused synchronously before anything is queued.
    pub(crate) fn validate_item(
        &self,
        config: &Config,
        input: &Input,
        models: &[String],
    ) -> Result<(), EngineError> {
        if models.is_empty() {
            return Err(EngineError::Incompatible("item specifies no models".into()));
        }
        let modality = input.modality();
        for label in models {
            let Some(model) = config.models.get(label) else {
                return Err(EngineError::UnknownModel(label.clone()));
            };
            if !model.enabled {
                return Err(EngineError::Incompatible(format!(
                    "model '{label}' is disabled"
                )));
            }
            // Every model is an image classifier: images run directly; video runs
            // as a per-frame scan, which requires a configured video_strategy.
            match modality {
                Modality::Image => {}
                Modality::Video => match &model.video_strategy {
                    Some(s) if config.strategies.contains_key(s) => {}
                    Some(s) => {
                        return Err(EngineError::Config(format!(
                            "model '{label}' references unknown strategy '{s}'"
                        )));
                    }
                    None => {
                        return Err(EngineError::Incompatible(format!(
                            "model '{label}' needs a video_strategy to accept video input"
                        )));
                    }
                },
                Modality::Text | Modality::Audio => {
                    return Err(EngineError::Incompatible(format!(
                        "{modality:?} input is not supported yet"
                    )));
                }
            }
        }
        Ok(())
    }
}

fn spawn(
    label: &str,
    model: &ModelConfig,
    files: &apollo_inference::ModelFiles,
    env: &WorkerEnv<'_>,
) -> ModelHandle {
    let cfg = model.clone();
    let files = files.clone();
    let dev = apollo_inference::open_device(env.kind);
    let dir = env.cache_dir.cloned();
    let loader: Loader = Box::new(move || {
        apollo_inference::load(&cfg, &files, &dev, dir.as_deref()).map_err(|e| e.to_string())
    });
    let spec = WorkerSpec {
        name: label.to_string(),
        batch: model.max_concurrent.max(1) as usize,
        keep_in_memory: model.keep_in_memory,
        idle_ms: Arc::clone(env.idle_ms),
        metrics: env.metrics.model(label),
    };
    let handle = spawn_worker(spec, loader);
    tracing::info!(model = %label, repo = %model.repo, "model worker started");
    handle
}
