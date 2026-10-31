//! `apollo-engine` — async orchestration core. Wires storage + inference + media
//! into a runnable pipeline.
//!
//! - `queue`     — submission, startup recovery, maintenance timer
//! - `scheduler` — per-task dispatch: fetch-once, fan-out, global concurrency cap
//! - `worker`    — per-model dedicated-thread batching worker (idle-unload)
//! - `registry`  — loaded-model registry + submission validation
//! - `aggregate` — result / lifecycle-state assembly, scan segments
//! - `webhook`   — task-event delivery via an injected sink
//! - `watch`     — task change notifications (`WatchTask`, `wait_seconds`)
//! - `catalog`   — the configured models and pipelines (`ListModels`)
//! - `live`      — the reloadable part of the engine, and config reload
//! - `metrics`   — Prometheus counters, histograms and the text format
//!
//! [`Engine`] is a cheap clonable handle around a shared `Inner`; cloning it does
//! not duplicate any models or state. What a config reload can change (the
//! config itself, the model workers, the webhook sink, the limits) lives in a
//! [`live::Live`] snapshot that a reload swaps out; each task runs on the
//! snapshot it started with.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use apollo_config::Config;
use apollo_domain::{Input, Item, Task};
use apollo_storage::Storage;
use tokio::sync::broadcast;

mod aggregate;
mod catalog;
mod error;
mod gate;
mod live;
mod memory;
mod metrics;
mod queue;
mod registry;
mod scheduler;
mod status;
mod watch;
mod webhook;
mod worker;

#[cfg(test)]
mod tests;

pub use aggregate::task_error;
pub use apollo_storage::{TaskCursor, TaskPage, TaskQuery};
pub use catalog::{Catalog, Gate, ModelInfo, PipelineInfo, PipelineStepInfo, Trigger};
pub use error::EngineError;
pub use live::ReloadReport;
pub use registry::RegistryChanges;
pub use status::{EngineStatus, ModelStatus, TaskCounts};
pub use watch::TaskWatch;
pub use webhook::{EventKind, TaskEvent, WebhookError, WebhookSink};

use crate::gate::PriorityGate;
use live::Live;
use registry::{Registry, WorkerEnv};

/// What to run on an input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Run {
    /// These models, concurrently.
    Models(Vec<String>),
    /// A named `[pipelines.<name>]`: its steps in order, each step's models
    /// concurrently, gated by `stop_if`.
    Pipeline(String),
}

/// One input, what to run on it, and the client's task fields.
#[derive(Debug, Clone, PartialEq)]
pub struct Submission {
    pub input: Input,
    pub run: Run,
    /// Echoed back on the task and in webhooks.
    pub metadata: BTreeMap<String, String>,
    /// Idempotency key: the same owner submitting it again gets the existing
    /// task back.
    pub request_id: Option<String>,
}

/// The task a submission resulted in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submitted {
    pub id: String,
    /// The submission's `request_id` matched an earlier task, which is
    /// returned instead of creating one (the submission's input is unused).
    pub existing: bool,
}

/// The orchestration engine. Clone freely — all clones share one core.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
    /// A snapshot this handle is pinned to (see [`Engine::pinned`]); `None`
    /// reads the current one.
    pinned: Option<Arc<Live>>,
}

struct Inner {
    storage: Arc<dyn Storage>,
    /// Compute backend the workers run on.
    device: apollo_inference::DeviceKind,
    /// Where model weights are cached (`[app].cache_dir`; restart-only).
    cache_dir: Option<PathBuf>,
    /// Idle-unload timeout in milliseconds, shared with every worker.
    idle_ms: Arc<AtomicU64>,
    /// The reloadable part: config, workers, webhook sink, limits.
    live: RwLock<Arc<Live>>,
    /// Serializes reloads.
    reloading: tokio::sync::Mutex<()>,
    metrics: metrics::Metrics,
    /// Priority-ordered global ceiling on concurrently processing items
    /// (`[app].max_concurrent`; restart-only).
    gate: Arc<PriorityGate>,
    /// Items currently queued or processing (admission backpressure counter).
    in_flight: AtomicUsize,
    /// Per-task cooperative cancellation signals, keyed by task id.
    cancels: Mutex<HashMap<String, Arc<AtomicBool>>>,
    /// Ids of changed tasks, announced by the storage wrapper.
    changes: broadcast::Sender<Arc<str>>,
    /// Serializes submissions that carry a `request_id`, so two concurrent
    /// ones cannot both create a task.
    idempotency: tokio::sync::Mutex<()>,
}

/// Decrements the in-flight item counter on drop. One guard is created at the
/// start of `run_item`, so the reservation is released on every exit path
/// (normal, error, cancellation, or panic).
pub(crate) struct InFlightGuard(pub(crate) Engine);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.inner.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Engine {
    /// Build the engine: select the compute device, spawn one worker per enabled
    /// model, and start the maintenance timer.
    ///
    /// Must be called within a Tokio runtime (it spawns background tasks). Loading
    /// of model weights is lazy — workers load on first use, not here.
    pub fn new(
        config: Arc<Config>,
        storage: Arc<dyn Storage>,
        webhook: Option<Arc<dyn WebhookSink>>,
    ) -> Engine {
        let (_probe, kind) = apollo_inference::select_device(config.app.device);
        tracing::info!(device = %kind, "selected compute device");

        let cache_dir = config.app.cache_dir.clone().map(PathBuf::from);
        let idle_ms = Arc::new(AtomicU64::new(live::idle_ms(&config)));
        let metrics = metrics::Metrics::new();
        let env = WorkerEnv {
            kind,
            cache_dir: cache_dir.as_ref(),
            idle_ms: &idle_ms,
            metrics: &metrics,
        };
        let (registry, _) = Registry::build(&config, &env, None);
        let gate = Arc::new(PriorityGate::new(config.app.max_concurrent.max(1) as usize));
        let (changes, _) = broadcast::channel(watch::CHANGE_BUFFER);
        let storage: Arc<dyn Storage> = Arc::new(watch::Notifying {
            inner: storage,
            changes: changes.clone(),
        });

        let engine = Engine {
            inner: Arc::new(Inner {
                storage,
                device: kind,
                cache_dir,
                idle_ms,
                live: RwLock::new(Arc::new(Live::new(config, registry, webhook, 1))),
                reloading: tokio::sync::Mutex::new(()),
                metrics,
                gate,
                in_flight: AtomicUsize::new(0),
                cancels: Mutex::new(HashMap::new()),
                changes,
                idempotency: tokio::sync::Mutex::new(()),
            }),
            pinned: None,
        };
        engine.spawn_maintenance();
        engine.spawn_redelivery();
        engine
    }

    /// The live snapshot: the pinned one, or the current one.
    pub(crate) fn live(&self) -> Arc<Live> {
        match &self.pinned {
            Some(live) => Arc::clone(live),
            None => Arc::clone(&self.inner.live.read().unwrap()),
        }
    }

    /// The config in effect (for this handle).
    pub fn config(&self) -> Arc<Config> {
        Arc::clone(&self.live().config)
    }

    /// A handle that reads the current snapshot, even from a pinned one.
    pub(crate) fn current(&self) -> Engine {
        Engine {
            inner: Arc::clone(&self.inner),
            pinned: None,
        }
    }

    /// A handle that keeps using the current snapshot even if the config is
    /// reloaded meanwhile, so a task runs start to finish on one config (and
    /// a removed model's worker lives until the tasks using it are done).
    pub(crate) fn pinned(&self) -> Engine {
        Engine {
            inner: Arc::clone(&self.inner),
            pinned: Some(self.live()),
        }
    }

    /// Directory under which `ClassifyStream` uploads are staged:
    /// `[app].cache_dir/uploads` when set, otherwise a per-user cache directory
    /// (`~/.cache/apollo/uploads` on Linux, `~/Library/Caches/apollo/uploads` on
    /// macOS). Never the shared system temp dir, where another local user could
    /// pre-create the directory and read or plant uploads. Not created here; see
    /// [`Engine::ensure_upload_dir`].
    pub fn upload_dir(&self) -> PathBuf {
        let base = match &self.inner.cache_dir {
            Some(dir) => dir.clone(),
            None => dirs::cache_dir()
                .unwrap_or_else(std::env::temp_dir)
                .join("apollo"),
        };
        base.join("uploads")
    }

    /// Create the upload directory if needed — owner-only (`0700`) on Unix — and
    /// return it.
    pub fn ensure_upload_dir(&self) -> std::io::Result<PathBuf> {
        let dir = self.upload_dir();
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&dir)?;
        Ok(dir)
    }

    /// Hard cap on accepted upload bytes, mirroring the remote-download cap
    /// (`[limits].max_download`). `None` means unlimited.
    pub fn max_upload_bytes(&self) -> Option<u64> {
        self.live().fetch_limits.max_download_bytes
    }

    /// Validate, persist, and start a task owned by `owner` (the submitting
    /// token's subject; `None` when authentication is disabled). Returns its id
    /// immediately; processing continues in the background. Validation is
    /// synchronous, so a bad request is rejected before anything is written.
    ///
    /// With a `request_id` that `owner` already used, the earlier task is
    /// returned instead (`existing`), whatever this submission contains. The
    /// caller then owns an `Input::Bytes` file it staged, as it does when
    /// `submit` fails.
    pub async fn submit(
        &self,
        submission: Submission,
        owner: Option<String>,
    ) -> Result<Submitted, EngineError> {
        let result = self.submit_once(submission, owner).await;
        self.inner.metrics.submission(match &result {
            Ok(Submitted { existing: true, .. }) => "existing",
            Ok(_) => "accepted",
            Err(EngineError::Overloaded(_)) => "overloaded",
            Err(
                EngineError::UnknownModel(_)
                | EngineError::Incompatible(_)
                | EngineError::Config(_),
            ) => "invalid",
            Err(_) => "error",
        });
        result
    }

    async fn submit_once(
        &self,
        submission: Submission,
        owner: Option<String>,
    ) -> Result<Submitted, EngineError> {
        let _serialized = match &submission.request_id {
            Some(_) => Some(self.inner.idempotency.lock().await),
            None => None,
        };
        if let Some(rid) = &submission.request_id
            && let Some(id) = self.find_request(owner.as_deref(), rid).await?
        {
            return Ok(Submitted { id, existing: true });
        }

        let (models, pipeline) = self.resolve(&submission.input, &submission.run)?;

        // Backpressure: shed load instead of growing memory without bound.
        self.check_memory()?;
        let max_pending = self.live().max_pending;
        let prev = self.inner.in_flight.fetch_add(1, Ordering::SeqCst);
        if max_pending > 0 && prev + 1 > max_pending {
            self.inner.in_flight.fetch_sub(1, Ordering::SeqCst);
            return Err(EngineError::Overloaded(format!(
                "queue full ({prev} items in flight, limit {max_pending})"
            )));
        }

        let id = uuid::Uuid::new_v4().to_string();
        let mut task = Task::new(
            id.clone(),
            vec![Item::new(submission.input, models, pipeline)],
            owner,
        );
        task.metadata = submission.metadata;
        task.request_id = submission.request_id;
        if let Err(e) = self.inner.storage.create_task(&task).await {
            // Release the reservation: no run_items will be spawned to do it.
            self.inner.in_flight.fetch_sub(1, Ordering::SeqCst);
            return Err(e.into());
        }
        tracing::info!(task = %task.id, "task submitted");

        let engine = self.clone();
        tokio::spawn(async move { engine.run_task(task).await });
        Ok(Submitted {
            id,
            existing: false,
        })
    }

    /// The task `owner` submitted with `request_id`, if any.
    pub async fn find_request(
        &self,
        owner: Option<&str>,
        request_id: &str,
    ) -> Result<Option<String>, EngineError> {
        Ok(self
            .inner
            .storage
            .find_task_by_request(owner, request_id)
            .await?)
    }

    /// Check a submission without queueing it: model / pipeline / modality
    /// validation plus a capacity check. Lets `ClassifyStream` reject a request
    /// before accepting any upload bytes. `submit` repeats every check.
    pub fn check_submission(&self, input: &Input, run: &Run) -> Result<(), EngineError> {
        self.resolve(input, run)?;
        self.check_memory()?;
        let max_pending = self.live().max_pending;
        let pending = self.inner.in_flight.load(Ordering::SeqCst);
        if max_pending > 0 && pending >= max_pending {
            return Err(EngineError::Overloaded(format!(
                "queue full ({pending} items in flight, limit {max_pending})"
            )));
        }
        Ok(())
    }

    /// Resolve what to run to the item's model list (a pipeline's models in
    /// step order, so they get the same compatibility checks as a direct list)
    /// and pipeline name, and validate the models against the input's modality
    /// (a pipeline step with an `input` classifies text an earlier step read,
    /// so its models are only checked to be enabled).
    fn resolve(
        &self,
        input: &Input,
        run: &Run,
    ) -> Result<(Vec<String>, Option<String>), EngineError> {
        let live = self.live();
        let (models, pipeline) = match run {
            Run::Pipeline(name) => {
                let p = live
                    .config
                    .pipelines
                    .get(name)
                    .ok_or_else(|| EngineError::Config(format!("unknown pipeline '{name}'")))?;
                let (on_text, on_input): (Vec<_>, Vec<_>) = p
                    .ordered_steps()
                    .into_iter()
                    .partition(|step| step.input.is_some());
                let names = |steps: Vec<&apollo_config::PipelineStep>| -> Vec<String> {
                    steps
                        .into_iter()
                        .flat_map(|s| s.models().iter().cloned())
                        .collect()
                };
                live.registry
                    .validate_item(&live.config, input, &names(on_input))?;
                live.registry
                    .validate_text_steps(&live.config, &names(on_text))?;
                (p.models(), Some(name.clone()))
            }
            Run::Models(models) => {
                let mut seen = std::collections::HashSet::new();
                if let Some(dup) = models.iter().find(|m| !seen.insert(*m)) {
                    return Err(EngineError::Incompatible(format!(
                        "model '{dup}' is listed twice"
                    )));
                }
                live.registry.validate_item(&live.config, input, models)?;
                (models.clone(), None)
            }
        };
        // Inline text is checked now; an uploaded one when it is read.
        if let Input::Text(text) = input
            && let Some(max) = live.config.limits.max_text_bytes()
            && text.len() as u64 > max
        {
            return Err(EngineError::Incompatible(format!(
                "the text is {} bytes, over the {max}-byte limit ([limits].max_text)",
                text.len()
            )));
        }
        Ok((models, pipeline))
    }

    /// Load `model`'s weights now (downloading them if needed) instead of on
    /// first use.
    pub async fn load_model(&self, model: &str) -> Result<(), EngineError> {
        let handle = self
            .live()
            .registry
            .get(model)
            .ok_or_else(|| EngineError::UnknownModel(model.to_string()))?;
        handle.ensure_loaded().await.map(|_| ())
    }

    /// A page of tasks (backs `ListTasks`).
    pub async fn list_tasks(&self, query: &TaskQuery) -> Result<TaskPage, EngineError> {
        Ok(self.inner.storage.list_tasks(query).await?)
    }

    /// Graceful-shutdown hook, called once the server has stopped accepting
    /// requests. Releases this run's resume attempt for every incomplete task, so
    /// a clean restart (e.g. a deploy) never counts toward the poison-task cap —
    /// only crashes do.
    pub async fn shutdown(&self) {
        match self.inner.storage.release_resume_attempts().await {
            Ok(n) if n > 0 => tracing::info!(tasks = n, "released resume attempts on shutdown"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "could not release resume attempts"),
        }
    }

    /// Fetch a task by id (backs the `GetTask` RPC).
    pub async fn get_task(&self, id: &str) -> Result<Option<Task>, EngineError> {
        Ok(self.inner.storage.get_task(id).await?)
    }

    /// Request cooperative cancellation of a task (backs `CancelTask`). Idempotent:
    /// a no-op returning `Ok` if the task has already finished. In-flight items
    /// stop at the next checkpoint (between models / between sampled frames).
    pub async fn cancel(&self, task_id: &str) -> Result<(), EngineError> {
        let task = self
            .inner
            .storage
            .get_task(task_id)
            .await?
            .ok_or_else(|| EngineError::UnknownTask(task_id.to_string()))?;
        if aggregate::task_terminal(task.state) {
            return Ok(());
        }
        // Set the signal, creating the token if run_task has not registered one
        // yet (so a not-yet-started run picks it up via the same map entry).
        {
            let mut map = self.inner.cancels.lock().unwrap();
            let token = map
                .entry(task_id.to_string())
                .or_insert_with(|| Arc::new(AtomicBool::new(false)))
                .clone();
            token.store(true, Ordering::SeqCst);
        }
        // Conditional write: if the task finished in the meantime it stays
        // finished rather than being relabelled as cancelled.
        if !self.inner.storage.cancel_task_if_active(task_id).await? {
            self.inner.cancels.lock().unwrap().remove(task_id);
            return Ok(());
        }
        tracing::info!(task = %task_id, "task cancellation requested");
        Ok(())
    }

    /// Reject new work while process memory is over the configured soft cap.
    fn check_memory(&self) -> Result<(), EngineError> {
        let Some(limit) = self.live().max_memory_bytes else {
            return Ok(());
        };
        let Some(used) = memory::current_memory_bytes() else {
            return Ok(());
        };
        if used >= limit {
            const MIB: u64 = 1024 * 1024;
            return Err(EngineError::Overloaded(format!(
                "memory limit reached ({} MiB in use >= {} MiB cap)",
                used / MIB,
                limit / MIB
            )));
        }
        Ok(())
    }
}
