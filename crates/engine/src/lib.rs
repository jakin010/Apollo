//! `apollo-engine` — async orchestration core. Wires storage + inference + media
//! into a runnable pipeline.
//!
//! - `queue`     — submission, startup recovery, retention timer
//! - `scheduler` — per-task dispatch: fetch-once, fan-out, global concurrency cap
//! - `worker`    — per-model dedicated-thread batching worker (idle-unload)
//! - `registry`  — loaded-model registry + submission validation
//! - `aggregate` — result / lifecycle-state assembly
//! - `webhook`   — terminal-item delivery via an injected sink
//!
//! [`Engine`] is a cheap clonable handle around a shared `Inner`; cloning it does
//! not duplicate any models or state.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apollo_config::Config;
use apollo_domain::{Input, Item, ItemState, Task, TaskState};
use apollo_media::FetchLimits;
use apollo_storage::Storage;

mod aggregate;
mod error;
mod gate;
mod memory;
mod queue;
mod registry;
mod scheduler;
mod webhook;
mod worker;

pub use error::EngineError;
pub use webhook::{WebhookError, WebhookSink};

use crate::gate::PriorityGate;
use registry::Registry;

/// One input plus the model labels to run on it.
pub struct Submission {
    pub input: Input,
    pub models: Vec<String>,
    /// Optional named pipeline. When set, the engine resolves it to an ordered
    /// model list and runs the item as a gated sequence instead of a parallel set.
    pub pipeline: Option<String>,
}

/// The orchestration engine. Clone freely — all clones share one core.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
}

struct Inner {
    storage: Arc<dyn Storage>,
    config: Arc<Config>,
    registry: Registry,
    /// Priority-ordered global ceiling on concurrently processing items.
    gate: Arc<PriorityGate>,
    webhook: Option<Arc<dyn WebhookSink>>,
    /// SSRF / resource limits applied to remote input fetches.
    fetch_limits: FetchLimits,
    /// Soft resident-memory cap in bytes; `None` disables the check.
    max_memory_bytes: Option<u64>,
    /// Max items queued or in-flight before submissions are rejected; `0` = off.
    max_pending: usize,
    /// Items currently queued or processing (admission backpressure counter).
    in_flight: AtomicUsize,
    /// Per-task cooperative cancellation signals, keyed by task id.
    cancels: Mutex<HashMap<String, Arc<AtomicBool>>>,
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
    /// model, and start the retention timer.
    ///
    /// Must be called within a Tokio runtime (it spawns background tasks). Loading
    /// of model weights is lazy — workers load on first use, not here.
    pub fn new(
        config: Arc<Config>,
        storage: Arc<dyn Storage>,
        webhook: Option<Arc<dyn WebhookSink>>,
    ) -> Engine {
        let (device, kind) = apollo_inference::select_device();
        tracing::info!(device = %kind, "selected compute device");

        let cache_dir = config.app.cache_dir.clone().map(PathBuf::from);
        let idle = Duration::from_secs(config.app.idle_timeout as u64);
        let registry = Registry::build(&config, device, cache_dir, idle);
        let gate = Arc::new(PriorityGate::new(config.app.max_concurrent.max(1) as usize));

        let fetch_limits = FetchLimits {
            allowed_schemes: config.limits.allowed_schemes.clone(),
            block_private_ips: config.limits.block_private_ips,
            max_download_bytes: config.limits.max_download_bytes(),
        };
        let max_memory_bytes = config.app.max_memory_bytes();
        let max_pending = config.app.max_pending as usize;

        let engine = Engine {
            inner: Arc::new(Inner {
                storage,
                config,
                registry,
                gate,
                webhook,
                fetch_limits,
                max_memory_bytes,
                max_pending,
                in_flight: AtomicUsize::new(0),
                cancels: Mutex::new(HashMap::new()),
            }),
        };
        engine.spawn_retention();
        engine.spawn_redelivery();
        engine
    }

    /// Directory under which `ClassifyStream` uploads are staged: `[app].cache_dir`
    /// (or the system temp dir if unset) joined with `uploads`. Not created here.
    pub fn upload_dir(&self) -> PathBuf {
        self.inner
            .config
            .app
            .cache_dir
            .clone()
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("apollo"))
            .join("uploads")
    }

    /// Hard cap on accepted upload bytes, mirroring the remote-download cap
    /// (`[limits].max_download`). `None` means unlimited.
    pub fn max_upload_bytes(&self) -> Option<u64> {
        self.inner.fetch_limits.max_download_bytes
    }

    /// Validate, persist, and start a task. Returns its id immediately; processing
    /// continues in the background. Validation is synchronous, so a bad request is
    /// rejected before anything is written.
    pub async fn submit(&self, submissions: Vec<Submission>) -> Result<String, EngineError> {
        if submissions.is_empty() {
            return Err(EngineError::Incompatible("no items submitted".into()));
        }
        // Resolve any named pipelines to their ordered model lists up front, so
        // the resulting models get the same compatibility checks as a direct
        // `models` list. Unknown pipeline names are rejected here.
        let mut resolved = Vec::with_capacity(submissions.len());
        for s in submissions {
            let Submission { input, models, pipeline } = s;
            let (models, pipeline) = match pipeline {
                Some(name) => {
                    let p = self.inner.config.pipelines.get(&name).ok_or_else(|| {
                        EngineError::Config(format!("unknown pipeline '{name}'"))
                    })?;
                    let mut steps = p.steps.clone();
                    steps.sort_by_key(|x| x.order);
                    (steps.into_iter().map(|x| x.model).collect::<Vec<_>>(), Some(name))
                }
                None => (models, None),
            };
            self.inner
                .registry
                .validate_item(&self.inner.config, &input, &models)?;
            resolved.push((input, models, pipeline));
        }

        // Backpressure: shed load instead of growing memory without bound.
        self.check_memory()?;
        let n = resolved.len();
        let prev = self.inner.in_flight.fetch_add(n, Ordering::SeqCst);
        if self.inner.max_pending > 0 && prev + n > self.inner.max_pending {
            self.inner.in_flight.fetch_sub(n, Ordering::SeqCst);
            return Err(EngineError::Overloaded(format!(
                "queue full ({prev} items in flight, limit {})",
                self.inner.max_pending
            )));
        }

        let id = uuid::Uuid::new_v4().to_string();
        let items = resolved
            .into_iter()
            .map(|(input, models, pipeline)| Item {
                input,
                models,
                pipeline,
                state: ItemState::Queued,
                results: Default::default(),
                error: None,
                retries: 0,
            })
            .collect();
        let task = Task {
            id: id.clone(),
            state: TaskState::Queued,
            items,
        };
        if let Err(e) = self.inner.storage.create_task(&task).await {
            // Release the reservation: no run_items will be spawned to do it.
            self.inner.in_flight.fetch_sub(n, Ordering::SeqCst);
            return Err(e.into());
        }
        tracing::info!(task = %task.id, items = task.items.len(), "task submitted");

        let engine = self.clone();
        tokio::spawn(async move { engine.run_task(task).await });
        Ok(id)
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
        self.inner
            .storage
            .set_task_state(task_id, TaskState::Cancelled)
            .await?;
        tracing::info!(task = %task_id, "task cancellation requested");
        Ok(())
    }

    /// Reject new work while resident memory is over the configured soft cap.
    fn check_memory(&self) -> Result<(), EngineError> {
        let Some(limit) = self.inner.max_memory_bytes else {
            return Ok(());
        };
        let Some(rss) = memory::current_rss_bytes() else {
            return Ok(());
        };
        if rss >= limit {
            const MIB: u64 = 1024 * 1024;
            return Err(EngineError::Overloaded(format!(
                "memory limit reached ({} MiB resident >= {} MiB cap)",
                rss / MIB,
                limit / MIB
            )));
        }
        Ok(())
    }
}
