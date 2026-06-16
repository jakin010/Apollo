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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use apollo_config::Config;
use apollo_domain::{Input, Item, ItemState, Task, TaskState};
use apollo_storage::Storage;

mod aggregate;
mod error;
mod queue;
mod registry;
mod scheduler;
mod webhook;
mod worker;

pub use error::EngineError;
pub use webhook::{WebhookError, WebhookSink};

use registry::Registry;

/// One input plus the model labels to run on it.
pub struct Submission {
    pub input: Input,
    pub models: Vec<String>,
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
    /// Global ceiling on concurrently processing items.
    global: Semaphore,
    webhook: Option<Arc<dyn WebhookSink>>,
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
        let global = Semaphore::new(config.app.max_concurrent.max(1) as usize);

        let engine = Engine {
            inner: Arc::new(Inner {
                storage,
                config,
                registry,
                global,
                webhook,
            }),
        };
        engine.spawn_retention();
        engine
    }

    /// Validate, persist, and start a task. Returns its id immediately; processing
    /// continues in the background. Validation is synchronous, so a bad request is
    /// rejected before anything is written.
    pub async fn submit(&self, submissions: Vec<Submission>) -> Result<String, EngineError> {
        if submissions.is_empty() {
            return Err(EngineError::Incompatible("no items submitted".into()));
        }
        for s in &submissions {
            self.inner
                .registry
                .validate_item(&self.inner.config, &s.input, &s.models)?;
        }

        let id = uuid::Uuid::new_v4().to_string();
        let items = submissions
            .into_iter()
            .map(|s| Item {
                input: s.input,
                models: s.models,
                state: ItemState::Queued,
                results: Default::default(),
                error: None,
            })
            .collect();
        let task = Task {
            id: id.clone(),
            state: TaskState::Queued,
            items,
        };
        self.inner.storage.create_task(&task).await?;
        tracing::info!(task = %task.id, items = task.items.len(), "task submitted");

        let engine = self.clone();
        tokio::spawn(async move { engine.run_task(task).await });
        Ok(id)
    }

    /// Fetch a task by id (backs the `GetTask` RPC).
    pub async fn get_task(&self, id: &str) -> Result<Option<Task>, EngineError> {
        Ok(self.inner.storage.get_task(id).await?)
    }
}
