//! A point-in-time view of the engine, for `apollo status`.

use std::time::SystemTime;

use apollo_domain::TaskState;

use crate::{Engine, EngineError, memory};

/// Live engine state.
#[derive(Debug, Clone, PartialEq)]
pub struct EngineStatus {
    /// Compute backend the model workers use (`cuda`, `metal`, `cpu`).
    pub device: String,
    /// Items accepted and not yet finished (queued, fetching, or running).
    pub items_in_flight: usize,
    /// Backpressure limit on `items_in_flight` (`0` = unlimited).
    pub max_pending: usize,
    /// Concurrency slots (`[app].max_concurrent`).
    pub slots_total: usize,
    /// Slots held by items being processed.
    pub slots_busy: usize,
    /// Items waiting for a slot.
    pub slots_waiting: usize,
    /// Memory the process is using (`phys_footprint` on macOS, which includes
    /// Metal buffers; resident set size elsewhere), when the platform reports it.
    pub memory_bytes: Option<u64>,
    /// Soft memory cap (`[app].max_memory`), when set.
    pub max_memory_bytes: Option<u64>,
    pub models: Vec<ModelStatus>,
    pub tasks: TaskCounts,
    /// Terminal items whose webhook is not yet delivered; `None` when no webhook
    /// is configured.
    pub webhooks_pending: Option<usize>,
}

/// One configured model.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelStatus {
    pub label: String,
    pub repo: String,
    pub architecture: String,
    /// `false` when the model is disabled in the config (no worker).
    pub enabled: bool,
    /// Weights currently resident.
    pub loaded: bool,
    pub keep_in_memory: bool,
    /// Max images per forward pass.
    pub batch: usize,
    /// Square input side, once the model has been loaded.
    pub input_size: Option<u32>,
    /// Requests waiting for the worker.
    pub queued_jobs: usize,
    /// Times the weights were loaded (idle unloads make this grow).
    pub loads: u64,
    /// Images (and video frames) classified.
    pub classified: u64,
    pub last_used: Option<SystemTime>,
}

/// Tasks in storage by state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskCounts {
    pub queued: u64,
    pub processing: u64,
    pub completed: u64,
    pub failed: u64,
    pub cancelled: u64,
}

impl Engine {
    /// Collect the engine's current state.
    pub async fn status(&self) -> Result<EngineStatus, EngineError> {
        let inner = &self.inner;
        let live = self.live();
        let (slots_total, slots_busy, slots_waiting) = inner.gate.snapshot();

        let mut tasks = TaskCounts::default();
        for (state, n) in inner.storage.count_tasks_by_state().await? {
            match state {
                TaskState::Queued => tasks.queued = n,
                TaskState::Processing => tasks.processing = n,
                TaskState::Completed => tasks.completed = n,
                TaskState::Failed => tasks.failed = n,
                TaskState::Cancelled => tasks.cancelled = n,
            }
        }

        let webhooks_pending = match live.webhook {
            Some(_) => Some(inner.storage.items_pending_webhook().await?.len()),
            None => None,
        };

        let models = live
            .config
            .models
            .iter()
            .map(|(label, cfg)| {
                let worker = live.registry.get(label).map(|h| h.snapshot());
                ModelStatus {
                    label: label.clone(),
                    repo: cfg.repo.clone(),
                    architecture: cfg.architecture.as_str().to_string(),
                    enabled: cfg.enabled,
                    loaded: worker.as_ref().is_some_and(|w| w.loaded),
                    keep_in_memory: cfg.keep_in_memory,
                    batch: cfg.max_concurrent.max(1) as usize,
                    input_size: worker.as_ref().and_then(|w| w.input_size),
                    queued_jobs: worker.as_ref().map_or(0, |w| w.queued_jobs),
                    loads: worker.as_ref().map_or(0, |w| w.loads),
                    classified: worker.as_ref().map_or(0, |w| w.classified),
                    last_used: worker.as_ref().and_then(|w| w.last_used),
                }
            })
            .collect();

        Ok(EngineStatus {
            device: inner.device.to_string(),
            items_in_flight: inner.in_flight.load(std::sync::atomic::Ordering::SeqCst),
            max_pending: live.max_pending,
            slots_total,
            slots_busy,
            slots_waiting,
            memory_bytes: memory::current_memory_bytes(),
            max_memory_bytes: live.max_memory_bytes,
            models,
            tasks,
            webhooks_pending,
        })
    }
}
