//! Task submission lifecycle, startup recovery, and the maintenance timer.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use apollo_domain::{Input, Task, TaskError, TaskState};

use crate::aggregate;
use crate::error::EngineError;
use crate::{Engine, Reservation};

/// Resume attempts before a task is declared poison and failed.
const MAX_RESUME_ATTEMPTS: u32 = 3;

impl Engine {
    /// Startup recovery, run once before serving:
    /// 1. re-fire webhooks stranded by a crash between terminal-state and delivery;
    /// 2. re-queue every incomplete task, bumping its attempt count and failing it
    ///    if it has exceeded [`MAX_RESUME_ATTEMPTS`] (poison-task guard). Only
    ///    crashes count: a graceful shutdown releases the attempt again (see
    ///    [`Engine::shutdown`]).
    ///
    /// Returns the number of tasks resumed.
    pub async fn recover(&self) -> Result<usize, EngineError> {
        match self.inner.storage.items_pending_webhook().await {
            Ok(pending) => {
                for p in pending {
                    self.deliver_webhook(&p.task_id, p.item_index).await;
                }
            }
            Err(e) => tracing::warn!(error = %e, "recover: could not list pending webhooks"),
        }

        let tasks = self.inner.storage.load_incomplete_tasks().await?;

        // Remove orphaned ClassifyStream uploads (files left by a task that
        // finished but crashed before its upload was deleted). Safe here because
        // recovery runs before the server accepts new uploads.
        self.sweep_uploads(&tasks);

        let mut resumed = 0usize;
        for task in tasks {
            let attempts = self.inner.storage.increment_attempts(&task.id).await?;
            if attempts > MAX_RESUME_ATTEMPTS {
                tracing::warn!(task = %task.id, attempts, "exceeded resume cap; failing task");
                self.fail_poison_task(&task).await?;
                continue;
            }
            tracing::info!(task = %task.id, attempts, "resuming task");
            // Count resumed items toward the in-flight backpressure gauge.
            let reservations = task
                .items
                .iter()
                .map(|item| self.reserve_resumed(&item.models))
                .collect();
            let engine = self.clone();
            tokio::spawn(async move { engine.run_task(task, reservations).await });
            resumed += 1;
        }
        Ok(resumed)
    }

    /// Fail a task that keeps crashing the process: mark it failed, fail each
    /// unfinished item (so the wire reports an error and the webhook fires), and
    /// let the rollup remove its staged upload.
    async fn fail_poison_task(&self, task: &Task) -> Result<(), EngineError> {
        if !self
            .inner
            .storage
            .set_task_state(&task.id, TaskState::Failed)
            .await?
        {
            // It finished meanwhile (elsewhere). If it was cancelled, its
            // items still finish, as cancelled; otherwise they ended already.
            if let Some(now) = self.inner.storage.get_task(&task.id).await?
                && now.state == TaskState::Cancelled
            {
                for (idx, item) in now.items.iter().enumerate() {
                    if !aggregate::item_terminal(item.state) {
                        self.cancel_item(&task.id, idx).await;
                    }
                }
            }
            return Ok(());
        }
        let err = TaskError::internal(format!(
            "task interrupted by {MAX_RESUME_ATTEMPTS} crashes; giving up"
        ));
        for (idx, item) in task.items.iter().enumerate() {
            if !aggregate::item_terminal(item.state) {
                self.fail_item(&task.id, idx, item, err.clone()).await;
            }
        }
        // Covers a task whose items were all already terminal.
        self.rollup_task_state(&task.id).await;
        Ok(())
    }

    /// The task's stored state, if it can be read.
    async fn stored_state(&self, task_id: &str) -> Option<TaskState> {
        match self.inner.storage.get_task(task_id).await {
            Ok(task) => task.map(|t| t.state),
            Err(e) => {
                tracing::warn!(task = %task_id, error = %e, "could not read the task");
                None
            }
        }
    }

    /// Remove `ClassifyStream` upload files no longer referenced by any incomplete
    /// task. Runs once at startup, so there is no race with in-flight uploads.
    fn sweep_uploads(&self, keep_tasks: &[Task]) {
        let dir = self.upload_dir();
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                tracing::warn!(dir = %dir.display(), error = %e, "upload sweep: cannot read dir");
                return;
            }
        };
        let keep: std::collections::HashSet<std::path::PathBuf> = keep_tasks
            .iter()
            .flat_map(|t| &t.items)
            .filter_map(|it| match &it.input {
                Input::Bytes { path, .. } => Some(path.clone()),
                _ => None,
            })
            .collect();
        let mut removed = 0usize;
        for entry in entries.flatten() {
            let path = entry.path();
            if keep.contains(&path) {
                continue;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => removed += 1,
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "upload sweep: remove failed")
                }
            }
        }
        if removed > 0 {
            tracing::info!(removed, "swept orphaned upload files");
        }
    }

    /// Drive one task to completion: mark it processing, run every item
    /// concurrently (each bounded by the global semaphore), then mark it completed
    /// once all items are terminal. `reservations` holds one per item, in item
    /// order; each item gives its own back when it ends.
    pub(crate) async fn run_task(&self, task: Task, reservations: Vec<Reservation>) {
        let task_id = task.id.clone();
        let n_items = task.items.len();

        // Adopt the cancellation token cancel() may have already created for this
        // task, otherwise register a fresh one.
        let cancel = {
            let mut map = self.inner.cancels.lock().unwrap();
            map.entry(task_id.clone())
                .or_insert_with(|| Arc::new(AtomicBool::new(false)))
                .clone()
        };

        // Mark it processing, unless it has finished meanwhile (cancelled, or
        // finished by another instance sharing the database): then there is
        // nothing left to run. A cancel through this instance that lands
        // meanwhile is still recorded, by the items at their first checkpoint.
        if !cancel.load(Ordering::SeqCst) {
            let started = self
                .inner
                .storage
                .set_task_state(&task_id, TaskState::Processing)
                .await;
            let stop = match started {
                Ok(true) => false,
                Ok(false) if cancel.load(Ordering::SeqCst) => false,
                // Cancelled through the database only (from another
                // instance): the items still finish, as cancelled.
                Ok(false) if self.stored_state(&task_id).await == Some(TaskState::Cancelled) => {
                    cancel.store(true, Ordering::SeqCst);
                    false
                }
                Ok(false) => {
                    tracing::info!(task = %task_id, "task already finished; not running it");
                    true
                }
                Err(e) => {
                    tracing::error!(task = %task_id, error = %e, "failed to mark task processing");
                    true
                }
            };
            if stop {
                // Dropping `reservations` gives every item's back.
                self.inner.cancels.lock().unwrap().remove(&task_id);
                return;
            }
        }

        // The items run on the config in effect now, even if it is reloaded
        // while they run.
        let pinned = self.pinned();
        let mut handles = Vec::with_capacity(n_items);
        debug_assert_eq!(reservations.len(), n_items);
        for ((idx, item), reservation) in task.items.into_iter().enumerate().zip(reservations) {
            let engine = pinned.clone();
            let tid = task_id.clone();
            let token = cancel.clone();
            handles.push(tokio::spawn(async move {
                engine.run_item(tid, idx, item, token, reservation).await
            }));
        }
        for h in handles {
            if let Err(e) = h.await {
                tracing::error!(task = %task_id, error = %e, "item task panicked");
            }
        }

        // Cancellation wins over the item rollup — unless the task finished first
        // (the write is conditional). Then reflect the items: a backstop in case an
        // item task panicked before it could roll up, and the point where a
        // cancelled task's uploads are removed.
        if cancel.load(Ordering::SeqCst)
            && let Err(e) = self.inner.storage.cancel_task_if_active(&task_id).await
        {
            tracing::error!(task = %task_id, error = %e, "failed to mark task cancelled");
        }
        self.rollup_task_state(&task_id).await;
        self.inner.cancels.lock().unwrap().remove(&task_id);
    }

    /// Start the hourly maintenance loop: purge finished tasks older than
    /// `database.retention` (when set) and result-cache entries older than
    /// `cache.ttl_secs` (when the cache is enabled with a TTL). Both are read
    /// from the current config on every pass, so a reload applies to the next
    /// one.
    pub(crate) fn spawn_maintenance(&self) {
        let engine = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(3600));
            loop {
                tick.tick().await;
                let config = engine.config();
                let retention = config.database.retention.as_deref().and_then(|spec| {
                    let window = apollo_storage::retention::parse(spec);
                    if window.is_none() {
                        tracing::warn!(retention = %spec, "could not parse retention window; skipped");
                    }
                    window.map(|w| w.as_secs() as i64)
                });
                let cache_ttl = config
                    .cache
                    .as_ref()
                    .filter(|c| c.enabled)
                    .and_then(|c| c.ttl_secs)
                    .map(|t| t as i64);
                let now = now_unix();
                if let Some(secs) = retention {
                    match engine.inner.storage.purge_finished_before(now - secs).await {
                        Ok(n) if n > 0 => tracing::info!(removed = n, "retention purge"),
                        Ok(_) => {}
                        Err(e) => tracing::warn!(error = %e, "retention purge failed"),
                    }
                }
                if let Some(ttl) = cache_ttl {
                    match engine.inner.storage.purge_cache_before(now - ttl).await {
                        Ok(n) if n > 0 => tracing::info!(removed = n, "cache purge"),
                        Ok(_) => {}
                        Err(e) => tracing::warn!(error = %e, "cache purge failed"),
                    }
                }
            }
        });
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
