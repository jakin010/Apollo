//! Task submission lifecycle, startup recovery, and the retention timer.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use apollo_domain::{Input, Task, TaskState};

use crate::Engine;
use crate::error::EngineError;

/// Resume attempts before a task is declared poison and failed.
const MAX_RESUME_ATTEMPTS: u32 = 3;

impl Engine {
    /// Startup recovery, run once before serving:
    /// 1. re-fire webhooks stranded by a crash between terminal-state and delivery;
    /// 2. re-queue every incomplete task, bumping its attempt count and failing it
    ///    if it has exceeded [`MAX_RESUME_ATTEMPTS`] (poison-task guard).
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
                self.inner
                    .storage
                    .set_task_state(&task.id, TaskState::Failed)
                    .await?;
                continue;
            }
            tracing::info!(task = %task.id, attempts, "resuming task");
            // Count resumed items toward the in-flight backpressure gauge.
            self.inner
                .in_flight
                .fetch_add(task.items.len(), Ordering::SeqCst);
            let engine = self.clone();
            tokio::spawn(async move { engine.run_task(task).await });
            resumed += 1;
        }
        Ok(resumed)
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
    /// once all items are terminal.
    pub(crate) async fn run_task(&self, task: Task) {
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

        if !cancel.load(Ordering::SeqCst)
            && let Err(e) = self
                .inner
                .storage
                .set_task_state(&task_id, TaskState::Processing)
                .await
            {
                tracing::error!(task = %task_id, error = %e, "failed to mark task processing");
                self.inner.in_flight.fetch_sub(n_items, Ordering::SeqCst);
                self.inner.cancels.lock().unwrap().remove(&task_id);
                return;
            }

        let mut handles = Vec::with_capacity(n_items);
        for (idx, item) in task.items.into_iter().enumerate() {
            let engine = self.clone();
            let tid = task_id.clone();
            let token = cancel.clone();
            handles.push(tokio::spawn(async move {
                engine.run_item(tid, idx, item, token).await
            }));
        }
        for h in handles {
            if let Err(e) = h.await {
                tracing::error!(task = %task_id, error = %e, "item task panicked");
            }
        }

        // Cancellation wins over the item rollup; otherwise reflect the items (a
        // backstop in case an item task panicked before it could roll up).
        if cancel.load(Ordering::SeqCst) {
            if let Err(e) = self
                .inner
                .storage
                .set_task_state(&task_id, TaskState::Cancelled)
                .await
            {
                tracing::error!(task = %task_id, error = %e, "failed to mark task cancelled");
            }
        } else {
            self.rollup_task_state(&task_id).await;
        }
        self.inner.cancels.lock().unwrap().remove(&task_id);
    }

    /// Start the hourly retention purge if `database.retention` parses to a window.
    pub(crate) fn spawn_retention(&self) {
        let Some(spec) = self.inner.config.database.retention.as_deref() else {
            return;
        };
        let Some(window) = apollo_storage::retention::parse(spec) else {
            tracing::warn!(retention = %spec, "could not parse retention window; disabled");
            return;
        };
        let secs = window.as_secs() as i64;
        let engine = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(3600));
            loop {
                tick.tick().await;
                let cutoff = now_unix() - secs;
                match engine.inner.storage.purge_finished_before(cutoff).await {
                    Ok(n) if n > 0 => tracing::info!(removed = n, "retention purge"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "retention purge failed"),
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
