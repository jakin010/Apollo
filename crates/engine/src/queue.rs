//! Task submission lifecycle, startup recovery, and the retention timer.

use std::time::Duration;

use apollo_domain::{Task, TaskState};

use crate::error::EngineError;
use crate::Engine;

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
            let engine = self.clone();
            tokio::spawn(async move { engine.run_task(task).await });
            resumed += 1;
        }
        Ok(resumed)
    }

    /// Drive one task to completion: mark it processing, run every item
    /// concurrently (each bounded by the global semaphore), then mark it completed
    /// once all items are terminal.
    pub(crate) async fn run_task(&self, task: Task) {
        let task_id = task.id.clone();
        if let Err(e) = self
            .inner
            .storage
            .set_task_state(&task_id, TaskState::Processing)
            .await
        {
            tracing::error!(task = %task_id, error = %e, "failed to mark task processing");
            return;
        }

        let mut handles = Vec::with_capacity(task.items.len());
        for (idx, item) in task.items.into_iter().enumerate() {
            let engine = self.clone();
            let tid = task_id.clone();
            handles.push(tokio::spawn(async move {
                engine.run_item(tid, idx, item).await
            }));
        }
        for h in handles {
            if let Err(e) = h.await {
                tracing::error!(task = %task_id, error = %e, "item task panicked");
            }
        }

        if let Err(e) = self
            .inner
            .storage
            .set_task_state(&task_id, TaskState::Completed)
            .await
        {
            tracing::error!(task = %task_id, error = %e, "failed to mark task completed");
        }
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
