//! Task change notifications, for `WatchTask` and `Classify`'s `wait_seconds`.
//!
//! The engine's storage is wrapped in [`Notifying`], which announces the id of
//! every task a write changed on a broadcast channel. Every lifecycle write goes
//! through storage, so nothing that changes a task can forget to announce it.
//! A [`TaskWatch`] follows one task: the task now, then again after each change,
//! until it finishes.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::broadcast;

use apollo_domain::{ItemState, ModelOutput, ModelResult, Sample, Task, TaskError, TaskState};
use apollo_storage::{
    ClaimMode, Claimable, Fence, Held, Lease, PendingWebhook, Renewal, Storage, StorageError,
    TaskCursor, TaskPage, TaskQuery,
};

use crate::Engine;
use crate::aggregate;
use crate::error::EngineError;

/// Queued change announcements per subscriber before it lags (a lagging
/// watcher just reloads its task).
pub(crate) const CHANGE_BUFFER: usize = 1024;

/// How long a watch waits for an announcement before reading the task again.
const REREAD: Duration = Duration::from_secs(2);

/// A [`Storage`] that announces each changed task id on `changes`.
pub(crate) struct Notifying {
    pub(crate) inner: Arc<dyn Storage>,
    pub(crate) changes: broadcast::Sender<Arc<str>>,
}

impl Notifying {
    fn changed(&self, task_id: &str) {
        // An error only means nobody is watching.
        let _ = self.changes.send(Arc::from(task_id));
    }
}

#[allow(clippy::double_must_use)]
#[async_trait]
impl Storage for Notifying {
    async fn migrate(&self) -> Result<(), StorageError> {
        self.inner.migrate().await
    }

    async fn create_task(
        &self,
        task: &Task,
        lease: Option<&Lease<'_>>,
        pin: Option<&str>,
    ) -> Result<(), StorageError> {
        self.inner.create_task(task, lease, pin).await?;
        self.changed(&task.id);
        Ok(())
    }

    async fn get_task(&self, id: &str) -> Result<Option<Task>, StorageError> {
        self.inner.get_task(id).await
    }

    async fn set_task_state(
        &self,
        id: &str,
        state: TaskState,
        fence: Fence<'_>,
    ) -> Result<bool, StorageError> {
        let written = self.inner.set_task_state(id, state, fence).await?;
        if written {
            self.changed(id);
        }
        Ok(written)
    }

    async fn cancel_task_if_active(&self, id: &str) -> Result<bool, StorageError> {
        let cancelled = self.inner.cancel_task_if_active(id).await?;
        if cancelled {
            self.changed(id);
        }
        Ok(cancelled)
    }

    async fn set_item_state(
        &self,
        task_id: &str,
        item: usize,
        state: ItemState,
        error: Option<&TaskError>,
        fence: Fence<'_>,
        delivery: apollo_storage::Delivery<'_>,
    ) -> Result<bool, StorageError> {
        let written = self
            .inner
            .set_item_state(task_id, item, state, error, fence, delivery)
            .await?;
        if written {
            self.changed(task_id);
        }
        Ok(written)
    }

    async fn set_item_failed(
        &self,
        task_id: &str,
        item: usize,
        error: &TaskError,
        models: &[String],
        fence: Fence<'_>,
        delivery: apollo_storage::Delivery<'_>,
    ) -> Result<bool, StorageError> {
        let written = self
            .inner
            .set_item_failed(task_id, item, error, models, fence, delivery)
            .await?;
        if written {
            self.changed(task_id);
        }
        Ok(written)
    }

    async fn find_task_by_request(
        &self,
        owner: Option<&str>,
        request_id: &str,
    ) -> Result<Option<String>, StorageError> {
        self.inner.find_task_by_request(owner, request_id).await
    }

    async fn list_tasks(&self, query: &TaskQuery) -> Result<TaskPage, StorageError> {
        self.inner.list_tasks(query).await
    }

    async fn upsert_model_result(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        result: &ModelResult,
        fence: Fence<'_>,
    ) -> Result<bool, StorageError> {
        let written = self
            .inner
            .upsert_model_result(task_id, item, label, result, fence)
            .await?;
        if written {
            self.changed(task_id);
        }
        Ok(written)
    }

    // Scan checkpoints are not part of a task until its model finishes, so
    // appending them changes nothing a watcher sees.
    async fn append_samples(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        samples: &[Sample],
    ) -> Result<(), StorageError> {
        self.inner
            .append_samples(task_id, item, label, samples)
            .await
    }

    async fn load_samples(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
    ) -> Result<Vec<Sample>, StorageError> {
        self.inner.load_samples(task_id, item, label).await
    }

    async fn set_steps_completed(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        steps: u32,
    ) -> Result<(), StorageError> {
        self.inner
            .set_steps_completed(task_id, item, label, steps)
            .await
    }

    async fn steps_completed(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
    ) -> Result<u32, StorageError> {
        self.inner.steps_completed(task_id, item, label).await
    }

    async fn count_tasks_by_state(&self) -> Result<Vec<(TaskState, u64)>, StorageError> {
        self.inner.count_tasks_by_state().await
    }

    async fn load_incomplete_tasks(&self) -> Result<Vec<Task>, StorageError> {
        self.inner.load_incomplete_tasks().await
    }

    async fn increment_attempts(&self, id: &str) -> Result<u32, StorageError> {
        self.inner.increment_attempts(id).await
    }

    // Claims, renewals and releases change nothing a watcher sees.
    fn shared(&self) -> bool {
        self.inner.shared()
    }

    async fn claimable_tasks(
        &self,
        mode: ClaimMode,
        node: &str,
        skip: &[String],
        grace: Duration,
        after: Option<&TaskCursor>,
        limit: usize,
    ) -> Result<Vec<Claimable>, StorageError> {
        self.inner
            .claimable_tasks(mode, node, skip, grace, after, limit)
            .await
    }

    async fn claim_task(
        &self,
        id: &str,
        lease: &Lease<'_>,
        mode: ClaimMode,
        grace: Duration,
    ) -> Result<Option<u32>, StorageError> {
        self.inner.claim_task(id, lease, mode, grace).await
    }

    async fn renew_claims(
        &self,
        held: &[Held],
        ttl: Duration,
    ) -> Result<Vec<Renewal>, StorageError> {
        self.inner.renew_claims(held, ttl).await
    }

    async fn release_claims(&self, held: &[Held]) -> Result<u64, StorageError> {
        self.inner.release_claims(held).await
    }

    async fn claim_pending_webhooks(
        &self,
        holder: &str,
        target: Option<&str>,
        mode: apollo_storage::ClaimMode,
        ttl: std::time::Duration,
    ) -> Result<Vec<apollo_storage::PendingWebhook>, StorageError> {
        self.inner
            .claim_pending_webhooks(holder, target, mode, ttl)
            .await
    }

    async fn renew_webhook_claims(
        &self,
        holder: &str,
        items: &[apollo_storage::PendingWebhook],
        ttl: std::time::Duration,
    ) -> Result<Vec<bool>, StorageError> {
        self.inner.renew_webhook_claims(holder, items, ttl).await
    }

    async fn release_webhook_claim(
        &self,
        task_id: &str,
        item: usize,
        holder: &str,
    ) -> Result<bool, StorageError> {
        self.inner
            .release_webhook_claim(task_id, item, holder)
            .await
    }

    async fn auth_key(&self) -> Result<Option<apollo_storage::SharedKey>, StorageError> {
        self.inner.auth_key().await
    }
    async fn publish_auth_key(
        &self,
        public_key: &str,
        node: &str,
    ) -> Result<apollo_storage::SharedKey, StorageError> {
        self.inner.publish_auth_key(public_key, node).await
    }
    async fn replace_auth_key(&self, public_key: &str, node: &str) -> Result<(), StorageError> {
        self.inner.replace_auth_key(public_key, node).await
    }
    async fn add_revocation(
        &self,
        revocation: &apollo_domain::Revocation,
    ) -> Result<(), StorageError> {
        self.inner.add_revocation(revocation).await
    }
    async fn remove_revocation(
        &self,
        revocation: &apollo_domain::Revocation,
    ) -> Result<bool, StorageError> {
        self.inner.remove_revocation(revocation).await
    }
    async fn revocations(&self) -> Result<Vec<apollo_domain::Revocation>, StorageError> {
        self.inner.revocations().await
    }
    async fn items_pending_webhook(&self) -> Result<Vec<PendingWebhook>, StorageError> {
        self.inner.items_pending_webhook().await
    }

    async fn mark_webhook_delivered(
        &self,
        task_id: &str,
        item: usize,
        holder: &str,
    ) -> Result<bool, StorageError> {
        self.inner
            .mark_webhook_delivered(task_id, item, holder)
            .await
    }

    async fn set_item_retries(
        &self,
        task_id: &str,
        item: usize,
        retries: u32,
        fence: Fence<'_>,
    ) -> Result<bool, StorageError> {
        let written = self
            .inner
            .set_item_retries(task_id, item, retries, fence)
            .await?;
        if written {
            self.changed(task_id);
        }
        Ok(written)
    }

    async fn purge_finished_before(&self, cutoff_unix_secs: i64) -> Result<u64, StorageError> {
        self.inner.purge_finished_before(cutoff_unix_secs).await
    }

    async fn cache_lookup(
        &self,
        content_hash: &str,
        model: &str,
        revision: &str,
        fresh_after: i64,
    ) -> Result<Option<ModelOutput>, StorageError> {
        self.inner
            .cache_lookup(content_hash, model, revision, fresh_after)
            .await
    }

    async fn cache_store(
        &self,
        content_hash: &str,
        model: &str,
        revision: &str,
        output: &ModelOutput,
    ) -> Result<(), StorageError> {
        self.inner
            .cache_store(content_hash, model, revision, output)
            .await
    }

    async fn url_cache_lookup(
        &self,
        url_hash: &str,
        model: &str,
        revision: &str,
        fresh_after: i64,
    ) -> Result<Option<String>, StorageError> {
        self.inner
            .url_cache_lookup(url_hash, model, revision, fresh_after)
            .await
    }

    async fn url_cache_store(
        &self,
        url_hash: &str,
        model: &str,
        revision: &str,
        content_hash: &str,
    ) -> Result<(), StorageError> {
        self.inner
            .url_cache_store(url_hash, model, revision, content_hash)
            .await
    }

    async fn purge_cache_before(&self, cutoff_unix_secs: i64) -> Result<u64, StorageError> {
        self.inner.purge_cache_before(cutoff_unix_secs).await
    }
}

/// Follows one task: [`TaskWatch::next`] yields the task now, then again each
/// time it changes, and `None` once it has yielded the task finished (or the
/// task is gone).
pub struct TaskWatch {
    engine: Engine,
    id: String,
    changes: broadcast::Receiver<Arc<str>>,
    last: Option<Task>,
    done: bool,
    closed: bool,
}

impl TaskWatch {
    /// The task's next state; `Ok(None)` when the watch is over.
    pub async fn next(&mut self) -> Result<Option<Task>, EngineError> {
        if self.done {
            return Ok(None);
        }
        loop {
            if self.last.is_some() {
                self.changed().await;
            }
            let Some(task) = self.engine.get_task(&self.id).await? else {
                self.done = true;
                return Ok(None);
            };
            if self.last.as_ref() == Some(&task) {
                if self.closed {
                    self.done = true;
                    return Ok(None);
                }
                continue;
            }
            self.done = aggregate::task_terminal(task.state);
            self.last = Some(task.clone());
            return Ok(Some(task));
        }
    }

    /// Wait until the task may have changed: its id is announced,
    /// announcements were missed, or [`REREAD`] has passed (another instance
    /// sharing the database changes tasks without announcing it here).
    async fn changed(&mut self) {
        let deadline = tokio::time::Instant::now() + REREAD;
        loop {
            match tokio::time::timeout_at(deadline, self.changes.recv()).await {
                Err(_) => return,
                Ok(Ok(id)) if *id == *self.id => return,
                Ok(Ok(_)) => {}
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => return,
                // No more announcements (cannot happen while the engine
                // lives, since the watch holds it): reload once more, then end.
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    self.closed = true;
                    return;
                }
            }
        }
    }
}

impl Engine {
    /// Watch the task `id` (see [`TaskWatch`]). Subscribes before the first
    /// read, so no change between the two is missed.
    pub fn watch(&self, id: &str) -> TaskWatch {
        TaskWatch {
            engine: self.clone(),
            id: id.to_string(),
            changes: self.inner.changes.subscribe(),
            last: None,
            done: false,
            closed: false,
        }
    }

    /// The task `id` once it has finished, or as it is when `timeout` runs out.
    /// `None` if there is no such task.
    pub async fn wait_for(&self, id: &str, timeout: Duration) -> Result<Option<Task>, EngineError> {
        let mut watch = self.watch(id);
        let mut latest = None;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match tokio::time::timeout_at(deadline, watch.next()).await {
                Ok(Ok(Some(task))) => latest = Some(task),
                Ok(Ok(None)) => break,
                Ok(Err(e)) => return Err(e),
                Err(_) => break,
            }
        }
        match latest {
            Some(task) => Ok(Some(task)),
            // Timed out before even the first read finished.
            None => self.get_task(id).await,
        }
    }
}
