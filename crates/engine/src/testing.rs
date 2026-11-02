//! Test helpers for code that drives an [`Engine`](crate::Engine), compiled for
//! this crate's tests and with the `test-util` feature (enabled only by other
//! crates' dev-dependencies).

use std::sync::Arc;

use apollo_domain::{ItemState, ModelResult, Task, TaskState};
use apollo_storage::{Storage, StorageError, TaskPage, TaskQuery};

/// A store whose `create_task` waits until the test lets it go (one permit of
/// `release` per write), and fails it when `fail` is set. Everything else goes
/// to `inner`.
pub struct HeldCreate {
    pub inner: Arc<dyn Storage>,
    pub release: Arc<tokio::sync::Semaphore>,
    pub fail: bool,
}

impl HeldCreate {
    /// Wrap `inner`; the returned semaphore lets task writes through.
    pub fn new(inner: Arc<dyn Storage>, fail: bool) -> (Self, Arc<tokio::sync::Semaphore>) {
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let held = HeldCreate {
            inner,
            release: release.clone(),
            fail,
        };
        (held, release)
    }
}

#[async_trait::async_trait]
impl Storage for HeldCreate {
    async fn migrate(&self) -> Result<(), StorageError> {
        self.inner.migrate().await
    }
    async fn create_task(&self, task: &Task) -> Result<(), StorageError> {
        // One permit per write: consumed, not returned.
        self.release.acquire().await.unwrap().forget();
        if self.fail {
            return Err(StorageError::Surreal("create failed".into()));
        }
        self.inner.create_task(task).await
    }
    async fn get_task(&self, id: &str) -> Result<Option<Task>, StorageError> {
        self.inner.get_task(id).await
    }
    async fn set_task_state(&self, id: &str, state: TaskState) -> Result<(), StorageError> {
        self.inner.set_task_state(id, state).await
    }
    async fn cancel_task_if_active(&self, id: &str) -> Result<bool, StorageError> {
        self.inner.cancel_task_if_active(id).await
    }
    async fn set_item_state(
        &self,
        task_id: &str,
        item: usize,
        state: ItemState,
        error: Option<&apollo_domain::TaskError>,
    ) -> Result<(), StorageError> {
        self.inner.set_item_state(task_id, item, state, error).await
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
    ) -> Result<(), StorageError> {
        self.inner
            .upsert_model_result(task_id, item, label, result)
            .await
    }
    async fn append_samples(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        samples: &[apollo_domain::Sample],
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
    ) -> Result<Vec<apollo_domain::Sample>, StorageError> {
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
    async fn release_resume_attempts(&self) -> Result<u64, StorageError> {
        self.inner.release_resume_attempts().await
    }
    async fn items_pending_webhook(
        &self,
    ) -> Result<Vec<apollo_storage::PendingWebhook>, StorageError> {
        self.inner.items_pending_webhook().await
    }
    async fn mark_webhook_delivered(&self, task_id: &str, item: usize) -> Result<(), StorageError> {
        self.inner.mark_webhook_delivered(task_id, item).await
    }
    async fn set_item_retries(
        &self,
        task_id: &str,
        item: usize,
        retries: u32,
    ) -> Result<(), StorageError> {
        self.inner.set_item_retries(task_id, item, retries).await
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
    ) -> Result<Option<apollo_domain::ModelOutput>, StorageError> {
        self.inner
            .cache_lookup(content_hash, model, revision, fresh_after)
            .await
    }
    async fn cache_store(
        &self,
        content_hash: &str,
        model: &str,
        revision: &str,
        output: &apollo_domain::ModelOutput,
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
