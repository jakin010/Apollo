//! Test helpers for code that drives an [`Engine`](crate::Engine), compiled for
//! this crate's tests and with the `test-util` feature (enabled only by other
//! crates' dev-dependencies).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use apollo_domain::{ItemState, ModelResult, Task, TaskState};
use std::time::Duration;

use apollo_storage::{
    ClaimMode, Claimable, Fence, Held, Lease, Renewal, Storage, StorageError, TaskCursor, TaskPage,
    TaskQuery,
};

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
    async fn create_task(
        &self,
        task: &Task,
        lease: Option<&Lease<'_>>,
        pin: Option<&str>,
    ) -> Result<(), StorageError> {
        // One permit per write: consumed, not returned.
        self.release.acquire().await.unwrap().forget();
        if self.fail {
            return Err(StorageError::Surreal("create failed".into()));
        }
        self.inner.create_task(task, lease, pin).await
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
        self.inner.set_task_state(id, state, fence).await
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
        fence: Fence<'_>,
        delivery: apollo_storage::Delivery<'_>,
    ) -> Result<bool, StorageError> {
        self.inner
            .set_item_state(task_id, item, state, error, fence, delivery)
            .await
    }
    async fn set_item_failed(
        &self,
        task_id: &str,
        item: usize,
        error: &apollo_domain::TaskError,
        models: &[String],
        fence: Fence<'_>,
        delivery: apollo_storage::Delivery<'_>,
    ) -> Result<bool, StorageError> {
        self.inner
            .set_item_failed(task_id, item, error, models, fence, delivery)
            .await
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
        self.inner
            .upsert_model_result(task_id, item, label, result, fence)
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
    async fn items_pending_webhook(
        &self,
    ) -> Result<Vec<apollo_storage::PendingWebhook>, StorageError> {
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
        self.inner
            .set_item_retries(task_id, item, retries, fence)
            .await
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

/// A store whose every call waits while its [`StallGate`] is shut, as when the
/// database is unreachable, and goes through once it opens. Everything goes to
/// `inner`.
pub struct Stalled {
    pub inner: Arc<dyn Storage>,
    open: tokio::sync::watch::Receiver<bool>,
    /// Reported as shared, whatever `inner` is.
    shared: bool,
}

/// Shuts and opens a [`Stalled`] store (open to begin with).
pub struct StallGate(tokio::sync::watch::Sender<bool>);

impl StallGate {
    pub fn shut(&self) {
        self.0.send_replace(false);
    }
    pub fn open(&self) {
        self.0.send_replace(true);
    }
}

impl Stalled {
    pub fn new(inner: Arc<dyn Storage>) -> (Self, StallGate) {
        let (open, rx) = tokio::sync::watch::channel(true);
        let stalled = Self {
            inner,
            open: rx,
            shared: false,
        };
        (stalled, StallGate(open))
    }

    /// [`Stalled::new`], reported as a shared database (so an engine on it
    /// renews its leases), whatever `inner` is.
    pub fn new_shared(inner: Arc<dyn Storage>) -> (Self, StallGate) {
        let (stalled, gate) = Self::new(inner);
        let stalled = Self {
            shared: true,
            ..stalled
        };
        (stalled, gate)
    }

    async fn pass(&self) {
        let mut open = self.open.clone();
        // A dropped gate leaves the store as it was: let calls through.
        let _ = open.wait_for(|open| *open).await;
    }
}

#[async_trait::async_trait]
impl Storage for Stalled {
    async fn migrate(&self) -> Result<(), StorageError> {
        self.pass().await;
        self.inner.migrate().await
    }
    async fn create_task(
        &self,
        task: &Task,
        lease: Option<&Lease<'_>>,
        pin: Option<&str>,
    ) -> Result<(), StorageError> {
        self.pass().await;
        self.inner.create_task(task, lease, pin).await
    }
    async fn get_task(&self, id: &str) -> Result<Option<Task>, StorageError> {
        self.pass().await;
        self.inner.get_task(id).await
    }
    async fn set_task_state(
        &self,
        id: &str,
        state: TaskState,
        fence: Fence<'_>,
    ) -> Result<bool, StorageError> {
        self.pass().await;
        self.inner.set_task_state(id, state, fence).await
    }
    async fn cancel_task_if_active(&self, id: &str) -> Result<bool, StorageError> {
        self.pass().await;
        self.inner.cancel_task_if_active(id).await
    }
    async fn set_item_state(
        &self,
        task_id: &str,
        item: usize,
        state: ItemState,
        error: Option<&apollo_domain::TaskError>,
        fence: Fence<'_>,
        delivery: apollo_storage::Delivery<'_>,
    ) -> Result<bool, StorageError> {
        self.pass().await;
        self.inner
            .set_item_state(task_id, item, state, error, fence, delivery)
            .await
    }
    async fn set_item_failed(
        &self,
        task_id: &str,
        item: usize,
        error: &apollo_domain::TaskError,
        models: &[String],
        fence: Fence<'_>,
        delivery: apollo_storage::Delivery<'_>,
    ) -> Result<bool, StorageError> {
        self.pass().await;
        self.inner
            .set_item_failed(task_id, item, error, models, fence, delivery)
            .await
    }
    async fn find_task_by_request(
        &self,
        owner: Option<&str>,
        request_id: &str,
    ) -> Result<Option<String>, StorageError> {
        self.pass().await;
        self.inner.find_task_by_request(owner, request_id).await
    }
    async fn list_tasks(&self, query: &TaskQuery) -> Result<TaskPage, StorageError> {
        self.pass().await;
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
        self.pass().await;
        self.inner
            .upsert_model_result(task_id, item, label, result, fence)
            .await
    }
    async fn append_samples(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        samples: &[apollo_domain::Sample],
    ) -> Result<(), StorageError> {
        self.pass().await;
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
        self.pass().await;
        self.inner.load_samples(task_id, item, label).await
    }
    async fn set_steps_completed(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        steps: u32,
    ) -> Result<(), StorageError> {
        self.pass().await;
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
        self.pass().await;
        self.inner.steps_completed(task_id, item, label).await
    }
    async fn count_tasks_by_state(&self) -> Result<Vec<(TaskState, u64)>, StorageError> {
        self.pass().await;
        self.inner.count_tasks_by_state().await
    }
    async fn load_incomplete_tasks(&self) -> Result<Vec<Task>, StorageError> {
        self.pass().await;
        self.inner.load_incomplete_tasks().await
    }
    async fn increment_attempts(&self, id: &str) -> Result<u32, StorageError> {
        self.pass().await;
        self.inner.increment_attempts(id).await
    }
    fn shared(&self) -> bool {
        self.shared || self.inner.shared()
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
        self.pass().await;
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
        self.pass().await;
        self.inner.claim_task(id, lease, mode, grace).await
    }

    async fn renew_claims(
        &self,
        held: &[Held],
        ttl: Duration,
    ) -> Result<Vec<Renewal>, StorageError> {
        self.pass().await;
        self.inner.renew_claims(held, ttl).await
    }

    async fn release_claims(&self, held: &[Held]) -> Result<u64, StorageError> {
        self.pass().await;
        self.inner.release_claims(held).await
    }
    async fn claim_pending_webhooks(
        &self,
        holder: &str,
        target: Option<&str>,
        mode: apollo_storage::ClaimMode,
        ttl: std::time::Duration,
    ) -> Result<Vec<apollo_storage::PendingWebhook>, StorageError> {
        self.pass().await;
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
        self.pass().await;
        self.inner.renew_webhook_claims(holder, items, ttl).await
    }
    async fn release_webhook_claim(
        &self,
        task_id: &str,
        item: usize,
        holder: &str,
    ) -> Result<bool, StorageError> {
        self.pass().await;
        self.inner
            .release_webhook_claim(task_id, item, holder)
            .await
    }
    async fn items_pending_webhook(
        &self,
    ) -> Result<Vec<apollo_storage::PendingWebhook>, StorageError> {
        self.pass().await;
        self.inner.items_pending_webhook().await
    }
    async fn mark_webhook_delivered(
        &self,
        task_id: &str,
        item: usize,
        holder: &str,
    ) -> Result<bool, StorageError> {
        self.pass().await;
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
        self.pass().await;
        self.inner
            .set_item_retries(task_id, item, retries, fence)
            .await
    }
    async fn purge_finished_before(&self, cutoff_unix_secs: i64) -> Result<u64, StorageError> {
        self.pass().await;
        self.inner.purge_finished_before(cutoff_unix_secs).await
    }
    async fn cache_lookup(
        &self,
        content_hash: &str,
        model: &str,
        revision: &str,
        fresh_after: i64,
    ) -> Result<Option<apollo_domain::ModelOutput>, StorageError> {
        self.pass().await;
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
        self.pass().await;
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
        self.pass().await;
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
        self.pass().await;
        self.inner
            .url_cache_store(url_hash, model, revision, content_hash)
            .await
    }
    async fn purge_cache_before(&self, cutoff_unix_secs: i64) -> Result<u64, StorageError> {
        self.pass().await;
        self.inner.purge_cache_before(cutoff_unix_secs).await
    }
}

/// A store that fails the next `failures` writes of a finished task state, as
/// when the database drops the connection then. Everything else goes to `inner`.
pub struct FailFinish {
    pub inner: Arc<dyn Storage>,
    pub failures: Arc<AtomicU32>,
}

impl FailFinish {
    pub fn new(inner: Arc<dyn Storage>, failures: u32) -> Self {
        Self {
            inner,
            failures: Arc::new(AtomicU32::new(failures)),
        }
    }
}

#[async_trait::async_trait]
impl Storage for FailFinish {
    async fn migrate(&self) -> Result<(), StorageError> {
        self.inner.migrate().await
    }
    async fn create_task(
        &self,
        task: &Task,
        lease: Option<&Lease<'_>>,
        pin: Option<&str>,
    ) -> Result<(), StorageError> {
        self.inner.create_task(task, lease, pin).await
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
        let finish = matches!(
            state,
            TaskState::Completed | TaskState::Failed | TaskState::Cancelled
        );
        let fail = finish
            && self
                .failures
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
        if fail {
            return Err(StorageError::Surreal("Connection reset".into()));
        }
        self.inner.set_task_state(id, state, fence).await
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
        fence: Fence<'_>,
        delivery: apollo_storage::Delivery<'_>,
    ) -> Result<bool, StorageError> {
        self.inner
            .set_item_state(task_id, item, state, error, fence, delivery)
            .await
    }
    async fn set_item_failed(
        &self,
        task_id: &str,
        item: usize,
        error: &apollo_domain::TaskError,
        models: &[String],
        fence: Fence<'_>,
        delivery: apollo_storage::Delivery<'_>,
    ) -> Result<bool, StorageError> {
        self.inner
            .set_item_failed(task_id, item, error, models, fence, delivery)
            .await
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
        self.inner
            .upsert_model_result(task_id, item, label, result, fence)
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
    async fn items_pending_webhook(
        &self,
    ) -> Result<Vec<apollo_storage::PendingWebhook>, StorageError> {
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
        self.inner
            .set_item_retries(task_id, item, retries, fence)
            .await
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
