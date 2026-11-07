//! Test helpers for code that drives an [`Engine`](crate::Engine), compiled for
//! this crate's tests and with the `test-util` feature (enabled only by other
//! crates' dev-dependencies).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

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
        limit: usize,
    ) -> Result<Vec<apollo_storage::PendingWebhook>, StorageError> {
        self.inner
            .claim_pending_webhooks(holder, target, mode, ttl, limit)
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
    gate: Arc<Gate>,
    /// Reported as shared, whatever `inner` is.
    shared: bool,
}

/// What a [`Stalled`] store and its [`StallGate`] share.
struct Gate {
    open: tokio::sync::watch::Sender<bool>,
    calls: AtomicUsize,
    /// Shut the gate once this call has run, holding its answer or not (see
    /// [`StallGate::shut_after`]).
    shut_after: std::sync::Mutex<Option<(&'static str, bool)>>,
}

/// Shuts and opens a [`Stalled`] store (open to begin with), and counts the
/// calls that reached it.
pub struct StallGate(Arc<Gate>);

impl StallGate {
    pub fn shut(&self) {
        self.0.open.send_replace(false);
    }
    pub fn open(&self) {
        self.0.open.send_replace(true);
    }
    /// Whether it is shut, by [`StallGate::shut`] or [`StallGate::shut_after`].
    pub fn is_shut(&self) -> bool {
        !*self.0.open.borrow()
    }
    /// Calls made to the store so far (`shared` aside), waiting or not.
    pub fn calls(&self) -> usize {
        self.0.calls.load(Ordering::SeqCst)
    }
    /// Shut the gate as soon as the next `call` (a [`Storage`] method, by
    /// name, such as `"claim_task"`) has run on `inner`: the calls after it
    /// wait. With `hold_answer` its own answer waits too, as when a write
    /// lands but its answer is late.
    pub fn shut_after(&self, call: &'static str, hold_answer: bool) {
        *self.0.shut_after.lock().unwrap() = Some((call, hold_answer));
    }
}

impl Drop for StallGate {
    /// A dropped gate leaves the store as it was: calls go through.
    fn drop(&mut self) {
        self.0.shut_after.lock().unwrap().take();
        self.0.open.send_replace(true);
    }
}

impl Stalled {
    pub fn new(inner: Arc<dyn Storage>) -> (Self, StallGate) {
        let (open, _) = tokio::sync::watch::channel(true);
        let gate = Arc::new(Gate {
            open,
            calls: AtomicUsize::new(0),
            shut_after: std::sync::Mutex::new(None),
        });
        let stalled = Self {
            inner,
            gate: Arc::clone(&gate),
            shared: false,
        };
        (stalled, StallGate(gate))
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

    async fn until_open(&self) {
        let mut open = self.gate.open.subscribe();
        let _ = open.wait_for(|open| *open).await;
    }

    /// `call`, the method `name`, once the gate is open.
    async fn call<T>(&self, name: &'static str, call: impl std::future::Future<Output = T>) -> T {
        self.gate.calls.fetch_add(1, Ordering::SeqCst);
        self.until_open().await;
        let answer = call.await;
        let hook = {
            let mut hook = self.gate.shut_after.lock().unwrap();
            if hook.is_some_and(|(call, _)| call == name) {
                hook.take()
            } else {
                None
            }
        };
        if let Some((_, hold_answer)) = hook {
            self.gate.open.send_replace(false);
            if hold_answer {
                self.until_open().await;
            }
        }
        answer
    }
}

#[async_trait::async_trait]
impl Storage for Stalled {
    async fn migrate(&self) -> Result<(), StorageError> {
        self.call("migrate", self.inner.migrate()).await
    }
    async fn create_task(
        &self,
        task: &Task,
        lease: Option<&Lease<'_>>,
        pin: Option<&str>,
    ) -> Result<(), StorageError> {
        self.call("create_task", self.inner.create_task(task, lease, pin))
            .await
    }
    async fn get_task(&self, id: &str) -> Result<Option<Task>, StorageError> {
        self.call("get_task", self.inner.get_task(id)).await
    }
    async fn set_task_state(
        &self,
        id: &str,
        state: TaskState,
        fence: Fence<'_>,
    ) -> Result<bool, StorageError> {
        self.call(
            "set_task_state",
            self.inner.set_task_state(id, state, fence),
        )
        .await
    }
    async fn cancel_task_if_active(&self, id: &str) -> Result<bool, StorageError> {
        self.call(
            "cancel_task_if_active",
            self.inner.cancel_task_if_active(id),
        )
        .await
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
        self.call(
            "set_item_state",
            self.inner
                .set_item_state(task_id, item, state, error, fence, delivery),
        )
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
        self.call(
            "set_item_failed",
            self.inner
                .set_item_failed(task_id, item, error, models, fence, delivery),
        )
        .await
    }
    async fn find_task_by_request(
        &self,
        owner: Option<&str>,
        request_id: &str,
    ) -> Result<Option<String>, StorageError> {
        self.call(
            "find_task_by_request",
            self.inner.find_task_by_request(owner, request_id),
        )
        .await
    }
    async fn list_tasks(&self, query: &TaskQuery) -> Result<TaskPage, StorageError> {
        self.call("list_tasks", self.inner.list_tasks(query)).await
    }
    async fn upsert_model_result(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        result: &ModelResult,
        fence: Fence<'_>,
    ) -> Result<bool, StorageError> {
        self.call(
            "upsert_model_result",
            self.inner
                .upsert_model_result(task_id, item, label, result, fence),
        )
        .await
    }
    async fn append_samples(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        samples: &[apollo_domain::Sample],
    ) -> Result<(), StorageError> {
        self.call(
            "append_samples",
            self.inner.append_samples(task_id, item, label, samples),
        )
        .await
    }
    async fn load_samples(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
    ) -> Result<Vec<apollo_domain::Sample>, StorageError> {
        self.call(
            "load_samples",
            self.inner.load_samples(task_id, item, label),
        )
        .await
    }
    async fn set_steps_completed(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        steps: u32,
    ) -> Result<(), StorageError> {
        self.call(
            "set_steps_completed",
            self.inner.set_steps_completed(task_id, item, label, steps),
        )
        .await
    }
    async fn steps_completed(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
    ) -> Result<u32, StorageError> {
        self.call(
            "steps_completed",
            self.inner.steps_completed(task_id, item, label),
        )
        .await
    }
    async fn count_tasks_by_state(&self) -> Result<Vec<(TaskState, u64)>, StorageError> {
        self.call("count_tasks_by_state", self.inner.count_tasks_by_state())
            .await
    }
    async fn load_incomplete_tasks(&self) -> Result<Vec<Task>, StorageError> {
        self.call("load_incomplete_tasks", self.inner.load_incomplete_tasks())
            .await
    }
    async fn increment_attempts(&self, id: &str) -> Result<u32, StorageError> {
        self.call("increment_attempts", self.inner.increment_attempts(id))
            .await
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
        self.call(
            "claimable_tasks",
            self.inner
                .claimable_tasks(mode, node, skip, grace, after, limit),
        )
        .await
    }

    async fn claim_task(
        &self,
        id: &str,
        lease: &Lease<'_>,
        mode: ClaimMode,
        grace: Duration,
    ) -> Result<Option<u32>, StorageError> {
        self.call("claim_task", self.inner.claim_task(id, lease, mode, grace))
            .await
    }

    async fn renew_claims(
        &self,
        held: &[Held],
        ttl: Duration,
    ) -> Result<Vec<Renewal>, StorageError> {
        self.call("renew_claims", self.inner.renew_claims(held, ttl))
            .await
    }

    async fn release_claims(&self, held: &[Held]) -> Result<u64, StorageError> {
        self.call("release_claims", self.inner.release_claims(held))
            .await
    }
    async fn claim_pending_webhooks(
        &self,
        holder: &str,
        target: Option<&str>,
        mode: apollo_storage::ClaimMode,
        ttl: std::time::Duration,
        limit: usize,
    ) -> Result<Vec<apollo_storage::PendingWebhook>, StorageError> {
        self.call(
            "claim_pending_webhooks",
            self.inner
                .claim_pending_webhooks(holder, target, mode, ttl, limit),
        )
        .await
    }
    async fn renew_webhook_claims(
        &self,
        holder: &str,
        items: &[apollo_storage::PendingWebhook],
        ttl: std::time::Duration,
    ) -> Result<Vec<bool>, StorageError> {
        self.call(
            "renew_webhook_claims",
            self.inner.renew_webhook_claims(holder, items, ttl),
        )
        .await
    }
    async fn release_webhook_claim(
        &self,
        task_id: &str,
        item: usize,
        holder: &str,
    ) -> Result<bool, StorageError> {
        self.call(
            "release_webhook_claim",
            self.inner.release_webhook_claim(task_id, item, holder),
        )
        .await
    }
    async fn auth_key(&self) -> Result<Option<apollo_storage::SharedKey>, StorageError> {
        self.call("auth_key", self.inner.auth_key()).await
    }
    async fn publish_auth_key(
        &self,
        public_key: &str,
        node: &str,
    ) -> Result<apollo_storage::SharedKey, StorageError> {
        self.call(
            "publish_auth_key",
            self.inner.publish_auth_key(public_key, node),
        )
        .await
    }
    async fn replace_auth_key(&self, public_key: &str, node: &str) -> Result<(), StorageError> {
        self.call(
            "replace_auth_key",
            self.inner.replace_auth_key(public_key, node),
        )
        .await
    }
    async fn add_revocation(
        &self,
        revocation: &apollo_domain::Revocation,
    ) -> Result<(), StorageError> {
        self.call("add_revocation", self.inner.add_revocation(revocation))
            .await
    }
    async fn remove_revocation(
        &self,
        revocation: &apollo_domain::Revocation,
    ) -> Result<bool, StorageError> {
        self.call(
            "remove_revocation",
            self.inner.remove_revocation(revocation),
        )
        .await
    }
    async fn revocations(&self) -> Result<Vec<apollo_domain::Revocation>, StorageError> {
        self.call("revocations", self.inner.revocations()).await
    }
    async fn items_pending_webhook(
        &self,
    ) -> Result<Vec<apollo_storage::PendingWebhook>, StorageError> {
        self.call("items_pending_webhook", self.inner.items_pending_webhook())
            .await
    }
    async fn mark_webhook_delivered(
        &self,
        task_id: &str,
        item: usize,
        holder: &str,
    ) -> Result<bool, StorageError> {
        self.call(
            "mark_webhook_delivered",
            self.inner.mark_webhook_delivered(task_id, item, holder),
        )
        .await
    }
    async fn set_item_retries(
        &self,
        task_id: &str,
        item: usize,
        retries: u32,
        fence: Fence<'_>,
    ) -> Result<bool, StorageError> {
        self.call(
            "set_item_retries",
            self.inner.set_item_retries(task_id, item, retries, fence),
        )
        .await
    }
    async fn purge_finished_before(&self, cutoff_unix_secs: i64) -> Result<u64, StorageError> {
        self.call(
            "purge_finished_before",
            self.inner.purge_finished_before(cutoff_unix_secs),
        )
        .await
    }
    async fn cache_lookup(
        &self,
        content_hash: &str,
        model: &str,
        revision: &str,
        fresh_after: i64,
    ) -> Result<Option<apollo_domain::ModelOutput>, StorageError> {
        self.call(
            "cache_lookup",
            self.inner
                .cache_lookup(content_hash, model, revision, fresh_after),
        )
        .await
    }
    async fn cache_store(
        &self,
        content_hash: &str,
        model: &str,
        revision: &str,
        output: &apollo_domain::ModelOutput,
    ) -> Result<(), StorageError> {
        self.call(
            "cache_store",
            self.inner
                .cache_store(content_hash, model, revision, output),
        )
        .await
    }
    async fn url_cache_lookup(
        &self,
        url_hash: &str,
        model: &str,
        revision: &str,
        fresh_after: i64,
    ) -> Result<Option<String>, StorageError> {
        self.call(
            "url_cache_lookup",
            self.inner
                .url_cache_lookup(url_hash, model, revision, fresh_after),
        )
        .await
    }
    async fn url_cache_store(
        &self,
        url_hash: &str,
        model: &str,
        revision: &str,
        content_hash: &str,
    ) -> Result<(), StorageError> {
        self.call(
            "url_cache_store",
            self.inner
                .url_cache_store(url_hash, model, revision, content_hash),
        )
        .await
    }
    async fn purge_cache_before(&self, cutoff_unix_secs: i64) -> Result<u64, StorageError> {
        self.call(
            "purge_cache_before",
            self.inner.purge_cache_before(cutoff_unix_secs),
        )
        .await
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
        limit: usize,
    ) -> Result<Vec<apollo_storage::PendingWebhook>, StorageError> {
        self.inner
            .claim_pending_webhooks(holder, target, mode, ttl, limit)
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

/// Publish `snapshot` as `lc`'s latest load sample, as an engine's sampler
/// would.
pub fn publish_load(lc: &crate::Lifecycle, snapshot: crate::LoadSnapshot) {
    lc.publish_load(snapshot);
}

/// [`Engine::with_lifecycle`](crate::Engine::with_lifecycle), whose model
/// workers call `hook` with the model's label before each load of its
/// weights, on the worker's thread: it may hold the load, as a slow download
/// would (see [`HeldLoad`]).
pub fn engine_with_load_hook(
    config: Arc<apollo_config::Config>,
    storage: Arc<dyn Storage>,
    webhook: Option<Arc<dyn crate::WebhookSink>>,
    node: String,
    lifecycle: crate::Lifecycle,
    hook: impl Fn(&str) + Send + Sync + 'static,
) -> crate::Engine {
    crate::Engine::build(
        config,
        storage,
        webhook,
        node,
        lifecycle,
        Some(Arc::new(hook)),
    )
}

/// Holds every load of one model's weights until [`HeldLoad::release`]: pass
/// [`HeldLoad::hook`] to [`engine_with_load_hook`].
#[derive(Clone)]
pub struct HeldLoad {
    model: String,
    released: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    held: Arc<AtomicUsize>,
}

impl HeldLoad {
    pub fn new(model: &str) -> Self {
        Self {
            model: model.to_string(),
            released: Arc::default(),
            held: Arc::default(),
        }
    }

    /// The hook that holds the model's loads.
    pub fn hook(&self) -> impl Fn(&str) + Send + Sync + 'static {
        let this = self.clone();
        move |label| {
            if label != this.model {
                return;
            }
            this.held.fetch_add(1, Ordering::SeqCst);
            let (released, woken) = &*this.released;
            let mut released = released.lock().unwrap();
            while !*released {
                released = woken.wait(released).unwrap();
            }
        }
    }

    /// Loads that have reached the hold so far.
    pub fn held(&self) -> usize {
        self.held.load(Ordering::SeqCst)
    }

    /// Let every held load go on, and every later one.
    pub fn release(&self) {
        let (released, woken) = &*self.released;
        *released.lock().unwrap() = true;
        woken.notify_all();
    }
}
