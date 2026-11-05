//! `apollo-storage` — persistence: the [`Storage`] trait + backends, plus resume
//! and retention helpers.
//!
//! The [`Storage`] trait is the seam new backends plug into. `[database].backend`
//! selects the implementation at startup via [`open`]. SQLite and SurrealDB are implemented; Postgres is a future seam.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use apollo_config::{Backend, DatabaseConfig};
use apollo_domain::{ItemState, ModelOutput, ModelResult, Sample, Task, TaskError, TaskState};

pub mod backends;
pub mod error;
pub mod lease;
pub mod resume;
pub mod retention;
#[cfg(feature = "test-util")]
pub mod testing;

pub use backends::{SqliteStorage, SurrealStorage};
pub use error::StorageError;
pub use lease::{ClaimMode, Claimable, Delivery, Fence, Held, Lease, Renewal};

/// Serialize a task error to the JSON stored in an `error` column.
fn error_to_json(error: Option<&TaskError>) -> Result<Option<String>, StorageError> {
    match error {
        Some(e) => Ok(Some(serde_json::to_string(e)?)),
        None => Ok(None),
    }
}

/// Parse a stored `error` column back into a task error, falling back to a
/// custom (uncategorized) error if the column is not valid JSON.
fn error_from_stored(raw: Option<String>) -> Option<TaskError> {
    raw.map(|s| serde_json::from_str::<TaskError>(&s).unwrap_or_else(|_| TaskError::custom(s)))
}

/// Current UNIX time in whole seconds, saturating to 0 before the epoch. Used for
/// the `created`/`updated` timestamps both backends stamp on writes.
pub(crate) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
pub use resume::PendingWebhook;

/// Which tasks [`Storage::list_tasks`] returns.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskQuery {
    /// `Some(owner)`: only that owner's tasks (`Some(None)` = tasks submitted
    /// without authentication). `None`: everyone's.
    pub owner: Option<Option<String>>,
    /// Only these states; all when empty.
    pub states: Vec<TaskState>,
    /// Created at or after / before these unix seconds.
    pub created_after: Option<i64>,
    pub created_before: Option<i64>,
    /// Page size (at least 1).
    pub limit: usize,
    /// Continue after this task (the last one of the previous page).
    pub after: Option<TaskCursor>,
}

/// A position in the newest-first task order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCursor {
    pub created_at: i64,
    pub id: String,
}

/// One page of [`Storage::list_tasks`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TaskPage {
    pub tasks: Vec<Task>,
    /// Where the next page starts; `None` on the last page.
    pub next: Option<TaskCursor>,
}

/// Whether a task in `state` is finished (it gets a `finished_at`).
pub(crate) fn is_terminal(state: TaskState) -> bool {
    matches!(
        state,
        TaskState::Completed | TaskState::Failed | TaskState::Cancelled
    )
}

/// Whether an item in `state` is finished (its finished webhook is due).
pub(crate) fn item_terminal(state: ItemState) -> bool {
    matches!(
        state,
        ItemState::Completed | ItemState::Failed | ItemState::Cancelled
    )
}

/// The stored form of a task's metadata (`None` when empty).
pub(crate) fn metadata_to_json(
    metadata: &std::collections::BTreeMap<String, String>,
) -> Result<Option<String>, StorageError> {
    if metadata.is_empty() {
        Ok(None)
    } else {
        Ok(Some(serde_json::to_string(metadata)?))
    }
}

/// Persistence operations the rest of the app depends on. Task lifecycle is fully
/// persisted (it survives restarts), tracked at per-(item, model) granularity so
/// interrupted work can resume without redoing completed pieces.
// `async_trait` marks the boxed-future methods `#[must_use]`; clippy >= 1.99
// reports that as `double_must_use`.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait Storage: Send + Sync {
    /// Apply pending schema migrations. Idempotent.
    async fn migrate(&self) -> Result<(), StorageError>;

    // ----------------------------- lifecycle -----------------------------

    /// Persist a freshly submitted task (and its items) in one transaction,
    /// claimed by `lease` when given, and pinned to `pin` (the node its input
    /// is local to) when given. Fails with [`StorageError::TaskExists`],
    /// writing nothing, when a task with its id is already stored.
    async fn create_task(
        &self,
        task: &Task,
        lease: Option<&Lease<'_>>,
        pin: Option<&str>,
    ) -> Result<(), StorageError>;

    /// Load a full task, reconstructing per-model results. Backs `GetTask`.
    async fn get_task(&self, id: &str) -> Result<Option<Task>, StorageError>;

    /// Set the state of a task that is still `Queued` or `Processing`, and
    /// still claimed as `fence` says. A finished task keeps its state, so the
    /// first finish wins over any later write (from a stale runner, or another
    /// instance sharing the database). Returns whether the state was written.
    async fn set_task_state(
        &self,
        id: &str,
        state: TaskState,
        fence: Fence<'_>,
    ) -> Result<bool, StorageError>;

    /// Mark a task `Cancelled` only if it is still `Queued` or `Processing`, in
    /// one conditional write, so a cancel can never overwrite a task that has
    /// just finished. Returns whether the state changed.
    async fn cancel_task_if_active(&self, id: &str) -> Result<bool, StorageError>;

    /// Set the state (and error) of an item that hasn't finished, while its
    /// task is claimed as `fence` says. A completed, failed or cancelled item
    /// keeps its state. A write that finishes the item also records its
    /// finished webhook as `delivery` says. Returns whether it was written.
    async fn set_item_state(
        &self,
        task_id: &str,
        item: usize,
        state: ItemState,
        error: Option<&TaskError>,
        fence: Fence<'_>,
        delivery: Delivery<'_>,
    ) -> Result<bool, StorageError>;

    /// Fail an item that hasn't finished with `error`, and each of `models`
    /// with it, in one transaction, while its task is claimed as `fence` says,
    /// recording its finished webhook as `delivery` says. Nothing is written
    /// otherwise. Returns whether it was written.
    async fn set_item_failed(
        &self,
        task_id: &str,
        item: usize,
        error: &TaskError,
        models: &[String],
        fence: Fence<'_>,
        delivery: Delivery<'_>,
    ) -> Result<bool, StorageError>;

    /// The id of the task `owner` created with idempotency key `request_id`, if
    /// any (`owner` is `None` for tasks submitted without authentication).
    async fn find_task_by_request(
        &self,
        owner: Option<&str>,
        request_id: &str,
    ) -> Result<Option<String>, StorageError>;

    /// A page of tasks matching `query`, newest first (by creation time, then
    /// id). Backs `ListTasks`.
    async fn list_tasks(&self, query: &TaskQuery) -> Result<TaskPage, StorageError>;

    /// Insert or update the result for one (item, model), while its task is
    /// claimed as `fence` says. Leaves frame progress and the steps-completed
    /// marker untouched. Returns whether it was written.
    async fn upsert_model_result(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        result: &ModelResult,
        fence: Fence<'_>,
    ) -> Result<bool, StorageError>;

    // ------------------------- scan checkpoint --------------------------

    /// Append (or replace) a batch of classified samples atomically. Written
    /// incrementally, the sample rows are the resume checkpoint for an
    /// interrupted scan.
    async fn append_samples(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        samples: &[Sample],
    ) -> Result<(), StorageError>;

    /// Samples already classified for a (item, model), ordered by index. Seeds
    /// the dedupe set on resume.
    async fn load_samples(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
    ) -> Result<Vec<Sample>, StorageError>;

    /// Record how many sampling steps have fully completed. Reserved for skipping
    /// finished steps on resume; the engine does not use it yet (resume relies on
    /// the frame rows alone).
    async fn set_steps_completed(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        steps: u32,
    ) -> Result<(), StorageError>;

    async fn steps_completed(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
    ) -> Result<u32, StorageError>;

    // ------------------------------- resume ------------------------------

    /// Number of tasks in each state (states with no tasks are omitted). Backs
    /// `apollo status`.
    async fn count_tasks_by_state(&self) -> Result<Vec<(TaskState, u64)>, StorageError>;

    /// All tasks still in a non-terminal state, fully reconstructed (the
    /// upload sweep keeps their files).
    async fn load_incomplete_tasks(&self) -> Result<Vec<Task>, StorageError>;

    /// Bump and return the resume attempt count. [`Storage::claim_task`] bumps
    /// it itself; this is for tools and tests.
    async fn increment_attempts(&self, id: &str) -> Result<u32, StorageError>;

    // ------------------------------- leases ------------------------------

    /// Whether other processes may share this database (SurrealDB): then
    /// leases decide who runs what. SQLite is one process's file.
    fn shared(&self) -> bool;

    /// Tasks a claim in `mode` may take, oldest first (creation time, then
    /// id), after `after`: unfinished ones, and cancelled ones with unfinished
    /// items (to finish them, as cancelled). Leaves out the ids in `skip`, and
    /// (except in [`ClaimMode::Force`]) tasks pinned to a node other than
    /// `node` whose lease ended less than `grace` ago.
    async fn claimable_tasks(
        &self,
        mode: ClaimMode,
        node: &str,
        skip: &[String],
        grace: std::time::Duration,
        after: Option<&TaskCursor>,
        limit: usize,
    ) -> Result<Vec<Claimable>, StorageError>;

    /// Claim the task `id` with `lease`, if it is still claimable as for
    /// [`Storage::claimable_tasks`]. Bumps its resume attempts when the claim
    /// it replaces was never handed back (its holder stopped: a crash), not
    /// when it was released or never claimed. Returns the attempts after the
    /// claim, or `None` when it wasn't claimable any more.
    async fn claim_task(
        &self,
        id: &str,
        lease: &Lease<'_>,
        mode: ClaimMode,
        grace: std::time::Duration,
    ) -> Result<Option<u32>, StorageError>;

    /// Renew each held claim for `ttl`, whatever its task's state, and say for
    /// each (in order) whether it is still held. Doesn't count as a change to
    /// the task.
    async fn renew_claims(
        &self,
        held: &[Held],
        ttl: std::time::Duration,
    ) -> Result<Vec<Renewal>, StorageError>;

    /// Hand held claims back (a graceful stop): the tasks are claimable at
    /// once, and the next claim counts no attempt. Returns how many were
    /// still held.
    async fn release_claims(&self, held: &[Held]) -> Result<u64, StorageError>;

    // -------------------------- webhook delivery -------------------------

    /// Every finished item whose webhook has not been delivered, whoever is
    /// delivering it (for status and metrics).
    async fn items_pending_webhook(&self) -> Result<Vec<PendingWebhook>, StorageError>;

    /// Claim the finished items whose webhook is pending for `holder` to
    /// deliver, for `ttl` on the database's clock: those whose delivery claim
    /// `mode` may take, for receiver `target` (or any, with `None`; items
    /// stored for any receiver go to every one). Returns the items claimed.
    async fn claim_pending_webhooks(
        &self,
        holder: &str,
        target: Option<&str>,
        mode: ClaimMode,
        ttl: std::time::Duration,
    ) -> Result<Vec<PendingWebhook>, StorageError>;

    /// Renew `holder`'s delivery claims on `items` for `ttl`, saying for each
    /// (in order) whether `holder` still holds it.
    async fn renew_webhook_claims(
        &self,
        holder: &str,
        items: &[PendingWebhook],
        ttl: std::time::Duration,
    ) -> Result<Vec<bool>, StorageError>;

    /// Hand back `holder`'s delivery claim on an item (its delivery failed),
    /// so any instance may deliver it. Returns whether `holder` held it.
    async fn release_webhook_claim(
        &self,
        task_id: &str,
        item: usize,
        holder: &str,
    ) -> Result<bool, StorageError>;

    /// Mark an item's webhook delivered, only while `holder` holds its delivery
    /// claim. Returns whether it did.
    async fn mark_webhook_delivered(
        &self,
        task_id: &str,
        item: usize,
        holder: &str,
    ) -> Result<bool, StorageError>;

    /// Persist an item's retry count (compared against `[app].max_retries`),
    /// while its task is claimed as `fence` says. Returns whether it was
    /// written.
    async fn set_item_retries(
        &self,
        task_id: &str,
        item: usize,
        retries: u32,
        fence: Fence<'_>,
    ) -> Result<bool, StorageError>;

    // ------------------------------ retention ----------------------------

    /// Delete finished tasks last updated before `cutoff` (unix seconds).
    /// Returns the number of tasks removed.
    async fn purge_finished_before(&self, cutoff_unix_secs: i64) -> Result<u64, StorageError>;

    // -------------------------------- cache ------------------------------

    /// Look up a cached model output by content hash. `fresh_after` is a lower
    /// bound on `created_at` in unix seconds (`0` = no TTL). Returns the output if
    /// a fresh entry exists.
    async fn cache_lookup(
        &self,
        content_hash: &str,
        model: &str,
        revision: &str,
        fresh_after: i64,
    ) -> Result<Option<ModelOutput>, StorageError>;

    /// Store a model output under its content hash (upsert; refreshes `created_at`).
    async fn cache_store(
        &self,
        content_hash: &str,
        model: &str,
        revision: &str,
        output: &ModelOutput,
    ) -> Result<(), StorageError>;

    /// Look up the content hash a URL last resolved to (the url->content hint).
    /// `fresh_after` as above.
    async fn url_cache_lookup(
        &self,
        url_hash: &str,
        model: &str,
        revision: &str,
        fresh_after: i64,
    ) -> Result<Option<String>, StorageError>;

    /// Record the content hash a URL resolved to (upsert; refreshes `created_at`).
    async fn url_cache_store(
        &self,
        url_hash: &str,
        model: &str,
        revision: &str,
        content_hash: &str,
    ) -> Result<(), StorageError>;

    /// Delete content-cache and url-cache entries created before `cutoff` (unix
    /// seconds). Returns the number of entries removed.
    async fn purge_cache_before(&self, cutoff_unix_secs: i64) -> Result<u64, StorageError>;
}

/// Open the configured backend and run migrations. Returns a trait object so the
/// rest of the app stays backend-agnostic.
pub async fn open(db: &DatabaseConfig) -> Result<Arc<dyn Storage>, StorageError> {
    match db.backend {
        Backend::Sqlite => {
            let cfg = db.sqlite.clone().unwrap_or_default();
            let store = SqliteStorage::connect(&cfg).await?;
            store.migrate().await?;
            Ok(Arc::new(store))
        }
        Backend::Postgres => Err(StorageError::UnsupportedBackend("postgres".into())),
        Backend::Surrealdb => {
            let cfg = db.surrealdb.clone().ok_or_else(|| {
                StorageError::Surreal(
                    "backend is 'surrealdb' but [database.surrealdb] is missing".into(),
                )
            })?;
            // Within `[database].timeout`, so a host that doesn't answer fails
            // the start instead of hanging it. The migration has no limit:
            // defining an index over existing rows builds it first.
            let connect = SurrealStorage::connect(&cfg);
            let store = match db.timeout {
                0 => connect.await?,
                secs => {
                    let limit = std::time::Duration::from_secs(secs.into());
                    tokio::time::timeout(limit, connect)
                        .await
                        .unwrap_or(Err(StorageError::Timeout(limit)))?
                }
            };
            store.migrate().await?;
            Ok(Arc::new(store))
        }
    }
}
