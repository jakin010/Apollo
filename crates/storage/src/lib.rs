//! `apollo-storage` — persistence: the [`Storage`] trait + backends, plus resume
//! and retention helpers.
//!
//! The [`Storage`] trait is the seam new backends plug into. `[database].backend`
//! selects the implementation at startup via [`open`]. SQLite and SurrealDB are implemented; Postgres is a future seam.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use apollo_config::{Backend, DatabaseConfig};
use apollo_domain::{Frame, ItemState, ModelOutput, ModelResult, Task, TaskError, TaskState};

pub mod backends;
pub mod error;
pub mod resume;
pub mod retention;

pub use backends::{SqliteStorage, SurrealStorage};
pub use error::StorageError;

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

/// Persistence operations the rest of the app depends on. Task lifecycle is fully
/// persisted (it survives restarts), tracked at per-(item, model) granularity so
/// interrupted work can resume without redoing completed pieces.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Apply pending schema migrations. Idempotent.
    async fn migrate(&self) -> Result<(), StorageError>;

    // ----------------------------- lifecycle -----------------------------

    /// Persist a freshly submitted task (and its items) in one transaction.
    async fn create_task(&self, task: &Task) -> Result<(), StorageError>;

    /// Load a full task, reconstructing per-model results. Backs `GetTask`.
    async fn get_task(&self, id: &str) -> Result<Option<Task>, StorageError>;

    async fn set_task_state(&self, id: &str, state: TaskState) -> Result<(), StorageError>;

    async fn set_item_state(
        &self,
        task_id: &str,
        item: usize,
        state: ItemState,
        error: Option<&TaskError>,
    ) -> Result<(), StorageError>;

    /// Insert or update the result for one (item, model). Leaves frame progress
    /// and the steps-completed marker untouched.
    async fn upsert_model_result(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        result: &ModelResult,
    ) -> Result<(), StorageError>;

    // ------------------------ video scan checkpoint ----------------------

    /// Append (or replace) one classified frame. Written incrementally, the frame
    /// rows are the resume checkpoint for an interrupted scan.
    async fn append_frame(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        frame: &Frame,
    ) -> Result<(), StorageError>;

    /// Frames already classified for a (item, model), ordered by index. Seeds the
    /// dedupe set on resume.
    async fn load_frames(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
    ) -> Result<Vec<Frame>, StorageError>;

    /// Record how many sampling steps have fully completed, so resume can skip
    /// cheap re-extraction.
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

    /// All tasks still in a non-terminal state, fully reconstructed, for
    /// re-queueing on startup.
    async fn load_incomplete_tasks(&self) -> Result<Vec<Task>, StorageError>;

    /// Bump and return the resume attempt count. The engine fails a task once
    /// this exceeds its cap, so a poison task can't crash-loop.
    async fn increment_attempts(&self, id: &str) -> Result<u32, StorageError>;

    // -------------------------- webhook delivery -------------------------

    /// Terminal items whose webhook has not been delivered (recovers a crash
    /// between reaching a terminal state and delivering).
    async fn items_pending_webhook(&self) -> Result<Vec<PendingWebhook>, StorageError>;

    async fn mark_webhook_delivered(&self, task_id: &str, item: usize) -> Result<(), StorageError>;

    /// Persist an item's retry count (compared against `[app].max_retries`).
    async fn set_item_retries(
        &self,
        task_id: &str,
        item: usize,
        retries: u32,
    ) -> Result<(), StorageError>;

    /// Permanently-failed items whose dead-letter (failure) webhook has not yet
    /// been delivered — recovers a crash between final failure and delivery.
    async fn items_pending_failure_webhook(&self) -> Result<Vec<PendingWebhook>, StorageError>;

    async fn mark_failure_delivered(&self, task_id: &str, item: usize) -> Result<(), StorageError>;

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
            let store = SurrealStorage::connect(&cfg).await?;
            store.migrate().await?;
            Ok(Arc::new(store))
        }
    }
}
