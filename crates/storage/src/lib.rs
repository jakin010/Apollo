//! `apollo-storage` — persistence: the [`Storage`] trait + backends, plus resume
//! and retention helpers.
//!
//! The [`Storage`] trait is the seam new backends plug into. `[database].backend`
//! selects the implementation at startup via [`open`]. SQLite and SurrealDB are implemented; Postgres is a future seam.

use std::sync::Arc;

use async_trait::async_trait;

use apollo_config::{Backend, DatabaseConfig};
use apollo_domain::{Frame, ItemState, ModelResult, Task, TaskState};

pub mod backends;
pub mod error;
pub mod resume;
pub mod retention;

pub use backends::{SqliteStorage, SurrealStorage};
pub use error::StorageError;
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
        error: Option<&str>,
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

    async fn mark_webhook_delivered(&self, task_id: &str, item: usize)
        -> Result<(), StorageError>;

    // ------------------------------ retention ----------------------------

    /// Delete finished tasks last updated before `cutoff` (unix seconds).
    /// Returns the number of tasks removed.
    async fn purge_finished_before(&self, cutoff_unix_secs: i64) -> Result<u64, StorageError>;
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
