//! SurrealDB backend, targeting the `surrealdb` Rust SDK 3.1.x (a remote server
//! reached over `ws(s)://` or `http(s)://`). Implements [`crate::Storage`].
//!
//! Data model mirrors the SQLite backend's relational shape, but as SurrealDB
//! records with deterministic composite (array) record ids, so every fine-grained
//! update targets exactly one record (no array-index read-modify-write races):
//!   `task:⟨id⟩`,
//!   `item:[id, idx]`,
//!   `model_result:[id, idx, label]`,
//!   `frame:[id, idx, label, frame_index]`.
//! The logical task id is also stored as the `key` field on `task` so it can be
//! selected back without parsing record ids. Complex domain values (`input`,
//! `models`, model `output`, frame `classification`) are stored as JSON strings
//! exactly as in SQLite, so they round-trip through `serde_json` and are opaque to
//! SurrealDB — this keeps storage robust across SurrealDB version changes.
//!
//! Upgrade notes: the dependency is pinned `3.1` (allowing future 3.x). The whole
//! SurrealDB surface used here is confined to this file — `engine::any::connect`,
//! `Root` auth, `use_ns`/`use_db`, and `query`/`bind`/`take` — plus the SurrealQL
//! in the `const` statements below. Moving to a new major (4.x) means bumping the
//! version literal and revisiting only this module.

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;
use surrealdb::types::SurrealValue;
use surrealdb::Surreal;

use apollo_config::SurrealdbConfig;
use apollo_domain::{
    Classification, Frame, Input, Item, ItemState, ModelOutput, ModelResult, ModelState, Task,
    TaskState,
};

use crate::error::StorageError;
use crate::resume::PendingWebhook;
use crate::Storage;

type Result<T> = std::result::Result<T, StorageError>;

// Convert SurrealDB errors at the crate boundary so `error.rs` stays free of the
// SurrealDB types (and `?` works throughout this module).
impl From<surrealdb::Error> for StorageError {
    fn from(e: surrealdb::Error) -> Self {
        StorageError::Surreal(e.to_string())
    }
}

/// Idempotent schema: schemaless tables plus the indexes the queries rely on.
/// `IF NOT EXISTS` makes re-running a no-op.
const SCHEMA: &str = "
    DEFINE TABLE IF NOT EXISTS task SCHEMALESS;
    DEFINE TABLE IF NOT EXISTS item SCHEMALESS;
    DEFINE TABLE IF NOT EXISTS model_result SCHEMALESS;
    DEFINE TABLE IF NOT EXISTS frame SCHEMALESS;
    DEFINE INDEX IF NOT EXISTS task_state ON TABLE task FIELDS state;
    DEFINE INDEX IF NOT EXISTS task_created ON TABLE task FIELDS created_at;
    DEFINE INDEX IF NOT EXISTS item_task ON TABLE item FIELDS task_id;
    DEFINE INDEX IF NOT EXISTS item_webhook ON TABLE item FIELDS webhook_delivered, state;
    DEFINE INDEX IF NOT EXISTS mr_item ON TABLE model_result FIELDS task_id, item_idx;
    DEFINE INDEX IF NOT EXISTS frame_key ON TABLE frame FIELDS task_id, item_idx, label;
";

/// Persist a task, its items, and any pre-existing model results in one
/// transaction. `FOR` loops keep the bound parameter set fixed regardless of how
/// many items the task has.
const CREATE_TASK: &str = "
    BEGIN TRANSACTION;
    CREATE type::record('task', $tid) SET
        key = $tid, state = $tstate, attempts = 0, created_at = $ts, updated_at = $ts;
    FOR $it IN $items {
        CREATE type::record('item', [$tid, $it.idx]) SET
            task_id = $tid, idx = $it.idx, input_json = $it.input_json,
            models_json = $it.models_json, state = $it.state, error = $it.error,
            webhook_delivered = false;
    };
    FOR $mr IN $mrs {
        CREATE type::record('model_result', [$tid, $mr.item_idx, $mr.label]) SET
            task_id = $tid, item_idx = $mr.item_idx, label = $mr.label,
            state = $mr.state, output_json = $mr.output_json, error = $mr.error,
            steps_completed = 0;
    };
    COMMIT TRANSACTION;
";

// ------------------------------- row types -------------------------------
// Deserialized from query results / serialized for binds. In SurrealDB 3.x these
// implement `SurrealValue` (via derive); complex fields are opaque JSON strings.

#[derive(SurrealValue)]
struct TaskStateRow {
    state: String,
}

#[derive(SurrealValue)]
struct ItemRow {
    idx: i64,
    input_json: String,
    models_json: String,
    state: String,
    error: Option<String>,
}

#[derive(SurrealValue)]
struct ModelRow {
    label: String,
    state: String,
    output_json: Option<String>,
    error: Option<String>,
}

#[derive(SurrealValue)]
struct FrameRow {
    frame_index: i64,
    timestamp: f64,
    class_json: String,
}

#[derive(SurrealValue)]
struct StepsRow {
    steps_completed: Option<i64>,
}

#[derive(SurrealValue)]
struct PendingRow {
    task_id: String,
    idx: i64,
}

#[derive(SurrealValue)]
struct ItemInsert {
    idx: i64,
    input_json: String,
    models_json: String,
    state: String,
    error: Option<String>,
}

#[derive(SurrealValue)]
struct ModelInsert {
    item_idx: i64,
    label: String,
    state: String,
    output_json: Option<String>,
    error: Option<String>,
}

/// SurrealDB-backed [`Storage`].
pub struct SurrealStorage {
    db: Surreal<Any>,
}

impl SurrealStorage {
    /// Connect to `[database.surrealdb].url` (any scheme the SDK supports: ws,
    /// wss, http, https), sign in as root if credentials are given, and select the
    /// configured namespace + database. The connection is established eagerly so a
    /// misconfiguration fails fast at startup.
    pub async fn connect(cfg: &SurrealdbConfig) -> Result<Self> {
        let db = surrealdb::engine::any::connect(cfg.url.clone()).await?;
        if let (Some(user), Some(pass)) = (cfg.user.as_ref(), cfg.password.as_ref()) {
            db.signin(Root {
                username: user.clone(),
                password: pass.clone(),
            })
            .await?;
        }
        db.use_ns(cfg.namespace.clone())
            .use_db(cfg.database.clone())
            .await?;
        Ok(Self { db })
    }

    /// Bump a task's `updated_at` (retention and recency tracking).
    async fn touch(&self, id: &str) -> Result<()> {
        self.db
            .query("UPDATE type::record('task', $tid) SET updated_at = $ts")
            .bind(("tid", id.to_string()))
            .bind(("ts", now()))
            .await?
            .check()?;
        Ok(())
    }

    /// Shared reconstruction used by `get_task` and `load_incomplete_tasks`.
    /// Every requested model starts `Queued`; stored `model_result` rows overlay
    /// that baseline, so a model that hasn't started still reads back as queued.
    async fn load_task(&self, id: &str) -> Result<Option<Task>> {
        let mut resp = self
            .db
            .query("SELECT state FROM type::record('task', $tid)")
            .bind(("tid", id.to_string()))
            .await?;
        let trows: Vec<TaskStateRow> = resp.take(0)?;
        let Some(trow) = trows.into_iter().next() else {
            return Ok(None);
        };
        let state = parse_task_state(&trow.state)?;

        let mut resp = self
            .db
            .query(
                "SELECT idx, input_json, models_json, state, error
                 FROM item WHERE task_id = $tid ORDER BY idx",
            )
            .bind(("tid", id.to_string()))
            .await?;
        let irows: Vec<ItemRow> = resp.take(0)?;

        let mut items = Vec::with_capacity(irows.len());
        for ir in irows {
            let input: Input = serde_json::from_str(&ir.input_json)?;
            let models: Vec<String> = serde_json::from_str(&ir.models_json)?;

            let mut results = std::collections::BTreeMap::new();
            for label in &models {
                results.insert(label.clone(), ModelResult::queued());
            }

            let mut resp = self
                .db
                .query(
                    "SELECT label, state, output_json, error
                     FROM model_result WHERE task_id = $tid AND item_idx = $idx",
                )
                .bind(("tid", id.to_string()))
                .bind(("idx", ir.idx))
                .await?;
            let mrows: Vec<ModelRow> = resp.take(0)?;
            for mr in mrows {
                let output = match mr.output_json {
                    Some(j) => Some(serde_json::from_str::<ModelOutput>(&j)?),
                    None => None,
                };
                results.insert(
                    mr.label,
                    ModelResult {
                        state: parse_model_state(&mr.state)?,
                        output,
                        error: mr.error,
                    },
                );
            }

            items.push(Item {
                input,
                models,
                state: parse_item_state(&ir.state)?,
                results,
                error: ir.error,
            });
        }

        Ok(Some(Task {
            id: id.to_string(),
            state,
            items,
        }))
    }
}

#[async_trait]
impl Storage for SurrealStorage {
    async fn migrate(&self) -> Result<()> {
        self.db.query(SCHEMA).await?.check()?;
        Ok(())
    }

    async fn create_task(&self, task: &Task) -> Result<()> {
        let ts = now();
        let mut items = Vec::with_capacity(task.items.len());
        let mut mrs = Vec::new();
        for (idx, item) in task.items.iter().enumerate() {
            items.push(ItemInsert {
                idx: idx as i64,
                input_json: serde_json::to_string(&item.input)?,
                models_json: serde_json::to_string(&item.models)?,
                state: item_state_str(item.state).to_string(),
                error: item.error.clone(),
            });
            for (label, mr) in &item.results {
                mrs.push(ModelInsert {
                    item_idx: idx as i64,
                    label: label.clone(),
                    state: model_state_str(mr.state).to_string(),
                    output_json: match &mr.output {
                        Some(o) => Some(serde_json::to_string(o)?),
                        None => None,
                    },
                    error: mr.error.clone(),
                });
            }
        }

        self.db
            .query(CREATE_TASK)
            .bind(("tid", task.id.clone()))
            .bind(("tstate", task_state_str(task.state).to_string()))
            .bind(("ts", ts))
            .bind(("items", items))
            .bind(("mrs", mrs))
            .await?
            .check()?;
        Ok(())
    }

    async fn get_task(&self, id: &str) -> Result<Option<Task>> {
        self.load_task(id).await
    }

    async fn set_task_state(&self, id: &str, state: TaskState) -> Result<()> {
        self.db
            .query("UPDATE type::record('task', $tid) SET state = $state, updated_at = $ts")
            .bind(("tid", id.to_string()))
            .bind(("state", task_state_str(state).to_string()))
            .bind(("ts", now()))
            .await?
            .check()?;
        Ok(())
    }

    async fn set_item_state(
        &self,
        task_id: &str,
        item: usize,
        state: ItemState,
        error: Option<&str>,
    ) -> Result<()> {
        self.db
            .query("UPDATE type::record('item', [$tid, $idx]) SET state = $state, error = $error")
            .bind(("tid", task_id.to_string()))
            .bind(("idx", item as i64))
            .bind(("state", item_state_str(state).to_string()))
            .bind(("error", error.map(|s| s.to_string())))
            .await?
            .check()?;
        self.touch(task_id).await
    }

    async fn upsert_model_result(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        result: &ModelResult,
    ) -> Result<()> {
        let output_json = match &result.output {
            Some(o) => Some(serde_json::to_string(o)?),
            None => None,
        };
        // UPSERT only touches the listed fields, so a concurrent `steps_completed`
        // marker and the frame rows are left intact.
        self.db
            .query(
                "UPSERT type::record('model_result', [$tid, $idx, $label]) SET
                    task_id = $tid, item_idx = $idx, label = $label,
                    state = $state, output_json = $output, error = $error",
            )
            .bind(("tid", task_id.to_string()))
            .bind(("idx", item as i64))
            .bind(("label", label.to_string()))
            .bind(("state", model_state_str(result.state).to_string()))
            .bind(("output", output_json))
            .bind(("error", result.error.clone()))
            .await?
            .check()?;
        self.touch(task_id).await
    }

    async fn append_frame(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        frame: &Frame,
    ) -> Result<()> {
        let class_json = serde_json::to_string(&frame.classification)?;
        self.db
            .query(
                "UPSERT type::record('frame', [$tid, $idx, $label, $fidx]) SET
                    task_id = $tid, item_idx = $idx, label = $label,
                    frame_index = $fidx, timestamp = $ts, class_json = $cj",
            )
            .bind(("tid", task_id.to_string()))
            .bind(("idx", item as i64))
            .bind(("label", label.to_string()))
            .bind(("fidx", frame.index as i64))
            .bind(("ts", frame.timestamp))
            .bind(("cj", class_json))
            .await?
            .check()?;
        Ok(())
    }

    async fn load_frames(&self, task_id: &str, item: usize, label: &str) -> Result<Vec<Frame>> {
        let mut resp = self
            .db
            .query(
                "SELECT frame_index, timestamp, class_json
                 FROM frame WHERE task_id = $tid AND item_idx = $idx AND label = $label
                 ORDER BY frame_index",
            )
            .bind(("tid", task_id.to_string()))
            .bind(("idx", item as i64))
            .bind(("label", label.to_string()))
            .await?;
        let rows: Vec<FrameRow> = resp.take(0)?;

        let mut frames = Vec::with_capacity(rows.len());
        for r in rows {
            let classification: Classification = serde_json::from_str(&r.class_json)?;
            frames.push(Frame {
                timestamp: r.timestamp,
                index: r.frame_index as u32,
                classification,
            });
        }
        Ok(frames)
    }

    async fn set_steps_completed(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        steps: u32,
    ) -> Result<()> {
        // Upsert so a scan that hasn't recorded a result row yet still works. On a
        // fresh row `state` is unset, so default it to 'processing'; on an existing
        // row the conditional preserves whatever state is already there.
        self.db
            .query(
                "UPSERT type::record('model_result', [$tid, $idx, $label]) SET
                    task_id = $tid, item_idx = $idx, label = $label,
                    steps_completed = $steps,
                    state = IF state THEN state ELSE 'processing' END",
            )
            .bind(("tid", task_id.to_string()))
            .bind(("idx", item as i64))
            .bind(("label", label.to_string()))
            .bind(("steps", steps as i64))
            .await?
            .check()?;
        Ok(())
    }

    async fn steps_completed(&self, task_id: &str, item: usize, label: &str) -> Result<u32> {
        let mut resp = self
            .db
            .query(
                "SELECT steps_completed FROM type::record('model_result', [$tid, $idx, $label])",
            )
            .bind(("tid", task_id.to_string()))
            .bind(("idx", item as i64))
            .bind(("label", label.to_string()))
            .await?;
        let rows: Vec<StepsRow> = resp.take(0)?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|r| r.steps_completed)
            .unwrap_or(0) as u32)
    }

    async fn load_incomplete_tasks(&self) -> Result<Vec<Task>> {
        let mut resp = self
            .db
            .query(
                "SELECT VALUE key FROM task
                 WHERE state IN ['queued', 'processing'] ORDER BY created_at",
            )
            .await?;
        let ids: Vec<String> = resp.take(0)?;

        let mut tasks = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(t) = self.load_task(&id).await? {
                tasks.push(t);
            }
        }
        Ok(tasks)
    }

    async fn increment_attempts(&self, id: &str) -> Result<u32> {
        let mut resp = self
            .db
            .query(
                "UPDATE type::record('task', $tid) SET attempts += 1, updated_at = $ts
                 RETURN VALUE attempts",
            )
            .bind(("tid", id.to_string()))
            .bind(("ts", now()))
            .await?;
        let vals: Vec<i64> = resp.take(0)?;
        Ok(vals.into_iter().next().unwrap_or(0) as u32)
    }

    async fn items_pending_webhook(&self) -> Result<Vec<PendingWebhook>> {
        let mut resp = self
            .db
            .query(
                "SELECT task_id, idx FROM item
                 WHERE webhook_delivered = false
                   AND state IN ['completed', 'failed', 'cancelled']",
            )
            .await?;
        let rows: Vec<PendingRow> = resp.take(0)?;
        Ok(rows
            .into_iter()
            .map(|r| PendingWebhook {
                task_id: r.task_id,
                item_index: r.idx as usize,
            })
            .collect())
    }

    async fn mark_webhook_delivered(&self, task_id: &str, item: usize) -> Result<()> {
        self.db
            .query("UPDATE type::record('item', [$tid, $idx]) SET webhook_delivered = true")
            .bind(("tid", task_id.to_string()))
            .bind(("idx", item as i64))
            .await?
            .check()?;
        Ok(())
    }

    async fn purge_finished_before(&self, cutoff_unix_secs: i64) -> Result<u64> {
        // SurrealDB does not cascade, so collect the doomed task ids first, then
        // delete children before the tasks themselves (a partial purge just leaves
        // a task to be retried next cycle — never an orphaned child).
        let mut resp = self
            .db
            .query(
                "SELECT VALUE key FROM task
                 WHERE state IN ['completed', 'failed', 'cancelled'] AND updated_at < $cutoff",
            )
            .bind(("cutoff", cutoff_unix_secs))
            .await?;
        let ids: Vec<String> = resp.take(0)?;
        if ids.is_empty() {
            return Ok(0);
        }

        self.db
            .query(
                "DELETE frame WHERE task_id IN $ids;
                 DELETE model_result WHERE task_id IN $ids;
                 DELETE item WHERE task_id IN $ids;
                 DELETE task WHERE key IN $ids;",
            )
            .bind(("ids", ids.clone()))
            .await?
            .check()?;
        Ok(ids.len() as u64)
    }
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn task_state_str(s: TaskState) -> &'static str {
    match s {
        TaskState::Queued => "queued",
        TaskState::Processing => "processing",
        TaskState::Completed => "completed",
        TaskState::Failed => "failed",
        TaskState::Cancelled => "cancelled",
    }
}

fn parse_task_state(s: &str) -> Result<TaskState> {
    Ok(match s {
        "queued" => TaskState::Queued,
        "processing" => TaskState::Processing,
        "completed" => TaskState::Completed,
        "failed" => TaskState::Failed,
        "cancelled" => TaskState::Cancelled,
        other => return Err(StorageError::Corrupt(format!("unknown task state '{other}'"))),
    })
}

fn item_state_str(s: ItemState) -> &'static str {
    match s {
        ItemState::Queued => "queued",
        ItemState::Processing => "processing",
        ItemState::Completed => "completed",
        ItemState::Failed => "failed",
        ItemState::Cancelled => "cancelled",
    }
}

fn parse_item_state(s: &str) -> Result<ItemState> {
    Ok(match s {
        "queued" => ItemState::Queued,
        "processing" => ItemState::Processing,
        "completed" => ItemState::Completed,
        "failed" => ItemState::Failed,
        "cancelled" => ItemState::Cancelled,
        other => return Err(StorageError::Corrupt(format!("unknown item state '{other}'"))),
    })
}

fn model_state_str(s: ModelState) -> &'static str {
    match s {
        ModelState::Queued => "queued",
        ModelState::Processing => "processing",
        ModelState::Done => "done",
        ModelState::Failed => "failed",
    }
}

fn parse_model_state(s: &str) -> Result<ModelState> {
    Ok(match s {
        "queued" => ModelState::Queued,
        "processing" => ModelState::Processing,
        "done" => ModelState::Done,
        "failed" => ModelState::Failed,
        other => return Err(StorageError::Corrupt(format!("unknown model state '{other}'"))),
    })
}
