//! SQLite backend (sqlx). Implements [`crate::Storage`].
//!
//! Schema: `tasks` 1—N `items` 1—N `model_results` 1—N `frames`, with
//! `ON DELETE CASCADE` so retention is a single delete from `tasks`. The full set
//! of requested models lives in `items.models_json`; `model_results` rows hold
//! per-model progress and are overlaid onto a queued baseline when reconstructing
//! a task, so a model that hasn't started yet still reads back as `Queued`.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

use apollo_config::SqliteConfig;
use apollo_domain::{
    Classification, Frame, Item, ItemState, ModelOutput, ModelResult, ModelState, Task, TaskError,
    TaskState,
};

use crate::Storage;
use crate::error::StorageError;
use crate::now;
use crate::resume::PendingWebhook;

type Result<T> = std::result::Result<T, StorageError>;

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS tasks (
        id          TEXT PRIMARY KEY,
        state       TEXT NOT NULL,
        attempts    INTEGER NOT NULL DEFAULT 0,
        created_at  INTEGER NOT NULL,
        updated_at  INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS items (
        task_id           TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
        idx               INTEGER NOT NULL,
        input_json        TEXT NOT NULL,
        models_json       TEXT NOT NULL,
        pipeline          TEXT,
        state             TEXT NOT NULL,
        error             TEXT,
        webhook_delivered INTEGER NOT NULL DEFAULT 0,
        retries           INTEGER NOT NULL DEFAULT 0,
        failure_delivered INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (task_id, idx)
    )",
    "CREATE TABLE IF NOT EXISTS model_results (
        task_id         TEXT NOT NULL,
        item_idx        INTEGER NOT NULL,
        label           TEXT NOT NULL,
        state           TEXT NOT NULL,
        output_json     TEXT,
        error           TEXT,
        steps_completed INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (task_id, item_idx, label),
        FOREIGN KEY (task_id, item_idx) REFERENCES items(task_id, idx) ON DELETE CASCADE
    )",
    "CREATE TABLE IF NOT EXISTS frames (
        task_id     TEXT NOT NULL,
        item_idx    INTEGER NOT NULL,
        label       TEXT NOT NULL,
        frame_index INTEGER NOT NULL,
        timestamp   REAL NOT NULL,
        class_json  TEXT NOT NULL,
        PRIMARY KEY (task_id, item_idx, label, frame_index),
        FOREIGN KEY (task_id, item_idx, label)
            REFERENCES model_results(task_id, item_idx, label) ON DELETE CASCADE
    )",
    "CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state)",
    "CREATE TABLE IF NOT EXISTS cache (
        content_hash TEXT NOT NULL,
        model        TEXT NOT NULL,
        revision     TEXT NOT NULL,
        output_json  TEXT NOT NULL,
        created_at   INTEGER NOT NULL,
        PRIMARY KEY (content_hash, model, revision)
    )",
    "CREATE TABLE IF NOT EXISTS url_cache (
        url_hash     TEXT NOT NULL,
        model        TEXT NOT NULL,
        revision     TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at   INTEGER NOT NULL,
        PRIMARY KEY (url_hash, model, revision)
    )",
];

/// SQLite-backed [`Storage`].
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    /// Open (creating if missing) and configure the pool from `[database.sqlite]`.
    pub async fn connect(cfg: &SqliteConfig) -> Result<Self> {
        let journal = if cfg.wal {
            SqliteJournalMode::Wal
        } else {
            SqliteJournalMode::Delete
        };
        let opts = SqliteConnectOptions::new()
            .filename(&cfg.path)
            .create_if_missing(true)
            .journal_mode(journal)
            .busy_timeout(Duration::from_millis(cfg.busy_timeout as u64))
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(cfg.max_connections.max(1))
            .connect_with(opts)
            .await?;
        Ok(Self { pool })
    }

    async fn touch(&self, id: &str) -> Result<()> {
        sqlx::query("UPDATE tasks SET updated_at = ? WHERE id = ?")
            .bind(now())
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Shared reconstruction used by `get_task` and `load_incomplete_tasks`.
    async fn load_task(&self, id: &str) -> Result<Option<Task>> {
        let Some(trow) = sqlx::query("SELECT state FROM tasks WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        let state = trow.try_get::<String, _>("state")?.parse::<TaskState>()?;

        let irows = sqlx::query(
            "SELECT idx, input_json, models_json, pipeline, state, error, retries
             FROM items WHERE task_id = ? ORDER BY idx",
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await?;

        let mut items = Vec::with_capacity(irows.len());
        for ir in irows {
            let idx: i64 = ir.try_get("idx")?;
            let input_json: String = ir.try_get("input_json")?;
            let models_json: String = ir.try_get("models_json")?;
            let ipipeline: Option<String> = ir.try_get("pipeline")?;
            let istate = ir.try_get::<String, _>("state")?.parse::<ItemState>()?;
            let ierror: Option<String> = ir.try_get("error")?;
            let iretries: i64 = ir.try_get("retries")?;

            let input = serde_json::from_str(&input_json)?;
            let models: Vec<String> = serde_json::from_str(&models_json)?;

            // Baseline: every requested model is queued; overlay stored progress.
            let mut results = std::collections::BTreeMap::new();
            for label in &models {
                results.insert(label.clone(), ModelResult::queued());
            }
            let mrows = sqlx::query(
                "SELECT label, state, output_json, error
                 FROM model_results WHERE task_id = ? AND item_idx = ?",
            )
            .bind(id)
            .bind(idx)
            .fetch_all(&self.pool)
            .await?;
            for mr in mrows {
                let label: String = mr.try_get("label")?;
                let mstate = mr.try_get::<String, _>("state")?.parse::<ModelState>()?;
                let output_json: Option<String> = mr.try_get("output_json")?;
                let merror: Option<String> = mr.try_get("error")?;
                let output = match output_json {
                    Some(j) => Some(serde_json::from_str::<ModelOutput>(&j)?),
                    None => None,
                };
                results.insert(
                    label,
                    ModelResult {
                        state: mstate,
                        output,
                        error: crate::error_from_stored(merror),
                    },
                );
            }

            items.push(Item {
                input,
                models,
                pipeline: ipipeline,
                state: istate,
                results,
                error: crate::error_from_stored(ierror),
                retries: iretries as u32,
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
impl Storage for SqliteStorage {
    async fn migrate(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for stmt in SCHEMA {
            sqlx::query(*stmt).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn create_task(&self, task: &Task) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let ts = now();
        sqlx::query(
            "INSERT INTO tasks (id, state, attempts, created_at, updated_at) VALUES (?, ?, 0, ?, ?)",
        )
        .bind(&task.id)
        .bind(task.state.as_str())
        .bind(ts)
        .bind(ts)
        .execute(&mut *tx)
        .await?;

        for (idx, item) in task.items.iter().enumerate() {
            let input_json = serde_json::to_string(&item.input)?;
            let models_json = serde_json::to_string(&item.models)?;
            sqlx::query(
                "INSERT INTO items
                    (task_id, idx, input_json, models_json, pipeline, state, error, webhook_delivered)
                 VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
            )
            .bind(&task.id)
            .bind(idx as i64)
            .bind(&input_json)
            .bind(&models_json)
            .bind(item.pipeline.as_deref())
            .bind(item.state.as_str())
            .bind(crate::error_to_json(item.error.as_ref())?)
            .execute(&mut *tx)
            .await?;

            for (label, mr) in &item.results {
                let output_json = match &mr.output {
                    Some(o) => Some(serde_json::to_string(o)?),
                    None => None,
                };
                sqlx::query(
                    "INSERT INTO model_results
                        (task_id, item_idx, label, state, output_json, error)
                     VALUES (?, ?, ?, ?, ?, ?)",
                )
                .bind(&task.id)
                .bind(idx as i64)
                .bind(label)
                .bind(mr.state.as_str())
                .bind(output_json)
                .bind(crate::error_to_json(mr.error.as_ref())?)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    async fn get_task(&self, id: &str) -> Result<Option<Task>> {
        self.load_task(id).await
    }

    async fn set_task_state(&self, id: &str, state: TaskState) -> Result<()> {
        sqlx::query("UPDATE tasks SET state = ?, updated_at = ? WHERE id = ?")
            .bind(state.as_str())
            .bind(now())
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn set_item_state(
        &self,
        task_id: &str,
        item: usize,
        state: ItemState,
        error: Option<&TaskError>,
    ) -> Result<()> {
        sqlx::query("UPDATE items SET state = ?, error = ? WHERE task_id = ? AND idx = ?")
            .bind(state.as_str())
            .bind(crate::error_to_json(error)?)
            .bind(task_id)
            .bind(item as i64)
            .execute(&self.pool)
            .await?;
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
        sqlx::query(
            "INSERT INTO model_results (task_id, item_idx, label, state, output_json, error)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(task_id, item_idx, label) DO UPDATE SET
                state = excluded.state,
                output_json = excluded.output_json,
                error = excluded.error",
        )
        .bind(task_id)
        .bind(item as i64)
        .bind(label)
        .bind(result.state.as_str())
        .bind(output_json)
        .bind(crate::error_to_json(result.error.as_ref())?)
        .execute(&self.pool)
        .await?;
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
        sqlx::query(
            "INSERT INTO frames (task_id, item_idx, label, frame_index, timestamp, class_json)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(task_id, item_idx, label, frame_index) DO UPDATE SET
                timestamp = excluded.timestamp,
                class_json = excluded.class_json",
        )
        .bind(task_id)
        .bind(item as i64)
        .bind(label)
        .bind(frame.index as i64)
        .bind(frame.timestamp)
        .bind(class_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn load_frames(&self, task_id: &str, item: usize, label: &str) -> Result<Vec<Frame>> {
        let rows = sqlx::query(
            "SELECT frame_index, timestamp, class_json
             FROM frames WHERE task_id = ? AND item_idx = ? AND label = ?
             ORDER BY frame_index",
        )
        .bind(task_id)
        .bind(item as i64)
        .bind(label)
        .fetch_all(&self.pool)
        .await?;

        let mut frames = Vec::with_capacity(rows.len());
        for r in rows {
            let index: i64 = r.try_get("frame_index")?;
            let timestamp: f64 = r.try_get("timestamp")?;
            let class_json: String = r.try_get("class_json")?;
            let classification: Classification = serde_json::from_str(&class_json)?;
            frames.push(Frame {
                timestamp,
                index: index as u32,
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
        // Upsert so a scan that hasn't recorded a result row yet still works.
        sqlx::query(
            "INSERT INTO model_results (task_id, item_idx, label, state, steps_completed)
             VALUES (?, ?, ?, 'processing', ?)
             ON CONFLICT(task_id, item_idx, label) DO UPDATE SET
                steps_completed = excluded.steps_completed",
        )
        .bind(task_id)
        .bind(item as i64)
        .bind(label)
        .bind(steps as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn steps_completed(&self, task_id: &str, item: usize, label: &str) -> Result<u32> {
        let v: Option<i64> = sqlx::query_scalar(
            "SELECT steps_completed FROM model_results
             WHERE task_id = ? AND item_idx = ? AND label = ?",
        )
        .bind(task_id)
        .bind(item as i64)
        .bind(label)
        .fetch_optional(&self.pool)
        .await?;
        Ok(v.unwrap_or(0) as u32)
    }

    async fn load_incomplete_tasks(&self) -> Result<Vec<Task>> {
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM tasks WHERE state IN ('queued', 'processing') ORDER BY created_at",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut tasks = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(t) = self.load_task(&id).await? {
                tasks.push(t);
            }
        }
        Ok(tasks)
    }

    async fn increment_attempts(&self, id: &str) -> Result<u32> {
        let n: i64 = sqlx::query_scalar(
            "UPDATE tasks SET attempts = attempts + 1, updated_at = ? WHERE id = ? RETURNING attempts",
        )
        .bind(now())
        .bind(id)
        .fetch_one(&self.pool)
        .await?;
        Ok(n as u32)
    }

    async fn items_pending_webhook(&self) -> Result<Vec<PendingWebhook>> {
        let rows = sqlx::query(
            "SELECT task_id, idx FROM items
             WHERE webhook_delivered = 0 AND state IN ('completed', 'failed', 'cancelled')",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let task_id: String = r.try_get("task_id")?;
            let idx: i64 = r.try_get("idx")?;
            out.push(PendingWebhook {
                task_id,
                item_index: idx as usize,
            });
        }
        Ok(out)
    }

    async fn mark_webhook_delivered(&self, task_id: &str, item: usize) -> Result<()> {
        sqlx::query("UPDATE items SET webhook_delivered = 1 WHERE task_id = ? AND idx = ?")
            .bind(task_id)
            .bind(item as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn set_item_retries(&self, task_id: &str, item: usize, retries: u32) -> Result<()> {
        sqlx::query("UPDATE items SET retries = ? WHERE task_id = ? AND idx = ?")
            .bind(retries as i64)
            .bind(task_id)
            .bind(item as i64)
            .execute(&self.pool)
            .await?;
        self.touch(task_id).await
    }

    async fn items_pending_failure_webhook(&self) -> Result<Vec<PendingWebhook>> {
        let rows = sqlx::query(
            "SELECT task_id, idx FROM items WHERE failure_delivered = 0 AND state = 'failed'",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let task_id: String = r.try_get("task_id")?;
            let idx: i64 = r.try_get("idx")?;
            out.push(PendingWebhook {
                task_id,
                item_index: idx as usize,
            });
        }
        Ok(out)
    }

    async fn mark_failure_delivered(&self, task_id: &str, item: usize) -> Result<()> {
        sqlx::query("UPDATE items SET failure_delivered = 1 WHERE task_id = ? AND idx = ?")
            .bind(task_id)
            .bind(item as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn purge_finished_before(&self, cutoff_unix_secs: i64) -> Result<u64> {
        let res =
            sqlx::query("DELETE FROM tasks WHERE state IN ('completed', 'failed', 'cancelled') AND updated_at < ?")
                .bind(cutoff_unix_secs)
                .execute(&self.pool)
                .await?;
        Ok(res.rows_affected())
    }

    async fn cache_lookup(
        &self,
        content_hash: &str,
        model: &str,
        revision: &str,
        fresh_after: i64,
    ) -> Result<Option<ModelOutput>> {
        let row = sqlx::query(
            "SELECT output_json FROM cache
             WHERE content_hash = ? AND model = ? AND revision = ? AND created_at >= ?",
        )
        .bind(content_hash)
        .bind(model)
        .bind(revision)
        .bind(fresh_after)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some(r) => {
                let json: String = r.get("output_json");
                Ok(Some(serde_json::from_str(&json)?))
            }
            None => Ok(None),
        }
    }

    async fn cache_store(
        &self,
        content_hash: &str,
        model: &str,
        revision: &str,
        output: &ModelOutput,
    ) -> Result<()> {
        let json = serde_json::to_string(output)?;
        sqlx::query(
            "INSERT INTO cache (content_hash, model, revision, output_json, created_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(content_hash, model, revision) DO UPDATE SET
                output_json = excluded.output_json, created_at = excluded.created_at",
        )
        .bind(content_hash)
        .bind(model)
        .bind(revision)
        .bind(json)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn url_cache_lookup(
        &self,
        url_hash: &str,
        model: &str,
        revision: &str,
        fresh_after: i64,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT content_hash FROM url_cache
             WHERE url_hash = ? AND model = ? AND revision = ? AND created_at >= ?",
        )
        .bind(url_hash)
        .bind(model)
        .bind(revision)
        .bind(fresh_after)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.get::<String, _>("content_hash")))
    }

    async fn url_cache_store(
        &self,
        url_hash: &str,
        model: &str,
        revision: &str,
        content_hash: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO url_cache (url_hash, model, revision, content_hash, created_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(url_hash, model, revision) DO UPDATE SET
                content_hash = excluded.content_hash, created_at = excluded.created_at",
        )
        .bind(url_hash)
        .bind(model)
        .bind(revision)
        .bind(content_hash)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apollo_config::SqliteConfig;
    use apollo_domain::{Classification, Frame, Input, Item, Prediction, Url};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_cfg() -> SqliteConfig {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "apollo-storage-test-{}-{}.db",
            std::process::id(),
            n
        ));
        SqliteConfig {
            path: p.to_string_lossy().into_owned(),
            wal: true,
            busy_timeout: 5000,
            max_connections: 1,
        }
    }

    fn cleanup(cfg: &SqliteConfig) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{}", cfg.path, suffix));
        }
    }

    fn sample_task() -> Task {
        let item = Item {
            input: Input::Image(Url {
                main: "http://x/a.jpg".into(),
                fallback: Some("http://y/a.jpg".into()),
            }),
            models: vec!["general".into(), "nsfw".into()],
            pipeline: None,
            state: ItemState::Queued,
            results: BTreeMap::new(),
            error: None,
            retries: 0,
        };
        Task {
            id: "task-1".into(),
            state: TaskState::Queued,
            items: vec![item],
        }
    }

    #[tokio::test]
    async fn lifecycle_and_resume() {
        let cfg = temp_cfg();
        let store = SqliteStorage::connect(&cfg).await.unwrap();
        store.migrate().await.unwrap();
        store.migrate().await.unwrap(); // idempotent

        store.create_task(&sample_task()).await.unwrap();

        // Round-trips; unstarted models read back as queued; fallback survived.
        let t = store.get_task("task-1").await.unwrap().unwrap();
        assert_eq!(t.state, TaskState::Queued);
        assert_eq!(t.items.len(), 1);
        assert_eq!(t.items[0].results.len(), 2);
        assert_eq!(t.items[0].results["general"].state, ModelState::Queued);
        match &t.items[0].input {
            Input::Image(u) => assert_eq!(u.fallback.as_deref(), Some("http://y/a.jpg")),
            other => panic!("unexpected input: {other:?}"),
        }

        // A completed result with output.
        let out = ModelOutput::Classification(Classification {
            predictions: vec![Prediction {
                label: 7,
                score: 0.99,
            }],
        });
        store
            .upsert_model_result("task-1", 0, "general", &ModelResult::done(out))
            .await
            .unwrap();
        let t = store.get_task("task-1").await.unwrap().unwrap();
        assert_eq!(t.items[0].results["general"].state, ModelState::Done);
        assert!(t.items[0].results["general"].output.is_some());

        // Frame checkpoint + steps marker.
        store
            .set_steps_completed("task-1", 0, "nsfw", 1)
            .await
            .unwrap();
        store
            .append_frame(
                "task-1",
                0,
                "nsfw",
                &Frame {
                    timestamp: 1.5,
                    index: 0,
                    classification: Classification {
                        predictions: vec![Prediction {
                            label: 0,
                            score: 0.8,
                        }],
                    },
                },
            )
            .await
            .unwrap();
        assert_eq!(
            store.load_frames("task-1", 0, "nsfw").await.unwrap().len(),
            1
        );
        assert_eq!(store.steps_completed("task-1", 0, "nsfw").await.unwrap(), 1);

        // Resume sees the in-flight task; attempts increment.
        store
            .set_task_state("task-1", TaskState::Processing)
            .await
            .unwrap();
        assert_eq!(store.load_incomplete_tasks().await.unwrap().len(), 1);
        assert_eq!(store.increment_attempts("task-1").await.unwrap(), 1);
        assert_eq!(store.increment_attempts("task-1").await.unwrap(), 2);

        // Webhook delivery bookkeeping.
        store
            .set_item_state("task-1", 0, ItemState::Completed, None)
            .await
            .unwrap();
        assert_eq!(store.items_pending_webhook().await.unwrap().len(), 1);
        store.mark_webhook_delivered("task-1", 0).await.unwrap();
        assert!(store.items_pending_webhook().await.unwrap().is_empty());

        // Retention purges finished tasks (and cascades).
        store
            .set_task_state("task-1", TaskState::Completed)
            .await
            .unwrap();
        assert_eq!(store.purge_finished_before(now() + 10).await.unwrap(), 1);
        assert!(store.get_task("task-1").await.unwrap().is_none());

        cleanup(&cfg);
    }
}
