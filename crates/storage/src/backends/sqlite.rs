//! SQLite backend (sqlx). Implements [`crate::Storage`].
//!
//! Schema: `tasks` 1—N `items` 1—N `model_results` 1—N `frames`, with
//! `ON DELETE CASCADE` so retention is a single delete from `tasks`. The full set
//! of requested models lives in `items.models_json`; `model_results` rows hold
//! per-model progress and are overlaid onto a queued baseline when reconstructing
//! a task, so a model that hasn't started yet still reads back as `Queued`.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, SqlitePool};

use apollo_config::SqliteConfig;
use apollo_domain::{
    Classification, Item, ItemState, ModelOutput, ModelResult, ModelState, Revocation, Sample,
    Task, TaskError, TaskState,
};

use crate::error::StorageError;
use crate::now;
use crate::resume::PendingWebhook;
use crate::{
    ClaimMode, Claimable, Delivery, Fence, Held, Lease, Renewal, SharedKey, Storage, TaskCursor,
    TaskPage, TaskQuery,
};

type Result<T> = std::result::Result<T, StorageError>;

/// A value bound into a dynamically built query.
enum Bind {
    Text(Option<String>),
    Int(i64),
}

/// Now on the database's clock, in milliseconds (lease times never come from
/// the host).
macro_rules! now_ms {
    () => {
        "CAST(unixepoch('subsec') * 1000 AS INTEGER)"
    };
}

/// A runner's fence on an item row: no fence, or its task's claim is the
/// token. Binds: the token, the task id, the token.
const ITEM_FENCE: &str =
    "(? IS NULL OR EXISTS (SELECT 1 FROM tasks WHERE tasks.id = ? AND tasks.claim = ?))";

/// Which tasks a claim may take: unfinished ones, and cancelled ones with
/// unfinished items. Prefix it with the task table's alias-free columns.
const CLAIMABLE_STATE: &str = "(state IN ('queued', 'processing')
     OR (state = 'cancelled' AND EXISTS (SELECT 1 FROM items
         WHERE items.task_id = tasks.id AND items.state IN ('queued', 'processing', 'retrying'))))";

/// The finished-webhook columns a finishing write sets. Binds: delivered,
/// target, holder, holder, ttl (ms).
const DELIVERY_SET: &str = concat!(
    "webhook_delivered = ?, webhook_target = ?, webhook_claim = ?,
     webhook_claim_until = CASE WHEN ? IS NULL THEN NULL ELSE ",
    now_ms!(),
    " + ? END"
);

/// The delivery-claim condition for `mode`, on an item.
fn delivery_clause(mode: ClaimMode) -> &'static str {
    match mode {
        ClaimMode::Force => "1 = 1",
        ClaimMode::Open => "webhook_claim IS NULL",
        ClaimMode::Expired => concat!(
            "(webhook_claim IS NULL OR webhook_claim_until < ",
            now_ms!(),
            ")"
        ),
    }
}

/// The lease condition for `mode`.
fn lease_clause(mode: ClaimMode) -> &'static str {
    match mode {
        ClaimMode::Force => "1 = 1",
        ClaimMode::Open => "claim IS NULL",
        ClaimMode::Expired => concat!(
            "(claim IS NULL OR (lease_until IS NOT NULL AND lease_until < ",
            now_ms!(),
            "))"
        ),
    }
}

/// Pinned to another node, and that node's lease ended less than the grace
/// ago: not claimable, unless it's cancelled (finishing it reads no input).
/// Binds: this node, the grace in ms.
const PIN_CLAUSE: &str = concat!(
    "(state = 'cancelled' OR pin IS NULL OR pin = ? OR (lease_until IS NOT NULL AND lease_until < ",
    now_ms!(),
    " - ?))"
);

/// What [`Claimable`] reports beyond the row. Binds: this node (twice), the
/// grace in ms.
const CLAIMABLE_FIELDS: &str = concat!(
    "id, state, created_at,
     CASE WHEN state != 'cancelled' AND pin IS NOT NULL AND pin != ? THEN pin END AS abandoned_pin,
     COALESCE(pin = ?, 0) AS pinned_here,
     ((claim IS NULL AND lease_until IS NULL)
        OR (lease_until IS NOT NULL AND lease_until < ",
    now_ms!(),
    " - ?)) AS stale"
);

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
    // Shared auth (used only where instances share the database, which
    // SQLite never is): the public key, and token revocations.
    "CREATE TABLE IF NOT EXISTS auth_key (
        slot       TEXT PRIMARY KEY CHECK (slot = 'current'),
        public_key TEXT NOT NULL,
        created_by TEXT,
        created_at INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS revocations (
        kind       TEXT NOT NULL CHECK (kind IN ('id', 'subject')),
        value      TEXT NOT NULL,
        revoked_at INTEGER NOT NULL,
        PRIMARY KEY (kind, value)
    )",
];

/// SQLite-backed [`Storage`].
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    /// Open (creating if missing) and configure the pool from `[database.sqlite]`.
    pub async fn connect(cfg: &SqliteConfig) -> Result<Self> {
        // With WAL, `synchronous = NORMAL` is the recommended setting: commits
        // skip the per-transaction fsync, the database stays consistent after a
        // crash or power loss, and at worst the last few transactions (frame
        // checkpoints, which resume simply redoes) are lost. Without WAL keep
        // FULL.
        let (journal, synchronous) = if cfg.wal {
            (SqliteJournalMode::Wal, SqliteSynchronous::Normal)
        } else {
            (SqliteJournalMode::Delete, SqliteSynchronous::Full)
        };
        let opts = SqliteConnectOptions::new()
            .filename(&cfg.path)
            .create_if_missing(true)
            .journal_mode(journal)
            .synchronous(synchronous)
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
        let Some(trow) = sqlx::query(
            "SELECT state, owner, metadata_json, request_id, created_at, updated_at, finished_at
             FROM tasks WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let state = trow.try_get::<String, _>("state")?.parse::<TaskState>()?;
        let owner: Option<String> = trow.try_get("owner")?;
        let metadata_json: Option<String> = trow.try_get("metadata_json")?;
        let metadata = match metadata_json {
            Some(j) => serde_json::from_str(&j)?,
            None => Default::default(),
        };

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
            owner,
            metadata,
            request_id: trow.try_get("request_id")?,
            created_at: trow.try_get("created_at")?,
            updated_at: trow.try_get("updated_at")?,
            finished_at: trow.try_get("finished_at")?,
        }))
    }
}

/// Add `column` (with SQL type `decl`) to `table` when an older database lacks
/// it: `CREATE TABLE IF NOT EXISTS` does not extend an existing table.
async fn add_column_if_missing(
    tx: &mut sqlx::SqliteConnection,
    table: &'static str,
    column: &'static str,
    decl: &'static str,
) -> Result<()> {
    let present: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?")
            .bind(table)
            .bind(column)
            .fetch_one(&mut *tx)
            .await?;
    if present == 0 {
        // Built only from the `'static` names passed above, never from input.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE {table} ADD COLUMN {column} {decl}"
        )))
        .execute(&mut *tx)
        .await?;
    }
    Ok(())
}

/// Bind a [`Delivery`] for [`DELIVERY_SET`].
fn bind_delivery<'q>(
    q: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>,
    delivery: Delivery<'_>,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments> {
    let (delivered, target, holder, ttl) = delivery.parts();
    q.bind(delivered)
        .bind(target.map(str::to_string))
        .bind(holder.map(str::to_string))
        .bind(holder.map(str::to_string))
        .bind(ttl.as_millis() as i64)
}

/// Who holds a claim, for operators: `<node>/<process>`.
fn holder(lease: &Lease<'_>) -> String {
    format!("{}/{}", lease.node, lease.process)
}

fn ttl_ms(lease: &Lease<'_>) -> i64 {
    lease.ttl.as_millis() as i64
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn migrate(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for stmt in SCHEMA {
            sqlx::query(*stmt).execute(&mut *tx).await?;
        }
        // Columns added after the first release.
        for (column, decl) in [
            ("owner", "TEXT"),
            ("metadata_json", "TEXT"),
            ("request_id", "TEXT"),
            ("finished_at", "INTEGER"),
            // Leases: the claim token, who holds it, until when (ms on the
            // database clock), and the node a node-local input pins it to.
            ("claim", "TEXT"),
            ("claimed_by", "TEXT"),
            ("lease_until", "INTEGER"),
            ("pin", "TEXT"),
        ] {
            add_column_if_missing(&mut tx, "tasks", column, decl).await?;
        }
        // Finished-webhook delivery claims: the receiver it is for (`NULL` =
        // any), who delivers it, until when (ms on the database clock).
        for (column, decl) in [
            ("webhook_target", "TEXT"),
            ("webhook_claim", "TEXT"),
            ("webhook_claim_until", "INTEGER"),
        ] {
            add_column_if_missing(&mut tx, "items", column, decl).await?;
        }
        for stmt in [
            "CREATE INDEX IF NOT EXISTS idx_tasks_request ON tasks(request_id)",
            "CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at, id)",
        ] {
            sqlx::query(stmt).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn create_task(
        &self,
        task: &Task,
        lease: Option<&Lease<'_>>,
        pin: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let ts = now();
        sqlx::query(concat!(
            "INSERT INTO tasks
                (id, state, attempts, created_at, updated_at, owner, metadata_json, request_id,
                 claim, claimed_by, lease_until, pin)
             VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? IS NULL THEN NULL ELSE ",
            now_ms!(),
            " + ? END, ?)"
        ))
        .bind(&task.id)
        .bind(task.state.as_str())
        .bind(ts)
        .bind(ts)
        .bind(task.owner.as_deref())
        .bind(crate::metadata_to_json(&task.metadata)?)
        .bind(task.request_id.as_deref())
        .bind(lease.map(|l| l.token))
        .bind(lease.map(holder))
        .bind(lease.map(|l| l.token))
        .bind(lease.map_or(0, ttl_ms))
        .bind(pin)
        .execute(&mut *tx)
        .await
        .map_err(|e| match &e {
            sqlx::Error::Database(d) if d.is_unique_violation() => {
                StorageError::TaskExists(task.id.clone())
            }
            _ => e.into(),
        })?;

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

    async fn set_task_state(&self, id: &str, state: TaskState, fence: Fence<'_>) -> Result<bool> {
        let ts = now();
        let finished = crate::is_terminal(state).then_some(ts);
        let res = sqlx::query(
            "UPDATE tasks SET state = ?, updated_at = ?, finished_at = ?
             WHERE id = ? AND state IN ('queued', 'processing') AND (? IS NULL OR claim = ?)",
        )
        .bind(state.as_str())
        .bind(ts)
        .bind(finished)
        .bind(id)
        .bind(fence.token())
        .bind(fence.token())
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn cancel_task_if_active(&self, id: &str) -> Result<bool> {
        let ts = now();
        let res = sqlx::query(
            "UPDATE tasks SET state = ?, updated_at = ?, finished_at = ?
             WHERE id = ? AND state IN ('queued', 'processing')",
        )
        .bind(TaskState::Cancelled.as_str())
        .bind(ts)
        .bind(ts)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn find_task_by_request(
        &self,
        owner: Option<&str>,
        request_id: &str,
    ) -> Result<Option<String>> {
        Ok(sqlx::query_scalar(
            "SELECT id FROM tasks WHERE request_id = ? AND owner IS ?
             ORDER BY created_at LIMIT 1",
        )
        .bind(request_id)
        .bind(owner)
        .fetch_optional(&self.pool)
        .await?)
    }

    async fn list_tasks(&self, query: &TaskQuery) -> Result<TaskPage> {
        let mut sql = String::from("SELECT id, created_at FROM tasks WHERE 1 = 1");
        let mut binds: Vec<Bind> = Vec::new();
        if let Some(owner) = &query.owner {
            sql.push_str(" AND owner IS ?");
            binds.push(Bind::Text(owner.clone()));
        }
        if !query.states.is_empty() {
            let marks = vec!["?"; query.states.len()].join(", ");
            sql.push_str(&format!(" AND state IN ({marks})"));
            binds.extend(
                query
                    .states
                    .iter()
                    .map(|s| Bind::Text(Some(s.as_str().into()))),
            );
        }
        if let Some(t) = query.created_after {
            sql.push_str(" AND created_at >= ?");
            binds.push(Bind::Int(t));
        }
        if let Some(t) = query.created_before {
            sql.push_str(" AND created_at < ?");
            binds.push(Bind::Int(t));
        }
        if let Some(c) = &query.after {
            sql.push_str(" AND (created_at < ? OR (created_at = ? AND id < ?))");
            binds.push(Bind::Int(c.created_at));
            binds.push(Bind::Int(c.created_at));
            binds.push(Bind::Text(Some(c.id.clone())));
        }
        let limit = query.limit.max(1);
        // One extra row tells whether another page follows.
        sql.push_str(&format!(
            " ORDER BY created_at DESC, id DESC LIMIT {}",
            limit + 1
        ));

        // Assembled from fixed fragments and `?` placeholders only; every value
        // is bound.
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql));
        for b in binds {
            q = match b {
                Bind::Text(v) => q.bind(v),
                Bind::Int(v) => q.bind(v),
            };
        }
        let rows = q.fetch_all(&self.pool).await?;
        let more = rows.len() > limit;
        let mut page = TaskPage::default();
        for r in rows.into_iter().take(limit) {
            let id: String = r.try_get("id")?;
            if let Some(task) = self.load_task(&id).await? {
                page.tasks.push(task);
            }
        }
        if more && let Some(last) = page.tasks.last() {
            page.next = Some(TaskCursor {
                created_at: last.created_at,
                id: last.id.clone(),
            });
        }
        Ok(page)
    }

    async fn set_item_state(
        &self,
        task_id: &str,
        item: usize,
        state: ItemState,
        error: Option<&TaskError>,
        fence: Fence<'_>,
        delivery: Delivery<'_>,
    ) -> Result<bool> {
        let finish = crate::item_terminal(state);
        let set = if finish {
            format!(", {DELIVERY_SET}")
        } else {
            String::new()
        };
        let q = sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE items SET state = ?, error = ?{set}
             WHERE task_id = ? AND idx = ? AND state IN ('queued', 'processing', 'retrying')
               AND {ITEM_FENCE}"
        )))
        .bind(state.as_str())
        .bind(crate::error_to_json(error)?);
        let q = if finish {
            bind_delivery(q, delivery)
        } else {
            q
        };
        let res = q
            .bind(task_id)
            .bind(item as i64)
            .bind(fence.token())
            .bind(task_id)
            .bind(fence.token())
            .execute(&self.pool)
            .await?;
        if res.rows_affected() == 0 {
            return Ok(false);
        }
        self.touch(task_id).await?;
        Ok(true)
    }

    async fn set_item_failed(
        &self,
        task_id: &str,
        item: usize,
        error: &TaskError,
        models: &[String],
        fence: Fence<'_>,
        delivery: Delivery<'_>,
    ) -> Result<bool> {
        let error_json = crate::error_to_json(Some(error))?;
        let mut tx = self.pool.begin().await?;
        let q = sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE items SET state = ?, error = ?, {DELIVERY_SET}
             WHERE task_id = ? AND idx = ? AND state IN ('queued', 'processing', 'retrying')
               AND {ITEM_FENCE}"
        )))
        .bind(ItemState::Failed.as_str())
        .bind(&error_json);
        let res = bind_delivery(q, delivery)
            .bind(task_id)
            .bind(item as i64)
            .bind(fence.token())
            .bind(task_id)
            .bind(fence.token())
            .execute(&mut *tx)
            .await?;
        if res.rows_affected() == 0 {
            return Ok(false); // dropping `tx` rolls it back
        }
        for label in models {
            sqlx::query(
                "INSERT INTO model_results (task_id, item_idx, label, state, output_json, error)
                 VALUES (?, ?, ?, ?, NULL, ?)
                 ON CONFLICT(task_id, item_idx, label) DO UPDATE SET
                    state = excluded.state,
                    output_json = excluded.output_json,
                    error = excluded.error",
            )
            .bind(task_id)
            .bind(item as i64)
            .bind(label)
            .bind(ModelState::Failed.as_str())
            .bind(&error_json)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("UPDATE tasks SET updated_at = ? WHERE id = ?")
            .bind(now())
            .bind(task_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }

    async fn upsert_model_result(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        result: &ModelResult,
        fence: Fence<'_>,
    ) -> Result<bool> {
        let output_json = match &result.output {
            Some(o) => Some(serde_json::to_string(o)?),
            None => None,
        };
        // INSERT … SELECT … WHERE, so a fenced-out write inserts nothing.
        let res = sqlx::query(
            "INSERT INTO model_results (task_id, item_idx, label, state, output_json, error)
             SELECT ?, ?, ?, ?, ?, ?
             WHERE ? IS NULL OR EXISTS (SELECT 1 FROM tasks WHERE id = ? AND claim = ?)
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
        .bind(fence.token())
        .bind(task_id)
        .bind(fence.token())
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Ok(false);
        }
        self.touch(task_id).await?;
        Ok(true)
    }

    async fn append_samples(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        samples: &[Sample],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for sample in samples {
            let class_json = serde_json::to_string(&sample.classification)?;
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
            .bind(sample.index as i64)
            .bind(sample.start)
            .bind(class_json)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn load_samples(&self, task_id: &str, item: usize, label: &str) -> Result<Vec<Sample>> {
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

        let mut samples = Vec::with_capacity(rows.len());
        for r in rows {
            let index: i64 = r.try_get("frame_index")?;
            let timestamp: f64 = r.try_get("timestamp")?;
            let class_json: String = r.try_get("class_json")?;
            let classification: Classification = serde_json::from_str(&class_json)?;
            samples.push(Sample::frame(index as u32, timestamp, classification));
        }
        Ok(samples)
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

    async fn count_tasks_by_state(&self) -> Result<Vec<(TaskState, u64)>> {
        let rows = sqlx::query("SELECT state, COUNT(*) AS n FROM tasks GROUP BY state")
            .fetch_all(&self.pool)
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let state = r.try_get::<String, _>("state")?.parse::<TaskState>()?;
            let n: i64 = r.try_get("n")?;
            out.push((state, n as u64));
        }
        Ok(out)
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

    fn shared(&self) -> bool {
        false
    }

    async fn claimable_tasks(
        &self,
        mode: ClaimMode,
        node: &str,
        skip: &[String],
        grace: Duration,
        after: Option<&TaskCursor>,
        limit: usize,
    ) -> Result<Vec<Claimable>> {
        let mut sql = format!(
            "SELECT {CLAIMABLE_FIELDS}
             FROM tasks
             WHERE {CLAIMABLE_STATE} AND {} AND id NOT IN (SELECT value FROM json_each(?))",
            lease_clause(mode)
        );
        let mut binds = vec![
            Bind::Text(Some(node.into())),
            Bind::Text(Some(node.into())),
            Bind::Int(grace.as_millis() as i64),
            Bind::Text(Some(serde_json::to_string(skip)?)),
        ];
        if mode != ClaimMode::Force {
            sql.push_str(" AND ");
            sql.push_str(PIN_CLAUSE);
            binds.push(Bind::Text(Some(node.into())));
            binds.push(Bind::Int(grace.as_millis() as i64));
        }
        if let Some(c) = after {
            sql.push_str(" AND (created_at > ? OR (created_at = ? AND id > ?))");
            binds.push(Bind::Int(c.created_at));
            binds.push(Bind::Int(c.created_at));
            binds.push(Bind::Text(Some(c.id.clone())));
        }
        sql.push_str(&format!(" ORDER BY created_at, id LIMIT {}", limit.max(1)));
        // Assembled from fixed fragments and `?` placeholders only.
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql));
        for b in binds {
            q = match b {
                Bind::Text(v) => q.bind(v),
                Bind::Int(v) => q.bind(v),
            };
        }
        let mut out = Vec::new();
        for r in q.fetch_all(&self.pool).await? {
            out.push(Claimable {
                id: r.try_get("id")?,
                state: r.try_get::<String, _>("state")?.parse()?,
                created_at: r.try_get("created_at")?,
                abandoned_pin: r.try_get("abandoned_pin")?,
                pinned_here: r.try_get("pinned_here")?,
                stale: r.try_get("stale")?,
            });
        }
        Ok(out)
    }

    async fn claim_task(
        &self,
        id: &str,
        lease: &Lease<'_>,
        mode: ClaimMode,
        grace: Duration,
    ) -> Result<Option<u32>> {
        // SET's right-hand sides see the row as it was: `claim` is the one
        // being replaced.
        let mut sql = format!(
            "UPDATE tasks SET attempts = attempts + (CASE WHEN claim IS NOT NULL THEN 1 ELSE 0 END),
                claim = ?, claimed_by = ?, lease_until = {} + ?
             WHERE id = ? AND {CLAIMABLE_STATE} AND {}",
            now_ms!(),
            lease_clause(mode)
        );
        if mode != ClaimMode::Force {
            sql.push_str(" AND ");
            sql.push_str(PIN_CLAUSE);
        }
        sql.push_str(" RETURNING attempts");
        let mut q = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql))
            .bind(lease.token)
            .bind(holder(lease))
            .bind(ttl_ms(lease))
            .bind(id);
        if mode != ClaimMode::Force {
            q = q.bind(lease.node).bind(grace.as_millis() as i64);
        }
        Ok(q.fetch_optional(&self.pool).await?.map(|n| n as u32))
    }

    async fn renew_claims(&self, held: &[Held], ttl: Duration) -> Result<Vec<Renewal>> {
        let mut out = Vec::with_capacity(held.len());
        for h in held {
            let state: Option<String> = sqlx::query_scalar(concat!(
                "UPDATE tasks SET lease_until = ",
                now_ms!(),
                " + ? WHERE id = ? AND claim = ? RETURNING state"
            ))
            .bind(ttl.as_millis() as i64)
            .bind(&h.task_id)
            .bind(&h.token)
            .fetch_optional(&self.pool)
            .await?;
            out.push(match state {
                Some(s) => Renewal::Held(s.parse()?),
                None => Renewal::Lost,
            });
        }
        Ok(out)
    }

    async fn release_claims(&self, held: &[Held]) -> Result<u64> {
        let mut released = 0;
        for h in held {
            let res = sqlx::query(concat!(
                "UPDATE tasks SET claim = NULL, claimed_by = NULL, lease_until = ",
                now_ms!(),
                " WHERE id = ? AND claim = ?"
            ))
            .bind(&h.task_id)
            .bind(&h.token)
            .execute(&self.pool)
            .await?;
            released += res.rows_affected();
        }
        Ok(released)
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

    async fn claim_pending_webhooks(
        &self,
        holder: &str,
        target: Option<&str>,
        mode: ClaimMode,
        ttl: Duration,
        limit: usize,
    ) -> Result<Vec<PendingWebhook>> {
        let open = delivery_clause(mode);
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE items SET webhook_claim = ?, webhook_claim_until = {} + ?
             WHERE rowid IN (SELECT rowid FROM items
                 WHERE webhook_delivered = 0 AND state IN ('completed', 'failed', 'cancelled')
                   AND (? IS NULL OR webhook_target IS NULL OR webhook_target = ?)
                   AND {open}
                 ORDER BY random() LIMIT ?)
             RETURNING task_id, idx",
            now_ms!()
        )))
        .bind(holder)
        .bind(ttl.as_millis() as i64)
        .bind(target)
        .bind(target)
        .bind(limit as i64)
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

    async fn renew_webhook_claims(
        &self,
        holder: &str,
        items: &[PendingWebhook],
        ttl: Duration,
    ) -> Result<Vec<bool>> {
        let mut out = Vec::with_capacity(items.len());
        for p in items {
            let res = sqlx::query(concat!(
                "UPDATE items SET webhook_claim_until = ",
                now_ms!(),
                " + ? WHERE task_id = ? AND idx = ? AND webhook_delivered = 0 AND webhook_claim = ?"
            ))
            .bind(ttl.as_millis() as i64)
            .bind(&p.task_id)
            .bind(p.item_index as i64)
            .bind(holder)
            .execute(&self.pool)
            .await?;
            out.push(res.rows_affected() > 0);
        }
        Ok(out)
    }

    async fn release_webhook_claim(
        &self,
        task_id: &str,
        item: usize,
        holder: &str,
    ) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE items SET webhook_claim = NULL, webhook_claim_until = NULL
             WHERE task_id = ? AND idx = ? AND webhook_delivered = 0 AND webhook_claim = ?",
        )
        .bind(task_id)
        .bind(item as i64)
        .bind(holder)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn mark_webhook_delivered(
        &self,
        task_id: &str,
        item: usize,
        holder: &str,
    ) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE items SET webhook_delivered = 1, webhook_claim = NULL, webhook_claim_until = NULL
             WHERE task_id = ? AND idx = ? AND webhook_delivered = 0 AND webhook_claim = ?",
        )
        .bind(task_id)
        .bind(item as i64)
        .bind(holder)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn set_item_retries(
        &self,
        task_id: &str,
        item: usize,
        retries: u32,
        fence: Fence<'_>,
    ) -> Result<bool> {
        let res = sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE items SET retries = ? WHERE task_id = ? AND idx = ? AND {ITEM_FENCE}"
        )))
        .bind(retries as i64)
        .bind(task_id)
        .bind(item as i64)
        .bind(fence.token())
        .bind(task_id)
        .bind(fence.token())
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Ok(false);
        }
        self.touch(task_id).await?;
        Ok(true)
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

    async fn purge_cache_before(&self, cutoff_unix_secs: i64) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        let a = sqlx::query("DELETE FROM cache WHERE created_at < ?")
            .bind(cutoff_unix_secs)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        let b = sqlx::query("DELETE FROM url_cache WHERE created_at < ?")
            .bind(cutoff_unix_secs)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(a + b)
    }

    async fn auth_key(&self) -> Result<Option<SharedKey>> {
        let row: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT public_key, created_by FROM auth_key WHERE slot = 'current'")
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(public_key, node)| SharedKey { public_key, node }))
    }

    async fn publish_auth_key(&self, public_key: &str, node: &str) -> Result<SharedKey> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO auth_key (slot, public_key, created_by, created_at)
             VALUES ('current', ?, ?, ?) ON CONFLICT(slot) DO NOTHING",
        )
        .bind(public_key)
        .bind(node)
        .bind(now())
        .execute(&mut *tx)
        .await?;
        let (public_key, node): (String, Option<String>) =
            sqlx::query_as("SELECT public_key, created_by FROM auth_key WHERE slot = 'current'")
                .fetch_one(&mut *tx)
                .await?;
        tx.commit().await?;
        Ok(SharedKey { public_key, node })
    }

    async fn replace_auth_key(&self, public_key: &str, node: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO auth_key (slot, public_key, created_by, created_at)
             VALUES ('current', ?, ?, ?)
             ON CONFLICT(slot) DO UPDATE SET public_key = excluded.public_key,
                created_by = excluded.created_by, created_at = excluded.created_at",
        )
        .bind(public_key)
        .bind(node)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn add_revocation(&self, revocation: &Revocation) -> Result<()> {
        sqlx::query(
            "INSERT INTO revocations (kind, value, revoked_at) VALUES (?, ?, ?)
             ON CONFLICT(kind, value) DO UPDATE SET revoked_at = excluded.revoked_at",
        )
        .bind(revocation.kind())
        .bind(revocation.value())
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn remove_revocation(&self, revocation: &Revocation) -> Result<bool> {
        let res = sqlx::query("DELETE FROM revocations WHERE kind = ? AND value = ?")
            .bind(revocation.kind())
            .bind(revocation.value())
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn revocations(&self) -> Result<Vec<Revocation>> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT kind, value FROM revocations ORDER BY kind, value")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows
            .into_iter()
            .filter_map(|(kind, value)| Revocation::from_parts(&kind, &value))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apollo_config::SqliteConfig;
    use apollo_domain::{Classification, Input, Item, Prediction, Sample, Url};
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
        Task::new("task-1", vec![item], None)
    }

    #[tokio::test]
    async fn lifecycle_and_resume() {
        let cfg = temp_cfg();
        let store = SqliteStorage::connect(&cfg).await.unwrap();
        store.migrate().await.unwrap();
        store.migrate().await.unwrap(); // idempotent

        store.create_task(&sample_task(), None, None).await.unwrap();

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
            predictions: vec![Prediction::new(7, "seven", 0.99)],
        });
        store
            .upsert_model_result(
                "task-1",
                0,
                "general",
                &ModelResult::done(out),
                Fence::Unchecked,
            )
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
            .append_samples(
                "task-1",
                0,
                "nsfw",
                &[Sample::frame(
                    0,
                    1.5,
                    Classification {
                        predictions: vec![Prediction::new(0, "normal", 0.8)],
                    },
                )],
            )
            .await
            .unwrap();
        let samples = store.load_samples("task-1", 0, "nsfw").await.unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!((samples[0].start, samples[0].end()), (1.5, 1.5));
        assert_eq!(store.steps_completed("task-1", 0, "nsfw").await.unwrap(), 1);

        // Resume sees the in-flight task; attempts increment.
        store
            .set_task_state("task-1", TaskState::Processing, Fence::Unchecked)
            .await
            .unwrap();
        assert_eq!(store.load_incomplete_tasks().await.unwrap().len(), 1);
        assert_eq!(store.increment_attempts("task-1").await.unwrap(), 1);
        assert_eq!(store.increment_attempts("task-1").await.unwrap(), 2);

        // Webhook delivery bookkeeping.
        let claim = Delivery::Claim {
            holder: "n/p",
            target: "t://x",
            ttl: Duration::from_secs(30),
        };
        store
            .set_item_state(
                "task-1",
                0,
                ItemState::Completed,
                None,
                Fence::Unchecked,
                claim,
            )
            .await
            .unwrap();
        assert_eq!(store.items_pending_webhook().await.unwrap().len(), 1);
        assert!(
            store
                .mark_webhook_delivered("task-1", 0, "n/p")
                .await
                .unwrap()
        );
        assert!(store.items_pending_webhook().await.unwrap().is_empty());

        // Retention purges finished tasks (and cascades).
        store
            .set_task_state("task-1", TaskState::Completed, Fence::Unchecked)
            .await
            .unwrap();
        assert_eq!(store.purge_finished_before(now() + 10).await.unwrap(), 1);
        assert!(store.get_task("task-1").await.unwrap().is_none());

        cleanup(&cfg);
    }

    #[tokio::test]
    async fn new_columns_are_added_to_existing_databases() {
        let cfg = temp_cfg();
        // A database created before the `owner` column existed.
        {
            let store = SqliteStorage::connect(&cfg).await.unwrap();
            sqlx::query(
                "CREATE TABLE tasks (id TEXT PRIMARY KEY, state TEXT NOT NULL,
                 attempts INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL)",
            )
            .execute(&store.pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO tasks VALUES ('old', 'completed', 0, 1, 1)")
                .execute(&store.pool)
                .await
                .unwrap();
            // And items from before delivery claims, one finished and pending.
            sqlx::query(
                "CREATE TABLE items (task_id TEXT NOT NULL, idx INTEGER NOT NULL,
                 input_json TEXT NOT NULL, models_json TEXT NOT NULL, pipeline TEXT,
                 state TEXT NOT NULL, error TEXT,
                 webhook_delivered INTEGER NOT NULL DEFAULT 0,
                 retries INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (task_id, idx))",
            )
            .execute(&store.pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO items VALUES ('old', 0, '{\"Text\":\"x\"}', '[]', NULL, 'completed',
                 NULL, 0, 0)",
            )
            .execute(&store.pool)
            .await
            .unwrap();
        }
        let store = SqliteStorage::connect(&cfg).await.unwrap();
        store.migrate().await.unwrap();
        store.migrate().await.unwrap(); // still idempotent
        let old = store.get_task("old").await.unwrap().unwrap();
        assert_eq!(old.owner, None);
        assert!(old.metadata.is_empty());
        assert_eq!((old.request_id, old.finished_at), (None, None));
        // Its pending event is for any receiver, and nobody's to deliver.
        let ttl = Duration::from_secs(30);
        let claimed = store
            .claim_pending_webhooks("n/p", Some("t://x"), ClaimMode::Open, ttl, 1000)
            .await
            .unwrap();
        assert_eq!(
            claimed,
            [PendingWebhook {
                task_id: "old".into(),
                item_index: 0
            }]
        );

        let mut t = sample_task();
        t.owner = Some("alice".into());
        store.create_task(&t, None, None).await.unwrap();
        let t = store.get_task("task-1").await.unwrap().unwrap();
        assert_eq!(t.owner.as_deref(), Some("alice"));
        cleanup(&cfg);
    }

    #[tokio::test]
    async fn cancel_only_applies_to_active_tasks() {
        let cfg = temp_cfg();
        let store = SqliteStorage::connect(&cfg).await.unwrap();
        store.migrate().await.unwrap();
        store.create_task(&sample_task(), None, None).await.unwrap();

        assert!(store.cancel_task_if_active("task-1").await.unwrap());
        let t = store.get_task("task-1").await.unwrap().unwrap();
        assert_eq!(t.state, TaskState::Cancelled);

        // A finished task is never flipped to cancelled.
        let mut done = sample_task();
        done.id = "task-2".into();
        store.create_task(&done, None, None).await.unwrap();
        assert!(
            store
                .set_task_state("task-2", TaskState::Completed, Fence::Unchecked)
                .await
                .unwrap()
        );
        assert!(!store.cancel_task_if_active("task-2").await.unwrap());
        let t = store.get_task("task-2").await.unwrap().unwrap();
        assert_eq!(t.state, TaskState::Completed);
        assert!(!store.cancel_task_if_active("missing").await.unwrap());
        // Nor is a cancelled one finished otherwise.
        assert!(
            !store
                .set_task_state("task-1", TaskState::Completed, Fence::Unchecked)
                .await
                .unwrap()
        );
        let t = store.get_task("task-1").await.unwrap().unwrap();
        assert_eq!(t.state, TaskState::Cancelled);

        cleanup(&cfg);
    }

    #[tokio::test]
    async fn only_a_claim_never_handed_back_counts_an_attempt() {
        let cfg = temp_cfg();
        let store = SqliteStorage::connect(&cfg).await.unwrap();
        store.migrate().await.unwrap();
        let lease = |token| crate::Lease {
            token,
            node: "n",
            process: "p",
            ttl: std::time::Duration::from_secs(60),
        };
        store
            .create_task(&sample_task(), Some(&lease("first")), None)
            .await
            .unwrap();
        let claim = |token| {
            let store = &store;
            async move {
                store
                    .claim_task(
                        "task-1",
                        &lease(token),
                        crate::ClaimMode::Force,
                        std::time::Duration::ZERO,
                    )
                    .await
                    .unwrap()
            }
        };

        // The first run crashed: its claim was never handed back.
        assert_eq!(claim("second").await, Some(1));
        // A graceful stop hands it back: the next claim counts nothing.
        let held = crate::Held {
            task_id: "task-1".into(),
            token: "second".into(),
        };
        assert_eq!(store.release_claims(&[held]).await.unwrap(), 1);
        assert_eq!(claim("third").await, Some(1));

        // A finished task isn't claimed.
        store
            .set_task_state("task-1", TaskState::Completed, Fence::Unchecked)
            .await
            .unwrap();
        assert_eq!(claim("fourth").await, None);

        cleanup(&cfg);
    }

    #[tokio::test]
    async fn cache_purge_drops_only_stale_entries() {
        let cfg = temp_cfg();
        let store = SqliteStorage::connect(&cfg).await.unwrap();
        store.migrate().await.unwrap();
        let out = ModelOutput::Classification(Classification::default());
        store.cache_store("h", "m", "main", &out).await.unwrap();
        store.url_cache_store("u", "m", "main", "h").await.unwrap();

        // Nothing is older than an hour ago.
        assert_eq!(store.purge_cache_before(now() - 3600).await.unwrap(), 0);
        assert!(
            store
                .cache_lookup("h", "m", "main", 0)
                .await
                .unwrap()
                .is_some()
        );

        assert_eq!(store.purge_cache_before(now() + 10).await.unwrap(), 2);
        assert!(
            store
                .cache_lookup("h", "m", "main", 0)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .url_cache_lookup("u", "m", "main", 0)
                .await
                .unwrap()
                .is_none()
        );

        cleanup(&cfg);
    }
}
