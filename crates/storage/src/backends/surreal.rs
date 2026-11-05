//! SurrealDB backend, targeting the `surrealdb` Rust SDK 4.0 (a remote
//! server reached over `ws(s)://` or `http(s)://`). Implements [`crate::Storage`].
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
//! Upgrade notes: the dependency is `4.0` (any 4.x release from 4.0.0 on). The
//! whole SurrealDB surface used here is confined to this file —
//! `engine::any::connect`, `Root` auth, `use_ns`/`use_db`, and
//! `query`/`bind`/`take` — plus the SurrealQL in the `const` statements below
//! (and, for tests, the throwaway databases in `testing.rs` and
//! `tests/backends.rs`). Moving to another major version means bumping the
//! version and revisiting only those.

use std::time::Duration;

use async_trait::async_trait;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;
use surrealdb::types::SurrealValue;

use apollo_config::SurrealdbConfig;
use apollo_domain::{
    Classification, Input, Item, ItemState, ModelOutput, ModelResult, ModelState, Revocation,
    Sample, Task, TaskError, TaskState,
};

use crate::error::StorageError;
use crate::now;
use crate::resume::PendingWebhook;
use crate::{
    ClaimMode, Claimable, Delivery, Fence, Held, Lease, Renewal, SharedKey, Storage, TaskCursor,
    TaskPage, TaskQuery,
};

/// Who holds a claim, for operators: `<node>/<process>`.
fn holder(lease: &Lease<'_>) -> String {
    format!("{}/{}", lease.node, lease.process)
}

type Result<T> = std::result::Result<T, StorageError>;

// Convert SurrealDB errors at the crate boundary so `error.rs` stays free of the
// SurrealDB types (and `?` works throughout this module).
impl From<surrealdb::Error> for StorageError {
    fn from(e: surrealdb::Error) -> Self {
        StorageError::Surreal(e.to_string())
    }
}

/// The shared auth tables: the public key (one record, `auth_key:current`)
/// and the token revocations (`revocation:[kind, value]`).
macro_rules! auth_schema {
    () => {
        "
    DEFINE TABLE IF NOT EXISTS auth_key SCHEMALESS;
    DEFINE TABLE IF NOT EXISTS revocation SCHEMALESS;"
    };
}

/// Idempotent schema: schemaless tables plus the indexes the queries rely on.
/// `IF NOT EXISTS` makes re-running a no-op.
const SCHEMA: &str = concat!(
    auth_schema!(),
    "
    DEFINE TABLE IF NOT EXISTS task SCHEMALESS;
    DEFINE TABLE IF NOT EXISTS item SCHEMALESS;
    DEFINE TABLE IF NOT EXISTS model_result SCHEMALESS;
    DEFINE TABLE IF NOT EXISTS frame SCHEMALESS;
    DEFINE TABLE IF NOT EXISTS cache SCHEMALESS;
    DEFINE TABLE IF NOT EXISTS url_cache SCHEMALESS;
    DEFINE INDEX IF NOT EXISTS task_state ON TABLE task FIELDS state;
    DEFINE INDEX IF NOT EXISTS task_created ON TABLE task FIELDS created_at;
    DEFINE INDEX IF NOT EXISTS task_request ON TABLE task FIELDS request_id;
    DEFINE INDEX IF NOT EXISTS item_task ON TABLE item FIELDS task_id;
    DEFINE INDEX IF NOT EXISTS item_webhook ON TABLE item FIELDS webhook_delivered, state;
    DEFINE INDEX IF NOT EXISTS mr_item ON TABLE model_result FIELDS task_id, item_idx;
    DEFINE INDEX IF NOT EXISTS frame_key ON TABLE frame FIELDS task_id, item_idx, label;
"
);

/// Run `op` until it isn't refused with a write conflict (instances doing the
/// same at the same moment), a little later each time; at most 8 more tries.
async fn retry_conflicts<T, F, Fut>(mut op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, surrealdb::Error>>,
{
    let mut tries = 0u64;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if tries < 8 && e.to_string().contains("conflict") => {
                tries += 1;
                tokio::time::sleep(Duration::from_millis(20 * tries + jitter_ms())).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// A few milliseconds (0-19), so writers retrying in lockstep drift apart.
fn jitter_ms() -> u64 {
    u64::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos()
            % 20,
    )
}

/// A runner's fence: no fence, or the task's claim is still the token.
///
/// A fenced write is one statement (an `IF` block), which SurrealDB runs as one
/// transaction and re-runs itself on a write conflict. It also writes the task
/// row (its `updated_at`), so it conflicts with a takeover's claim of that row:
/// a stale runner's write and the new claim can't both commit.
macro_rules! held {
    () => {
        "$fence = NONE OR (SELECT VALUE claim FROM ONLY type::record('task', $tid)) = $fence"
    };
}

/// Set an unfinished item's state while the fence holds; returns whether it
/// did.
const SET_ITEM_STATE: &str = concat!(
    "IF ",
    held!(),
    " {
        LET $won = (UPDATE type::record('item', [$tid, $idx]) SET state = $state, error = $error
            WHERE state IN ['queued', 'processing', 'retrying'] RETURN VALUE state);
        IF array::len($won) > 0 { UPDATE type::record('task', $tid) SET updated_at = $ts; };
        array::len($won) > 0
    } ELSE { false }"
);

/// The finished-webhook fields a finishing write sets (see
/// [`Delivery`](crate::Delivery)). Binds: `$delivered`, `$target`, `$holder`,
/// `$ttl`.
macro_rules! delivery_fields {
    () => {
        "webhook_delivered = $delivered, webhook_target = $target, webhook_claim = $holder,
            webhook_claim_until = IF $holder = NONE THEN NONE ELSE time::now() + $ttl END"
    };
}

/// Finish an unfinished item while the fence holds, recording its finished
/// webhook; returns whether it did.
const FINISH_ITEM_STATE: &str = concat!(
    "IF ",
    held!(),
    " {
        LET $won = (UPDATE type::record('item', [$tid, $idx]) SET state = $state, error = $error, ",
    delivery_fields!(),
    "
            WHERE state IN ['queued', 'processing', 'retrying'] RETURN VALUE state);
        IF array::len($won) > 0 { UPDATE type::record('task', $tid) SET updated_at = $ts; };
        array::len($won) > 0
    } ELSE { false }"
);

/// Fail an unfinished item, and the listed models with it, while the fence
/// holds; returns whether it did.
const FAIL_ITEM: &str = concat!(
    "IF ",
    held!(),
    " {
        LET $won = (UPDATE type::record('item', [$tid, $idx]) SET state = $state, error = $error, ",
    delivery_fields!(),
    "
            WHERE state IN ['queued', 'processing', 'retrying'] RETURN VALUE state);
        IF array::len($won) > 0 {
            FOR $label IN $models {
                UPSERT type::record('model_result', [$tid, $idx, $label]) SET
                    task_id = $tid, item_idx = $idx, label = $label,
                    state = $mstate, output_json = NONE, error = $error;
            };
            UPDATE type::record('task', $tid) SET updated_at = $ts;
        };
        array::len($won) > 0
    } ELSE { false }"
);

/// Insert or update a model result while the fence holds. UPSERT only touches
/// the listed fields, so the `steps_completed` marker and the frame rows are
/// left intact.
const UPSERT_RESULT: &str = concat!(
    "IF ",
    held!(),
    " {
        UPSERT type::record('model_result', [$tid, $idx, $label]) SET
            task_id = $tid, item_idx = $idx, label = $label,
            state = $state, output_json = $output, error = $error;
        UPDATE type::record('task', $tid) SET updated_at = $ts;
        true
    } ELSE { false }"
);

/// Set an item's retry count while the fence holds.
const SET_RETRIES: &str = concat!(
    "IF ",
    held!(),
    " {
        LET $set = (UPDATE type::record('item', [$tid, $idx]) SET retries = $retries RETURN VALUE retries);
        IF array::len($set) > 0 { UPDATE type::record('task', $tid) SET updated_at = $ts; };
        array::len($set) > 0
    } ELSE { false }"
);

/// Claimable states: unfinished, or cancelled with unfinished items (`$tid`'s,
/// for a claim of one task).
const CLAIMABLE_STATE: &str = "(state IN ['queued', 'processing']
    OR (state = 'cancelled' AND array::len((SELECT VALUE idx FROM item
        WHERE task_id = $tid AND state IN ['queued', 'processing', 'retrying'] LIMIT 1)) > 0))";

/// Pinned to another node, and that node's lease ended less than `$grace`
/// ago: not claimable, unless it's cancelled (finishing it reads no input).
const PIN_OPEN: &str = "(state = 'cancelled' OR pin = NONE OR pin = $node
    OR (lease_until != NONE AND lease_until < time::now() - $grace))";

/// The delivery-claim condition for `mode`, on an item. A missing
/// `webhook_claim_until` is guarded explicitly: `NONE < time::now()` is true.
fn delivery_open(mode: ClaimMode) -> &'static str {
    match mode {
        ClaimMode::Force => "true",
        ClaimMode::Open => "webhook_claim = NONE",
        ClaimMode::Expired => {
            "(webhook_claim = NONE
              OR (webhook_claim_until != NONE AND webhook_claim_until < time::now()))"
        }
    }
}

/// The bound values of a [`Delivery`]: `$delivered`, `$target`, `$holder`,
/// `$ttl`.
fn bind_delivery<'q>(
    q: surrealdb::method::Query<'q, Any>,
    delivery: Delivery<'_>,
) -> surrealdb::method::Query<'q, Any> {
    let (delivered, target, holder, ttl) = delivery.parts();
    q.bind(("delivered", delivered))
        .bind(("target", target.map(str::to_string)))
        .bind(("holder", holder.map(str::to_string)))
        .bind(("ttl", ttl))
}

/// The lease condition for `mode`. A missing `lease_until` is guarded
/// explicitly: `NONE < time::now()` is true.
fn lease_open(mode: ClaimMode) -> &'static str {
    match mode {
        ClaimMode::Force => "true",
        ClaimMode::Open => "claim = NONE",
        ClaimMode::Expired => {
            "(claim = NONE OR (lease_until != NONE AND lease_until < time::now()))"
        }
    }
}

/// Persist a task, its items, and any pre-existing model results in one
/// transaction. `FOR` loops keep the bound parameter set fixed regardless of how
/// many items the task has.
const CREATE_TASK: &str = "
    BEGIN TRANSACTION;
    CREATE type::record('task', $tid) SET
        key = $tid, state = $tstate, attempts = 0, created_at = $ts, updated_at = $ts,
        owner = $owner, metadata_json = $metadata, request_id = $rid,
        claim = $claim, claimed_by = $holder, pin = $pin,
        lease_until = IF $claim = NONE THEN NONE ELSE time::now() + $ttl END;
    FOR $it IN $items {
        CREATE type::record('item', [$tid, $it.idx]) SET
            task_id = $tid, idx = $it.idx, input_json = $it.input_json,
            models_json = $it.models_json, pipeline = $it.pipeline, state = $it.state, error = $it.error,
            webhook_delivered = false, retries = 0;
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
// Deserialized from query results / serialized for binds. These implement
// `SurrealValue` (via derive); complex fields are opaque JSON strings.

#[derive(SurrealValue)]
struct AuthKeyRow {
    public_key: String,
    created_by: Option<String>,
}

#[derive(SurrealValue)]
struct RevocationRow {
    kind: String,
    value: String,
}

#[derive(SurrealValue)]
struct TaskStateRow {
    state: String,
    owner: Option<String>,
    metadata_json: Option<String>,
    request_id: Option<String>,
    created_at: Option<i64>,
    updated_at: Option<i64>,
    finished_at: Option<i64>,
}

#[derive(SurrealValue)]
struct TaskKeyRow {
    key: String,
    created_at: i64,
}

#[derive(SurrealValue)]
struct ItemRow {
    idx: i64,
    input_json: String,
    models_json: String,
    pipeline: Option<String>,
    state: String,
    error: Option<String>,
    retries: i64,
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
struct ClaimableRow {
    key: String,
    state: String,
    created_at: i64,
    abandoned_pin: Option<String>,
    pinned_here: bool,
    stale: bool,
}

#[derive(SurrealValue)]
struct PendingRow {
    task_id: String,
    idx: i64,
}

#[derive(Clone, SurrealValue)]
struct ItemInsert {
    idx: i64,
    input_json: String,
    models_json: String,
    pipeline: Option<String>,
    state: String,
    error: Option<String>,
}

#[derive(SurrealValue)]
struct StateCountRow {
    state: String,
    n: i64,
}

#[derive(SurrealValue)]
struct SampleInsert {
    frame_index: i64,
    timestamp: f64,
    class_json: String,
}

#[derive(Clone, SurrealValue)]
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
    /// Define the shared auth tables only (for the CLI, which runs no full
    /// migration).
    pub async fn define_auth_tables(&self) -> Result<()> {
        retry_conflicts(|| async { self.db.query(auth_schema!()).await.and_then(|r| r.check()) })
            .await?;
        Ok(())
    }

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

    /// Shared reconstruction used by `get_task` and `load_incomplete_tasks`.
    /// Every requested model starts `Queued`; stored `model_result` rows overlay
    /// that baseline, so a model that hasn't started still reads back as queued.
    async fn load_task(&self, id: &str) -> Result<Option<Task>> {
        let mut resp = self
            .db
            .query(
                "SELECT state, owner, metadata_json, request_id, created_at, updated_at, finished_at
                 FROM type::record('task', $tid)",
            )
            .bind(("tid", id.to_string()))
            .await?;
        let trows: Vec<TaskStateRow> = resp.take(0)?;
        let Some(trow) = trows.into_iter().next() else {
            return Ok(None);
        };
        let state = trow.state.parse::<TaskState>()?;
        let owner = trow.owner;
        let metadata = match trow.metadata_json {
            Some(j) => serde_json::from_str(&j)?,
            None => Default::default(),
        };

        let mut resp = self
            .db
            .query(
                "SELECT idx, input_json, models_json, pipeline, state, error, retries
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
                        state: mr.state.parse::<ModelState>()?,
                        output,
                        error: crate::error_from_stored(mr.error),
                    },
                );
            }

            items.push(Item {
                input,
                models,
                pipeline: ir.pipeline,
                state: ir.state.parse::<ItemState>()?,
                results,
                error: crate::error_from_stored(ir.error),
                retries: ir.retries as u32,
            });
        }

        Ok(Some(Task {
            id: id.to_string(),
            state,
            items,
            owner,
            metadata,
            request_id: trow.request_id,
            created_at: trow.created_at.unwrap_or(0),
            updated_at: trow.updated_at.unwrap_or(0),
            finished_at: trow.finished_at,
        }))
    }
}

#[async_trait]
impl Storage for SurrealStorage {
    async fn migrate(&self) -> Result<()> {
        // Instances starting together on a new database define the schema at
        // the same moment, and SurrealDB refuses some of them with a write
        // conflict: try again, a little later.
        retry_conflicts(|| async { self.db.query(SCHEMA).await.and_then(|r| r.check()) }).await?;
        Ok(())
    }

    async fn create_task(
        &self,
        task: &Task,
        lease: Option<&Lease<'_>>,
        pin: Option<&str>,
    ) -> Result<()> {
        let ts = now();
        let mut items = Vec::with_capacity(task.items.len());
        let mut mrs = Vec::new();
        for (idx, item) in task.items.iter().enumerate() {
            items.push(ItemInsert {
                idx: idx as i64,
                input_json: serde_json::to_string(&item.input)?,
                models_json: serde_json::to_string(&item.models)?,
                pipeline: item.pipeline.clone(),
                state: item.state.as_str().to_string(),
                error: crate::error_to_json(item.error.as_ref())?,
            });
            for (label, mr) in &item.results {
                mrs.push(ModelInsert {
                    item_idx: idx as i64,
                    label: label.clone(),
                    state: mr.state.as_str().to_string(),
                    output_json: match &mr.output {
                        Some(o) => Some(serde_json::to_string(o)?),
                        None => None,
                    },
                    error: crate::error_to_json(mr.error.as_ref())?,
                });
            }
        }

        let metadata = crate::metadata_to_json(&task.metadata)?;
        let mut tries = 0;
        loop {
            let created = self
                .db
                .query(CREATE_TASK)
                .bind(("tid", task.id.clone()))
                .bind(("tstate", task.state.as_str().to_string()))
                .bind(("owner", task.owner.clone()))
                .bind(("metadata", metadata.clone()))
                .bind(("rid", task.request_id.clone()))
                .bind(("ts", ts))
                .bind(("items", items.clone()))
                .bind(("mrs", mrs.clone()))
                .bind(("claim", lease.map(|l| l.token.to_string())))
                .bind(("holder", lease.map(holder)))
                .bind(("ttl", lease.map_or(Duration::ZERO, |l| l.ttl)))
                .bind(("pin", pin.map(str::to_string)))
                .await
                .and_then(|resp| resp.check());
            let Err(e) = created else {
                return Ok(());
            };
            // Another writer may have stored this id first: reported as
            // "already exists", or, when the two writes overlapped, as a
            // failed commit. Either way it rolled back; look.
            let mut resp = self
                .db
                .query("SELECT VALUE key FROM ONLY type::record('task', $tid)")
                .bind(("tid", task.id.clone()))
                .await?;
            let stored: Option<String> = resp.take(0)?;
            if stored.is_some() {
                return Err(StorageError::TaskExists(task.id.clone()));
            }
            // Not stored: a conflict with some other write; try again, a
            // little later each time (writers racing in lockstep drift apart).
            tries += 1;
            if tries == 6 {
                return Err(e.into());
            }
            let jitter = u64::from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos()
                    % 20,
            );
            tokio::time::sleep(Duration::from_millis(20 * tries + jitter)).await;
        }
    }

    async fn get_task(&self, id: &str) -> Result<Option<Task>> {
        self.load_task(id).await
    }

    async fn set_task_state(&self, id: &str, state: TaskState, fence: Fence<'_>) -> Result<bool> {
        // Conditional on the current state, like `cancel_task_if_active`, and
        // on the claim.
        let query = if crate::is_terminal(state) {
            "UPDATE type::record('task', $tid) SET state = $state, updated_at = $ts, finished_at = $ts
             WHERE state IN ['queued', 'processing'] AND ($fence = NONE OR claim = $fence)
             RETURN VALUE state"
        } else {
            "UPDATE type::record('task', $tid) SET state = $state, updated_at = $ts
             WHERE state IN ['queued', 'processing'] AND ($fence = NONE OR claim = $fence)
             RETURN VALUE state"
        };
        let mut resp = self
            .db
            .query(query)
            .bind(("tid", id.to_string()))
            .bind(("state", state.as_str().to_string()))
            .bind(("ts", now()))
            .bind(("fence", fence.token().map(str::to_string)))
            .await?;
        let vals: Vec<String> = resp.take(0)?;
        Ok(!vals.is_empty())
    }

    async fn cancel_task_if_active(&self, id: &str) -> Result<bool> {
        // Conditional on the current state, so a task that has just finished is
        // never overwritten; RETURN VALUE yields a row only when it matched.
        let mut resp = self
            .db
            .query(
                "UPDATE type::record('task', $tid)
                 SET state = $state, updated_at = $ts, finished_at = $ts
                 WHERE state IN ['queued', 'processing']
                 RETURN VALUE state",
            )
            .bind(("tid", id.to_string()))
            .bind(("state", TaskState::Cancelled.as_str().to_string()))
            .bind(("ts", now()))
            .await?;
        let vals: Vec<String> = resp.take(0)?;
        Ok(!vals.is_empty())
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
        let q = self
            .db
            .query(if finish {
                FINISH_ITEM_STATE
            } else {
                SET_ITEM_STATE
            })
            .bind(("tid", task_id.to_string()))
            .bind(("idx", item as i64))
            .bind(("state", state.as_str().to_string()))
            .bind(("error", crate::error_to_json(error)?))
            .bind(("fence", fence.token().map(str::to_string)))
            .bind(("ts", now()));
        let q = if finish {
            bind_delivery(q, delivery)
        } else {
            q
        };
        let mut resp = q.await?;
        let written: Option<bool> = resp.take(0)?;
        Ok(written == Some(true))
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
        let q = self
            .db
            .query(FAIL_ITEM)
            .bind(("tid", task_id.to_string()))
            .bind(("idx", item as i64))
            .bind(("state", ItemState::Failed.as_str().to_string()))
            .bind(("mstate", ModelState::Failed.as_str().to_string()))
            .bind(("error", crate::error_to_json(Some(error))?))
            .bind(("models", models.to_vec()))
            .bind(("fence", fence.token().map(str::to_string)))
            .bind(("ts", now()));
        let mut resp = bind_delivery(q, delivery).await?;
        let written: Option<bool> = resp.take(0)?;
        Ok(written == Some(true))
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
        let mut resp = self
            .db
            .query(UPSERT_RESULT)
            .bind(("tid", task_id.to_string()))
            .bind(("idx", item as i64))
            .bind(("label", label.to_string()))
            .bind(("state", result.state.as_str().to_string()))
            .bind(("output", output_json))
            .bind(("error", crate::error_to_json(result.error.as_ref())?))
            .bind(("fence", fence.token().map(str::to_string)))
            .bind(("ts", now()))
            .await?;
        let written: Option<bool> = resp.take(0)?;
        Ok(written == Some(true))
    }

    async fn append_samples(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        samples: &[Sample],
    ) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        let mut rows = Vec::with_capacity(samples.len());
        for sample in samples {
            rows.push(SampleInsert {
                frame_index: sample.index as i64,
                timestamp: sample.start,
                class_json: serde_json::to_string(&sample.classification)?,
            });
        }
        // One round trip and one transaction for the whole batch.
        self.db
            .query(
                "BEGIN TRANSACTION;
                 FOR $f IN $frames {
                     UPSERT type::record('frame', [$tid, $idx, $label, $f.frame_index]) SET
                         task_id = $tid, item_idx = $idx, label = $label,
                         frame_index = $f.frame_index, timestamp = $f.timestamp,
                         class_json = $f.class_json;
                 };
                 COMMIT TRANSACTION;",
            )
            .bind(("tid", task_id.to_string()))
            .bind(("idx", item as i64))
            .bind(("label", label.to_string()))
            .bind(("frames", rows))
            .await?
            .check()?;
        Ok(())
    }

    async fn load_samples(&self, task_id: &str, item: usize, label: &str) -> Result<Vec<Sample>> {
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

        let mut samples = Vec::with_capacity(rows.len());
        for r in rows {
            let classification: Classification = serde_json::from_str(&r.class_json)?;
            samples.push(Sample::frame(
                r.frame_index as u32,
                r.timestamp,
                classification,
            ));
        }
        Ok(samples)
    }

    async fn find_task_by_request(
        &self,
        owner: Option<&str>,
        request_id: &str,
    ) -> Result<Option<String>> {
        let mut resp = self
            .db
            .query(
                "SELECT VALUE key FROM task WHERE request_id = $rid AND owner = $owner
                 ORDER BY created_at LIMIT 1",
            )
            .bind(("rid", request_id.to_string()))
            .bind(("owner", owner.map(str::to_string)))
            .await?;
        let ids: Vec<String> = resp.take(0)?;
        Ok(ids.into_iter().next())
    }

    async fn list_tasks(&self, query: &TaskQuery) -> Result<TaskPage> {
        let mut conditions = Vec::new();
        if query.owner.is_some() {
            conditions.push("owner = $owner");
        }
        if !query.states.is_empty() {
            conditions.push("state IN $states");
        }
        if query.created_after.is_some() {
            conditions.push("created_at >= $after_ts");
        }
        if query.created_before.is_some() {
            conditions.push("created_at < $before_ts");
        }
        if query.after.is_some() {
            conditions.push("(created_at < $cur_ts OR (created_at = $cur_ts AND key < $cur_id))");
        }
        let filter = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };
        let limit = query.limit.max(1);
        // One extra row tells whether another page follows.
        let sql = format!(
            "SELECT key, created_at FROM task{filter}
             ORDER BY created_at DESC, key DESC LIMIT {}",
            limit + 1
        );
        let states: Vec<String> = query
            .states
            .iter()
            .map(|s| s.as_str().to_string())
            .collect();
        let (cur_ts, cur_id) = query
            .after
            .as_ref()
            .map_or((0, String::new()), |c| (c.created_at, c.id.clone()));
        let mut resp = self
            .db
            .query(sql)
            .bind(("owner", query.owner.clone().flatten()))
            .bind(("states", states))
            .bind(("after_ts", query.created_after.unwrap_or(0)))
            .bind(("before_ts", query.created_before.unwrap_or(0)))
            .bind(("cur_ts", cur_ts))
            .bind(("cur_id", cur_id))
            .await?;
        let rows: Vec<TaskKeyRow> = resp.take(0)?;
        let more = rows.len() > limit;
        let mut page = TaskPage::default();
        let mut last = None;
        for row in rows.into_iter().take(limit) {
            if let Some(task) = self.load_task(&row.key).await? {
                page.tasks.push(task);
            }
            last = Some(TaskCursor {
                created_at: row.created_at,
                id: row.key,
            });
        }
        if more {
            page.next = last;
        }
        Ok(page)
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
            .query("SELECT steps_completed FROM type::record('model_result', [$tid, $idx, $label])")
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

    async fn count_tasks_by_state(&self) -> Result<Vec<(TaskState, u64)>> {
        let mut resp = self
            .db
            .query("SELECT state, count() AS n FROM task GROUP BY state")
            .await?;
        let rows: Vec<StateCountRow> = resp.take(0)?;
        rows.into_iter()
            .map(|r| Ok((r.state.parse::<TaskState>()?, r.n.max(0) as u64)))
            .collect()
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

    fn shared(&self) -> bool {
        true
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
        let pin = if mode == ClaimMode::Force {
            "true"
        } else {
            PIN_OPEN
        };
        let cursor = if after.is_some() {
            " AND (created_at > $cur_ts OR (created_at = $cur_ts AND key > $cur_id))"
        } else {
            ""
        };
        let filter = format!(
            "{} AND {pin} AND key NOTINSIDE $skip{cursor}",
            lease_open(mode)
        );
        let fields = "key, state, created_at,
            (IF state != 'cancelled' AND pin != NONE AND pin != $node THEN pin ELSE NONE END)
                AS abandoned_pin,
            (pin = $node) AS pinned_here,
            ((claim = NONE AND lease_until = NONE)
                OR (lease_until != NONE AND lease_until < time::now() - $grace)) AS stale";
        // Unfinished tasks, then cancelled ones with unfinished items (found
        // through those items, not by scanning every cancelled task kept).
        let sql = format!(
            "SELECT {fields} FROM task WHERE state IN ['queued', 'processing'] AND {filter}
                ORDER BY created_at, key LIMIT $limit;
             LET $open = array::distinct((SELECT VALUE task_id FROM item
                WHERE state IN ['queued', 'processing', 'retrying']));
             SELECT {fields} FROM task WHERE state = 'cancelled' AND key INSIDE $open AND {filter}
                ORDER BY created_at, key LIMIT $limit;"
        );
        let (cur_ts, cur_id) = after.map_or((0, String::new()), |c| (c.created_at, c.id.clone()));
        let mut resp = self
            .db
            .query(sql)
            .bind(("node", node.to_string()))
            .bind(("skip", skip.to_vec()))
            .bind(("grace", grace))
            .bind(("cur_ts", cur_ts))
            .bind(("cur_id", cur_id))
            .bind(("limit", limit.max(1) as i64))
            .await?;
        let mut rows: Vec<ClaimableRow> = resp.take(0)?;
        let cancelled: Vec<ClaimableRow> = resp.take(2)?;
        rows.extend(cancelled);
        rows.sort_by(|a, b| (a.created_at, &a.key).cmp(&(b.created_at, &b.key)));
        rows.truncate(limit.max(1));
        rows.into_iter()
            .map(|r| {
                Ok(Claimable {
                    id: r.key,
                    state: r.state.parse()?,
                    created_at: r.created_at,
                    abandoned_pin: r.abandoned_pin,
                    pinned_here: r.pinned_here,
                    stale: r.stale,
                })
            })
            .collect()
    }

    async fn claim_task(
        &self,
        id: &str,
        lease: &Lease<'_>,
        mode: ClaimMode,
        grace: Duration,
    ) -> Result<Option<u32>> {
        let pin = if mode == ClaimMode::Force {
            "true"
        } else {
            PIN_OPEN
        };
        // SET's right-hand sides see the record as it was: `claim` is the one
        // being replaced.
        let sql = format!(
            "UPDATE type::record('task', $tid) SET
                attempts += IF claim != NONE THEN 1 ELSE 0 END,
                claim = $claim, claimed_by = $holder, lease_until = time::now() + $ttl
             WHERE {CLAIMABLE_STATE} AND {} AND {pin}
             RETURN VALUE attempts",
            lease_open(mode)
        );
        let mut resp = self
            .db
            .query(sql)
            .bind(("tid", id.to_string()))
            .bind(("claim", lease.token.to_string()))
            .bind(("holder", holder(lease)))
            .bind(("ttl", lease.ttl))
            .bind(("node", lease.node.to_string()))
            .bind(("grace", grace))
            .await?;
        let attempts: Vec<i64> = resp.take(0)?;
        Ok(attempts.first().map(|&n| n as u32))
    }

    async fn renew_claims(&self, held: &[Held], ttl: Duration) -> Result<Vec<Renewal>> {
        // One statement per claim: each runs (and may conflict) on its own.
        let mut out = Vec::with_capacity(held.len());
        for chunk in held.chunks(64) {
            let sql: String = (0..chunk.len())
                .map(|i| {
                    format!(
                        "UPDATE type::record('task', $id{i}) SET lease_until = time::now() + $ttl
                         WHERE claim = $tok{i} RETURN VALUE state;"
                    )
                })
                .collect();
            let mut q = self.db.query(sql).bind(("ttl", ttl));
            for (i, h) in chunk.iter().enumerate() {
                q = q
                    .bind((format!("id{i}"), h.task_id.clone()))
                    .bind((format!("tok{i}"), h.token.clone()));
            }
            let mut resp = q.await?;
            for i in 0..chunk.len() {
                out.push(match resp.take::<Vec<String>>(i) {
                    Ok(states) => match states.first() {
                        Some(s) => Renewal::Held(s.parse()?),
                        None => Renewal::Lost,
                    },
                    Err(_) => Renewal::Unknown,
                });
            }
        }
        Ok(out)
    }

    async fn release_claims(&self, held: &[Held]) -> Result<u64> {
        // One statement per claim. One that fails (a write conflict with a
        // late renewal of the same task, say) is tried again; any still
        // failing makes the whole call fail, so the caller keeps them.
        let mut released = 0;
        let mut pending: Vec<&Held> = held.iter().collect();
        for tries in 1u64.. {
            let mut failed = Vec::new();
            for chunk in pending.chunks(64) {
                let sql: String = (0..chunk.len())
                    .map(|i| {
                        format!(
                            "UPDATE type::record('task', $id{i})
                             SET claim = NONE, claimed_by = NONE, lease_until = time::now()
                             WHERE claim = $tok{i} RETURN VALUE key;"
                        )
                    })
                    .collect();
                let mut q = self.db.query(sql);
                for (i, h) in chunk.iter().enumerate() {
                    q = q
                        .bind((format!("id{i}"), h.task_id.clone()))
                        .bind((format!("tok{i}"), h.token.clone()));
                }
                let mut resp = q.await?;
                for (i, h) in chunk.iter().enumerate() {
                    match resp.take::<Vec<String>>(i) {
                        Ok(keys) if !keys.is_empty() => released += 1,
                        Ok(_) => {}
                        Err(_) => failed.push(*h),
                    }
                }
            }
            if failed.is_empty() {
                break;
            }
            if tries == 5 {
                return Err(StorageError::Surreal(format!(
                    "handing back {} of {} claims kept failing",
                    failed.len(),
                    held.len()
                )));
            }
            tokio::time::sleep(Duration::from_millis(20 * tries + jitter_ms())).await;
            pending = failed;
        }
        Ok(released)
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

    async fn claim_pending_webhooks(
        &self,
        holder: &str,
        target: Option<&str>,
        mode: ClaimMode,
        ttl: Duration,
        limit: usize,
    ) -> Result<Vec<PendingWebhook>> {
        let open = delivery_open(mode);
        let mut resp = self
            .db
            .query(format!(
                "SELECT task_id, idx FROM item
                 WHERE webhook_delivered = false
                   AND state IN ['completed', 'failed', 'cancelled']
                   AND ($target = NONE OR webhook_target = NONE OR webhook_target = $target)
                   AND {open}
                 ORDER BY rand() LIMIT $limit"
            ))
            .bind(("target", target.map(str::to_string)))
            .bind(("limit", limit as i64))
            .await?;
        let rows: Vec<PendingRow> = resp.take(0)?;
        // In no particular order, so events that keep failing don't keep the
        // rest waiting. One statement per item: each runs (and may conflict
        // with another claimer) on its own, and a conflict only means not
        // claimed.
        let mut claimed = Vec::new();
        for chunk in rows.chunks(64) {
            let sql: String = (0..chunk.len())
                .map(|i| {
                    format!(
                        "UPDATE type::record('item', [$t{i}, $x{i}])
                         SET webhook_claim = $holder, webhook_claim_until = time::now() + $ttl
                         WHERE webhook_delivered = false AND {open} RETURN VALUE idx;"
                    )
                })
                .collect();
            let mut q = self
                .db
                .query(sql)
                .bind(("holder", holder.to_string()))
                .bind(("ttl", ttl));
            for (i, r) in chunk.iter().enumerate() {
                q = q
                    .bind((format!("t{i}"), r.task_id.clone()))
                    .bind((format!("x{i}"), r.idx));
            }
            // A chunk that fails is not claimed; those claimed before it are
            // still returned.
            let mut resp = match q.await {
                Ok(resp) => resp,
                Err(e) if claimed.is_empty() => return Err(e.into()),
                Err(_) => break,
            };
            for (i, r) in chunk.iter().enumerate() {
                if resp.take::<Vec<i64>>(i).is_ok_and(|won| !won.is_empty()) {
                    claimed.push(PendingWebhook {
                        task_id: r.task_id.clone(),
                        item_index: r.idx as usize,
                    });
                }
            }
        }
        Ok(claimed)
    }

    async fn renew_webhook_claims(
        &self,
        holder: &str,
        items: &[PendingWebhook],
        ttl: Duration,
    ) -> Result<Vec<bool>> {
        let mut out = Vec::with_capacity(items.len());
        for chunk in items.chunks(64) {
            let sql: String = (0..chunk.len())
                .map(|i| {
                    format!(
                        "UPDATE type::record('item', [$t{i}, $x{i}])
                         SET webhook_claim_until = time::now() + $ttl
                         WHERE webhook_delivered = false AND webhook_claim = $holder
                         RETURN VALUE idx;"
                    )
                })
                .collect();
            let mut q = self
                .db
                .query(sql)
                .bind(("holder", holder.to_string()))
                .bind(("ttl", ttl));
            for (i, p) in chunk.iter().enumerate() {
                q = q
                    .bind((format!("t{i}"), p.task_id.clone()))
                    .bind((format!("x{i}"), p.item_index as i64));
            }
            let mut resp = q.await?;
            for i in 0..chunk.len() {
                // A statement that failed (a conflict) says nothing: the claim
                // is taken as held, and lapses by itself if it isn't.
                out.push(
                    resp.take::<Vec<i64>>(i)
                        .map_or(true, |kept| !kept.is_empty()),
                );
            }
        }
        Ok(out)
    }

    async fn release_webhook_claim(
        &self,
        task_id: &str,
        item: usize,
        holder: &str,
    ) -> Result<bool> {
        // Retried on a conflict (with this process's renewal of it, say).
        let released: Vec<i64> = retry_conflicts(|| async {
            self.db
                .query(
                    "UPDATE type::record('item', [$tid, $idx])
                     SET webhook_claim = NONE, webhook_claim_until = NONE
                     WHERE webhook_delivered = false AND webhook_claim = $holder RETURN VALUE idx",
                )
                .bind(("tid", task_id.to_string()))
                .bind(("idx", item as i64))
                .bind(("holder", holder.to_string()))
                .await?
                .take(0)
        })
        .await?;
        Ok(!released.is_empty())
    }

    async fn mark_webhook_delivered(
        &self,
        task_id: &str,
        item: usize,
        holder: &str,
    ) -> Result<bool> {
        // Retried on a conflict (with this process's renewal of it, say): a
        // mark that fails leaves the event to be sent again.
        let marked: Vec<i64> = retry_conflicts(|| async {
            self.db
                .query(
                    "UPDATE type::record('item', [$tid, $idx])
                     SET webhook_delivered = true, webhook_claim = NONE, webhook_claim_until = NONE
                     WHERE webhook_delivered = false AND webhook_claim = $holder RETURN VALUE idx",
                )
                .bind(("tid", task_id.to_string()))
                .bind(("idx", item as i64))
                .bind(("holder", holder.to_string()))
                .await?
                .take(0)
        })
        .await?;
        Ok(!marked.is_empty())
    }

    async fn set_item_retries(
        &self,
        task_id: &str,
        item: usize,
        retries: u32,
        fence: Fence<'_>,
    ) -> Result<bool> {
        let mut resp = self
            .db
            .query(SET_RETRIES)
            .bind(("tid", task_id.to_string()))
            .bind(("idx", item as i64))
            .bind(("retries", retries as i64))
            .bind(("fence", fence.token().map(str::to_string)))
            .bind(("ts", now()))
            .await?;
        let written: Option<bool> = resp.take(0)?;
        Ok(written == Some(true))
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

    async fn cache_lookup(
        &self,
        content_hash: &str,
        model: &str,
        revision: &str,
        fresh_after: i64,
    ) -> Result<Option<ModelOutput>> {
        let mut resp = self
            .db
            .query(
                "SELECT VALUE output_json FROM type::record('cache', [$ch, $model, $rev])
                 WHERE created_at >= $fresh",
            )
            .bind(("ch", content_hash.to_string()))
            .bind(("model", model.to_string()))
            .bind(("rev", revision.to_string()))
            .bind(("fresh", fresh_after))
            .await?;
        let rows: Vec<String> = resp.take(0)?;
        match rows.into_iter().next() {
            Some(j) => Ok(Some(serde_json::from_str(&j)?)),
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
        self.db
            .query(
                "UPSERT type::record('cache', [$ch, $model, $rev]) SET
                    content_hash = $ch, model = $model, revision = $rev,
                    output_json = $output, created_at = $ts",
            )
            .bind(("ch", content_hash.to_string()))
            .bind(("model", model.to_string()))
            .bind(("rev", revision.to_string()))
            .bind(("output", json))
            .bind(("ts", now()))
            .await?
            .check()?;
        Ok(())
    }

    async fn url_cache_lookup(
        &self,
        url_hash: &str,
        model: &str,
        revision: &str,
        fresh_after: i64,
    ) -> Result<Option<String>> {
        let mut resp = self
            .db
            .query(
                "SELECT VALUE content_hash FROM type::record('url_cache', [$uh, $model, $rev])
                 WHERE created_at >= $fresh",
            )
            .bind(("uh", url_hash.to_string()))
            .bind(("model", model.to_string()))
            .bind(("rev", revision.to_string()))
            .bind(("fresh", fresh_after))
            .await?;
        let rows: Vec<String> = resp.take(0)?;
        Ok(rows.into_iter().next())
    }

    async fn url_cache_store(
        &self,
        url_hash: &str,
        model: &str,
        revision: &str,
        content_hash: &str,
    ) -> Result<()> {
        self.db
            .query(
                "UPSERT type::record('url_cache', [$uh, $model, $rev]) SET
                    url_hash = $uh, model = $model, revision = $rev,
                    content_hash = $ch, created_at = $ts",
            )
            .bind(("uh", url_hash.to_string()))
            .bind(("model", model.to_string()))
            .bind(("rev", revision.to_string()))
            .bind(("ch", content_hash.to_string()))
            .bind(("ts", now()))
            .await?
            .check()?;
        Ok(())
    }

    async fn purge_cache_before(&self, cutoff_unix_secs: i64) -> Result<u64> {
        // Count first, then delete the same predicate (mirrors
        // purge_finished_before); an entry written in between is simply fresh.
        let mut resp = self
            .db
            .query(
                "SELECT VALUE created_at FROM cache WHERE created_at < $cutoff;
                 SELECT VALUE created_at FROM url_cache WHERE created_at < $cutoff;",
            )
            .bind(("cutoff", cutoff_unix_secs))
            .await?;
        let content: Vec<i64> = resp.take(0)?;
        let urls: Vec<i64> = resp.take(1)?;
        if content.is_empty() && urls.is_empty() {
            return Ok(0);
        }
        self.db
            .query(
                "DELETE cache WHERE created_at < $cutoff;
                 DELETE url_cache WHERE created_at < $cutoff;",
            )
            .bind(("cutoff", cutoff_unix_secs))
            .await?
            .check()?;
        Ok((content.len() + urls.len()) as u64)
    }

    async fn auth_key(&self) -> Result<Option<SharedKey>> {
        let mut resp = self
            .db
            .query("SELECT public_key, created_by FROM ONLY type::record('auth_key', 'current')")
            .await?;
        let row: Option<AuthKeyRow> = resp.take(0)?;
        Ok(row.map(|r| SharedKey {
            public_key: r.public_key,
            node: r.created_by,
        }))
    }

    async fn publish_auth_key(&self, public_key: &str, node: &str) -> Result<SharedKey> {
        // CREATE on the one record id is the set-if-absent.
        let mut tries = 0;
        loop {
            let created = self
                .db
                .query(
                    "CREATE type::record('auth_key', 'current')
                     CONTENT { public_key: $key, created_by: $node, created_at: time::now() }
                     RETURN NONE",
                )
                .bind(("key", public_key.to_string()))
                .bind(("node", node.to_string()))
                .await
                .and_then(|r| r.check());
            let Err(e) = created else {
                return Ok(SharedKey {
                    public_key: public_key.to_string(),
                    node: Some(node.to_string()),
                });
            };
            // Stored first by another ("already exists", or a failed commit
            // when the two overlapped), or ours went through and its answer
            // was lost: look.
            if let Some(stored) = self.auth_key().await? {
                return Ok(stored);
            }
            tries += 1;
            if tries == 6 {
                return Err(e.into());
            }
            tokio::time::sleep(Duration::from_millis(20 * tries + jitter_ms())).await;
        }
    }

    async fn replace_auth_key(&self, public_key: &str, node: &str) -> Result<()> {
        retry_conflicts(|| async {
            self.db
                .query(
                    "UPSERT type::record('auth_key', 'current')
                     CONTENT { public_key: $key, created_by: $node, created_at: time::now() }
                     RETURN NONE",
                )
                .bind(("key", public_key.to_string()))
                .bind(("node", node.to_string()))
                .await
                .and_then(|r| r.check())
        })
        .await?;
        Ok(())
    }

    async fn add_revocation(&self, revocation: &Revocation) -> Result<()> {
        retry_conflicts(|| async {
            self.db
                .query(
                    "UPSERT type::record('revocation', [$kind, $value])
                     CONTENT { kind: $kind, value: $value, revoked_at: time::now() }
                     RETURN NONE",
                )
                .bind(("kind", revocation.kind().to_string()))
                .bind(("value", revocation.value().to_string()))
                .await
                .and_then(|r| r.check())
        })
        .await?;
        Ok(())
    }

    async fn remove_revocation(&self, revocation: &Revocation) -> Result<bool> {
        let mut resp = self
            .db
            .query("DELETE type::record('revocation', [$kind, $value]) RETURN BEFORE")
            .bind(("kind", revocation.kind().to_string()))
            .bind(("value", revocation.value().to_string()))
            .await?;
        let removed: Vec<RevocationRow> = resp.take(0)?;
        Ok(!removed.is_empty())
    }

    async fn revocations(&self) -> Result<Vec<Revocation>> {
        // A row of the wrong shape (written by hand) is left out, not a
        // failure of every read.
        let mut resp = self
            .db
            .query(
                "SELECT kind, value FROM revocation
                 WHERE type::is_string(kind) AND type::is_string(value)",
            )
            .await?;
        let rows: Vec<RevocationRow> = resp.take(0)?;
        Ok(rows
            .into_iter()
            .filter_map(|r| Revocation::from_parts(&r.kind, &r.value))
            .collect())
    }
}
