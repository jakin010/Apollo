# Database & persistence

Apollo persists the **entire task lifecycle**, so work survives restarts and interrupted video scans resume from where they stopped. Persistence is tracked at **per‑`(item, model)`** granularity — a crash mid‑task doesn't redo already‑completed pieces — with individual classified video frames checkpointed for fine‑grained resume. The same storage layer also holds the optional result cache and the webhook delivery flag.

> **Storage keeps an item layer; the wire API does not.** Each request submits a single input, so the gRPC/webhook `Task` reports one result directly (`Task → Models → Model`, see [grpc-api.md](./grpc-api.md#result-messages)). Internally the engine still models that input as an *item* — the unit of fetch, retry (`RETRYING`), and resume — which is why the schema below is `(item, model)`-grained.

All of this sits behind one `Storage` trait; the `[database].backend` setting selects the implementation at startup, and `open()` runs schema migrations before the server begins serving.

---

## Backends

| Backend | Status | Notes |
|---------|--------|-------|
| **`sqlite`** | Implemented (default) | Embedded file database. Best for single‑node deployments. |
| **`surrealdb`** | Implemented | Remote SurrealDB 4.x (tested with 4.0.0) over `ws(s)://` / `http(s)://`. |
| **`postgres`** | Future seam | Config shape exists; selecting it currently fails at startup with an "unsupported backend" error. |

Selection and per‑backend settings live under `[database]` — see [configuration.md → Database](./configuration.md#database--persistence). Choosing `surrealdb` (or `postgres`) requires its matching sub‑section.

```toml
[database]
backend   = "sqlite"     # sqlite | surrealdb | postgres(future)
# retention = "30d"      # optional; omit to keep finished tasks forever

[database.sqlite]
path            = "/var/lib/apollo/classifications.db"
wal             = true
busy_timeout    = 5000
max_connections = 5
```

---

## Migrations

Migrations are **idempotent** and run automatically inside `open()` at startup, in one transaction, before the gRPC server starts. There is no separate migrate command and no schema‑version table — the schema is created with `IF NOT EXISTS` guards, so starting against an existing database is a no‑op.

---

## What is stored, and how

Values are stored in backend‑native columns where it helps queries (ids, states, timestamps, flags) and as **JSON** where the shape is rich:

- **Lifecycle states** (`TaskState`, `ItemState`, `ModelState`) are stored as their lowercase string tokens (`queued`, `processing`, `completed`, `retrying`, `done`, `skipped`, `failed`, `cancelled`).
- **Model outputs** (`Classification` / `Scan` / `Text`) and **cache entries** are stored as serialized JSON. Outputs stored before the API redesign (`FrameScan` with `frames`) still read back.
- **Errors** (the typed `TaskError`: a category + message) are stored as JSON; a value that doesn't parse is read back as an uncategorized error carrying the raw text.
- **Timestamps** (`created_at` / `updated_at`) are UNIX seconds.

---

## SQLite schema

Six tables. `tasks → items → model_results → frames` form a cascade (deleting a task removes everything under it); `cache` and `url_cache` are independent.

### `tasks`

One row per submission.

| Column | Type | Notes |
|--------|------|-------|
| `id` | TEXT | Primary key (the task id). |
| `state` | TEXT | `TaskState` token. |
| `owner` | TEXT | Subject of the token that submitted the task (`NULL` when auth was disabled). Only that subject or an admin can read / cancel / list it. |
| `attempts` | INTEGER | Crash‑interrupted resume count; a task is failed once this exceeds the cap (poison‑task guard). A takeover counts one only when the claim it replaces was never handed back (its runner crashed); a graceful shutdown hands claims back, so restarts and deploys never count. |
| `claim` | TEXT | The current runner's token. Its writes apply only while this is still its token. `NULL` when handed back, or for a task stored before leases. |
| `claimed_by` | TEXT | Who holds the claim: `<node>/<process>`. |
| `lease_until` | INTEGER | When the claim lapses unless renewed (milliseconds, on the database's clock). |
| `pin` | TEXT | The node whose upload or local file the task reads, if any: only it may run the task. |
| `metadata_json` | TEXT | The client's `metadata` as a JSON object (`NULL` when empty). |
| `request_id` | TEXT | The client's idempotency key, if any. |
| `created_at` / `updated_at` | INTEGER | UNIX seconds. |
| `finished_at` | INTEGER | UNIX seconds when the task first reached a terminal state; `NULL` until then. |

Indexed on `state` (`idx_tasks_state`) for fast recovery scans, on `request_id` (`idx_tasks_request`) for idempotent resubmission, and on `(created_at, id)` (`idx_tasks_created`) for `ListTasks`. The `owner`, `metadata_json`, `request_id` and `finished_at` columns are added to existing databases automatically.

### `items`

One row per input within a task. `PRIMARY KEY (task_id, idx)`.

| Column | Type | Notes |
|--------|------|-------|
| `task_id` | TEXT | → `tasks(id)`, `ON DELETE CASCADE`. |
| `idx` | INTEGER | Item index within the task. |
| `input_json` | TEXT | The serialized input (URL / bytes reference). |
| `models_json` | TEXT | The model labels to run. |
| `pipeline` | TEXT | Pipeline name, if the item runs a pipeline. |
| `state` | TEXT | `ItemState` token. |
| `error` | TEXT | Item‑level error as JSON, if failed. |
| `webhook_delivered` | INTEGER | 0/1 — whether the `FINISHED` webhook event was delivered. |
| `retries` | INTEGER | Retry count, compared against `[app].max_retries`. |

### `model_results`

One row per `(item, model)`. `PRIMARY KEY (task_id, item_idx, label)`, cascading from `items`.

| Column | Type | Notes |
|--------|------|-------|
| `task_id`, `item_idx` | TEXT/INTEGER | → `items(task_id, idx)`. |
| `label` | TEXT | Model label. |
| `state` | TEXT | `ModelState` token. |
| `output_json` | TEXT | Serialized `ModelOutput` (`Classification`/`Scan`, or `Text` for a model that reads text), when done. |
| `error` | TEXT | Model‑level error as JSON, if failed. |
| `steps_completed` | INTEGER | Reserved for skipping finished sampling steps on resume; not used yet (resume relies on the `frames` rows). |

### `frames`

The **video‑scan checkpoint**: one row per classified frame, written one batch (one transaction) at a time so an interrupted scan resumes without re‑classifying frames. `PRIMARY KEY (task_id, item_idx, label, frame_index)`, cascading from `model_results`.

| Column | Type | Notes |
|--------|------|-------|
| `task_id`, `item_idx`, `label` | — | The owning `(item, model)`. |
| `frame_index` | INTEGER | Ordinal among sampled frames. |
| `timestamp` | REAL | Seconds into the video. |
| `class_json` | TEXT | That frame's `Classification`. |

### `cache` and `url_cache`

The optional result cache (see below).

| `cache` column | Type | | `url_cache` column | Type |
|----------------|------|---|--------------------|------|
| `content_hash` | TEXT | | `url_hash` | TEXT |
| `model` | TEXT | | `model` | TEXT |
| `revision` | TEXT | | `revision` | TEXT |
| `output_json` | TEXT | | `content_hash` | TEXT |
| `created_at` | INTEGER | | `created_at` | INTEGER |

`cache` is keyed by `(content_hash, model, revision)`; `url_cache` by `(url_hash, model, revision)`.

---

## SurrealDB schema

The SurrealDB backend models the same data with `SCHEMALESS` tables and explicit indexes:

- **Tables:** `task`, `item`, `model_result`, `frame`, `cache`, `url_cache`.
- **Indexes:** `task(state)`, `task(created_at)`, `task(request_id)`, `item(task_id)`, `item(webhook_delivered, state)`, `model_result(task_id, item_idx)`, `frame(task_id, item_idx, label)`.

Records use composite record ids (e.g. `item:[task_id, idx]`, `model_result:[task_id, item_idx, label]`) mirroring the SQLite primary keys, so the same lifecycle, checkpointing, and cache semantics apply.

### SurrealDB versions and upgrades

Apollo uses the SurrealDB 4.0 client (4.0.0) and is tested against a 4.0.0 server. The client also accepts 3.x servers, but that combination is not tested.

- **Prefer `ws(s)://`.** Over `http(s)://` every request carries the token from signing in, and SurrealDB's root token lasts 1 hour by default. Once it expires, every storage call fails until Apollo restarts. A `ws(s)://` connection stays signed in and reconnects by itself after a dropped connection or a server restart. Meanwhile requests wait for it. Background work (running tasks, webhooks) carries on once it's back, while a client's call gives up after `[database].timeout` (30 s by default) with `UNAVAILABLE`. A Classify told so may still have its task stored and run once the database answers, so retry it with the same `request_id`: that returns the task rather than making another. Requests already sent when the connection broke fail, at the latest once the client reconnects. Each call that gave up and is answered later logs one harmless `Failed to send query results to channel` error from the SurrealDB client.
- **A failed sign-in after a reconnect needs an Apollo restart.** After a reconnect the client signs in again with the credentials it started with. If that fails for any reason (for example, changed credentials), the connection stays broken until Apollo restarts, even once the cause is gone. So restart Apollo after changing the server's credentials.

Upgrading a 3.x server to 4.0 (see SurrealDB's [3.x to 4.x guide](https://surrealdb.com/docs/build/migrating/from-old-surrealdb-versions/3x-to-4x)):

1. **Deploy this Apollo build first.** Builds on the 3.x client refuse 4.x servers at startup; this one accepts the 3.x server meanwhile.
2. **Export the data, check the file, and keep it.** For example `surreal export --endpoint … --username … --password … --namespace <ns> --database <db> backup.surql`, run with the 3.x CLI. Before 4.0 an export can stop part-way without an error, so make sure the file is complete, e.g. by importing it into a scratch 3.x server. Also keep a copy of the data directory, and rehearse the upgrade on a copy first. The upgrade is one-way: 3.x can't open the datastore again afterwards.
3. **Start 4.0 on the same datastore, opting in to the upgrade.** In 4.0 the in-place upgrade is experimental: by default 4.0 refuses a 3.x datastore at startup and leaves it unchanged. Set `SURREAL_EXPERIMENTAL_UPGRADE_FROM_3X=true` for the first start (e.g. `SURREAL_EXPERIMENTAL_UPGRADE_FROM_3X=true surreal db start …`, or the same variable in the container's environment). That start converts the data, with no separate migration command. Keep the variable set until it finishes: an interrupted start resumes the conversion on the next start, which needs the variable again. Once the conversion is done the variable does nothing. A RocksDB or SurrealKV datastore is rewritten, so leave free disk for a second copy. `/health` doesn't answer until the conversion finishes, so give health checks time, and don't let a liveness probe restart the server meanwhile. On a datastore several nodes share (e.g. TiKV), first move every node to the latest 3.3 and follow the guide's cluster steps. From 4.0.0, TiKV needs the separate server built from [surrealdb-tikv](https://github.com/surrealdb/surrealdb-tikv): the standard `surreal` binary and images refuse `tikv://`.
4. **Optionally, once every node runs 4.0,** run `REBUILD INDEX task_created ON task;` on its own (outside a transaction). Results are correct without it; it only speeds up task listings filtered by creation time.

---

## Resume & recovery

Every task is claimed by the instance that runs it, with a lease it renews every third of `[database].lease_secs`. On startup the engine takes over unfinished tasks, fully reconstructed from these tables, and re‑queues them: on SQLite (one process per file) every one; on SurrealDB the ones nobody holds (stored before leases, or handed back by a graceful stop), and, every third of the lease from then on, also the ones whose lease lapsed (their runner stopped), but only once it has itself been in touch with the database for a whole lease, and only up to its free room and for tasks whose models it has (a task whose input is local to it, it takes back whatever its room). A task cancelled while its runner was gone is taken over too, to finish its items as cancelled. A task that no instance has held for ten leases and that the instance looking at it can't run (its model or pipeline is gone) is failed, saying why. Recovery relies on:

- `tasks.state` (indexed) to find incomplete work,
- `tasks.claim` and `lease_until` to tell held tasks from lapsed ones,
- `model_results.state` + `frames` to skip already‑finished models and frames,
- `tasks.attempts` (bumped by a takeover of a claim never handed back) to **fail a poison task** once it exceeds its cap instead of crash‑looping — its items are marked failed, its webhook fires, and its staged upload is removed,
- `items.webhook_delivered` to redeliver any `FINISHED` webhook event that a crash left un‑acknowledged.

A runner whose lease was taken over (it lost touch with the database for longer than the lease) is fenced out: its writes no longer apply, and it stops when its next renewal finds the claim gone. A task whose input is an upload or a local file is pinned to the node that accepted it (`[app].node_id`): other instances leave it alone, and fail it with a clear message (submit it again) only once its lease has lapsed for ten leases. Cancelling one needs no input, so any instance finishes a cancelled one. The startup upload sweep removes only unreferenced files older than an hour, so a shared upload directory doesn't lose one still arriving.

State writes only apply to an unfinished task or item: the first finish wins, so a late or second runner can't reopen a task, overwrite a cancel, or start an item another runner finished (one already partway through it can still overwrite its model results). An upload is read, and removed, only from the server's own upload directory (`[app].cache_dir/uploads`), whatever path a task's row holds; anything else fails the item.

---

## Retention

If `[database].retention` is set (e.g. `"30d"`), a background timer periodically deletes **finished** tasks whose `updated_at` is older than the cutoff, returning the number removed. The cascade removes their items, model results, and frames in one step. Omit `retention` to keep finished tasks forever. The cache is governed separately by its own TTL.

---

## Result cache

When `[cache]` is enabled, model outputs are cached so identical inputs skip inference:

- **Content cache** (`cache` table): keyed by the input's **content hash** + model + revision. A hit returns the stored `ModelOutput` directly, unless it is not the kind of output the model gives now (its label was repointed at a model that reads text, or back). A pipeline step with an `input` is keyed by the text it classifies instead, and not cached when that text is empty.
- **URL→content hint** (`url_cache` table): maps a URL's hash to the content hash it last resolved to, so a repeated URL can reach the content cache without re‑downloading.

Freshness is controlled by `[cache].ttl_secs`: a lookup passes a lower bound on `created_at`, so entries older than the TTL are ignored, and an hourly job deletes them. Omit `ttl_secs` for entries that never expire. Keying on `revision` means bumping a model's revision naturally invalidates its cached outputs.
