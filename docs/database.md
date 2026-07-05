# Database & persistence

Apollo persists the **entire task lifecycle**, so work survives restarts and interrupted video scans resume from where they stopped. Persistence is tracked at **per‑`(item, model)`** granularity — a crash mid‑task doesn't redo already‑completed pieces — with individual classified video frames checkpointed for fine‑grained resume. The same storage layer also holds the optional result cache and the webhook delivery flag.

> **Storage keeps an item layer; the wire API does not.** Each request submits a single input, so the gRPC/webhook `Task` reports one result directly (`Task → Models → Model`, see [grpc-api.md](./grpc-api.md#result-messages)). Internally the engine still models that input as an *item* — the unit of fetch, retry (`RETRYING`), and resume — which is why the schema below is `(item, model)`-grained.

All of this sits behind one `Storage` trait; the `[database].backend` setting selects the implementation at startup, and `open()` runs schema migrations before the server begins serving.

---

## Backends

| Backend | Status | Notes |
|---------|--------|-------|
| **`sqlite`** | Implemented (default) | Embedded file database. Best for single‑node deployments. |
| **`surrealdb`** | Implemented | Remote SurrealDB (3.1.x) over `ws(s)://` / `http(s)://`. |
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
- **Model outputs** (`Classification` / `FrameScan`) and **cache entries** are stored as serialized JSON.
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
| `attempts` | INTEGER | Resume‑attempt count; a task is failed once this exceeds the cap (poison‑task guard). |
| `created_at` / `updated_at` | INTEGER | UNIX seconds. |

Indexed on `state` (`idx_tasks_state`) for fast recovery scans.

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
| `webhook_delivered` | INTEGER | 0/1 — the `TaskStatus` delivery flag. |
| `retries` | INTEGER | Retry count, compared against `[app].max_retries`. |

### `model_results`

One row per `(item, model)`. `PRIMARY KEY (task_id, item_idx, label)`, cascading from `items`.

| Column | Type | Notes |
|--------|------|-------|
| `task_id`, `item_idx` | TEXT/INTEGER | → `items(task_id, idx)`. |
| `label` | TEXT | Model label. |
| `state` | TEXT | `ModelState` token. |
| `output_json` | TEXT | Serialized `ModelOutput` (`Classification`/`FrameScan`), when done. |
| `error` | TEXT | Model‑level error as JSON, if failed. |
| `steps_completed` | INTEGER | How many video sampling steps have fully finished (lets resume skip cheap re‑extraction). |

### `frames`

The **video‑scan checkpoint**: one row per classified frame, written incrementally so an interrupted scan resumes without re‑classifying frames. `PRIMARY KEY (task_id, item_idx, label, frame_index)`, cascading from `model_results`.

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
- **Indexes:** `task(state)`, `task(created_at)`, `item(task_id)`, `item(webhook_delivered, state)`, `model_result(task_id, item_idx)`, `frame(task_id, item_idx, label)`.

Records use composite record ids (e.g. `item:[task_id, idx]`, `model_result:[task_id, item_idx, label]`) mirroring the SQLite primary keys, so the same lifecycle, checkpointing, and cache semantics apply.

---

## Resume & recovery

On startup the engine loads every task still in a non‑terminal state, fully reconstructed from these tables, and re‑queues it. Recovery relies on:

- `tasks.state` (indexed) to find incomplete work,
- `model_results.state` + `frames` + `steps_completed` to skip already‑finished models and frames,
- `tasks.attempts` (bumped per resume) to **fail a poison task** once it exceeds its cap instead of crash‑looping,
- `items.webhook_delivered` to redeliver any `TaskStatus` webhook that a crash left un‑acknowledged.

---

## Retention

If `[database].retention` is set (e.g. `"30d"`), a background timer periodically deletes **finished** tasks whose `updated_at` is older than the cutoff, returning the number removed. The cascade removes their items, model results, and frames in one step. Omit `retention` to keep finished tasks forever. The cache is governed separately by its own TTL.

---

## Result cache

When `[cache]` is enabled, model outputs are cached so identical inputs skip inference:

- **Content cache** (`cache` table): keyed by the input's **content hash** + model + revision. A hit returns the stored `ModelOutput` directly.
- **URL→content hint** (`url_cache` table): maps a URL's hash to the content hash it last resolved to, so a repeated URL can reach the content cache without re‑downloading.

Freshness is controlled by `[cache].ttl_secs`: a lookup passes a lower bound on `created_at`, so entries older than the TTL are ignored (and become eligible for purge). Omit `ttl_secs` for entries that never expire. Keying on `revision` means bumping a model's revision naturally invalidates its cached outputs.
