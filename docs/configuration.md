# Configuration

Apollo is configured with a single **TOML** file. This document lists every section and field, its default, and what it does, plus the CLI for reading/writing config and minting auth tokens.

Working examples ship in the repo: [`config.example.toml`](../config.example.toml) and [`taxonomy.example.toml`](../taxonomy.example.toml).

---

## File location

The config file is resolved in this order:

1. the `--config <path>` flag, else
2. the built‑in default path: the per‑user application config directory —
   - Linux: `~/.config/apollo/config.toml`
   - macOS: `~/Library/Application Support/apollo/config.toml`
   - Windows: `%APPDATA%\apollo\config.toml`
   - (falls back to `./apollo/config.toml` when no home directory is known).

The server **exits at startup if no `[models.<label>]` section is defined** — there would be nothing to serve. Unknown keys are rejected (`deny_unknown_fields`), so a typo fails fast rather than being silently ignored.

Relative `taxonomy_file` paths inside a model are resolved against the **config file's directory**.

### Units

- **Sizes** accept a suffix: `512mb`, `4gb`, `1024kb`, or a bare byte count. `0` means "no limit / off".
- **Durations** in `retention` and cache TTLs use compact forms like `30d`, `12h`, `90m`, `3600s`.

---

## `[app]` — application‑wide settings

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `endpoint` | string | `"0.0.0.0"` | Bind host/IP. (The example uses `127.0.0.1` for local‑only.) |
| `port` | u16 | `8080` | Bind port. |
| `cache_dir` | string | *(none)* | Optional directory for model/download caches. |
| `max_concurrent` | u32 | `20` | Global ceiling on concurrent in‑flight inferences (your VRAM budget). |
| `idle_timeout` | u32 (secs) | `300` | How long a model stays resident while idle before its weights are unloaded, unless it sets `keep_in_memory`. |
| `log_level` | string | `"info"` | `trace`/`debug`/`info`/`warn`/`error`. The `RUST_LOG` env var overrides this at startup. |
| `max_memory` | size | `"4gb"` | Soft resident‑memory ceiling. New work is rejected with `RESOURCE_EXHAUSTED` while usage is above it (`0` = off). |
| `max_pending` | u32 | `1024` | Max items queued or in‑flight before submissions are rejected with `RESOURCE_EXHAUSTED` (backpressure). `0` = off. |
| `max_retries` | u32 | `3` | Times a failed item is retried before it is marked failed. `0` = no retries. |

---

## `[webhook]` — outbound delivery

Optional. Omit the whole section to disable delivery and let clients poll. See [webhooks.md](./webhooks.md) for semantics.

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `url` | string | *(required if section present)* | gRPC receiver target. Scheme selects transport: `https` = TLS, `http` = plaintext. Path is ignored. |
| `secret` | string | *(none)* | Shared secret. When set, each delivery carries `x-apollo-webhook-signature` = hex HMAC‑SHA256 of the task id. |
| `redelivery_secs` | u32 | `60` | Background retry interval for failed deliveries. `0` disables periodic retry (still retried on restart). |

---

## `[auth]` — authentication

Optional. When present, **every `Inference` RPC** must carry a valid PASETO v4 token (health and reflection stay open).

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `public_key` | string | *(required if section present)* | PASERK‑encoded v4 **public** key (`k4.public.…`) used to verify tokens. |

Setup:

```bash
apollo keygen                                 # prints public_key + secret_key (PASERK)
# put public_key in [auth].public_key; keep secret_key safe
export APOLLO_SECRET_KEY="k4.secret.…"
apollo token --subject ci-runner --expires 30d   # prints a token for a client to present
```

Clients send the token in the **`authorization`** metadata header (with or without a `Bearer ` prefix). See [grpc-api.md → Authentication](./grpc-api.md#authentication).

---

## `[limits]` — input safety (SSRF, local‑file, and resource guards)

Guards applied to inputs: SSRF protection when fetching remote URLs, a gate on local‑file inputs, and caps that bound resource use during download and decode.

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `max_download` | size | `"512mb"` | Per‑input download cap (also the `ClassifyStream` upload cap). `0` = unlimited. |
| `max_video_seconds` | u32 | `3600` | Reject videos longer than this. `0` = unlimited. |
| `block_private_ips` | bool | `true` | Refuse hosts resolving to private / loopback / link‑local addresses (SSRF guard). |
| `allowed_schemes` | list | `["http", "https"]` | URL schemes permitted for remote fetches. |
| `allow_local_files` | bool | `false` | Allow `file://` and bare local‑path inputs. Off by default; even when on, a path must resolve inside one of `local_roots`. |
| `local_roots` | list | `[]` | Directories a local‑file input may resolve inside (checked after canonicalization, so `..`/symlinks can't escape). Empty means no local path is permitted, even with `allow_local_files = true`. |
| `max_pixels` | u64 | `50000000` | Max decoded image pixels (width × height), rejecting decompression bombs before the pixel buffer is allocated. `0` = unlimited. Applies to still images and to sampled video frames. |

> `max_download` caps the *encoded* bytes; `max_pixels` caps the *decoded* resolution — a small, highly compressible image can blow past memory even under the download cap, so both matter. Local‑file inputs (`file://` / bare paths) are refused unless you explicitly opt in via `allow_local_files` **and** list `local_roots`; this keeps an untrusted request from reading arbitrary files off the server.

---

## `[cache]` — result cache

Optional. A bare `[cache]` section turns it on (`enabled` defaults to `true`). When enabled, model outputs are cached by **content hash** (with a URL→content‑hash hint) so identical inputs skip inference. See [database.md → Result cache](./database.md#result-cache).

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `enabled` | bool | `true` | Master switch (so `[cache]` alone enables caching). |
| `ttl_secs` | u64 | *(none)* | Freshness window in seconds; older entries are ignored and eligible for purge. Omit for entries that never expire. |

---

## `[database]` — persistence

See [database.md](./database.md) for the full picture.

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `backend` | enum | `sqlite` | `sqlite` \| `postgres` \| `surrealdb`. (`postgres` is a future seam and currently fails at startup.) |
| `retention` | duration | *(none)* | How long to keep finished tasks (e.g. `"30d"`). Omit to keep forever. |

Each backend has its own sub‑table; the one matching `backend` is required (except `sqlite`, which has defaults).

### `[database.sqlite]`

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `path` | string | `"apollo.db"` | Database file path. |
| `wal` | bool | `true` | Enable WAL so concurrent reads don't block. |
| `busy_timeout` | u32 (ms) | `5000` | How long to wait on a momentarily locked file. |
| `max_connections` | u32 | `5` | Connection pool size. |

### `[database.surrealdb]`

Connects to a remote SurrealDB over `ws(s)://` or `http(s)://`.

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `url` | string | *(required)* | SurrealDB endpoint. |
| `namespace` | string | *(required)* | Namespace. |
| `database` | string | *(required)* | Database name. |
| `user` | string | *(none)* | Username (omit user/password for an unauthenticated server). |
| `password` | string | *(none)* | Password. |

### `[database.postgres]` — future

Present in the schema for shape; selecting `backend = "postgres"` currently fails at startup. Fields: `host`, `port` (default `5432`), `user`, `password`, `dbname`, `sslmode`, `max_connections` (default `10`).

---

## `[strategies.<name>]` — applying an image classifier to a video

A named, reusable recipe: how to sample frames, how to aggregate their scores, and whether to early‑exit. Referenced by a model via `video_strategy`.

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `aggregation` | enum | `mean` | How per‑frame scores roll up into one video result: `max` or `mean` (alias: `average`). |
| `early_exit` | bool | `false` | Stop the scan as soon as a model's trigger fires (requires the model to define `early_exit.labels`, else no effect). |
| `sampling` | list | *(required, non‑empty)* | Ordered sampling steps (see below). |

### `[[strategies.<name>.sampling]]` — a sampling step

Steps run in ascending `step` order (cheapest first, typically). Each step's required parameter depends on its `method`:

| `method` | Required param | Meaning |
|----------|----------------|---------|
| `iframes` | *(none)* | Keyframes only — cheapest, no full decode. |
| `fps` | `fps` (float) | Sample at N frames per second. |
| `uniform` | `count` (u32) | N frames, evenly spaced across the clip. |
| `every_nth` | `nth` (u32) | Take every Nth frame. |
| `scene` | `threshold` (0..1) | One frame per shot change above the threshold. |

Example:

```toml
[strategies.progressive_scan]
aggregation = "max"
early_exit  = true

[[strategies.progressive_scan.sampling]]
step = 1
method = "iframes"           # cheap first pass

[[strategies.progressive_scan.sampling]]
step = 2
method = "scene"
threshold = 0.4

[[strategies.progressive_scan.sampling]]
step = 3
method = "fps"               # densest; only reached if earlier steps didn't exit
fps = 5
```

---

## `[models.<label>]` — a registered model

Models are keyed by a **label** (how requests refer to them). `architecture` and `repo` are the essentials; nearly everything else is optional.

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `architecture` | enum | *(required)* | `vit` (fixed‑head classifier) or `siglip` (open‑vocabulary). |
| `repo` | string | *(required)* | Hugging Face repo. |
| `revision` | string | `"main"` | Repo revision/branch/tag. |
| `enabled` | bool | `true` | If `false`, the model is rejected at submit time. |
| `keep_in_memory` | bool | `false` | Pin the weights resident (skip idle‑unload). |
| `max_concurrent` | u32 | `8` | GPU **batch size** for this model's worker. |
| `timeout` | u32 (secs) | `30` | Max **processing** time per `(input, model)`; queue wait excluded. |
| `priority` | i32 | `0` | Scheduling priority; higher is admitted from the queue ahead of earlier‑submitted lower‑priority work. May be negative. |
| `video_strategy` | string | *(none)* | Opts an image classifier into **video** input via the named strategy. |
| `early_exit` | table | *(none)* | Video‑scan trigger for this model (see below). |
| `labels` | list | *(empty)* | Candidate labels for open‑vocabulary models (`siglip`). Ignored by `vit` (which gets labels from the weights). Mutually exclusive with `taxonomy_file`. |
| `prompt_template` | string | *(none)* | Wraps each `siglip` label before encoding, e.g. `"a photo of a {}"` (a `{}` is substituted; otherwise it's a prefix). The bare label is what's returned. |
| `score_threshold` | f32 | `0.5` | Keep `siglip` labels scoring at/above this (sigmoid probability). For video frame scans, set this **low** so true peaks survive the temporal pool. |
| `max_results` | usize | *(none)* | Cap on labels returned (highest first). Omit to return every label above the threshold. |
| `taxonomy_file` | string | *(none)* | Path to a taxonomy TOML for a `siglip` model. Relative paths resolve from the config file's directory. Mutually exclusive with `labels`. |

### `[models.<label>.early_exit]`

What counts as a trigger during a **video** frame scan (has no effect without `video_strategy`).

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `labels` | list of u32 | *(required, non‑empty)* | Category ids that trigger early exit (a class index for `vit`; a label index or taxonomy child id for `siglip`). |
| `threshold` | f32 | `0.85` | The score a listed label must reach on a frame to trigger. |

Example:

```toml
[models.general]
architecture   = "vit"
repo           = "google/vit-base-patch16-224"
keep_in_memory = true
max_concurrent = 8
timeout        = 30
video_strategy = "representative"

[models.nsfw]
architecture   = "vit"
repo           = "Falconsai/nsfw_image_detection"
video_strategy = "progressive_scan"

[models.nsfw.early_exit]
labels    = [1]
threshold = 0.85

[models.category]
architecture    = "siglip"
repo            = "google/siglip-base-patch16-224"
taxonomy_file   = "taxonomy.example.toml"
score_threshold = 0.1        # low, so frame‑scan peaks survive aggregation
```

---

## `[pipelines.<name>]` — ordered, gated model execution

A pipeline runs its steps in ascending `order` for one input, instead of running `models` as a parallel set. If a step's optional `stop_if` gate fires (any listed category id at/above the threshold on that model's output), the pipeline **early‑exits** and later steps are marked `SKIPPED` — the task completes normally and fires the task webhook. A step **failure** (inference error) instead fails the whole pipeline, which is retried up to `[app].max_retries` and then marked failed.

A request opts in by setting `pipeline = "<name>"` on its item (instead of `models`).

```toml
[pipelines.moderation]
steps = [
  { model = "nsfw",     order = 1, stop_if = { labels = [1], threshold = 0.85 } },
  { model = "category", order = 2 },
  { model = "general",  order = 3 },
]
```

**`stop_if`** reuses the `early_exit` shape (`labels` + `threshold`, default `0.85`) but is **independent** of a model's own `[models.<l>.early_exit]` (which governs video frame scans, not pipeline gating).

| Step field | Type | Meaning |
|------------|------|---------|
| `model` | string | The model label to run (must exist under `[models.*]`). |
| `order` | u32 | Execution position; unique within the pipeline. |
| `stop_if` | table | Optional gate: `{ labels = [...], threshold = ... }`. |

---

## Taxonomy files (`siglip`)

A `taxonomy_file` defines grouped, prompt‑backed categories for a `siglip` model. It's a **two‑level tree**: top‑level tables are **parent** categories (each needs an integer `id`); nested tables are **child** categories (each needs an `id`, a list of `prompts`, and an optional `aggregation`).

SigLIP scores each prompt independently against the image; a child's score is the **aggregation of its own prompts** — `mean` (default), `average` (alias for mean), or `max`. Use `max` when the prompts are alternatives (any one matching should light the category); use `mean` when they're corroborating evidence for the same concept.

At inference, each child that scores at/above the model's `score_threshold` is returned as a flat `Prediction` whose `label` is the **child id**. (Parent grouping is not carried on the wire — reconstruct parent→child from the taxonomy file if you need it.) Parent ids and child ids live in separate namespaces; ids are arbitrary unique `u32`s.

```toml
[scene]
id = 2

[scene.nature]
id = 2001
prompts = ["a nature landscape", "a forest", "mountains or hills", "a beach"]
aggregation = "max"      # alternatives, not a conjunction
```

See [`taxonomy.example.toml`](../taxonomy.example.toml) for a full example.

---

## Validation

`apollo start` validates the config before serving and reports **all** problems at once. Enforced rules include:

- **Strategies:** at least one sampling step; unique `step` numbers; each method's required parameter present (`fps`→`fps`, `uniform`→`count`, `every_nth`→`nth`, `scene`→`threshold`).
- **Models:** `video_strategy` must name a defined strategy; `early_exit` must list at least one label **and** requires a `video_strategy`; `labels` and `taxonomy_file` are mutually exclusive; a referenced `taxonomy_file` must exist on disk.
- **Pipelines:** at least one step; every `model` must be defined; unique `order`s; a `stop_if` must list at least one label id.
- **Database:** selecting `postgres`/`surrealdb` requires the matching `[database.<backend>]` section.

---

## CLI reference

```text
apollo start [--config PATH] [--endpoint HOST] [--port N] [--webhook-url URL] [--daemon]
apollo stop  [...]                       # gracefully stop a running daemon
apollo config get    <key> [--config PATH]
apollo config set    <key> <value> [--config PATH]
apollo config remove <key> [--config PATH]
apollo keygen                            # print a PASETO v4 keypair (PASERK)
apollo token --subject NAME [--expires 30d] [--secret-key-file PATH]
```

- **`start`** loads the config, applies **run‑only** overrides (`--endpoint`/`--port`/`--webhook-url` do **not** persist to the file), validates, and serves. `--daemon` re‑executes the binary detached, writing a PID file and logging to `apollo.log` in the temp dir.
- **`config get|set|remove`** edit the file by **dotted key** (e.g. `models.nsfw.repo`), format‑preserving. `set` creates missing tables — and a default `[app]`‑only file — as needed.
- **`keygen`** prints a fresh public/secret PASERK keypair.
- **`token`** signs an API token with the secret key. The key is read from `--secret-key-file` or the `APOLLO_SECRET_KEY` env var. `--expires` accepts `s`/`m`/`h`/`d` suffixes; omit it for a non‑expiring key (revoke by rotating the keypair). The token goes in the client's `authorization` metadata.
