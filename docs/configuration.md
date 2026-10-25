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

A running server picks up an edited file with `apollo reload` (or `SIGHUP`), without a restart; a few settings (the listen address, TLS, auth, the database, the device, …) still need one. See [operations.md → Live config reload](./operations.md#live-config-reload).

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
| `max_concurrent` | u32 | `20` | Global ceiling on items being processed at once — fetching their input and running models (your VRAM budget). A slot is released while an item backs off between retries. |
| `idle_timeout` | u32 (secs) | `300` | How long a model stays resident while idle before its weights are unloaded, unless it sets `keep_in_memory`. |
| `log_level` | string | `"info"` | `trace`/`debug`/`info`/`warn`/`error`. The `RUST_LOG` env var overrides this at startup. |
| `max_memory` | size | `"4gb"` | Soft ceiling on the server process's memory. New work is rejected with `RESOURCE_EXHAUSTED` while usage is above it (`0` = off). On macOS usage is the process's physical footprint (what Activity Monitor and `footprint` show), which includes Metal GPU buffers; on Apple Silicon these hold the model weights and activations and are not part of the resident set size. Elsewhere it is the resident set size, so CUDA device memory is not counted. |
| `max_pending` | u32 | `1024` | Max items queued or in‑flight before submissions are rejected with `RESOURCE_EXHAUSTED` (backpressure). `0` = off. |
| `max_retries` | u32 | `3` | Times an item is retried after a **transient** failure (network error, HTTP 5xx/408/429, model timeout or load failure, inference error): a failed fetch is re‑fetched, failed models (or the failed pipeline step) re‑run while finished ones are kept. Permanent failures (refused URL, HTTP 4xx, undecodable or over‑limit input) are not retried. `0` = no retries. |
| `device` | string | `"auto"` | Compute backend for the models. `auto` picks the first that initializes: CUDA (only in `cuda` builds), then Metal (macOS), then CPU. `cpu` always uses the CPU, even on a GPU machine. `metal` / `cuda` pin that backend and fall back to CPU, with a warning, if it can't be used. |

**CPU threads.** Inference on the CPU uses one thread per physical core the server may run on: the performance cores on Apple silicon, and on Linux no more than its CPU affinity (`taskset`) and container CPU limit allow. The count is logged at startup (`CPU threads for inference`). To use fewer, for example when other heavy processes share the machine, set `CANDLE_NUM_THREADS` (or `RAYON_NUM_THREADS`) in the server's environment.

**CPU speed features.** Three build features make inference on the CPU faster, at the cost of bit-for-bit reproducible scores: they move in the last digits (at most 1e-5; no label changed in testing). They are off by default; enable them with `cargo build -p apollo --release --features <name>[,<name>]`.

| Feature | Where | Effect (measured) |
|---|---|---|
| `accelerate` | macOS | Matmuls on Apple's BLAS (the AMX units on Apple silicon): 1.7–3.7× faster CPU inference. |
| `avx512` | x86-64 CPUs with AVX-512 (AMD Zen 4/5, Intel Xeon) | `gemm`'s AVX-512 matmul kernels: images, video and long texts 1.2–1.7× faster, batched short texts 1.2–1.3×. Small matmuls (a few tokens) stay on the AVX2 kernels, so single short texts are not slower. |
| `fast-gelu` | any CPU | Vectorized GELU: 1.1–1.2× on BERT and ViT models. `CANDLE_FAST_GELU=0` turns it off at run time. |

---

## `[webhook]` — outbound delivery

Optional. Omit the whole section to disable delivery and let clients poll. See [webhooks.md](./webhooks.md) for semantics.

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `url` | string | *(required if section present)* | gRPC receiver target. Scheme selects transport: `https` = TLS, `http` = plaintext. Path is ignored. |
| `secret` | string | *(none)* | Shared secret. When set, each delivery is signed: `x-apollo-webhook-signature` = `v1=` + hex HMAC‑SHA256 over the delivery timestamp and the exact payload bytes (see [webhooks.md → Signing](./webhooks.md#signing-and-authenticity)). |
| `redelivery_secs` | u32 | `60` | Background retry interval for failed deliveries. `0` disables periodic retry (still retried on restart). |

---

## `[auth]` — authentication

**On by default.** Every `Inference` RPC must carry a valid PASETO v4 token (health and reflection stay open). The section is optional: with no `[auth]` at all, authentication is still enabled.

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `enabled` | bool | `true` | Require a token on every `Inference` RPC. Set `false` only on a trusted network — anyone who can reach the port can then submit work and read results (the server logs a warning at startup). |
| `public_key` | string | *(none)* | PASERK‑encoded v4 **public** key (`k4.public.…`) used to verify tokens. When unset, the server uses a **local keypair** (below). |
| `admin_subjects` | list | `[]` | Token subjects that may read and cancel **every** task. Everyone else only sees tasks created by their own subject. |
| `revocation_file` | string | `<state dir>/revoked_tokens` | Revoked token ids / subjects, written by `apollo revoke` and re‑read by running servers within ~10 s. Relative paths resolve from the config file's directory. |

**Zero‑config (local keypair).** Without `public_key`, the server creates a signing keypair on first start and keeps it in `auth.key` in its private per‑user state directory (`~/.local/state/apollo` on Linux, `~/Library/Application Support/apollo` on macOS; owner‑only). `apollo token` reads that file when no other secret key is given, so the minimal setup is:

```bash
apollo start --daemon                                   # first start creates the keypair
apollo token --subject my-client --expires 30d           # prints a token for a client to present
```

Run `apollo token` as the same OS user as the server (it looks in that user's state directory).

**Explicit keypair** (e.g. several servers sharing one key, or keys managed by your secret store):

```bash
apollo keygen                                 # prints public_key + secret_key (PASERK)
apollo config set auth.public_key "k4.public.…"   # keep secret_key safe
export APOLLO_SECRET_KEY="k4.secret.…"
apollo token --subject ci-runner --expires 30d
```

To turn authentication off: `apollo config set auth.enabled false`.

**Task ownership.** Every task records the subject (`sub`) of the token that submitted it. `GetTask` and `CancelTask` from any other subject return `NOT_FOUND` — the same answer as for an id that does not exist — unless the caller's subject is in `admin_subjects`. Tasks submitted while authentication was disabled have no owner and are visible only to admins once it is on. Give each client (or tenant) its own subject.

**Revoking tokens.** `apollo token` gives every token a random id and prints it on stderr. To revoke without rotating the keypair:

```bash
apollo revoke --id 3f9c…             # one token, by the id `apollo token` printed
apollo revoke --token "v4.public.…"  # one token, reading its id from the token
apollo revoke --subject ci-runner    # every token of a subject (incl. tokens minted before ids existed)
```

Each command appends a line (`id <id>` or `subject <name>`) to the revocation file; running servers pick it up within about 10 seconds. The file is plain text: delete a line to un‑revoke. Run `apollo revoke` as the server's OS user (or pass `--config` when `revocation_file` is set there). If the file becomes unreadable, the server keeps the last list it loaded.

Clients send the token in the **`authorization`** metadata header (with or without a `Bearer ` prefix). See [grpc-api.md → Authentication](./grpc-api.md#authentication).

---

## `[tls]` — serve over TLS

Optional. With this section the gRPC API is served over TLS (HTTP/2 + ALPN); without it Apollo speaks plaintext, which is fine on loopback or behind a TLS‑terminating proxy. The server logs a warning when it serves plaintext on a non‑loopback address.

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `cert_file` | string | *(required)* | PEM certificate chain, leaf first. |
| `key_file` | string | *(required)* | PEM private key for the certificate. |
| `client_ca_file` | string | *(none)* | PEM CA bundle. When set, **mutual TLS** is required: clients must present a certificate signed by it (in addition to any `[auth]` token). |

Relative paths resolve from the config file's directory; startup fails if a file is missing. Certificates are read at startup — restart after renewing them.

```toml
[tls]
cert_file = "/etc/apollo/tls/fullchain.pem"
key_file  = "/etc/apollo/tls/privkey.pem"
# client_ca_file = "/etc/apollo/tls/clients-ca.pem"   # require client certificates
```

Clients then connect to `https://host:port`. With `apollo-client`, a private or self‑signed CA is trusted via `.ca_certificate(pem)`, a client certificate for mutual TLS is set via `.client_identity(cert_pem, key_pem)`, and `.tls_domain_name(name)` overrides the name checked against the certificate.

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
| `max_text` | size | `"100kb"` | Max size of a text input. Inline text over it is rejected at submit; uploaded text fails the task. `0` = unlimited. |

> Video inputs are probed and decoded by ffmpeg restricted to the local `file` protocol, so a crafted container or playlist cannot make ffmpeg open network URLs. Downloads are stored under a fixed name (keeping only a known media extension), and frame dimensions are checked against `max_pixels` before any frame is decoded.
>
> If `HTTP_PROXY`/`HTTPS_PROXY` are set, remote fetches go through that proxy and the proxy resolves hostnames, so `block_private_ips` can only vet what Apollo itself resolves — enforce egress rules on the proxy too.

> `max_download` caps the *encoded* bytes; `max_pixels` caps the *decoded* resolution — a small, highly compressible image can blow past memory even under the download cap, so both matter. Local‑file inputs (`file://` / bare paths) are refused unless you explicitly opt in via `allow_local_files` **and** list `local_roots`; this keeps an untrusted request from reading arbitrary files off the server.

---

## `[cache]` — result cache

Optional. A bare `[cache]` section turns it on (`enabled` defaults to `true`). When enabled, model outputs are cached by **content hash** (with a URL→content‑hash hint) so identical inputs skip inference. The URL hint assumes a URL's content does not change within `ttl_secs` (forever, without a TTL); set a TTL if your URLs can serve different content over time. The hint is only recorded when the content came from `main`, not from `fallback`. See [database.md → Result cache](./database.md#result-cache).

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `enabled` | bool | `true` | Master switch (so `[cache]` alone enables caching). |
| `ttl_secs` | u64 | *(none)* | Freshness window in seconds; older entries are ignored and deleted by an hourly purge. Omit for entries that never expire. |

---

## `[metrics]` — Prometheus metrics

Optional, off by default. When enabled, `GET /metrics` serves Prometheus metrics (queue, slots, memory, per-model inferences, batch sizes and latency, loads, scans, fetches, webhooks, …) on a separate listener. The endpoint is not authenticated, so it binds to loopback unless you change it. Read at startup. See [operations.md → Prometheus metrics](./operations.md#prometheus-metrics) for every metric.

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `enabled` | bool | `false` | Serve the endpoint. |
| `endpoint` | string | `"127.0.0.1"` | Bind host/IP. A non-loopback address logs a warning. |
| `port` | u16 | `9464` | Bind port. |

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
| `wal` | bool | `true` | Enable WAL so concurrent reads don't block. With WAL, `synchronous = NORMAL` is used: commits skip the per‑transaction fsync and the database stays consistent after a crash or power loss, at the cost of possibly losing the last few writes (resume redoes them). Without WAL, `FULL`. |
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
| `confirm` | u32 | `0` | Don't trust a single frame. A frame reaching the model's `early_exit.threshold` only counts as a trigger if at least this many **other** frames within `confirm_window` seconds of it reach it too; when the scan has no frames there, it samples `confirm` frames either side of it. An unconfirmed frame doesn't stop the scan, and in the video's aggregated result it counts as its surroundings (the highest score below the threshold around it), so a lone spike can't flag the video. Per‑frame results are reported as classified. Needs the model's `early_exit`; at most `8`. `0` trusts single frames. |
| `max_frames` | u32 | *(none)* | Most frames the sampling and `refine` steps may classify per video, for a predictable worst case on small servers. Steps take frames in order; the step that doesn't fit is thinned to what's left — keeping its **strongest** scene cuts, or otherwise spreading its frames evenly across the video — and later steps get none; `refine` gets the remainder, looking at the most ambiguous frames first. Frames sampled to `confirm` a trigger don't count. |
| `confirm_window` | float (s) | `0.25` | How far either side of a triggering frame `confirm` looks. Above `0.05`, at most `5`. Keep it short: real content lasts longer than a frame, while a model flickering on one frame doesn't. |
| `sampling` | list | *(required, non‑empty)* | Ordered sampling steps (see below). |

### `[[strategies.<name>.sampling]]` — a sampling step

Steps run in ascending `step` order (cheapest first, typically). Each step's required parameter depends on its `method`:

| `method` | Required param | Meaning |
|----------|----------------|---------|
| `iframes` | *(none)* | Keyframes only — cheapest, no full decode. |
| `fps` | `fps` (float) | Sample at N frames per second. |
| `uniform` | `count` (u32) | N frames spread across the clip: one in each of N equal stretches, at its midpoint. With `random = true`, at a random time within each stretch instead, all chosen when the step is planned. The times are fixed per scan (a retry, a resume and every model scanning the input get the same frames) but can't be known before the task exists, so nothing can be placed between them on purpose. |
| `every_nth` | `nth` (u32) | Take every Nth frame. |
| `scene` | `threshold` (0..1) | One frame per shot change above the threshold. Optional `min_gap` (seconds, default 0): of cuts closer together than this, keep only the strongest (highest scene score). That caps the cost on fast‑cut video, but an insert shorter than `min_gap` has both its cuts (in and out) inside it, and only the frame at the cut *in* shows it, so it is found only when that cut scores higher: about half the time. Leave `min_gap` at 0 where short inserts matter and cap the cost with `max_frames`. Detection decodes every frame, including non‑reference (B‑)frames, where a cut no keyframe marks can land, and skips the deblocking filter. |
| `refine` | `min_score` (0..1), `window` (s), `fps` | **Look closer.** After every other step has run without a trigger, sample at `fps` within `window` seconds either side of each frame whose trigger score was ambiguous — from `min_score` up to the model's `early_exit.threshold`. Needs the model's `early_exit` labels, and must be the last step(s). Clean, confidently scored video gets no extra frames. |

Frames closer than 0.05 s to one already chosen by an earlier step are skipped, and a resumed scan re-plans exactly the same frames (including `refine`'s, which are planned from the stored scores).

Steps are planned **lazily**: a step that needs a probe — keyframes for `iframes`, a full decode for `scene` — is only planned once the steps before it were classified without a trigger, so a video that triggers early never pays for scene detection. The probe starts in the background while the earlier frames are classified (and is cancelled if they trigger), and steps that need no probe are planned and classified together with the step before them.

All models scanning the same video share the work: keyframe and scene detection run once per video (per threshold), and each frame is decoded once per input size and handed to every model that samples it — also when they ask at the same moment, and on a retry. Recently decoded frames are kept briefly (up to 128 per video) for models that ask later.

Two strategies from `config.example.toml`, for different hardware:

```toml
# Sparse but thorough — suited to CPU-only servers. On the test set it used
# 10-25x fewer frames than progressive_scan on clean video, and still caught
# inserts from a single frame to 2 s at hard cuts, and brief moments inside a
# continuous shot.
[strategies.coverage_scan]
aggregation = "max"
early_exit  = true

[[strategies.coverage_scan.sampling]]
step = 1
method = "uniform"           # an early, spread-out look; finds long segments fast
count = 8
random = true                # a random frame in each eighth: no gaps to aim at

[[strategies.coverage_scan.sampling]]
step = 2
method = "scene"             # a frame at every hard cut: catches short inserts
threshold = 0.3
min_gap = 0                  # every cut; a larger gap can skip inserts shorter than it

[[strategies.coverage_scan.sampling]]
step = 3
method = "fps"               # gap filler: long single shots get a frame every 8 s
fps = 0.125

[[strategies.coverage_scan.sampling]]
step = 4
method = "refine"            # look closer around anything the model was unsure about
min_score = 0.1
window = 4.0
fps = 5

# Dense — ends in a 5 fps pass (~5 model runs per second of clean video), so it
# misses nothing the model can see. Best with a GPU.
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

Choosing a scene `threshold`: `0.3` found both cuts of every spliced‑in clip in testing, while `0.4` missed some of the cuts back out. Keyframes (`iframes`) are cheap but are usually placed at a fixed interval by the encoder, not at scene changes, so they are no substitute for `scene`.

---

## `[models.<label>]` — a registered model

Models are keyed by a **label** (how requests refer to them). `architecture` and `repo` are the essentials; nearly everything else is optional.

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `architecture` | enum | *(required)* | Image models: `vit` (fixed‑head classifier), `siglip` (open‑vocabulary), or `pdq` (perceptual-hash block list; see [below](#perceptual-hash-block-lists-pdq)). Text models: `bert`, `distilbert`, `xlm_roberta` (also RoBERTa checkpoints; `roberta` is an alias), `modernbert`, `deberta_v2` (also DeBERTa-v3); see [text models](#text-models). |
| `repo` | string | *(required, except for `pdq`)* | Hugging Face repo. `pdq` has none. |
| `revision` | string | `"main"` | Repo revision/branch/tag. |
| `enabled` | bool | `true` | If `false`, the model is rejected at submit time. |
| `keep_in_memory` | bool | `false` | Load the weights at startup and keep them resident (skip idle‑unload). |
| `max_concurrent` | u32 | `8` | GPU **batch size** for this model's worker. |
| `timeout` | u32 (secs) | `30` | Max time for one classification — one image, or one sampled video frame — measured from when it is handed to the model's batch queue. Excludes waiting for an admission slot and loading a cold model (loading has its own 30‑minute limit). |
| `priority` | i32 | `0` | Scheduling priority; higher is admitted from the queue ahead of earlier‑submitted lower‑priority work. May be negative. |
| `video_strategy` | string | *(none)* | Opts an image classifier into **video** input via the named strategy. |
| `early_exit` | table | *(none)* | Video‑scan trigger for this model (see below). |
| `labels` | list | *(empty)* | Candidate labels for open‑vocabulary models (`siglip`). Ignored by `vit` (which gets labels from the weights). Mutually exclusive with `taxonomy_file`. |
| `prompt_template` | string | *(none)* | Wraps each `siglip` label before encoding, e.g. `"a photo of a {}"` (a `{}` is substituted; otherwise it's a prefix). The bare label is what's returned. |
| `score_threshold` | f32 | `0.5` (taxonomy: none) | Keep `siglip` labels scoring at/above this: a number from 0 to 1 with at most 2 decimals. SigLIP scores are calibrated: a clear match scores about 0.5–0.9, noise below about 0.1 (see [Scores](#siglip-scores)). For video frame scans with `mean` aggregation, set it lower so labels seen in part of the video survive the average. A taxonomy with no threshold keeps every category. |
| `max_results` | usize | *(none)* | Cap on `siglip` labels or taxonomy categories returned (highest first). Omit to return every one above the threshold. |
| `taxonomy_file` | string | *(none)* | Path to a taxonomy TOML for a `siglip` model. Relative paths resolve from the config file's directory. Mutually exclusive with `labels`. |
| `hash_list` | string | *(required for `pdq`)* | `pdq`: the block list file. Relative paths resolve from the config file's directory. |
| `categories` | table | *(required for `pdq`)* | `pdq`: category names by id, e.g. `{ 1 = "known", 2 = "spam" }`. |
| `max_distance` | u32 | `31` | `pdq`: most differing bits (of 256) that still count as a match. |
| `min_quality` | u32 | `50` | `pdq`: images whose hash quality (0–100) is lower never match. |
| `match_rotations` | bool | `true` | `pdq`: also match rotated (90°/180°/270°) and mirrored copies. |
| `max_tokens` | usize | `128` | Text models: tokens per window (at most the model's limit). |
| `multi_label` | bool | *(from `config.json`)* | Text models: score each label on its own (sigmoid) instead of as one choice among them (softmax). |

### `[models.<label>.early_exit]`

What counts as a trigger during a **video** frame scan (has no effect without `video_strategy`). A trigger stops the scan early when the strategy's `early_exit` is on, steers its `confirm` and `refine` steps, and defines the scan's **segments**: the stretches of the video where a trigger label reached the threshold (see [gRPC API → Segments](./grpc-api.md#segments)). A model without `early_exit` gets no segments.

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `labels` | list of u32 | *(required, non‑empty)* | Category ids that trigger early exit (a class index for `vit`; a label index or taxonomy child id for `siglip`; a category id for `pdq`). |
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

A pipeline runs its steps in ascending `order` for one input, instead of running `models` as one parallel set. A step runs one model or several **concurrently**. When the whole step has finished and its optional `stop_if` gate fires (any listed category id at/above the threshold on the gating model's output), the pipeline **early‑exits**: every model of the later steps is marked `SKIPPED` and the task completes normally (firing the task webhook). A model **failure** instead fails the whole pipeline: a transient one is retried up to `[app].max_retries` (models that finished are kept), then the task is marked failed.

A request opts in by setting `pipeline = "<name>"` instead of `models`. Results come back in step order.

```toml
[pipelines.moderation]
steps = [
  { model = "nsfw", order = 1, stop_if = { labels = [1], threshold = 0.85 } },
  # Runs only if nsfw did not fire; both models at once.
  { model = ["category", "general"], order = 2 },
]
```

A multi‑model step can gate too; `stop_if.model` then names whose output is checked:

```toml
{ model = ["nsfw", "violence"], order = 1, stop_if = { model = "nsfw", labels = [1] } }
```

**`stop_if`** uses the `early_exit` shape (`labels` + `threshold`, default `0.85`) but is **independent** of a model's own `[models.<l>.early_exit]` (which governs video frame scans, not pipeline gating). For a video, it is checked on the scan's aggregated result.

| Step field | Type | Meaning |
|------------|------|---------|
| `model` | string or list | The model to run, or models to run concurrently (each must exist under `[models.*]`). |
| `order` | u32 | Execution position; unique within the pipeline. |
| `stop_if` | table | Optional gate: `{ model = "...", labels = [...], threshold = ... }`. `model` may be left out when the step runs one model. |

Validation rejects a step with no models, an unknown model, a model that appears in more than one step, a `stop_if.model` that is not one of its step's models, and a multi‑model step whose `stop_if` names no model.

---

## Perceptual-hash block lists (`pdq`)

A `pdq` model matches images against a list of known images, by their [PDQ](https://github.com/facebook/ThreatExchange/tree/main/pdq) hashes: 256-bit perceptual hashes that stay close when an image is re-encoded, resized, recoloured or lightly edited. It needs no weights and runs on the CPU. Like any image model it works on images, on video (frame by frame, through a `video_strategy`), in sets and in pipelines, and its `early_exit` gives video **segments**.

```toml
[models.blocklist]
architecture   = "pdq"
hash_list      = "blocklist.txt"
categories     = { 1 = "known", 2 = "spam" }
max_distance   = 31                    # default
video_strategy = "every_second"
early_exit     = { labels = [1, 2], threshold = 0.87 }
```

The list has one entry per line: a hash (64 hex digits, as other PDQ tools write them) and a category id. Blank lines and `#` comments are allowed:

```text
# known images
b2876d28d15e8657b8a56928c7ceb6570db1317b026ed881ad0145bf366ee993 1  # kyoto_1.jpg
339b33644e668c665b24233359b3c9b38d1918dbb0db661b66936ce1e3669a66 2
```

`apollo hash --category 1 DIR >> blocklist.txt` writes such lines for a folder of images. The list is read and checked at startup and on every reload: a malformed line or an unknown category rejects the config, and the running list stays. Editing the file has no effect until `apollo reload` (or a restart); a reload that finds it changed loads the model again.

An image is hashed and compared with every entry. With `match_rotations` (the default), its 8 dihedral hashes are compared: the hashes of its rotations by 90°, 180° and 270° and of its mirror images. They are derived from the same transform, as in Meta's reference. Each category with an entry within `max_distance` bits of any of them is predicted, with `score = 1 − distance / 256` for its closest entry, so an identical image scores 1. Any match scores at least `1 − max_distance / 256` (0.879 by default), so an `early_exit` threshold of 0.87 fires on every match. Results report the category ids and names from `categories`.

What matches, measured on photos:

- **Matches:** JPEG re-encoding and colour or brightness changes move a hash by a few bits. Rotated and mirrored copies match through the dihedral hashes (about 8–16 bits); with `match_rotations = false` they don't.
- **Usually matches:** downscaled copies stay close, but small thumbnails can drift past 31.
- **Doesn't match:** crops of more than a few percent.
- **Unrelated photos:** about 128 bits apart.

Flat or nearly blank images (hash quality below `min_quality`) never match, since they all look alike to PDQ.

**Compatibility with other PDQ tools.** Apollo's hashes match Meta's reference implementation ([ThreatExchange `pdq/cpp`](https://github.com/facebook/ThreatExchange/tree/main/pdq/cpp)) bit for bit: hash, quality and all 8 dihedral hashes. This was checked on photos and synthetic images of many sizes, against the reference built without fused multiply-add. Builds that fuse can differ in a bit or two on near-flat images. Like Meta's file hasher, Apollo first resizes images larger than 512 px on a side to 512×512 (nearest neighbour), so lists exchanged with other PDQ systems work.

**Orientation.** `apollo hash` hashes images as stored, ignoring EXIF orientation, as Meta's tools do. The server decodes images upright (EXIF applied, as for every model). With `match_rotations`, the two still match.

---

## Text models

A text model classifies UTF-8 text sent in a request's `text` (or as `TEXT` media). Five families of Hugging Face sequence-classification checkpoints load, by `architecture`:

| `architecture` | Loads | Example |
|----------------|-------|---------|
| `bert` | `BertForSequenceClassification` | `unitary/toxic-bert` (toxicity, multi-label) |
| `distilbert` | `DistilBertForSequenceClassification` | `distilbert/distilbert-base-uncased-finetuned-sst-2-english` (sentiment) |
| `xlm_roberta` | `XLMRobertaForSequenceClassification`, `RobertaForSequenceClassification` | `papluca/xlm-roberta-base-language-detection` (20 languages) |
| `modernbert` | `ModernBertForSequenceClassification` | `cirimus/modernbert-base-go-emotions` (28 emotions, multi-label) |
| `deberta_v2` | `DebertaV2ForSequenceClassification` (v2 and v3) | `protectai/deberta-v3-base-prompt-injection-v2` (prompt injection) |

```toml
[models.toxicity]
architecture = "bert"
repo         = "unitary/toxic-bert"
```

The repo needs a `config.json` (with `id2label`, which gives the labels), `model.safetensors`, and a `tokenizer.json`, or a `vocab.txt` for BERT-style models that ship only that (the standard WordPiece tokenizer is built from it, lower-casing as `tokenizer_config.json` says).

**Scores.** A multi-label model (`problem_type = "multi_label_classification"` in its `config.json`, or `multi_label = true`) scores every label on its own with a sigmoid; otherwise the labels' scores are a softmax and sum to 1. Like image models, a result has the top 5 labels plus any above 0.90.

**Long text.** Text is split into overlapping windows of `max_tokens` tokens (128 by default, a quarter of it shared with the next window), each classified on its own; a label's score is its highest in any window, so a passage anywhere in the text counts. A window's score is about the whole window, so smaller windows find a short passage in a long text better, while larger ones give each window more context. Measured with `unitary/toxic-bert`, one toxic sentence in 3,000 words of neutral text scored:

| `max_tokens` | Sentence at the start | In the middle | At the end |
|---|---|---|---|
| 512 | 0.66 | 0.03 | 0.09 |
| 128 (default) | 0.89 | 0.46 | 0.88 |
| 64 | 0.95 | 0.80 | 0.96 |

The same text without it scored 0.001. Text shorter than a window is classified in one piece, so short texts are unaffected.

Text models take neither `video_strategy` nor `early_exit` (they run no video scans), nor the `siglip` or `pdq` fields. They work in sets and pipelines like any model; a pipeline's models all need to accept the input.

---

## Taxonomy files (`siglip`)

A `taxonomy_file` defines grouped, prompt‑backed categories for a `siglip` model. It's a **two‑level tree**: top‑level tables are **parent** categories (each needs an integer `id`); nested tables are **child** categories (each needs an `id`, a list of `prompts`, an optional `aggregation`, and an optional `threshold`).

SigLIP scores each prompt independently against the image; a child's score is the **aggregation of its own prompts** — `mean` (default), `average` (alias for mean), or `max` — then calibrated (see [Scores](#siglip-scores)). Use `max` when the prompts are alternatives (any one matching should light the category); use `mean` when they're corroborating evidence for the same concept.

All prompts (taxonomy or `labels`) are encoded when the model loads. That's the slow part of loading — about 35 s for ~1,000 prompts on an 8‑core CPU — so the result is cached in an `apollo-prompt-embeds` folder inside the model's Hub cache directory and reused on later loads (well under a second). Changing a prompt, the model revision, or the tokenizer starts a new cache entry automatically; deleting the folder is always safe.

At inference, each child that scores at/above its threshold is returned as a flat `Prediction` whose `label` is the **child id**, highest first and capped by `max_results`. The threshold is the child's own `threshold` (a number from 0 to 1 with at most 2 decimals) when it sets one, else the model's `score_threshold`. Categories differ in how high SigLIP scores them, so a per-category threshold lets each be tuned on its own; `apollo calibrate --write` sets them from labelled examples (see [operations.md → Threshold calibration](./operations.md#threshold-calibration)). (Parent grouping is not carried on the wire — reconstruct parent→child from the taxonomy file if you need it.) Parent ids and child ids live in separate namespaces; ids are arbitrary unique `u32`s.

```toml
[scene]
id = 2

[scene.nature]
id = 2001
prompts = ["a nature landscape", "a forest", "mountains or hills", "a beach"]
aggregation = "max"      # alternatives, not a conjunction
threshold = 0.35         # this category's own cut-off (else score_threshold)
```

See [`taxonomy.example.toml`](../taxonomy.example.toml) for a full example.

### SigLIP scores

SigLIP's own sigmoid probabilities against short prompts are tiny: a clear match such as a dog photo against "dogs" scores about 0.006, and unrelated categories about 0.00001. Apollo therefore reports every SigLIP score (a label's, or a category's after aggregation) shifted by 7 on the logit scale: `sigmoid(logit(p) + 7)`. A clear match then scores about 0.5–0.9 and noise about 0.00–0.10, so thresholds of two decimals are enough. The shift is monotonic: the order of the labels is exactly SigLIP's. An old threshold maps to `sigmoid(logit(t) + 7)` on the new scale, rounded to two decimals: `0.00001` becomes about `0.01`. It was tuned on `google/siglip-base-patch16-224` with a 117-category taxonomy; on that set a `score_threshold` of 0.20 kept every clear and every weaker-but-correct top category (0.24 and up) and dropped nearly all noise.

---

## Validation

`apollo start` validates the config before serving and reports **all** problems at once. Enforced rules include:

- **Strategies:** `confirm` at most 8 with `confirm_window` in (0.05, 5] s, and only for models that define `early_exit`; at least one sampling step; unique `step` numbers; each method's required parameter present (`fps`→`fps`, `uniform`→`count`, `every_nth`→`nth`, `scene`→`threshold`, `refine`→`min_score` + `window` + `fps`); `min_gap` only on `scene` and ≥ 0; `refine` steps after all others, with `min_score` in 0..1, `window` in (0, 60] s and `fps` in (0, 60]. A model using a strategy with `refine` must define `early_exit`, with a `threshold` above the step's `min_score`.
- **Models:** `repo` is required, except for `pdq`; text models cannot set `video_strategy` or `early_exit`, `max_tokens` is at least 16, and `max_tokens`/`multi_label` only apply to text models; a `pdq` model needs a `hash_list` whose lines parse and name its `categories`, and cannot set `labels`, `taxonomy_file`, `prompt_template`, `score_threshold` or `max_results` (while the `pdq` fields are rejected on other architectures); `video_strategy` must name a defined strategy; `early_exit` must list at least one label **and** requires a `video_strategy`; `labels` and `taxonomy_file` are mutually exclusive; a referenced `taxonomy_file` must exist on disk.
- **Pipelines:** at least one step; no empty step; every `model` must be defined, and in at most one step; unique `order`s; a `stop_if` must list at least one label id, and name its step's model (`stop_if.model`) when the step runs several.
- **Taxonomies:** a category `threshold` must be a number from 0 to 1 with at most 2 decimals, as must a `siglip` model's `score_threshold`.
- **Database:** selecting `postgres`/`surrealdb` requires the matching `[database.<backend>]` section.

---

## CLI reference

```text
apollo start [--config PATH] [--endpoint HOST] [--port N] [--webhook-url URL] [--daemon]
apollo stop  [...]                       # gracefully stop a running daemon
apollo status [--json] [--watch [-n SECONDS]]   # is it running, and what is it doing
apollo reload [--json]                   # apply the edited config without a restart
apollo calibrate --model NAME --data DIR|FILE.jsonl [--label L]... [--target-precision 0.95]
                 [--json] [--write] [--config PATH]
apollo config get    <key> [--config PATH]
apollo config set    <key> <value> [--config PATH]
apollo config remove <key> [--config PATH]
apollo hash PATH... [--category ID] [--min-quality 50]   # PDQ hashes, for block lists
apollo keygen                            # print a PASETO v4 keypair (PASERK)
apollo token --subject NAME [--expires 30d] [--secret-key-file PATH]
apollo revoke (--id ID | --token TOKEN | --subject NAME) [--config PATH]
```

- **`start`** loads the config, applies **run‑only** overrides (`--endpoint`/`--port`/`--webhook-url` do **not** persist to the file), validates, and serves. `--webhook-url` replaces `[webhook].url`, or enables delivery to that URL when the section is absent. `--daemon` re‑executes the binary detached, writing `apollo.pid` and logging to `apollo.log` in a private per‑user state directory (`~/.local/state/apollo` on Linux, `~/Library/Application Support/apollo` on macOS).
- **`status`** reports whether the server is running and its live state:
  - version, PID, uptime, listen address, TLS/auth, config file (with its generation and when it was reloaded, after a reload), database, and log file;
  - memory in use against `max_memory` (measured as for `max_memory`: physical footprint on macOS, resident set size elsewhere);
  - the queue (items in flight, busy/waiting concurrency slots);
  - task counts by state, and pending webhook deliveries;
  - per model: loaded or not, input size, queued requests, batch size, images classified, and last use.

  `--json` prints the same as JSON; memory in use is `memory.used_bytes` (earlier builds named it `memory.rss_bytes`). The exit status is `0` when running and `3` when not (like `systemctl status`).

  `--watch` (`-w`) keeps the view open and refreshes it in place every second (`-n 0.5` for twice a second; minimum 0.2) until Ctrl‑C, adding live throughput: images per second per model and in total. If the server stops or restarts, it shows that and waits for it. With `--json`, `--watch` prints one JSON object per line per refresh (`time`, `running`, `status`, `rates`, `error`); when the output is not a terminal, each refresh is printed in sequence instead of redrawn. The server answers on a Unix socket in its private per‑user directory (`$XDG_RUNTIME_DIR/apollo/` on Linux, otherwise the state directory), so only the same OS user can query it — run `apollo status` as that user.
- **`reload`** re-reads the config file and applies it to the running server, then reports which models were added, reloaded, kept or removed, and which changed settings need a restart. An invalid config is rejected and the running one stays. `SIGHUP` does the same. See [operations.md → Live config reload](./operations.md#live-config-reload).
- **`calibrate`** measures a model's precision, recall and F1 on labelled examples over a range of thresholds (0.05–0.95, and down to 0.0001 for low-scoring labels), and recommends the lowest threshold reaching `--target-precision`. `--write` stores it in `early_exit.threshold`, or per category in the model's taxonomy file. See [operations.md → Threshold calibration](./operations.md#threshold-calibration).
- **`config get|set|remove`** edit the file by **dotted key** (e.g. `models.nsfw.repo`), format‑preserving. `set` creates missing tables — and a default `[app]`‑only file — as needed.
- **`hash`** prints the PDQ hash of each image given (directories are searched recursively), as stored (EXIF orientation not applied), so the hashes equal those of Meta's PDQ tools. With `--category`, each line is a block-list entry (`<hash> <category>  # file (quality N)`) ready to append to a `hash_list`. Images below `--min-quality` are skipped with a note on stderr.
- **`keygen`** prints a fresh public/secret PASERK keypair.
- **`token`** signs an API token with the secret key. The key is read from `--secret-key-file` (a bare PASERK or `keygen` output), else the `APOLLO_SECRET_KEY` env var, else the server's local keypair file (`auth.key` in the state directory). `--expires` accepts `s`/`m`/`h`/`d` suffixes; omit it for a non‑expiring key. The token goes to stdout (it goes in the client's `authorization` metadata) and its id to stderr.
- **`revoke`** adds a token id or a subject to the revocation file (see [Authentication](#auth--authentication)); running servers apply it within ~10 s.
