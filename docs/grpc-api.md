# gRPC API — the `Inference` service

Apollo exposes one gRPC service that clients call: **`apollo.v1.Inference`**. It is defined in `proto/inference.proto` and shares its message types with `proto/common.proto`.

- **Transport:** gRPC over HTTP/2.
- **TLS:** served natively when `[tls]` is configured (optionally with mutual TLS), or terminate it at a proxy in front. The examples below use `-plaintext` for a local server; with `[tls]`, drop `-plaintext` (add `-cacert ca.pem` for a private CA).
- **Server reflection** is enabled, so tools like `grpcurl` and Postman can discover the schema without a local copy of the `.proto` files.
- **Health checking** (`grpc.health.v1.Health`) is enabled; the server reports the `Inference` service as `SERVING`.
- **Authentication** is **on by default** and applies to every `Inference` RPC (health and reflection stay open). See [Authentication](#authentication).

Work is asynchronous: `Classify` returns the queued task right away. Get the result by waiting in the call (`wait_seconds`), streaming updates with `WatchTask`, polling `GetTask`, or receiving a [webhook](./webhooks.md).

---

## Methods

| RPC | Request | Response | Purpose |
|-----|---------|----------|---------|
| `Classify` | `ClassifyRequest` | `Task` | Submit one input (by URL or inline bytes). Returns the task, finished if it finished within `wait_seconds`. |
| `ClassifyStream` | stream of `ClassifyChunk` | `Task` | Submit one input as a stream of raw bytes. Returns like `Classify`. |
| `GetTask` | `GetTaskRequest` | `Task` | A task's state and results. Backed by the database, so it survives restarts. |
| `WatchTask` | `WatchTaskRequest` | stream of `Task` | The task now, then again each time it changes; ends once it is finished. |
| `ListTasks` | `ListTasksRequest` | `ListTasksResponse` | Your tasks, newest first, a page at a time. |
| `CancelTask` | `CancelTaskRequest` | `Task` | Request cooperative cancellation; returns the task afterwards. |
| `ListModels` | `ListModelsRequest` | `ListModelsResponse` | The configured models (with their labels) and pipelines. |

### `Classify(ClassifyRequest) → Task`

Submits a single input. Validation happens **before** the task is created:

- `media` and `run` must be set (`INVALID_ARGUMENT` otherwise);
- every model must exist and be enabled, appear once, and accept the input's kind (see [modality rules](#modality-rules));
- `metadata` must stay within its limits, and `wait_seconds` at most 300.

A validation failure returns `INVALID_ARGUMENT` and no task is created. On success the task is persisted and returned:

- with `wait_seconds = 0` (the default), as it is now (usually `QUEUED`);
- otherwise once it finishes, or after `wait_seconds` with whatever state it reached by then. Either way you get a `Task` back; check `state`.

**Idempotency:** with a `request_id`, submitting again with the same `request_id` (as the same caller) returns the existing task instead of creating a new one, whatever the new request contains. Use it to retry a `Classify` that timed out without risking a duplicate. Keys are scoped to the caller (token subject), so two callers can use the same key.

**Inline content:** `media.data` carries the bytes themselves instead of a `url`. It is bounded by the gRPC message size (4 MiB by default) and by `[limits].max_download`; stream larger files with `ClassifyStream`. The server stages the bytes to a private upload file and removes it when the task finishes.

### `ClassifyStream(stream ClassifyChunk) → Task`

Submits one input as a byte stream instead of a URL, useful for local content you don't want to (or can't) expose over HTTP, or files too big for one message.

- The **first** message MUST be the `init` frame (`ClassifyStreamInit`): the input's `kind`, what to `run` (models or a pipeline), and the same optional fields as `Classify` (`metadata`, `request_id`, `wait_seconds`, `view`).
- Every later message carries `data` bytes, **in order**. Send as many `data` frames as you like; the server appends each to its staging file as it arrives, so neither side holds the whole input in memory.

The `init` frame is validated (kind, models or pipeline, server capacity) **before** any content is accepted, so a bad request fails fast without uploading anything. A `request_id` already used by the caller returns the existing task without reading the content. The server streams the bytes to an owner‑only staging file (under `[app].cache_dir/uploads`, or a per‑user cache directory), enforcing the upload byte cap (`[limits].max_download`) **incrementally**, then submits it exactly like `Classify`. A second `init`, a stream with no data, or exceeding the cap is rejected (`INVALID_ARGUMENT` / `RESOURCE_EXHAUSTED`) and the staging file is removed. The staged file is also removed once the task finishes.

### `GetTask(GetTaskRequest) → Task`

Returns the current `Task`. `view` picks the result detail: `SUMMARY` (the default) leaves out the per-sample results of a scan, `FULL` includes them (see [`ResultView`](#resultview)). Returns `NOT_FOUND` for an unknown id. Because task state is persisted, this works across restarts and for finished tasks (until [retention](./database.md#retention) purges them).

### `WatchTask(WatchTaskRequest) → stream Task`

Sends the task as it is now, then again each time it changes (it starts processing, a model finishes, a retry begins, …), and ends the stream after sending it `COMPLETED`, `FAILED` or `CANCELLED`. Updates may be coalesced: a burst of changes can arrive as one message carrying the latest state. Use it when you can't receive webhooks and don't want to poll. `view` works as for `GetTask`.

### `ListTasks(ListTasksRequest) → ListTasksResponse`

Lists tasks newest first, with summary results. Filter by `states` (any of them; all states when empty) and by creation time (`created_after` inclusive, `created_before` exclusive). `page_size` defaults to 50 (at most 500); pass the response's `next_page_token` as the next request's `page_token` to continue, until it comes back empty.

With authentication on, a caller lists only its own tasks; [admin subjects](./configuration.md#auth--authentication) (and a server with authentication off) list every task.

### `CancelTask(CancelTaskRequest) → Task`

Requests cooperative cancellation and returns the task **after** the request: `CANCELLED`, unless it had already finished. Running work stops at the next checkpoint: between models, or between sampled video frames. Results of models that finished before are kept; models that had not finished are reported `CANCELLED`. Returns `NOT_FOUND` for an unknown id.

### `ListModels(ListModelsRequest) → ListModelsResponse`

The configured models and pipelines, so clients need not hard-code them. Each model lists the input kinds it accepts, whether it is enabled and currently loaded, its labels (id and name, as in predictions), its trigger (`[models.<name>.early_exit]`) and its video strategy. Labels are read without loading or downloading the model: from the config for `siglip` and `pdq` (its `categories`), from the cached `config.json` for `vit`. A `vit` model that was never downloaded has no labels listed until its first use.

---

## Requests

```protobuf
message ClassifyRequest {
  oneof input { Media media = 1; }          // what to classify
  oneof run {                               // what to run on it
    ModelList models   = 2;                 // these models, concurrently
    string    pipeline = 3;                 // a named [pipelines.<name>]
  }
  map<string, string> metadata = 4;         // echoed back on the task and in webhooks
  string     request_id   = 5;              // idempotency key
  uint32     wait_seconds = 6;              // wait up to this long (max 300) for the result
  ResultView view         = 7;              // detail of the returned task
}

message Media {
  MediaKind kind = 1;                       // IMAGE, VIDEO or AUDIO (required)
  oneof source {
    string url  = 2;                        // local path, file://, or http(s)://
    bytes  data = 3;                        // the content itself
  }
  string fallback_url = 4;                  // tried only if `url` cannot be fetched or decoded
}

message ModelList { repeated string names = 1; }

// ClassifyStream: `init` first, then `data` chunks in order.
message ClassifyStreamInit {
  MediaKind kind = 1;
  oneof run { ModelList models = 2; string pipeline = 3; }
  map<string, string> metadata = 4;
  string     request_id   = 5;
  uint32     wait_seconds = 6;
  ResultView view         = 7;
}
message ClassifyChunk { oneof payload { ClassifyStreamInit init = 1; bytes data = 2; } }

message GetTaskRequest    { string task_id = 1; ResultView view = 2; }
message WatchTaskRequest  { string task_id = 1; ResultView view = 2; }
message CancelTaskRequest { string task_id = 1; }

message ListTasksRequest {
  repeated TaskState states = 1;
  google.protobuf.Timestamp created_after  = 2;
  google.protobuf.Timestamp created_before = 3;
  uint32 page_size  = 4;
  string page_token = 5;
}
message ListTasksResponse { repeated Task tasks = 1; string next_page_token = 2; }
```

Notes:

- `models` run as a **set**, concurrently; a pipeline runs its **steps** in order, each step's models concurrently, gated by `stop_if` (see [configuration → pipelines](./configuration.md#pipelinesname--ordered-gated-model-execution)).
- `metadata`: at most 16 entries; keys 1–64 bytes, values up to 512 bytes. Use it for your own record id, batch name, and so on.
- `request_id`: up to 128 bytes.
- `AUDIO` is accepted by the schema but no audio models exist yet, so audio input is rejected as incompatible.

---

## Results

```protobuf
message Task {
  string    id    = 1;
  TaskState state = 2;                      // always set
  Error     error = 3;                      // why the task failed, when FAILED
  repeated ModelResult results = 4;         // one per model, in run order
  string    pipeline   = 5;                 // empty for a plain set of models
  map<string, string> metadata = 6;         // from the request
  string    request_id = 7;                 // from the request
  google.protobuf.Timestamp created_at  = 8;
  google.protobuf.Timestamp updated_at  = 9;
  google.protobuf.Timestamp finished_at = 10; // unset until finished
  Progress  progress = 11;
  uint32    retries  = 12;                  // retries after transient failures
}

message Progress { uint32 models_total = 1; uint32 models_finished = 2; }

message ModelResult {
  string     model = 1;                     // as configured
  ModelState state = 2;                     // always set
  Error      error = 3;                     // why it failed, when FAILED
  oneof output {                            // set when COMPLETED
    Classification classification = 4;      // an image, classified once
    Scan           scan           = 5;      // an image classifier over a video's frames
  }
}

message Classification { repeated Prediction predictions = 1; }
message Prediction { uint32 label = 1; string name = 2; float score = 3; }

message Scan {
  Classification   aggregated   = 1;        // rolled up across samples (max/mean)
  repeated Segment segments     = 2;        // where the trigger labels fired
  repeated Sample  samples      = 3;        // every classified sample — FULL view only
  uint32           sample_count = 4;        // also set in the SUMMARY view
}

message Sample {                            // one classified video frame (start == end)
  uint32 index = 1;                         // position in processing order
  double start = 2;                         // seconds into the media
  double end   = 3;
  Classification classification = 4;
}

message Segment {
  uint32 label = 1;  string name = 2;       // the trigger label
  double start = 3;  double end  = 4;       // first and last flagged sample
  float  peak  = 5;  double peak_at = 6;    // highest score, and where
  uint32 samples = 7;                       // flagged samples in the segment
  double bound_start = 8;                   // nearest clear sample before (or 0)
  double bound_end   = 9;                   // nearest clear sample after (or the end)
}
```

**Result semantics:**

- `Task.state` and every `ModelResult.state` are always set. A task is `COMPLETED` once every model was **attempted**; check each result's `state`, since one model may have `FAILED` while the others completed. A task is `FAILED` when the task as a whole failed (its input could not be fetched or decoded, a pipeline step failed, or it was abandoned after repeated crashes); `Task.error` says why.
- A task whose input is being **retried** after a transient failure stays `PROCESSING`, with `retries` counting the attempts; `error` is only set once it has failed for good.
- `results` has one entry per model **in run order**: the pipeline's step order, or the order you listed `models`. Results of models that have not started are `QUEUED`.
- A `Classification` returns the **top 5 predictions unioned with any label scoring above 0.90**, so a confident sixth label is never dropped (models with a `score_threshold` return the labels at or above it instead).
- Each prediction has the label's `id` (a class index for `vit`, a label‑list index for `siglip` with `labels`, a taxonomy child id for `siglip` with a taxonomy, a category id for `pdq`) and its `name` (e.g. `nsfw`, or `food_and_drink.drinks` for a taxonomy child). `ListModels` lists every label a model can predict.
- For `siglip`, `score` is an independent **sigmoid probability**.
- Video results come back as a `Scan`: `aggregated` is the roll-up across the classified frames (the strategy's `max` or `mean`), `segments` the stretches where a trigger label fired, and `samples` the individual frames (`FULL` view only). A scan that stopped early (`early_exit`) has only the frames classified up to then.

### Segments

A segment is a stretch of the video where one of the model's trigger labels (`[models.<name>.early_exit] labels`) scored at or above its `threshold` on consecutive sampled frames, in time order, with no frame below the threshold in between. With the strategy's `confirm` set, a frame only counts once confirmed, the same rule that decides the aggregated result. Models without `early_exit` produce no segments.

`start`/`end` are the first and last flagged frame. Frames are only sampled every so often, so the content may extend further; `bound_start`/`bound_end` are the nearest frames on either side that were checked and found clear (or the start/end of the video), the furthest it can extend.

### `ResultView`

| Value | Scans contain |
|-------|---------------|
| `RESULT_VIEW_SUMMARY` (default, also `UNSPECIFIED`) | `aggregated`, `segments`, `sample_count`. |
| `RESULT_VIEW_FULL` | Everything, including every classified sample. |

A dense scan of a long video can hold thousands of samples, so ask for `FULL` only when you need them. `ListTasks` and webhooks always use the summary.

### `Error` — structured failures

```protobuf
message Error {
  ErrorType kind = 1;    // machine‑readable category
  string message = 2;    // human‑readable detail (always populated)
}

enum ErrorType {
  ERROR_TYPE_UNSPECIFIED       = 0;   // uncategorized / custom (see message)
  ERROR_TYPE_FETCH             = 1;   // the input could not be fetched
  ERROR_TYPE_DECODE            = 2;   // fetched, but could not be decoded
  ERROR_TYPE_INFERENCE         = 3;   // a model failed during inference
  ERROR_TYPE_TIMEOUT           = 4;   // an operation exceeded its deadline
  ERROR_TYPE_CANCELLED         = 5;   // the task was cancelled
  ERROR_TYPE_MODEL_UNAVAILABLE = 6;   // a model could not be loaded
  ERROR_TYPE_INTERNAL          = 7;   // an unexpected server error
}
```

When each kind is used: `FETCH` — the input could not be retrieved (unreachable, refused by the SSRF / local‑file policy, HTTP error); `DECODE` — it was retrieved but could not be decoded or probed, or exceeds `max_pixels` / `max_video_seconds`; `INFERENCE` — the model failed on it; `TIMEOUT` — one classification exceeded the model's `timeout`; `MODEL_UNAVAILABLE` — the model's weights could not be loaded; `INTERNAL` — anything else (e.g. a task abandoned after repeated crashes).

`Error` appears at two levels: on the `Task` (a task‑wide failure such as an unfetchable input) and on a `ModelResult` (that model's failure). Switch on `kind` for programmatic handling; `message` carries the detail.

---

## States

```protobuf
enum TaskState {
  TASK_STATE_UNSPECIFIED = 0;
  TASK_STATE_QUEUED      = 1;
  TASK_STATE_PROCESSING  = 2;   // running, or waiting to retry
  TASK_STATE_COMPLETED   = 3;   // every model attempted (check each result)
  TASK_STATE_FAILED      = 4;   // the task as a whole failed; see Task.error
  TASK_STATE_CANCELLED   = 5;
}

enum ModelState {
  MODEL_STATE_UNSPECIFIED = 0;
  MODEL_STATE_QUEUED      = 1;
  MODEL_STATE_PROCESSING  = 2;
  MODEL_STATE_COMPLETED   = 3;  // `output` is set
  MODEL_STATE_FAILED      = 4;  // `error` is set
  MODEL_STATE_SKIPPED     = 5;  // an earlier pipeline step's gate fired
  MODEL_STATE_CANCELLED   = 6;  // the task was cancelled before it finished
}
```

---

## Model discovery

```protobuf
message ListModelsResponse { repeated ModelInfo models = 1; repeated PipelineInfo pipelines = 2; }

message ModelInfo {
  string name = 1;  string architecture = 2;  string repo = 3;
  repeated MediaKind accepts = 4;           // IMAGE, plus VIDEO with a video strategy
  bool enabled = 5;  bool loaded = 6;       // loaded = weights in memory now
  repeated Label labels = 7;                // empty if not readable offline
  Trigger trigger = 8;                      // the model's early_exit, if any
  string video_strategy = 9;
}
message Label   { uint32 id = 1; string name = 2; }
message Trigger { repeated uint32 labels = 1; float threshold = 2; }

message PipelineInfo     { string name = 1; repeated PipelineStepInfo steps = 2; }  // in run order
message PipelineStepInfo { repeated string models = 1; StopIf stop_if = 2; }
message StopIf           { string model = 1; Trigger trigger = 2; }  // the model it checks
```

---

## Modality rules

The input's kind fixes which models are valid for it. Invalid combinations are rejected synchronously:

| Input | Valid models |
|-------|--------------|
| image | any image model (`vit`, `siglip`, `pdq`) |
| video | any image classifier that has a `video_strategy` configured (it is run over sampled frames) |
| audio | none yet |

Runtime failures (an unfetchable URL, a decode error, an inference error, a timeout) are **not** rejected up front; they are reported on the task and its model results.

---

## Error → gRPC status mapping

| Condition | gRPC status |
|-----------|-------------|
| Missing `media` / `run`, unknown/disabled/duplicate model, incompatible input, unknown pipeline, metadata or `wait_seconds` over its limit, bad `page_token` | `INVALID_ARGUMENT` |
| Unknown task id, or a task that belongs to another caller | `NOT_FOUND` |
| Backpressure (queue full or memory ceiling exceeded), content over the upload cap | `RESOURCE_EXHAUSTED` |
| Missing/invalid auth token (when auth is enabled) | `UNAUTHENTICATED` |
| Everything else | `INTERNAL` |

---

## Authentication

By default every `Inference` RPC must carry a valid **PASETO v4** token; health and reflection remain open. (Set `[auth] enabled = false` to turn this off on a trusted network.)

- Mint tokens with `apollo token --subject NAME [--expires 30d]`. Without `[auth].public_key` the server uses a local keypair it creates on first start, which `apollo token` reads; or generate one with `apollo keygen` and configure its public key.
- Clients send the token in the **`authorization`** metadata header, with or without a `Bearer ` prefix.
- The server verifies the signature and any `iat`/`nbf`/`exp` claims; non‑expiring tokens are accepted (revoke by rotating the keypair).
- With `apollo-client`, pass the token via `Client::builder(url).token("v4.public.…")`.
- A missing, invalid, or **revoked** token is rejected with `UNAUTHENTICATED`. Tokens carry an id; `apollo revoke --id …` (or `--subject …`) revokes them without rotating the keypair, effective within ~10 s.
- Tasks belong to the token subject that submitted them: `GetTask`, `WatchTask` and `CancelTask` by any other subject return `NOT_FOUND` (as for an unknown id), and `ListTasks` lists only the caller's own tasks, unless the subject is listed in `[auth].admin_subjects`.

See [configuration.md → Authentication](./configuration.md#auth--authentication) for the full setup.

---

## Examples (grpcurl)

Reflection is enabled, so no `-import-path`/`-proto` flags are needed. Authentication is on by default, so every call carries a token from `apollo token`.

```bash
TOKEN=$(apollo token --subject grpcurl --expires 1h)

# Classify an image with two models and wait up to 30 s for the result.
grpcurl -plaintext -H "authorization: $TOKEN" \
  -d '{"media": {"kind": "MEDIA_KIND_IMAGE",
                 "url": "https://example.com/cat.jpg",
                 "fallback_url": "file:///tmp/cat.jpg"},
       "models": {"names": ["general", "nsfw"]},
       "metadata": {"ref": "upload-42"},
       "request_id": "upload-42",
       "wait_seconds": 30}' \
  localhost:8080 apollo.v1.Inference/Classify

# Classify a video through a pipeline; returns the queued task at once.
grpcurl -plaintext -H "authorization: $TOKEN" \
  -d '{"media": {"kind": "MEDIA_KIND_VIDEO", "url": "https://example.com/clip.mp4"},
       "pipeline": "moderation"}' \
  localhost:8080 apollo.v1.Inference/Classify

# Send a small image inline (grpcurl takes bytes as base64).
grpcurl -plaintext -H "authorization: $TOKEN" \
  -d "{\"media\": {\"kind\": \"MEDIA_KIND_IMAGE\", \"data\": \"$(base64 < cat.jpg)\"},
       \"models\": {\"names\": [\"general\"]}, \"wait_seconds\": 30}" \
  localhost:8080 apollo.v1.Inference/Classify

# Follow a task until it finishes, with every classified frame.
grpcurl -plaintext -H "authorization: $TOKEN" \
  -d '{"task_id": "6f1c…", "view": "RESULT_VIEW_FULL"}' \
  localhost:8080 apollo.v1.Inference/WatchTask

# Poll once instead.
grpcurl -plaintext -H "authorization: $TOKEN" -d '{"task_id": "6f1c…"}' \
  localhost:8080 apollo.v1.Inference/GetTask

# Your failed tasks of the last day.
grpcurl -plaintext -H "authorization: $TOKEN" \
  -d "{\"states\": [\"TASK_STATE_FAILED\"], \"created_after\": \"$(date -u -v-1d +%FT%TZ)\"}" \
  localhost:8080 apollo.v1.Inference/ListTasks

# Cancel a task.
grpcurl -plaintext -H "authorization: $TOKEN" -d '{"task_id": "6f1c…"}' \
  localhost:8080 apollo.v1.Inference/CancelTask

# What can I run?
grpcurl -plaintext -H "authorization: $TOKEN" localhost:8080 apollo.v1.Inference/ListModels
```

(`date -v-1d` is the macOS form; on Linux use `date -u -d '1 day ago' +%FT%TZ`.)

`ClassifyStream` is a client‑streaming RPC (init frame, then data frames); it is normally driven from the generated client (`apollo-client`, which also connects to `https://` endpoints over TLS). Its `Client::classify_stream` takes a `StreamInit` (the media kind, models or a pipeline, and the optional fields) and an async stream of byte chunks, so you can feed a large file or live source incrementally; `Client::classify_stream_bytes` is the convenience for content already in memory.
