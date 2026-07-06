# gRPC API — the `Inference` service

Apollo exposes one gRPC service that clients call: **`apollo.v1.Inference`**. It is defined in `proto/inference.proto` and shares its message types with `proto/common.proto`.

- **Transport:** gRPC over HTTP/2.
- **TLS:** terminated at the transport (use `https://`/a TLS proxy in front); the examples below use `-plaintext` for a local server.
- **Server reflection** is enabled, so tools like `grpcurl` and Postman can discover the schema without a local copy of the `.proto` files.
- **Health checking** (`grpc.health.v1.Health`) is enabled; the server reports the `Inference` service as `SERVING`.
- **Authentication** is optional and, when enabled, applies to every `Inference` RPC (health and reflection stay open). See [Authentication](#authentication).

The service is asynchronous: `Classify`/`ClassifyStream` return a task id right away, and you retrieve results with `GetTask`.

---

## Methods

| RPC | Request | Response | Purpose |
|-----|---------|----------|---------|
| `Classify` | `ClassifyRequest` | `TaskCreated` | Submit one input by URL. Returns a task id immediately. |
| `GetTask` | `GetTaskRequest` | `Task` | Poll task state + results. Backed by the database, so it survives restarts. |
| `CancelTask` | `CancelRequest` | `Task` | Request cooperative cancellation; returns the task's state after the request. |
| `ClassifyStream` | stream of `ClassifyChunk` | `TaskCreated` | Submit one input as raw content bytes (no URL fetch). |

### `Classify(ClassifyRequest) → TaskCreated`

Submits a single input. Validation happens **before** the task is created:

- every model label must exist and be enabled, and
- every label must be compatible with the input's modality (see [modality rules](#modality-rules)).

A validation failure returns `InvalidArgument` and no task is created. On success the task is persisted and its id is returned.

### `GetTask(GetTaskRequest) → Task`

Returns the full current `Task` — its `result` oneof carries a task‑level `error`, a live `state`, or the per‑model `models` once finished. Returns `NotFound` if the id is unknown. Because task state is persisted, this works across server restarts and for finished tasks (until they are purged by [retention](./database.md#retention)).

### `CancelTask(CancelRequest) → Task`

Requests cooperative cancellation and returns the task's state **after** the request — `CANCELLED` unless it had already finished. In‑flight work stops at the next checkpoint: between models, or between sampled video frames. Returns `NotFound` for an unknown id.

### `ClassifyStream(stream ClassifyChunk) → TaskCreated`

Submits one input as a byte stream instead of a URL — useful for local content you don't want to (or can't) expose over HTTP.

- The **first** message MUST be the `init` frame (`ClassifyStreamInit`): the models **or** a `pipeline` to run (set one, exactly as with `Classify`), and whether the bytes are a `video` (`true`) or a single image (`false`).
- Every subsequent message carries `data` bytes, **in order**. Send **as many `data` frames as you like** — keep streaming until you close the request; you don't have to send the whole input in one frame. The server appends each frame to the staging file as it arrives, so neither side needs to hold the entire input in memory.

The server streams the bytes to a staging file, enforcing the upload byte cap (`[limits].max_download`) **incrementally** as frames arrive, then submits it exactly like `Classify`. A second `init`, a stream with no data, an `init` frame that sets neither `models` nor `pipeline`, or exceeding the cap is rejected (`InvalidArgument` / `ResourceExhausted`) and the staging file is cleaned up. The staged file is removed automatically once the task reaches a terminal state.

---

## Messages

### Request messages

```protobuf
message ClassifyRequest { InputItem item = 1; }
message GetTaskRequest  { string task_id = 1; }
message CancelRequest   { string task_id = 1; }
message TaskCreated     { string task_id = 1; }

// For ClassifyStream:
message ClassifyStreamInit {
  repeated string models   = 1;   // model labels to run (set this or `pipeline`)
  bool            video    = 2;   // true = video bytes, false = a single image
  optional string pipeline = 3;   // run a named pipeline instead of `models`
}
message ClassifyChunk {
  oneof payload {
    ClassifyStreamInit init = 1;   // required, first message
    bytes              data = 2;   // content bytes, in order
  }
}
```

### `InputItem` — what to classify

```protobuf
message Url {                       // a content reference
  string main = 1;                  // local path, file://, or http(s)://
  optional string fallback = 2;     // tried only if `main` fails
}

message InputItem {
  repeated string models = 1;       // model labels to run (parallel set)

  oneof input {
    Url    image_url = 2;
    Url    video_url = 3;
    string text      = 4;           // inline content (future)
    Url    audio_url = 5;           // (future)
  }

  optional string pipeline = 6;     // run a named [pipelines.<name>] instead of `models`
}
```

Notes:

- `Url` is **not** a oneof — `main` and `fallback` coexist; `fallback` is only fetched if `main` cannot be fetched or decoded.
- Set **either** `models` (run them as a parallel set) **or** `pipeline` (run a named ordered pipeline). At least one must be set. When both are present the pipeline is used.
- `text` and `audio_url` are reserved for future modalities.

### Result messages

```protobuf
message Task {
  string id = 1;

  reserved 2 to 99;                         // previous layout (state/items) — retired

  oneof result {
    Error     error  = 100;                 // task‑level failure (implicit "failed")
    TaskState state  = 101;                 // non‑terminal or cancelled
    Models    models = 102;                 // per‑model results (implicit "completed")
  }
}

message Models {
  optional string pipeline = 1;             // the pipeline the input ran through, if any
  map<string, Model> models = 2;            // keyed by model label
}

message Model {
  oneof result {
    Error          error          = 1;      // this model failed (implicit "failed")
    ModelState     state          = 2;      // still queued / processing, or skipped
    Classification classification = 3;      // image input (implicit "done")
    FrameScan      frame_scan     = 4;      // classifier over a video (implicit "done")
  }
}

message Classification {
  repeated Prediction predictions = 1;      // top 5 ∪ any label scoring > 0.90
}

message FrameScan {
  Classification aggregated = 1;            // per‑frame scores rolled up (max/mean)
  repeated Frame frames = 2;                // frames actually classified (early exit truncates)
}

message Frame {
  double timestamp = 1;                     // seconds into the video
  uint32 index     = 2;                     // ordinal among sampled frames
  Classification classification = 3;
}

message Prediction { uint32 label = 1; float score = 2; }
```

**Result semantics:**

- A `Task` reports its outcome through a single `result` oneof: `error` for a task‑level failure, `state` while it is queued / processing (or cancelled), or `models` once it finishes. The presence of `models` is itself the "completed" signal and `error` the "failed" signal — there is no explicit `COMPLETED`/`FAILED` state. Each `Model` likewise carries exactly one of an `error`, a live `state`, or a result payload (`classification` / `frame_scan`).
- A `Classification` returns the **top 5 predictions unioned with any label scoring above 0.90** — so a confident sixth label is never dropped, and you always get at least the top few.
- `label` is an integer id, never a name: a class index for `vit`, a label‑list index for `siglip` with a plain `labels` list, or a **taxonomy child‑category id** for `siglip` with a taxonomy. Map ids back to names using your `labels`/taxonomy definition.
- For `siglip`, `score` is an independent **sigmoid probability**; the `score_threshold` on the model controls which labels are kept.
- Video results come back as a `FrameScan`: `aggregated` is the roll‑up across classified frames (using the strategy's `max`/`mean` aggregation), and `frames` are the individual classified frames (truncated if the scan early‑exited).
- One model failing does **not** sink the others — that model's `Model.result` is an `error`; sibling models still report their `classification` / `frame_scan`.

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

`Error` appears at two levels: on `Task.result` (a task‑wide failure such as an unfetchable input) and on `Model.result` (a single model's failure). Switch on `kind` for programmatic handling; `message` carries the detail (for a purely ad‑hoc error, `kind` is `UNSPECIFIED` and everything is in `message`).

---

## Enums (lifecycle states)

```protobuf
enum TaskState {
  TASK_STATE_UNSPECIFIED = 0;
  TASK_STATE_QUEUED      = 1;
  TASK_STATE_PROCESSING  = 2;
  reserved 3, 4;                 // was COMPLETED / FAILED — now implicit (see Task.result)
  TASK_STATE_CANCELLED   = 5;
}

enum ModelState {
  MODEL_STATE_UNSPECIFIED = 0;
  MODEL_STATE_QUEUED      = 1;
  MODEL_STATE_PROCESSING  = 2;
  reserved 3, 4;                 // was DONE / FAILED — now implicit (see Model.result)
  MODEL_STATE_SKIPPED     = 5;   // skipped because an earlier pipeline gate fired
}
```

There is no `COMPLETED`/`FAILED` state: a finished task carries `models` in its `result` (completed) or `error` (failed), and `state` appears only while it is queued / processing or was cancelled. Completion means every model was **attempted** — not that every model succeeded, so inspect each `Model.result` for per‑model outcomes. The same pattern applies to `Model`: a finished model carries a result payload or an `error`, and `ModelState` conveys only queued / processing / skipped.

---

## Modality rules

The input's modality fixes which models are valid for it. Invalid combinations are rejected synchronously by `Classify`:

| Input | Valid models |
|-------|--------------|
| image | any image classifier (`vit`, `siglip`) |
| video | any image classifier that has a `video_strategy` configured (it is run over sampled frames) |
| text / audio | models of that kind (future) |

Runtime failures — an unfetchable URL, a decode error, an inference error, a timeout — are **not** rejected up front; they are reported per item / per model in the result.

---

## Error → gRPC status mapping

Submit‑time validation failures are client errors; everything else is internal:

| Condition | gRPC status |
|-----------|-------------|
| Unknown/disabled model, incompatible input/model, bad config value | `INVALID_ARGUMENT` |
| Unknown task id (`GetTask`/`CancelTask`) | `NOT_FOUND` |
| Backpressure — queue full or memory ceiling exceeded | `RESOURCE_EXHAUSTED` |
| Missing/invalid auth token (when auth is enabled) | `UNAUTHENTICATED` |
| Everything else | `INTERNAL` |

---

## Authentication

When `[auth]` is configured, every `Inference` RPC must carry a valid **PASETO v4** token; health and reflection remain open.

- Generate a keypair with `apollo keygen`, put the **public** key in `[auth].public_key`, and mint tokens with `apollo token --subject NAME`.
- Clients send the token in the **`authorization`** metadata header, with or without a `Bearer ` prefix.
- The server verifies the signature and any `iat`/`nbf`/`exp` claims; non‑expiring tokens are accepted (revoke by rotating the keypair).

See [configuration.md → Authentication](./configuration.md#auth--authentication) for the full setup.

---

## Examples (grpcurl)

Reflection is enabled, so no `-import-path`/`-proto` flags are needed.

```bash
# Classify an image with two models.
grpcurl -plaintext \
  -d '{"item": {"models": ["general", "nsfw"],
                "image_url": {"main": "https://example.com/cat.jpg",
                              "fallback": "file:///tmp/cat.jpg"}}}' \
  localhost:8080 apollo.v1.Inference/Classify

# Classify a video through a pipeline instead of a model set.
grpcurl -plaintext \
  -d '{"item": {"pipeline": "moderation",
                "video_url": {"main": "https://example.com/clip.mp4"}}}' \
  localhost:8080 apollo.v1.Inference/Classify

# Poll for the result.
grpcurl -plaintext -d '{"task_id": "01H…"}' \
  localhost:8080 apollo.v1.Inference/GetTask

# Cancel a task.
grpcurl -plaintext -d '{"task_id": "01H…"}' \
  localhost:8080 apollo.v1.Inference/CancelTask

# With authentication enabled:
grpcurl -plaintext -H "authorization: v4.public.…" \
  -d '{"task_id": "01H…"}' localhost:8080 apollo.v1.Inference/GetTask
```

`ClassifyStream` is a client‑streaming RPC (init frame, then data frames); it is normally driven from the generated client (`apollo-client`). Its `Client::classify_stream` takes a `StreamInit` (models or pipeline, plus `video`) and an async stream of byte chunks, so you can feed a large file or live source incrementally; `Client::classify_stream_bytes` is the convenience for content already in memory.
