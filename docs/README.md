# Apollo

Apollo is a gRPC service that runs machine‑learning **classification models over images, video and text**. You submit an input (an image or video URL, its bytes inline, or a stream of raw bytes) together with the names of the models to run on it; Apollo returns the **task** immediately and does the work asynchronously. You then wait for it in the same call, watch it, poll it, or receive it over a webhook.

Models are loaded from [Hugging Face](https://huggingface.co/) and executed with the [candle](https://github.com/huggingface/candle) inference framework. The service is built to be operated as a long‑running daemon: task state is fully persisted, so work survives restarts, and interrupted video scans resume from where they stopped.

---

## What it does, concretely

- **Classify an image.** Run one or more image classifiers on a picture and get back scored labels.
- **Classify text.** Text classifiers (BERT, DistilBERT, XLM-RoBERTa, ModernBERT, DeBERTa) score toxicity, sentiment, language, prompt injection, emotions — whatever a Hugging Face checkpoint was fine-tuned for. Long text is split into overlapping windows.
- **Match known images.** A `pdq` model compares perceptual hashes against a block list, so re-encoded or resized copies of listed images are caught, in images and in video frames.
- **Classify a video.** Any image classifier can be pointed at a video; Apollo samples frames according to a named **strategy**, classifies each frame, and rolls the per‑frame scores up into one result, with the **segments** where a trigger label fired. Scans can **early‑exit** as soon as a trigger label fires.
- **Run ordered pipelines.** Instead of running models as a parallel set, an input can be sent through a named **pipeline** whose steps run in order (each step's models concurrently) and can **gate** (stop early) based on a previous step's output — e.g. "run the NSFW model first; only run the category and general models if it's clean."
- **Skip repeated work.** An optional result **cache** keys model outputs by content hash, so identical inputs don't get re‑inferred, and a `request_id` makes a retried submission return the existing task.
- **Follow a task.** `Classify` can wait for the result (`wait_seconds`), `WatchTask` streams each change, and an optional **webhook** sends a `TaskEvent` as the task moves on and once it finishes.
- **Discover what to run.** `ListModels` lists the configured models with their labels, and the pipelines; `ListTasks` lists your tasks.
- **Operate it.** Prometheus metrics on `/metrics`, config changes applied without a restart (`apollo reload`), and thresholds chosen from labelled examples (`apollo calibrate`).

## Core concepts

| Term | Meaning |
|------|---------|
| **Task** | One submission: one input, the models (or the pipeline) to run on it, and their results. Created by `Classify`/`ClassifyStream`. (Internally a task holds one **item**, the input and its per‑model results.) |
| **Model** | A registered `[models.<label>]` entry: an architecture + a Hugging Face repo + options. Referenced by its **label**, not its repo. |
| **Architecture** | The model family. Image models: **`vit`** (a fixed‑head image classifier whose labels come from the weights), **`siglip`** (an open‑vocabulary classifier scored against a caller‑supplied label list or a **taxonomy**), and **`pdq`** (perceptual hashes matched against a **block list** of known images). Text models: **`bert`**, **`distilbert`**, **`xlm_roberta`**, **`modernbert`**, **`deberta_v2`**. |
| **Strategy** | A reusable `[strategies.<name>]` recipe describing how an image classifier is applied to a video: which frames to sample, how to aggregate per‑frame scores (`max`/`mean`), and whether to early‑exit. |
| **Pipeline** | A reusable `[pipelines.<name>]` sequence of steps run in order (a step's models concurrently), with optional per‑step gates. |
| **Prediction** | A `(label, name, score)` triple. `label` is an integer id (a class index for `vit` and text models, a label‑list index or taxonomy child id for `siglip`, a category id for `pdq`), `name` its readable name. |
| **Segment** | A stretch of a video where one of a model's trigger labels scored at or above its threshold on consecutive sampled frames. |

There is **no separate video architecture** — video is always "an image classifier run over sampled frames."

## How it works

```
                         ┌──────────────────────────────────────────────┐
   gRPC client ─────────▶│  apollo-server   (Inference service)         │
   (Classify, GetTask,   │   • auth interceptor (PASETO, optional)      │
    WatchTask, List…,    │   • proto ⇄ domain conversion                │
    CancelTask)          └───────────────────────┬──────────────────────┘
                                                 │ submit / query
                                                 ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │  apollo-engine   (async orchestration core)                           │
   │   queue      – submission, backpressure, startup recovery, retention  │
   │   scheduler  – per‑task dispatch: fetch‑once, fan‑out, concurrency cap │
   │   worker     – one dedicated OS thread per model: batching + idle‑     │
   │                unload of weights                                       │
   │   registry   – loaded‑model registry + submit‑time validation         │
   │   aggregate  – assembles results, segments and lifecycle state        │
   │   watch      – announces task changes (WatchTask, wait_seconds)       │
   │   webhook    – sends task events via an injected sink                 │
   │   live       – the reloadable config snapshot, config reload          │
   │   metrics    – Prometheus counters, histograms and the text format     │
   └───────┬───────────────────────────┬───────────────────────┬───────────┘
           ▼                           ▼                       ▼
   ┌───────────────┐          ┌────────────────┐      ┌──────────────────┐
   │ apollo-storage│          │ apollo-inference│      │  apollo-media    │
   │ sqlite/surreal│          │  candle (vit,   │      │ fetch, decode,   │
   │ (persisted    │          │  siglip)        │      │ probe, sample,   │
   │  lifecycle +  │          │                 │      │ extract frames   │
   │  cache)       │          └────────────────┘      └──────────────────┘
   └───────────────┘
```

**Request lifecycle:**

1. A `Classify` call is validated **synchronously** — unknown/disabled models and input/model modality mismatches are rejected before anything is queued (mapped to gRPC `InvalidArgument`). The task is persisted and returned (after up to `wait_seconds`, if the caller asked to wait).
2. The scheduler fetches the input **once**, then fans it out to each model's worker — a task's models (or a pipeline step's) run **concurrently**. A global concurrency cap (a priority‑ordered gate) bounds how many inputs are processed at once (your VRAM budget).
3. Each model runs on its own dedicated worker thread that **lazily loads** weights on first use, **batches** concurrent requests into one forward pass, and **unloads** after an idle timeout (unless the model is pinned with `keep_in_memory`).
4. Results are written per model as they complete. For video, frames are extracted in batches by a single ffmpeg pass (scaled to the model's input size, the next batch extracted while the current one is classified), and each classified batch is checkpointed so an interrupted scan resumes without re‑doing frames.
5. When every model is done, the task reaches a terminal state, the webhook (if configured) fires, watchers get the final task, and `GetTask` reflects the final result. Transient failures (network errors, model timeouts) are **retried** up to `[app].max_retries`; permanent ones (a refused URL, undecodable input) fail immediately.

On startup the engine **recovers** any tasks left non‑terminal by a previous run and re‑queues them; a poison task that keeps crashing is failed once its resume‑attempt count exceeds the cap.

## Workspace layout

Apollo is a Cargo workspace of small crates:

| Crate | Responsibility |
|-------|----------------|
| `apollo-domain` | Shared runtime types (`Task`, `Item`, `ModelResult`, `Classification`, enums). Wire‑free. |
| `apollo-config` | TOML config schema, defaults, validation, and format‑preserving edits. |
| `apollo-proto` | Generated protobuf/gRPC types + reflection descriptor. |
| `apollo-storage` | The `Storage` trait and its SQLite / SurrealDB backends. |
| `apollo-inference` | candle model loading + inference (`vit`, `siglip`, and the text models), and PDQ hashing (`pdq`). |
| `apollo-media` | Fetching, decoding, probing, and frame sampling/extraction. |
| `apollo-engine` | Async orchestration (queue, scheduler, workers, webhook). |
| `apollo-server` | The gRPC surface: `Inference` service, auth, proto conversion, webhook client. |
| `apollo-client` | A convenience gRPC client (re‑exports the wire types). |
| `apollo-app` | The `apollo` binary and its CLI. |

## Quick start

```bash
# 1. Create a config (writes ~/.config/apollo/config.toml on Linux).
apollo config set app.port 8080
apollo config set models.general.architecture vit
apollo config set models.general.repo google/vit-base-patch16-224

# 2. Start the server (add --daemon to detach). Authentication is on by default:
#    the first start creates a signing keypair in the server's state directory.
apollo start --daemon               # or: apollo start --config ./config.toml --port 8080

# 3. Mint a token for your client.
TOKEN=$(apollo token --subject my-client --expires 30d)

# 4. Check it is up (models, queue, tasks, memory).
apollo status

# 5. Submit an image and wait for the result (reflection is enabled, so
#    grpcurl needs no .proto files).
grpcurl -plaintext \
  -H "authorization: $TOKEN" \
  -d '{"media": {"kind": "MEDIA_KIND_IMAGE", "url": "https://example.com/cat.jpg"},
       "models": {"names": ["general"]}, "wait_seconds": 30}' \
  localhost:8080 apollo.v1.Inference/Classify
# → {"id": "…", "state": "TASK_STATE_COMPLETED", "results": [{"model": "general", …}], …}

# Or submit without waiting and poll.
grpcurl -plaintext -H "authorization: $TOKEN" -d '{"task_id": "…"}' localhost:8080 apollo.v1.Inference/GetTask
```

The server exits at startup if **no models are configured** — there would be nothing to serve.

## Documentation index

| Document | Covers |
|----------|--------|
| [grpc-api.md](./grpc-api.md) | The `Inference` service: methods, messages, states, segments, error codes, auth, and examples. |
| [webhooks.md](./webhooks.md) | The `Webhook` service you implement to receive task events, delivery semantics, and payload signing. |
| [configuration.md](./configuration.md) | Every config section and field, defaults, units, validation, and the CLI. |
| [operations.md](./operations.md) | Prometheus metrics, live config reload, and threshold calibration. |
| [database.md](./database.md) | Persistence: backends, schema, migrations, retention, and the result cache. |
