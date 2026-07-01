# Apollo

Apollo is a gRPC service that runs machine‑learning **classification models over images and video**. You submit an input (an image or video URL, or a stream of raw bytes) together with the names of the models to run on it; Apollo returns a **task id** immediately and does the work asynchronously. You then poll for the result or receive it over a webhook.

Models are loaded from [Hugging Face](https://huggingface.co/) and executed with the [candle](https://github.com/huggingface/candle) inference framework. The service is built to be operated as a long‑running daemon: task state is fully persisted, so work survives restarts, and interrupted video scans resume from where they stopped.

---

## What it does, concretely

- **Classify an image.** Run one or more image classifiers on a picture and get back scored labels.
- **Classify a video.** Any image classifier can be pointed at a video; Apollo samples frames according to a named **strategy**, classifies each frame, and rolls the per‑frame scores up into one result. Scans can **early‑exit** as soon as a trigger label fires.
- **Run ordered pipelines.** Instead of running models as a parallel set, an input can be sent through a named **pipeline** whose steps run in order and can **gate** (stop early) based on a previous step's output — e.g. "run the NSFW model first; only run the expensive category model if it's clean."
- **Skip repeated work.** An optional result **cache** keys model outputs by content hash, so identical inputs don't get re‑inferred.
- **Notify on completion.** An optional **webhook** fires as each item reaches a terminal state, with a dead‑letter signal for items that exhaust their retries.

## Core concepts

| Term | Meaning |
|------|---------|
| **Task** | One submission. A `Classify`/`ClassifyStream` call creates a task with exactly one **item**. |
| **Item** | One input within a task, plus the set of models (or the pipeline) to run on it. |
| **Model** | A registered `[models.<label>]` entry: an architecture + a Hugging Face repo + options. Referenced by its **label**, not its repo. |
| **Architecture** | The model family. Two are supported: **`vit`** (a fixed‑head image classifier whose labels come from the weights) and **`siglip`** (an open‑vocabulary classifier scored against a caller‑supplied label list or a **taxonomy**). |
| **Strategy** | A reusable `[strategies.<name>]` recipe describing how an image classifier is applied to a video: which frames to sample, how to aggregate per‑frame scores (`max`/`mean`), and whether to early‑exit. |
| **Pipeline** | A reusable `[pipelines.<name>]` sequence of models run in order, with optional per‑step gates. |
| **Prediction** | A `(label, score)` pair. `label` is an integer id (a class index for `vit`, a label‑list index or taxonomy child id for `siglip`). |

There is **no separate video architecture** — video is always "an image classifier run over sampled frames."

## How it works

```
                         ┌──────────────────────────────────────────────┐
   gRPC client ─────────▶│  apollo-server   (Inference service)         │
   (Classify /           │   • auth interceptor (PASETO, optional)      │
    GetTask / Cancel /   │   • proto ⇄ domain conversion                │
    ClassifyStream)      └───────────────────────┬──────────────────────┘
                                                 │ submit / query
                                                 ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │  apollo-engine   (async orchestration core)                           │
   │   queue      – submission, backpressure, startup recovery, retention  │
   │   scheduler  – per‑task dispatch: fetch‑once, fan‑out, concurrency cap │
   │   worker     – one dedicated OS thread per model: batching + idle‑     │
   │                unload of weights                                       │
   │   registry   – loaded‑model registry + submit‑time validation         │
   │   aggregate  – assembles results and lifecycle state                  │
   │   webhook    – fires terminal‑item deliveries via an injected sink     │
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

1. A `Classify` call is validated **synchronously** — unknown/disabled models and input/model modality mismatches are rejected before anything is queued (mapped to gRPC `InvalidArgument`). The task is persisted and a task id is returned.
2. The scheduler fetches the input **once**, then fans it out to each model's worker. A global concurrency cap (a priority‑ordered gate) bounds how many inferences run at once (your VRAM budget).
3. Each model runs on its own dedicated worker thread that **lazily loads** weights on first use, **batches** concurrent requests into one forward pass, and **unloads** after an idle timeout (unless the model is pinned with `keep_in_memory`).
4. Results are written per `(item, model)` as they complete. For video, each classified frame is checkpointed so an interrupted scan resumes without re‑doing frames.
5. When every model for an item is done, the item reaches a terminal state, the webhook (if configured) fires, and `GetTask` reflects the final result. A failing item is **retried** up to `[app].max_retries`, then **dead‑lettered** (the `ItemFailed` webhook).

On startup the engine **recovers** any tasks left non‑terminal by a previous run and re‑queues them; a poison task that keeps crashing is failed once its resume‑attempt count exceeds the cap.

## Workspace layout

Apollo is a Cargo workspace of small crates:

| Crate | Responsibility |
|-------|----------------|
| `apollo-domain` | Shared runtime types (`Task`, `Item`, `ModelResult`, `Classification`, enums). Wire‑free. |
| `apollo-config` | TOML config schema, defaults, validation, and format‑preserving edits. |
| `apollo-proto` | Generated protobuf/gRPC types + reflection descriptor. |
| `apollo-storage` | The `Storage` trait and its SQLite / SurrealDB backends. |
| `apollo-inference` | candle model loading + inference (`vit`, `siglip`). |
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

# 2. (Optional) enable authentication.
apollo keygen                       # prints public_key + secret_key
apollo config set auth.public_key "k4.public.…"
export APOLLO_SECRET_KEY="k4.secret.…"
apollo token --subject my-client --expires 30d   # prints an API token

# 3. Start the server (foreground; add --daemon to detach).
apollo start                        # or: apollo start --config ./config.toml --port 8080

# 4. Submit an image (reflection is enabled, so grpcurl needs no .proto files).
grpcurl -plaintext \
  -H "authorization: <token>" \
  -d '{"item": {"models": ["general"], "image_url": {"main": "https://example.com/cat.jpg"}}}' \
  localhost:8080 apollo.v1.Inference/Classify
# → {"taskId": "…"}

grpcurl -plaintext -d '{"task_id": "…"}' localhost:8080 apollo.v1.Inference/GetTask
```

The server exits at startup if **no models are configured** — there would be nothing to serve.

## Documentation index

| Document | Covers |
|----------|--------|
| [grpc-api.md](./grpc-api.md) | The `Inference` service: methods, messages, enums, error codes, auth, and examples. |
| [webhooks.md](./webhooks.md) | The `Webhook` service you implement to receive results, delivery semantics, and HMAC signing. |
| [configuration.md](./configuration.md) | Every config section and field, defaults, units, validation, and the CLI. |
| [database.md](./database.md) | Persistence: backends, schema, migrations, retention, and the result cache. |
