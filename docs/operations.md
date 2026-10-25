# Operations — metrics, live reload, calibration

This page covers running Apollo day to day: scraping its metrics, changing its config without a restart, choosing thresholds from labelled examples, and the memory allocator Linux builds use. For the server's live state at a glance, see `apollo status` in [configuration.md → CLI reference](./configuration.md#cli-reference).

---

## Prometheus metrics

Turn on the scrape endpoint in the config:

```toml
[metrics]
enabled  = true
endpoint = "127.0.0.1"   # default; the endpoint has no authentication
port     = 9464          # default
```

Apollo then serves `GET /metrics` on that address in the Prometheus text format, on its own listener separate from the gRPC port. The section is read at startup; changing it needs a restart. Binding it to a non-loopback address logs a warning, since anyone who can reach it can read the metrics. Put it behind a firewall or a proxy if you need that.

```bash
curl -s http://127.0.0.1:9464/metrics
```

All metrics are prefixed `apollo_`. Counters start at zero when the server starts. Per-model metrics are kept by model name, so they keep counting when a reload replaces a model's worker.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `apollo_build_info` | gauge | `version` | Always 1. |
| `apollo_config_generation` | gauge | | Configs applied since start: 1 for the startup config, then one more per reload. |
| `apollo_config_loaded_timestamp_seconds` | gauge | | When the running config was applied. |
| `apollo_submissions_total` | counter | `outcome` | Submissions: `accepted`, `existing` (a repeated `request_id`), `invalid`, `overloaded` (backpressure), `error`. |
| `apollo_items_finished_total` | counter | `state` | Inputs that finished: `completed`, `failed`, `cancelled`. |
| `apollo_tasks` | gauge | `state` | Tasks in the database by state (until [retention](./database.md#retention) purges them). |
| `apollo_items_in_flight` | gauge | | Inputs accepted and not finished. |
| `apollo_max_pending` | gauge | | `[app].max_pending` (0 = no limit). |
| `apollo_slots` / `apollo_slots_busy` / `apollo_slots_waiting` | gauge | | `[app].max_concurrent`, the inputs being processed, and those waiting for a slot. |
| `apollo_memory_bytes` / `apollo_memory_limit_bytes` | gauge | | Memory in use (as for `max_memory`) and the cap, when known / set. |
| `apollo_fetch_bytes_total` | counter | | Bytes fetched for URL and local-file inputs (uploads are not fetches). |
| `apollo_fetch_seconds` | histogram | | Time to fetch an input. |
| `apollo_fetch_failures_total` | counter | `kind` | Failed fetches: `transient` (retried) or `permanent`. |
| `apollo_cache_hits_total` | counter | | Model results served from the result cache. |
| `apollo_webhooks_pending` | gauge | | Finished inputs whose webhook event is not delivered yet (with a webhook configured). |
| `apollo_webhook_deliveries_total` | counter | `event`, `result` | Delivery attempts: `progress` / `finished`, `ok` / `error`. |
| `apollo_model_loaded` | gauge | `model` | 1 while the model's weights are in memory. |
| `apollo_model_queued_jobs` | gauge | `model` | Requests waiting for the model's worker. |
| `apollo_model_inferences_total` | counter | `model` | Images (and video frames) classified. |
| `apollo_model_failures_total` | counter | `model` | Images whose forward pass failed. |
| `apollo_model_batch_size` | histogram | `model` | Images per forward pass. |
| `apollo_model_batch_seconds` | histogram | `model` | Duration of a forward pass. |
| `apollo_model_loads_total` | counter | `model` | Weight loads (an idle unload followed by use counts again). |
| `apollo_model_load_seconds` | histogram | `model` | Duration of a load, including download and warm-up. |
| `apollo_scans_total` | counter | `model` | Video scans finished. |
| `apollo_scan_frames_total` | counter | `model` | Video frames classified by scans. |
| `apollo_scan_early_exits_total` | counter | `model` | Scans that stopped early on a trigger. |

Some useful queries:

```promql
# Images per second, per model.
rate(apollo_model_inferences_total[5m])
# 95th-percentile forward-pass time.
histogram_quantile(0.95, sum by (le, model) (rate(apollo_model_batch_seconds_bucket[5m])))
# Average batch fill: low values on a busy server mean the batch size could be smaller.
rate(apollo_model_batch_size_sum[5m]) / rate(apollo_model_batch_size_count[5m])
# Share of submissions shed by backpressure.
rate(apollo_submissions_total{outcome="overloaded"}[5m]) / rate(apollo_submissions_total[5m])
```

---

## Live config reload

Edit the config file, then apply it to the running server:

```bash
apollo reload
```

or send the server process `SIGHUP` (`kill -HUP $(cat …/apollo.pid)`), which does the same without reporting back. `apollo reload` prints what changed:

```text
config reloaded (generation 3)
  models added: violence
  models reloaded (loads on next use): category
  models kept: general, nsfw
  restart to apply: app.port (the running values stay until then)
```

The new config is read the way `start` read it, with the same file and the same `--endpoint` / `--port` / `--webhook-url` overrides. It is validated first; an invalid config (or one without models) is rejected with its errors, and **the running config stays**. `apollo status` shows the generation and when the running config was applied.

**What applies at once:** thresholds and `early_exit`, strategies, pipelines, model `timeout` and `priority`, `[limits]`, `[cache]`, `[webhook]` (URL, secret, redelivery interval), `max_retries`, `max_pending`, `max_memory`, `idle_timeout`, `database.retention`, and `log_level` (unless `RUST_LOG` is set, which keeps winning).

**Models:** each model's worker is kept, loaded weights included, unless something its weights depend on changed: `architecture`, `repo`, `revision`, `labels`, `prompt_template`, `score_threshold`, `max_results`, `taxonomy_file` (or the taxonomy file's contents), `keep_in_memory` or `max_concurrent`. A model with such a change gets a new worker, which loads on first use (or at once with `keep_in_memory`). Added or newly enabled models get a worker. Removed or disabled models are rejected for new requests at once; their workers finish the work they have, then unload.

**Files a model reads** — a SigLIP taxonomy, a PDQ hash list — are read when the config is applied, at startup or on a reload; the model loads from that copy (also after an idle unload), so editing a file takes effect on the next reload, and a broken file is rejected with the config.

**Tasks already running** finish on the config they started with, so a scan is not switched to a different strategy halfway, and a pipeline keeps its steps. New submissions use the new config.

**Restart-only:** `app.endpoint`, `app.port`, `app.device`, `app.cache_dir`, `app.max_concurrent`, `[tls]`, `[auth]`, `[database]` (except `retention`) and `[metrics]`. A reload reports these and keeps their running values until the next restart.

The request goes over the same owner-only local socket as `apollo status`, so only the OS user running the server can reload it.

---

## Threshold calibration

`apollo calibrate` runs a model over labelled examples and reports its precision, recall, F1 and false-positive rate for each label at thresholds 0.05, 0.10, … 0.95. When a label's positive examples score below 0.05, the table also covers 0.0001 to 0.02. It shows the threshold with the best F1, and the lowest threshold that reaches a target precision (the most recall for that precision). Both are searched over the table's thresholds and every example's own score, so the cut-off between examples is found exactly rather than rounded to the table. For a `siglip` model, whose thresholds have at most 2 decimals, both are searched over every hundredth (0.01 … 0.99) instead, so a recommendation can always be written as it is.

```bash
apollo calibrate --model nsfw --data ./labelled --target-precision 0.95
apollo calibrate --model category --data ./set.jsonl --label food_and_drink.drinks --json
apollo calibrate --model nsfw --data ./labelled --write   # then: apollo reload
```

**Examples** come as a directory or a manifest:

- **A directory with one subdirectory per label**, named after the label (`nsfw`, `food_and_drink.drinks`) or its id. A subdirectory named `negative` (or `none` / `clean`) holds examples with none of the labels. Subdirectories inside a label's directory belong to it; other files are skipped.

  ```text
  labelled/
    nsfw/      a.jpg b.mp4 …
    negative/  c.jpg d.webm …
  ```

- **A JSONL manifest:** one `{"path": "...", "labels": [...]}` per line, labels as names or ids, an empty or missing list for a negative. Relative paths resolve from the manifest's directory. Blank lines and `#` comments are ignored.

Images (`jpg`, `png`, `webp`, `gif`, `bmp`, `tiff`), videos (`mp4`, `m4v`, `mov`, `mkv`, `webm`, `avi`) and texts (`txt`, UTF-8, for text models) are recognised by extension.

By default every label that appears in the data is reported, or the model's `early_exit` labels if the data has only negatives. `--label` (repeatable) picks labels instead. An example counts as positive for a label when it is listed under it; every other example is a negative for it.

**How it runs.** Calibration starts an engine in the same process, with a throwaway database, the model from your config and nothing else. Each example is classified exactly as the server would classify it: a video through the model's strategy, its score being the scan's aggregated result. A few settings change for the run, so that every score is seen:

- **Video scans run to the end:** the strategy's `early_exit` is turned off, since a scan stopping at the current threshold would hide higher scores. `confirm` and `refine` still use the current threshold.
- **Every SigLIP label is kept:** `score_threshold` is 0 and `max_results` is off, and a taxonomy's per-category thresholds are left out.
- **Labels the model doesn't return count as 0:** a ViT model returns its top five labels plus any above 0.90, so another label it scored lower counts as 0. This only matters when you calibrate a label outside a model's usual top five.

Failed examples (unreadable, too large, …) are listed and left out of the counts.

**`--write`** stores the recommended thresholds, keeping the files' comments and formatting:

- **Trigger labels:** for the model's `early_exit` labels, the highest of their recommended thresholds becomes `[models.<name>.early_exit].threshold`. One threshold covers all of a model's trigger labels, so the highest keeps each of them at the target precision. If any trigger label reaches no threshold at the target precision, nothing is written.
- **Taxonomy categories:** for a model with a `taxonomy_file`, each evaluated category with a recommendation gets its own `threshold` in the taxonomy file (see [taxonomy files](./configuration.md#taxonomy-files-siglip)).

Run `apollo reload` (or restart) to apply them.

Calibration loads the model on the machine you run it on, with the config's `device` and `cache_dir`, so run it where the weights are already cached to avoid a download.

---

## Memory allocator

Linux builds use [mimalloc](https://github.com/microsoft/mimalloc) as the allocator. glibc's malloc hands candle's large, short-lived tensor buffers back to the kernel, and every forward pass then faults them in again: 1.9 million page faults in one 18 s video scan on a test server. mimalloc keeps the memory, which made CPU video scans 1.2–1.3× faster there, with unchanged scores. The cost is about 120 MB more resident memory. macOS builds stay on the system allocator, since only Linux was measured.
