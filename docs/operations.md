# Operations — metrics, health checks, live reload, calibration

This page covers running Apollo day to day: scraping its metrics, health checks for load balancers, how a stop drains and how to take an instance out of rotation, changing its config without a restart, choosing thresholds from labelled examples, and the memory allocator Linux builds use. For the server's live state at a glance, see `apollo status` in [configuration.md → CLI reference](./configuration.md#cli-reference).

---

## Prometheus metrics

Turn on the scrape endpoint in the config:

```toml
[metrics]
enabled  = true
endpoint = "127.0.0.1"   # default; the endpoint has no authentication
port     = 9464          # default
```

Apollo then serves `GET /metrics` on that address in the Prometheus text format, on its own listener separate from the gRPC port. `enabled`, `endpoint` and `port` are read at startup; changing them needs a restart. Binding to a non-loopback address without `allow` logs a warning, since anyone who can reach it can read the metrics. To choose who may connect, see [Who may connect](#who-may-connect).

```bash
curl -s http://127.0.0.1:9464/metrics
```

All metrics are prefixed `apollo_`. Counters start at zero when the server starts. Per-model metrics are kept by model name, so they keep counting when a reload replaces a model's worker.

Two metrics come from the database: `apollo_tasks` and `apollo_webhooks_pending`. A scrape waits for them as long as `[database].timeout`, and never more than 5 s (also when `timeout` is 0). If the database fails or doesn't answer in time, that scrape leaves them out and sets `apollo_metrics_database_ok` to 0. It still answers 200 with every other metric. `apollo status` shows `database unavailable` for the same counts.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `apollo_build_info` | gauge | `version` | Always 1. |
| `apollo_config_generation` | gauge | | Configs applied since start: 1 for the startup config, then one more per reload. |
| `apollo_config_loaded_timestamp_seconds` | gauge | | When the running config was applied. |
| `apollo_ready` | gauge | | 1 while the server is ready for work (what `/readyz` answers). |
| `apollo_lifecycle_phase` | gauge | `phase` | 1 for the phase the server is in: `starting`, `serving`, `draining` or `stopping`. See [Health checks and load balancers](#health-checks-and-load-balancers). |
| `apollo_drained` | gauge | | 1 while taken out of rotation with [`apollo drain`](#taking-an-instance-out-of-rotation). |
| `apollo_ready_strict` | gauge | | 1 while ready and in touch with the database, or it isn't shared (what `/readyz/strict` answers). |
| `apollo_utilization` | gauge | | How close the server is to refusing work, from 0 to 1 (the `utilization` of [`/load`](#the-load-figure)). |
| `apollo_lb_weight` | gauge | | The weight for load balancers: 1 to 100, 1 while the database is stale, 0 when not ready. |
| `apollo_degraded` | gauge | `reason` | 1 while degraded for that reason: `load` or `database`. See [Degraded](#degraded). |
| `apollo_database_up` | gauge | | 1 while in touch with a shared database, 0 while contact is stale. Only with a shared database (SurrealDB). |
| `apollo_database_last_contact_seconds` | gauge | | Seconds since a shared database last answered. Only with a shared database. |
| `apollo_submissions_total` | counter | `outcome` | Submissions: `accepted`, `existing` (a repeated `request_id`), `invalid`, `overloaded` (backpressure), `error`. |
| `apollo_submissions_overloaded_total` | counter | `reason` | Overloaded submissions by the limit that refused them: `queue` (`[app].max_pending`), `memory` (`[app].max_memory`), `model` (a model's `max_pending`). |
| `apollo_items_finished_total` | counter | `state` | Inputs that finished: `completed`, `failed`, `cancelled`. |
| `apollo_metrics_database_ok` | gauge | | 1 when the database answered this scrape's counts in time; 0 when `apollo_tasks` and `apollo_webhooks_pending` are left out. |
| `apollo_tasks` | gauge | `state` | Tasks in the database by state (until [retention](./database.md#retention) purges them). Left out when the database doesn't answer in time. |
| `apollo_items_in_flight` | gauge | | Inputs accepted and not finished. |
| `apollo_max_pending` | gauge | | `[app].max_pending` (0 = no limit). |
| `apollo_slots` / `apollo_slots_busy` / `apollo_slots_waiting` | gauge | | `[app].max_concurrent`, the inputs being processed, and those waiting for a slot. |
| `apollo_memory_bytes` / `apollo_memory_limit_bytes` | gauge | | Memory in use (as for `max_memory`) and the cap, when known / set. |
| `apollo_fetch_bytes_total` | counter | | Bytes fetched for URL and local-file inputs (uploads are not fetches). |
| `apollo_fetch_seconds` | histogram | | Time to fetch an input. |
| `apollo_fetch_failures_total` | counter | `kind` | Failed fetches: `transient` (retried) or `permanent`. |
| `apollo_cache_hits_total` | counter | | Model results served from the result cache. |
| `apollo_webhooks_pending` | gauge | | Finished inputs whose webhook event is not delivered yet (with a webhook configured). Left out when the database doesn't answer in time. |
| `apollo_webhook_deliveries_total` | counter | `event`, `result` | Delivery attempts: `progress` / `finished`, `ok` / `error`. |
| `apollo_listener_refused_total` | counter | `listener` | Peers a listener's `allow` list refused, at connect or with a 403: `metrics`, `health` or `agent`. |
| `apollo_model_loaded` | gauge | `model` | 1 while the model's weights are in memory. |
| `apollo_model_queued_jobs` | gauge | `model` | Requests waiting for the model's worker. |
| `apollo_model_items_admitted` | gauge | `model` | Inputs accepted and not finished that run the model (what its `max_pending` is checked against). |
| `apollo_model_utilization` | gauge | `model` | `apollo_model_items_admitted` over the model's `max_pending`, for models with one. |
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

## Health checks and load balancers

Turn on the health port in the config:

```toml
[app]
drain_delay_secs   = 5    # a stop keeps serving this long, unready (default 0)
drain_timeout_secs = 15   # then waits this long for open calls (default; 0 = no limit)

[health]
enabled = true
port    = 9465            # default; binds where [app].endpoint does unless endpoint is set
```

Apollo then answers plaintext HTTP/1.1 on that port, from right after it starts (before the database opens) until the process exits:

| Path | 200 | 503 | Body |
|------|-----|-----|------|
| `/livez` | always | never | `ok` |
| `/readyz` (and `/`) | ready for work | otherwise | `ok`, or why not: `not ready: starting (recovering tasks, 41s)`, `not ready: drained`, `not ready: draining`, `not ready: stopping` |
| `/readyz/strict` | `/readyz` passes and the server is in touch with a shared database (or it isn't shared) | otherwise | as `/readyz`, or `not ready: database (no contact 47s)` |
| `/load` | always | never | the load figure as JSON (see [The load figure](#the-load-figure)) |

A ready server that is [degraded](#degraded) answers `200 ok degraded: load,database` on `/readyz` and `/readyz/strict`, naming the reasons that apply, with the header `x-envoy-degraded: 1`.

- `GET`, `HEAD` and `OPTIONS` work. Other methods get `405` with `Allow`, and unknown paths `404`. The `Host` header is ignored, and so is the query except `?verbose` (see [Details for local tools](#details-for-local-tools)). HTTP/1.0 works.
- Every answer has `cache-control: no-store`. `/load` and `?verbose` answer `application/json`; the others `text/plain; charset=utf-8`, whose first word is meant for matching. There are no redirects and no `429`.
- The answers come from the server's state in memory, and the load figure from a sample taken every second. A probe never waits on the database, a model or a busy server, so a probe that times out means the process (or the network) is stuck. The time in `starting (…, 41s)` is how long the current step has run.
- There is no authentication and no TLS. To limit who may connect, see [Who may connect](#who-may-connect).
- At most 256 connections are open at once; more wait until one closes. A connection that sends no complete request head within 10 s, or stays idle for 10 s between requests, is closed.

gRPC health on the gRPC port follows the same states: the names `""`, `apollo.v1.Inference` and `readiness` answer like `/readyz`, `readiness-strict` like `/readyz/strict`, and `liveness` like `/livez`, until the stop removes them all. gRPC health has no degraded state: a degraded server is `SERVING` (see [grpc-api.md → Health checking](./grpc-api.md#health-checking)). Prefer the health port: it speaks HTTP/1.1, needs no client certificate under mutual TLS, and answers before tasks are recovered.

**The rules:**

1. Liveness (`/livez`) means the process answers. Nothing else.
2. Readiness (`/readyz`) follows the server's own state only. The database is never part of it: an outage of a shared database would take every instance out at once (see [database.md → Health checks and the database](./database.md#health-checks-and-the-database)). `/readyz/strict` adds the server's own contact with a shared database, for a balancer that should drop an instance cut off from it, at the price of dropping every instance when the database is out.
3. Overload never makes a server unready. Its weight drops as it fills, near its limits it is [degraded](#degraded), and at them it refuses work with `RESOURCE_EXHAUSTED` and `apollo-overloaded`, as before (see [Several instances](#several-instances-sending-work-to-a-less-busy-one)).

**What each check answers:**

| State | `/livez` | `/readyz`, gRPC readiness | `/readyz/strict`, `readiness-strict` | Weight | gRPC port |
|-------|----------|---------------------------|--------------------------------------|--------|-----------|
| Opening the database (and migrating) | 200 | 503 starting | 503 starting | 0 | not bound |
| Configuring auth | 200 | 503 starting | 503 starting | 0 | not bound |
| Recovering tasks | 200 | 503 starting | 503 starting | 0 | bound, but answers nothing (a TCP check passes) |
| Loading the models with `keep_in_memory` | 200 | 503 starting | 503 starting | 0 | serves; a Classify waits for its model |
| Serving | 200 | 200 | 200 | 100 × (1 − utilization) | serves |
| Overloaded | 200 | 200 degraded | 200 degraded | low, down to 1 | refuses new work with `apollo-overloaded` |
| This instance is cut off from a shared database | 200 | 200 degraded | 503 database | 1 | calls that need it fail with `UNAVAILABLE` after `[database].timeout` |
| A shared database is out for every instance | 200 | 200 degraded on every instance | 503 database on every instance | 1 on every instance | as above |
| SQLite fails | 200 | 200 | 200 | as when serving | as above |
| Reloading the config | 200 | 200, with no dip | 200, with no dip | unchanged | serves |
| Drained with [`apollo drain`](#taking-an-instance-out-of-rotation) | 200 | 503 drained | 503 drained | 0 | serves as usual; takes no tasks over from other instances |
| Draining (`drain_delay_secs`) | 200 | 503 draining | 503 draining | 0 | serves as usual; takes no tasks over from other instances |
| Stopping (up to `drain_timeout_secs`) | 200 | 503 stopping | 503 stopping | 0 | closed: open calls finish, `WatchTask` ends with `UNAVAILABLE`, waits return |
| Handing tasks back (up to 5 s) | 200 | 503 stopping | 503 stopping | 0 | closed |
| The process is stuck | no answer | no answer | no answer | no answer | no answer |

gRPC health answers `SERVING` where the table says 200 (degraded or not), and `NOT_SERVING` for 503. The weight is the one `/load` and `apollo_lb_weight` give. The [HAProxy agent](#the-haproxy-agent) sends it where it is above 0, `drain` where it is 0, and closes its port once stopping.

Starting ends once every model with `keep_in_memory` has tried its first load, whether it loaded or not; a model that failed to load shows in the log and `apollo status`. A model download can take long, and `/livez` passes meanwhile.

### Degraded

A ready server can be degraded, for either reason or both:

- `load`: its [utilization](#the-load-figure) reached `[health].degraded_at` (default 0.9). It stays degraded until utilization falls below `degraded_at − 0.15` (0.75 by default; half of `degraded_at` when that is under 0.3). After each change the new state holds at least 10 s, so a server near the line doesn't flap.
- `database`: its contact with a shared database is stale (see [database.md → Health checks and the database](./database.md#health-checks-and-the-database)). Its weight is then 1, and `/readyz/strict` fails.

A degraded server stays in rotation. `/readyz` and `/readyz/strict` (when it passes) answer `200 ok degraded: load,database`, naming the reasons that apply, with the header `x-envoy-degraded: 1`: Envoy's active health checks then mark the host degraded and send it traffic only when the healthy hosts aren't enough. To other balancers it is a 200. `apollo_degraded{reason}` and `apollo status` show the reasons.

### The load figure

`/load` answers `200` in every phase, until the process exits, with the server's load as JSON:

```json
{"phase":"serving","ready":true,"ready_strict":true,"drained":false,"degraded":[],
 "utilization":0.42,"weight":60,
 "items":{"in_flight":21,"max_pending":50,"free":29},
 "slots":{"total":8,"busy":8,"waiting":13},
 "memory":{"utilization":0.31},"database":"ok","sampled_ms_ago":412}
```

- `utilization`: how close the server is to refusing work, from 0 to 1. It is the larger of `in_flight / [app].max_pending` and the memory in use over `[app].max_memory`; with no `max_pending`, the inputs being processed or waiting over `[app].max_concurrent` stand in for the queue. A server at its limit (tasks resumed after a restart can take it past) counts as 1. It rises at once and falls over about 5 s (an exponential decay, τ = 5 s), so a burst that just ended still counts for a moment.
- `weight`: `5 × round(20 × (1 − utilization))`, from 100 when idle down to 1 in steps of 5; 1 while the database contact is stale, and 0 when not ready. For a balancer that takes weights.
- `ready`, `ready_strict`: what `/readyz` and `/readyz/strict` answer. `drained`: taken out of rotation with [`apollo drain`](#taking-an-instance-out-of-rotation). `degraded`: the [reasons](#degraded), `[]` when none.
- `items`: inputs accepted and not finished, `[app].max_pending` (0 = no limit), and the room left (`null` with no limit). `slots`: `[app].max_concurrent`, the inputs being processed, and those waiting for a slot.
- `memory.utilization`: memory in use over `[app].max_memory`; `null` with no cap, or when memory use can't be read.
- `database`: `ok` or `stale` for a shared database, `n/a` for SQLite (or before the database opens).
- `sampled_ms_ago`: the age of the sample, taken every second; `null` before the first one.

The figure covers the whole server. It names no models and no node; per-model load is in [Details for local tools](#details-for-local-tools) and `apollo_model_utilization`.

### The HAProxy agent

For HAProxy's `agent-check`, Apollo can report its weight and state on a TCP port of its own:

```toml
[health]
enabled    = true
agent_port = 9466   # 0 (the default) is off; same address as the health port
```

On each connection Apollo sends one line, ending in a newline, and closes the connection. It ignores what HAProxy sends (`agent-send`).

| State | Line | What HAProxy does |
|-------|------|-------------------|
| Serving | `up ready 60%` | `up` marks the server up again if HAProxy's own health check passes too, `ready` ends a drain, and `60%` sets the weight to that share of the server's configured `weight`. The figure is the [weight](#the-load-figure): 1 to 100, and 1 while out of touch with a shared database. |
| Starting, drained, draining | `drain` | Sends it no new connections, except persistent ones. |
| Stopping | the port is closed | HAProxy doesn't count an agent it can't reach as an error: the server stays in `drain`, and its health check (503 since the drain began) marks it down. |

Apollo never sends `down` or `maint`: whether the server is up stays with HAProxy's own health check. A backend might look like this:

```text
backend apollo
    mode http
    balance roundrobin
    option httpchk GET /readyz
    default-server check port 9465 check-proto h1 rise 2 fall 3 agent-check agent-port 9466 weight 100 slowstart 30s
    server apollo1 10.0.0.11:8080 proto h2
    server apollo2 10.0.0.12:8080 proto h2
```

- `weight 100` matters. HAProxy's default weight is 1, and it rounds a percentage of it down: with weight 1, any line under `100%` gives weight 0.
- Use a dynamic `balance` such as `roundrobin` or `leastconn`. With `static-rr` HAProxy takes only 0 and 100%.
- `slowstart 30s` gives a server that comes back up its full weight over 30 s, not at once.
- `check-proto h1`: the health port speaks HTTP/1.1, while `proto h2` would make the checks speak HTTP/2 too.
- `drain` takes effect at the next agent check (`agent-inter`, 2 s by default), without waiting for `fall` failed health checks. With the agent, a `drain_delay_secs` a little above `agent-inter` lets HAProxy see the drain before the gRPC port closes.
- `ready` also ends a drain or maintenance set by hand in HAProxy. To take a server out by hand, use [`apollo drain`](#taking-an-instance-out-of-rotation) on it, so the agent sends `drain`; or first stop its agent checks on HAProxy's runtime API (`disable agent apollo/apollo1`).
- `[health].allow`, when set, covers the agent port too, and a refused peer is reset as on the health port.

### Details for local tools

`/readyz?verbose` and `/readyz/strict?verbose` answer the same status as JSON, with the details behind it:

```json
{"phase":"starting","step":"loading models","since_secs":41,"reasons":["starting (loading models, 41s)"],
 "models":[{"model":"nsfw","pinned":true,"preload":"loading","admitted":0,"max_pending":4}],
 "database":{"state":"n/a","last_contact_secs":null},"load":{…},
 "config_generation":1,"restart_required":[]}
```

- `step` is `null` outside `starting`, and `since_secs` is how long the phase (or the step) has run. `reasons` says why the server isn't ready, and why `/readyz/strict` fails when it does.
- For each model: whether it has `keep_in_memory` (`pinned`), its first load (`lazy` without `keep_in_memory`, else `loading`, `loaded` or `failed`), the inputs admitted that run it, and its `max_pending` (`null` without one).
- `load` is the `/load` body. `config_generation` counts configs applied since start (as `apollo_config_generation`), and `restart_required` lists the settings the last reload left at their running values until a restart (such as `app.port`).

Only local tools get them. `?verbose` is honoured when the peer is exactly `127.0.0.1` or `::1` (an IPv4-mapped `::ffff:127.0.0.1` counts as `127.0.0.1`), the request has none of the headers `Forwarded`, `X-Forwarded-For`, `X-Real-IP` and `Via`, and `[health].allow`, when set, admits the peer. Anyone else gets the plain answer, as if the query weren't there. A proxy on the same host connects from loopback too, so make it add one of those headers.

### Stopping

The first `SIGTERM` or Ctrl-C (`apollo stop` sends `SIGTERM`) starts the stop:

1. **Draining**, for `[app].drain_delay_secs`: `/readyz` answers 503 and gRPC readiness `NOT_SERVING`, so load balancers stop sending work here. Every call is still served, new ones included. The server takes no tasks or webhook deliveries over from other instances.
2. **Stopping**, for at most `[app].drain_timeout_secs`: the gRPC port closes, so new connections are refused at once, and open connections are told to finish. An open `WatchTask` ends with `UNAVAILABLE`, and a `Classify` waiting in `wait_seconds` returns the task as it is. The stop waits for the calls still running (an upload, say) and cuts them when the timeout passes; `0` waits as long as they take.
3. **Handing back**, for at most 5 s: unfinished tasks are handed back, so another instance (or the next start) takes them at once, with no resume attempt counted. If the database doesn't answer in time, Apollo logs a warning and exits. Those claims then lapse after `[database].lease_secs`, and each counts a resume attempt.

A second signal stops at once: it skips the rest of the drain delay and cuts the open calls, then hands the tasks back (up to 5 s). A signal before the gRPC port serves (while the database opens, auth is set up or tasks are recovered) ends the start instead: that step is dropped, the tasks recovery took are handed back (up to 5 s), and the process exits with status 0. A second one then exits without handing them back. The health port answers all along, until the process exits.

Both drain settings apply on reload. The delay in force when the signal arrives counts, and the timeout in force when stopping begins. At startup Apollo logs the longest a stop can take:

```text
a stop takes at most 25s (drain delay 5s + drain timeout 15s + 5s to hand tasks back): give it that long, e.g. terminationGracePeriodSeconds >= 25
```

**Choosing the values:**

- Give the process manager at least `drain_delay_secs + drain_timeout_secs + 5` seconds before it kills the process: Kubernetes `terminationGracePeriodSeconds`, systemd `TimeoutStopSec`.
- Make `drain_delay_secs` at least the balancer's check `interval × unhealthy threshold + timeout` (add the jitter to the interval if it has one), so every balancer sees the instance unready before its port closes. A check every 5 s, unready after 2 failures, with a 1 s timeout, needs at least 11 s.
- Use either `drain_delay_secs` or a Kubernetes `preStop` hook that sleeps, not both: they add up.

**On Kubernetes**, for example:

```yaml
spec:
  terminationGracePeriodSeconds: 30   # >= drain_delay_secs + drain_timeout_secs + 5
  containers:
    - name: apollo
      ports:
        - { name: grpc, containerPort: 8080 }
        - { name: health, containerPort: 9465 }
      readinessProbe:
        httpGet: { path: /readyz, port: health }
        periodSeconds: 5
        failureThreshold: 2
      livenessProbe:
        httpGet: { path: /livez, port: health }
        periodSeconds: 10
        timeoutSeconds: 5
        failureThreshold: 6
```

`[app].endpoint` must then be `0.0.0.0` (or `::`), so the kubelet reaches both ports. Liveness passes while the server starts, so a long model download doesn't restart the pod. A `startupProbe` on `/readyz` is optional; give it longer than the slowest model download.

**Warnings:**

- A TCP check on the gRPC port passes while tasks are recovered, before the port answers anything. Check `/readyz` instead.
- Never make liveness depend on the database: an outage would restart every instance at once. `/livez` doesn't look at it.
- Don't point a Kubernetes `readinessProbe` at `/readyz/strict`: an outage of a shared database would take every pod out of the Service at once. Use `/readyz`.
- `/metrics`, the PID file and `apollo start --daemon` returning are not readiness signals: all of them come before the server is ready.

### Taking an instance out of rotation

`apollo drain` takes a running server out of rotation without stopping it, for maintenance or a look at it while load balancers send it no new work. `apollo drain --undo` takes it back:

```bash
apollo drain          # out of rotation
apollo drain --undo   # back in
```

While drained:

- `/readyz`, `/` and `/readyz/strict` answer `503 not ready: drained`, and the gRPC readiness names (`""`, `apollo.v1.Inference`, `readiness`, `readiness-strict`) `NOT_SERVING`. The weight is 0, and the [HAProxy agent](#the-haproxy-agent) sends `drain`. `/livez` and gRPC `liveness` still pass.
- Every call is still served, new ones included, as while a stop drains. Balancers stop sending work here at their next check; a client that connects directly can still send it.
- The server takes no tasks over from other instances, and the background redelivery of failed webhooks (`[webhook].redelivery_secs`) waits. Tasks it runs go on, and their webhooks are sent.
- `apollo status` shows `drained` next to the phase, `/load` has `"drained": true`, and `apollo_drained` is 1.

What it doesn't do:

- It closes no port and stops nothing. A stop (`SIGTERM`, Ctrl-C, `apollo stop`) drains and stops as usual, drained or not.
- It isn't kept: a restart starts the server undrained.
- `--undo` makes the server ready again only while it serves. During startup it is ready once startup ends; during a stop it stays unready.

`apollo drain` prints what changed. It goes over the same owner-only local socket as `apollo status`, so only the OS user running the server can drain it. When no server answers there, it fails and nothing changes.

### Load balancer recipes

Point the checks at the health port (9465 here) wherever the balancer allows, and give the process manager the stop budget, `drain_delay_secs + drain_timeout_secs + 5` seconds:

| Balancer | What to configure |
|----------|-------------------|
| Kubernetes | A `readinessProbe` on `/readyz` (period 5 s, failure threshold 2) and a `livenessProbe` on `/livez` (period 10 s, timeout 5 s, failure threshold 6), as in the [example above](#stopping). A `startupProbe` on `/readyz` is optional; give it longer than the slowest model download. `terminationGracePeriodSeconds` at least the stop budget. |
| Envoy | An HTTP health check on `/readyz` (or `/readyz/strict`) through `health_check_config: { port_value: 9465 }`, every 2 s with 1 s of jitter, healthy and unhealthy thresholds of 2, a panic threshold of 50%, and outlier detection with `consecutive_5xx: 5` and `max_ejection_percent` at about 100 / instances. See the [cluster sketch](#several-instances-sending-work-to-a-less-busy-one). Envoy also reads [`x-envoy-degraded`](#degraded). |
| HAProxy | `option httpchk GET /readyz` with `check port 9465 check-proto h1 rise 2 fall 3`, `agent-check agent-port 9466 weight 100` for the weight, and `slowstart 30s`. See [The HAProxy agent](#the-haproxy-agent). |
| AWS NLB | An HTTP health check on `/readyz/strict`, port 9465, every 5 or 10 s. Turn off `target_health_state.unhealthy.connection_termination.enabled` on the target group: otherwise the NLB cuts the open connections of a target as soon as it fails its check, a draining one included. When every target fails, an NLB sends traffic to all of them, so an outage of a shared database doesn't empty it. An NLB keeps each connection on one target, so also set [`max_connection_age_secs`](#several-instances-sending-work-to-a-less-busy-one). |
| AWS ALB (gRPC target group) | A check that only finds a process that doesn't answer: see below. |
| Google Cloud | An HTTP health check on `/readyz`, port 9465; or, while the gRPC port is plaintext, a gRPC health check with the service name `readiness`. On GKE, set the check with a `HealthCheckPolicy` (Gateway) or a `BackendConfig` (Ingress). |
| Azure Load Balancer, NGINX Plus, Traefik, Consul | An HTTP check on `/readyz`, port 9465. |

**AWS ALB with a gRPC target group.** Its health check speaks gRPC on the gRPC port and looks only at the gRPC status code, and Apollo has no call whose status follows readiness: `grpc.health.v1.Health/Check` answers `OK` (code 0) for `NOT_SERVING` too. Set the health check path to `/grpc.health.v1.Health/Check` and the success code to `0`. That finds an instance that doesn't answer (stuck, or not serving gRPC yet), not one that is draining. If that check fails on a healthy instance, use the default path `/AWS.ALB/healthcheck` with success code `12` (`UNIMPLEMENTED`), which finds the same. Draining then relies on the target group: the instance must be deregistered before it stops, as ECS and the AWS Load Balancer Controller do, and the deregistration delay (`deregistration_delay.timeout_seconds`) lets the calls in progress finish. On Kubernetes the controller deregisters the pod while it stops, so make `drain_delay_secs` long enough for the ALB to stop sending it requests.

---

## Who may connect

Two lists choose, by IP or CIDR, who may connect to the monitoring ports: `[metrics].allow` for the metrics port, and `[health].allow` for the health port and the [agent port](#the-haproxy-agent). Each covers only its own ports; neither falls back to the other.

`[metrics].allow` lists the peers that may scrape:

```toml
[metrics]
enabled  = true
endpoint = "0.0.0.0"
allow    = ["10.0.0.0/8", "127.0.0.1"]   # the Prometheus servers, and local tools
```

Unset, anyone who can reach the port may scrape it. A peer that isn't listed is reset as soon as it connects, without an HTTP answer. The list applies on reload, to new connections and to the next request on open ones: a scraper the new list leaves out gets a `403` with `connection: close`, and is reset when it reconnects. The other `[metrics]` fields need a restart. At startup and on each reload, Apollo logs the list in force:

```text
metrics 0.0.0.0:9464 allows 10.0.0.0/8 (loopback refused)
```

`(loopback refused)` means a local `curl http://127.0.0.1:9464/metrics` is refused. Add `127.0.0.1` (and `::1`) to allow it. A list that can't match the bound address is an error (see [configuration.md → `[metrics]`](./configuration.md#metrics--prometheus-metrics)).

`[health].allow` lists the peers that may connect to the health port and the agent port, by the same rules:

```toml
[health]
enabled = true
allow   = ["10.20.0.0/16", "127.0.0.1"]   # the load balancers' subnet, and local tools
```

Unlike `[metrics].allow`, it needs a restart. A list that misses the checkers fails liveness as well as readiness, so an orchestrator restarts the instance; applied by one reload across a fleet, it would hit every instance at once. Unset, anyone who can reach the port may connect, and Apollo logs `health 0.0.0.0:9465 allows everyone` at info.

**What it checks:** the address of the TCP peer, nothing else. Apollo never reads `X-Forwarded-For`, `Forwarded` or the PROXY protocol.

- It is not a firewall. The kernel completes the TCP handshake before Apollo sees the peer, so a TCP health check still passes and a port scan still finds the port open. Use security groups, a NetworkPolicy or nftables as the main control.
- A proxy, SNAT or a service mesh connects with its own address for every client it forwards (an Istio sidecar uses `127.0.0.6` or `::6`). Listing that address admits everyone behind it.
- The gRPC port is not covered. Its gRPC health and reflection services answer anyone who can connect there, whatever either list says.

**A refused scraper or checker** reports "connection reset by peer" or an EOF; a Kubernetes probe shows that error text, not a status code. Apollo logs a warning that names the peer, the port and the field to change:

```text
refused 192.0.2.7 on the metrics port 0.0.0.0:9464 (not in [metrics].allow)
```

Each source is logged at most once a minute (IPv6 sources by /64), and at most 10 lines a minute in all. The rest go into one summary a minute: `refused 3412 connections from 977 sources on the metrics port 0.0.0.0:9464 (not in [metrics].allow); latest: …`. `apollo_listener_refused_total` counts every refusal by `listener` (`metrics`, `health` or `agent`), so `increase(apollo_listener_refused_total[10m]) > 0` works as an alert. Change a list on one instance first, and check its log before changing the others.

**Lists for common load balancers.** Check the provider's page before you copy a range: they change without notice.

- **Kubernetes:** leave `[health].allow` unset and limit the port with a [NetworkPolicy](https://kubernetes.io/docs/concepts/services-networking/network-policies/) instead; traffic from the node, where the kubelet's probes come from, is always allowed there. If you set it anyway, the kubelet's source address depends on the network plugin, so read it from the refusal log. A list that misses the kubelet makes Kubernetes restart the pods.
- **Istio sidecar:** probes and mesh traffic arrive from `127.0.0.6` or `::6` ([Istio's health checks](https://istio.io/latest/docs/ops/configuration/mesh/app-health-check/)), so listing them admits everyone behind the sidecar. Use an [AuthorizationPolicy](https://istio.io/latest/docs/reference/config/security/authorization-policy/) instead.
- **AWS ALB and NLB:** checks come from the load balancer's private IPs, which change. List the load balancer's subnet CIDRs (IPv4 and IPv6) or, as AWS's [network ACL table](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html) does, the VPC CIDR. With NodePort targets, where kube-proxy forwards the traffic, list the node CIDRs. Without client IP preservation, or behind PrivateLink, every client arrives from the load balancer's IPs, so listing them admits all its clients.
- **Google Cloud** ([health check ranges](https://docs.cloud.google.com/load-balancing/docs/health-check-concepts)): most load balancers check from `35.191.0.0/16` and `2600:2d00:1:b029::/64`. Regional external passthrough load balancers add `209.85.152.0/22`, `209.85.204.0/22` and `2600:1901:8001::/48`. Global external passthrough ones use `136.124.104.0/22`, `136.124.108.0/22`, `2600:1904:7::/48` and `2600:1904:8::/48`. Target pools may check from `169.254.169.254`, and hybrid or internet NEGs from the proxy-only subnet.
- **Azure Load Balancer** ([probes](https://learn.microsoft.com/en-us/azure/load-balancer/load-balancer-custom-probe-overview)): `168.63.129.16` and `fe80::1234:5678:9abc`. The link-local entry logs a warning at startup, which is expected.
- **HAProxy or Envoy on other hosts:** each proxy host's own address, not the VIP.
- **Local agents** (a sidecar, `curl` on the host): `127.0.0.1` and `::1`.

---

## Several instances: sending work to a less busy one

With Apollo on several servers, a busy instance refuses work it can't start soon, and a proxy or the client sends that request to another instance.

**What "busy" means.** A refusal is `RESOURCE_EXHAUSTED` with metadata `apollo-overloaded`, whose value says which limit was reached:

- `queue`: `[app].max_pending` inputs are in flight;
- `memory`: the process is over `[app].max_memory`;
- `model`: a model's own `[models.<label>].max_pending` is reached.

Nothing is kept when a request is refused as overloaded, so the same request can be sent again elsewhere. It's usually refused before any content arrives (at a `ClassifyStream` init frame, or before inline content is written to disk). But if the instance fills up while an upload is arriving, the refusal comes after it, and the received bytes are discarded. Size limits are different: every instance would refuse them, so they carry `grpc-retry-pushback-ms: -1` and no tag.

Free slots alone don't show how busy a server is. Each model works through its inputs one batch at a time, and OCR reads one image at a time. So set `max_pending` on heavy models: for OCR on a CPU, `max_pending = 2` refuses a third OCR input while two are unfinished (one being read, one waiting), even if the server has free slots for other models. An input counts toward its models from when it's accepted until it finishes; a pipeline input counts toward every model in the pipeline, including later steps and ones a `stop_if` may skip. A cancelled input stops counting at once. Keep `[app].max_pending` at about 3–4× `[app].max_concurrent`, so a full server refuses early rather than queueing minutes of work. `apollo_model_items_admitted` and `apollo_model_queued_jobs` show which model is the bottleneck.

**A proxy.** Balance every request separately with an HTTP/2-aware proxy. With an L4 or DNS balancer, one client's connection stays on one server. Classify returns at once, so open-request counts say little about load; retrying refusals is what moves the work. An Envoy route might look like this (a sketch: the `apollo` cluster needs HTTP/2, and TLS when the servers are on other hosts):

```yaml
routes:
  - match: { prefix: "/apollo.v1.Inference/Classify" }   # Classify and ClassifyStream
    request_body_buffer_limit: 5242880 # so a refused inline Classify can be resent
    route:
      cluster: apollo
      timeout: 0s                      # wait_seconds may hold a call for up to 300 s,
      idle_timeout: 0s                 # silent until it answers (Envoy's default is 5 min)
      retry_policy:
        retry_on: retriable-headers,connect-failure,refused-stream
        retriable_headers:
          - name: apollo-overloaded
            present_match: true
        num_retries: 2                 # instances − 1
        retry_host_predicate:
          - name: envoy.retry_host_predicates.previous_hosts
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.retry.host.previous_hosts.v3.PreviousHostsPredicate
        host_selection_retry_max_attempts: 3
  - match: { prefix: "/" }
    route: { cluster: apollo, timeout: 0s, idle_timeout: 0s }   # WatchTask streams can be quiet
```

Give the `apollo` cluster health checks on the health port, so a stopping instance leaves before its port closes, and outlier detection for one that fails calls:

```yaml
clusters:
  - name: apollo
    health_checks:
      - timeout: 1s
        interval: 2s
        interval_jitter: 1s
        unhealthy_threshold: 2
        healthy_threshold: 2
        http_health_check: { path: /readyz }   # over HTTP/1.1, which the health port speaks
    common_lb_config:
      healthy_panic_threshold: { value: 50 }
    outlier_detection:
      consecutive_5xx: 5               # overload (RESOURCE_EXHAUSTED) doesn't count
      max_ejection_percent: 34         # about 100 / instances
    load_assignment:
      cluster_name: apollo
      endpoints:
        - lb_endpoints:
            - endpoint:
                address: { socket_address: { address: 10.0.0.11, port_value: 8080 } }
                health_check_config: { port_value: 9465 }
```

With those numbers Envoy sees a draining instance unready within about 6 s (two checks, each up to 3 s apart), so `drain_delay_secs = 7` covers it.

Envoy resends only a request that still fits its buffer: `request_body_buffer_limit` above covers inline content up to gRPC's 4 MiB message limit. On an Envoy without that field, raise the listener's `per_connection_buffer_limit_bytes` (1 MiB by default) together with the route's `per_request_buffer_limit_bytes`. Resend large `ClassifyStream` uploads from the client. Also raise the cluster's `circuit_breakers` `max_retries` above its default of 3, or bursts aren't retried.

**An L4 or DNS balancer** (an AWS NLB, a Kubernetes Service without a mesh, DNS round robin) picks an instance for each connection, and a gRPC client keeps its connection as long as it can, so an instance that starts later gets no share of the work. Set `[app].max_connection_age_secs` (a few minutes, such as 300): a connection that old, give or take 10% so they don't all go at once, is told to finish (`GOAWAY`), and the client opens a new one, which the balancer may send elsewhere. The calls still running on it go on for up to `[app].max_connection_age_grace_secs` (300 s by default, the longest `wait_seconds`) and are then cut; a `WatchTask` cut that way should watch again. Both settings need a restart.

**In the client.** Keep a connection to each instance. When a `Classify` or `ClassifyStream` is refused with `apollo-overloaded`, or the connection fails, send it to the next instance. Don't resend after an ambiguous error (a reset or a deadline): the first instance may have taken it.

**A shared database.** Instances sharing one SurrealDB database share its tasks: each task is claimed by the instance running it, under a lease it renews (`[database].lease_secs`, 30 s by default; keep it the same everywhere). When an instance stops, another takes its tasks over: at once after a graceful stop, which hands them back, or once their leases lapse after a crash. A database outage of more than a third of `lease_secs` can stop running tasks everywhere (depending on when it starts; one longer than 5/6 always does); they resume within about two thirds of the lease once it is back, without using up their resume attempts. A longer lease rides out longer outages (to ride out D seconds, use `lease_secs` ≥ 3·D), a shorter one takes over a crashed instance's tasks sooner. It takes over only tasks whose models it has, up to its own free room, and an upload or local-file input stays with the node that accepted it (`[app].node_id`; give each instance its own), which takes it back after a restart; other instances fail it once its lease has lapsed for ten leases. A task no instance has held for ten leases and the one looking can't run (its model is gone everywhere) is failed, saying why. A cancel through any instance reaches the runner at its next renewal; a `request_id` deduplicates across all of them; `WatchTask` and `wait_seconds` see changes another instance makes (within about 2 seconds); and a finished task or item keeps its state, whoever writes later. Run the same version everywhere: older instances don't take part in leases. Give every instance the same `[webhook]` `url` and `secret`: each sends only the pending events for its own receiver, so after a fleet-wide `url` change, events that had already failed against the old one stay pending (see `apollo_webhooks_pending`). Cached results are shared only between instances whose model output settings match (each logs its models' `result cache key` at startup). They verify tokens with one public key, stored in the database: the first instance to start publishes its own, and one configured with a different `[auth].public_key` refuses to start. Upgrading a fleet whose instances each made their own keypair: the first to start publishes its key, and tokens minted on the others stop working there once each restarts (it logs a warning), so pick the host to mint on and start it first, or publish its key with `apollo publish-key` before the restarts. Revocations go to the database too (`apollo revoke`), and `apollo publish-key --replace` plus a restart of each instance rotates the key. Not covered yet: an instance with `[auth] enabled = false`, or with other `admin_subjects`, shows tasks the others keep apart, so give every instance the same `[auth]`. To keep instances apart instead, give each its own database (`[database.surrealdb].database`). Each instance then knows only its own tasks:

- `GetTask`, `WatchTask`, `CancelTask` and `ListTasks` must reach the instance that accepted the task. A proxy that balances every request sends them anywhere, so behind one, get results with `wait_seconds` or a webhook, or make those calls from a client that keeps one connection per instance.
- A `request_id` only deduplicates within one instance. Don't resend through the proxy after an ambiguous error: another instance would run the request again.

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

The new config is read the way `start` read it, with the same file and the same `--endpoint` / `--port` / `--webhook-url` / `--health-port` / `--metrics-port` overrides. It is validated first; an invalid config (or one without models) is rejected with its errors, and **the running config stays**. `apollo status` shows the generation and when the running config was applied.

**What applies at once:** thresholds and `early_exit`, strategies, pipelines, model `timeout` and `priority`, `[limits]`, `[cache]`, `[webhook]` (URL, secret, redelivery interval), `max_retries`, `max_pending`, `max_memory`, `idle_timeout`, `drain_delay_secs`, `drain_timeout_secs`, `database.retention`, `database.timeout` (for calls from then on), `metrics.allow`, `health.degraded_at`, and `log_level` (unless `RUST_LOG` is set, which keeps winning). A `metrics.allow` that can't match the running listener's address rejects the reload.

**Models:** each model's worker is kept, loaded weights included, unless something its weights depend on changed: `architecture`, `repo`, `revision`, `labels`, `prompt_template`, `score_threshold`, `max_results`, `taxonomy_file` (or the taxonomy file's contents), `keep_in_memory` or `max_concurrent`. A model with such a change gets a new worker, which loads on first use (or at once with `keep_in_memory`). Added or newly enabled models get a worker. Removed or disabled models are rejected for new requests at once; their workers finish the work they have, then unload. A change to any setting that changes outputs (including `early_exit`, the video strategy and a list's contents) starts new result-cache entries, even when the worker is kept.

**Files a model reads** — a SigLIP taxonomy, a PDQ hash list — are read when the config is applied, at startup or on a reload; the model loads from that copy (also after an idle unload), so editing a file takes effect on the next reload, and a broken file is rejected with the config.

**Tasks already running** finish on the config they started with, so a scan is not switched to a different strategy halfway, and a pipeline keeps its steps. New submissions use the new config.

**Restart-only:** `app.endpoint`, `app.port`, `app.device`, `app.cache_dir`, `app.max_concurrent`, `app.node_id`, `app.max_connection_age_secs`, `app.max_connection_age_grace_secs`, `[tls]`, `[auth]`, `[database]` except `retention` and `timeout` (`backend`, `lease_secs` and the backend's own section), `metrics.enabled`, `metrics.endpoint`, `metrics.port`, and `[health]` except `degraded_at` (`enabled`, `endpoint`, `port`, `allow`, `agent_port`). A reload reports these and keeps their running values until the next restart. Uploads are read only from the current `app.cache_dir`, so uploaded inputs still waiting when it changes fail: let them finish first.

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

Images (`jpg`, `png`, `webp`, `gif`, `bmp`, `tiff`), videos (`mp4`, `m4v`, `mov`, `mkv`, `webm`, `avi`) and texts (`txt`, UTF-8, for text models) are recognised by extension. A model that reads text (`paddleocr_vl`) has no thresholds to calibrate; calibrate the text model that classifies what it reads, on texts.

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
