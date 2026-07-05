# Webhooks — the `Webhook` service

Apollo can push task updates to a receiver instead of (or in addition to) clients polling `GetTask`. The direction is inverted from the main API: here **Apollo is the gRPC client** and **you implement the server**.

The contract is `apollo.v1.Webhook`, defined in `proto/webhook.proto` (sharing message types with `common.proto`). Enable it by configuring `[webhook].url`; omit the section to disable delivery and rely on polling.

---

## The service you implement

```protobuf
service Webhook {
  rpc TaskStatus (Task) returns (Ack);
}

message Ack {}   // empty acknowledgement
```

`TaskStatus` takes a **bare `Task`** (the same `Task` message `GetTask` returns) and returns an empty `Ack`. Return a successful gRPC response (an `Ack`) to acknowledge; return an error status to signal a delivery failure (Apollo will retry — see below).

| Method | Fired when |
|--------|-----------|
| `TaskStatus` | The task reaches a terminal state (finished with `models`, failed with `error`, or cancelled), and on intermediate transitions while it is processing. |

It carries the **full current `Task`**, so the receiver sees the task's `result` — its per‑model `models`, an `error`, or a live `state` — not just what changed.

---

## Delivery semantics

- **One delivery as the task finishes** (plus intermediate transitions). Every `Classify` submits a single input, so a task produces a `TaskStatus` delivery when it reaches a terminal state.
- **At‑least‑once.** Apollo persists a "delivered" flag and only clears it after a successful call, but a crash between finishing and the flag being set will cause a redelivery. **Receivers must dedupe on `Task.id`.**
- **Retries on failure.** If a delivery fails (the receiver is down or returns an error), it is retried by a background loop every `[webhook].redelivery_secs` seconds (default 60; `0` disables periodic retry). Undelivered webhooks are also retried on server **restart**, recovered from the persisted flag.
- **Whether the task finished, failed, or is still processing** is read from the payload's `result` (`models` / `error` / `state`); the wire message is intentionally the whole `Task`.

Because delivery is at‑least‑once and unordered across retries, treat each webhook as "here is the current state of this task" rather than "here is a single event."

---

## Signing and authenticity

When `[webhook].secret` is set, every delivery carries an HMAC signature in gRPC metadata so the receiver can confirm the call came from a holder of the secret:

- **Header:** `x-apollo-webhook-signature`
- **Value:** lowercase‑hex **HMAC‑SHA256** of the **task id** (`Task.id`), keyed by the shared secret.

To verify on the receiver: recompute `HMAC-SHA256(secret, task.id)`, hex‑encode it, and compare (ideally with a constant‑time comparison) against the header value.

The signature authenticates the caller and binds the delivery to a task id; it does **not** encrypt the payload. Pair it with a TLS `https://` webhook URL for transport confidentiality. (The signature does not cover the full message body, so don't rely on it for body integrity beyond the task id.)

---

## Configuration

```toml
[webhook]
url             = "https://hooks.example.com:443"   # gRPC target; scheme selects TLS (https) vs plaintext (http)
secret          = "change-me"                        # optional; enables x-apollo-webhook-signature
redelivery_secs = 60                                 # background retry interval for failed deliveries (0 = off)
```

- `url` is a gRPC endpoint. The **scheme selects transport**: `https://` uses TLS, `http://` is plaintext. Any path component is ignored — the gRPC method path is fixed by the service definition.
- The channel connects **lazily and reconnects automatically**, so the receiver need not be reachable when Apollo starts.
- For a quick local override during `start`, use `--webhook-url http://127.0.0.1:9090` (run‑only; it does not persist to the file).

See [configuration.md → Webhook](./configuration.md#webhook--outbound-delivery) for field details.

---

## Implementing a receiver

Generate server stubs from `proto/webhook.proto` (which imports `common.proto`) in your language of choice, then implement `TaskStatus`. A minimal sketch:

```text
service Webhook:
  rpc TaskStatus(task):
      verify x-apollo-webhook-signature == hex(HMAC_SHA256(secret, task.id))   # if a secret is set
      if already_processed(task.id):        # idempotency / dedupe
          return Ack()
      switch task.result:
          case models:  record success (per‑model results in models.models)
          case error:   record the task‑level failure
          case state:   still processing / cancelled (optional to handle)
      return Ack()
```

Return `Ack{}` (success) to acknowledge. Returning a gRPC error causes Apollo to keep the item marked undelivered and retry it later.
