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

When `[webhook].secret` is set, every delivery is signed (scheme `v1`) with two gRPC metadata headers:

| Header | Value |
|--------|-------|
| `x-apollo-webhook-timestamp` | Unix seconds when the delivery was sent (fresh on every retry). |
| `x-apollo-webhook-signature` | `v1=` + lowercase‑hex **HMAC‑SHA256**(secret, `"<timestamp>."` ‖ `payload`) |

`payload` is the protobuf‑encoded `Task` **exactly as sent** — the gRPC message bytes, without gRPC's 5‑byte frame header. So the signature covers the whole delivery: a captured delivery cannot be altered (any change to the payload or timestamp breaks the signature), and an old one cannot be replayed later (the receiver rejects stale timestamps).

**To verify on the receiver:**

1. Read both headers; reject the call if either is missing.
2. Reject it if the timestamp is more than your tolerance (5 minutes recommended) away from your clock.
3. Compute `HMAC-SHA256(secret, timestamp + "." + payload)` over the **raw message bytes**, hex‑encode it, and compare it to the header (after `v1=`) in constant time.

If your gRPC framework only gives you the decoded message, re‑encode it with **deterministic** serialization (fields in number order, map entries sorted by key — e.g. Go `proto.MarshalOptions{Deterministic: true}`, Java `CodedOutputStream.useDeterministicSerialization()`). Apollo encodes that way, so the bytes match as long as both sides were generated from the same schema version; verifying raw bytes avoids even that caveat.

The Rust SDK does all of this for you: `apollo_client::WebhookReceiver::new(handler).secret("…")` verifies the raw bytes before your handler runs (tolerance adjustable with `.signature_tolerance(…)`), and `apollo_client::webhook_signature::{sign, verify}` are available for custom receivers.

The signature authenticates the sender and protects the payload's integrity; it does **not** encrypt it. Pair it with a TLS `https://` webhook URL for confidentiality.

> **Changed from the original scheme:** the signature used to be HMAC‑SHA256 of the task id only, which let anyone holding one captured delivery forge any payload for that task. Receivers written against the old scheme reject `v1` deliveries (which Apollo then retries) until they are updated.

---

## Configuration

```toml
[webhook]
url             = "https://hooks.example.com:443"   # gRPC target; scheme selects TLS (https) vs plaintext (http)
secret          = "change-me"                        # optional; signs every delivery (payload + timestamp)
redelivery_secs = 60                                 # background retry interval for failed deliveries (0 = off)
```

- `url` is a gRPC endpoint. The **scheme selects transport**: `https://` uses TLS, `http://` is plaintext. Any path component is ignored — the gRPC method path is fixed by the service definition.
- The channel connects **lazily and reconnects automatically**, so the receiver need not be reachable when Apollo starts.
- For a quick local override during `start`, use `--webhook-url http://127.0.0.1:9090` (run‑only; it does not persist to the file). It also enables delivery when the config has no `[webhook]` section.
- Each delivery has a 10 s connect timeout and a 30 s request timeout; a call that exceeds them counts as a failed delivery and is retried.

See [configuration.md → Webhook](./configuration.md#webhook--outbound-delivery) for field details.

---

## Implementing a receiver

Generate server stubs from `proto/webhook.proto` (which imports `common.proto`) in your language of choice, then implement `TaskStatus`. A minimal sketch:

```text
service Webhook:
  rpc TaskStatus(task):
      if a secret is set:                   # over the raw message bytes, see "Signing"
          ts  = header x-apollo-webhook-timestamp;  reject if |now - ts| > 300s
          sig = header x-apollo-webhook-signature
          reject unless sig == "v1=" + hex(HMAC_SHA256(secret, ts + "." + raw_payload))
      if already_processed(task.id):        # idempotency / dedupe
          return Ack()
      switch task.result:
          case models:  record success (per‑model results in models.models)
          case error:   record the task‑level failure
          case state:   still processing / cancelled (optional to handle)
      return Ack()
```

Return `Ack{}` (success) to acknowledge. Returning a gRPC error causes Apollo to keep the item marked undelivered and retry it later.
