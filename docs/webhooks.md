# Webhooks — the `Webhook` service

Apollo can push task updates to a receiver instead of (or in addition to) clients polling `GetTask`. The direction is inverted from the main API: here **Apollo is the gRPC client** and **you implement the server**.

The contract is `apollo.v1.Webhook`, defined in `proto/webhook.proto` (sharing message types with `common.proto`). Enable it by configuring `[webhook].url`; omit the section to disable delivery and rely on polling.

---

## The service you implement

```protobuf
service Webhook {
  rpc TaskStatus (Task) returns (Ack);
  rpc ItemFailed (Task) returns (Ack);
}

message Ack {}   // empty acknowledgement
```

Both methods take a **bare `Task`** (the same `Task` message `GetTask` returns) and return an empty `Ack`. Return a successful gRPC response to acknowledge; return an error status to signal a delivery failure (Apollo will retry — see below).

| Method | Fired when |
|--------|-----------|
| `TaskStatus` | An item reaches a terminal state (`queued → … → completed / failed / cancelled`), including intermediate transitions like `retrying`. |
| `ItemFailed` | **Additionally** for an item that has exhausted all retries — a dead‑letter signal. The permanently‑failed item is the one in the `FAILED` state. |

Both carry the **full current `Task`**, so the receiver sees all items and their results, not just the one that changed.

---

## Delivery semantics

- **One delivery per terminal item.** The webhook fires as each item reaches a terminal state; a single‑item task (every `Classify`) therefore produces one `TaskStatus` delivery when it finishes.
- **At‑least‑once.** Apollo persists a per‑item "delivered" flag and only clears it after a successful call, but a crash between the model finishing and the flag being set will cause a redelivery. **Receivers must dedupe.** Dedupe on `Task.id` plus the index of the item that is terminal (for `ItemFailed`, the item in the `FAILED` state).
- **Retries on failure.** If a delivery fails (the receiver is down or returns an error), it is retried by a background loop every `[webhook].redelivery_secs` seconds (default 60; `0` disables periodic retry). Undelivered items are also retried on server **restart**, recovered from the persisted flag.
- **Which items reached a terminal state** is something the receiver computes from the payload; the wire message is intentionally the whole `Task`.

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

Generate server stubs from `proto/webhook.proto` (which imports `common.proto`) in your language of choice, then implement `TaskStatus` and `ItemFailed`. A minimal sketch:

```text
service Webhook:
  rpc TaskStatus(task):
      verify x-apollo-webhook-signature == hex(HMAC_SHA256(secret, task.id))   # if a secret is set
      terminal = [i for i in task.items if i.state in (COMPLETED, FAILED, CANCELLED)]
      if already_processed(task.id, terminal_indexes):   # idempotency / dedupe
          return Ack()
      persist(task)
      return Ack()

  rpc ItemFailed(task):
      # dead‑letter: an item exhausted its retries (the one in state FAILED)
      verify signature as above
      alert_or_record_dead_letter(task)
      return Ack()
```

Return `Ack{}` (success) to acknowledge. Returning a gRPC error causes Apollo to keep the item marked undelivered and retry it later.
