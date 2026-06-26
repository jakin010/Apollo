//! The `Webhook` receiver: serve this to get per-item results pushed to you.
//!
//! If you set a shared secret (the same value as the apollo server's
//! `[webhook].secret`), each delivery's `x-apollo-webhook-signature` is verified
//! for you — a missing or invalid signature is rejected with `unauthenticated`
//! before your handler runs, so you never have to compute the HMAC yourself.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use apollo_proto::webhook_server::{Webhook, WebhookServer};
use apollo_proto::{Ack, Task};

/// Metadata header carrying the delivery signature.
const SIGNATURE_HEADER: &str = "x-apollo-webhook-signature";

/// Handle task-status callbacks. Invoked once per item as it reaches a terminal
/// state, carrying the full current [`Task`]. Delivery is at-least-once, so
/// dedupe (e.g. by `Task.id` plus which items are terminal).
///
/// Implement with `#[tonic::async_trait]` (re-exported as
/// [`apollo_client::async_trait`](crate::async_trait)).
#[tonic::async_trait]
pub trait WebhookHandler: Send + Sync + 'static {
    async fn on_task_status(&self, task: Task);
}

/// A webhook receiver. Build it from a [`WebhookHandler`], optionally set a shared
/// `secret` so deliveries are signature-verified for you, then `serve`.
///
/// ```no_run
/// # async fn ex() -> Result<(), Box<dyn std::error::Error>> {
/// # use apollo_client::{WebhookReceiver, WebhookHandler, Task};
/// # struct Sink;
/// # #[tonic::async_trait] impl WebhookHandler for Sink { async fn on_task_status(&self, _t: Task) {} }
/// WebhookReceiver::new(Sink)
///     .secret("shared-secret")          // must match the server's [webhook].secret
///     .serve("0.0.0.0:9090".parse()?)
///     .await?;
/// # Ok(()) }
/// ```
pub struct WebhookReceiver<H> {
    handler: H,
    secret: Option<String>,
}

impl<H: WebhookHandler> WebhookReceiver<H> {
    /// A receiver that does not verify signatures.
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            secret: None,
        }
    }

    /// Verify each delivery's HMAC-SHA256 signature using this shared secret (the
    /// same value as the apollo server's `[webhook].secret`). Deliveries with a
    /// missing or invalid signature are rejected with `unauthenticated` before the
    /// handler runs.
    pub fn secret(mut self, secret: impl Into<String>) -> Self {
        self.secret = Some(secret.into());
        self
    }

    fn into_service(self) -> Service<H> {
        Service {
            handler: Arc::new(self.handler),
            secret: self.secret.map(|s| Arc::from(s.into_bytes())),
        }
    }

    /// Serve the webhook receiver on `addr` until the process exits.
    pub async fn serve(self, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
        let router = Server::builder().add_service(WebhookServer::new(self.into_service()));
        #[cfg(feature = "reflection")]
        let router = router.add_service(reflection_service());
        router.serve(addr).await
    }

    /// Like [`serve`](Self::serve), but stops gracefully when `shutdown` resolves.
    pub async fn serve_with_shutdown<F>(
        self,
        addr: SocketAddr,
        shutdown: F,
    ) -> Result<(), tonic::transport::Error>
    where
        F: Future<Output = ()>,
    {
        let router = Server::builder().add_service(WebhookServer::new(self.into_service()));
        #[cfg(feature = "reflection")]
        let router = router.add_service(reflection_service());
        router.serve_with_shutdown(addr, shutdown).await
    }
}

struct Service<H> {
    handler: Arc<H>,
    /// HMAC key; when `Some`, every delivery is signature-verified.
    secret: Option<Arc<[u8]>>,
}

// Manual Clone: tonic clones the service per connection, and the fields are
// always cloneable regardless of whether `H` is.
impl<H> Clone for Service<H> {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::clone(&self.handler),
            secret: self.secret.clone(),
        }
    }
}

#[tonic::async_trait]
impl<H: WebhookHandler> Webhook for Service<H> {
    async fn task_status(&self, request: Request<Task>) -> Result<Response<Ack>, Status> {
        if let Some(secret) = &self.secret {
            verify_signature(secret, &request)?;
        }
        self.handler.on_task_status(request.into_inner()).await;
        Ok(Response::new(Ack {}))
    }
}

/// Verify the `x-apollo-webhook-signature` header against the task id: it must be
/// the lowercase-hex HMAC-SHA256 of `task.id` under `secret`. Constant-time
/// comparison. Note this authenticates the sender and binds the call to the task
/// id; pair with a TLS (`https`) receiver for full transport integrity.
fn verify_signature(secret: &[u8], request: &Request<Task>) -> Result<(), Status> {
    let header = request
        .metadata()
        .get(SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Status::unauthenticated("missing webhook signature"))?;
    let provided =
        hex_decode(header).ok_or_else(|| Status::unauthenticated("malformed webhook signature"))?;

    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts a key of any length");
    mac.update(request.get_ref().id.as_bytes());
    let expected = mac.finalize().into_bytes();

    if provided.len() == expected.len() && constant_time_eq(&provided, &expected) {
        Ok(())
    } else {
        Err(Status::unauthenticated("invalid webhook signature"))
    }
}

/// Decode a hex string (either case) to bytes, or `None` if malformed.
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    fn nibble(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks(2) {
        out.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    Some(out)
}

/// Constant-time equality for equal-length byte slices.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Build the v1 server-reflection service for a webhook receiver. Present only
/// with the `reflection` feature; advertises the `Webhook` service (and the
/// shared messages) — not `Inference`.
#[cfg(feature = "reflection")]
fn reflection_service() -> tonic_reflection::server::v1::ServerReflectionServer<
    impl tonic_reflection::server::v1::ServerReflection,
> {
    tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(apollo_proto::WEBHOOK_DESCRIPTOR_SET)
        .build_v1()
        .expect("apollo Webhook reflection descriptor must be valid")
}

/// Serve the webhook receiver on `addr` until the process exits. Convenience for
/// `WebhookReceiver::new(handler).serve(addr)` with no signature verification.
pub async fn serve_webhook<H: WebhookHandler>(
    addr: SocketAddr,
    handler: H,
) -> Result<(), tonic::transport::Error> {
    WebhookReceiver::new(handler).serve(addr).await
}

/// Like [`serve_webhook`], but stops gracefully when `shutdown` resolves.
pub async fn serve_webhook_with_shutdown<H, F>(
    addr: SocketAddr,
    handler: H,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    H: WebhookHandler,
    F: Future<Output = ()>,
{
    WebhookReceiver::new(handler)
        .serve_with_shutdown(addr, shutdown)
        .await
}
