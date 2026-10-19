//! The `Webhook` receiver: serve this to get task results pushed to you.
//!
//! If you set a shared secret (the same value as the apollo server's
//! `[webhook].secret`), every delivery's signature is verified for you against
//! the raw request bytes before your handler runs: a missing, invalid, or stale
//! (replayed) signature is rejected with `unauthenticated`. See
//! [`webhook_signature`](crate::webhook_signature) for the scheme, e.g. to verify
//! deliveries in a receiver you build yourself.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http_body_util::{BodyExt, Full, Limited};
use tonic::body::Body;
use tonic::codegen::{BoxFuture, Service, http};
use tonic::server::NamedService;
use tonic::transport::Server;
use tonic::transport::server::Router;
use tonic::{Request, Response, Status};

use apollo_proto::webhook_server::{Webhook, WebhookServer};
use apollo_proto::webhook_signature::{
    self, DEFAULT_TOLERANCE, SIGNATURE_HEADER, TIMESTAMP_HEADER,
};
use apollo_proto::{Ack, TaskEvent};

/// Largest delivery accepted. Events carry the summary view (no per-sample
/// results), but a task with many models and long scans still adds up; tonic's
/// 4 MiB default is too tight to rely on.
const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// Handle task events. A `FINISHED` [`TaskEvent`] arrives once a task is
/// completed, failed or cancelled, and best-effort `PROGRESS` events on the way
/// (processing, retrying); each carries the task as it is then. Finished events
/// are delivered at least once, so dedupe (e.g. by `task.id`).
///
/// Return `Ok(Response::new(Ack {}))` to acknowledge the delivery; returning an
/// error `Status` tells the sender the delivery was not accepted, so it will be
/// retried on the next redelivery pass.
///
/// Implement with `#[tonic::async_trait]` (re-exported as
/// [`apollo_client::async_trait`](crate::async_trait)).
// `async_trait` marks the boxed-future methods `#[must_use]`; clippy >= 1.99
// reports that as `double_must_use`.
#[allow(clippy::double_must_use)]
#[tonic::async_trait]
pub trait WebhookHandler: Send + Sync + 'static {
    async fn on_event(&self, event: TaskEvent) -> Result<Response<Ack>, Status>;
}

/// A webhook receiver. Build it from a [`WebhookHandler`], optionally set a shared
/// `secret` so deliveries are signature-verified for you, then `serve`.
///
/// ```no_run
/// # async fn ex() -> Result<(), Box<dyn std::error::Error>> {
/// # use apollo_client::{WebhookReceiver, WebhookHandler, TaskEvent, Ack};
/// # use tonic::{Response, Status};
/// # struct Sink;
/// # #[tonic::async_trait] impl WebhookHandler for Sink {
/// #     async fn on_event(&self, _e: TaskEvent) -> Result<Response<Ack>, Status> { Ok(Response::new(Ack {})) }
/// # }
/// WebhookReceiver::new(Sink)
///     .secret("shared-secret")          // must match the server's [webhook].secret
///     .serve("0.0.0.0:9090".parse()?)
///     .await?;
/// # Ok(()) }
/// ```
pub struct WebhookReceiver<H> {
    handler: H,
    secret: Option<String>,
    tolerance: Duration,
}

impl<H: WebhookHandler> WebhookReceiver<H> {
    /// A receiver that does not verify signatures.
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            secret: None,
            tolerance: DEFAULT_TOLERANCE,
        }
    }

    /// Verify each delivery's signature using this shared secret (the same value
    /// as the apollo server's `[webhook].secret`). Deliveries with a missing,
    /// invalid, or stale signature are rejected with `unauthenticated` before the
    /// handler runs.
    pub fn secret(mut self, secret: impl Into<String>) -> Self {
        self.secret = Some(secret.into());
        self
    }

    /// How far a delivery's signed timestamp may be from this machine's clock
    /// (default 5 minutes). Older deliveries are treated as replays.
    pub fn signature_tolerance(mut self, tolerance: Duration) -> Self {
        self.tolerance = tolerance;
        self
    }

    fn router(self) -> Router {
        let server = WebhookServer::new(Handler {
            handler: Arc::new(self.handler),
        })
        .max_decoding_message_size(MAX_MESSAGE_BYTES);
        let router = match self.secret {
            Some(secret) => Server::builder().add_service(Verified {
                inner: server,
                secret: Arc::from(secret.into_bytes()),
                tolerance: self.tolerance,
            }),
            None => Server::builder().add_service(server),
        };
        #[cfg(feature = "reflection")]
        let router = router.add_service(reflection_service());
        router
    }

    /// Serve the webhook receiver on `addr` until the process exits.
    pub async fn serve(self, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
        self.router().serve(addr).await
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
        self.router().serve_with_shutdown(addr, shutdown).await
    }
}

/// Adapts a [`WebhookHandler`] to the generated `Webhook` service.
struct Handler<H> {
    handler: Arc<H>,
}

#[tonic::async_trait]
impl<H: WebhookHandler> Webhook for Handler<H> {
    async fn notify(&self, request: Request<TaskEvent>) -> Result<Response<Ack>, Status> {
        self.handler.on_event(request.into_inner()).await
    }
}

/// Wraps the `Webhook` service and verifies each request's signature against the
/// raw message bytes (as signed by the server) before passing it on. Verifying
/// the bytes as received — rather than a re-encoding of the decoded message —
/// keeps verification exact even if this receiver's schema is older or newer
/// than the server's.
#[derive(Clone)]
struct Verified<S> {
    inner: S,
    secret: Arc<[u8]>,
    tolerance: Duration,
}

impl<S: NamedService> NamedService for Verified<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> Service<http::Request<Body>> for Verified<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        // Use the instance that was driven to readiness; leave a fresh clone.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let secret = Arc::clone(&self.secret);
        let tolerance = self.tolerance;
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let bytes = match Limited::new(body, MAX_MESSAGE_BYTES + 5).collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(_) => {
                    return Ok(Status::resource_exhausted(
                        "webhook request is unreadable or too large",
                    )
                    .into_http());
                }
            };
            if let Err(status) = check_signature(&parts.headers, &bytes, &secret, tolerance) {
                return Ok(status.into_http());
            }
            let request = http::Request::from_parts(parts, Body::new(Full::new(bytes)));
            inner.call(request).await
        })
    }
}

/// Verify the signature headers against the single gRPC message in `body`.
fn check_signature(
    headers: &http::HeaderMap,
    body: &[u8],
    secret: &[u8],
    tolerance: Duration,
) -> Result<(), Status> {
    let payload = grpc_message(body).ok_or_else(|| {
        Status::unauthenticated("cannot verify a compressed or malformed webhook request")
    })?;
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    webhook_signature::verify(
        secret,
        header(SIGNATURE_HEADER),
        header(TIMESTAMP_HEADER),
        payload,
        unix_now(),
        tolerance,
    )
    .map_err(|e| Status::unauthenticated(e.to_string()))
}

/// The message bytes of a unary gRPC request body: exactly one uncompressed
/// frame (1-byte compression flag, 4-byte big-endian length, message).
fn grpc_message(body: &[u8]) -> Option<&[u8]> {
    let (header, message) = body.split_at_checked(5)?;
    if header[0] != 0 {
        return None;
    }
    let len = u32::from_be_bytes(header[1..5].try_into().ok()?) as usize;
    (message.len() == len).then_some(message)
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    use apollo_proto::webhook_client::WebhookClient;
    use prost::Message;
    use tokio::sync::{mpsc, oneshot};
    use tonic::metadata::MetadataValue;

    const SECRET: &str = "s3cret";

    struct Recorder(mpsc::UnboundedSender<String>);

    #[tonic::async_trait]
    impl WebhookHandler for Recorder {
        async fn on_event(&self, event: TaskEvent) -> Result<Response<Ack>, Status> {
            let _ = self.0.send(event.task.unwrap_or_default().id);
            Ok(Response::new(Ack {}))
        }
    }

    fn task(id: &str) -> TaskEvent {
        TaskEvent {
            task: Some(apollo_proto::Task {
                id: id.into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Serve a receiver on a free port; returns a client to it, the handler's
    /// channel, and a shutdown trigger.
    async fn start(
        secret: Option<&str>,
    ) -> (
        WebhookClient<tonic::transport::Channel>,
        mpsc::UnboundedReceiver<String>,
        oneshot::Sender<()>,
    ) {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let addr: SocketAddr = ([127, 0, 0, 1], port).into();
        let (tx, rx) = mpsc::unbounded_channel();
        let (stop, stopped) = oneshot::channel::<()>();
        let mut receiver = WebhookReceiver::new(Recorder(tx));
        if let Some(s) = secret {
            receiver = receiver.secret(s);
        }
        tokio::spawn(receiver.serve_with_shutdown(addr, async {
            let _ = stopped.await;
        }));
        let url = format!("http://{addr}");
        for _ in 0..100 {
            if let Ok(client) = WebhookClient::connect(url.clone()).await {
                return (client, rx, stop);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("receiver did not start");
    }

    /// A request carrying `sent`, signed as if the payload were `signed_as` at
    /// `timestamp`.
    fn request(sent: &TaskEvent, signed_as: &TaskEvent, timestamp: i64) -> Request<TaskEvent> {
        let sig = webhook_signature::sign(SECRET.as_bytes(), timestamp, &signed_as.encode_to_vec());
        let mut req = Request::new(sent.clone());
        let md = req.metadata_mut();
        md.insert(
            TIMESTAMP_HEADER,
            MetadataValue::try_from(timestamp.to_string()).unwrap(),
        );
        md.insert(SIGNATURE_HEADER, MetadataValue::try_from(sig).unwrap());
        req
    }

    #[tokio::test]
    async fn accepts_valid_and_rejects_forged_deliveries() {
        let (mut client, mut seen, stop) = start(Some(SECRET)).await;
        let now = unix_now();

        client
            .notify(request(&task("ok"), &task("ok"), now))
            .await
            .unwrap();
        assert_eq!(seen.recv().await.unwrap(), "ok");

        // A captured signature does not cover a different payload...
        let err = client
            .notify(request(&task("forged"), &task("ok"), now))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("invalid"), "{}", err.message());

        // ...nor can an old delivery be replayed.
        let old = now - 3600;
        let err = client
            .notify(request(&task("ok"), &task("ok"), old))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("window"), "{}", err.message());

        // Unsigned deliveries are refused.
        let err = client.notify(task("plain")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);

        assert!(
            seen.try_recv().is_err(),
            "no forged delivery reached the handler"
        );
        let _ = stop.send(());
    }

    #[tokio::test]
    async fn without_a_secret_everything_is_accepted() {
        let (mut client, mut seen, stop) = start(None).await;
        client.notify(task("plain")).await.unwrap();
        assert_eq!(seen.recv().await.unwrap(), "plain");
        let _ = stop.send(());
    }

    #[test]
    fn grpc_frames_are_parsed_strictly() {
        assert_eq!(grpc_message(&[0, 0, 0, 0, 2, 7, 8]), Some(&[7u8, 8][..]));
        assert_eq!(grpc_message(&[0, 0, 0, 0, 0]), Some(&[][..]));
        assert_eq!(grpc_message(&[1, 0, 0, 0, 2, 7, 8]), None); // compressed
        assert_eq!(grpc_message(&[0, 0, 0, 0, 3, 7, 8]), None); // truncated
        assert_eq!(grpc_message(&[0, 0, 0, 0, 1, 7, 8]), None); // trailing bytes
        assert_eq!(grpc_message(&[0, 0]), None);
    }
}
