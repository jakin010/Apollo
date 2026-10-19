//! Outbound webhook client: the concrete [`WebhookSink`] injected into the engine.
//!
//! Delivers `TaskEvent`s to the receiver configured by `[webhook].url` over a
//! lazily-connected, auto-reconnecting channel. When `[webhook].secret` is set,
//! each delivery is signed with scheme `v1` (see
//! [`apollo_proto::webhook_signature`]): an HMAC-SHA256 over the delivery
//! timestamp and the exact protobuf bytes sent, so a receiver can verify both
//! the sender and the payload, and reject replays. Pair with a TLS `https://` URL
//! for confidentiality. The engine decides *when* to fire and owns the persisted
//! delivered-flag; this type only performs the gRPC call.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use apollo_engine::{EventKind, TaskEvent, WebhookError, WebhookSink};
use apollo_proto::webhook_client::WebhookClient;
use apollo_proto::webhook_signature::{self, SIGNATURE_HEADER, TIMESTAMP_HEADER};

use crate::convert::{View, task_to_proto};

/// Deadline for establishing a connection to the receiver.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Deadline for one delivery call. A hung receiver must not pin delivery tasks
/// forever (the redelivery loop would otherwise keep stacking them up).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A [`WebhookSink`] that delivers task events to a gRPC `Webhook` receiver.
pub struct GrpcWebhookSink {
    channel: Channel,
    secret: Option<String>,
}

impl GrpcWebhookSink {
    /// Target `url` (e.g. `http://127.0.0.1:9090`, or `https://…` for TLS with
    /// the system / webpki trust roots); `secret`, when set, enables signing.
    /// The channel connects lazily, so the receiver need not be reachable at
    /// construction time.
    pub fn new(url: &str, secret: Option<String>) -> Result<Self, tonic::transport::Error> {
        let mut endpoint = Endpoint::from_shared(url.to_string())?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT);
        if endpoint.uri().scheme_str() == Some("https") {
            endpoint = endpoint.tls_config(ClientTlsConfig::new().with_enabled_roots())?;
        }
        Ok(Self {
            channel: endpoint.connect_lazy(),
            secret,
        })
    }

    /// Build the `TaskEvent` request (the task in its summary view), signed
    /// when a secret is set.
    fn build_request(
        &self,
        event: &TaskEvent,
    ) -> Result<tonic::Request<apollo_proto::TaskEvent>, WebhookError> {
        let timestamp = unix_now();
        let kind = match event.kind {
            EventKind::Progress => apollo_proto::TaskEventType::Progress,
            EventKind::Finished => apollo_proto::TaskEventType::Finished,
        };
        let mut request = tonic::Request::new(apollo_proto::TaskEvent {
            r#type: kind as i32,
            task: Some(task_to_proto(event.task.clone(), View::Summary)),
            attempt: event.attempt,
            sent_at: Some(apollo_proto::Timestamp {
                seconds: timestamp,
                nanos: 0,
            }),
        });
        if let Some(secret) = &self.secret {
            // Sign the exact bytes tonic will send: encoding is deterministic
            // (map entries sorted by key), so encoding this message again on the
            // wire reproduces `payload` byte for byte.
            let payload = request.get_ref().encode_to_vec();
            let signature = webhook_signature::sign(secret.as_bytes(), timestamp, &payload);
            let header = |v: &str| {
                MetadataValue::try_from(v)
                    .map_err(|e| WebhookError(format!("building signature header: {e}")))
            };
            let metadata = request.metadata_mut();
            metadata.insert(TIMESTAMP_HEADER, header(&timestamp.to_string())?);
            metadata.insert(SIGNATURE_HEADER, header(&signature)?);
        }
        Ok(request)
    }
}

#[async_trait]
impl WebhookSink for GrpcWebhookSink {
    async fn deliver(&self, event: &TaskEvent) -> Result<(), WebhookError> {
        // Built per attempt, so each retry carries a fresh timestamp.
        let request = self.build_request(event)?;
        let mut client = WebhookClient::new(self.channel.clone());
        client
            .notify(request)
            .await
            .map_err(|status| WebhookError(status.to_string()))?;
        Ok(())
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{SocketAddr, TcpListener};

    use apollo_client::{Ack, WebhookHandler, WebhookReceiver};
    use apollo_domain::{Task, TaskState};
    use tokio::sync::{mpsc, oneshot};
    use tonic::{Response, Status};

    fn task(id: &str) -> TaskEvent {
        let mut task = Task::new(id, vec![], None);
        task.state = TaskState::Completed;
        task.metadata = [("ref".to_string(), "42".to_string())].into();
        TaskEvent {
            kind: EventKind::Finished,
            task,
            attempt: 2,
        }
    }

    #[tokio::test]
    async fn https_receivers_use_tls() {
        // Nothing listens on port 1: the delivery must fail at the network layer,
        // not with tonic's "HTTPS without TLS enabled" (which meant TLS was never
        // compiled in and every https:// webhook silently failed).
        let sink = GrpcWebhookSink::new("https://127.0.0.1:1", None).unwrap();
        let err = sink.deliver(&task("t")).await.unwrap_err().to_string();
        assert!(!err.contains("without TLS"), "{err}");
    }

    #[tokio::test]
    async fn signature_covers_the_encoded_payload() {
        let sink = GrpcWebhookSink::new("http://127.0.0.1:1", Some("s3cret".into())).unwrap();
        let request = sink.build_request(&task("t1")).unwrap();
        let header = |name| request.metadata().get(name).unwrap().to_str().unwrap();
        let (sig, ts) = (header(SIGNATURE_HEADER), header(TIMESTAMP_HEADER));
        let payload = request.get_ref().encode_to_vec();
        let check = |secret: &[u8], payload: &[u8]| {
            webhook_signature::verify(
                secret,
                Some(sig),
                Some(ts),
                payload,
                unix_now(),
                webhook_signature::DEFAULT_TOLERANCE,
            )
        };
        assert_eq!(check(b"s3cret", &payload), Ok(()));
        // A different payload for the same task id does not verify.
        let mut other = request.get_ref().clone();
        let t = other.task.as_mut().unwrap();
        t.state = apollo_proto::TaskState::Cancelled as i32;
        assert_eq!(t.id, "t1");
        assert!(check(b"s3cret", &other.encode_to_vec()).is_err());
        assert!(check(b"wrong", &payload).is_err());
    }

    /// Records every delivery it accepts.
    struct Recorder(mpsc::UnboundedSender<apollo_proto::TaskEvent>);

    #[tonic::async_trait]
    impl WebhookHandler for Recorder {
        async fn on_event(&self, event: apollo_proto::TaskEvent) -> Result<Response<Ack>, Status> {
            let _ = self.0.send(event);
            Ok(Response::new(Ack {}))
        }
    }

    /// Serve a signature-verifying receiver on a free local port.
    async fn receiver(
        secret: &str,
    ) -> (
        String,
        mpsc::UnboundedReceiver<apollo_proto::TaskEvent>,
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
        let rcv = WebhookReceiver::new(Recorder(tx)).secret(secret);
        tokio::spawn(rcv.serve_with_shutdown(addr, async {
            let _ = stopped.await;
        }));
        // Wait until it accepts connections.
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        (format!("http://{addr}"), rx, stop)
    }

    #[tokio::test]
    async fn signed_delivery_is_accepted_end_to_end() {
        let (url, mut seen, stop) = receiver("s3cret").await;

        let sink = GrpcWebhookSink::new(&url, Some("s3cret".into())).unwrap();
        sink.deliver(&task("ok")).await.unwrap();
        let event = seen.recv().await.unwrap();
        assert_eq!(event.r#type(), apollo_proto::TaskEventType::Finished);
        assert_eq!(event.attempt, 2);
        assert!(event.sent_at.is_some());
        let delivered = event.task.unwrap();
        assert_eq!(delivered.id, "ok");
        assert_eq!(delivered.metadata["ref"], "42");

        let wrong = GrpcWebhookSink::new(&url, Some("wrong".into())).unwrap();
        let err = wrong.deliver(&task("bad")).await.unwrap_err().to_string();
        assert!(err.contains("invalid webhook signature"), "{err}");

        let unsigned = GrpcWebhookSink::new(&url, None).unwrap();
        let err = unsigned
            .deliver(&task("none"))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing webhook signature"), "{err}");

        // Only the correctly signed delivery reached the handler.
        assert!(seen.try_recv().is_err());
        let _ = stop.send(());
    }
}
