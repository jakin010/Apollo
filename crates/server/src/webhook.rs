//! Outbound webhook client: the concrete [`WebhookSink`] injected into the engine.
//!
//! Delivers `TaskStatus` to the receiver configured by `[webhook].url` over a
//! lazily-connected, auto-reconnecting channel. When `[webhook].secret` is set,
//! each delivery carries an `x-apollo-webhook-signature` metadata header —
//! lowercase-hex HMAC-SHA256 of the task id — so the receiver can confirm the call
//! came from a holder of the secret (pair with a TLS `https://` URL for transport
//! confidentiality). The engine decides *when* to fire and owns the persisted
//! delivered-flag; this type only performs the gRPC call.

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};

use apollo_domain::Task;
use apollo_engine::{WebhookError, WebhookSink};
use apollo_proto::webhook_client::WebhookClient;

use crate::convert::task_to_proto;

/// Metadata header carrying the delivery signature.
const SIGNATURE_HEADER: &str = "x-apollo-webhook-signature";

/// A [`WebhookSink`] that delivers task status to a gRPC `Webhook` receiver.
pub struct GrpcWebhookSink {
    channel: Channel,
    secret: Option<String>,
}

impl GrpcWebhookSink {
    /// Target `url` (e.g. `http://127.0.0.1:9090`); `secret`, when set, enables
    /// HMAC signing. The channel connects lazily, so the receiver need not be
    /// reachable at construction time.
    pub fn new(url: &str, secret: Option<String>) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(url.to_string())?.connect_lazy();
        Ok(Self { channel, secret })
    }
}

#[async_trait]
impl WebhookSink for GrpcWebhookSink {
    async fn deliver(&self, task: &Task, _item_index: usize) -> Result<(), WebhookError> {
        // The wire message is the full Task (bare, per the proto); the receiver
        // dedupes by id + which items are terminal.
        let mut request = tonic::Request::new(task_to_proto(task.clone()));
        if let Some(secret) = &self.secret {
            let signature = sign(secret.as_bytes(), task.id.as_bytes());
            let value = MetadataValue::try_from(signature.as_str())
                .map_err(|e| WebhookError(format!("building signature header: {e}")))?;
            request.metadata_mut().insert(SIGNATURE_HEADER, value);
        }
        let mut client = WebhookClient::new(self.channel.clone());
        client
            .task_status(request)
            .await
            .map_err(|status| WebhookError(status.to_string()))?;
        Ok(())
    }
}

/// Lowercase-hex HMAC-SHA256 of `msg` keyed by `key`.
fn sign(key: &[u8], msg: &[u8]) -> String {
    use std::fmt::Write;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts a key of any length");
    mac.update(msg);
    let bytes = mac.finalize().into_bytes();
    let mut hex = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(hex, "{b:02x}");
    }
    hex
}
