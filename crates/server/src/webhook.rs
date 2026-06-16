//! Outbound webhook client: the concrete [`WebhookSink`] injected into the engine.
//!
//! Delivers `TaskStatus` to the receiver configured by `[webhook].url` over a
//! lazily-connected, auto-reconnecting channel. The engine decides *when* to fire
//! and owns the persisted delivered-flag; this type only performs the gRPC call.

use async_trait::async_trait;
use tonic::transport::{Channel, Endpoint};

use apollo_domain::Task;
use apollo_engine::{WebhookError, WebhookSink};
use apollo_proto::webhook_client::WebhookClient;

use crate::convert::task_to_proto;

/// A [`WebhookSink`] that POSTs task status to a gRPC `Webhook` receiver.
pub struct GrpcWebhookSink {
    channel: Channel,
}

impl GrpcWebhookSink {
    /// Target `url` (e.g. `http://127.0.0.1:9090`). The channel connects lazily,
    /// so the receiver need not be reachable at construction time.
    pub fn new(url: &str) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(url.to_string())?.connect_lazy();
        Ok(Self { channel })
    }
}

#[async_trait]
impl WebhookSink for GrpcWebhookSink {
    async fn deliver(&self, task: &Task, _item_index: usize) -> Result<(), WebhookError> {
        // The wire message is the full Task (bare, per the proto); the receiver
        // dedupes by id + which items are terminal.
        let mut client = WebhookClient::new(self.channel.clone());
        client
            .task_status(task_to_proto(task.clone()))
            .await
            .map_err(|status| WebhookError(status.to_string()))?;
        Ok(())
    }
}
