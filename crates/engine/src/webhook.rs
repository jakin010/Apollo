//! Outbound webhook delivery.
//!
//! The engine decides *when* to fire (each time an item reaches a terminal state)
//! and tracks the delivered flag; the *how* — converting to the wire format and
//! making the gRPC call — is injected as a [`WebhookSink`] by the server/app layer,
//! so this crate stays free of the protobuf types.

use async_trait::async_trait;

use apollo_domain::Task;

use crate::Engine;

/// An error from a webhook delivery attempt.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct WebhookError(pub String);

impl From<String> for WebhookError {
    fn from(s: String) -> Self {
        WebhookError(s)
    }
}

/// Receives terminal-item notifications. Delivery is at-least-once: a failure is
/// left undelivered and retried on the next startup.
#[async_trait]
pub trait WebhookSink: Send + Sync {
    /// Deliver the current `task`, identifying which item just became terminal.
    async fn deliver(&self, task: &Task, item_index: usize) -> Result<(), WebhookError>;
}

impl Engine {
    /// Fire the webhook for one terminal item, marking it delivered on success.
    /// No-op when no sink is configured. Failures are logged and left pending.
    pub(crate) async fn deliver_webhook(&self, task_id: &str, item_index: usize) {
        let Some(sink) = self.inner.webhook.as_ref() else {
            return;
        };
        let task = match self.inner.storage.get_task(task_id).await {
            Ok(Some(t)) => t,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(task = %task_id, error = %e, "webhook: could not load task");
                return;
            }
        };
        match sink.deliver(&task, item_index).await {
            Ok(()) => {
                let _ = self
                    .inner
                    .storage
                    .mark_webhook_delivered(task_id, item_index)
                    .await;
            }
            Err(e) => tracing::warn!(
                task = %task_id, item = item_index, error = %e,
                "webhook delivery failed; will retry on restart"
            ),
        }
    }
}
