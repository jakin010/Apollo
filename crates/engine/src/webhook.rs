//! Outbound webhook delivery.
//!
//! The engine decides *when* to fire — each time an item changes state (it starts
//! processing, then reaches a terminal state) — and tracks a delivered flag for
//! terminal notifications so they survive restarts; the *how* (converting to the
//! wire format and making the gRPC call) is injected as a [`WebhookSink`] by the
//! server/app layer, so this crate stays free of the protobuf types.

use std::time::Duration;

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

/// Receives item state-change notifications. Terminal notifications are
/// at-least-once (a failed delivery is retried on the next startup); intermediate
/// notifications (e.g. an item entering `Processing`) are best-effort.
#[async_trait]
pub trait WebhookSink: Send + Sync {
    /// Deliver routine task status (the `TaskStatus` call): the current `task`,
    /// identifying which item just changed state.
    async fn deliver(&self, task: &Task, item_index: usize) -> Result<(), WebhookError>;
}

impl Engine {
    /// Load a task for delivery, logging and returning `None` on miss/error.
    async fn load_task_for_webhook(&self, task_id: &str) -> Option<Task> {
        match self.inner.storage.get_task(task_id).await {
            Ok(Some(t)) => Some(t),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(task = %task_id, error = %e, "webhook: could not load task");
                None
            }
        }
    }

    /// Push routine task status (`TaskStatus`). No-op returning `false` if no sink
    /// is configured or the task is missing. No bookkeeping.
    async fn push_webhook(&self, task_id: &str, item_index: usize) -> bool {
        let Some(sink) = self.inner.webhook.as_ref() else {
            return false;
        };
        let Some(task) = self.load_task_for_webhook(task_id).await else {
            return false;
        };
        match sink.deliver(&task, item_index).await {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(task = %task_id, item = item_index, error = %e, "webhook delivery failed");
                false
            }
        }
    }

    /// Fire-and-forget notification that an item changed state (e.g. it started
    /// processing). Spawned so a slow or unreachable receiver never blocks the
    /// scheduler; not tracked for redelivery (only terminal states are).
    pub(crate) async fn notify_item_change(&self, task_id: &str, item_index: usize) {
        if self.inner.webhook.is_none() {
            return;
        }
        let engine = self.clone();
        let task_id = task_id.to_string();
        tokio::spawn(async move {
            engine.push_webhook(&task_id, item_index).await;
        });
    }

    /// Fire the webhook for a terminal item. Spawned so it never blocks the
    /// scheduler; retries a few times with backoff and marks the item delivered on
    /// success, otherwise leaving it pending for the periodic redelivery loop (and,
    /// ultimately, restart recovery).
    pub(crate) async fn deliver_webhook(&self, task_id: &str, item_index: usize) {
        if self.inner.webhook.is_none() {
            return;
        }
        let engine = self.clone();
        let task_id = task_id.to_string();
        tokio::spawn(async move {
            for delay in [0u64, 1, 3] {
                if delay > 0 {
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                }
                if engine.push_webhook(&task_id, item_index).await {
                    let _ = engine
                        .inner
                        .storage
                        .mark_webhook_delivered(&task_id, item_index)
                        .await;
                    return;
                }
            }
            tracing::warn!(
                task = %task_id, item = item_index,
                "webhook undelivered after retries; left pending for redelivery"
            );
        });
    }

    /// Start the periodic redelivery loop: every `[webhook].redelivery_secs`,
    /// re-attempt every terminal item whose task-status or dead-letter webhook is
    /// still undelivered. No-op when no sink is configured or the interval is zero.
    pub(crate) fn spawn_redelivery(&self) {
        if self.inner.webhook.is_none() {
            return;
        }
        let secs = self
            .inner
            .config
            .webhook
            .as_ref()
            .map(|w| w.redelivery_secs)
            .unwrap_or(0);
        if secs == 0 {
            return;
        }
        let engine = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(secs as u64));
            tick.tick().await; // consume the immediate first tick
            loop {
                tick.tick().await;
                match engine.inner.storage.items_pending_webhook().await {
                    Ok(pending) if !pending.is_empty() => {
                        tracing::debug!(count = pending.len(), "redelivering pending webhooks");
                        for p in pending {
                            engine.deliver_webhook(&p.task_id, p.item_index).await;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "redelivery: could not list pending webhooks")
                    }
                }
            }
        });
    }
}
