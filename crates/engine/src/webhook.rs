//! Outbound webhook delivery.
//!
//! The engine decides *when* to fire — a progress event each time the task moves
//! on (processing, retrying), and a finished event once it is completed, failed
//! or cancelled — and tracks a delivered flag for finished events so they
//! survive restarts; the *how* (converting to the wire format and making the
//! gRPC call) is injected as a [`WebhookSink`] by the server/app layer, so this
//! crate stays free of the protobuf types.

use std::time::Duration;

use async_trait::async_trait;

use apollo_domain::Task;

use crate::Engine;
use crate::aggregate;

/// An error from a webhook delivery attempt.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct WebhookError(pub String);

impl From<String> for WebhookError {
    fn from(s: String) -> Self {
        WebhookError(s)
    }
}

/// What a [`TaskEvent`] reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// The task moved on but has not finished.
    Progress,
    /// The task is completed, failed or cancelled.
    Finished,
}

/// One webhook notification: the task as it is now.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskEvent {
    pub kind: EventKind,
    pub task: Task,
    /// Delivery attempt for this event, from 1 (counted again after a restart).
    pub attempt: u32,
}

/// Receives task events. Finished events are at-least-once (a failed delivery
/// is retried periodically and on the next startup); progress events are
/// best-effort.
// `async_trait` marks the boxed-future methods `#[must_use]`; clippy >= 1.99
// reports that as `double_must_use`.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait WebhookSink: Send + Sync {
    async fn deliver(&self, event: &TaskEvent) -> Result<(), WebhookError>;
}

/// What became of one [`Engine::push_webhook`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pushed {
    /// The receiver took it, or none was needed (a progress event for a task
    /// that has finished meanwhile).
    Done,
    /// Not sent: the task couldn't be read or rolled up, or delivery failed.
    Failed,
    /// Webhooks are off.
    Off,
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

    /// Send the task's current state as a `kind` event. A progress event for a
    /// task that has finished meanwhile is dropped (its finished event follows),
    /// and a finished event for a task with items still running goes out as
    /// progress. A finished event for a task whose items have all finished but
    /// that isn't rolled up yet rolls it up first. No bookkeeping.
    async fn push_webhook(
        &self,
        task_id: &str,
        item_index: usize,
        kind: EventKind,
        attempt: u32,
    ) -> Pushed {
        let Some(sink) = self.live().webhook.clone() else {
            return Pushed::Off;
        };
        let Some(mut task) = self.load_task_for_webhook(task_id).await else {
            return Pushed::Failed;
        };
        if kind == EventKind::Finished
            && !aggregate::task_terminal(task.state)
            && aggregate::task_terminal(aggregate::task_state_for(&task))
        {
            // The task's rollup didn't land (a storage error as its last item
            // finished): do it now, so the finished event goes out rather than
            // a progress one taken for it. Failing that, it stays pending.
            self.rollup_task_state(task_id).await;
            match self.load_task_for_webhook(task_id).await {
                Some(t) if aggregate::task_terminal(t.state) => task = t,
                _ => return Pushed::Failed,
            }
        }
        let finished = aggregate::task_terminal(task.state);
        let kind = match kind {
            EventKind::Progress if finished => return Pushed::Done,
            EventKind::Finished if !finished => EventKind::Progress,
            kind => kind,
        };
        let event = TaskEvent {
            kind,
            task,
            attempt,
        };
        let name = match kind {
            EventKind::Progress => "progress",
            EventKind::Finished => "finished",
        };
        let delivered = sink.deliver(&event).await;
        self.inner.metrics.webhook(name, delivered.is_ok());
        match delivered {
            Ok(()) => Pushed::Done,
            Err(e) => {
                tracing::warn!(task = %task_id, item = item_index, error = %e, "webhook delivery failed");
                Pushed::Failed
            }
        }
    }

    /// Fire-and-forget progress event: an item changed state (e.g. it started
    /// processing). Spawned so a slow or unreachable receiver never blocks the
    /// scheduler; not tracked for redelivery (only terminal states are).
    pub(crate) async fn notify_item_change(&self, task_id: &str, item_index: usize) {
        if self.live().webhook.is_none() {
            return;
        }
        // The current webhook, also for a task pinned to an older config.
        let engine = self.current();
        let task_id = task_id.to_string();
        tokio::spawn(async move {
            engine
                .push_webhook(&task_id, item_index, EventKind::Progress, 1)
                .await;
        });
    }

    /// Fire the finished event for a terminal item. Spawned so it never blocks the
    /// scheduler; retries a few times with backoff and marks the item delivered on
    /// success, otherwise leaving it pending for the periodic redelivery loop (and,
    /// ultimately, restart recovery). With webhooks off the item is marked
    /// delivered at once: nothing is kept to send once they're on again.
    pub(crate) async fn deliver_webhook(&self, task_id: &str, item_index: usize) {
        // The current webhook, also for a task pinned to an older config.
        let engine = self.current();
        if engine.live().webhook.is_none() {
            let _ = engine
                .inner
                .storage
                .mark_webhook_delivered(task_id, item_index)
                .await;
            return;
        }
        let task_id = task_id.to_string();
        tokio::spawn(async move {
            for (attempt, delay) in (1..).zip([0u64, 1, 3]) {
                if delay > 0 {
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                }
                let pushed = engine
                    .push_webhook(&task_id, item_index, EventKind::Finished, attempt)
                    .await;
                // Sent, or webhooks were turned off meanwhile.
                if pushed != Pushed::Failed {
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
    /// re-attempt every finished item whose webhook event is still
    /// undelivered. The interval is read from the current config each time, so
    /// a reload that adds, removes or retimes the webhook applies; while there
    /// is no webhook (or the interval is zero) the loop only checks back.
    pub(crate) fn spawn_redelivery(&self) {
        /// How often to look again while redelivery is off.
        const IDLE_CHECK: Duration = Duration::from_secs(30);
        let engine = self.clone();
        tokio::spawn(async move {
            loop {
                let live = engine.live();
                let secs = match (&live.webhook, &live.config.webhook) {
                    (Some(_), Some(w)) => w.redelivery_secs,
                    _ => 0,
                };
                drop(live);
                if secs == 0 {
                    tokio::time::sleep(IDLE_CHECK).await;
                    continue;
                }
                tokio::time::sleep(Duration::from_secs(secs.into())).await;
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
