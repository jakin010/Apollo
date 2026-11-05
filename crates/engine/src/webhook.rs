//! Outbound webhook delivery.
//!
//! The engine decides *when* to fire — a progress event each time the task moves
//! on (processing, retrying), and a finished event once it is completed, failed
//! or cancelled — and tracks a delivered flag for finished events so they
//! survive restarts; the *how* (converting to the wire format and making the
//! gRPC call) is injected as a [`WebhookSink`] by the server/app layer, so this
//! crate stays free of the protobuf types.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use apollo_domain::Task;
use apollo_storage::{ClaimMode, Delivery, PendingWebhook};

use crate::live::Live;
use crate::{Engine, Inner, aggregate};

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

    /// The receiver this sink delivers to. Instances sharing a database send
    /// each other's pending events only when their sinks' targets are equal.
    fn target(&self) -> String;
}

/// The most finished webhooks this process delivers at once: their claims
/// are renewed in one go, which must fit in a renewal period.
const MAX_DELIVERING: usize = 512;

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
        if self.live().webhook.is_none() || self.lost() {
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

    /// What a write that finishes an item records about its finished webhook:
    /// with webhooks on, this process's claim to deliver it, for the current
    /// webhook's receiver; otherwise delivered (nothing is kept to send).
    pub(crate) fn delivery<'a>(&'a self, live: &'a Live) -> Delivery<'a> {
        match &live.webhook_target {
            Some(target) => Delivery::Claim {
                holder: &self.inner.holder,
                target,
                ttl: self.inner.lease_ttl,
            },
            None => Delivery::Off,
        }
    }

    /// Fire the finished event for a terminal item, whose delivery claim this
    /// process holds (its finishing write stamped it, or a redelivery pass
    /// took it). Spawned so it never blocks the scheduler; retries a few times
    /// with backoff and marks the item delivered on success. Otherwise it
    /// hands the claim back, leaving the item to the redelivery loop of any
    /// instance with the same receiver (and, ultimately, restart recovery).
    /// It stops once another instance has taken the claim over. With webhooks
    /// off the item is marked delivered at once: nothing is kept to send once
    /// they're on again.
    pub(crate) async fn deliver_webhook(&self, task_id: &str, item_index: usize) {
        if let Some(delivering) = self.start_delivery(task_id, item_index) {
            self.send_webhook(delivering).await;
        }
    }

    /// Register the delivery of an item's finished webhook, as soon as its
    /// finishing write won: from then on its claim is renewed, also while the
    /// task is rolled up. `None` if this process is delivering it already.
    pub(crate) fn start_delivery(&self, task_id: &str, item_index: usize) -> Option<Delivering> {
        Delivering::start(&self.inner, task_id, item_index)
    }

    /// Send the finished webhook `delivering` registered (see
    /// [`Engine::deliver_webhook`]).
    pub(crate) async fn send_webhook(&self, delivering: Delivering) {
        // The current webhook, also for a task pinned to an older config.
        let engine = self.current();
        let (task_id, item_index) = delivering.key.clone();
        if engine.live().webhook.is_none() {
            engine.mark_delivered(&task_id, item_index).await;
            return;
        }
        tokio::spawn(async move {
            for (attempt, delay) in (1..).zip([0u64, 1, 3]) {
                if delay > 0 {
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                }
                if delivering.lost() {
                    tracing::info!(task = %task_id, item = item_index, "another instance took the webhook delivery over");
                    return;
                }
                let pushed = engine
                    .push_webhook(&task_id, item_index, EventKind::Finished, attempt)
                    .await;
                // Sent, or webhooks were turned off meanwhile.
                if pushed != Pushed::Failed {
                    engine.mark_delivered(&task_id, item_index).await;
                    return;
                }
            }
            if let Err(e) = engine
                .inner
                .storage
                .release_webhook_claim(&task_id, item_index, &engine.inner.holder)
                .await
            {
                tracing::warn!(task = %task_id, item = item_index, error = %e, "could not hand back a webhook delivery");
            }
            tracing::warn!(
                task = %task_id, item = item_index,
                "webhook undelivered after retries; left pending for redelivery"
            );
        });
    }

    /// Mark an item's webhook delivered, if this process still holds its
    /// delivery claim.
    async fn mark_delivered(&self, task_id: &str, item_index: usize) {
        match self
            .inner
            .storage
            .mark_webhook_delivered(task_id, item_index, &self.inner.holder)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::debug!(task = %task_id, item = item_index, "webhook already marked, or another instance had taken its delivery over")
            }
            Err(e) => {
                tracing::warn!(task = %task_id, item = item_index, error = %e, "could not mark a webhook delivered")
            }
        }
    }

    /// Take the delivery of the finished webhooks still pending that this
    /// instance may send, and send them. On a database only this process uses
    /// (SQLite) that is every one; on a shared one, those for this instance's
    /// receiver that nobody holds, and once this instance is settled those
    /// whose claim lapsed (their holder stopped). With webhooks off on a shared
    /// database it takes nothing: other instances' events are theirs to send.
    /// Returns whether more may be left (it took as many as it may at once).
    pub(crate) async fn redeliver_pending(&self) -> bool {
        // A stopping instance takes nothing new: its claims would lapse with
        // the delivery perhaps sent, and be sent again.
        if self.inner.closing.load(Ordering::SeqCst) {
            return false;
        }
        let engine = self.current();
        let live = engine.live();
        let storage = &engine.inner.storage;
        let shared = storage.shared();
        // On a shared database no more than one renewal can keep up with;
        // SQLite renews nothing.
        let room = if shared {
            MAX_DELIVERING.saturating_sub(
                self.inner
                    .deliveries
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .len(),
            )
        } else {
            i64::MAX as usize
        };
        if room == 0 {
            return true;
        }
        let (target, mode) = if shared {
            let Some(target) = live.webhook_target.as_deref() else {
                return false;
            };
            let mode = if engine.settled() {
                ClaimMode::Expired
            } else {
                ClaimMode::Open
            };
            (Some(target), mode)
        } else {
            (None, ClaimMode::Force)
        };
        let pending = match storage
            .claim_pending_webhooks(
                &engine.inner.holder,
                target,
                mode,
                engine.inner.lease_ttl,
                room,
            )
            .await
        {
            Ok(pending) => {
                if shared {
                    engine.connected(true);
                }
                pending
            }
            Err(e) => {
                if shared {
                    engine.connected(false);
                }
                tracing::warn!(error = %e, "redelivery: could not claim pending webhooks");
                return false;
            }
        };
        let more = pending.len() >= room;
        // Stopping meanwhile: give them back rather than leave them to lapse.
        if self.inner.closing.load(Ordering::SeqCst) {
            for p in &pending {
                let _ = storage
                    .release_webhook_claim(&p.task_id, p.item_index, &engine.inner.holder)
                    .await;
            }
            return false;
        }
        if !pending.is_empty() {
            tracing::debug!(count = pending.len(), "redelivering pending webhooks");
        }
        for p in pending {
            engine.deliver_webhook(&p.task_id, p.item_index).await;
        }
        more
    }

    /// Renew the delivery claims of the webhooks this process is delivering,
    /// waiting a renewal period at most, and stop the deliveries whose claim
    /// another instance has taken over.
    pub(crate) async fn renew_deliveries(&self) {
        let delivering: Vec<(PendingWebhook, Arc<AtomicBool>)> = self
            .inner
            .deliveries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|((task_id, item_index), lost)| {
                let item = PendingWebhook {
                    task_id: task_id.clone(),
                    item_index: *item_index,
                };
                (item, lost.clone())
            })
            .collect();
        if delivering.is_empty() {
            return;
        }
        let items: Vec<PendingWebhook> = delivering.iter().map(|(p, _)| p.clone()).collect();
        let limit = self.renew_every();
        let renewal = self.inner.storage.renew_webhook_claims(
            &self.inner.holder,
            &items,
            self.inner.lease_ttl,
        );
        match tokio::time::timeout(limit, renewal).await {
            Ok(Ok(kept)) => {
                for ((p, lost), kept) in delivering.iter().zip(kept) {
                    if !kept && !lost.swap(true, Ordering::SeqCst) {
                        tracing::info!(task = %p.task_id, item = p.item_index, "lost a webhook delivery claim; leaving the delivery to its new holder");
                    }
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "could not renew webhook delivery claims");
                self.connected(false);
            }
            Err(_) => {
                tracing::warn!(after = ?limit, "renewing webhook delivery claims timed out");
                self.connected(false);
            }
        }
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
                // A backlog is taken a bounded batch at a time.
                while engine.redeliver_pending().await {
                    tokio::time::sleep(engine.renew_every()).await;
                }
            }
        });
    }
}

/// A delivery this process is making, in [`Inner::deliveries`] until dropped.
pub(crate) struct Delivering {
    inner: Arc<Inner>,
    key: (String, usize),
    lost: Arc<AtomicBool>,
}

impl Delivering {
    /// Register the delivery of `task_id`'s item; `None` if this process is
    /// delivering it already.
    fn start(inner: &Arc<Inner>, task_id: &str, item_index: usize) -> Option<Self> {
        let key = (task_id.to_string(), item_index);
        let lost = Arc::new(AtomicBool::new(false));
        let mut deliveries = inner.deliveries.lock().unwrap_or_else(|e| e.into_inner());
        // Already being delivered here (a claim taken meanwhile waits for its
        // claim to lapse, a delay rather than a second send).
        if deliveries.contains_key(&key) {
            return None;
        }
        deliveries.insert(key.clone(), lost.clone());
        Some(Self {
            inner: inner.clone(),
            key,
            lost,
        })
    }

    /// Another instance has taken the delivery claim over.
    fn lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst)
    }
}

impl Drop for Delivering {
    fn drop(&mut self) {
        let mut deliveries = self
            .inner
            .deliveries
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if deliveries
            .get(&self.key)
            .is_some_and(|lost| Arc::ptr_eq(lost, &self.lost))
        {
            deliveries.remove(&self.key);
        }
    }
}
