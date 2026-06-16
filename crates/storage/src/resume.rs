//! Resume support.
//!
//! Persistence is at per-(item, model) granularity. On restart the engine loads
//! [`Storage::load_incomplete_tasks`](crate::Storage::load_incomplete_tasks),
//! reuses every result already `Done`, and re-runs only what's left. An
//! interrupted video frame-scan continues from its saved frames: they are written
//! incrementally via [`append_frame`](crate::Storage::append_frame) and replayed
//! by re-running the strategy from the top with the dedupe set seeded from
//! [`load_frames`](crate::Storage::load_frames). Deterministic sampling guarantees
//! the already-classified frames are skipped exactly, and
//! [`steps_completed`](crate::Storage::steps_completed) lets fully-finished early
//! steps be skipped outright. A per-task attempt counter
//! ([`increment_attempts`](crate::Storage::increment_attempts)) caps retries so a
//! poison task can't crash-loop.

/// A terminal item awaiting webhook delivery, returned by
/// [`items_pending_webhook`](crate::Storage::items_pending_webhook).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingWebhook {
    pub task_id: String,
    pub item_index: usize,
}
