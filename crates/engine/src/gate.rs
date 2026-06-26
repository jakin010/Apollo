//! Priority admission gate.
//!
//! A fair, priority-ordered replacement for a counting semaphore. It caps the
//! number of items processing concurrently (the global `max_concurrent` ceiling)
//! and, when slots are scarce, hands the next freed slot to the highest-priority
//! waiter rather than the longest-waiting one. Ties break FIFO (lower sequence
//! number first), so equal-priority items keep submission order. An item's
//! priority is the maximum `priority` among the models it targets — so a
//! high-priority item jumps ahead of earlier-queued low-priority ones.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

/// A queued acquirer waiting for a slot.
struct Waiter {
    priority: i32,
    seq: u64,
    tx: oneshot::Sender<()>,
}

impl PartialEq for Waiter {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}
impl Eq for Waiter {}
impl Ord for Waiter {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority wins; within a priority the earlier arrival (smaller
        // seq) wins. BinaryHeap pops the max, so a smaller seq must compare as
        // greater — hence the reversed seq comparison.
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for Waiter {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct GateInner {
    available: usize,
    next_seq: u64,
    waiters: BinaryHeap<Waiter>,
}

/// A priority-ordered concurrency gate.
pub(crate) struct PriorityGate {
    inner: Mutex<GateInner>,
}

impl PriorityGate {
    pub(crate) fn new(slots: usize) -> Self {
        PriorityGate {
            inner: Mutex::new(GateInner {
                available: slots.max(1),
                next_seq: 0,
                waiters: BinaryHeap::new(),
            }),
        }
    }

    /// Acquire a slot, waiting if necessary. Higher `priority` is served first;
    /// the returned permit releases the slot on drop.
    pub(crate) async fn acquire(self: Arc<Self>, priority: i32) -> GatePermit {
        let rx = {
            let mut g = self.inner.lock().unwrap();
            if g.available > 0 {
                g.available -= 1;
                return GatePermit {
                    gate: Arc::clone(&self),
                };
            }
            let (tx, rx) = oneshot::channel();
            let seq = g.next_seq;
            g.next_seq += 1;
            g.waiters.push(Waiter { priority, seq, tx });
            rx
        };
        // A releasing permit hands the slot over directly. A dropped sender (only
        // at shutdown) is treated as a grant so callers never wedge.
        let _ = rx.await;
        GatePermit { gate: self }
    }

    fn release(&self) {
        let mut g = self.inner.lock().unwrap();
        // Transfer the freed slot to the highest-priority live waiter, if any;
        // otherwise return it to the pool.
        while let Some(w) = g.waiters.pop() {
            if w.tx.send(()).is_ok() {
                return;
            }
            // Receiver gone (cancelled) — discard and try the next.
        }
        g.available += 1;
    }
}

/// Permit for one admitted slot. Releasing on drop wakes the next waiter.
pub(crate) struct GatePermit {
    gate: Arc<PriorityGate>,
}

impl Drop for GatePermit {
    fn drop(&mut self) {
        self.gate.release();
    }
}
