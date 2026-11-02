//! Priority admission gate.
//!
//! A fair, priority-ordered replacement for a counting semaphore. It caps the
//! number of items processing concurrently (the global `max_concurrent` ceiling)
//! and, when slots are scarce, hands the next freed slot to the highest-priority
//! waiter rather than the longest-waiting one. Ties break FIFO (lower sequence
//! number first), so equal-priority items keep submission order. An item's
//! priority is the maximum `priority` among the models it targets — so a
//! high-priority item jumps ahead of earlier-queued low-priority ones.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::oneshot;

/// Orders waiters: higher priority first, then earlier arrival (smaller
/// sequence number), so the map's first entry is the next to be served.
type WaiterKey = (Reverse<i32>, u64);

struct GateInner {
    total: usize,
    available: usize,
    next_seq: u64,
    /// Ordered so serving the next waiter and dropping any one are both
    /// O(log n).
    waiters: BTreeMap<WaiterKey, oneshot::Sender<()>>,
}

/// A priority-ordered concurrency gate.
pub(crate) struct PriorityGate {
    inner: Mutex<GateInner>,
}

impl PriorityGate {
    pub(crate) fn new(slots: usize) -> Self {
        PriorityGate {
            inner: Mutex::new(GateInner {
                total: slots.max(1),
                available: slots.max(1),
                next_seq: 0,
                waiters: BTreeMap::new(),
            }),
        }
    }

    /// The state, also after a panic elsewhere held the lock: its updates are
    /// single steps, so it's never left half-changed.
    fn state(&self) -> MutexGuard<'_, GateInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `(total slots, slots in use, waiters)` right now.
    pub(crate) fn snapshot(&self) -> (usize, usize, usize) {
        let g = self.state();
        (g.total, g.total - g.available, g.waiters.len())
    }

    /// Acquire a slot, waiting if necessary. Higher `priority` is served first;
    /// the returned permit releases the slot on drop. Dropping the wait before
    /// it is granted (a cancelled item) leaves the queue, and gives back a slot
    /// handed over meanwhile.
    pub(crate) async fn acquire(self: Arc<Self>, priority: i32) -> GatePermit {
        let mut pending = {
            let mut g = self.state();
            if g.available > 0 {
                g.available -= 1;
                return GatePermit {
                    gate: Arc::clone(&self),
                };
            }
            let (tx, rx) = oneshot::channel();
            let key = (Reverse(priority), g.next_seq);
            g.next_seq += 1;
            g.waiters.insert(key, tx);
            Pending {
                gate: Some(Arc::clone(&self)),
                key,
                rx,
            }
        };
        // A releasing permit hands the slot over directly. A dropped sender (only
        // at shutdown) is treated as a grant so callers never wedge.
        let _ = (&mut pending.rx).await;
        pending.gate = None; // granted: the permit owns the slot now
        GatePermit { gate: self }
    }

    fn release(&self) {
        let mut g = self.state();
        // Transfer the freed slot to the highest-priority live waiter, if any;
        // otherwise return it to the pool.
        while let Some((_, tx)) = g.waiters.pop_first() {
            if tx.send(()).is_ok() {
                return;
            }
            // Receiver gone (cancelled) — discard and try the next.
        }
        g.available += 1;
    }
}

/// A wait for a slot that hasn't been granted yet.
struct Pending {
    /// `None` once granted.
    gate: Option<Arc<PriorityGate>>,
    key: WaiterKey,
    rx: oneshot::Receiver<()>,
}

impl Drop for Pending {
    fn drop(&mut self) {
        let Some(gate) = self.gate.take() else {
            return;
        };
        // Leave the queue, then refuse any later hand-over; a slot handed over
        // before that goes back to the gate.
        gate.state().waiters.remove(&self.key);
        self.rx.close();
        if self.rx.try_recv().is_ok() {
            gate.release();
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn higher_priority_goes_first_then_arrival_order() {
        let gate = Arc::new(PriorityGate::new(1));
        let held = gate.clone().acquire(0).await;
        let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel();
        for (name, priority) in [("a0", 0), ("b5", 5), ("c0", 0), ("d5", 5)] {
            let waiter = gate.clone();
            let order_tx = order_tx.clone();
            tokio::spawn(async move {
                let _permit = waiter.acquire(priority).await;
                order_tx.send(name).unwrap();
            });
            // Queue them in this order.
            while gate.snapshot().2 < name_count(name) {
                tokio::task::yield_now().await;
            }
        }
        drop(held);
        let mut served = Vec::new();
        for _ in 0..4 {
            served.push(order_rx.recv().await.unwrap());
        }
        assert_eq!(served, ["b5", "d5", "a0", "c0"]);
    }

    /// How many waiters are queued once `name` is (they queue in name order).
    fn name_count(name: &str) -> usize {
        "abcd".find(&name[..1]).unwrap() + 1
    }

    #[tokio::test]
    async fn a_dropped_wait_leaves_the_queue() {
        let gate = Arc::new(PriorityGate::new(1));
        let held = gate.clone().acquire(0).await;
        let wait = tokio::spawn(gate.clone().acquire(0));
        tokio::task::yield_now().await;
        while gate.snapshot().2 == 0 {
            tokio::task::yield_now().await;
        }
        wait.abort();
        let _ = wait.await;
        assert_eq!(
            gate.snapshot(),
            (1, 1, 0),
            "the cancelled wait left the queue"
        );
        drop(held);
        assert_eq!(gate.snapshot(), (1, 0, 0), "the slot went back to the pool");
    }

    #[tokio::test]
    async fn a_slot_handed_to_a_dropped_wait_comes_back() {
        let gate = Arc::new(PriorityGate::new(1));
        let held = gate.clone().acquire(0).await;
        let mut wait = Box::pin(gate.clone().acquire(0));
        // Poll once so it joins the queue, then hand it the slot without
        // letting it see the grant.
        assert!(futures::poll!(wait.as_mut()).is_pending());
        drop(held);
        assert_eq!(
            gate.snapshot(),
            (1, 1, 0),
            "the slot is in flight to the wait"
        );
        drop(wait);
        assert_eq!(gate.snapshot(), (1, 0, 0), "not lost with the dropped wait");
        let _again = gate.clone().acquire(0).await;
    }
}
