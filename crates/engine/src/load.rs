//! The load figure load balancers read, and whether this instance is in
//! touch with a shared database. A sampler takes both every second into the
//! [`Lifecycle`], so probes read them without waiting on anything.
//!
//! - **Utilization** (0 to 1) is how close the instance is to refusing work:
//!   inputs in flight over `[app].max_pending`, or memory in use over
//!   `[app].max_memory`, whichever is higher. Without a `max_pending`, the
//!   busy and waiting slots over all slots stand in for the queue. It rises at
//!   once and falls with a time constant of 5 s.
//! - **Weight** is `100·(1−u)` in steps of 5, from 1 to 100; 1 while the
//!   database is stale, and 0 when not ready.
//! - **Degraded:** `load` from `[health].degraded_at` until 0.15 below it
//!   (or half of it, if higher), each state held at least 10 s; `database`
//!   while the database is stale.
//! - **Database** (shared only): stale 5 s after a call to it failed or timed
//!   out, or with no answer for two leases; ok again after a third of a lease
//!   in touch.

use std::future::Future;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::time::{Instant, MissedTickBehavior};

use crate::{Engine, Lifecycle, memory};

/// How often the load is sampled.
const PERIOD: Duration = Duration::from_secs(1);
/// How fast utilization falls: to about a third of a peak in 5 s.
const TAU: Duration = Duration::from_secs(5);
/// How far below `[health].degraded_at` utilization must fall to clear `load`.
const HYSTERESIS: f64 = 0.15;
/// The shortest time `load` stays set or clear.
const DWELL: Duration = Duration::from_secs(10);
/// How long out of touch makes the database stale.
const OUT_OF_TOUCH: Duration = Duration::from_secs(5);

/// The reasons an instance can be degraded, in the order they are listed.
pub const DEGRADED: [&str; 2] = ["load", "database"];

/// The latest sample, as the lifecycle holds it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LoadSnapshot {
    /// When it was taken; `None` until the engine takes the first.
    pub sampled_at: Option<Instant>,
    /// 0 to 1: how close the instance is to refusing work.
    pub utilization: f64,
    /// Inputs accepted and not finished.
    pub in_flight: usize,
    /// `[app].max_pending`; 0 = no limit.
    pub max_pending: usize,
    /// Concurrency slots: all of them, those in use, and the inputs waiting.
    pub slots_total: usize,
    pub slots_busy: usize,
    pub slots_waiting: usize,
    /// Memory in use over `[app].max_memory`, when both are known.
    pub memory: Option<f64>,
    /// Degraded for load: utilization reached `[health].degraded_at`.
    pub overloaded: bool,
    pub database: Database,
    /// The models with a worker.
    pub models: Vec<ModelLoad>,
    /// The running config's generation, and the changed settings that wait
    /// for a restart.
    pub generation: u64,
    pub restart_required: Vec<String>,
}

impl LoadSnapshot {
    /// Inputs it would still accept; `None` without a `max_pending`.
    pub fn free(&self) -> Option<usize> {
        (self.max_pending > 0).then(|| self.max_pending.saturating_sub(self.in_flight))
    }

    /// Why it is degraded, from [`DEGRADED`]; empty when it isn't.
    pub fn degraded(&self) -> Vec<&'static str> {
        let on = [self.overloaded, self.database.is_stale()];
        DEGRADED
            .into_iter()
            .zip(on)
            .filter_map(|(reason, on)| on.then_some(reason))
            .collect()
    }

    /// Whether an instance that is `ready` (or not) is also strictly ready:
    /// in touch with the database, or it isn't shared. What
    /// `/readyz/strict` answers.
    pub fn ready_strict(&self, ready: bool) -> bool {
        ready && !self.database.is_stale()
    }

    /// The weight for load balancers, for an instance that is `ready` (or
    /// not): 0 when not ready, 1 while the database is stale, otherwise from
    /// utilization.
    pub fn weight(&self, ready: bool) -> u32 {
        if !ready {
            0
        } else if self.database.is_stale() {
            1
        } else {
            weight(self.utilization)
        }
    }
}

/// Whether this instance is in touch with its database.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Database {
    /// Not shared (SQLite), or not open yet.
    #[default]
    NotShared,
    /// In touch; it last answered at `last_contact`.
    Ok { last_contact: std::time::Instant },
    /// Out of touch since about `last_contact`, its last answer.
    Stale { last_contact: std::time::Instant },
}

impl Database {
    /// `ok`, `stale` or `n/a`.
    pub fn name(self) -> &'static str {
        match self {
            Database::NotShared => "n/a",
            Database::Ok { .. } => "ok",
            Database::Stale { .. } => "stale",
        }
    }

    pub fn is_stale(self) -> bool {
        matches!(self, Database::Stale { .. })
    }

    /// When a shared database last answered.
    pub fn last_contact(self) -> Option<std::time::Instant> {
        match self {
            Database::NotShared => None,
            Database::Ok { last_contact } | Database::Stale { last_contact } => Some(last_contact),
        }
    }
}

/// One model, as the verbose readiness answer shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelLoad {
    pub label: String,
    /// `keep_in_memory`: loaded at startup, and readiness waits for it.
    pub pinned: bool,
    pub preload: Preload,
    /// Inputs in flight that run it, and its `max_pending` (0 = none).
    pub admitted: usize,
    pub max_pending: usize,
}

/// Where a model's load at startup stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Preload {
    /// Not pinned: it loads on first use.
    Lazy,
    Loading,
    Loaded,
    /// The first load failed; a later use tries again.
    Failed,
}

impl Preload {
    pub fn name(self) -> &'static str {
        match self {
            Preload::Lazy => "lazy",
            Preload::Loading => "loading",
            Preload::Loaded => "loaded",
            Preload::Failed => "failed",
        }
    }
}

/// `100·(1−u)` in steps of 5, from 1 to 100.
fn weight(utilization: f64) -> u32 {
    let w = 5.0 * (20.0 * (1.0 - utilization)).round();
    w.clamp(1.0, 100.0) as u32
}

/// What a sample reads from the engine.
pub(crate) struct Inputs {
    pub(crate) in_flight: usize,
    pub(crate) max_pending: usize,
    /// `(total, busy, waiting)` slots.
    pub(crate) slots: (usize, usize, usize),
    /// Memory in use over the cap, when both are known.
    pub(crate) memory: Option<f64>,
    pub(crate) degraded_at: f64,
    /// `None` when the database isn't shared.
    pub(crate) contact: Option<Contact>,
    pub(crate) models: Vec<ModelLoad>,
    pub(crate) generation: u64,
    pub(crate) restart_required: Vec<String>,
}

/// What the engine knows of its contact with a shared database.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Contact {
    /// Since when it has answered without a break; `None` after a failure.
    pub(crate) connected_since: Option<std::time::Instant>,
    /// When it last answered.
    pub(crate) last_ok: std::time::Instant,
    pub(crate) lease: Duration,
}

/// Utilization as the inputs give it now, before smoothing.
fn raw(inputs: &Inputs) -> f64 {
    let work = if inputs.max_pending > 0 {
        inputs.in_flight as f64 / inputs.max_pending as f64
    } else {
        let (total, busy, waiting) = inputs.slots;
        (busy + waiting) as f64 / total.max(1) as f64
    };
    work.max(inputs.memory.unwrap_or(0.0))
}

/// `raw`, or `prev` decayed over `dt`, whichever is higher, from 0 to 1.
fn smooth(raw: f64, prev: f64, dt: Duration) -> f64 {
    let decayed = prev * (-dt.as_secs_f64() / TAU.as_secs_f64()).exp();
    raw.max(decayed).clamp(0.0, 1.0)
}

/// Where `load` clears for `degraded_at`: 0.15 below it, but no lower than
/// half of it, so a low `degraded_at` still clears.
fn clears_below(degraded_at: f64) -> f64 {
    (degraded_at - HYSTERESIS).max(degraded_at / 2.0)
}

/// Degraded for load, held at least [`DWELL`] in each state.
#[derive(Debug, Default)]
struct Overload {
    on: bool,
    /// When it last changed; `None` before the first change.
    since: Option<Instant>,
}

impl Overload {
    fn step(&mut self, utilization: f64, degraded_at: f64, now: Instant) -> bool {
        let held = self
            .since
            .is_none_or(|at| now.saturating_duration_since(at) >= DWELL);
        let flip = if self.on {
            utilization < clears_below(degraded_at)
        } else {
            utilization >= degraded_at
        };
        if held && flip {
            self.on = !self.on;
            self.since = Some(now);
        }
        self.on
    }
}

/// Whether a shared database is stale, from one sample to the next.
#[derive(Debug, Default)]
struct ContactState {
    stale: bool,
    /// Since the first sample that found it out of touch.
    out_since: Option<std::time::Instant>,
}

impl ContactState {
    fn step(&mut self, contact: Contact, now: std::time::Instant) -> Database {
        match contact.connected_since {
            None => {
                self.out_since.get_or_insert(now);
            }
            Some(_) => self.out_since = None,
        }
        let fresh = now.saturating_duration_since(contact.last_ok) < 2 * contact.lease;
        self.stale = if self.stale {
            let settled = contact
                .connected_since
                .is_some_and(|t| now.saturating_duration_since(t) >= contact.lease / 3);
            !(fresh && settled)
        } else {
            let out = self
                .out_since
                .is_some_and(|t| now.saturating_duration_since(t) >= OUT_OF_TOUCH);
            !fresh || out
        };
        let last_contact = contact.last_ok;
        if self.stale {
            Database::Stale { last_contact }
        } else {
            Database::Ok { last_contact }
        }
    }
}

/// What the sampler carries from one sample to the next.
#[derive(Debug, Default)]
pub(crate) struct Sampler {
    utilization: f64,
    last: Option<Instant>,
    overload: Overload,
    contact: ContactState,
}

impl Sampler {
    pub(crate) fn sample(&mut self, inputs: Inputs, now: Instant) -> LoadSnapshot {
        let dt = self
            .last
            .map_or(Duration::ZERO, |t| now.saturating_duration_since(t));
        self.utilization = smooth(raw(&inputs), self.utilization, dt);
        self.last = Some(now);
        let overloaded = self
            .overload
            .step(self.utilization, inputs.degraded_at, now);
        let database = match inputs.contact {
            None => Database::NotShared,
            Some(contact) => self.contact.step(contact, std::time::Instant::now()),
        };
        let (slots_total, slots_busy, slots_waiting) = inputs.slots;
        LoadSnapshot {
            sampled_at: Some(now),
            utilization: self.utilization,
            in_flight: inputs.in_flight,
            max_pending: inputs.max_pending,
            slots_total,
            slots_busy,
            slots_waiting,
            memory: inputs.memory,
            overloaded,
            database,
            models: inputs.models,
            generation: inputs.generation,
            restart_required: inputs.restart_required,
        }
    }
}

/// Publish a sample of `read` into `lc` now and every second after, going
/// on from `sampler`.
pub(crate) fn spawn_sampler<R, F>(lc: Lifecycle, mut sampler: Sampler, mut read: R)
where
    R: FnMut() -> F + Send + 'static,
    F: Future<Output = Inputs> + Send,
{
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(PERIOD);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let inputs = read().await;
            lc.publish_load(sampler.sample(inputs, Instant::now()));
        }
    });
}

impl Engine {
    /// Publish a first sample now, without memory, then sample every second.
    pub(crate) fn spawn_load(&self) {
        let mut sampler = Sampler::default();
        let first = sampler.sample(self.load_inputs(None), Instant::now());
        self.inner.lifecycle.publish_load(first);
        let engine = self.clone();
        spawn_sampler(self.inner.lifecycle.clone(), sampler, move || {
            let engine = engine.clone();
            async move { engine.read_load(memory::current_memory_bytes).await }
        });
    }

    /// What the sampler reads, with memory in use from `probe`, which runs
    /// only when there is a memory cap to compare it with: it is a system
    /// call (on Linux it reads every mapping of the process), so it is kept
    /// off the runtime too.
    pub(crate) async fn read_load<P>(&self, probe: P) -> Inputs
    where
        P: FnOnce() -> Option<u64> + Send + 'static,
    {
        let used = match self.live().max_memory_bytes {
            Some(cap) if cap > 0 => tokio::task::spawn_blocking(probe).await.ok().flatten(),
            _ => None,
        };
        self.load_inputs(used)
    }

    /// What the sampler reads, with `memory_used` bytes in use.
    pub(crate) fn load_inputs(&self, memory_used: Option<u64>) -> Inputs {
        let inner = &self.inner;
        let live = self.live();
        let memory = match (memory_used, live.max_memory_bytes) {
            (Some(used), Some(cap)) if cap > 0 => Some(used as f64 / cap as f64),
            _ => None,
        };
        let contact = inner.storage.shared().then(|| Contact {
            connected_since: *inner
                .connected_since
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
            last_ok: self.last_ok(),
            lease: inner.lease_ttl,
        });
        let admitted = inner
            .admitted
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let models = live
            .config
            .models
            .iter()
            .filter_map(|(label, cfg)| {
                let handle = live.registry.get(label)?;
                let preload = if !cfg.keep_in_memory {
                    Preload::Lazy
                } else if handle.snapshot().loaded {
                    Preload::Loaded
                } else if handle.preload_attempted() {
                    Preload::Failed
                } else {
                    Preload::Loading
                };
                Some(ModelLoad {
                    label: label.clone(),
                    pinned: cfg.keep_in_memory,
                    preload,
                    admitted: admitted.get(label).copied().unwrap_or(0),
                    max_pending: cfg.max_pending as usize,
                })
            })
            .collect();
        Inputs {
            in_flight: inner.in_flight.load(Ordering::SeqCst),
            max_pending: live.max_pending,
            slots: inner.gate.snapshot(),
            memory,
            degraded_at: live.config.health.degraded_at,
            contact,
            models,
            generation: live.generation,
            restart_required: live.restart_required.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    const SEC: Duration = Duration::from_secs(1);

    fn inputs(in_flight: usize, max_pending: usize) -> Inputs {
        Inputs {
            in_flight,
            max_pending,
            slots: (4, 0, 0),
            memory: None,
            degraded_at: 0.9,
            contact: None,
            models: Vec::new(),
            generation: 1,
            restart_required: Vec::new(),
        }
    }

    #[test]
    fn utilization_comes_from_the_queue_or_memory_and_falls_back_to_the_slots() {
        // The queue, or memory, whichever is higher.
        assert_eq!(raw(&inputs(3, 12)), 0.25);
        let memory = Inputs {
            memory: Some(0.6),
            ..inputs(3, 12)
        };
        assert_eq!(raw(&memory), 0.6);
        // No max_pending: busy and waiting slots over all slots.
        let slots = Inputs {
            slots: (4, 2, 1),
            ..inputs(40, 0)
        };
        assert_eq!(raw(&slots), 0.75);
        let memory = Inputs {
            memory: Some(0.9),
            ..slots
        };
        assert_eq!(raw(&memory), 0.9);
        // More in flight than the limit (resumed tasks) is still 1.
        assert_eq!(smooth(raw(&inputs(30, 12)), 0.0, SEC), 1.0);
        let snapshot = Sampler::default().sample(inputs(30, 12), Instant::now());
        assert_eq!((snapshot.utilization, snapshot.free()), (1.0, Some(0)));
        // No limit, no free count.
        let snapshot = Sampler::default().sample(inputs(30, 0), Instant::now());
        assert_eq!(snapshot.free(), None);
    }

    #[test]
    fn utilization_rises_at_once_and_falls_over_5_s() {
        assert_eq!(smooth(0.8, 0.2, SEC), 0.8);
        let after = |secs: u64| smooth(0.0, 1.0, Duration::from_secs(secs));
        assert!((after(5) - (-1.0f64).exp()).abs() < 1e-12, "{}", after(5));
        assert!((after(1) - (-0.2f64).exp()).abs() < 1e-12, "{}", after(1));
        // A lower reading than the decayed one doesn't pull it down faster.
        assert_eq!(smooth(0.1, 1.0, Duration::from_secs(5)), after(5));
        assert_eq!(smooth(0.3, 1.0, Duration::from_secs(60)), 0.3);
    }

    #[tokio::test(start_paused = true)]
    async fn the_sampler_rises_at_once_and_falls_under_paused_time() {
        let lc = Lifecycle::new();
        let in_flight = Arc::new(Mutex::new(0usize));
        let read = {
            let in_flight = Arc::clone(&in_flight);
            move || {
                let n = *in_flight.lock().unwrap();
                async move { inputs(n, 10) }
            }
        };
        spawn_sampler(lc.clone(), Sampler::default(), read);
        tokio::time::sleep(SEC / 2).await;
        assert_eq!(lc.load().utilization, 0.0);

        *in_flight.lock().unwrap() = 8;
        tokio::time::sleep(SEC).await;
        assert_eq!(lc.load().utilization, 0.8, "the next sample has it");

        *in_flight.lock().unwrap() = 0;
        tokio::time::sleep(5 * SEC).await;
        let u = lc.load().utilization;
        assert!((u - 0.8 * (-1.0f64).exp()).abs() < 1e-9, "{u}");
        tokio::time::sleep(30 * SEC).await;
        assert!(lc.load().utilization < 0.01);
        assert_eq!(lc.load().sampled_at, Some(Instant::now() - SEC / 2));
    }

    #[test]
    fn the_weight_goes_in_steps_of_5_down_to_1() {
        assert_eq!(weight(0.0), 100);
        assert_eq!(weight(0.024), 100);
        assert_eq!(weight(0.026), 95);
        assert_eq!(weight(0.42), 60);
        assert_eq!(weight(0.974), 5);
        assert_eq!(weight(0.976), 1);
        assert_eq!(weight(1.0), 1);
        let snapshot = LoadSnapshot {
            utilization: 0.42,
            ..LoadSnapshot::default()
        };
        assert_eq!((snapshot.weight(true), snapshot.weight(false)), (60, 0));
        let stale = LoadSnapshot {
            database: Database::Stale {
                last_contact: std::time::Instant::now(),
            },
            ..snapshot
        };
        assert_eq!((stale.weight(true), stale.weight(false)), (1, 0));
        assert_eq!(stale.degraded(), ["database"]);
    }

    #[test]
    fn strict_readiness_is_readiness_with_the_database_in_touch() {
        let last_contact = std::time::Instant::now();
        for (database, in_touch) in [
            (Database::NotShared, true),
            (Database::Ok { last_contact }, true),
            (Database::Stale { last_contact }, false),
        ] {
            let snapshot = LoadSnapshot {
                database,
                ..LoadSnapshot::default()
            };
            assert_eq!(snapshot.ready_strict(true), in_touch, "{database:?}");
            assert!(!snapshot.ready_strict(false), "{database:?}");
        }
    }

    #[test]
    fn degraded_load_has_hysteresis_and_holds_each_state_10_s() {
        let t0 = Instant::now();
        let at = |secs: u64| t0 + Duration::from_secs(secs);
        let mut load = Overload::default();
        assert!(!load.step(0.89, 0.9, at(0)));
        // The first change comes at once.
        assert!(load.step(0.9, 0.9, at(1)));
        // Within 10 s of turning on: still on, however low it goes.
        assert!(load.step(0.5, 0.9, at(5)));
        // Below the threshold but not 0.15 below: still on.
        assert!(load.step(0.76, 0.9, at(11)));
        assert!(!load.step(0.74, 0.9, at(12)));
        // Off for at least 10 s, however high it goes.
        assert!(!load.step(1.0, 0.9, at(20)));
        assert!(load.step(1.0, 0.9, at(22)));
        // A low threshold still clears, at half of it.
        let mut low = Overload::default();
        assert!(low.step(0.1, 0.1, at(0)));
        assert!(low.step(0.06, 0.1, at(10)));
        assert!(!low.step(0.04, 0.1, at(10)));
        assert_eq!(clears_below(0.9), 0.9 - HYSTERESIS);
        assert_eq!(clears_below(0.2), 0.1);
    }

    #[test]
    fn the_database_goes_stale_and_is_ok_again_after_a_third_of_a_lease() {
        let lease = Duration::from_secs(30);
        let t0 = std::time::Instant::now();
        let at = |secs: u64| t0 + Duration::from_secs(secs);
        let contact = |since: Option<u64>, last_ok: u64| Contact {
            connected_since: since.map(at),
            last_ok: at(last_ok),
            lease,
        };
        let mut state = ContactState::default();
        assert_eq!(state.step(contact(Some(0), 0), at(1)).name(), "ok");
        // A failure: stale once it has lasted 5 s.
        assert_eq!(state.step(contact(None, 10), at(12)).name(), "ok");
        assert_eq!(state.step(contact(None, 10), at(16)).name(), "ok");
        assert_eq!(
            state.step(contact(None, 10), at(17)),
            Database::Stale {
                last_contact: at(10)
            }
        );
        // Back in touch: ok only after a third of the lease.
        assert_eq!(state.step(contact(Some(20), 20), at(20)).name(), "stale");
        assert_eq!(state.step(contact(Some(20), 29), at(29)).name(), "stale");
        assert_eq!(state.step(contact(Some(20), 30), at(30)).name(), "ok");

        // A short failure doesn't count.
        let mut state = ContactState::default();
        assert_eq!(state.step(contact(None, 0), at(1)).name(), "ok");
        assert_eq!(state.step(contact(None, 0), at(4)).name(), "ok");
        assert_eq!(state.step(contact(Some(5), 5), at(5)).name(), "ok");
        assert_eq!(state.step(contact(None, 5), at(8)).name(), "ok");

        // Calls that hang (none fails yet): stale after two leases.
        let mut state = ContactState::default();
        assert_eq!(state.step(contact(Some(0), 0), at(59)).name(), "ok");
        assert_eq!(state.step(contact(Some(0), 0), at(60)).name(), "stale");
        // In touch all along, but no answer since: not ok yet.
        assert_eq!(state.step(contact(Some(0), 0), at(70)).name(), "stale");
        assert_eq!(state.step(contact(Some(0), 70), at(71)).name(), "ok");
    }
}
