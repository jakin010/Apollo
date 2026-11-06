//! Where the process is in its life: starting, serving, draining before a
//! stop, or stopping. It only moves forward. Probes read it, and the latest
//! load sample it holds, without waiting on anything; the engine takes no
//! work over once it drains, and ends watches and waits once it stops.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::load::LoadSnapshot;
use crate::metrics::CounterVec;

/// How long a stop waits for the tasks to be handed back (see
/// [`Engine::hand_back`](crate::Engine::hand_back)).
pub const HAND_BACK_LIMIT: Duration = Duration::from_secs(5);

/// A startup step, in the order they run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Step {
    OpeningDatabase,
    ConfiguringAuth,
    RecoveringTasks,
    LoadingModels,
}

/// A phase of the process's life, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Phase {
    Starting(Step),
    Serving,
    /// A stop was asked for: still serving, but no longer ready, and taking
    /// no work over from other instances.
    Draining,
    /// Serving no new connections; waits and watches end.
    Stopping,
}

impl Step {
    /// How probes and logs name it: `recovering tasks`.
    pub fn name(self) -> &'static str {
        match self {
            Step::OpeningDatabase => "opening database",
            Step::ConfiguringAuth => "configuring auth",
            Step::RecoveringTasks => "recovering tasks",
            Step::LoadingModels => "loading models",
        }
    }
}

impl Phase {
    /// Every phase's name, in order.
    pub const NAMES: [&'static str; 4] = ["starting", "serving", "draining", "stopping"];

    /// How probes, logs and metrics name it: `starting`, without the step.
    pub fn name(self) -> &'static str {
        match self {
            Phase::Starting(_) => Self::NAMES[0],
            Phase::Serving => Self::NAMES[1],
            Phase::Draining => Self::NAMES[2],
            Phase::Stopping => Self::NAMES[3],
        }
    }
}

/// The process's lifecycle. Clone freely: all clones share one state.
#[derive(Clone)]
pub struct Lifecycle(Arc<Shared>);

struct Shared {
    phase: watch::Sender<Phase>,
    /// When the current phase (or startup step) began.
    since: Mutex<Instant>,
    /// Stop now, without waiting for the drain (a second signal).
    forced: AtomicBool,
    /// Connections and requests the monitoring listeners' allow lists
    /// refused, by listener. Kept here, as those listeners may start before
    /// the engine.
    refused: CounterVec,
    /// The engine's latest load sample (see [`crate::load`]).
    load: watch::Sender<Arc<LoadSnapshot>>,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl Lifecycle {
    /// A lifecycle opening the database, its first step.
    pub fn new() -> Self {
        let (phase, _) = watch::channel(Phase::Starting(Step::OpeningDatabase));
        let (load, _) = watch::channel(Arc::new(LoadSnapshot::default()));
        Self(Arc::new(Shared {
            phase,
            since: Mutex::new(Instant::now()),
            forced: AtomicBool::new(false),
            refused: CounterVec::default(),
            load,
        }))
    }

    pub fn phase(&self) -> Phase {
        *self.0.phase.borrow()
    }

    /// When the current phase (or startup step) began.
    pub fn since(&self) -> Instant {
        *self.0.since.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Whether a stop without waiting was asked for.
    pub fn forced(&self) -> bool {
        self.0.forced.load(Ordering::SeqCst)
    }

    /// On to startup step `step`.
    pub fn starting(&self, step: Step) {
        self.advance(Phase::Starting(step));
    }

    /// Ready for work. Nothing once a stop was asked for.
    pub fn serving(&self) {
        self.advance(Phase::Serving);
    }

    /// Begin draining. False when a stop was already asked for.
    pub fn drain(&self) -> bool {
        self.advance(Phase::Draining)
    }

    pub fn stopping(&self) {
        self.advance(Phase::Stopping);
    }

    /// Stop now: stopping, and without waiting for anything a stop would
    /// wait for.
    pub fn force(&self) {
        self.0.forced.store(true, Ordering::SeqCst);
        // Always tells subscribers, so a wait for `forced` wakes even when
        // already stopping.
        self.0.phase.send_modify(|phase| {
            if *phase < Phase::Stopping {
                *phase = Phase::Stopping;
                self.restart_clock();
            }
        });
    }

    /// Whether load balancers should send work here.
    pub fn ready(&self) -> bool {
        self.phase() == Phase::Serving
    }

    /// [`Lifecycle::ready`], and in touch with the database (or it isn't
    /// shared): what `/readyz/strict` answers.
    pub fn ready_strict(&self) -> bool {
        self.ready() && !self.load().database.is_stale()
    }

    /// The weight load balancers should give this instance: 0 when not
    /// ready, 1 while the database is stale, otherwise 1 to 100 from its
    /// utilization.
    pub fn weight(&self) -> u32 {
        self.load().weight(self.ready())
    }

    /// The latest load sample; an empty one until the engine takes the first.
    pub fn load(&self) -> Arc<LoadSnapshot> {
        Arc::clone(&self.0.load.borrow())
    }

    /// The latest load sample, and each one after it.
    pub fn subscribe_load(&self) -> watch::Receiver<Arc<LoadSnapshot>> {
        self.0.load.subscribe()
    }

    pub(crate) fn publish_load(&self, snapshot: LoadSnapshot) {
        self.0.load.send_replace(Arc::new(snapshot));
    }

    /// Whether this instance takes tasks and webhook deliveries over from
    /// other instances.
    pub fn taking_work(&self) -> bool {
        self.phase() < Phase::Draining
    }

    /// The phase now, and each change after it.
    pub fn subscribe(&self) -> watch::Receiver<Phase> {
        self.0.phase.subscribe()
    }

    /// Returns once the lifecycle has reached `phase` (at once if it has).
    pub async fn reached(&self, phase: Phase) {
        let mut rx = self.0.phase.subscribe();
        // The sender lives as long as `self`, so this can't fail.
        let _ = rx.wait_for(|now| *now >= phase).await;
    }

    /// Returns once a stop without waiting was asked for.
    pub async fn until_forced(&self) {
        let mut rx = self.0.phase.subscribe();
        let _ = rx.wait_for(|_| self.forced()).await;
    }

    /// Count a connection or request that `listener`'s allow list refused
    /// (`apollo_listener_refused_total`).
    pub fn note_refused(&self, listener: &'static str) {
        self.0.refused.inc(&[listener]);
    }

    pub(crate) fn refused(&self) -> &CounterVec {
        &self.0.refused
    }

    /// Move on to `to` if it is later than the phase now; whether it moved.
    fn advance(&self, to: Phase) -> bool {
        self.0.phase.send_if_modified(|phase| {
            if to <= *phase {
                return false;
            }
            *phase = to;
            self.restart_clock();
            true
        })
    }

    fn restart_clock(&self) {
        *self.0.since.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn phases_only_move_forward() {
        let lc = Lifecycle::new();
        assert_eq!(lc.phase(), Phase::Starting(Step::OpeningDatabase));
        assert!(!lc.ready() && lc.taking_work());

        lc.starting(Step::RecoveringTasks);
        let recovering = lc.since();
        // An earlier step is no step back.
        lc.starting(Step::ConfiguringAuth);
        assert_eq!(lc.phase(), Phase::Starting(Step::RecoveringTasks));
        assert_eq!(lc.since(), recovering);

        lc.serving();
        assert!(lc.ready() && lc.taking_work());
        assert!(lc.since() >= recovering);
        lc.starting(Step::LoadingModels);
        assert_eq!(lc.phase(), Phase::Serving);

        assert!(lc.drain());
        assert!(!lc.ready() && !lc.taking_work());
        // A second signal: already draining.
        assert!(!lc.drain());
        lc.serving();
        assert_eq!(lc.phase(), Phase::Draining);

        let mut rx = lc.subscribe();
        lc.stopping();
        assert!(rx.has_changed().unwrap());
        assert_eq!(*rx.borrow_and_update(), Phase::Stopping);
        assert!(!lc.drain());
        lc.serving();
        assert_eq!(lc.phase(), Phase::Stopping);
        // Every earlier phase counts as reached.
        lc.reached(Phase::Draining).await;
        lc.reached(Phase::Starting(Step::LoadingModels)).await;

        // Forcing it wakes a wait for that, though it is stopping already.
        assert!(!lc.forced());
        let forced = tokio::spawn({
            let lc = lc.clone();
            async move { lc.until_forced().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!forced.is_finished());
        lc.force();
        tokio::time::timeout(Duration::from_secs(1), forced)
            .await
            .expect("woken by the force")
            .unwrap();
        assert!(lc.forced());
        assert_eq!(lc.phase(), Phase::Stopping);

        // A forced stop from startup goes straight to stopping.
        let lc = Lifecycle::new();
        lc.force();
        assert_eq!(lc.phase(), Phase::Stopping);
        assert!(lc.forced() && !lc.ready());
    }

    #[test]
    fn refusals_are_counted_by_listener() {
        let lc = Lifecycle::new();
        lc.note_refused("health");
        lc.note_refused("health");
        lc.note_refused("metrics");
        assert_eq!(
            lc.refused().snapshot(),
            [(vec!["health"], 2), (vec!["metrics"], 1)]
        );
    }
}
