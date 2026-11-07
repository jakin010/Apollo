//! `apollo-engine` — async orchestration core. Wires storage + inference + media
//! into a runnable pipeline.
//!
//! - `queue`     — submission, startup recovery, maintenance timer
//! - `lease`     — which tasks this process runs; takeover and hand-back
//! - `scheduler` — per-task dispatch: fetch-once, fan-out, global concurrency cap
//! - `worker`    — per-model dedicated-thread batching worker (idle-unload)
//! - `registry`  — loaded-model registry + submission validation
//! - `aggregate` — result / lifecycle-state assembly, scan segments
//! - `webhook`   — task-event delivery via an injected sink
//! - `watch`     — task change notifications (`WatchTask`, `wait_seconds`)
//! - `catalog`   — the configured models and pipelines (`ListModels`)
//! - `live`      — the reloadable part of the engine, and config reload
//! - `lifecycle` — starting, serving, draining and stopping
//! - `load`      — the load figure and database contact load balancers read
//! - `metrics`   — Prometheus counters, histograms and the text format
//!
//! [`Engine`] is a cheap clonable handle around a shared `Inner`; cloning it does
//! not duplicate any models or state. What a config reload can change (the
//! config itself, the model workers, the webhook sink, the limits) lives in a
//! [`live::Live`] snapshot that a reload swaps out; each task runs on the
//! snapshot it started with.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use apollo_config::Config;
use apollo_domain::{Input, Item, Task};
use apollo_storage::{Storage, StorageError};
use tokio::sync::broadcast;

mod aggregate;
mod catalog;
mod error;
mod gate;
mod lease;
mod lifecycle;
mod live;
mod load;
mod memory;
mod metrics;
mod queue;
mod registry;
mod scheduler;
mod status;
mod watch;
mod webhook;
mod worker;

#[cfg(test)]
mod multi_instance_tests;
#[cfg(any(test, feature = "test-util"))]
pub mod testing;
#[cfg(test)]
mod tests;

pub use aggregate::task_error;
pub use apollo_storage::{TaskCursor, TaskPage, TaskQuery};
pub use catalog::{Catalog, Gate, ModelInfo, PipelineInfo, PipelineStepInfo, Trigger};
pub use error::{EngineError, Overload};
pub use lifecycle::{HAND_BACK_LIMIT, Lifecycle, Phase, Step};
pub use live::ReloadReport;
pub use load::{DEGRADED, Database, LoadSnapshot, ModelLoad, Preload};
pub use registry::RegistryChanges;
pub use status::{EngineStatus, ModelStatus, StoredCounts, TaskCounts};
pub use watch::TaskWatch;
pub use webhook::{EventKind, TaskEvent, WebhookError, WebhookSink};

use crate::gate::PriorityGate;
use live::Live;
use registry::{Registry, WorkerEnv};

/// What to run on an input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Run {
    /// These models, concurrently.
    Models(Vec<String>),
    /// A named `[pipelines.<name>]`: its steps in order, each step's models
    /// concurrently, gated by `stop_if`.
    Pipeline(String),
}

/// One input, what to run on it, and the client's task fields.
#[derive(Debug, Clone, PartialEq)]
pub struct Submission {
    pub input: Input,
    pub run: Run,
    /// Echoed back on the task and in webhooks.
    pub metadata: BTreeMap<String, String>,
    /// Idempotency key: the same owner submitting it again gets the existing
    /// task back.
    pub request_id: Option<String>,
}

/// The task a submission resulted in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submitted {
    pub id: String,
    /// The submission's `request_id` matched an earlier task, which is
    /// returned instead of creating one (the submission's input is unused).
    pub existing: bool,
}

/// The orchestration engine. Clone freely — all clones share one core.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
    /// A snapshot this handle is pinned to (see [`Engine::pinned`]); `None`
    /// reads the current one.
    pinned: Option<Arc<Live>>,
    /// The run this handle works for: its claim fences the handle's writes
    /// (see [`Engine::running`]).
    run: Option<Arc<lease::RunCtl>>,
}

struct Inner {
    storage: Arc<dyn Storage>,
    /// Compute backend the workers run on.
    device: apollo_inference::DeviceKind,
    /// Where model weights are cached (`[app].cache_dir`; restart-only).
    cache_dir: Option<PathBuf>,
    /// Idle-unload timeout in milliseconds, shared with every worker.
    idle_ms: Arc<AtomicU64>,
    /// The reloadable part: config, workers, webhook sink, limits.
    live: RwLock<Arc<Live>>,
    /// Serializes reloads.
    reloading: tokio::sync::Mutex<()>,
    metrics: metrics::Metrics,
    /// Priority-ordered global ceiling on concurrently processing items
    /// (`[app].max_concurrent`; restart-only).
    gate: Arc<PriorityGate>,
    /// Items currently queued or processing (admission backpressure counter).
    /// Changed only through [`Reservation`]s.
    in_flight: AtomicUsize,
    /// Of those, how many run each model (by label), for the models'
    /// `max_pending`. Labels with none are absent.
    admitted: Mutex<HashMap<String, usize>>,
    /// The tasks this process runs, by id: each run's claim and its
    /// cooperative cancel signal.
    runs: Mutex<HashMap<String, Arc<lease::RunCtl>>>,
    /// Claims of runs stopped because no renewal was confirmed in time, not
    /// yet handed back: the renewal loop hands them back once the database
    /// answers.
    unreleased: Mutex<Vec<apollo_storage::Held>>,
    /// Woken on every cancel, so items waiting for a slot can check theirs.
    cancelled: tokio::sync::Notify,
    /// Woken as a run registers, for the deadline watch.
    registered: tokio::sync::Notify,
    /// This instance's node id, and this process's id (one per start).
    node: String,
    process: String,
    /// Who holds this process's claims: `<node>/<process>`.
    holder: String,
    /// The finished webhooks this process is delivering, by (task, item):
    /// each one's lost flag, set once its delivery claim is found taken.
    deliveries: Mutex<HashMap<(String, usize), Arc<AtomicBool>>>,
    /// How long a claim lasts unless renewed (`[database].lease_secs`).
    lease_ttl: Duration,
    /// Since when the database has answered without a break (for taking over
    /// lapsed leases).
    connected_since: Mutex<Option<Instant>>,
    /// When the database last answered, in milliseconds after `epoch`.
    last_ok: AtomicU64,
    /// When the engine was built.
    epoch: Instant,
    /// Serializes takeover passes, and what they remember between passes.
    takeover: tokio::sync::Mutex<lease::Takeover>,
    /// Shutting down: no new runs, no takeovers.
    closing: AtomicBool,
    /// Woken as `closing` is set, so a takeover pass in progress ends.
    closed: tokio::sync::Notify,
    /// The process's lifecycle: draining stops takeovers, stopping ends
    /// watches.
    lifecycle: Lifecycle,
    /// Woken as each pinned model's worker finishes its first load attempt.
    preloaded: Arc<tokio::sync::Notify>,
    /// Called before each load of a model's weights (tests only).
    load_hook: Option<LoadHook>,
    /// Ids of changed tasks, announced by the storage wrapper.
    changes: broadcast::Sender<Arc<str>>,
    /// One lock per `(owner, request_id)` in use: serializes submissions with
    /// the same key, so two concurrent ones cannot both create a task. Held
    /// until the task is stored, even when the submitting call is dropped
    /// first. Other keys never wait on it.
    idempotency: Mutex<RequestLocks>,
}

/// Called on a model's worker thread, with its label, before each load of
/// its weights. Only tests set one (`testing::engine_with_load_hook`); it
/// may hold the load, as a slow download would.
pub(crate) type LoadHook = Arc<dyn Fn(&str) + Send + Sync>;

/// Locks by `(owner, request_id)`; an entry nobody holds is dropped.
type RequestLocks = HashMap<(Option<String>, String), Weak<tokio::sync::Mutex<()>>>;

/// One accepted, unfinished item's share of the admission counters: one unit
/// of `in_flight` and one per model it runs. Given back on drop, so every way
/// an item ends (finished, failed, cancelled, panicked, or a submission whose
/// task could not be stored) releases it exactly once.
pub(crate) struct Reservation {
    inner: Arc<Inner>,
    models: Vec<String>,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.inner.in_flight.fetch_sub(1, Ordering::SeqCst);
        let mut admitted = self
            .inner
            .admitted
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for label in &self.models {
            if let Some(n) = admitted.get_mut(label) {
                *n = n.saturating_sub(1);
                if *n == 0 {
                    admitted.remove(label);
                }
            }
        }
    }
}

impl Engine {
    /// Build the engine: select the compute device, spawn one worker per enabled
    /// model, and start the maintenance timer.
    ///
    /// Must be called within a Tokio runtime (it spawns background tasks). Loading
    /// of model weights is lazy — workers load on first use, not here. Its node
    /// id is new each time (see [`Engine::with_node`]).
    pub fn new(
        config: Arc<Config>,
        storage: Arc<dyn Storage>,
        webhook: Option<Arc<dyn WebhookSink>>,
    ) -> Engine {
        Self::with_node(config, storage, webhook, uuid::Uuid::new_v4().to_string())
    }

    /// [`Engine::new`], as node `node`: the name its node-local inputs
    /// (uploads, local files) stay with among instances sharing a database,
    /// the same across restarts. Its lifecycle is serving already.
    pub fn with_node(
        config: Arc<Config>,
        storage: Arc<dyn Storage>,
        webhook: Option<Arc<dyn WebhookSink>>,
        node: String,
    ) -> Engine {
        let lifecycle = Lifecycle::new();
        lifecycle.serving();
        Self::with_lifecycle(config, storage, webhook, node, lifecycle)
    }

    /// [`Engine::with_node`], following the process's `lifecycle`: once it
    /// drains, the engine takes no tasks or webhook deliveries over from
    /// other instances; once it stops, watches and waits end.
    pub fn with_lifecycle(
        config: Arc<Config>,
        storage: Arc<dyn Storage>,
        webhook: Option<Arc<dyn WebhookSink>>,
        node: String,
        lifecycle: Lifecycle,
    ) -> Engine {
        Self::build(config, storage, webhook, node, lifecycle, None)
    }

    /// [`Engine::with_lifecycle`], with a `load_hook` for its workers.
    pub(crate) fn build(
        config: Arc<Config>,
        storage: Arc<dyn Storage>,
        webhook: Option<Arc<dyn WebhookSink>>,
        node: String,
        lifecycle: Lifecycle,
        load_hook: Option<LoadHook>,
    ) -> Engine {
        let (_probe, kind) = apollo_inference::select_device(config.app.device);
        tracing::info!(device = %kind, "selected compute device");

        let cache_dir = config.app.cache_dir.clone().map(PathBuf::from);
        let idle_ms = Arc::new(AtomicU64::new(live::idle_ms(&config)));
        let metrics = metrics::Metrics::new();
        let preloaded = Arc::new(tokio::sync::Notify::new());
        let env = WorkerEnv {
            kind,
            cache_dir: cache_dir.as_ref(),
            idle_ms: &idle_ms,
            metrics: &metrics,
            preloaded: &preloaded,
            load_hook: load_hook.as_ref(),
        };
        let (registry, _) = Registry::build(&config, &env, None);
        let lease_ttl = Duration::from_secs(config.database.lease_secs.max(1).into());
        let shared = storage.shared();
        let gate = Arc::new(PriorityGate::new(config.app.max_concurrent.max(1) as usize));
        let (changes, _) = broadcast::channel(watch::CHANGE_BUFFER);
        let storage: Arc<dyn Storage> = Arc::new(watch::Notifying {
            inner: storage,
            changes: changes.clone(),
        });

        let process = uuid::Uuid::new_v4().to_string();
        let holder = format!("{node}/{process}");
        let engine = Engine {
            inner: Arc::new(Inner {
                storage,
                device: kind,
                cache_dir,
                idle_ms,
                live: RwLock::new(Arc::new(Live::new(config, registry, webhook, 1))),
                reloading: tokio::sync::Mutex::new(()),
                metrics,
                gate,
                in_flight: AtomicUsize::new(0),
                admitted: Mutex::new(HashMap::new()),
                runs: Mutex::new(HashMap::new()),
                unreleased: Mutex::new(Vec::new()),
                cancelled: tokio::sync::Notify::new(),
                registered: tokio::sync::Notify::new(),
                changes,
                idempotency: Mutex::new(HashMap::new()),
                node,
                process,
                holder,
                deliveries: Mutex::new(HashMap::new()),
                lease_ttl,
                connected_since: Mutex::new(Some(Instant::now())),
                last_ok: AtomicU64::new(0),
                epoch: Instant::now(),
                takeover: tokio::sync::Mutex::new(lease::Takeover::default()),
                closing: AtomicBool::new(false),
                closed: tokio::sync::Notify::new(),
                lifecycle,
                preloaded,
                load_hook,
            }),
            pinned: None,
            run: None,
        };
        engine.spawn_maintenance();
        engine.spawn_redelivery();
        engine.spawn_load();
        if shared {
            engine.spawn_renewal();
            engine.spawn_deadlines();
        }
        engine
    }

    /// The live snapshot: the pinned one, or the current one.
    pub(crate) fn live(&self) -> Arc<Live> {
        match &self.pinned {
            Some(live) => Arc::clone(live),
            None => Arc::clone(&self.inner.live.read().unwrap()),
        }
    }

    /// The config in effect (for this handle).
    pub fn config(&self) -> Arc<Config> {
        Arc::clone(&self.live().config)
    }

    /// The lifecycle the engine follows.
    pub fn lifecycle(&self) -> &Lifecycle {
        &self.inner.lifecycle
    }

    /// Returns once every pinned model (`keep_in_memory`) has made its first
    /// load attempt, whether it loaded or not.
    pub async fn preloads_attempted(&self) {
        worker::wait_preloads(&self.inner.preloaded, || {
            self.inner
                .live
                .read()
                .unwrap()
                .registry
                .preloads_attempted()
        })
        .await;
    }

    /// `work`, a wait for the database on a client's behalf, failing with a
    /// storage timeout once `[database].timeout` passes. Only reads, and waits
    /// for work running in a task of its own, go through here: dropping them
    /// cuts no write short. Background work waits as long as the database takes,
    /// so none of its writes is dropped or overtaken by a later one. Lease
    /// renewals have a limit of their own, from `[database].lease_secs`.
    async fn within<T>(
        &self,
        work: impl Future<Output = Result<T, EngineError>>,
    ) -> Result<T, EngineError> {
        match self.config().database.timeout {
            0 => work.await,
            secs => {
                let limit = std::time::Duration::from_secs(secs.into());
                tokio::time::timeout(limit, work)
                    .await
                    .unwrap_or(Err(StorageError::Timeout(limit).into()))
            }
        }
    }

    /// A handle that reads the current snapshot, even from a pinned one.
    pub(crate) fn current(&self) -> Engine {
        Engine {
            inner: Arc::clone(&self.inner),
            pinned: None,
            run: None,
        }
    }

    /// A handle that keeps using the current snapshot even if the config is
    /// reloaded meanwhile, so a task runs start to finish on one config (and
    /// a removed model's worker lives until the tasks using it are done).
    pub(crate) fn pinned(&self) -> Engine {
        Engine {
            inner: Arc::clone(&self.inner),
            pinned: Some(self.live()),
            run: self.run.clone(),
        }
    }

    /// Directory under which `ClassifyStream` uploads are staged:
    /// `[app].cache_dir/uploads` when set, otherwise a per-user cache directory
    /// (`~/.cache/apollo/uploads` on Linux, `~/Library/Caches/apollo/uploads` on
    /// macOS). Never the shared system temp dir, where another local user could
    /// pre-create the directory and read or plant uploads. Not created here; see
    /// [`Engine::ensure_upload_dir`].
    pub fn upload_dir(&self) -> PathBuf {
        let base = match &self.inner.cache_dir {
            Some(dir) => dir.clone(),
            None => dirs::cache_dir()
                .unwrap_or_else(std::env::temp_dir)
                .join("apollo"),
        };
        base.join("uploads")
    }

    /// Whether `path` is a file directly in [`Engine::upload_dir`], where the
    /// server stages uploads. A task's upload path is read from the database,
    /// which other instances (or anyone who can write to it) share, so a file
    /// anywhere else is never read or removed.
    pub(crate) fn is_staged_upload(&self, path: &std::path::Path) -> bool {
        path.parent() == Some(self.upload_dir().as_path())
            && matches!(
                path.components().next_back(),
                Some(std::path::Component::Normal(_))
            )
    }

    /// Create the upload directory if needed — owner-only (`0700`) on Unix — and
    /// return it.
    pub fn ensure_upload_dir(&self) -> std::io::Result<PathBuf> {
        let dir = self.upload_dir();
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&dir)?;
        Ok(dir)
    }

    /// Hard cap on accepted upload bytes, mirroring the remote-download cap
    /// (`[limits].max_download`). `None` means unlimited.
    pub fn max_upload_bytes(&self) -> Option<u64> {
        self.live().fetch_limits.max_download_bytes
    }

    /// Validate, persist, and start a task owned by `owner` (the submitting
    /// token's subject; `None` when authentication is disabled). Returns its id
    /// immediately; processing continues in the background. Validation is
    /// synchronous, so a bad request is rejected before anything is written.
    ///
    /// With a `request_id` that `owner` already used, the earlier task is
    /// returned instead (`existing`), whatever this submission contains. The
    /// caller then owns an `Input::Bytes` file it staged, as it does when
    /// `submit` fails.
    pub async fn submit(
        &self,
        submission: Submission,
        owner: Option<String>,
    ) -> Result<Submitted, EngineError> {
        let result = self.submit_once(submission, owner).await;
        self.record_submission(result.as_ref().map(|s| s.existing));
        result
    }

    /// Count a submission's outcome: `Ok(existing)` or the error it got.
    fn record_submission(&self, outcome: Result<bool, &EngineError>) {
        let metrics = &self.inner.metrics;
        metrics.submission(match outcome {
            Ok(true) => "existing",
            Ok(false) => "accepted",
            Err(EngineError::Overloaded { reason, .. }) => {
                metrics.overloaded(*reason);
                "overloaded"
            }
            Err(
                EngineError::UnknownModel(_)
                | EngineError::Incompatible(_)
                | EngineError::Config(_),
            ) => "invalid",
            Err(_) => "error",
        });
    }

    async fn submit_once(
        &self,
        submission: Submission,
        owner: Option<String>,
    ) -> Result<Submitted, EngineError> {
        let (serialized, earlier) = self
            .within(async {
                let Some(rid) = &submission.request_id else {
                    return Ok((None, None));
                };
                let serialized = self.request_lock(owner.as_deref(), rid).lock_owned().await;
                let earlier = self
                    .inner
                    .storage
                    .find_task_by_request(owner.as_deref(), rid)
                    .await?;
                Ok((Some(serialized), earlier))
            })
            .await?;
        if let Some(id) = earlier {
            return Ok(Submitted { id, existing: true });
        }

        let (models, pipeline) = self.resolve(&submission.input, &submission.run)?;

        // Backpressure: shed load instead of growing memory without bound.
        self.check_memory()?;
        let reservation = self.reserve(&models)?;

        // A keyed task's id comes from its key, so two instances sharing the
        // database can't both store one: the second finds the first.
        let id = match &submission.request_id {
            Some(rid) => request_task_id(owner.as_deref(), rid),
            None => uuid::Uuid::new_v4().to_string(),
        };
        let mut task = Task::new(
            id.clone(),
            vec![Item::new(submission.input, models, pipeline)],
            owner,
        );
        task.metadata = submission.metadata;
        task.request_id = submission.request_id;

        // Store and start the task in a task of its own, which this call only
        // waits for. A caller that goes away meanwhile (a client cancel or
        // deadline drops this future) then neither leaks the reservation nor
        // leaves a stored task that nothing runs: the write finishes, and the
        // task either starts or gives the reservation back. The idempotency
        // lock is held until the task is stored, for the same reason. So this
        // wait is limited ([database].timeout) but the store isn't: a caller
        // told the database is unavailable leaves a task that is stored and
        // starts once it answers, which a retry with the same `request_id` gets.
        let engine = self.clone();
        let stored = tokio::spawn(async move {
            let _serialized = serialized;
            let token = uuid::Uuid::new_v4().to_string();
            // Node-local input stays with this node.
            let pin = lease::pinned_input(&task).then(|| engine.inner.node.as_str());
            let claimed_at = Instant::now();
            match engine
                .inner
                .storage
                .create_task(&task, Some(&engine.lease(&token)), pin)
                .await
            {
                Ok(()) => {}
                // Another instance stored this key's task after this one
                // looked for it: that's the task.
                Err(StorageError::TaskExists(_)) if task.request_id.is_some() => {
                    return Ok(true);
                }
                Err(e) => return Err(e.into()),
            }
            tracing::info!(task = %task.id, "task submitted");
            match engine.register(&task.id, token.clone(), claimed_at) {
                Some(run) => {
                    let runner = engine.clone();
                    tokio::spawn(
                        async move { runner.run_task(task, run, vec![reservation]).await },
                    );
                }
                // Stopping: the next start (or another instance) runs it.
                None => engine.release(&task.id, &token).await,
            }
            Ok::<bool, EngineError>(false)
        });
        let existing = self.within(joined(stored)).await?;
        Ok(Submitted { id, existing })
    }

    /// Room for one more item that runs `models`: refused when `[app].max_pending`
    /// items are in flight or one of the models has its own `max_pending`.
    fn reserve(&self, models: &[String]) -> Result<Reservation, EngineError> {
        let live = self.live();
        // Held across the check and the increments, so concurrent submissions
        // can't both take a model's last place.
        let mut admitted = self
            .inner
            .admitted
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        check_models(&live, &admitted, models)?;
        let max_pending = live.max_pending;
        let prev = self.inner.in_flight.fetch_add(1, Ordering::SeqCst);
        if max_pending > 0 && prev + 1 > max_pending {
            self.inner.in_flight.fetch_sub(1, Ordering::SeqCst);
            return Err(queue_full(prev, max_pending));
        }
        for label in models {
            *admitted.entry(label.clone()).or_default() += 1;
        }
        Ok(Reservation {
            inner: Arc::clone(&self.inner),
            models: models.to_vec(),
        })
    }

    /// A reservation for an item accepted before (resumed after a restart),
    /// whatever the limits say now.
    pub(crate) fn reserve_resumed(&self, models: &[String]) -> Reservation {
        self.inner.in_flight.fetch_add(1, Ordering::SeqCst);
        let mut admitted = self
            .inner
            .admitted
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for label in models {
            *admitted.entry(label.clone()).or_default() += 1;
        }
        Reservation {
            inner: Arc::clone(&self.inner),
            models: models.to_vec(),
        }
    }

    /// Like [`Engine::find_request`], but first waits for submissions still
    /// storing their task, so a repeat that arrives meanwhile finds it instead
    /// of being checked as a new submission (and maybe refused as overloaded
    /// by the first one's reservation).
    pub async fn find_request_settled(
        &self,
        owner: Option<&str>,
        request_id: &str,
    ) -> Result<Option<String>, EngineError> {
        let lock = self.request_lock(owner, request_id);
        self.within(async {
            let _settled = lock.lock().await;
            Ok(self
                .inner
                .storage
                .find_task_by_request(owner, request_id)
                .await?)
        })
        .await
    }

    /// The lock for submissions with `owner`'s `request_id`.
    fn request_lock(&self, owner: Option<&str>, request_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .inner
            .idempotency
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Forget keys nobody holds any more, so this stays as small as the
        // number of keyed submissions in progress.
        locks.retain(|_, lock| lock.strong_count() > 0);
        let key = (owner.map(str::to_string), request_id.to_string());
        if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(key, Arc::downgrade(&lock));
        lock
    }

    /// The task `owner` submitted with `request_id`, if any.
    pub async fn find_request(
        &self,
        owner: Option<&str>,
        request_id: &str,
    ) -> Result<Option<String>, EngineError> {
        self.within(async {
            Ok(self
                .inner
                .storage
                .find_task_by_request(owner, request_id)
                .await?)
        })
        .await
    }

    /// Check a submission without queueing it: model / pipeline / modality
    /// validation plus a capacity check. Lets `ClassifyStream` reject a request
    /// before accepting any upload bytes. `submit` repeats every check.
    pub fn check_submission(&self, input: &Input, run: &Run) -> Result<(), EngineError> {
        let result = self.check_room(input, run);
        // A refusal here ends the request, so count it like one from `submit`.
        if let Err(e) = &result {
            self.record_submission(Err(e));
        }
        result
    }

    fn check_room(&self, input: &Input, run: &Run) -> Result<(), EngineError> {
        let (models, _) = self.resolve(input, run)?;
        self.check_memory()?;
        let live = self.live();
        {
            let admitted = self
                .inner
                .admitted
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            check_models(&live, &admitted, &models)?;
        }
        let max_pending = live.max_pending;
        let pending = self.inner.in_flight.load(Ordering::SeqCst);
        if max_pending > 0 && pending >= max_pending {
            return Err(queue_full(pending, max_pending));
        }
        Ok(())
    }

    /// Resolve what to run to the item's model list (a pipeline's models in
    /// step order, so they get the same compatibility checks as a direct list)
    /// and pipeline name, and validate the models against the input's modality
    /// (a pipeline step with an `input` classifies text an earlier step read,
    /// so its models are only checked to be enabled).
    fn resolve(
        &self,
        input: &Input,
        run: &Run,
    ) -> Result<(Vec<String>, Option<String>), EngineError> {
        let live = self.live();
        let (models, pipeline) = match run {
            Run::Pipeline(name) => {
                let p = live
                    .config
                    .pipelines
                    .get(name)
                    .ok_or_else(|| EngineError::Config(format!("unknown pipeline '{name}'")))?;
                let (on_text, on_input): (Vec<_>, Vec<_>) = p
                    .ordered_steps()
                    .into_iter()
                    .partition(|step| step.input.is_some());
                let names = |steps: Vec<&apollo_config::PipelineStep>| -> Vec<String> {
                    steps
                        .into_iter()
                        .flat_map(|s| s.models().iter().cloned())
                        .collect()
                };
                live.registry
                    .validate_item(&live.config, input, &names(on_input))?;
                live.registry
                    .validate_text_steps(&live.config, &names(on_text))?;
                (p.models(), Some(name.clone()))
            }
            Run::Models(models) => {
                let mut seen = std::collections::HashSet::new();
                if let Some(dup) = models.iter().find(|m| !seen.insert(*m)) {
                    return Err(EngineError::Incompatible(format!(
                        "model '{dup}' is listed twice"
                    )));
                }
                live.registry.validate_item(&live.config, input, models)?;
                (models.clone(), None)
            }
        };
        // Inline text is checked now; an uploaded one when it is read.
        if let Input::Text(text) = input
            && let Some(max) = live.config.limits.max_text_bytes()
            && text.len() as u64 > max
        {
            return Err(EngineError::Incompatible(format!(
                "the text is {} bytes, over the {max}-byte limit ([limits].max_text)",
                text.len()
            )));
        }
        Ok((models, pipeline))
    }

    /// Load `model`'s weights now (downloading them if needed) instead of on
    /// first use.
    pub async fn load_model(&self, model: &str) -> Result<(), EngineError> {
        let handle = self
            .live()
            .registry
            .get(model)
            .ok_or_else(|| EngineError::UnknownModel(model.to_string()))?;
        handle.ensure_loaded().await.map(|_| ())
    }

    /// A page of tasks (backs `ListTasks`).
    pub async fn list_tasks(&self, query: &TaskQuery) -> Result<TaskPage, EngineError> {
        self.within(async { Ok(self.inner.storage.list_tasks(query).await?) })
            .await
    }

    /// Graceful-shutdown hook, called once the server has stopped accepting
    /// requests. Stops this process's runs and hands their tasks back, so a
    /// clean restart (e.g. a deploy) doesn't count toward the poison-task cap
    /// and another instance may take them at once. It waits as long as the
    /// database takes; the server calls [`Engine::hand_back`], which gives up
    /// after [`HAND_BACK_LIMIT`].
    pub async fn shutdown(&self) {
        self.hand_back_all().await;
    }

    /// [`Engine::shutdown`], giving up after [`HAND_BACK_LIMIT`] (the database
    /// doesn't answer). Returns whether it finished.
    pub async fn hand_back(&self) -> bool {
        self.shutdown_within(HAND_BACK_LIMIT).await
    }

    /// [`Engine::shutdown`], giving up after `limit`. The claims not handed
    /// back then lapse after the lease, and each counts a resume attempt
    /// when it is taken over.
    pub(crate) async fn shutdown_within(&self, limit: Duration) -> bool {
        if tokio::time::timeout(limit, self.shutdown()).await.is_ok() {
            return true;
        }
        tracing::warn!(
            after = ?limit,
            lease = ?self.inner.lease_ttl,
            "could not hand the tasks back in time; their claims lapse after the lease, and each counts a resume attempt when it is taken over"
        );
        false
    }

    /// Fetch a task by id (backs the `GetTask` RPC, and watches).
    pub async fn get_task(&self, id: &str) -> Result<Option<Task>, EngineError> {
        self.within(async { Ok(self.inner.storage.get_task(id).await?) })
            .await
    }

    /// Request cooperative cancellation of a task (backs `CancelTask`). Idempotent:
    /// a no-op returning `Ok` if the task has already finished. In-flight items
    /// stop at the next checkpoint (between models / between sampled frames).
    pub async fn cancel(&self, task_id: &str) -> Result<(), EngineError> {
        let task = self
            .get_task(task_id)
            .await?
            .ok_or_else(|| EngineError::UnknownTask(task_id.to_string()))?;
        if aggregate::task_terminal(task.state) {
            return Ok(());
        }
        // The rest runs in a task of its own, so a caller that stops waiting
        // (the database is slow to answer) doesn't cut it short between the
        // write and the signal: the task would be cancelled in the database but
        // keep running here until its next lease renewal.
        let engine = self.clone();
        let id = task_id.to_string();
        let cancelled = tokio::spawn(async move {
            // The stored state first, so an item that stops on the signal rolls
            // the task up as cancelled. Conditional: if the task finished in the
            // meantime it stays finished rather than being relabelled as
            // cancelled.
            if !engine.inner.storage.cancel_task_if_active(&id).await? {
                return Ok(());
            }
            // Then the run's signal, if it runs here; a run elsewhere learns of
            // the cancel when it next renews its lease, and one that hasn't
            // started yet when its start write finds the task cancelled.
            let run = engine
                .inner
                .runs
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&id)
                .cloned();
            if let Some(run) = run {
                run.cancel.store(true, Ordering::SeqCst);
            }
            // Items waiting for a slot stop waiting (and give back their place).
            engine.inner.cancelled.notify_waiters();
            tracing::info!(task = %id, "task cancellation requested");
            Ok(())
        });
        self.within(joined(cancelled)).await
    }

    /// Reject new work while process memory is over the configured soft cap.
    fn check_memory(&self) -> Result<(), EngineError> {
        let Some(limit) = self.live().max_memory_bytes else {
            return Ok(());
        };
        let Some(used) = memory::current_memory_bytes() else {
            return Ok(());
        };
        if used >= limit {
            const MIB: u64 = 1024 * 1024;
            return Err(EngineError::Overloaded {
                reason: Overload::Memory,
                message: format!(
                    "memory limit reached ({} MiB in use >= {} MiB cap)",
                    used / MIB,
                    limit / MIB
                ),
            });
        }
        Ok(())
    }
}

/// The id of the task `owner` submits with `request_id`: a UUID made from a
/// SHA-256 hash of both, the same on every instance.
fn request_task_id(owner: Option<&str>, request_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"apollo task\0");
    match owner {
        Some(owner) => {
            hash.update([1]);
            hash.update((owner.len() as u64).to_be_bytes());
            hash.update(owner.as_bytes());
        }
        None => hash.update([0]),
    }
    hash.update(request_id.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash.finalize()[..16]);
    uuid::Builder::from_custom_bytes(bytes)
        .into_uuid()
        .to_string()
}

/// Refuse an item when one of its `models` already has its own `max_pending`
/// items admitted.
fn check_models(
    live: &Live,
    admitted: &HashMap<String, usize>,
    models: &[String],
) -> Result<(), EngineError> {
    for label in models {
        let limit = live
            .config
            .models
            .get(label)
            .map_or(0, |m| m.max_pending as usize);
        let n = admitted.get(label).copied().unwrap_or(0);
        if limit > 0 && n >= limit {
            return Err(EngineError::Overloaded {
                reason: Overload::Model,
                message: format!("model '{label}' has {n} inputs in flight, limit {limit}"),
            });
        }
    }
    Ok(())
}

/// The result of work spawned as a task of its own.
async fn joined<T>(
    task: tokio::task::JoinHandle<Result<T, EngineError>>,
) -> Result<T, EngineError> {
    task.await
        .unwrap_or_else(|e| Err(EngineError::Join(e.to_string())))
}

fn queue_full(in_flight: usize, limit: usize) -> EngineError {
    EngineError::Overloaded {
        reason: Overload::Queue,
        message: format!("queue full ({in_flight} items in flight, limit {limit})"),
    }
}
