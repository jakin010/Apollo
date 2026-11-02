//! Several instances sharing one SurrealDB database. Each instance is an
//! [`Engine`] with its own runtime, database connection and cache directory,
//! as separate servers have; only the database is shared. Work is held at
//! exact points (a download that stalls until answered, a held storage write or
//! webhook, a taken slot), so no test sleeps to make something happen, and no
//! model weights are needed. A check that something doesn't happen, or happens
//! soon enough, waits a fixed time.
//!
//! They need a SurrealDB server (see `apollo_storage::testing`); without
//! `APOLLO_TEST_REQUIRE_SURREAL`, a missing URL skips them, and they pass:
//!
//! ```text
//! APOLLO_TEST_SURREAL_URL=ws://127.0.0.1:8000 APOLLO_TEST_SURREAL_USER=root \
//! APOLLO_TEST_SURREAL_PASS=root APOLLO_TEST_REQUIRE_SURREAL=1 \
//! cargo test -p apollo-engine multi_instance -- --include-ignored
//! ```
//!
//! The `#[ignore]`d tests reproduce problems with sharing a database: each
//! asserts the fixed behaviour, so it fails until its fix lands (its ignore
//! reason names the fix), then loses the `#[ignore]`.

mod cancel;
mod data;
mod recovery;

use std::fmt::Display;
use std::future::Future;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use apollo_config::{Backend, DatabaseConfig, DevicePreference};
use apollo_domain::{
    Input, ItemState, MediaKind, ModelOutput, ModelResult, ModelState, Sample, Task, TaskState, Url,
};
use apollo_storage::testing::SurrealScratch;
use apollo_storage::{PendingWebhook, Storage, StorageError, SurrealStorage, TaskPage, TaskQuery};
use tokio::sync::Semaphore;

use crate::{Engine, EventKind, Run, Submission, Submitted, TaskEvent, WebhookError, WebhookSink};

/// The longest any one wait may take before the test fails.
const WAIT: Duration = Duration::from_secs(20);

/// An image model that is never loaded: an item fails before reaching it.
const NSFW: &str = "[models.nsfw]\narchitecture = \"vit\"\nrepo = \"test/nsfw\"\n";

/// Run a test against a fresh database and a fresh [`Fleet`], then remove the
/// database, also when the test panics. Skipped without a SurrealDB server.
async fn fleet_test<F, Fut>(body: F)
where
    F: FnOnce(Fleet) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    // An instance's drop waits for its runtime to stop, which needs a worker
    // thread that may block.
    assert_eq!(
        tokio::runtime::Handle::current().runtime_flavor(),
        tokio::runtime::RuntimeFlavor::MultiThread,
        "use #[tokio::test(flavor = \"multi_thread\")]"
    );
    let Some(db) = SurrealScratch::create().await else {
        return;
    };
    let db = Arc::new(db);
    let store: Arc<dyn Storage> = Arc::new(SurrealStorage::connect(&db.config()).await.unwrap());
    let fleet = Fleet {
        db: db.clone(),
        log: Arc::new(Log::default()),
        store,
    };
    let outcome = tokio::spawn(body(fleet)).await;
    db.remove().await;
    if let Err(e) = outcome {
        std::panic::resume_unwind(e.into_panic());
    }
}

/// reqwest sends downloads through a proxy from the environment, which would
/// never reach the test's origin. Checked when an origin starts, inside the
/// test, so the database is still removed.
fn check_no_proxy() {
    let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    let proxied = ["HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"]
        .into_iter()
        .any(|k| var(k).is_some());
    let bypassed = ["NO_PROXY", "no_proxy"]
        .into_iter()
        .filter_map(var)
        .any(|v| v.contains("127.0.0.1") || v.trim() == "*");
    assert!(
        !proxied || bypassed,
        "an HTTP proxy is set: add 127.0.0.1 to NO_PROXY so downloads reach the test origin"
    );
}

/// The shared database, a log of what every instance did, and a connection of
/// the test's own for reading and planting rows.
struct Fleet {
    db: Arc<SurrealScratch>,
    log: Arc<Log>,
    store: Arc<dyn Storage>,
}

impl Fleet {
    /// An instance named `who` with `toml`'s config and a cache directory of
    /// its own. Like a server's startup, except: the test calls
    /// [`Engine::recover`] when the instance should start; the database was
    /// migrated once, up front; and with no `[webhook]` in `toml` (the sink is
    /// always there), the instance's redelivery loop stays off.
    async fn start(&self, who: &'static str, toml: &str) -> Instance {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let mut instance = self.start_in(who, toml, &path).await;
        instance._dir = Some(dir);
        instance
    }

    /// Like [`Fleet::start`], with `cache_dir` as the cache directory.
    async fn start_in(&self, who: &'static str, toml: &str, cache_dir: &Path) -> Instance {
        let mut config = apollo_config::load::from_str(toml).unwrap();
        config.app.device = DevicePreference::Cpu;
        // The tests share one process, whose memory isn't any instance's.
        config.app.max_memory = "0".into();
        config.app.max_retries = 0;
        // The test origin listens on 127.0.0.1.
        config.limits.block_private_ips = false;
        config.validate().unwrap();
        config.database = DatabaseConfig {
            backend: Backend::Surrealdb,
            surrealdb: Some(self.db.config()),
            ..Default::default()
        };
        config.app.cache_dir = Some(cache_dir.to_string_lossy().into_owned());

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name(format!("instance-{who}"))
            .enable_all()
            .build()
            .unwrap();
        let db = self.db.config();
        let log = self.log.clone();
        // Opened on the instance's runtime, so its connection lives and dies
        // with the instance.
        let (engine, probe, sink) = rt
            .spawn(async move {
                let store = SurrealStorage::connect(&db).await.unwrap();
                let probe = Arc::new(Probe {
                    who,
                    inner: Arc::new(store),
                    log: log.clone(),
                    holds: Holds::default(),
                });
                let sink = Arc::new(Sink {
                    who,
                    log,
                    holds: Holds::default(),
                });
                let engine = Engine::new(Arc::new(config), probe.clone(), Some(sink.clone()));
                (engine, probe, sink)
            })
            .await
            .unwrap();
        Instance {
            who,
            rt: Some(rt),
            engine,
            probe,
            sink,
            _dir: None,
        }
    }

    /// The task as stored now.
    async fn task(&self, id: &str) -> Task {
        self.store.get_task(id).await.unwrap().expect("the task")
    }

    /// Every task with `owner` (all owners when `None`).
    async fn tasks(&self, owner: Option<&str>) -> Vec<Task> {
        let query = TaskQuery {
            owner: owner.map(|o| Some(o.to_string())),
            states: Vec::new(),
            created_after: None,
            created_before: None,
            limit: 100,
            after: None,
        };
        self.store.list_tasks(&query).await.unwrap().tasks
    }
}

/// One Apollo instance: an engine on a runtime of its own.
struct Instance {
    who: &'static str,
    rt: Option<tokio::runtime::Runtime>,
    engine: Engine,
    probe: Arc<Probe>,
    sink: Arc<Sink>,
    _dir: Option<tempfile::TempDir>,
}

impl Instance {
    fn rt(&self) -> &tokio::runtime::Runtime {
        self.rt.as_ref().expect("a running instance")
    }

    /// Start `f` on this instance, as a request to this server would run.
    fn spawn<F, Fut, T>(&self, f: F) -> tokio::task::JoinHandle<T>
    where
        F: FnOnce(Engine) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.rt().spawn(f(self.engine.clone()))
    }

    /// Run `f` on this instance to its end.
    async fn on<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce(Engine) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let who = self.who;
        match tokio::time::timeout(WAIT, self.spawn(f)).await {
            Ok(Ok(value)) => value,
            Ok(Err(e)) => std::panic::resume_unwind(e.into_panic()),
            Err(_) => panic!("{who} didn't finish a call within {WAIT:?}"),
        }
    }

    async fn submit(&self, submission: Submission) -> Submitted {
        self.on(move |e| async move { e.submit(submission, None).await.unwrap() })
            .await
    }

    /// The startup step that takes over unfinished tasks.
    async fn recover(&self) -> usize {
        self.on(|e| async move { e.recover().await.unwrap() }).await
    }

    async fn cancel(&self, id: &str) {
        let id = id.to_string();
        self.on(move |e| async move { e.cancel(&id).await.unwrap() })
            .await
    }

    /// Items this instance has accepted or resumed and not finished.
    fn in_flight(&self) -> usize {
        self.engine.inner.in_flight.load(Ordering::SeqCst)
    }

    /// Items waiting for one of this instance's slots.
    fn slots_waiting(&self) -> usize {
        self.engine.inner.gate.snapshot().2
    }

    /// Wait until this instance has no item left in flight.
    async fn wait_idle(&self) {
        wait_until(format!("{} has no item in flight", self.who), || {
            self.in_flight() == 0
        })
        .await;
    }

    /// A new upload of `bytes` in this instance's upload directory, as an
    /// inline-bytes `Classify` stages it.
    fn stage(&self, bytes: &[u8]) -> PathBuf {
        let path = self
            .engine
            .ensure_upload_dir()
            .unwrap()
            .join(format!("{}.bin", uuid::Uuid::new_v4()));
        std::fs::write(&path, bytes).unwrap();
        path
    }

    /// Hold this instance's next `times` storage writes that `matches` picks,
    /// before they run.
    fn hold_storage(
        &self,
        times: usize,
        matches: impl Fn(&Call) -> bool + Send + Sync + 'static,
    ) -> Hold {
        self.probe.holds.add(times, matches)
    }

    /// Hold this instance's next `times` webhook deliveries that `matches`
    /// picks.
    fn hold_webhook(
        &self,
        times: usize,
        matches: impl Fn(&Call) -> bool + Send + Sync + 'static,
    ) -> Hold {
        self.sink.holds.add(times, matches)
    }

    /// Stop the instance as a crash would: everything it was doing stops where
    /// it is, and its connections close. Returns its cache directory, which a
    /// crash leaves on disk.
    fn crash(mut self) -> Option<tempfile::TempDir> {
        self._dir.take()
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            // Waits until every task of the instance is gone, so nothing it was
            // doing lands later.
            tokio::task::block_in_place(|| rt.shutdown_timeout(Duration::from_secs(10)));
        }
    }
}

/// What instances did, in order across all of them.
#[derive(Debug, Clone, PartialEq)]
enum Call {
    CreateTask(String),
    GetTask(String),
    /// With whether it was written.
    SetTaskState(String, TaskState, bool),
    /// With whether the task was still active.
    CancelIfActive(String, bool),
    /// With whether it was written.
    SetItemState(String, usize, ItemState, bool),
    ModelResult(String, usize, String, ModelState),
    /// With the new count.
    IncrementAttempts(String, u32),
    /// With the number of tasks released.
    ReleaseAttempts(u64),
    WebhookDelivered(String, usize),
    /// A webhook event the sink received.
    Webhook(String, EventKind, TaskState),
}

#[derive(Default)]
struct Log {
    entries: Mutex<Vec<(&'static str, Call)>>,
}

impl Log {
    fn lock(&self) -> MutexGuard<'_, Vec<(&'static str, Call)>> {
        self.entries.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn push(&self, who: &'static str, call: Call) {
        self.lock().push((who, call));
    }

    fn count(&self, f: impl Fn(&str, &Call) -> bool) -> usize {
        self.lock().iter().filter(|(who, c)| f(who, c)).count()
    }

    /// Who sent a finished webhook for `task`, and the task state each
    /// carried, in order.
    fn finished(&self, task: &str) -> Vec<(&'static str, TaskState)> {
        self.lock()
            .iter()
            .filter_map(|(who, c)| match c {
                Call::Webhook(id, EventKind::Finished, state) if id == task => Some((*who, *state)),
                _ => None,
            })
            .collect()
    }

    /// Wait for a matching entry.
    async fn wait_for(&self, what: &str, f: impl Fn(&str, &Call) -> bool) {
        wait_until(what, || self.count(&f) > 0).await;
    }

    /// Once `who` has no item of `task` in flight: wait for the finished
    /// webhook it owes, if it finished an item of `task` (another runner may
    /// have finished them first).
    async fn settled(&self, who: &'static str, task: &str) {
        let finished_one = self.count(|w, c| {
            w == who
                && matches!(c, Call::SetItemState(id, _, state, true)
                    if id == task && crate::aggregate::item_terminal(*state))
        }) > 0;
        if finished_one {
            self.delivered(who, task).await;
        }
    }

    /// Wait for `who`'s finished webhook for `task` to be marked delivered:
    /// after that, `who` writes nothing more for it.
    async fn delivered(&self, who: &'static str, task: &str) {
        self.wait_for(&format!("{who} delivers the webhook of {task}"), |w, c| {
            w == who && matches!(c, Call::WebhookDelivered(id, _) if id == task)
        })
        .await;
    }
}

/// Wait until `done`, failing the test after [`WAIT`].
async fn wait_until(what: impl Display, mut done: impl FnMut() -> bool) {
    assert!(
        wait_within(WAIT, &mut done).await,
        "timed out waiting until {what}"
    );
}

/// Whether `done` became true within `limit`.
async fn wait_within(limit: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    loop {
        if done() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Calls that wait until the test lets them go.
#[derive(Default)]
struct Holds(Mutex<Vec<HoldSlot>>);

struct HoldSlot {
    matches: Box<dyn Fn(&Call) -> bool + Send + Sync>,
    left: usize,
    hold: Hold,
}

/// A hold on some calls: [`Hold::entered`] waits for one to arrive,
/// [`Hold::release`] lets one go.
#[derive(Clone)]
struct Hold(Arc<(Semaphore, Semaphore)>);

impl Hold {
    async fn entered(&self, what: &str) {
        tokio::time::timeout(WAIT, self.0.0.acquire())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting until {what}"))
            .unwrap()
            .forget();
    }

    fn release(&self) {
        self.0.1.add_permits(1);
    }
}

impl Holds {
    fn add(&self, times: usize, matches: impl Fn(&Call) -> bool + Send + Sync + 'static) -> Hold {
        let hold = Hold(Arc::new((Semaphore::new(0), Semaphore::new(0))));
        self.0.lock().unwrap().push(HoldSlot {
            matches: Box::new(matches),
            left: times,
            hold: hold.clone(),
        });
        hold
    }

    /// Wait here if a hold picks `call` (which carries default results: it
    /// hasn't run yet).
    async fn pass(&self, call: &Call) {
        let hold = {
            let mut slots = self.0.lock().unwrap();
            slots
                .iter_mut()
                .find(|s| s.left > 0 && (s.matches)(call))
                .map(|s| {
                    s.left -= 1;
                    s.hold.clone()
                })
        };
        if let Some(hold) = hold {
            hold.0.0.add_permits(1);
            hold.0.1.acquire().await.unwrap().forget();
        }
    }
}

/// An instance's storage: logs the calls listed in [`Call`] and can hold them.
struct Probe {
    who: &'static str,
    inner: Arc<dyn Storage>,
    log: Arc<Log>,
    holds: Holds,
}

impl Probe {
    async fn pass(&self, call: Call) -> Call {
        self.holds.pass(&call).await;
        call
    }

    fn done(&self, call: Call) {
        self.log.push(self.who, call);
    }
}

#[async_trait::async_trait]
impl Storage for Probe {
    async fn migrate(&self) -> Result<(), StorageError> {
        self.inner.migrate().await
    }
    async fn create_task(&self, task: &Task) -> Result<(), StorageError> {
        let call = self.pass(Call::CreateTask(task.id.clone())).await;
        let r = self.inner.create_task(task).await;
        self.done(call);
        r
    }
    async fn get_task(&self, id: &str) -> Result<Option<Task>, StorageError> {
        let r = self.inner.get_task(id).await;
        self.done(Call::GetTask(id.into()));
        r
    }
    async fn set_task_state(&self, id: &str, state: TaskState) -> Result<bool, StorageError> {
        self.pass(Call::SetTaskState(id.into(), state, false)).await;
        let r = self.inner.set_task_state(id, state).await;
        if let Ok(written) = &r {
            self.done(Call::SetTaskState(id.into(), state, *written));
        }
        r
    }
    async fn cancel_task_if_active(&self, id: &str) -> Result<bool, StorageError> {
        self.pass(Call::CancelIfActive(id.into(), false)).await;
        let r = self.inner.cancel_task_if_active(id).await;
        if let Ok(won) = &r {
            self.done(Call::CancelIfActive(id.into(), *won));
        }
        r
    }
    async fn set_item_state(
        &self,
        task_id: &str,
        item: usize,
        state: ItemState,
        error: Option<&apollo_domain::TaskError>,
    ) -> Result<bool, StorageError> {
        self.pass(Call::SetItemState(task_id.into(), item, state, false))
            .await;
        let r = self.inner.set_item_state(task_id, item, state, error).await;
        if let Ok(written) = &r {
            self.done(Call::SetItemState(task_id.into(), item, state, *written));
        }
        r
    }
    async fn set_item_failed(
        &self,
        task_id: &str,
        item: usize,
        error: &apollo_domain::TaskError,
        models: &[String],
    ) -> Result<bool, StorageError> {
        let call = |written| Call::SetItemState(task_id.into(), item, ItemState::Failed, written);
        self.pass(call(false)).await;
        let r = self
            .inner
            .set_item_failed(task_id, item, error, models)
            .await;
        if let Ok(written) = &r {
            self.done(call(*written));
        }
        r
    }
    async fn find_task_by_request(
        &self,
        owner: Option<&str>,
        request_id: &str,
    ) -> Result<Option<String>, StorageError> {
        self.inner.find_task_by_request(owner, request_id).await
    }
    async fn list_tasks(&self, query: &TaskQuery) -> Result<TaskPage, StorageError> {
        self.inner.list_tasks(query).await
    }
    async fn upsert_model_result(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        result: &ModelResult,
    ) -> Result<(), StorageError> {
        let call = self
            .pass(Call::ModelResult(
                task_id.into(),
                item,
                label.into(),
                result.state,
            ))
            .await;
        let r = self
            .inner
            .upsert_model_result(task_id, item, label, result)
            .await;
        self.done(call);
        r
    }
    async fn append_samples(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        samples: &[Sample],
    ) -> Result<(), StorageError> {
        self.inner
            .append_samples(task_id, item, label, samples)
            .await
    }
    async fn load_samples(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
    ) -> Result<Vec<Sample>, StorageError> {
        self.inner.load_samples(task_id, item, label).await
    }
    async fn set_steps_completed(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
        steps: u32,
    ) -> Result<(), StorageError> {
        self.inner
            .set_steps_completed(task_id, item, label, steps)
            .await
    }
    async fn steps_completed(
        &self,
        task_id: &str,
        item: usize,
        label: &str,
    ) -> Result<u32, StorageError> {
        self.inner.steps_completed(task_id, item, label).await
    }
    async fn count_tasks_by_state(&self) -> Result<Vec<(TaskState, u64)>, StorageError> {
        self.inner.count_tasks_by_state().await
    }
    async fn load_incomplete_tasks(&self) -> Result<Vec<Task>, StorageError> {
        self.inner.load_incomplete_tasks().await
    }
    async fn increment_attempts(&self, id: &str) -> Result<u32, StorageError> {
        self.pass(Call::IncrementAttempts(id.into(), 0)).await;
        let r = self.inner.increment_attempts(id).await;
        if let Ok(n) = &r {
            self.done(Call::IncrementAttempts(id.into(), *n));
        }
        r
    }
    async fn release_resume_attempts(&self) -> Result<u64, StorageError> {
        let r = self.inner.release_resume_attempts().await;
        if let Ok(n) = &r {
            self.done(Call::ReleaseAttempts(*n));
        }
        r
    }
    async fn items_pending_webhook(&self) -> Result<Vec<PendingWebhook>, StorageError> {
        self.inner.items_pending_webhook().await
    }
    async fn mark_webhook_delivered(&self, task_id: &str, item: usize) -> Result<(), StorageError> {
        let call = self
            .pass(Call::WebhookDelivered(task_id.into(), item))
            .await;
        let r = self.inner.mark_webhook_delivered(task_id, item).await;
        self.done(call);
        r
    }
    async fn set_item_retries(
        &self,
        task_id: &str,
        item: usize,
        retries: u32,
    ) -> Result<(), StorageError> {
        self.inner.set_item_retries(task_id, item, retries).await
    }
    async fn purge_finished_before(&self, cutoff_unix_secs: i64) -> Result<u64, StorageError> {
        self.inner.purge_finished_before(cutoff_unix_secs).await
    }
    async fn cache_lookup(
        &self,
        content_hash: &str,
        model: &str,
        revision: &str,
        fresh_after: i64,
    ) -> Result<Option<ModelOutput>, StorageError> {
        self.inner
            .cache_lookup(content_hash, model, revision, fresh_after)
            .await
    }
    async fn cache_store(
        &self,
        content_hash: &str,
        model: &str,
        revision: &str,
        output: &ModelOutput,
    ) -> Result<(), StorageError> {
        self.inner
            .cache_store(content_hash, model, revision, output)
            .await
    }
    async fn url_cache_lookup(
        &self,
        url_hash: &str,
        model: &str,
        revision: &str,
        fresh_after: i64,
    ) -> Result<Option<String>, StorageError> {
        self.inner
            .url_cache_lookup(url_hash, model, revision, fresh_after)
            .await
    }
    async fn url_cache_store(
        &self,
        url_hash: &str,
        model: &str,
        revision: &str,
        content_hash: &str,
    ) -> Result<(), StorageError> {
        self.inner
            .url_cache_store(url_hash, model, revision, content_hash)
            .await
    }
    async fn purge_cache_before(&self, cutoff_unix_secs: i64) -> Result<u64, StorageError> {
        self.inner.purge_cache_before(cutoff_unix_secs).await
    }
}

/// An instance's webhook receiver: logs each event and can hold it.
struct Sink {
    who: &'static str,
    log: Arc<Log>,
    holds: Holds,
}

#[async_trait::async_trait]
impl WebhookSink for Sink {
    async fn deliver(&self, event: &TaskEvent) -> Result<(), WebhookError> {
        let call = Call::Webhook(event.task.id.clone(), event.kind, event.task.state);
        self.holds.pass(&call).await;
        self.log.push(self.who, call);
        Ok(())
    }
}

/// An HTTP server whose downloads wait until the test answers them, so an item
/// can be held mid-run, before any model is needed.
struct Origin {
    addr: SocketAddr,
    arrivals: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Arrival>>,
    hits: Arc<AtomicUsize>,
}

/// A download waiting for its answer.
struct Arrival(std::sync::mpsc::Sender<Reply>);

enum Reply {
    /// 200 with bytes that aren't an image: the item fails, permanently.
    Garbage,
    /// 200 with these bytes.
    Bytes(Vec<u8>),
}

impl Arrival {
    fn reply(self, reply: Reply) {
        let _ = self.0.send(reply);
    }
}

impl Origin {
    fn start() -> Origin {
        check_no_proxy();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (tx, counter) = (tx.clone(), counter.clone());
                std::thread::spawn(move || serve(stream, &tx, &counter));
            }
        });
        Origin {
            addr,
            arrivals: tokio::sync::Mutex::new(rx),
            hits,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    /// The next download.
    async fn next(&self, what: &str) -> Arrival {
        tokio::time::timeout(WAIT, self.arrivals.lock().await.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting until {what}"))
            .unwrap()
    }

    /// The next download, unless `done` turns true first.
    async fn next_unless(&self, what: &str, done: impl Fn() -> bool) -> Option<Arrival> {
        let deadline = Instant::now() + WAIT;
        let mut arrivals = self.arrivals.lock().await;
        loop {
            if let Ok(arrival) = arrivals.try_recv() {
                return Some(arrival);
            }
            if done() {
                return None;
            }
            assert!(Instant::now() < deadline, "timed out waiting until {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// The next download within `limit`, if any.
    async fn next_within(&self, limit: Duration) -> Option<Arrival> {
        tokio::time::timeout(limit, self.arrivals.lock().await.recv())
            .await
            .ok()
            .flatten()
    }

    /// Downloads so far.
    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}

fn serve(
    stream: TcpStream,
    arrivals: &tokio::sync::mpsc::UnboundedSender<Arrival>,
    hits: &AtomicUsize,
) {
    let mut reader = BufReader::new(&stream);
    // The request head; the body (none for a GET) is ignored.
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => return,
            Ok(_) if line == "\r\n" || line == "\n" => break,
            Ok(_) => {}
        }
    }
    hits.fetch_add(1, Ordering::SeqCst);
    let (tx, rx) = std::sync::mpsc::channel();
    if arrivals.send(Arrival(tx)).is_err() {
        return;
    }
    // A dropped arrival (the test ended) closes the connection.
    let Ok(reply) = rx.recv() else {
        return;
    };
    let body = match reply {
        Reply::Garbage => b"not an image".to_vec(),
        Reply::Bytes(bytes) => bytes,
    };
    let mut stream = &stream;
    let _ = write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(&body);
}

/// A `[models.hash]` PDQ block list in `dir`, its one entry `hash` (two hex
/// digits, repeated) in category `id` named `name`. Any image matches it
/// (`max_distance = 256`), so a result shows which list answered. No weights.
fn pdq_model(dir: &Path, hash: &str, id: u32, name: &str) -> String {
    let list = dir.join(format!("{name}.txt"));
    std::fs::write(&list, format!("{} {id}\n", hash.repeat(32))).unwrap();
    format!(
        "[models.hash]\narchitecture = \"pdq\"\nhash_list = \"{}\"\n\
         categories = {{ {id} = \"{name}\" }}\nmax_distance = 256\nmin_quality = 0\n",
        list.display()
    )
}

/// The category names a finished `hash` result gave, if it gave one.
fn hash_names(task: &Task) -> Option<Vec<String>> {
    match &task.items[0].results.get("hash")?.output {
        Some(ModelOutput::Classification(c)) => {
            Some(c.predictions.iter().map(|p| p.name.clone()).collect())
        }
        _ => None,
    }
}

/// An image download from `url`, run through `nsfw`.
fn image_at(url: &str) -> Submission {
    Submission {
        input: Input::Image(Url {
            main: url.into(),
            fallback: None,
        }),
        run: Run::Models(vec!["nsfw".into()]),
        metadata: Default::default(),
        request_id: None,
    }
}

/// A submission whose fetch is refused at once (local files are off), so its
/// task finishes without the network or a model.
fn refused() -> Submission {
    image_at("file:///nonexistent/apollo-test.png")
}

/// The staged upload at `path`, as an image.
fn uploaded(path: &Path) -> Submission {
    Submission {
        input: Input::Bytes {
            path: path.to_path_buf(),
            kind: MediaKind::Image,
        },
        ..refused()
    }
}

/// A `w`×`h` binary PPM image, its pixels varied by `seed`.
fn ppm(w: u32, h: u32, seed: u8) -> Vec<u8> {
    let mut bytes = format!("P6\n{w} {h}\n255\n").into_bytes();
    for y in 0..h {
        for x in 0..w {
            bytes.extend([
                (x * 4) as u8 ^ seed,
                (y * 4) as u8,
                ((x + y) * 2) as u8 ^ seed,
            ]);
        }
    }
    bytes
}

/// The cache's key for `bytes` (lowercase hex SHA-256).
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn finished(state: TaskState) -> bool {
    crate::aggregate::task_terminal(state)
}

/// Not ignored: checks the harness itself, so it runs wherever a SurrealDB
/// server is configured (CI included).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn instances_share_only_the_database() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let a = fleet.start("a", NSFW).await;
        let b = fleet.start("b", NSFW).await;
        assert!(!Arc::ptr_eq(&a.engine.inner, &b.engine.inner));
        assert_ne!(a.engine.upload_dir(), b.engine.upload_dir());

        // A task A finishes is visible to B, through the database only.
        let t = a.submit(refused()).await.id;
        a.wait_idle().await;
        fleet.log.delivered("a", &t).await;
        let seen = b
            .on({
                let t = t.clone();
                move |e| async move { e.get_task(&t).await.unwrap().unwrap() }
            })
            .await;
        assert_eq!(seen.state, TaskState::Failed);
        assert_eq!(fleet.log.finished(&t), [("a", TaskState::Failed)]);
        assert_eq!(fleet.log.count(|who, _| who == "b"), 1, "only B's read");

        // A download stalls until it is answered.
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let download = origin.next("a downloads the input").await;
        assert_eq!(fleet.task(&t).await.state, TaskState::Processing);
        download.reply(Reply::Garbage);
        a.wait_idle().await;
        fleet.log.delivered("a", &t).await;
        let task = fleet.task(&t).await;
        assert_eq!(task.state, TaskState::Failed);
        assert_eq!(
            task.items[0].error.as_ref().map(|e| e.kind),
            Some(apollo_domain::ErrorKind::Decode),
            "{task:?}"
        );
        assert_eq!(origin.hits(), 1);

        // A held write waits for the test.
        let hold = a.hold_storage(1, |c| matches!(c, Call::CreateTask(_)));
        let submitting = a.spawn(|e| async move { e.submit(refused(), None).await });
        hold.entered("a stores a task").await;
        assert!(!submitting.is_finished());
        hold.release();
        let t = submitting.await.unwrap().unwrap().id;
        a.wait_idle().await;
        fleet.log.delivered("a", &t).await;
        assert_eq!(fleet.tasks(None).await.len(), 3);
        assert_eq!(fleet.db.attempts(&t).await, 0);

        // A crash stops the instance's work where it is: the item's task is
        // dropped (giving back its place), though its download never ended.
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let _download = origin.next("a downloads the input").await;
        let engine_a = a.engine.clone();
        assert_eq!(engine_a.inner.in_flight.load(Ordering::SeqCst), 1);
        let _ = a.crash();
        assert_eq!(engine_a.inner.in_flight.load(Ordering::SeqCst), 0);
        assert_eq!(fleet.task(&t).await.state, TaskState::Processing);
    })
    .await;
}

/// Instances starting together on a new database all set its schema up:
/// SurrealDB refuses some of their racing schema writes, which are retried.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn instances_starting_together_on_a_new_database_all_start() {
    // A race, so it takes a few rounds to show.
    for _ in 0..20 {
        let Some(db) = SurrealScratch::create_empty().await else {
            return;
        };
        let starts: Vec<_> = (0..8)
            .map(|_| {
                let config = db.config();
                tokio::spawn(async move { SurrealStorage::connect(&config).await?.migrate().await })
            })
            .collect();
        let mut failed = Vec::new();
        for start in starts {
            if let Err(e) = start.await.unwrap() {
                failed.push(e.to_string());
            }
        }
        db.remove().await;
        assert!(
            failed.is_empty(),
            "{} of 8 instances failed to start: {failed:?}",
            failed.len()
        );
    }
}
