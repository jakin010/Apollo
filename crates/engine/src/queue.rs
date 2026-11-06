//! Task submission lifecycle, startup recovery, and the maintenance timer.

use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

use apollo_domain::{Input, Task, TaskError, TaskState};
use apollo_storage::Renewal;

use crate::aggregate;
use crate::error::EngineError;
use crate::lease::{MAX_RESUME_ATTEMPTS, Running};
use crate::{Engine, Phase, Reservation};

/// Upload files younger than this are never swept: one may still be arriving
/// (a `ClassifyStream`), here or at another instance sharing the directory.
const UPLOAD_SWEEP_AGE: Duration = Duration::from_secs(3600);

impl Engine {
    /// Startup recovery, run once before serving:
    /// 1. re-fire webhooks stranded by a crash between terminal-state and
    ///    delivery: on a shared database the ones nobody is delivering, and
    ///    one lease later (settled by then) also those whose claim lapsed, so
    ///    a crash's events are sent even with `redelivery_secs = 0`;
    /// 2. take over the unfinished tasks this instance may: on SQLite (one
    ///    process per database) every one; on a shared database the ones nobody
    ///    holds, and, from then on, every third of the lease, also the ones
    ///    whose holder stopped renewing its lease. A takeover of a claim that
    ///    was never handed back counts a resume attempt, failing the task once
    ///    it exceeds [`MAX_RESUME_ATTEMPTS`] (poison-task guard): only crashes
    ///    count, since a graceful shutdown hands claims back (see
    ///    [`Engine::shutdown`]).
    ///
    /// Returns the number of tasks taken over now.
    pub async fn recover(&self) -> Result<usize, EngineError> {
        self.redeliver_pending().await;
        if self.inner.storage.shared() {
            // Once settled, also those whose claim lapsed, until none is left
            // (a pass takes a limited number), whatever `redelivery_secs` is.
            let engine = self.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(engine.renew_every());
                tick.tick().await;
                let mut tries = 0;
                loop {
                    tick.tick().await;
                    // Drained by hand: wait for the undrain. A stop still
                    // ends it below.
                    let lc = engine.lifecycle();
                    if lc.drained() && lc.phase() < Phase::Draining {
                        continue;
                    }
                    if !engine.settled() {
                        tries += 1;
                        // Out of touch for long: the redelivery loop (or
                        // the next start) takes over.
                        if tries > 30 {
                            return;
                        }
                        continue;
                    }
                    if !engine.redeliver_pending().await {
                        return;
                    }
                }
            });
        }

        // Remove orphaned upload files: left by a task that finished but
        // crashed before its upload was deleted. Every unfinished task in the
        // database keeps its file, and young files are kept anyway.
        let tasks = self.inner.storage.load_incomplete_tasks().await?;
        self.sweep_uploads(&tasks);

        // As long as `[database].timeout` says, like opening the database.
        let limit = match self.config().database.timeout {
            0 => None,
            secs => Some(Duration::from_secs(secs.into())),
        };
        let resumed = self.take_over(limit).await?;
        if self.inner.storage.shared() {
            self.spawn_takeover();
        }
        Ok(resumed)
    }

    /// Fail a task that keeps crashing the process: mark it failed, fail each
    /// unfinished item (so the wire reports an error and the webhook fires), and
    /// let the rollup remove its staged upload. Called on the claim's run
    /// handle, so its writes are fenced.
    pub(crate) async fn fail_poison_task(&self, task: &Task) -> Result<(), EngineError> {
        if !self
            .inner
            .storage
            .set_task_state(&task.id, TaskState::Failed, self.fence())
            .await?
        {
            // It finished meanwhile (elsewhere). If it was cancelled, its
            // items still finish, as cancelled; otherwise they ended already.
            if let Some(now) = self.inner.storage.get_task(&task.id).await?
                && now.state == TaskState::Cancelled
            {
                for (idx, item) in now.items.iter().enumerate() {
                    if !aggregate::item_terminal(item.state) {
                        self.cancel_item(&task.id, idx).await;
                    }
                }
            }
            return Ok(());
        }
        let err = TaskError::internal(format!(
            "task interrupted by {MAX_RESUME_ATTEMPTS} crashes; giving up"
        ));
        for (idx, item) in task.items.iter().enumerate() {
            if !aggregate::item_terminal(item.state) {
                self.fail_item(&task.id, idx, item, err.clone()).await;
            }
        }
        // Covers a task whose items were all already terminal.
        self.rollup_task_state(&task.id).await;
        Ok(())
    }

    /// Remove upload files no longer referenced by any incomplete task and
    /// older than [`UPLOAD_SWEEP_AGE`] (a shared upload directory may have one
    /// arriving at another instance). Runs at startup.
    fn sweep_uploads(&self, keep_tasks: &[Task]) {
        let dir = self.upload_dir();
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                tracing::warn!(dir = %dir.display(), error = %e, "upload sweep: cannot read dir");
                return;
            }
        };
        let keep: std::collections::HashSet<std::path::PathBuf> = keep_tasks
            .iter()
            .flat_map(|t| &t.items)
            .filter_map(|it| match &it.input {
                Input::Bytes { path, .. } => Some(path.clone()),
                _ => None,
            })
            .collect();
        let mut removed = 0usize;
        for entry in entries.flatten() {
            let path = entry.path();
            if keep.contains(&path) {
                continue;
            }
            // Only old plain files; one we can't date stays.
            let old = entry.metadata().ok().filter(|m| m.is_file()).and_then(|m| {
                let age = SystemTime::now().duration_since(m.modified().ok()?).ok()?;
                Some(age >= UPLOAD_SWEEP_AGE)
            });
            if old != Some(true) {
                continue;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => removed += 1,
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "upload sweep: remove failed")
                }
            }
        }
        if removed > 0 {
            tracing::info!(removed, "swept orphaned upload files");
        }
    }

    /// Drive one task to completion under the claim `run` holds: mark it
    /// processing, run every item concurrently (each bounded by the global
    /// semaphore), then mark it completed once all items are terminal.
    /// `reservations` holds one per item, in item order; each item gives its
    /// own back when it ends. Every write is fenced by the claim.
    pub(crate) async fn run_task(&self, task: Task, run: Running, reservations: Vec<Reservation>) {
        let task_id = task.id.clone();
        let n_items = task.items.len();
        // The items run on the config in effect now, even if it is reloaded
        // while they run, and write only while the claim holds.
        let runner = self.running(&run);
        let cancel = run.ctl.cancel.clone();

        // Mark it processing, unless it has finished meanwhile or isn't ours
        // any more: then there is nothing left to run. One cancelled while we
        // still hold it (through another instance, or before this run
        // registered) still has its items finish, as cancelled.
        if !cancel.load(Ordering::SeqCst) {
            let started = self
                .inner
                .storage
                .set_task_state(&task_id, TaskState::Processing, runner.fence())
                .await;
            match started {
                Ok(true) => {}
                Ok(false) => match self.renew_one(&run.ctl).await {
                    Renewal::Held(TaskState::Cancelled) => cancel.store(true, Ordering::SeqCst),
                    renewal => {
                        tracing::info!(task = %task_id, "task already finished, or not this instance's; not running it");
                        // Perhaps still ours: hand it back (fenced by token).
                        if renewal != Renewal::Lost {
                            self.unrelease_later(&run.ctl);
                        }
                        // Dropping `reservations` gives every item's back.
                        return;
                    }
                },
                Err(e) => {
                    tracing::error!(task = %task_id, error = %e, "failed to mark task processing");
                    // Still ours: hand it back once the database answers.
                    self.unrelease_later(&run.ctl);
                    return;
                }
            }
        }

        let pinned = runner;
        let mut handles = Vec::with_capacity(n_items);
        debug_assert_eq!(reservations.len(), n_items);
        for ((idx, item), reservation) in task.items.into_iter().enumerate().zip(reservations) {
            let engine = pinned.clone();
            let tid = task_id.clone();
            let token = cancel.clone();
            handles.push(tokio::spawn(async move {
                engine.run_item(tid, idx, item, token, reservation).await
            }));
        }
        for h in handles {
            if let Err(e) = h.await {
                tracing::error!(task = %task_id, error = %e, "item task panicked");
            }
        }

        // Reflect the items: a backstop in case an item task panicked before it
        // could roll up, and the point where a cancelled task's uploads are
        // removed. (A cancel wrote the task's state before signalling.) A run
        // that lost its claim writes nothing.
        pinned.rollup_task_state(&task_id).await;
        drop(run);
    }

    /// Start the hourly maintenance loop: purge finished tasks older than
    /// `database.retention` (when set) and result-cache entries older than
    /// `cache.ttl_secs` (when the cache is enabled with a TTL). Both are read
    /// from the current config on every pass, so a reload applies to the next
    /// one.
    pub(crate) fn spawn_maintenance(&self) {
        let engine = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(3600));
            loop {
                tick.tick().await;
                let config = engine.config();
                let retention = config.database.retention.as_deref().and_then(|spec| {
                    let window = apollo_storage::retention::parse(spec);
                    if window.is_none() {
                        tracing::warn!(retention = %spec, "could not parse retention window; skipped");
                    }
                    window.map(|w| w.as_secs() as i64)
                });
                let cache_ttl = config
                    .cache
                    .as_ref()
                    .filter(|c| c.enabled)
                    .and_then(|c| c.ttl_secs)
                    .map(|t| t as i64);
                let now = now_unix();
                if let Some(secs) = retention {
                    match engine.inner.storage.purge_finished_before(now - secs).await {
                        Ok(n) if n > 0 => tracing::info!(removed = n, "retention purge"),
                        Ok(_) => {}
                        Err(e) => tracing::warn!(error = %e, "retention purge failed"),
                    }
                }
                if let Some(ttl) = cache_ttl {
                    match engine.inner.storage.purge_cache_before(now - ttl).await {
                        Ok(n) if n > 0 => tracing::info!(removed = n, "cache purge"),
                        Ok(_) => {}
                        Err(e) => tracing::warn!(error = %e, "cache purge failed"),
                    }
                }
            }
        });
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
