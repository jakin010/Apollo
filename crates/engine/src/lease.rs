//! Task leases on the engine's side: which tasks this process runs (and keeps
//! renewing), taking over tasks of instances that stopped, and handing tasks
//! back on a graceful stop. The storage side is `apollo_storage::lease`.
//!
//! Every run holds a claim: a token written with the task (by the instance
//! that created it, or took it over). Its writes only apply while the claim is
//! still its token, and it renews the claim every third of the lease. With a
//! shared database another instance takes a task over once its lease has
//! lapsed; the old runner, if it still runs, is then fenced out, and stops when
//! its renewal says so.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use apollo_domain::{ErrorKind, Input, Task, TaskError, TaskState};
use apollo_storage::{ClaimMode, Claimable, Fence, Held, Lease, Renewal, TaskCursor};

use crate::error::{EngineError, Overload};
use crate::{Engine, Inner, Reservation, Run, aggregate};

/// Resume attempts before a task is declared poison and failed.
pub(crate) const MAX_RESUME_ATTEMPTS: u32 = 3;

/// How many lease periods another node's pinned task waits before it is
/// failed (its node may come back for it meanwhile).
const PIN_GRACE_LEASES: u32 = 10;

/// Candidates read per page of a takeover pass.
const PAGE: usize = 64;

/// Pages a takeover pass reads at most (a forced one reads them all).
const PASS_PAGES: usize = 4;

/// How long a task this instance can't run is left out of takeover passes, in
/// lease periods.
const UNRUNNABLE_LEASES: u32 = 10;

/// One run of a task by this process.
pub(crate) struct RunCtl {
    pub(crate) task_id: String,
    /// The claim: fences the run's writes.
    pub(crate) token: String,
    /// Stops the run's items at their next checkpoint (a cancel).
    pub(crate) cancel: Arc<AtomicBool>,
    /// The claim is gone (taken over, or handed back on a stop): the run writes
    /// nothing more.
    lost: AtomicBool,
}

impl RunCtl {
    pub(crate) fn lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst)
    }

    fn held(&self) -> Held {
        Held {
            task_id: self.task_id.clone(),
            token: self.token.clone(),
        }
    }
}

/// A run's registration; dropping it (however the run ends) unregisters it.
pub(crate) struct Running {
    inner: Arc<Inner>,
    pub(crate) ctl: Arc<RunCtl>,
}

impl Drop for Running {
    fn drop(&mut self) {
        let mut runs = self.inner.runs.lock().unwrap_or_else(|e| e.into_inner());
        if runs
            .get(&self.ctl.task_id)
            .is_some_and(|c| Arc::ptr_eq(c, &self.ctl))
        {
            runs.remove(&self.ctl.task_id);
        }
    }
}

/// What a takeover pass remembers between passes.
#[derive(Default)]
pub(crate) struct Takeover {
    /// Tasks this instance can't run (a model or pipeline it lacks), and when
    /// that was found.
    unrunnable: HashMap<String, Instant>,
}

/// What became of one candidate.
enum Taken {
    Yes,
    No,
    /// No room for more work: end the pass.
    Full,
}

impl Engine {
    /// The lease for a claim with `token`.
    pub(crate) fn lease<'a>(&'a self, token: &'a str) -> Lease<'a> {
        Lease {
            token,
            node: &self.inner.node,
            process: &self.inner.process,
            ttl: self.inner.lease_ttl,
        }
    }

    /// This instance's node id.
    pub fn node(&self) -> &str {
        &self.inner.node
    }

    /// The fence for this handle's writes: its run's claim, if it is a run's.
    pub(crate) fn fence(&self) -> Fence<'_> {
        match &self.run {
            Some(run) => Fence::Claim(&run.token),
            None => Fence::Unchecked,
        }
    }

    /// Whether this handle's run has lost its claim (it writes nothing more).
    pub(crate) fn lost(&self) -> bool {
        self.run.as_ref().is_some_and(|run| run.lost())
    }

    /// Register a run of `task_id` with claim `token`. `None` once the engine
    /// is shutting down.
    pub(crate) fn register(&self, task_id: &str, token: String) -> Option<Running> {
        let ctl = Arc::new(RunCtl {
            task_id: task_id.to_string(),
            token,
            cancel: Arc::new(AtomicBool::new(false)),
            lost: AtomicBool::new(false),
        });
        let old = {
            // Checked under the lock the shutdown's snapshot takes: a run is
            // either in that snapshot, or refused.
            let mut runs = self.inner.runs.lock().unwrap_or_else(|e| e.into_inner());
            if self.inner.closing.load(Ordering::SeqCst) {
                return None;
            }
            runs.insert(task_id.to_string(), ctl.clone())
        };
        // Claims leave out tasks this process runs, so there's no earlier run;
        // if there were, it would be fenced out anyway: stop it.
        if let Some(old) = old {
            self.lose(&old);
        }
        Some(Running {
            inner: Arc::clone(&self.inner),
            ctl,
        })
    }

    /// The handle a run's work goes through: pinned to the current config,
    /// and fenced by the run's claim.
    pub(crate) fn running(&self, run: &Running) -> Engine {
        let mut handle = self.pinned();
        handle.run = Some(Arc::clone(&run.ctl));
        handle
    }

    /// The run lost its claim: it writes nothing more, and its items stop at
    /// their next checkpoint.
    fn lose(&self, ctl: &RunCtl) {
        ctl.lost.store(true, Ordering::SeqCst);
        ctl.cancel.store(true, Ordering::SeqCst);
        self.inner.cancelled.notify_waiters();
    }

    fn runs(&self) -> Vec<Arc<RunCtl>> {
        self.inner
            .runs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    /// The claim `run` holds, renewed now: whether it still holds it, and the
    /// task's state.
    pub(crate) async fn renew_one(&self, run: &RunCtl) -> Renewal {
        match self
            .inner
            .storage
            .renew_claims(&[run.held()], self.inner.lease_ttl)
            .await
        {
            Ok(r) => r.first().copied().unwrap_or(Renewal::Unknown),
            Err(_) => Renewal::Unknown,
        }
    }

    /// Start renewing this process's claims every third of the lease (with a
    /// shared database; on SQLite nobody takes a task over). It goes on while
    /// the engine stops, until its claims are handed back.
    pub(crate) fn spawn_renewal(&self) {
        let engine = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(engine.inner.lease_ttl / 3);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            loop {
                tick.tick().await;
                engine.renew_runs().await;
            }
        });
    }

    /// Renew every run's claim, and act on what the renewal found.
    async fn renew_runs(&self) {
        let runs = self.runs();
        let held: Vec<Held> = runs.iter().map(|r| r.held()).collect();
        let renewals = match self
            .inner
            .storage
            .renew_claims(&held, self.inner.lease_ttl)
            .await
        {
            Ok(r) => {
                self.connected(true);
                r
            }
            Err(e) => {
                // Out of touch with the database: keep running, and try again.
                tracing::warn!(error = %e, "could not renew task leases");
                self.connected(false);
                return;
            }
        };
        for (run, renewal) in runs.iter().zip(renewals) {
            match renewal {
                // Finished or cancelled elsewhere (a cancel through another
                // instance, say): the items stop and finish as cancelled.
                Renewal::Held(state) if aggregate::task_terminal(state) => {
                    if !run.cancel.swap(true, Ordering::SeqCst) {
                        tracing::info!(task = %run.task_id, state = state.as_str(), "task finished elsewhere; stopping it");
                        self.inner.cancelled.notify_waiters();
                    }
                }
                Renewal::Held(_) | Renewal::Unknown => {}
                Renewal::Lost => {
                    if !run.lost() {
                        tracing::warn!(task = %run.task_id, "lost the task's lease (another instance took it over); stopping it");
                        self.lose(run);
                    }
                }
            }
        }
    }

    /// Note whether the database answered, for [`Engine::settled`].
    fn connected(&self, ok: bool) {
        let mut since = self
            .inner
            .connected_since
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match (ok, *since) {
            (true, None) => *since = Some(Instant::now()),
            (false, _) => *since = None,
            _ => {}
        }
    }

    /// Whether this instance has been in touch with the database for a whole
    /// lease, so a lease that lapsed meanwhile really lapsed (not just while
    /// the database was out of reach for everyone).
    pub(crate) fn settled(&self) -> bool {
        self.inner
            .connected_since
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some_and(|t| t.elapsed() >= self.inner.lease_ttl)
    }

    /// How long another node's pinned task waits before it is failed.
    fn pin_grace(&self) -> Duration {
        self.inner.lease_ttl * PIN_GRACE_LEASES
    }

    /// Start the takeover loop: every third of the lease, take over tasks
    /// whose instance stopped (shared database only).
    pub(crate) fn spawn_takeover(&self) {
        let engine = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(engine.inner.lease_ttl / 3);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            loop {
                tick.tick().await;
                if engine.inner.closing.load(Ordering::SeqCst) {
                    return;
                }
                if let Err(e) = engine.take_over().await {
                    tracing::warn!(error = %e, "could not look for tasks to take over");
                }
            }
        });
    }

    /// One takeover pass: claim and start the tasks this instance may take,
    /// up to its free room. On a database only this process uses (SQLite), at
    /// startup, that is every unfinished task, as before leases existed.
    /// Returns how many were taken.
    pub(crate) async fn take_over(&self) -> Result<usize, EngineError> {
        let mut memo = self.inner.takeover.lock().await;
        if self.inner.closing.load(Ordering::SeqCst) {
            return Ok(0);
        }
        let storage = &self.inner.storage;
        let force = !storage.shared();
        let mode = if force {
            ClaimMode::Force
        } else if self.settled() {
            ClaimMode::Expired
        } else {
            ClaimMode::Open
        };
        let forget_after = self.inner.lease_ttl * UNRUNNABLE_LEASES;
        memo.unrunnable.retain(|_, at| at.elapsed() < forget_after);
        let mut taken = 0;
        let mut after: Option<TaskCursor> = None;
        for page_no in 0.. {
            if !force && page_no == PASS_PAGES {
                break;
            }
            let mut skip: Vec<String> = memo.unrunnable.keys().cloned().collect();
            skip.extend(self.runs().iter().map(|r| r.task_id.clone()));
            let page = match storage
                .claimable_tasks(
                    mode,
                    &self.inner.node,
                    &skip,
                    self.pin_grace(),
                    after.as_ref(),
                    PAGE,
                )
                .await
            {
                Ok(page) => {
                    self.connected(true);
                    page
                }
                Err(e) => {
                    self.connected(false);
                    return Err(e.into());
                }
            };
            let Some(last) = page.last() else { break };
            after = Some(TaskCursor {
                created_at: last.created_at,
                id: last.id.clone(),
            });
            let full = page.len() < PAGE;
            for candidate in page {
                match self.take(&candidate, mode, &mut memo).await {
                    Taken::Yes => taken += 1,
                    Taken::No => {}
                    Taken::Full => return Ok(taken),
                }
            }
            if full {
                break;
            }
        }
        Ok(taken)
    }

    /// Claim and start one candidate, if this instance may.
    async fn take(&self, candidate: &Claimable, mode: ClaimMode, memo: &mut Takeover) -> Taken {
        let storage = &self.inner.storage;
        let force = mode == ClaimMode::Force;
        let snapshot = match storage.get_task(&candidate.id).await {
            Ok(Some(task)) => task,
            Ok(None) => return Taken::No,
            Err(e) => {
                tracing::warn!(task = %candidate.id, error = %e, "could not read a task to take over");
                return Taken::No;
            }
        };
        // A cancelled task only needs its items finished, as cancelled: no
        // model, input or room needed.
        let finishing = candidate.state == TaskState::Cancelled;
        // Another node's local input, and that node is gone: it can only fail.
        let abandoned = candidate
            .abandoned_pin
            .clone()
            .filter(|_| !force && !finishing);
        let checked = !(force || finishing || abandoned.is_some());
        // A task nobody can run here: left for an instance that can, unless
        // nobody has run it for longer than the grace (none can, then).
        let mut unrunnable = None;
        if checked && let Err(why) = self.runs_here(&snapshot) {
            if !candidate.stale {
                tracing::info!(task = %snapshot.id, reason = %why, "can't run a task here; leaving it for another instance");
                memo.unrunnable.insert(snapshot.id.clone(), Instant::now());
                return Taken::No;
            }
            unrunnable = Some(why);
        }
        // This node's own local input: nobody else can run it, and it was
        // admitted here already, so the limits for new work don't apply.
        let limited = checked && unrunnable.is_none() && !candidate.pinned_here;
        let task = snapshot;
        let reservations = if limited {
            match self.reserve_task(&task) {
                Ok(r) => r,
                Err(EngineError::Overloaded {
                    reason: Overload::Model,
                    ..
                }) => return Taken::No,
                Err(_) => return Taken::Full,
            }
        } else {
            task.items
                .iter()
                .map(|item| self.reserve_resumed(&item.models))
                .collect()
        };

        let token = uuid::Uuid::new_v4().to_string();
        let attempts = match storage
            .claim_task(&task.id, &self.lease(&token), mode, self.pin_grace())
            .await
        {
            Ok(Some(attempts)) => attempts,
            // Claimed by someone else first, or changed meanwhile.
            Ok(None) => return Taken::No,
            Err(e) => {
                tracing::warn!(task = %task.id, error = %e, "could not claim a task");
                return Taken::No;
            }
        };
        let Some(run) = self.register(&task.id, token.clone()) else {
            self.release(&task.id, &token).await;
            return Taken::Full;
        };
        // Read it again: it may have moved on between the read and the claim.
        // If that read fails, the earlier copy will do: every write of the run
        // is fenced and conditional.
        let task = match storage.get_task(&task.id).await {
            Ok(Some(fresh)) => fresh,
            Ok(None) => return Taken::No,
            Err(e) => {
                tracing::warn!(task = %task.id, error = %e, "could not read a task again after claiming it");
                task
            }
        };
        tracing::info!(task = %task.id, attempts, "took over a task");
        let runner = self.running(&run);
        // A task that can only fail is failed now (a few writes), and isn't
        // counted as resumed.
        if let Some(node) = abandoned {
            runner.fail_abandoned(&task, &node).await;
            drop(run);
            return Taken::No;
        }
        if let Some(why) = unrunnable {
            tracing::warn!(task = %task.id, reason = %why, "no instance has run this task for a long time and this one can't; failing it");
            let err = TaskError::new(
                ErrorKind::ModelUnavailable,
                format!("no instance sharing the database can run the task: {why}"),
            );
            runner.fail_unfinished(&task, err).await;
            drop(run);
            return Taken::No;
        }
        if attempts > MAX_RESUME_ATTEMPTS {
            tracing::warn!(task = %task.id, attempts, "exceeded resume cap; failing task");
            if let Err(e) = runner.fail_poison_task(&task).await {
                tracing::error!(task = %task.id, error = %e, "could not fail a poison task");
            }
            drop(run);
            return Taken::No;
        }
        let engine = self.clone();
        tokio::spawn(async move { engine.run_task(task, run, reservations).await });
        Taken::Yes
    }

    /// Hand a claim that never got a run back at once (the engine is
    /// stopping, so the shutdown's release wouldn't cover it).
    pub(crate) async fn release(&self, task_id: &str, token: &str) {
        let held = Held {
            task_id: task_id.to_string(),
            token: token.to_string(),
        };
        if let Err(e) = self.inner.storage.release_claims(&[held]).await {
            tracing::warn!(task = %task_id, error = %e, "could not hand a task back");
        }
    }

    /// Whether every unfinished item of `task` can run here: its models (or
    /// pipeline, unchanged) are configured and take its input.
    pub(crate) fn runs_here(&self, task: &Task) -> Result<(), String> {
        for item in task
            .items
            .iter()
            .filter(|i| !aggregate::item_terminal(i.state))
        {
            let run = match &item.pipeline {
                Some(name) => Run::Pipeline(name.clone()),
                None => Run::Models(item.models.clone()),
            };
            let (models, _) = self.resolve(&item.input, &run).map_err(|e| e.to_string())?;
            if models != item.models {
                return Err(format!(
                    "pipeline '{}' has other models here",
                    item.pipeline.as_deref().unwrap_or_default()
                ));
            }
        }
        Ok(())
    }

    /// Room for `task`'s unfinished items, under the limits that apply to new
    /// work (its finished items only take a place they give back at once).
    fn reserve_task(&self, task: &Task) -> Result<Vec<Reservation>, EngineError> {
        self.check_memory()?;
        task.items
            .iter()
            .map(|item| {
                if aggregate::item_terminal(item.state) {
                    Ok(self.reserve_resumed(&item.models))
                } else {
                    self.reserve(&item.models)
                }
            })
            .collect()
    }

    /// Fail a task whose input is local to `node`, which stopped before
    /// finishing it: nothing else can read that input.
    async fn fail_abandoned(&self, task: &Task, node: &str) {
        let what = match task.items.iter().map(|i| &i.input).next() {
            Some(Input::Bytes { .. }) => "was uploaded to",
            _ => "is a local file on",
        };
        let err = TaskError::fetch(format!(
            "the input {what} node {node}, which stopped before finishing the task; submit it again"
        ));
        self.fail_unfinished(task, err).await;
    }

    /// Fail every unfinished item of `task` with `err`, and roll it up.
    async fn fail_unfinished(&self, task: &Task, err: TaskError) {
        for (idx, item) in task.items.iter().enumerate() {
            if !aggregate::item_terminal(item.state) {
                self.fail_item(&task.id, idx, item, err.clone()).await;
            }
        }
        self.rollup_task_state(&task.id).await;
    }

    /// Stop every run and hand its claim back (a graceful stop): the tasks are
    /// claimable at once, and the next claim counts no resume attempt.
    pub(crate) async fn hand_back_all(&self) {
        self.inner.closing.store(true, Ordering::SeqCst);
        // Wait out a takeover pass in progress, so it can't claim after this.
        let _pass = self.inner.takeover.lock().await;
        // A stopped run writes nothing more (and its writes are fenced once its
        // claim is handed back), so there's no need to wait for it.
        let runs = self.runs();
        for run in &runs {
            self.lose(run);
        }
        let held: Vec<Held> = runs.iter().map(|r| r.held()).collect();
        match self.inner.storage.release_claims(&held).await {
            Ok(n) if n > 0 => tracing::info!(tasks = n, "handed unfinished tasks back"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "could not hand tasks back"),
        }
    }
}

/// Whether `task` reads something only the node that accepted it can see:
/// an upload, or a local file.
pub(crate) fn pinned_input(task: &Task) -> bool {
    task.items.iter().any(|item| match &item.input {
        Input::Bytes { .. } => true,
        Input::Image(url) | Input::Video(url) | Input::Audio(url) => {
            apollo_media::fetch::is_local_source(&url.main)
                || url
                    .fallback
                    .as_deref()
                    .is_some_and(apollo_media::fetch::is_local_source)
        }
        Input::Text(_) => false,
    })
}
