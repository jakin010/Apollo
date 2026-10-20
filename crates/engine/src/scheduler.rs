//! Per-task dispatch: fetch + decode each input once, fan out to model workers,
//! and gate total in-flight work via the global semaphore.
//!
//! Resume-aware throughout: a model already `Done` from a previous run is skipped,
//! and an interrupted video scan continues from its persisted frame checkpoint.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apollo_config::{EarlyExit, ModelConfig, SamplingKind, StrategyConfig};
use apollo_domain::{
    Classification, DecodedImage, Input, Item, ItemState, MediaKind, Modality, ModelOutput,
    ModelResult, ModelState, Sample, TaskError, Url,
};
use apollo_media::{
    Confirm, FrameRef, FrameRequest, LocalMedia, MediaError, VideoInfo, VideoShare,
};

use crate::Engine;
use crate::aggregate;
use crate::error::EngineError;
use crate::gate::GatePermit;
use crate::worker::{InputShape, ModelHandle};

/// How long to wait for a cold model to download and load. Separate from (and
/// much longer than) the per-classification `timeout`, which only starts once
/// the weights are resident.
const MODEL_LOAD_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// A fetched, decoded/probed input ready for inference.
enum Fetched {
    Image(StillImage),
    Video {
        /// Owns the file `share` reads: a download is deleted when dropped.
        _media: LocalMedia,
        info: VideoInfo,
        /// Probes and decoded frames, shared by every model scanning this video
        /// (and by retries), so each is done once.
        share: Arc<VideoShare>,
    },
    Text(Arc<str>),
}

/// A decoded still image, and copies of it resized to the models' input
/// sizes. Each size is resized once, on the blocking pool, and shared by every
/// model of the item that takes it (and by later pipeline steps), so a model's
/// worker thread gets its input ready to batch instead of resizing a
/// full-resolution photo while the GPU waits.
struct StillImage {
    /// Shared so each model that needs the full image classifies the same
    /// buffer, uncopied.
    full: Arc<DecodedImage>,
    resized: Mutex<HashMap<u32, Arc<tokio::sync::OnceCell<Arc<DecodedImage>>>>>,
}

impl StillImage {
    fn new(full: DecodedImage) -> Self {
        Self {
            full: Arc::new(full),
            resized: Mutex::new(HashMap::new()),
        }
    }

    /// The image as a model with `shape` takes it.
    async fn input_for(&self, shape: InputShape) -> Result<Arc<DecodedImage>, EngineError> {
        let size = shape.size;
        if !shape.resize_ahead || (self.full.width == size && self.full.height == size) {
            return Ok(Arc::clone(&self.full));
        }
        let cell = Arc::clone(self.resized.lock().unwrap().entry(size).or_default());
        let resized = cell
            .get_or_try_init(|| async {
                let full = Arc::clone(&self.full);
                let img = tokio::task::spawn_blocking(move || {
                    apollo_inference::resize_to_input(&full, size)
                })
                .await
                .map_err(|e| EngineError::Join(e.to_string()))?
                .map_err(|e| EngineError::Inference(e.to_string()))?;
                Ok::<_, EngineError>(Arc::new(img))
            })
            .await?;
        Ok(Arc::clone(resized))
    }
}

/// What every model of an item's parallel set shares.
struct ModelRun<'a> {
    task_id: &'a str,
    idx: usize,
    item: &'a Item,
    fetched: &'a Fetched,
    content_hash: Option<&'a str>,
    url_hash: Option<&'a str>,
    fresh_after: i64,
    cancel: &'a AtomicBool,
}

/// How one model of a parallel set ended.
enum SetOutcome {
    /// Done (or served from cache); already persisted.
    Finished(ModelResult),
    /// Failed; already persisted. `transient` failures may be retried.
    Failed {
        result: ModelResult,
        transient: bool,
    },
    /// The task was cancelled mid-run.
    Cancelled,
}

/// Frames extracted per ffmpeg batch. The next batch is extracted while the
/// current one is classified.
const EXTRACT_CHUNK: usize = 32;

/// One model's frame scan over a fetched video.
#[derive(Clone, Copy)]
struct FrameScanRun<'a> {
    task_id: &'a str,
    idx: usize,
    label: &'a str,
    handle: &'a ModelHandle,
    /// The video, with the probes and frames other scans of it share.
    share: &'a Arc<VideoShare>,
    info: &'a VideoInfo,
    /// The model's square input side; frames are extracted at this size.
    input_size: u32,
    /// Per-frame classification deadline.
    timeout: Duration,
    cancel: &'a AtomicBool,
}

/// A frame chunk being extracted in the background. Dropping it (early exit,
/// cancellation, error) aborts the extraction, which kills its ffmpeg process.
struct Prefetch(tokio::task::JoinHandle<Result<Vec<Arc<DecodedImage>>, apollo_media::MediaError>>);

impl Drop for Prefetch {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The result of fetching an item's input once.
struct FetchedInput {
    fetched: Fetched,
    /// SHA-256 of the content, when the result cache is enabled.
    content_hash: Option<String>,
    /// Whether the content came from the `fallback` URL rather than `main`.
    used_fallback: bool,
}

impl Engine {
    /// Process one item: skip if already terminal, fetch the input once, run every
    /// requested model (resumed models skipped), then mark the item terminal and
    /// fire its webhook. Owns `self` so it can be spawned per item.
    pub(crate) async fn run_item(
        self,
        task_id: String,
        idx: usize,
        mut item: Item,
        cancel: Arc<AtomicBool>,
    ) {
        // Release the admission reservation on every exit path.
        let _inflight = crate::InFlightGuard(self.clone());

        // recover() already re-fired webhooks for terminal items.
        if aggregate::item_terminal(item.state) {
            return;
        }
        // Cancelled before this item started: mark it and stop.
        if cancel.load(Ordering::SeqCst) {
            self.cancel_item(&task_id, idx).await;
            return;
        }
        // Resumed item whose models all finished but never got marked complete
        // (crash between the last result and the item transition): finalize without
        // re-fetching or re-running anything.
        if !item.models.is_empty() && item.models.iter().all(|m| model_done(&item, m)) {
            self.complete_item(&task_id, idx).await;
            return;
        }

        // Content cache — URL fast path: when every still-pending model resolves
        // through the url->content-hash->result chain, persist those results and
        // finish without fetching or running anything (bounded by the cache TTL).
        // Skipped for pipeline items: the URL fast path resolves every model from
        // cache at once, which would bypass the per-step gate (a step the gate
        // should skip could be resurrected from another task's content cache).
        // Pipelines consult the content cache per step, after the gate decision.
        if self.cache_enabled()
            && item.pipeline.is_none()
            && let Some(url) = item_url(&item.input)
            && self.try_url_cache(&task_id, idx, &item, url).await
        {
            self.complete_item(&task_id, idx).await;
            return;
        }

        // Bound concurrent items globally (a coarse VRAM/throughput cap; the real
        // GPU batching happens inside each model worker). Admission is by priority:
        // an item's priority is the highest among the models it targets, so a
        // high-priority item is admitted ahead of earlier-queued lower-priority ones.
        // The slot is held while fetching and running models, and released while
        // backing off between retries.
        let priority = self.item_priority(&item);
        let mut permit = Some(self.inner.gate.clone().acquire(priority).await);
        // Waiting for a slot can take a while; honour a cancel that arrived meanwhile.
        if cancel.load(Ordering::SeqCst) {
            self.cancel_item(&task_id, idx).await;
            return;
        }

        if let Err(e) = self
            .inner
            .storage
            .set_item_state(&task_id, idx, ItemState::Processing, None)
            .await
        {
            tracing::error!(task = %task_id, item = idx, error = %e, "set item processing");
        }
        self.notify_item_change(&task_id, idx).await;

        tracing::debug!(task = %task_id, item = idx, models = item.models.len(), "processing item");
        // Fetch with retries: a transient failure (e.g. an unreachable host) puts
        // the item into `Retrying` and tries again, up to `[app].max_retries`. A
        // permanent one (refused URL, undecodable content) fails at once.
        let max_retries = self.config().app.max_retries;
        let FetchedInput {
            fetched,
            content_hash,
            used_fallback,
        } = loop {
            let fetched = self.fetch_item(&item.input).await;
            if let Err(e) = &fetched {
                self.inner.metrics.fetch_failed(e.is_transient());
            }
            match fetched {
                Ok(f) => break f,
                Err(e) if e.is_transient() && item.retries < max_retries => {
                    let err = e.to_fetch_error();
                    if !self
                        .retry_after_failure(
                            &task_id,
                            idx,
                            &mut item,
                            err,
                            &mut permit,
                            priority,
                            &cancel,
                        )
                        .await
                    {
                        return;
                    }
                }
                Err(e) => {
                    // Permanent, or retries exhausted: dead-lettered failure.
                    self.fail_item(&task_id, idx, &item, e.to_fetch_error())
                        .await;
                    return;
                }
            }
        };

        let fresh_after = self.cache_fresh_after();
        // The url->content hint is keyed by `main`, so only record it when the
        // bytes actually came from `main` (not from the fallback).
        let url_hash = if used_fallback {
            None
        } else {
            item_url(&item.input).map(|u| sha256_hex(u.main.as_bytes()))
        };

        // Pipeline items run as an ordered, gated sequence with strict failure
        // semantics; the input was fetched once above and is reused per step.
        if let Some(name) = item.pipeline.clone() {
            self.run_pipeline(
                &task_id,
                idx,
                item,
                &name,
                &fetched,
                content_hash.as_deref(),
                url_hash.as_deref(),
                fresh_after,
                &cancel,
                &mut permit,
                priority,
            )
            .await;
            return;
        }

        // Parallel set: the models run concurrently (each has its own worker
        // thread, and a video scan's frame extraction overlaps the others'
        // inference); a model failure does not sink its siblings. Models that
        // failed transiently are retried (only those — finished models are kept)
        // up to `[app].max_retries`; after that the item completes with the
        // per-model errors.
        let mut permanent_failures: HashSet<String> = HashSet::new();
        loop {
            if cancel.load(Ordering::SeqCst) {
                self.cancel_item(&task_id, idx).await;
                return;
            }
            let todo: Vec<String> = item
                .models
                .iter()
                .filter(|label| !model_done(&item, label) && !permanent_failures.contains(*label))
                .cloned()
                .collect();
            let ctx = ModelRun {
                task_id: &task_id,
                idx,
                item: &item,
                fetched: &fetched,
                content_hash: content_hash.as_deref(),
                url_hash: url_hash.as_deref(),
                fresh_after,
                cancel: &cancel,
            };
            let outcomes =
                futures::future::join_all(todo.iter().map(|label| self.run_set_model(&ctx, label)))
                    .await;

            let mut retryable: Option<TaskError> = None;
            let mut cancelled = false;
            for (label, outcome) in todo.into_iter().zip(outcomes) {
                match outcome {
                    SetOutcome::Finished(result) => {
                        item.results.insert(label, result);
                    }
                    SetOutcome::Failed { result, transient } => {
                        if transient {
                            retryable = result.error.clone();
                        } else {
                            permanent_failures.insert(label.clone());
                        }
                        item.results.insert(label, result);
                    }
                    SetOutcome::Cancelled => cancelled = true,
                }
            }
            if cancelled {
                self.cancel_item(&task_id, idx).await;
                return;
            }

            match retryable {
                Some(err) if item.retries < max_retries => {
                    if !self
                        .retry_after_failure(
                            &task_id,
                            idx,
                            &mut item,
                            err,
                            &mut permit,
                            priority,
                            &cancel,
                        )
                        .await
                    {
                        return;
                    }
                }
                _ => break,
            }
        }

        self.complete_item(&task_id, idx).await;
    }

    /// Record a failed attempt that will be retried: bump and persist the retry
    /// count, mark the item `Retrying` (keeping the error for diagnostics; the
    /// wire still reports the task as processing), notify, and back off. The
    /// concurrency slot is released during the backoff so a retrying item never
    /// blocks other work. Returns `false` if the task was cancelled meanwhile (the
    /// item is then already marked cancelled).
    #[allow(clippy::too_many_arguments)]
    async fn retry_after_failure(
        &self,
        task_id: &str,
        idx: usize,
        item: &mut Item,
        err: TaskError,
        permit: &mut Option<GatePermit>,
        priority: i32,
        cancel: &AtomicBool,
    ) -> bool {
        let max = self.config().app.max_retries;
        item.retries += 1;
        let _ = self
            .inner
            .storage
            .set_item_retries(task_id, idx, item.retries)
            .await;
        let _ = self
            .inner
            .storage
            .set_item_state(task_id, idx, ItemState::Retrying, Some(&err))
            .await;
        tracing::warn!(
            task = %task_id, item = idx, attempt = item.retries, max,
            error = %err.message, "item attempt failed; will retry"
        );
        self.notify_item_change(task_id, idx).await;

        permit.take();
        tokio::time::sleep(retry_backoff(item.retries)).await;
        if cancel.load(Ordering::SeqCst) {
            self.cancel_item(task_id, idx).await;
            return false;
        }
        *permit = Some(self.inner.gate.clone().acquire(priority).await);
        if cancel.load(Ordering::SeqCst) {
            self.cancel_item(task_id, idx).await;
            return false;
        }
        let _ = self
            .inner
            .storage
            .set_item_state(task_id, idx, ItemState::Processing, None)
            .await;
        true
    }

    /// Run one model of a parallel set or pipeline step: content-cache fast
    /// path, else run it, cache its output, and persist the result. Runs
    /// concurrently with the set's (or step's) other models.
    async fn run_set_model(&self, ctx: &ModelRun<'_>, label: &str) -> SetOutcome {
        let (task_id, idx) = (ctx.task_id, ctx.idx);

        // Content-cache fast path: identical bytes already classified by this
        // model + revision -> reuse the stored output, skip inference.
        if let Some(ch) = ctx.content_hash {
            let rev = self.model_revision(label);
            match self
                .inner
                .storage
                .cache_lookup(ch, label, &rev, ctx.fresh_after)
                .await
            {
                Ok(Some(output)) => {
                    let done = ModelResult::done(output);
                    let _ = self
                        .inner
                        .storage
                        .upsert_model_result(task_id, idx, label, &done)
                        .await;
                    if let Some(uh) = ctx.url_hash {
                        let _ = self
                            .inner
                            .storage
                            .url_cache_store(uh, label, &rev, ch)
                            .await;
                    }
                    tracing::debug!(task = %task_id, item = idx, model = %label, "cache hit");
                    self.inner
                        .metrics
                        .cache_hits
                        .fetch_add(1, Ordering::Relaxed);
                    return SetOutcome::Finished(done);
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(task = %task_id, model = %label, error = %e, "cache lookup failed")
                }
            }
        }

        let _ = self
            .inner
            .storage
            .upsert_model_result(task_id, idx, label, &ModelResult::processing())
            .await;

        tracing::debug!(task = %task_id, item = idx, model = %label, "running model");
        // The model's `timeout` bounds each individual classification — a single
        // image, or *each frame* of a video scan — not the whole video, and not
        // the time to load a cold model.
        let outcome = match self
            .run_model(task_id, idx, label, ctx.item, ctx.fetched, ctx.cancel)
            .await
        {
            Ok(output) => {
                self.store_in_cache(task_id, label, ctx.content_hash, ctx.url_hash, &output)
                    .await;
                SetOutcome::Finished(ModelResult::done(output))
            }
            Err(EngineError::Cancelled) => return SetOutcome::Cancelled,
            Err(e) => SetOutcome::Failed {
                result: ModelResult::failed(e.to_model_error()),
                transient: e.is_transient(),
            },
        };
        let result = match &outcome {
            SetOutcome::Finished(r) | SetOutcome::Failed { result: r, .. } => r,
            SetOutcome::Cancelled => unreachable!("returned above"),
        };
        let _ = self
            .inner
            .storage
            .upsert_model_result(task_id, idx, label, result)
            .await;
        outcome
    }

    /// Populate the content cache (and the url->content hint) after a successful
    /// model run. Best-effort.
    async fn store_in_cache(
        &self,
        task_id: &str,
        label: &str,
        content_hash: Option<&str>,
        url_hash: Option<&str>,
        output: &ModelOutput,
    ) {
        let Some(ch) = content_hash else {
            return;
        };
        let rev = self.model_revision(label);
        if let Err(e) = self
            .inner
            .storage
            .cache_store(ch, label, &rev, output)
            .await
        {
            tracing::warn!(task = %task_id, model = %label, error = %e, "cache store failed");
        }
        if let Some(uh) = url_hash {
            let _ = self
                .inner
                .storage
                .url_cache_store(uh, label, &rev, ch)
                .await;
        }
    }

    /// Run an item through a named pipeline: its steps in `order`, each step's
    /// models concurrently (as in a parallel set), each step optionally gated by
    /// `stop_if`, checked once the whole step has finished. A gate firing skips
    /// every model of the remaining steps and completes the task normally. A
    /// model *failure* fails the whole pipeline: a transient one is retried up to
    /// `[app].max_retries` (finished models are kept), then the item is
    /// dead-lettered. The input was fetched once by the caller and is reused.
    #[allow(clippy::too_many_arguments)]
    async fn run_pipeline(
        &self,
        task_id: &str,
        idx: usize,
        mut item: Item,
        pipeline: &str,
        fetched: &Fetched,
        content_hash: Option<&str>,
        url_hash: Option<&str>,
        fresh_after: i64,
        cancel: &AtomicBool,
        permit: &mut Option<GatePermit>,
        priority: i32,
    ) {
        let live_config = self.config();
        let Some(config) = live_config.pipelines.get(pipeline) else {
            self.fail_item(
                task_id,
                idx,
                &item,
                TaskError::internal(format!("unknown pipeline '{pipeline}'")),
            )
            .await;
            return;
        };
        let steps = config.ordered_steps();
        let max_retries = self.config().app.max_retries;

        let stopped_at = 'attempt: loop {
            for (i, step) in steps.iter().enumerate() {
                if cancel.load(Ordering::SeqCst) {
                    self.cancel_item(task_id, idx).await;
                    return;
                }

                // Models already finished (resume, or a prior attempt) are not
                // re-run; the step's gate is still checked below.
                let todo: Vec<String> = step
                    .models()
                    .iter()
                    .filter(|m| !model_done(&item, m))
                    .cloned()
                    .collect();
                if !todo.is_empty() {
                    let ctx = ModelRun {
                        task_id,
                        idx,
                        item: &item,
                        fetched,
                        content_hash,
                        url_hash,
                        fresh_after,
                        cancel,
                    };
                    let outcomes = futures::future::join_all(
                        todo.iter().map(|model| self.run_set_model(&ctx, model)),
                    )
                    .await;

                    // A permanent failure outranks a transient one.
                    let mut failure: Option<(TaskError, bool)> = None;
                    let mut cancelled = false;
                    for (model, outcome) in todo.into_iter().zip(outcomes) {
                        match outcome {
                            SetOutcome::Finished(result) => {
                                item.results.insert(model, result);
                            }
                            SetOutcome::Failed { result, transient } => {
                                if !matches!(failure, Some((_, false))) {
                                    let err = result.error.clone().unwrap_or_else(|| {
                                        TaskError::internal(format!("model '{model}' failed"))
                                    });
                                    failure = Some((err, transient));
                                }
                                item.results.insert(model, result);
                            }
                            SetOutcome::Cancelled => cancelled = true,
                        }
                    }
                    if cancelled {
                        self.cancel_item(task_id, idx).await;
                        return;
                    }
                    if let Some((err, transient)) = failure {
                        if transient && item.retries < max_retries {
                            if !self
                                .retry_after_failure(
                                    task_id, idx, &mut item, err, permit, priority, cancel,
                                )
                                .await
                            {
                                return;
                            }
                            continue 'attempt;
                        }
                        // Strict: a step failure fails the whole pipeline.
                        self.fail_item(task_id, idx, &item, err).await;
                        return;
                    }
                }

                if let (Some(stop), Some(gate)) = (&step.stop_if, step.gate_model())
                    && let Some(out) = item.results.get(gate).and_then(|r| r.output.as_ref())
                    && output_triggers(out, &stop.labels, stop.threshold)
                {
                    break 'attempt Some(i);
                }
            }
            break 'attempt None;
        };

        // A gate fired: mark every model of the later steps Skipped.
        if let Some(i) = stopped_at {
            for model in steps.iter().skip(i + 1).flat_map(|s| s.models()) {
                if !model_done(&item, model) {
                    let _ = self
                        .inner
                        .storage
                        .upsert_model_result(task_id, idx, model, &ModelResult::skipped())
                        .await;
                }
            }
        }

        self.complete_item(task_id, idx).await;
    }

    /// Whether the result cache is configured and enabled.
    fn cache_enabled(&self) -> bool {
        self.config().cache.as_ref().is_some_and(|c| c.enabled)
    }

    /// Lower bound (unix secs) on a cache entry's `created_at` for it to count as
    /// fresh: `now - ttl`, or `0` (no cutoff) when no TTL is configured.
    fn cache_fresh_after(&self) -> i64 {
        match self.config().cache.as_ref().and_then(|c| c.ttl_secs) {
            Some(ttl) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                now - ttl as i64
            }
            None => 0,
        }
    }

    /// The configured revision for a model label (part of the cache key, so a
    /// revision bump invalidates prior cache entries). Empty if unknown.
    fn model_revision(&self, label: &str) -> String {
        self.config()
            .models
            .get(label)
            .map(|m| m.revision.clone())
            .unwrap_or_default()
    }

    /// Finalize an item as completed: persist the state, roll up the task, and
    /// fire the terminal webhook.
    async fn complete_item(&self, task_id: &str, idx: usize) {
        if let Err(e) = self
            .inner
            .storage
            .set_item_state(task_id, idx, ItemState::Completed, None)
            .await
        {
            tracing::error!(task = %task_id, item = idx, error = %e, "set item completed");
        }
        self.inner.metrics.item_finished(ItemState::Completed);
        self.rollup_task_state(task_id).await;
        self.deliver_webhook(task_id, idx).await;
    }

    /// All-or-nothing URL fast path. If every not-yet-done model for this item
    /// resolves through the url->content-hash->result chain (fresh per the TTL),
    /// persist those results and return true; on any miss, persist nothing and
    /// return false so the caller fetches and hashes the actual bytes instead.
    async fn try_url_cache(&self, task_id: &str, idx: usize, item: &Item, url: &Url) -> bool {
        let fresh_after = self.cache_fresh_after();
        let url_hash = sha256_hex(url.main.as_bytes());
        let mut resolved: Vec<(String, ModelOutput)> = Vec::new();
        for label in &item.models {
            if model_done(item, label) {
                continue;
            }
            let rev = self.model_revision(label);
            let content_hash = match self
                .inner
                .storage
                .url_cache_lookup(&url_hash, label, &rev, fresh_after)
                .await
            {
                Ok(Some(ch)) => ch,
                _ => return false,
            };
            match self
                .inner
                .storage
                .cache_lookup(&content_hash, label, &rev, fresh_after)
                .await
            {
                Ok(Some(output)) => resolved.push((label.clone(), output)),
                _ => return false,
            }
        }
        for (label, output) in resolved {
            self.inner
                .metrics
                .cache_hits
                .fetch_add(1, Ordering::Relaxed);
            let _ = self
                .inner
                .storage
                .upsert_model_result(task_id, idx, &label, &ModelResult::done(output))
                .await;
        }
        true
    }

    /// Mark an item cancelled (preserving any results already produced), roll the
    /// task state up, and fire the terminal webhook.
    async fn cancel_item(&self, task_id: &str, idx: usize) {
        let _ = self
            .inner
            .storage
            .set_item_state(
                task_id,
                idx,
                ItemState::Cancelled,
                Some(&TaskError::cancelled("cancelled")),
            )
            .await;
        self.inner.metrics.item_finished(ItemState::Cancelled);
        self.rollup_task_state(task_id).await;
        self.deliver_webhook(task_id, idx).await;
    }

    /// Mark the item (and its not-yet-done models) failed, then fire the webhook.
    /// Preserves results from models that already finished on a prior run, and
    /// the errors of models that already failed.
    pub(crate) async fn fail_item(&self, task_id: &str, idx: usize, item: &Item, err: TaskError) {
        for label in &item.models {
            let failed = item
                .results
                .get(label)
                .is_some_and(|r| r.state == ModelState::Failed);
            if model_done(item, label) || failed {
                continue;
            }
            let _ = self
                .inner
                .storage
                .upsert_model_result(task_id, idx, label, &ModelResult::failed(err.clone()))
                .await;
        }
        let _ = self
            .inner
            .storage
            .set_item_state(task_id, idx, ItemState::Failed, Some(&err))
            .await;
        self.inner.metrics.item_finished(ItemState::Failed);
        self.rollup_task_state(task_id).await;
        self.deliver_webhook(task_id, idx).await;
    }

    /// Recompute the task's lifecycle state from its items and persist it when it
    /// changed. Called as each item reaches a terminal state, so the task flips to
    /// `Completed` exactly when its last item does (and the webhook payload then
    /// carries the correct task state). A task already in a terminal state
    /// (cancelled, failed) keeps it.
    pub async fn rollup_task_state(&self, task_id: &str) {
        let task = match self.inner.storage.get_task(task_id).await {
            Ok(Some(t)) => t,
            _ => return,
        };
        let state = if aggregate::task_terminal(task.state) {
            task.state
        } else {
            let desired = aggregate::task_state_for(&task);
            if desired != task.state
                && let Err(e) = self.inner.storage.set_task_state(task_id, desired).await
            {
                tracing::error!(task = %task_id, error = %e, "failed to roll up task state");
            }
            desired
        };
        // Once the task is finished for good and no item can still be reading
        // them, remove any staged upload files. They persist across restarts (so
        // resume can re-read them) and are cleaned exactly here — including for
        // cancelled and failed tasks.
        if aggregate::task_terminal(state)
            && task
                .items
                .iter()
                .all(|it| aggregate::item_terminal(it.state))
        {
            self.cleanup_uploads(&task.items);
        }
    }

    /// Best-effort removal of `Input::Bytes` upload files for a finished task.
    fn cleanup_uploads(&self, items: &[Item]) {
        for item in items {
            if let Input::Bytes { path, .. } = &item.input {
                match std::fs::remove_file(path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "failed to remove upload file"
                    ),
                }
            }
        }
    }

    /// Run a single model against the already-fetched input, choosing the path by
    /// input modality (image → one classification; video → per-frame scan).
    async fn run_model(
        &self,
        task_id: &str,
        idx: usize,
        label: &str,
        item: &Item,
        fetched: &Fetched,
        cancel: &AtomicBool,
    ) -> Result<ModelOutput, EngineError> {
        let handle = self
            .live()
            .registry
            .get(label)
            .ok_or_else(|| EngineError::UnknownModel(label.to_string()))?;

        // Wait for a cold model to download/load before starting the per-
        // classification clock, so first use is not reported as a timeout.
        let shape = match tokio::time::timeout(MODEL_LOAD_TIMEOUT, handle.ensure_loaded()).await {
            Ok(loaded) => loaded?,
            Err(_) => {
                return Err(EngineError::ModelUnavailable(format!(
                    "model '{label}' did not load within {}s",
                    MODEL_LOAD_TIMEOUT.as_secs()
                )));
            }
        };

        let timeout = self.model_timeout(label);
        match (item.input.modality(), fetched) {
            (Modality::Image, Fetched::Image(still)) => {
                // One image: the whole classification is bounded by `timeout`,
                // which starts once the image is resized for the model.
                let img = still.input_for(shape).await?;
                let on_timeout = format!("model '{label}' timed out classifying image");
                let c =
                    classify_with_timeout(timeout, handle.classify_image(img), &on_timeout).await?;
                Ok(ModelOutput::Classification(c))
            }
            (Modality::Text, Fetched::Text(text)) => {
                // One text, all its windows: bounded by `timeout`.
                let on_timeout = format!("model '{label}' timed out classifying text");
                let c = classify_with_timeout(
                    timeout,
                    handle.classify_text(Arc::clone(text)),
                    &on_timeout,
                )
                .await?;
                Ok(ModelOutput::Classification(c))
            }
            (Modality::Video, Fetched::Video { info, share, .. }) => {
                // Video: an image classifier over sampled frames, each frame bounded
                // by `timeout` (applied inside run_frame_scan).
                let scan = FrameScanRun {
                    task_id,
                    idx,
                    label,
                    handle: &handle,
                    share,
                    info,
                    input_size: shape.size,
                    timeout,
                    cancel,
                };
                self.run_frame_scan(&scan).await
            }
            _ => Err(EngineError::Incompatible(format!(
                "input/model mismatch for '{label}'"
            ))),
        }
    }

    /// Fetch the input once: download (with fallback), then decode an image or
    /// probe a video.
    async fn fetch_item(&self, input: &Input) -> Result<FetchedInput, EngineError> {
        let want_hash = self.cache_enabled();
        let (media, video) = match input {
            Input::Image(url) => (self.fetch_url(url).await?, false),
            Input::Video(url) => (self.fetch_url(url).await?, true),
            Input::Text(text) => return self.text_input(text.as_str().into(), want_hash),
            Input::Bytes {
                path,
                kind: MediaKind::Text,
            } => {
                let bytes = LocalMedia::adopt(path.clone()).read_bytes()?;
                let text = String::from_utf8(bytes).map_err(|_| {
                    EngineError::Media(MediaError::Decode("the uploaded text is not UTF-8".into()))
                })?;
                return self.text_input(text.into(), want_hash);
            }
            Input::Bytes { path, kind } => (
                LocalMedia::adopt(path.clone()),
                match kind {
                    MediaKind::Image => false,
                    MediaKind::Video => true,
                    MediaKind::Audio | MediaKind::Text => {
                        return Err(EngineError::Incompatible(
                            "audio inputs are not supported yet".into(),
                        ));
                    }
                },
            ),
            Input::Audio(_) => {
                return Err(EngineError::Incompatible(
                    "audio inputs are not supported yet".into(),
                ));
            }
        };
        let used_fallback = media.used_fallback();
        let (fetched, content_hash) = if video {
            self.video_from_media(media, want_hash).await?
        } else {
            self.image_from_media(media, want_hash).await?
        };
        Ok(FetchedInput {
            fetched,
            content_hash,
            used_fallback,
        })
    }

    /// A text input, checked against `[limits].max_text` and hashed for the
    /// cache when `want_hash` is set.
    fn text_input(&self, text: Arc<str>, want_hash: bool) -> Result<FetchedInput, EngineError> {
        if let Some(max) = self.config().limits.max_text_bytes()
            && text.len() as u64 > max
        {
            return Err(EngineError::Media(MediaError::Decode(format!(
                "the text is {} bytes, over the {max}-byte limit ([limits].max_text)",
                text.len()
            ))));
        }
        let content_hash = want_hash.then(|| sha256_hex(text.as_bytes()));
        Ok(FetchedInput {
            fetched: Fetched::Text(text),
            content_hash,
            used_fallback: false,
        })
    }

    /// Fetch a URL input (with its fallback), recording the time and size.
    async fn fetch_url(&self, url: &Url) -> Result<LocalMedia, EngineError> {
        let started = std::time::Instant::now();
        let limits = self.live().fetch_limits.clone();
        let media = apollo_media::fetch(url, &limits).await?;
        let metrics = &self.inner.metrics;
        metrics.fetch_seconds.observe_duration(started.elapsed());
        if let Ok(meta) = std::fs::metadata(media.path()) {
            metrics.fetch_bytes.fetch_add(meta.len(), Ordering::Relaxed);
        }
        Ok(media)
    }

    /// Decode `media` as an image, hashing the raw bytes for the cache key when
    /// `want_hash` is set.
    async fn image_from_media(
        &self,
        media: LocalMedia,
        want_hash: bool,
    ) -> Result<(Fetched, Option<String>), EngineError> {
        let bytes = media.read_bytes()?;
        let hash = if want_hash {
            Some(sha256_hex(&bytes))
        } else {
            None
        };
        let max_pixels = self.live().max_pixels;
        let img =
            tokio::task::spawn_blocking(move || apollo_media::decode_image(&bytes, max_pixels))
                .await
                .map_err(|e| EngineError::Join(e.to_string()))??;
        Ok((Fetched::Image(StillImage::new(img)), hash))
    }

    /// Probe `media` as a video, hashing the file for the cache key when
    /// `want_hash` is set. A hash failure degrades to "no caching", never an error.
    async fn video_from_media(
        &self,
        media: LocalMedia,
        want_hash: bool,
    ) -> Result<(Fetched, Option<String>), EngineError> {
        let info = apollo_media::probe(media.path()).await?;
        let max = self.config().limits.max_video_seconds;
        if max > 0 && info.duration > max as f64 {
            return Err(EngineError::Incompatible(format!(
                "video is {:.0}s, over the {max}s limit",
                info.duration
            )));
        }
        // Reject oversized frames up front, before ffmpeg decodes (and PNG-encodes)
        // any of them at full resolution.
        if let Some(max_px) = self.live().max_pixels {
            let px = u64::from(info.width) * u64::from(info.height);
            if px > max_px {
                return Err(EngineError::Media(MediaError::Decode(format!(
                    "video frames are {}x{} ({px} pixels), over the {max_px}-pixel limit",
                    info.width, info.height
                ))));
            }
        }
        let hash = if want_hash {
            let p = media.path().to_path_buf();
            tokio::task::spawn_blocking(move || sha256_file(&p))
                .await
                .ok()
                .and_then(|r| r.ok())
        } else {
            None
        };
        let share = Arc::new(VideoShare::new(media.path().to_path_buf(), info.clone()));
        Ok((
            Fetched::Video {
                _media: media,
                info,
                share,
            },
            hash,
        ))
    }

    /// Image-classifier over a video: plan frames per the model's strategy one
    /// step at a time, skip frames already classified (resume), and classify the
    /// rest, stopping early when a trigger fires (later steps are then never
    /// planned). With the strategy's `confirm`, a frame reaching the
    /// trigger only counts once enough frames close to it agree (frames are
    /// sampled there when the plan has none). If nothing triggers and the
    /// strategy has `refine` steps, frames around the ambiguous scores are
    /// planned from the results and classified the same way. Rolls up into a
    /// `Scan`, with the segments where the trigger fired.
    async fn run_frame_scan(&self, scan: &FrameScanRun<'_>) -> Result<ModelOutput, EngineError> {
        let FrameScanRun {
            task_id,
            idx,
            label,
            share,
            info,
            ..
        } = *scan;
        let model = self.model_cfg(label)?;
        let strategy = self.strategy_for(&model)?;
        let trigger = Trigger::for_scan(&strategy, &model);
        let scan_metrics = self.inner.metrics.model(label);
        let result = |frames: Vec<Sample>, stopped_early: bool| {
            scan_metrics.scans.fetch_add(1, Ordering::Relaxed);
            if stopped_early {
                scan_metrics
                    .scan_early_exits
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(ModelOutput::Scan(aggregate::scan(
                frames,
                strategy.aggregation,
                trigger.as_ref().map(Trigger::flagging),
                info.duration,
            )))
        };

        let budget = strategy.max_frames.map(|m| m as usize);

        // Seed from frames already persisted on a previous run.
        let mut frames = self
            .inner
            .storage
            .load_samples(task_id, idx, label)
            .await
            .unwrap_or_default();
        let done_idx: HashSet<u32> = frames.iter().map(|f| f.index).collect();

        // A resumed scan may already have hit its trigger before the interruption
        // (crash after the frame checkpoint, before the result was written), or
        // been interrupted while confirming one.
        if let Some(t) = trigger.as_ref() {
            let fired = match t.rule {
                None => frames.iter().any(|f| t.reached(f)),
                Some(_) => {
                    let candidates: Vec<FrameRef> = frames
                        .iter()
                        .filter(|f| t.candidate(f))
                        .map(frame_ref)
                        .collect();
                    self.confirm_candidates(scan, &model, t, &candidates, &mut frames)
                        .await?
                }
            };
            if t.stop && fired {
                return result(frames, true);
            }
        }

        // Plan lazily: a step that needs a probe still running — like scene
        // detection's full decode — is only planned once the steps before it
        // were classified without a trigger. Steps that need no probe (or whose
        // probe has finished) join the same batch, so frames still go through
        // ffmpeg and the model together. While a batch is classified, the next
        // step's probe runs in the background; it is cancelled if the scan stops
        // first. Indices run on across steps, so a resumed scan plans the same
        // frames and skips the ones it already has.
        let mut planner = apollo_media::Planner::new(info, &strategy.sampling, budget);
        let mut plan: Vec<FrameRef> = Vec::new();
        let mut stopped = false;
        // Held while the batch before its step is classified; dropping it cancels.
        let mut _warmup: Option<apollo_media::Warmup> = None;
        while let Some(mut batch) = planner.next_step(share).await? {
            while planner.upcoming().is_some_and(|next| share.ready(next))
                && let Some(more) = planner.next_step(share).await?
            {
                batch.extend(more);
            }
            _warmup = planner.upcoming().and_then(|next| share.warm(next));
            let pending: Vec<FrameRef> = batch
                .iter()
                .filter(|f| !done_idx.contains(&f.index))
                .copied()
                .collect();
            tracing::debug!(
                model = %label,
                frames = batch.len(),
                resuming_from = batch.len() - pending.len(),
                "frame scan steps planned"
            );
            plan.extend(batch);
            if self
                .classify_frames(scan, &model, trigger.as_ref(), &pending, &mut frames)
                .await?
            {
                stopped = true;
                break;
            }
        }

        // Look closer around ambiguous scores. Planned only once every planned
        // frame is classified, from their stored scores, so a resumed scan plans
        // the same frames and skips the ones it already has. With confirmation,
        // an unconfirmed frame counts as its surroundings here too.
        let has_refine = strategy
            .sampling
            .iter()
            .any(|s| s.method == SamplingKind::Refine);
        if !stopped
            && has_refine
            && let Some(t) = trigger.as_ref()
        {
            let all: Vec<(f64, f32)> = frames.iter().map(|f| (f.start, t.score(f))).collect();
            let effective = match t.rule {
                Some(rule) => apollo_media::confirm::confirmed_scores(&all, t.threshold, rule),
                None => all.iter().map(|&(_, s)| s).collect(),
            };
            let by_index: HashMap<u32, f32> = frames
                .iter()
                .zip(effective)
                .map(|(f, score)| (f.index, score))
                .collect();
            let scored: Vec<(FrameRef, f32)> = plan
                .iter()
                .filter_map(|f| by_index.get(&f.index).map(|&score| (*f, score)))
                .collect();
            let extra = apollo_media::refine_plan(
                info,
                &strategy.sampling,
                &scored,
                t.threshold,
                plan.len() as u32,
                budget.map_or(usize::MAX, |b| b.saturating_sub(plan.len())),
            );
            let pending: Vec<FrameRef> = extra
                .into_iter()
                .filter(|f| !done_idx.contains(&f.index))
                .collect();
            tracing::debug!(model = %label, frames = pending.len(), "frame scan refining");
            self.classify_frames(scan, &model, Some(t), &pending, &mut frames)
                .await?;
        }

        let shared = share.stats();
        tracing::debug!(
            model = %label,
            probes_run = shared.probes_run,
            probes_shared = shared.probes_shared,
            frames_decoded = shared.frames_decoded,
            frames_shared = shared.frames_shared,
            "frame scan done (video totals so far)"
        );
        result(frames, stopped)
    }

    /// Classify `pending` frames in order, appending results to `frames`.
    /// Frames are extracted [`EXTRACT_CHUNK`] at a time by one ffmpeg pass at the
    /// model's input size, the next chunk extracted while the current one is
    /// classified; classification goes in worker-sized batches, each persisted as
    /// one checkpoint. Returns whether a trigger fired and stopped the scan.
    async fn classify_frames(
        &self,
        scan: &FrameScanRun<'_>,
        model: &ModelConfig,
        trigger: Option<&Trigger>,
        pending: &[FrameRef],
        frames: &mut Vec<Sample>,
    ) -> Result<bool, EngineError> {
        let FrameScanRun {
            share,
            input_size,
            cancel,
            ..
        } = *scan;
        let batch = model.max_concurrent.max(1) as usize;

        let chunks: Vec<&[FrameRef]> = pending.chunks(EXTRACT_CHUNK.max(batch)).collect();
        let extract = |chunk: &[FrameRef]| {
            let share = Arc::clone(share);
            let requests = frame_requests(chunk);
            Prefetch(tokio::spawn(async move {
                share.frames(&requests, input_size).await
            }))
        };

        let mut next = chunks.first().map(|c| extract(c));
        for (n, chunk) in chunks.iter().enumerate() {
            // Cancellation checkpoint between extraction chunks.
            if cancel.load(Ordering::SeqCst) {
                return Err(EngineError::Cancelled);
            }
            let images = match next.take() {
                Some(mut prefetch) => (&mut prefetch.0)
                    .await
                    .map_err(|e| EngineError::Join(e.to_string()))??,
                None => break,
            };
            // Start extracting the next chunk while this one is classified.
            next = chunks.get(n + 1).map(|c| extract(c));

            for (refs, imgs) in chunk.chunks(batch).zip(images.chunks(batch)) {
                if cancel.load(Ordering::SeqCst) {
                    return Err(EngineError::Cancelled);
                }
                let (mut checkpoint, failure) = self.classify_batch(scan, refs, imgs).await;
                // Without confirmation the first frame reaching the trigger stops
                // the scan; the rest of the batch is not kept.
                let mut stop = false;
                if let Some(t) = trigger.filter(|t| t.stop && t.rule.is_none())
                    && let Some(at) = checkpoint.iter().position(|f| t.reached(f))
                {
                    checkpoint.truncate(at + 1);
                    stop = true;
                }
                // Persist what this batch produced, even if a frame failed, so a
                // retry or resume does not redo it.
                self.save_checkpoint(scan, &checkpoint).await;
                let candidates: Vec<FrameRef> = match trigger {
                    Some(t) if t.rule.is_some() => checkpoint
                        .iter()
                        .filter(|f| t.candidate(f))
                        .map(frame_ref)
                        .collect(),
                    _ => Vec::new(),
                };
                frames.extend(checkpoint);
                if let Some(e) = failure {
                    return Err(e);
                }
                if stop {
                    return Ok(true);
                }
                if let Some(t) = trigger
                    && !candidates.is_empty()
                    && self
                        .confirm_candidates(scan, model, t, &candidates, frames)
                        .await?
                    && t.stop
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Decide `candidates` (frames that reached the trigger) under the strategy's
    /// confirmation rule, first sampling around those the scan has no frames
    /// near (see [`apollo_media::confirm::targets`]; frames already classified,
    /// e.g. before a resume, are reused). Returns whether any is confirmed.
    async fn confirm_candidates(
        &self,
        scan: &FrameScanRun<'_>,
        model: &ModelConfig,
        trigger: &Trigger,
        candidates: &[FrameRef],
        frames: &mut Vec<Sample>,
    ) -> Result<bool, EngineError> {
        let Some(rule) = trigger.rule else {
            return Ok(false);
        };
        // Which candidates the frames so far already confirm.
        let confirmed = |frames: &[Sample]| -> Vec<bool> {
            let scored: Vec<(f64, f32)> =
                frames.iter().map(|f| (f.start, trigger.score(f))).collect();
            candidates
                .iter()
                .map(|c| {
                    frames
                        .iter()
                        .position(|f| f.index == c.index)
                        .is_some_and(|i| {
                            apollo_media::confirm::is_confirmed(&scored, i, trigger.threshold, rule)
                        })
                })
                .collect()
        };
        let before = confirmed(frames);
        if trigger.stop && before.contains(&true) {
            return Ok(true);
        }

        // Sample around the others, where the scan has no frames yet.
        let mut targets: Vec<FrameRef> = Vec::new();
        for (c, _) in candidates.iter().zip(&before).filter(|(_, done)| !**done) {
            for t in apollo_media::confirm::targets(scan.info, c, rule) {
                let near = |u: f64| (u - t.timestamp).abs() <= apollo_media::DEDUPE_TOLERANCE;
                let have = frames.iter().any(|f| f.index == t.index || near(f.start))
                    || targets.iter().any(|x| near(x.timestamp));
                if !have {
                    targets.push(t);
                }
            }
        }
        if !targets.is_empty() {
            tracing::debug!(model = %scan.label, frames = targets.len(), "confirming a trigger");
            self.classify_refs(scan, model, &targets, frames).await?;
        }
        Ok(confirmed(frames).contains(&true))
    }

    /// Extract and classify `refs` (in worker-sized batches, each persisted as a
    /// checkpoint), appending the results to `frames`.
    async fn classify_refs(
        &self,
        scan: &FrameScanRun<'_>,
        model: &ModelConfig,
        refs: &[FrameRef],
        frames: &mut Vec<Sample>,
    ) -> Result<(), EngineError> {
        let batch = model.max_concurrent.max(1) as usize;
        for chunk in refs.chunks(EXTRACT_CHUNK.max(batch)) {
            if scan.cancel.load(Ordering::SeqCst) {
                return Err(EngineError::Cancelled);
            }
            let images = scan
                .share
                .frames(&frame_requests(chunk), scan.input_size)
                .await?;
            for (refs, imgs) in chunk.chunks(batch).zip(images.chunks(batch)) {
                let (checkpoint, failure) = self.classify_batch(scan, refs, imgs).await;
                self.save_checkpoint(scan, &checkpoint).await;
                frames.extend(checkpoint);
                if let Some(e) = failure {
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Classify one batch of extracted frames, issued concurrently so the worker
    /// merges it into one forward pass; each frame is bounded by the per-frame
    /// timeout. Returns the frames classified before the first failure, and that
    /// failure.
    async fn classify_batch(
        &self,
        scan: &FrameScanRun<'_>,
        refs: &[FrameRef],
        imgs: &[Arc<DecodedImage>],
    ) -> (Vec<Sample>, Option<EngineError>) {
        let timeout_msg = format!("model '{}' timed out classifying a video frame", scan.label);
        let calls = imgs.iter().map(|img| {
            classify_with_timeout(
                scan.timeout,
                scan.handle.classify_image(Arc::clone(img)),
                &timeout_msg,
            )
        });
        let results = futures::future::join_all(calls).await;
        let classified = results.iter().filter(|r| r.is_ok()).count() as u64;
        self.inner
            .metrics
            .model(scan.label)
            .scan_frames
            .fetch_add(classified, Ordering::Relaxed);
        let mut out = Vec::with_capacity(refs.len());
        for (fref, res) in refs.iter().zip(results) {
            match res {
                Ok(classification) => {
                    out.push(Sample::frame(fref.index, fref.timestamp, classification))
                }
                Err(e) => return (out, Some(e)),
            }
        }
        (out, None)
    }

    /// Persist classified frames as one checkpoint; a failure is logged, not
    /// fatal (the frames are only re-done on resume).
    async fn save_checkpoint(&self, scan: &FrameScanRun<'_>, frames: &[Sample]) {
        if let Err(e) = self
            .inner
            .storage
            .append_samples(scan.task_id, scan.idx, scan.label, frames)
            .await
        {
            tracing::warn!(task = %scan.task_id, model = %scan.label, error = %e, "frame checkpoint failed");
        }
    }

    // ------------------------- small config lookups -------------------------

    fn model_cfg(&self, label: &str) -> Result<ModelConfig, EngineError> {
        self.config()
            .models
            .get(label)
            .cloned()
            .ok_or_else(|| EngineError::UnknownModel(label.to_string()))
    }

    fn model_timeout(&self, label: &str) -> Duration {
        let secs = self
            .config()
            .models
            .get(label)
            .map(|m| m.timeout)
            .unwrap_or(30);
        Duration::from_secs(secs as u64)
    }

    /// An item's scheduling priority: the maximum `priority` among the models it
    /// targets (default 0 when none are known).
    fn item_priority(&self, item: &Item) -> i32 {
        let config = self.config();
        item.models
            .iter()
            .filter_map(|m| config.models.get(m))
            .map(|m| m.priority)
            .max()
            .unwrap_or(0)
    }

    fn strategy_for(&self, model: &ModelConfig) -> Result<StrategyConfig, EngineError> {
        let name = model.video_strategy.as_deref().ok_or_else(|| {
            EngineError::Config("video input requires the model to set video_strategy".into())
        })?;
        self.config()
            .strategies
            .get(name)
            .cloned()
            .ok_or_else(|| EngineError::Config(format!("unknown strategy '{name}'")))
    }
}

/// Whether a model already reached `Done` for this item (used to skip on resume).
fn model_done(item: &Item, label: &str) -> bool {
    matches!(
        item.results.get(label),
        Some(r) if matches!(r.state, ModelState::Done | ModelState::Skipped)
    )
}

/// Whether a model output trips a stop condition: any of `labels` scoring at or
/// above `threshold` (checked on the aggregated result for a video scan).
fn output_triggers(out: &ModelOutput, labels: &[u32], threshold: f32) -> bool {
    let class = match out {
        ModelOutput::Classification(c) => c,
        ModelOutput::Scan(scan) => &scan.aggregated,
    };
    apollo_media::triggered(class, labels, threshold)
}

/// Backoff before the Nth retry: 1s, 2s, 4s, 8s, … capped at 30s.
fn retry_backoff(attempt: u32) -> Duration {
    let secs = 1u64
        .checked_shl(attempt.saturating_sub(1))
        .unwrap_or(u64::MAX);
    Duration::from_secs(secs.min(30))
}

/// The fetchable URL of an input, if any (used as the cache's url key).
fn item_url(input: &Input) -> Option<&Url> {
    match input {
        Input::Image(u) | Input::Video(u) | Input::Audio(u) => Some(u),
        Input::Text(_) | Input::Bytes { .. } => None,
    }
}

/// Hex-encoded SHA-256 of a byte slice.
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    hex_encode(&digest)
}

/// Hex-encoded SHA-256 of a file, read in chunks so large videos are not loaded
/// into memory at once. Blocking I/O — call from `spawn_blocking`.
fn sha256_file(path: &std::path::Path) -> std::io::Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let digest = hasher.finalize();
    Ok(hex_encode(&digest))
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// How a frame scan treats triggers: the model's trigger labels and threshold
/// (`[models.<l>.early_exit]`), whether the strategy stops on one, and its
/// confirmation rule.
struct Trigger {
    labels: Vec<u32>,
    threshold: f32,
    /// Stop the scan on a (confirmed) trigger: the strategy's `early_exit`.
    stop: bool,
    rule: Option<Confirm>,
}

impl Trigger {
    /// `None` when the model defines no trigger labels.
    fn for_scan(strategy: &StrategyConfig, model: &ModelConfig) -> Option<Self> {
        match &model.early_exit {
            Some(EarlyExit { labels, threshold }) if !labels.is_empty() => Some(Self {
                labels: labels.clone(),
                threshold: *threshold,
                stop: strategy.early_exit,
                rule: Confirm::from_strategy(strategy),
            }),
            _ => None,
        }
    }

    /// The frame's highest trigger-label score.
    fn score(&self, frame: &Sample) -> f32 {
        apollo_media::trigger_score(&frame.classification, &self.labels)
    }

    /// Whether the frame reaches the threshold on its own.
    fn reached(&self, frame: &Sample) -> bool {
        apollo_media::triggered(&frame.classification, &self.labels, self.threshold)
    }

    /// Whether the frame needs confirming: it reaches the threshold and was not
    /// itself sampled to confirm another frame.
    fn candidate(&self, frame: &Sample) -> bool {
        self.reached(frame) && !apollo_media::confirm::is_confirmation_frame(frame.index)
    }

    fn flagging(&self) -> aggregate::Flagging<'_> {
        aggregate::Flagging {
            labels: &self.labels,
            threshold: self.threshold,
            rule: self.rule,
        }
    }
}

fn frame_ref(frame: &Sample) -> FrameRef {
    FrameRef {
        index: frame.index,
        timestamp: frame.start,
        keyframe: false,
    }
}

fn frame_requests(refs: &[FrameRef]) -> Vec<FrameRequest> {
    refs.iter()
        .map(|f| FrameRequest {
            timestamp: f.timestamp,
            keyframe: f.keyframe,
        })
        .collect()
}

/// Bound a single classification future by `timeout`; an elapse becomes a
/// `Timeout` error carrying `on_timeout`.
async fn classify_with_timeout(
    timeout: Duration,
    fut: impl std::future::Future<Output = Result<Classification, EngineError>>,
    on_timeout: &str,
) -> Result<Classification, EngineError> {
    match tokio::time::timeout(timeout, fut).await {
        Ok(result) => result,
        Err(_) => Err(EngineError::Timeout(on_timeout.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn photo(width: u32, height: u32) -> DecodedImage {
        DecodedImage {
            width,
            height,
            data: (0..width * height * 3).map(|i| (i % 251) as u8).collect(),
        }
    }

    fn shape(size: u32, resize_ahead: bool) -> InputShape {
        InputShape { size, resize_ahead }
    }

    #[tokio::test]
    async fn models_that_resize_get_one_shared_copy_per_size() {
        let still = StillImage::new(photo(90, 60));
        let (a, b) = tokio::join!(
            still.input_for(shape(16, true)),
            still.input_for(shape(16, true))
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert!(Arc::ptr_eq(&a, &b), "concurrent models share one resize");
        assert_eq!((a.width, a.height), (16, 16));
        let expected = apollo_inference::resize_to_input(&still.full, 16).unwrap();
        assert_eq!(a.data, expected.data);

        let again = still.input_for(shape(16, true)).await.unwrap();
        assert!(Arc::ptr_eq(&a, &again), "later steps reuse it");
        let other = still.input_for(shape(8, true)).await.unwrap();
        assert_eq!((other.width, other.height), (8, 8));
    }

    #[tokio::test]
    async fn the_full_image_when_resizing_is_not_wanted_or_needed() {
        let still = StillImage::new(photo(90, 60));
        // A model that reads the full image (pdq).
        let full = still.input_for(shape(512, false)).await.unwrap();
        assert!(Arc::ptr_eq(&full, &still.full));

        // Already the model's size: nothing to resize.
        let square = StillImage::new(photo(16, 16));
        let same = square.input_for(shape(16, true)).await.unwrap();
        assert!(Arc::ptr_eq(&same, &square.full));
    }
}
