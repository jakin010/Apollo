//! Per-task dispatch: fetch + decode each input once, fan out to model workers,
//! and gate total in-flight work via the global semaphore.
//!
//! Resume-aware throughout: a model already `Done` from a previous run is skipped,
//! and an interrupted video scan continues from its persisted frame checkpoint.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use apollo_config::{EarlyExit, ModelConfig, PipelineStep, StrategyConfig};
use apollo_domain::{
    Classification, DecodedImage, Frame, Input, Item, ItemState, Modality, ModelOutput,
    ModelResult, ModelState, TaskError, Url,
};
use apollo_media::{FrameRef, LocalMedia, VideoInfo};

use crate::Engine;
use crate::aggregate;
use crate::error::EngineError;
use crate::worker::ModelHandle;

/// A fetched, decoded/probed input ready for inference.
enum Fetched {
    Image(DecodedImage),
    Video { media: LocalMedia, info: VideoInfo },
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
        let priority = self.item_priority(&item);
        let _permit = self.inner.gate.clone().acquire(priority).await;

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
        // Fetch with retries: a failed attempt (e.g. an unreachable URL) puts the
        // item into `Retrying` — reported as such on the webhook — and tries again,
        // up to `[app].max_retries`, before failing it permanently.
        let max_retries = self.inner.config.app.max_retries;
        let (fetched, content_hash) = loop {
            match self.fetch_item(&item.input).await {
                Ok(f) => break f,
                Err(e) if item.retries < max_retries => {
                    item.retries += 1;
                    let _ = self
                        .inner
                        .storage
                        .set_item_retries(&task_id, idx, item.retries)
                        .await;
                    let _ = self
                        .inner
                        .storage
                        .set_item_state(
                            &task_id,
                            idx,
                            ItemState::Retrying,
                            Some(&TaskError::fetch(e.to_string())),
                        )
                        .await;
                    tracing::warn!(
                        task = %task_id, item = idx, attempt = item.retries, max = max_retries,
                        error = %e, "item attempt failed; will retry"
                    );
                    self.notify_item_change(&task_id, idx).await;
                    tokio::time::sleep(retry_backoff(item.retries)).await;
                    if cancel.load(Ordering::SeqCst) {
                        self.cancel_item(&task_id, idx).await;
                        return;
                    }
                }
                Err(e) => {
                    // Retries exhausted (or disabled): permanent, dead-lettered failure.
                    self.fail_item(&task_id, idx, &item, TaskError::fetch(e.to_string()))
                        .await;
                    return;
                }
            }
        };
        // A retried item was left in `Retrying`; reflect that it is processing again
        // (persisted for GetTask; the terminal webhook fires at the end).
        if item.retries > 0 {
            let _ = self
                .inner
                .storage
                .set_item_state(&task_id, idx, ItemState::Processing, None)
                .await;
        }

        let fresh_after = self.cache_fresh_after();
        let url_hash = item_url(&item.input).map(|u| sha256_hex(u.main.as_bytes()));

        // Pipeline items run as an ordered, gated sequence with strict failure
        // semantics; the input was fetched once above and is reused per step.
        if let Some(name) = item.pipeline.clone() {
            self.run_pipeline(
                &task_id,
                idx,
                &item,
                &name,
                &fetched,
                content_hash.as_deref(),
                url_hash.as_deref(),
                fresh_after,
                &cancel,
            )
            .await;
            return;
        }

        for label in &item.models {
            // Cancellation checkpoint between models.
            if cancel.load(Ordering::SeqCst) {
                self.cancel_item(&task_id, idx).await;
                return;
            }
            if model_done(&item, label) {
                continue;
            }

            // Content-cache fast path: identical bytes already classified by this
            // model + revision -> reuse the stored output, skipping inference.
            if let Some(ch) = content_hash.as_deref() {
                let rev = self.model_revision(label);
                match self
                    .inner
                    .storage
                    .cache_lookup(ch, label, &rev, fresh_after)
                    .await
                {
                    Ok(Some(output)) => {
                        let _ = self
                            .inner
                            .storage
                            .upsert_model_result(&task_id, idx, label, &ModelResult::done(output))
                            .await;
                        if let Some(uh) = url_hash.as_deref() {
                            let _ = self
                                .inner
                                .storage
                                .url_cache_store(uh, label, &rev, ch)
                                .await;
                        }
                        tracing::debug!(task = %task_id, item = idx, model = %label, "cache hit");
                        continue;
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
                .upsert_model_result(&task_id, idx, label, &ModelResult::processing())
                .await;

            tracing::debug!(task = %task_id, item = idx, model = %label, "running model");
            // The model's `timeout` bounds each individual classification — a single
            // image, or *each frame* of a video scan — not the whole video. run_model
            // applies it internally per classification.
            let result = match self
                .run_model(&task_id, idx, label, &item, &fetched, &cancel)
                .await
            {
                Ok(output) => {
                    // Populate the cache on success (best-effort).
                    if let Some(ch) = content_hash.as_deref() {
                        let rev = self.model_revision(label);
                        if let Err(e) = self
                            .inner
                            .storage
                            .cache_store(ch, label, &rev, &output)
                            .await
                        {
                            tracing::warn!(task = %task_id, model = %label, error = %e, "cache store failed");
                        }
                        if let Some(uh) = url_hash.as_deref() {
                            let _ = self
                                .inner
                                .storage
                                .url_cache_store(uh, label, &rev, ch)
                                .await;
                        }
                    }
                    ModelResult::done(output)
                }
                Err(EngineError::Cancelled) => {
                    self.cancel_item(&task_id, idx).await;
                    return;
                }
                Err(e) => ModelResult::failed(TaskError::inference(e.to_string())),
            };
            let _ = self
                .inner
                .storage
                .upsert_model_result(&task_id, idx, label, &result)
                .await;
        }

        self.complete_item(&task_id, idx).await;
    }

    /// Run an item through a named pipeline: its steps in `order`, each optionally
    /// gated by `stop_if`. A gate firing skips the remaining steps and completes
    /// the task normally (task webhook). A step *failure* fails the whole pipeline:
    /// it is retried up to `[app].max_retries` (completed steps resume), then
    /// dead-lettered. The input was fetched once by the caller and is reused.
    #[allow(clippy::too_many_arguments)]
    async fn run_pipeline(
        &self,
        task_id: &str,
        idx: usize,
        item: &Item,
        pipeline: &str,
        fetched: &Fetched,
        content_hash: Option<&str>,
        url_hash: Option<&str>,
        fresh_after: i64,
        cancel: &AtomicBool,
    ) {
        let steps = match self.pipeline_steps(pipeline) {
            Some(s) => s,
            None => {
                self.fail_item(
                    task_id,
                    idx,
                    item,
                    TaskError::internal(format!("unknown pipeline '{pipeline}'")),
                )
                .await;
                return;
            }
        };
        let max_retries = self.inner.config.app.max_retries;
        let mut item = item.clone();

        let stopped_at = 'attempt: loop {
            let mut stop_idx: Option<usize> = None;
            for (i, step) in steps.iter().enumerate() {
                if cancel.load(Ordering::SeqCst) {
                    self.cancel_item(task_id, idx).await;
                    return;
                }

                // Already terminal (resume or a prior attempt): don't re-run, but
                // still honour a completed step's gate so later steps stay skipped.
                if let Some(r) = item.results.get(&step.model) {
                    match r.state {
                        ModelState::Done => {
                            if let (Some(cond), Some(out)) = (&step.stop_if, &r.output)
                                && output_triggers(out, cond)
                            {
                                stop_idx = Some(i);
                                break;
                            }
                            continue;
                        }
                        ModelState::Skipped => continue,
                        _ => {}
                    }
                }

                // Content-cache fast path, mirroring the parallel loop.
                if let Some(ch) = content_hash {
                    let rev = self.model_revision(&step.model);
                    if let Ok(Some(output)) = self
                        .inner
                        .storage
                        .cache_lookup(ch, &step.model, &rev, fresh_after)
                        .await
                    {
                        let done = ModelResult::done(output.clone());
                        let _ = self
                            .inner
                            .storage
                            .upsert_model_result(task_id, idx, &step.model, &done)
                            .await;
                        item.results.insert(step.model.clone(), done);
                        if let Some(uh) = url_hash {
                            let _ = self
                                .inner
                                .storage
                                .url_cache_store(uh, &step.model, &rev, ch)
                                .await;
                        }
                        if let Some(cond) = &step.stop_if
                            && output_triggers(&output, cond)
                        {
                            stop_idx = Some(i);
                            break;
                        }
                        continue;
                    }
                }

                let _ = self
                    .inner
                    .storage
                    .upsert_model_result(task_id, idx, &step.model, &ModelResult::processing())
                    .await;

                match self
                    .run_model(task_id, idx, &step.model, &item, fetched, cancel)
                    .await
                {
                    Ok(output) => {
                        if let Some(ch) = content_hash {
                            let rev = self.model_revision(&step.model);
                            if let Err(e) = self
                                .inner
                                .storage
                                .cache_store(ch, &step.model, &rev, &output)
                                .await
                            {
                                tracing::warn!(task = %task_id, model = %step.model, error = %e, "cache store failed");
                            }
                            if let Some(uh) = url_hash {
                                let _ = self
                                    .inner
                                    .storage
                                    .url_cache_store(uh, &step.model, &rev, ch)
                                    .await;
                            }
                        }
                        let done = ModelResult::done(output.clone());
                        let _ = self
                            .inner
                            .storage
                            .upsert_model_result(task_id, idx, &step.model, &done)
                            .await;
                        item.results.insert(step.model.clone(), done);
                        if let Some(cond) = &step.stop_if
                            && output_triggers(&output, cond)
                        {
                            stop_idx = Some(i);
                            break;
                        }
                    }
                    Err(EngineError::Cancelled) => {
                        self.cancel_item(task_id, idx).await;
                        return;
                    }
                    Err(e) => {
                        // Strict: a step failure fails the whole pipeline. Retry the
                        // attempt (completed steps resume) up to the limit, then
                        // dead-letter via fail_item.
                        if item.retries < max_retries {
                            item.retries += 1;
                            let _ = self
                                .inner
                                .storage
                                .set_item_retries(task_id, idx, item.retries)
                                .await;
                            self.notify_item_change(task_id, idx).await;
                            tokio::time::sleep(retry_backoff(item.retries)).await;
                            continue 'attempt;
                        }
                        let _ = self
                            .inner
                            .storage
                            .upsert_model_result(
                                task_id,
                                idx,
                                &step.model,
                                &ModelResult::failed(TaskError::inference(e.to_string())),
                            )
                            .await;
                        self.fail_item(task_id, idx, &item, TaskError::inference(e.to_string()))
                            .await;
                        return;
                    }
                }
            }
            break 'attempt stop_idx;
        };

        // A gate fired: mark every later step Skipped.
        if let Some(i) = stopped_at {
            for step in steps.iter().skip(i + 1) {
                let already_done = matches!(
                    item.results.get(&step.model).map(|r| &r.state),
                    Some(ModelState::Done)
                );
                if !already_done {
                    let _ = self
                        .inner
                        .storage
                        .upsert_model_result(task_id, idx, &step.model, &ModelResult::skipped())
                        .await;
                }
            }
        }

        self.complete_item(task_id, idx).await;
    }

    /// The steps of a named pipeline, sorted by `order`. `None` if undefined.
    fn pipeline_steps(&self, name: &str) -> Option<Vec<PipelineStep>> {
        let p = self.inner.config.pipelines.get(name)?;
        let mut steps = p.steps.clone();
        steps.sort_by_key(|s| s.order);
        Some(steps)
    }

    /// Whether the result cache is configured and enabled.
    fn cache_enabled(&self) -> bool {
        self.inner.config.cache.as_ref().is_some_and(|c| c.enabled)
    }

    /// Lower bound (unix secs) on a cache entry's `created_at` for it to count as
    /// fresh: `now - ttl`, or `0` (no cutoff) when no TTL is configured.
    fn cache_fresh_after(&self) -> i64 {
        match self.inner.config.cache.as_ref().and_then(|c| c.ttl_secs) {
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
        self.inner
            .config
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
        self.rollup_task_state(task_id).await;
        self.deliver_webhook(task_id, idx).await;
    }

    /// Mark the item (and its not-yet-done models) failed, then fire the webhook.
    /// Preserves results from models that already finished on a prior run.
    async fn fail_item(&self, task_id: &str, idx: usize, item: &Item, err: TaskError) {
        for label in &item.models {
            if model_done(item, label) {
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
        self.rollup_task_state(task_id).await;
        self.deliver_webhook(task_id, idx).await;
    }

    /// Recompute the task's lifecycle state from its items and persist it when it
    /// changed. Called as each item reaches a terminal state, so the task flips to
    /// `Completed` exactly when its last item does (and the webhook payload then
    /// carries the correct task state).
    pub async fn rollup_task_state(&self, task_id: &str) {
        let task = match self.inner.storage.get_task(task_id).await {
            Ok(Some(t)) => t,
            _ => return,
        };
        // A terminal state is final — never roll a Cancelled/Failed/Completed task
        // back to Processing or Completed.
        if aggregate::task_terminal(task.state) {
            return;
        }
        let desired = aggregate::task_state_for(&task);
        if desired != task.state
            && let Err(e) = self.inner.storage.set_task_state(task_id, desired).await
        {
            tracing::error!(task = %task_id, error = %e, "failed to roll up task state");
        }
        // Once a task is terminal, remove any staged upload files. They persist
        // across restarts (so resume can re-read them) and are cleaned exactly
        // here, when the task is finished for good.
        if aggregate::task_terminal(desired) {
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
            .inner
            .registry
            .get(label)
            .ok_or_else(|| EngineError::UnknownModel(label.to_string()))?;

        let timeout = self.model_timeout(label);
        match (item.input.modality(), fetched) {
            (Modality::Image, Fetched::Image(img)) => {
                // One image: the whole classification is bounded by `timeout`.
                let on_timeout = format!("model '{label}' timed out classifying image");
                let c =
                    classify_with_timeout(timeout, handle.classify_image(img.clone()), &on_timeout)
                        .await?;
                Ok(ModelOutput::Classification(c))
            }
            (Modality::Video, Fetched::Video { media, info }) => {
                // Video: an image classifier over sampled frames, each frame bounded
                // by `timeout` (applied inside run_frame_scan).
                self.run_frame_scan(
                    task_id,
                    idx,
                    label,
                    &handle,
                    media.path(),
                    info,
                    timeout,
                    cancel,
                )
                .await
            }
            _ => Err(EngineError::Incompatible(format!(
                "input/model mismatch for '{label}'"
            ))),
        }
    }

    /// Fetch the input once: download (with fallback), then decode an image or
    /// probe a video.
    async fn fetch_item(&self, input: &Input) -> Result<(Fetched, Option<String>), EngineError> {
        let want_hash = self.cache_enabled();
        match input {
            Input::Image(url) => {
                let media = apollo_media::fetch(url, &self.inner.fetch_limits).await?;
                self.image_from_media(media, want_hash).await
            }
            Input::Video(url) => {
                let media = apollo_media::fetch(url, &self.inner.fetch_limits).await?;
                self.video_from_media(media, want_hash).await
            }
            Input::Bytes { path, video } => {
                let media = LocalMedia::adopt(path.clone());
                if *video {
                    self.video_from_media(media, want_hash).await
                } else {
                    self.image_from_media(media, want_hash).await
                }
            }
            Input::Text(_) | Input::Audio(_) => Err(EngineError::Incompatible(
                "text/audio inputs are not supported yet".into(),
            )),
        }
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
        let max_pixels = self.inner.max_pixels;
        let img =
            tokio::task::spawn_blocking(move || apollo_media::decode_image(&bytes, max_pixels))
                .await
                .map_err(|e| EngineError::Join(e.to_string()))??;
        Ok((Fetched::Image(img), hash))
    }

    /// Probe `media` as a video, hashing the file for the cache key when
    /// `want_hash` is set. A hash failure degrades to "no caching", never an error.
    async fn video_from_media(
        &self,
        media: LocalMedia,
        want_hash: bool,
    ) -> Result<(Fetched, Option<String>), EngineError> {
        let info = apollo_media::probe(media.path()).await?;
        let max = self.inner.config.limits.max_video_seconds;
        if max > 0 && info.duration > max as f64 {
            return Err(EngineError::Incompatible(format!(
                "video is {:.0}s, over the {max}s limit",
                info.duration
            )));
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
        Ok((Fetched::Video { media, info }, hash))
    }

    /// Image-classifier over a video: plan frames per the model's strategy, skip
    /// frames already classified (resume), classify the rest in worker-batched
    /// chunks (persisting each as a checkpoint), and early-exit when a trigger
    /// fires. Rolls up into a `FrameScan`.
    #[allow(clippy::too_many_arguments)]
    async fn run_frame_scan(
        &self,
        task_id: &str,
        idx: usize,
        label: &str,
        handle: &ModelHandle,
        path: &Path,
        info: &VideoInfo,
        timeout: Duration,
        cancel: &AtomicBool,
    ) -> Result<ModelOutput, EngineError> {
        let model = self.model_cfg(label)?;
        let strategy = self.strategy_for(&model)?;
        let frame_timeout_msg = format!("model '{label}' timed out classifying a video frame");

        let plan = apollo_media::plan(path, info, &strategy.sampling).await?;

        // Seed from frames already persisted on a previous run.
        let prior = self
            .inner
            .storage
            .load_frames(task_id, idx, label)
            .await
            .unwrap_or_default();
        let done_idx: HashSet<u32> = prior.iter().map(|f| f.index).collect();
        let mut frames = prior;

        let early = early_exit_for(&strategy, &model);
        let batch = model.max_concurrent.max(1) as usize;
        let pending: Vec<FrameRef> = plan
            .into_iter()
            .filter(|f| !done_idx.contains(&f.index))
            .collect();

        tracing::debug!(
            model = %label,
            planned = pending.len() + done_idx.len(),
            resuming_from = done_idx.len(),
            "frame scan planned"
        );
        let mut stop = false;
        'scan: for chunk in pending.chunks(batch) {
            // Cancellation checkpoint between sampled-frame batches.
            if cancel.load(Ordering::SeqCst) {
                return Err(EngineError::Cancelled);
            }
            let timestamps: Vec<f64> = chunk.iter().map(|f| f.timestamp).collect();
            let images =
                apollo_media::extract_frames(path, &timestamps, self.inner.max_pixels).await?;

            // Issue the whole chunk concurrently so the worker merges it into one
            // forward pass; each frame is bounded by the model's per-frame timeout.
            let calls = images.into_iter().map(|img| {
                classify_with_timeout(timeout, handle.classify_image(img), &frame_timeout_msg)
            });
            let results = futures::future::join_all(calls).await;

            for (fref, res) in chunk.iter().zip(results) {
                let classification = res?;
                let frame = Frame {
                    timestamp: fref.timestamp,
                    index: fref.index,
                    classification,
                };
                let _ = self
                    .inner
                    .storage
                    .append_frame(task_id, idx, label, &frame)
                    .await;
                if let Some((labels, threshold)) = early.as_ref()
                    && apollo_media::triggered(&frame.classification, labels, *threshold)
                {
                    stop = true;
                }
                frames.push(frame);
                if stop {
                    break 'scan;
                }
            }
        }

        Ok(ModelOutput::FrameScan(aggregate::frame_scan(
            frames,
            strategy.aggregation,
        )))
    }

    // ------------------------- small config lookups -------------------------

    fn model_cfg(&self, label: &str) -> Result<ModelConfig, EngineError> {
        self.inner
            .config
            .models
            .get(label)
            .cloned()
            .ok_or_else(|| EngineError::UnknownModel(label.to_string()))
    }

    fn model_timeout(&self, label: &str) -> Duration {
        let secs = self
            .inner
            .config
            .models
            .get(label)
            .map(|m| m.timeout)
            .unwrap_or(30);
        Duration::from_secs(secs as u64)
    }

    /// An item's scheduling priority: the maximum `priority` among the models it
    /// targets (default 0 when none are known).
    fn item_priority(&self, item: &Item) -> i32 {
        item.models
            .iter()
            .filter_map(|m| self.inner.config.models.get(m))
            .map(|m| m.priority)
            .max()
            .unwrap_or(0)
    }

    fn strategy_for(&self, model: &ModelConfig) -> Result<StrategyConfig, EngineError> {
        let name = model.video_strategy.as_deref().ok_or_else(|| {
            EngineError::Config("video input requires the model to set video_strategy".into())
        })?;
        self.inner
            .config
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

/// Whether a model output trips a stop condition: any listed id scoring at or
/// above the threshold (checked on the aggregated result for a video frame scan).
fn output_triggers(out: &ModelOutput, cond: &EarlyExit) -> bool {
    let class = match out {
        ModelOutput::Classification(c) => c,
        ModelOutput::FrameScan(fs) => &fs.aggregated,
    };
    class
        .predictions
        .iter()
        .any(|p| p.score >= cond.threshold && cond.labels.contains(&p.label))
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

/// `(labels, threshold)` when both the strategy enables early-exit and the model
/// defines trigger labels; otherwise `None` (early-exit needs both).
fn early_exit_for(strategy: &StrategyConfig, model: &ModelConfig) -> Option<(Vec<u32>, f32)> {
    if !strategy.early_exit {
        return None;
    }
    match &model.early_exit {
        Some(EarlyExit { labels, threshold }) if !labels.is_empty() => {
            Some((labels.clone(), *threshold))
        }
        _ => None,
    }
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
