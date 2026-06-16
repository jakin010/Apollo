//! Per-task dispatch: fetch + decode each input once, fan out to model workers,
//! and gate total in-flight work via the global semaphore.
//!
//! Resume-aware throughout: a model already `Done` from a previous run is skipped,
//! and an interrupted video scan continues from its persisted frame checkpoint.

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use apollo_config::{EarlyExit, ModelConfig, SamplingKind, SamplingStep, StrategyConfig};
use apollo_domain::{
    DecodedImage, Frame, Input, Item, ItemState, ModelKind, ModelOutput, ModelResult, ModelState,
    Modality,
};
use apollo_media::{FrameRef, LocalMedia, VideoInfo};

use crate::aggregate;
use crate::error::{media_err, EngineError};
use crate::worker::ModelHandle;
use crate::Engine;

/// A fetched, decoded/probed input ready for inference.
enum Fetched {
    Image(DecodedImage),
    Video { media: LocalMedia, info: VideoInfo },
}

impl Engine {
    /// Process one item: skip if already terminal, fetch the input once, run every
    /// requested model (resumed models skipped), then mark the item terminal and
    /// fire its webhook. Owns `self` so it can be spawned per item.
    pub(crate) async fn run_item(self, task_id: String, idx: usize, item: Item) {
        // recover() already re-fired webhooks for terminal items.
        if aggregate::item_terminal(item.state) {
            return;
        }
        // Resumed item whose models all finished but never got marked complete
        // (crash between the last result and the item transition): finalize without
        // re-fetching or re-running anything.
        if !item.models.is_empty() && item.models.iter().all(|m| model_done(&item, m)) {
            let _ = self
                .inner
                .storage
                .set_item_state(&task_id, idx, ItemState::Completed, None)
                .await;
            self.deliver_webhook(&task_id, idx).await;
            return;
        }

        // Bound concurrent items globally (a coarse VRAM/throughput cap; the real
        // GPU batching happens inside each model worker).
        let _permit = match self.inner.global.acquire().await {
            Ok(p) => p,
            Err(_) => return, // semaphore closed -> shutting down
        };

        if let Err(e) = self
            .inner
            .storage
            .set_item_state(&task_id, idx, ItemState::Processing, None)
            .await
        {
            tracing::error!(task = %task_id, item = idx, error = %e, "set item processing");
        }

        tracing::debug!(task = %task_id, item = idx, models = item.models.len(), "processing item");
        let fetched = match self.fetch_item(&item.input).await {
            Ok(f) => f,
            Err(e) => {
                self.fail_item(&task_id, idx, &item, &e.to_string()).await;
                return;
            }
        };

        for label in &item.models {
            if model_done(&item, label) {
                continue;
            }
            let _ = self
                .inner
                .storage
                .upsert_model_result(&task_id, idx, label, &ModelResult::processing())
                .await;

            tracing::debug!(task = %task_id, item = idx, model = %label, "running model");
            // model.timeout bounds processing only (queue wait and the one-time
            // fetch are excluded).
            let timeout = self.model_timeout(label);
            let outcome = tokio::time::timeout(
                timeout,
                self.run_model(&task_id, idx, label, &item, &fetched),
            )
            .await;
            let result = match outcome {
                Ok(Ok(output)) => ModelResult::done(output),
                Ok(Err(e)) => ModelResult::failed(e.to_string()),
                Err(_) => ModelResult::failed(format!("model '{label}' timed out")),
            };
            let _ = self
                .inner
                .storage
                .upsert_model_result(&task_id, idx, label, &result)
                .await;
        }

        if let Err(e) = self
            .inner
            .storage
            .set_item_state(&task_id, idx, ItemState::Completed, None)
            .await
        {
            tracing::error!(task = %task_id, item = idx, error = %e, "set item completed");
        }
        self.deliver_webhook(&task_id, idx).await;
    }

    /// Mark the item (and its not-yet-done models) failed, then fire the webhook.
    /// Preserves results from models that already finished on a prior run.
    async fn fail_item(&self, task_id: &str, idx: usize, item: &Item, msg: &str) {
        for label in &item.models {
            if model_done(item, label) {
                continue;
            }
            let _ = self
                .inner
                .storage
                .upsert_model_result(task_id, idx, label, &ModelResult::failed(msg))
                .await;
        }
        let _ = self
            .inner
            .storage
            .set_item_state(task_id, idx, ItemState::Failed, Some(msg))
            .await;
        self.deliver_webhook(task_id, idx).await;
    }

    /// Run a single model against the already-fetched input, choosing the path by
    /// (input modality, model kind).
    async fn run_model(
        &self,
        task_id: &str,
        idx: usize,
        label: &str,
        item: &Item,
        fetched: &Fetched,
    ) -> Result<ModelOutput, EngineError> {
        let handle = self
            .inner
            .registry
            .get(label)
            .ok_or_else(|| EngineError::UnknownModel(label.to_string()))?;

        match (item.input.modality(), fetched) {
            (Modality::Image, Fetched::Image(img)) => {
                let c = handle.classify_image(img.clone()).await?;
                Ok(ModelOutput::Classification(c))
            }
            (Modality::Video, Fetched::Video { media, info }) => match handle.kind {
                ModelKind::ImageClassifier => {
                    self.run_frame_scan(task_id, idx, label, &handle, media.path(), info)
                        .await
                }
                ModelKind::VideoClassifier => self.run_whole_clip(&handle, media.path(), info).await,
            },
            _ => Err(EngineError::Incompatible(format!(
                "input/model mismatch for '{label}'"
            ))),
        }
    }

    /// Fetch the input once: download (with fallback), then decode an image or
    /// probe a video.
    async fn fetch_item(&self, input: &Input) -> Result<Fetched, EngineError> {
        match input {
            Input::Image(url) => {
                let media = apollo_media::fetch(url).await.map_err(media_err)?;
                let bytes = media.read_bytes().map_err(media_err)?;
                let img = tokio::task::spawn_blocking(move || apollo_media::decode_image(&bytes))
                    .await
                    .map_err(|e| EngineError::Join(e.to_string()))?
                    .map_err(media_err)?;
                Ok(Fetched::Image(img))
            }
            Input::Video(url) => {
                let media = apollo_media::fetch(url).await.map_err(media_err)?;
                let info = apollo_media::probe(media.path()).await.map_err(media_err)?;
                Ok(Fetched::Video { media, info })
            }
            Input::Text(_) | Input::Audio(_) => Err(EngineError::Incompatible(
                "text/audio inputs are not supported yet".into(),
            )),
        }
    }

    /// Image-classifier over a video: plan frames per the model's strategy, skip
    /// frames already classified (resume), classify the rest in worker-batched
    /// chunks (persisting each as a checkpoint), and early-exit when a trigger
    /// fires. Rolls up into a `FrameScan`.
    async fn run_frame_scan(
        &self,
        task_id: &str,
        idx: usize,
        label: &str,
        handle: &ModelHandle,
        path: &Path,
        info: &VideoInfo,
    ) -> Result<ModelOutput, EngineError> {
        let model = self.model_cfg(label)?;
        let strategy = self.strategy_for(&model)?;

        let plan = apollo_media::plan(path, info, &strategy.sampling)
            .await
            .map_err(media_err)?;

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
            let timestamps: Vec<f64> = chunk.iter().map(|f| f.timestamp).collect();
            let images = apollo_media::extract_frames(path, &timestamps)
                .await
                .map_err(media_err)?;

            // Issue the whole chunk concurrently so the worker merges it into one
            // forward pass.
            let calls = images.into_iter().map(|img| handle.classify_image(img));
            let results = futures::future::join_all(calls).await;

            for (fref, res) in chunk.iter().zip(results.into_iter()) {
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
                if let Some((labels, threshold)) = early.as_ref() {
                    if apollo_media::triggered(&frame.classification, labels, *threshold) {
                        stop = true;
                    }
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

    /// Whole-clip video-classifier: sample exactly `clip_len` frames uniformly and
    /// run one forward pass.
    async fn run_whole_clip(
        &self,
        handle: &ModelHandle,
        path: &Path,
        info: &VideoInfo,
    ) -> Result<ModelOutput, EngineError> {
        let clip_len = handle.clip_len().await?;
        let step = SamplingStep {
            step: 0,
            method: SamplingKind::Uniform,
            fps: None,
            count: Some(clip_len as u32),
            nth: None,
            threshold: None,
        };
        let plan = apollo_media::plan(path, info, &[step])
            .await
            .map_err(media_err)?;
        let timestamps: Vec<f64> = plan.iter().map(|f| f.timestamp).collect();
        let frames = apollo_media::extract_frames(path, &timestamps)
            .await
            .map_err(media_err)?;
        let c = handle.classify_clip(frames).await?;
        Ok(ModelOutput::Classification(c))
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
    matches!(item.results.get(label), Some(r) if matches!(r.state, ModelState::Done))
}

/// `(labels, threshold)` when both the strategy enables early-exit and the model
/// defines trigger labels; otherwise `None` (early-exit needs both).
fn early_exit_for(strategy: &StrategyConfig, model: &ModelConfig) -> Option<(Vec<String>, f32)> {
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
