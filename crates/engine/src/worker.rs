//! Per-model batching worker.
//!
//! Each enabled model gets one dedicated OS thread that owns the model's resident
//! weights. The async control plane submits image-classification [`Job`]s over a
//! channel; the thread loads the model lazily on first use, serves requests with
//! no-wait-window dynamic batching (pending images are merged up to the model's
//! batch size into a single forward pass), and unloads the model after an idle
//! period unless the model sets `keep_in_memory`.
//!
//! Video inputs are handled one frame at a time through this same image path
//! (the scheduler samples frames and submits them as ordinary image jobs); there
//! is no separate whole-clip path.

use std::path::PathBuf;
use std::time::Duration;

use candle_core::Device;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use tokio::sync::oneshot;

use apollo_config::ModelConfig;
use apollo_domain::{Classification, DecodedImage};
use apollo_inference::ImageClassifier;

use crate::error::EngineError;

type Reply = oneshot::Sender<Result<Classification, EngineError>>;

/// A unit of inference work: one image, merged with other pending images into a
/// single batched forward pass by the worker.
pub(crate) struct Job {
    image: DecodedImage,
    reply: Reply,
}

/// Async-side handle to a model worker.
pub(crate) struct ModelHandle {
    tx: Sender<Job>,
}

impl ModelHandle {
    pub(crate) async fn classify_image(
        &self,
        image: DecodedImage,
    ) -> Result<Classification, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Job { image, reply })
            .map_err(|_| EngineError::WorkerGone)?;
        rx.await.map_err(|_| EngineError::WorkerGone)?
    }
}

/// Spawn the worker thread for `cfg` and return its handle.
pub(crate) fn spawn_worker(
    cfg: ModelConfig,
    device: Device,
    cache_dir: Option<PathBuf>,
    idle: Duration,
) -> ModelHandle {
    let (tx, rx) = crossbeam_channel::unbounded::<Job>();
    let batch = cfg.max_concurrent.max(1) as usize;
    let keep = cfg.keep_in_memory;
    let name = format!("apollo-model-{}", cfg.repo);
    std::thread::Builder::new()
        .name(name)
        .spawn(move || worker_loop(cfg, device, cache_dir, idle, batch, keep, rx))
        .expect("failed to spawn model worker thread");
    ModelHandle { tx }
}

fn worker_loop(
    cfg: ModelConfig,
    device: Device,
    cache_dir: Option<PathBuf>,
    idle: Duration,
    batch: usize,
    keep: bool,
    rx: Receiver<Job>,
) {
    let mut model: Option<Box<dyn ImageClassifier>> = None;
    loop {
        match rx.recv_timeout(idle) {
            Ok(job) => {
                if model.is_none() {
                    match apollo_inference::load(&cfg, &device, cache_dir.as_deref()) {
                        Ok(m) => {
                            tracing::info!(model = %cfg.repo, "model loaded");
                            model = Some(m);
                        }
                        Err(e) => {
                            let _ = job.reply.send(Err(EngineError::Inference(format!(
                                "model load failed: {e}"
                            ))));
                            continue;
                        }
                    }
                }
                run_batch(model.as_deref().unwrap(), job, &rx, batch);
            }
            // Idle: drop resident weights unless pinned.
            Err(RecvTimeoutError::Timeout) => {
                if !keep && model.take().is_some() {
                    tracing::debug!(model = %cfg.repo, "unloaded idle model");
                }
            }
            // All handles dropped (engine shutting down).
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}

/// Merge `first` plus whatever else is queued (up to `batch`) into one forward
/// pass, then fan the per-image results back to each caller.
fn run_batch(model: &dyn ImageClassifier, first: Job, rx: &Receiver<Job>, batch: usize) {
    let mut images = Vec::with_capacity(batch);
    let mut replies = Vec::with_capacity(batch);
    images.push(first.image);
    replies.push(first.reply);
    while images.len() < batch {
        match rx.try_recv() {
            Ok(job) => {
                images.push(job.image);
                replies.push(job.reply);
            }
            Err(_) => break,
        }
    }
    match model.classify(&images) {
        Ok(results) => {
            for (reply, c) in replies.into_iter().zip(results.into_iter()) {
                let _ = reply.send(Ok(c));
            }
        }
        Err(e) => {
            let msg = e.to_string();
            for reply in replies {
                let _ = reply.send(Err(EngineError::Inference(msg.clone())));
            }
        }
    }
}
