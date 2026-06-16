//! Per-model batching worker.
//!
//! Each enabled model gets one dedicated OS thread that owns the model's resident
//! weights. The async control plane submits [`Job`]s over a channel; the thread
//! loads the model lazily on first use, serves requests with no-wait-window
//! dynamic batching (image jobs are merged up to the model's batch size into a
//! single forward pass), and unloads the model after an idle period unless the
//! model sets `keep_in_memory`.

use std::path::PathBuf;
use std::time::Duration;

use candle_core::Device;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use tokio::sync::oneshot;

use apollo_config::ModelConfig;
use apollo_domain::{Classification, DecodedImage, ModelKind};
use apollo_inference::Loaded;

use crate::error::EngineError;

type Reply = oneshot::Sender<Result<Classification, EngineError>>;

/// A unit of inference work sent to a model worker.
pub(crate) enum Job {
    /// One image (batched with other pending images by the worker).
    Image { image: DecodedImage, reply: Reply },
    /// A whole clip of frames (one forward pass, not batched).
    Clip { frames: Vec<DecodedImage>, reply: Reply },
    /// Query the model's expected clip length (video classifiers only).
    ClipLen { reply: oneshot::Sender<Result<usize, EngineError>> },
}

/// Async-side handle to a model worker.
pub(crate) struct ModelHandle {
    tx: Sender<Job>,
    pub(crate) kind: ModelKind,
}

impl ModelHandle {
    pub(crate) async fn classify_image(&self, image: DecodedImage) -> Result<Classification, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Job::Image { image, reply })
            .map_err(|_| EngineError::WorkerGone)?;
        rx.await.map_err(|_| EngineError::WorkerGone)?
    }

    pub(crate) async fn classify_clip(&self, frames: Vec<DecodedImage>) -> Result<Classification, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Job::Clip { frames, reply })
            .map_err(|_| EngineError::WorkerGone)?;
        rx.await.map_err(|_| EngineError::WorkerGone)?
    }

    /// Number of frames a video classifier expects in a clip.
    pub(crate) async fn clip_len(&self) -> Result<usize, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Job::ClipLen { reply })
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
    let kind = cfg.architecture.kind();
    let batch = cfg.max_concurrent.max(1) as usize;
    let keep = cfg.keep_in_memory;
    let name = format!("apollo-model-{}", cfg.repo);
    std::thread::Builder::new()
        .name(name)
        .spawn(move || worker_loop(cfg, device, cache_dir, idle, batch, keep, rx))
        .expect("failed to spawn model worker thread");
    ModelHandle { tx, kind }
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
    let mut model: Option<Loaded> = None;
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
                            fail_job(job, &format!("model load failed: {e}"));
                            continue;
                        }
                    }
                }
                let loaded = model.as_ref().unwrap();
                match job {
                    Job::ClipLen { reply } => {
                        let _ = reply.send(clip_len_of(loaded));
                    }
                    other => run_batch(loaded, other, &rx, batch),
                }
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

fn clip_len_of(model: &Loaded) -> Result<usize, EngineError> {
    match model {
        Loaded::Video(clf) => Ok(clf.clip_len()),
        Loaded::Image(_) => Err(EngineError::Incompatible(
            "clip length requested from an image classifier".into(),
        )),
    }
}

fn run_batch(model: &Loaded, first: Job, rx: &Receiver<Job>, batch: usize) {
    match model {
        Loaded::Image(clf) => {
            let mut images = Vec::with_capacity(batch);
            let mut replies = Vec::with_capacity(batch);
            push_image(first, &mut images, &mut replies);
            // Greedily merge whatever else is queued, up to the batch size.
            while images.len() < batch {
                match rx.try_recv() {
                    Ok(job) => push_image(job, &mut images, &mut replies),
                    Err(_) => break,
                }
            }
            match clf.classify(&images) {
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
        Loaded::Video(clf) => match first {
            Job::Clip { frames, reply } => {
                let r = clf
                    .classify_clip(&frames)
                    .map_err(|e| EngineError::Inference(e.to_string()));
                let _ = reply.send(r);
            }
            Job::Image { reply, .. } => {
                let _ = reply.send(Err(EngineError::Inference(
                    "image job routed to a video classifier".into(),
                )));
            }
            Job::ClipLen { reply } => {
                let _ = reply.send(Ok(clf.clip_len()));
            }
        },
    }
}

fn push_image(job: Job, images: &mut Vec<DecodedImage>, replies: &mut Vec<Reply>) {
    match job {
        Job::Image { image, reply } => {
            images.push(image);
            replies.push(reply);
        }
        Job::Clip { reply, .. } => {
            let _ = reply.send(Err(EngineError::Inference(
                "clip job routed to an image classifier".into(),
            )));
        }
        Job::ClipLen { reply } => {
            let _ = reply.send(Err(EngineError::Incompatible(
                "clip length requested from an image classifier".into(),
            )));
        }
    }
}

fn fail_job(job: Job, msg: &str) {
    match job {
        Job::Image { reply, .. } => {
            let _ = reply.send(Err(EngineError::Inference(msg.to_string())));
        }
        Job::Clip { reply, .. } => {
            let _ = reply.send(Err(EngineError::Inference(msg.to_string())));
        }
        Job::ClipLen { reply } => {
            let _ = reply.send(Err(EngineError::Inference(msg.to_string())));
        }
    }
}
