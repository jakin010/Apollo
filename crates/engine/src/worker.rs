//! Per-model batching worker.
//!
//! Each enabled model gets one dedicated OS thread that owns the model's resident
//! weights. The async control plane submits [`Job`]s over a channel; the thread
//! loads the model lazily on first use (or at startup when the model is pinned
//! with `keep_in_memory`), serves requests with no-wait-window dynamic batching
//! (pending images are merged up to the model's batch size into a single forward
//! pass), and unloads the model after an idle period unless it is pinned.
//!
//! Loading is a separate job ([`ModelHandle::ensure_loaded`]) so callers can wait
//! for a cold model without that wait counting against the per-classification
//! timeout. A panic inside the model (load or forward pass) is caught: waiters get
//! an error, the possibly-corrupt model is dropped, and the next job reloads it —
//! the worker thread itself never dies.
//!
//! Video inputs are handled one frame at a time through this same image path
//! (the scheduler samples frames and submits them as ordinary image jobs); there
//! is no separate whole-clip path.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use tokio::sync::oneshot;

use apollo_domain::{Classification, DecodedImage};
use apollo_inference::ImageClassifier;

use crate::error::EngineError;

type ClassifyReply = oneshot::Sender<Result<Classification, EngineError>>;
/// Answers a load request with the model's input size.
type LoadReply = oneshot::Sender<Result<u32, EngineError>>;

/// Builds a model on the worker thread. Errors are reported as text.
pub(crate) type Loader = Box<dyn Fn() -> Result<Box<dyn ImageClassifier>, String> + Send>;

/// A unit of work for a model worker.
pub(crate) enum Job {
    /// Classify one image, merged with other pending images into a single
    /// batched forward pass.
    Classify {
        /// Shared, so several models can classify one decoded image without
        /// copying it.
        image: Arc<DecodedImage>,
        reply: ClassifyReply,
    },
    /// Make sure the weights are resident; replies with the model's input size
    /// once they are (or the load error).
    Load { reply: LoadReply },
}

/// Async-side handle to a model worker.
pub(crate) struct ModelHandle {
    tx: Sender<Job>,
    stats: Arc<WorkerStats>,
}

/// Live counters a worker keeps for `apollo status`.
#[derive(Default)]
struct WorkerStats {
    loaded: AtomicBool,
    /// Input size of the loaded model (0 until first loaded).
    input_size: AtomicU32,
    /// Times the weights were loaded (reloads after idle unload count again).
    loads: AtomicU64,
    /// Images classified successfully.
    classified: AtomicU64,
    /// Unix milliseconds of the last classification (0 = never).
    last_used_ms: AtomicU64,
}

/// A point-in-time view of one worker.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WorkerSnapshot {
    pub(crate) loaded: bool,
    pub(crate) input_size: Option<u32>,
    pub(crate) loads: u64,
    pub(crate) classified: u64,
    pub(crate) last_used: Option<SystemTime>,
    /// Classification / load requests waiting for the worker.
    pub(crate) queued_jobs: usize,
}

impl ModelHandle {
    pub(crate) fn snapshot(&self) -> WorkerSnapshot {
        let s = &self.stats;
        let ms = s.last_used_ms.load(Ordering::Relaxed);
        let size = s.input_size.load(Ordering::Relaxed);
        WorkerSnapshot {
            loaded: s.loaded.load(Ordering::Relaxed),
            input_size: (size > 0).then_some(size),
            loads: s.loads.load(Ordering::Relaxed),
            classified: s.classified.load(Ordering::Relaxed),
            last_used: (ms > 0).then(|| UNIX_EPOCH + Duration::from_millis(ms)),
            queued_jobs: self.tx.len(),
        }
    }

    /// Wait until the model's weights are loaded and return its input size
    /// (square side in pixels). Cheap when already resident.
    pub(crate) async fn ensure_loaded(&self) -> Result<u32, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Job::Load { reply })
            .map_err(|_| EngineError::WorkerGone)?;
        rx.await.map_err(|_| EngineError::WorkerGone)?
    }

    pub(crate) async fn classify_image(
        &self,
        image: Arc<DecodedImage>,
    ) -> Result<Classification, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Job::Classify { image, reply })
            .map_err(|_| EngineError::WorkerGone)?;
        rx.await.map_err(|_| EngineError::WorkerGone)?
    }
}

/// Settings for one worker thread.
pub(crate) struct WorkerSpec {
    /// Thread / log name.
    pub(crate) name: String,
    /// Max images merged into one forward pass.
    pub(crate) batch: usize,
    /// Pin the weights: load at startup and never idle-unload.
    pub(crate) keep_in_memory: bool,
    /// Unload after this long without work (unless pinned).
    pub(crate) idle: Duration,
}

/// Spawn the worker thread and return its handle.
pub(crate) fn spawn_worker(spec: WorkerSpec, loader: Loader) -> ModelHandle {
    let (tx, rx) = crossbeam_channel::unbounded::<Job>();
    let stats = Arc::new(WorkerStats::default());
    let worker_stats = Arc::clone(&stats);
    std::thread::Builder::new()
        .name(format!("apollo-model-{}", spec.name))
        .spawn(move || Worker::new(spec, loader, worker_stats).run(rx))
        .expect("failed to spawn model worker thread");
    ModelHandle { tx, stats }
}

struct Worker {
    spec: WorkerSpec,
    loader: Loader,
    model: Option<Box<dyn ImageClassifier>>,
    stats: Arc<WorkerStats>,
}

impl Worker {
    fn new(spec: WorkerSpec, loader: Loader, stats: Arc<WorkerStats>) -> Self {
        Self {
            spec,
            loader,
            model: None,
            stats,
        }
    }

    /// Drop the resident model (idle unload or after a panic).
    fn unload(&mut self) -> bool {
        self.stats.loaded.store(false, Ordering::Relaxed);
        self.model.take().is_some()
    }

    fn run(mut self, rx: Receiver<Job>) {
        if self.spec.keep_in_memory
            && let Err(e) = self.ensure_loaded()
        {
            tracing::warn!(model = %self.spec.name, error = %e, "preload failed; will retry on first use");
        }
        loop {
            match rx.recv_timeout(self.spec.idle) {
                Ok(Job::Load { reply }) => {
                    let _ = reply.send(self.ensure_loaded());
                }
                Ok(Job::Classify { image, reply }) => match self.ensure_loaded() {
                    Ok(_) => self.run_batch(image, reply, &rx),
                    Err(e) => {
                        let _ = reply.send(Err(e));
                    }
                },
                // Idle: drop resident weights unless pinned.
                Err(RecvTimeoutError::Timeout) => {
                    if !self.spec.keep_in_memory && self.unload() {
                        tracing::debug!(model = %self.spec.name, "unloaded idle model");
                    }
                }
                // All handles dropped (engine shutting down).
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    /// Load the model if it is not resident and return its input size. A failure
    /// (or panic) leaves it unloaded, so the next job tries again.
    fn ensure_loaded(&mut self) -> Result<u32, EngineError> {
        if let Some(model) = &self.model {
            return Ok(model.input_size());
        }
        match catch_unwind(AssertUnwindSafe(|| (self.loader)())) {
            Ok(Ok(m)) => {
                // Warm up with a 1x1 image: the first forward pass compiles GPU
                // kernels / allocates buffers and can take seconds, which would
                // otherwise eat into the first real request's timeout.
                let started = std::time::Instant::now();
                let warm = DecodedImage {
                    width: 1,
                    height: 1,
                    data: vec![0; 3],
                };
                match catch_unwind(AssertUnwindSafe(|| m.classify(&[&warm]))) {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        tracing::warn!(model = %self.spec.name, error = %e, "warm-up inference failed")
                    }
                    Err(panic) => {
                        return Err(EngineError::ModelUnavailable(format!(
                            "model panicked during warm-up: {}",
                            panic_message(&panic)
                        )));
                    }
                }
                tracing::info!(
                    model = %self.spec.name,
                    warmup_ms = started.elapsed().as_millis() as u64,
                    "model loaded"
                );
                let size = m.input_size();
                self.model = Some(m);
                self.stats.input_size.store(size, Ordering::Relaxed);
                self.stats.loads.fetch_add(1, Ordering::Relaxed);
                self.stats.loaded.store(true, Ordering::Relaxed);
                Ok(size)
            }
            Ok(Err(e)) => Err(EngineError::ModelUnavailable(format!(
                "model load failed: {e}"
            ))),
            Err(panic) => Err(EngineError::ModelUnavailable(format!(
                "model load panicked: {}",
                panic_message(&panic)
            ))),
        }
    }

    /// Merge `first` plus whatever else is queued (up to the batch size) into one
    /// forward pass, then fan the per-image results back to each caller. Load
    /// requests found while draining are answered immediately (the model is
    /// resident).
    fn run_batch(
        &mut self,
        first: Arc<DecodedImage>,
        first_reply: ClassifyReply,
        rx: &Receiver<Job>,
    ) {
        let batch = self.spec.batch.max(1);
        let mut images = Vec::with_capacity(batch);
        let mut replies = Vec::with_capacity(batch);
        images.push(first);
        replies.push(first_reply);
        while images.len() < batch {
            match rx.try_recv() {
                Ok(Job::Classify { image, reply }) => {
                    images.push(image);
                    replies.push(reply);
                }
                Ok(Job::Load { reply }) => {
                    let _ = reply.send(self.ensure_loaded());
                }
                Err(_) => break,
            }
        }

        let Some(model) = self.model.as_deref() else {
            for reply in replies {
                let _ = reply.send(Err(EngineError::ModelUnavailable(
                    "model is not loaded".into(),
                )));
            }
            return;
        };
        let views: Vec<&DecodedImage> = images.iter().map(|img| img.as_ref()).collect();
        let outcome = catch_unwind(AssertUnwindSafe(|| model.classify(&views)));
        let err = match outcome {
            // A non-finite score means the forward pass went wrong (e.g. a GPU
            // fault); never hand such a result on — it would be stored and break
            // reads of the task. Failing the batch lets the scheduler retry it.
            Ok(Ok(results)) if results.iter().any(has_non_finite) => {
                tracing::warn!(model = %self.spec.name, "model produced non-finite scores; batch failed");
                "model produced non-finite scores".to_string()
            }
            Ok(Ok(results)) if results.len() == replies.len() => {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                self.stats
                    .classified
                    .fetch_add(results.len() as u64, Ordering::Relaxed);
                self.stats.last_used_ms.store(now_ms, Ordering::Relaxed);
                for (reply, c) in replies.into_iter().zip(results) {
                    let _ = reply.send(Ok(c));
                }
                return;
            }
            Ok(Ok(results)) => format!(
                "model returned {} results for {} images",
                results.len(),
                replies.len()
            ),
            Ok(Err(e)) => e.to_string(),
            Err(panic) => {
                // The model may be in a broken state after a panic: drop it so
                // the next job reloads it fresh.
                self.unload();
                let msg = panic_message(&panic);
                tracing::error!(model = %self.spec.name, panic = %msg, "model panicked; unloaded");
                format!("model panicked: {msg}")
            }
        };
        for reply in replies {
            let _ = reply.send(Err(EngineError::Inference(err.clone())));
        }
    }
}

fn has_non_finite(c: &Classification) -> bool {
    c.predictions.iter().any(|p| !p.score.is_finite())
}

fn panic_message(panic: &Box<dyn Any + Send>) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use apollo_domain::Prediction;
    use apollo_inference::InferenceError;

    /// Classifies every image as label = its width; panics on width 0 and scores
    /// NaN on width 13.
    struct Fake;

    impl ImageClassifier for Fake {
        fn classify(
            &self,
            images: &[&DecodedImage],
        ) -> Result<Vec<Classification>, InferenceError> {
            Ok(images
                .iter()
                .map(|img| {
                    assert!(img.width > 0, "boom");
                    Classification {
                        predictions: vec![Prediction {
                            label: img.width,
                            score: if img.width == 13 { f32::NAN } else { 1.0 },
                        }],
                    }
                })
                .collect())
        }
        fn labels(&self) -> &[String] {
            &[]
        }
        fn input_size(&self) -> u32 {
            224
        }
    }

    fn img(width: u32) -> Arc<DecodedImage> {
        Arc::new(DecodedImage {
            width,
            height: 1,
            data: vec![0; width as usize * 3],
        })
    }

    fn spawn(loads: Arc<AtomicUsize>, fail_first: bool, keep: bool) -> ModelHandle {
        let loader: Loader = Box::new(move || {
            let n = loads.fetch_add(1, Ordering::SeqCst);
            if fail_first && n == 0 {
                return Err("hub unreachable".into());
            }
            Ok(Box::new(Fake) as Box<dyn ImageClassifier>)
        });
        spawn_worker(
            WorkerSpec {
                name: "test".into(),
                batch: 4,
                keep_in_memory: keep,
                idle: Duration::from_secs(60),
            },
            loader,
        )
    }

    #[tokio::test]
    async fn loads_once_and_classifies() {
        let loads = Arc::new(AtomicUsize::new(0));
        let h = spawn(loads.clone(), false, false);
        assert_eq!(h.ensure_loaded().await.unwrap(), 224);
        h.ensure_loaded().await.unwrap();
        let c = h.classify_image(img(7)).await.unwrap();
        assert_eq!(c.predictions[0].label, 7);
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn snapshot_reports_live_counters() {
        let loads = Arc::new(AtomicUsize::new(0));
        let h = spawn(loads, false, false);
        let before = h.snapshot();
        assert!(!before.loaded && before.loads == 0 && before.classified == 0);
        assert_eq!((before.input_size, before.last_used), (None, None));

        h.classify_image(img(3)).await.unwrap();
        h.classify_image(img(4)).await.unwrap();
        let after = h.snapshot();
        assert!(after.loaded);
        assert_eq!(after.input_size, Some(224));
        assert_eq!(
            (after.loads, after.classified, after.queued_jobs),
            (1, 2, 0)
        );
        assert!(after.last_used.is_some());
    }

    #[tokio::test]
    async fn pinned_models_preload() {
        let loads = Arc::new(AtomicUsize::new(0));
        let h = spawn(loads.clone(), false, true);
        h.ensure_loaded().await.unwrap();
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn load_failure_is_reported_and_retried() {
        let loads = Arc::new(AtomicUsize::new(0));
        let h = spawn(loads.clone(), true, false);
        let err = h.ensure_loaded().await.unwrap_err();
        assert!(matches!(err, EngineError::ModelUnavailable(_)), "{err:?}");
        h.ensure_loaded().await.unwrap();
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn non_finite_scores_are_an_error() {
        let h = spawn(Arc::new(AtomicUsize::new(0)), false, false);
        let err = h.classify_image(img(13)).await.unwrap_err();
        assert!(err.to_string().contains("non-finite"), "{err}");
        assert!(err.is_transient());
        assert_eq!(
            h.classify_image(img(5)).await.unwrap().predictions[0].label,
            5
        );
    }

    #[tokio::test]
    async fn panic_is_contained_and_model_reloads() {
        let loads = Arc::new(AtomicUsize::new(0));
        let h = spawn(loads.clone(), false, false);
        let err = h.classify_image(img(0)).await.unwrap_err();
        assert!(matches!(err, EngineError::Inference(_)), "{err:?}");
        // The worker survived, reloaded, and serves the next request.
        let c = h.classify_image(img(3)).await.unwrap();
        assert_eq!(c.predictions[0].label, 3);
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }
}
