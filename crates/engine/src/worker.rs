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
//! is no separate whole-clip path. Text models take text jobs the same way, and
//! models that read text (OCR) take image jobs and answer with the text.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use tokio::sync::oneshot;

use apollo_domain::{Classification, DecodedImage};
use apollo_inference::LoadedModel;

use crate::error::EngineError;
use crate::metrics::ModelMetrics;

type RunReply = oneshot::Sender<Result<Output, EngineError>>;
/// Answers a load request with the model's input shape.
type LoadReply = oneshot::Sender<Result<InputShape, EngineError>>;

/// What a loaded model takes as input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InputShape {
    /// Side of the square input of an image model (video frames are extracted
    /// at it); 0 for a text model.
    pub(crate) size: u32,
    /// Still images may be resized to `size` before they are sent, with
    /// `apollo_inference::resize_to_input`, so the worker thread doesn't spend
    /// its time resizing while the GPU waits. False when the model needs the
    /// full image (`pdq`).
    pub(crate) resize_ahead: bool,
}

/// Builds a model on the worker thread. Errors are reported as text.
pub(crate) type Loader = Box<dyn Fn() -> Result<LoadedModel, String> + Send>;

/// What a model gave for one input.
#[derive(Debug)]
pub(crate) enum Output {
    Classification(Classification),
    /// The text a model read.
    Text(String),
}

/// What a job runs on.
pub(crate) enum Payload {
    /// Shared, so several models can classify one decoded image without
    /// copying it.
    Image(Arc<DecodedImage>),
    Text(Arc<str>),
}

/// A unit of work for a model worker.
pub(crate) enum Job {
    /// Run the model on one input (classify it, or read its text), merged
    /// with other pending inputs into a single batch.
    Run { input: Payload, reply: RunReply },
    /// Make sure the weights are resident; replies with the model's input shape
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

    /// Wait until the model's weights are loaded and return its input shape.
    /// Cheap when already resident.
    pub(crate) async fn ensure_loaded(&self) -> Result<InputShape, EngineError> {
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
        classification(self.run(Payload::Image(image)).await?)
    }

    pub(crate) async fn classify_text(
        &self,
        text: Arc<str>,
    ) -> Result<Classification, EngineError> {
        classification(self.run(Payload::Text(text)).await?)
    }

    /// The text in an image, from a model that reads text.
    pub(crate) async fn read_image(&self, image: Arc<DecodedImage>) -> Result<String, EngineError> {
        match self.run(Payload::Image(image)).await? {
            Output::Text(text) => Ok(text),
            Output::Classification(_) => Err(EngineError::Incompatible(
                "the model classifies images; it does not read text".into(),
            )),
        }
    }

    async fn run(&self, input: Payload) -> Result<Output, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Job::Run { input, reply })
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
    /// Unload after this many milliseconds without work (unless pinned).
    /// Shared by every worker, so a config reload changes it for all of them.
    pub(crate) idle_ms: Arc<AtomicU64>,
    /// Where the worker counts its loads and batches.
    pub(crate) metrics: Arc<ModelMetrics>,
}

impl WorkerSpec {
    fn idle(&self) -> Duration {
        Duration::from_millis(self.idle_ms.load(Ordering::Relaxed).max(1))
    }
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
    model: Option<LoadedModel>,
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
            match rx.recv_timeout(self.spec.idle()) {
                Ok(Job::Load { reply }) => {
                    let _ = reply.send(self.ensure_loaded());
                }
                Ok(Job::Run { input, reply }) => match self.ensure_loaded() {
                    Ok(_) => self.run_batch(input, reply, &rx),
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

    /// Load the model if it is not resident and return its input shape. A
    /// failure (or panic) leaves it unloaded, so the next job tries again.
    fn ensure_loaded(&mut self) -> Result<InputShape, EngineError> {
        if let Some(model) = &self.model {
            return Ok(input_shape(model));
        }
        let load_started = std::time::Instant::now();
        match catch_unwind(AssertUnwindSafe(|| (self.loader)())) {
            Ok(Ok(m)) => {
                // Warm up with a tiny input: the first forward pass compiles GPU
                // kernels / allocates buffers and can take seconds, which would
                // otherwise eat into the first real request's timeout.
                let started = std::time::Instant::now();
                match catch_unwind(AssertUnwindSafe(|| m.warm_up())) {
                    Ok(Ok(())) => {}
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
                let shape = input_shape(&m);
                self.model = Some(m);
                self.stats.input_size.store(shape.size, Ordering::Relaxed);
                self.stats.loads.fetch_add(1, Ordering::Relaxed);
                self.stats.loaded.store(true, Ordering::Relaxed);
                let metrics = &self.spec.metrics;
                metrics.loads.fetch_add(1, Ordering::Relaxed);
                metrics
                    .load_seconds
                    .observe_duration(load_started.elapsed());
                Ok(shape)
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
    /// batch, then fan the per-input results back to each caller. Load
    /// requests found while draining are answered immediately (the model is
    /// resident). An input of the wrong kind for the model (text for an image
    /// model, or the reverse) fails on its own.
    fn run_batch(&mut self, first: Payload, first_reply: RunReply, rx: &Receiver<Job>) {
        // A model that reads text reads one image at a time anyway: batching
        // would only hold each reply (and the caller's timeout) until the
        // whole batch was read, and fail it with any other image's error.
        let batch = match self.model {
            Some(LoadedModel::ImageToText(_)) => 1,
            _ => self.spec.batch.max(1),
        };
        let mut jobs = Vec::with_capacity(batch);
        jobs.push((first, first_reply));
        while jobs.len() < batch {
            match rx.try_recv() {
                Ok(Job::Run { input, reply }) => jobs.push((input, reply)),
                Ok(Job::Load { reply }) => {
                    let _ = reply.send(self.ensure_loaded());
                }
                Err(_) => break,
            }
        }

        let Some(model) = self.model.as_ref() else {
            for (_, reply) in jobs {
                let _ = reply.send(Err(EngineError::ModelUnavailable(
                    "model is not loaded".into(),
                )));
            }
            return;
        };
        let wrong_kind = |reply: RunReply, what: &str| {
            let _ = reply.send(Err(EngineError::Incompatible(format!(
                "model '{}' does not take {what}",
                self.spec.name
            ))));
        };
        let mut replies = Vec::with_capacity(jobs.len());
        let mut images = Vec::new();
        let mut take_images = |jobs: Vec<(Payload, RunReply)>| {
            for (input, reply) in jobs {
                match input {
                    Payload::Image(img) => {
                        images.push(img);
                        replies.push(reply);
                    }
                    Payload::Text(_) => wrong_kind(reply, "text"),
                }
            }
        };
        let outcome = match model {
            LoadedModel::Image(m) => {
                take_images(jobs);
                if images.is_empty() {
                    return;
                }
                let views: Vec<&DecodedImage> = images.iter().map(|img| img.as_ref()).collect();
                self.timed(views.len(), || classified(m.classify(&views)))
            }
            LoadedModel::ImageToText(m) => {
                take_images(jobs);
                if images.is_empty() {
                    return;
                }
                let views: Vec<&DecodedImage> = images.iter().map(|img| img.as_ref()).collect();
                self.timed(views.len(), || {
                    m.read(&views)
                        .map(|texts| texts.into_iter().map(Output::Text).collect())
                })
            }
            LoadedModel::Text(m) => {
                let mut texts = Vec::with_capacity(jobs.len());
                for (input, reply) in jobs {
                    match input {
                        Payload::Text(t) => {
                            texts.push(t);
                            replies.push(reply);
                        }
                        Payload::Image(_) => wrong_kind(reply, "images"),
                    }
                }
                if texts.is_empty() {
                    return;
                }
                let views: Vec<&str> = texts.iter().map(|t| t.as_ref()).collect();
                self.timed(views.len(), || classified(m.classify(&views)))
            }
        };
        self.deliver(outcome, replies);
    }

    /// Run one batch of `n` inputs, timed and with panics caught.
    fn timed(&self, n: usize, f: impl FnOnce() -> BatchResult) -> std::thread::Result<BatchResult> {
        let metrics = &self.spec.metrics;
        metrics.batch_size.observe(n as f64);
        let started = std::time::Instant::now();
        let outcome = catch_unwind(AssertUnwindSafe(f));
        metrics.batch_seconds.observe_duration(started.elapsed());
        outcome
    }

    /// Hand each caller its result, or the batch's error.
    fn deliver(&mut self, outcome: std::thread::Result<BatchResult>, replies: Vec<RunReply>) {
        let metrics = Arc::clone(&self.spec.metrics);
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
                metrics
                    .inferences
                    .fetch_add(results.len() as u64, Ordering::Relaxed);
                self.stats.last_used_ms.store(now_ms, Ordering::Relaxed);
                for (reply, c) in replies.into_iter().zip(results) {
                    let _ = reply.send(Ok(c));
                }
                return;
            }
            Ok(Ok(results)) => format!(
                "model returned {} results for {} inputs",
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
        metrics
            .failures
            .fetch_add(replies.len() as u64, Ordering::Relaxed);
        for reply in replies {
            let _ = reply.send(Err(EngineError::Inference(err.clone())));
        }
    }
}

/// What a batch gave: an output per input, or the error.
type BatchResult = Result<Vec<Output>, apollo_inference::InferenceError>;

fn classified(
    results: Result<Vec<Classification>, apollo_inference::InferenceError>,
) -> BatchResult {
    results.map(|cs| cs.into_iter().map(Output::Classification).collect())
}

fn classification(output: Output) -> Result<Classification, EngineError> {
    match output {
        Output::Classification(c) => Ok(c),
        Output::Text(_) => Err(EngineError::Incompatible(
            "the model reads text; it does not classify".into(),
        )),
    }
}

fn input_shape(model: &LoadedModel) -> InputShape {
    match model {
        LoadedModel::Image(m) => InputShape {
            size: m.input_size(),
            resize_ahead: m.resizes_to_input(),
        },
        // Images are sized by the model itself (see
        // `apollo_inference::read_size`).
        LoadedModel::Text(_) | LoadedModel::ImageToText(_) => InputShape {
            size: 0,
            resize_ahead: false,
        },
    }
}

fn has_non_finite(output: &Output) -> bool {
    match output {
        Output::Classification(c) => c.predictions.iter().any(|p| !p.score.is_finite()),
        Output::Text(_) => false,
    }
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
    use apollo_inference::{ImageClassifier, InferenceError, TextClassifier};

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
                    let score = if img.width == 13 { f32::NAN } else { 1.0 };
                    Classification {
                        predictions: vec![Prediction::new(img.width, "w", score)],
                    }
                })
                .collect())
        }
        fn labels(&self) -> &[apollo_domain::Label] {
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

    /// Classifies every text as label = its length in bytes.
    struct FakeText;

    impl TextClassifier for FakeText {
        fn classify(&self, texts: &[&str]) -> Result<Vec<Classification>, InferenceError> {
            Ok(texts
                .iter()
                .map(|t| Classification {
                    predictions: vec![Prediction::new(t.len() as u32, "len", 0.9)],
                })
                .collect())
        }
        fn labels(&self) -> &[apollo_domain::Label] {
            &[]
        }
    }

    fn spawn_with(model: fn() -> LoadedModel) -> ModelHandle {
        spawn_worker(
            WorkerSpec {
                name: "test".into(),
                batch: 4,
                keep_in_memory: false,
                idle_ms: Arc::new(AtomicU64::new(60_000)),
                metrics: crate::metrics::Metrics::new().model("test"),
            },
            Box::new(move || Ok(model())),
        )
    }

    #[tokio::test]
    async fn text_models_take_text_and_refuse_images() {
        let text = spawn_with(|| LoadedModel::Text(Box::new(FakeText)));
        // Text models report no input size.
        assert_eq!(text.ensure_loaded().await.unwrap().size, 0);
        let c = text.classify_text(Arc::from("hello")).await.unwrap();
        assert_eq!(c.predictions[0].label, 5);
        let err = text.classify_image(img(3)).await.unwrap_err();
        assert!(err.to_string().contains("does not take images"), "{err}");

        let image = spawn_with(|| LoadedModel::Image(Box::new(Fake)));
        let err = image.classify_text(Arc::from("hi")).await.unwrap_err();
        assert!(err.to_string().contains("does not take text"), "{err}");
        // The image model still serves images afterwards.
        assert_eq!(
            image.classify_image(img(3)).await.unwrap().predictions[0].label,
            3
        );
    }

    /// Reads every image as "w<width>"; nothing on width 1.
    struct FakeReader;

    impl apollo_inference::ImageToText for FakeReader {
        fn read(&self, images: &[&DecodedImage]) -> Result<Vec<String>, InferenceError> {
            Ok(images
                .iter()
                .map(|img| match img.width {
                    1 => String::new(),
                    w => format!("w{w}"),
                })
                .collect())
        }
    }

    /// Reads every image after `ms_per_image`, failing on width 2.
    struct SlowReader {
        ms_per_image: u64,
    }

    impl apollo_inference::ImageToText for SlowReader {
        fn read(&self, images: &[&DecodedImage]) -> Result<Vec<String>, InferenceError> {
            images
                .iter()
                .map(|img| {
                    std::thread::sleep(Duration::from_millis(self.ms_per_image));
                    if img.width == 2 {
                        Err(InferenceError::Preprocess("unreadable".into()))
                    } else {
                        Ok(format!("w{}", img.width))
                    }
                })
                .collect()
        }
    }

    #[tokio::test]
    async fn reads_are_answered_one_by_one() {
        let h = Arc::new(spawn_with(|| {
            LoadedModel::ImageToText(Box::new(SlowReader { ms_per_image: 150 }))
        }));
        h.ensure_loaded().await.unwrap();
        let started = std::time::Instant::now();
        let read = |w: u32| {
            let h = Arc::clone(&h);
            tokio::spawn(async move { (h.read_image(img(w)).await, started.elapsed()) })
        };
        let (a, b, c) = (read(3), read(2), read(5));
        let (a, b, c) = (a.await.unwrap(), b.await.unwrap(), c.await.unwrap());
        // Each read is answered when it is done, not when the last one is.
        assert_eq!(a.0.unwrap(), "w3");
        assert!(a.1 < Duration::from_millis(400), "{:?}", a.1);
        // One image's error is its own.
        assert!(b.0.unwrap_err().to_string().contains("unreadable"));
        assert_eq!(c.0.unwrap(), "w5");
    }

    #[tokio::test]
    async fn models_that_read_text_answer_with_it() {
        let reader = spawn_with(|| LoadedModel::ImageToText(Box::new(FakeReader)));
        // Images are sized by the model, not to a square.
        let shape = reader.ensure_loaded().await.unwrap();
        assert_eq!((shape.size, shape.resize_ahead), (0, false));
        assert_eq!(reader.read_image(img(7)).await.unwrap(), "w7");
        assert_eq!(reader.read_image(img(1)).await.unwrap(), "");
        let err = reader.classify_image(img(3)).await.unwrap_err();
        assert!(err.to_string().contains("reads text"), "{err}");
        let err = reader.classify_text(Arc::from("hi")).await.unwrap_err();
        assert!(err.to_string().contains("does not take text"), "{err}");

        let classifier = spawn_with(|| LoadedModel::Image(Box::new(Fake)));
        let err = classifier.read_image(img(3)).await.unwrap_err();
        assert!(err.to_string().contains("does not read text"), "{err}");
    }

    fn spawn(loads: Arc<AtomicUsize>, fail_first: bool, keep: bool) -> ModelHandle {
        let loader: Loader = Box::new(move || {
            let n = loads.fetch_add(1, Ordering::SeqCst);
            if fail_first && n == 0 {
                return Err("hub unreachable".into());
            }
            Ok(LoadedModel::Image(Box::new(Fake)))
        });
        spawn_worker(
            WorkerSpec {
                name: "test".into(),
                batch: 4,
                keep_in_memory: keep,
                idle_ms: Arc::new(AtomicU64::new(60_000)),
                metrics: crate::metrics::Metrics::new().model("test"),
            },
            loader,
        )
    }

    #[tokio::test]
    async fn loads_once_and_classifies() {
        let loads = Arc::new(AtomicUsize::new(0));
        let h = spawn(loads.clone(), false, false);
        let shape = h.ensure_loaded().await.unwrap();
        assert_eq!((shape.size, shape.resize_ahead), (224, false));
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
