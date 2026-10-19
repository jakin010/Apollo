//! Prometheus metrics: counters and histograms recorded as work happens, plus
//! gauges read from the engine's live state, rendered in the Prometheus text
//! exposition format (version 0.0.4) by [`Engine::metrics_text`].
//!
//! Written by hand rather than with a metrics crate: a few atomics and mutexed
//! histograms cover what Apollo reports. Per-model metrics are keyed by model
//! name, so they keep counting when a reload replaces a model's worker.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apollo_domain::ItemState;

use crate::{Engine, EngineError};

/// Upper bounds (seconds) for latency histograms: 5 ms to 2 min.
const SECONDS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
];
/// Upper bounds for batch sizes.
const BATCH: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0];
/// Upper bounds (seconds) for model loads, which include downloads: 0.5 s to 30 min.
const LOAD_SECONDS: &[f64] = &[0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 1800.0];

/// A cumulative histogram over fixed bucket bounds.
pub(crate) struct Histogram {
    bounds: &'static [f64],
    state: Mutex<HistogramState>,
}

#[derive(Clone)]
struct HistogramState {
    /// Observations at or below each bound (not cumulative; summed on render).
    counts: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Histogram {
    fn new(bounds: &'static [f64]) -> Self {
        Self {
            bounds,
            state: Mutex::new(HistogramState {
                counts: vec![0; bounds.len()],
                sum: 0.0,
                count: 0,
            }),
        }
    }

    pub(crate) fn observe(&self, value: f64) {
        let mut s = self.state.lock().unwrap();
        if let Some(i) = self.bounds.iter().position(|&b| value <= b) {
            s.counts[i] += 1;
        }
        s.sum += value;
        s.count += 1;
    }

    pub(crate) fn observe_duration(&self, d: Duration) {
        self.observe(d.as_secs_f64());
    }

    fn snapshot(&self) -> HistogramState {
        self.state.lock().unwrap().clone()
    }
}

/// Counters keyed by a small set of label values.
#[derive(Default)]
struct CounterVec(Mutex<BTreeMap<Vec<&'static str>, u64>>);

impl CounterVec {
    fn inc(&self, labels: &[&'static str]) {
        *self.0.lock().unwrap().entry(labels.to_vec()).or_default() += 1;
    }

    fn snapshot(&self) -> Vec<(Vec<&'static str>, u64)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }
}

/// One model's metrics, shared with its worker (and its replacements).
pub(crate) struct ModelMetrics {
    /// Images (and video frames) classified.
    pub(crate) inferences: AtomicU64,
    /// Images whose batch failed (inference error, panic, bad output).
    pub(crate) failures: AtomicU64,
    /// Images per forward pass.
    pub(crate) batch_size: Histogram,
    /// Duration of each forward pass.
    pub(crate) batch_seconds: Histogram,
    /// Weight loads (reloads after an idle unload count again).
    pub(crate) loads: AtomicU64,
    /// Duration of each load, including download and warm-up.
    pub(crate) load_seconds: Histogram,
    /// Finished video scans, the frames they classified, and how many stopped
    /// early on a trigger.
    pub(crate) scans: AtomicU64,
    pub(crate) scan_frames: AtomicU64,
    pub(crate) scan_early_exits: AtomicU64,
}

impl ModelMetrics {
    fn new() -> Self {
        Self {
            inferences: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            batch_size: Histogram::new(BATCH),
            batch_seconds: Histogram::new(SECONDS),
            loads: AtomicU64::new(0),
            load_seconds: Histogram::new(LOAD_SECONDS),
            scans: AtomicU64::new(0),
            scan_frames: AtomicU64::new(0),
            scan_early_exits: AtomicU64::new(0),
        }
    }
}

/// Everything the engine counts.
pub(crate) struct Metrics {
    /// By outcome: `accepted`, `existing`, `invalid`, `overloaded`, `error`.
    submissions: CounterVec,
    /// By final state.
    items_finished: CounterVec,
    /// Bytes fetched for remote and local-file inputs (not uploads).
    pub(crate) fetch_bytes: AtomicU64,
    pub(crate) fetch_seconds: Histogram,
    /// By kind: `transient` (retried) or `permanent`.
    fetch_failures: CounterVec,
    /// Model results served from the result cache.
    pub(crate) cache_hits: AtomicU64,
    /// By event (`progress`, `finished`) and result (`ok`, `error`).
    webhook_deliveries: CounterVec,
    models: Mutex<BTreeMap<String, Arc<ModelMetrics>>>,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        Self {
            submissions: CounterVec::default(),
            items_finished: CounterVec::default(),
            fetch_bytes: AtomicU64::new(0),
            fetch_seconds: Histogram::new(SECONDS),
            fetch_failures: CounterVec::default(),
            cache_hits: AtomicU64::new(0),
            webhook_deliveries: CounterVec::default(),
            models: Mutex::new(BTreeMap::new()),
        }
    }

    /// The metrics of model `name`, created on first use.
    pub(crate) fn model(&self, name: &str) -> Arc<ModelMetrics> {
        Arc::clone(
            self.models
                .lock()
                .unwrap()
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(ModelMetrics::new())),
        )
    }

    pub(crate) fn submission(&self, outcome: &'static str) {
        self.submissions.inc(&[outcome]);
    }

    pub(crate) fn item_finished(&self, state: ItemState) {
        self.items_finished.inc(&[state.as_str()]);
    }

    pub(crate) fn fetch_failed(&self, transient: bool) {
        self.fetch_failures
            .inc(&[if transient { "transient" } else { "permanent" }]);
    }

    pub(crate) fn webhook(&self, event: &'static str, ok: bool) {
        self.webhook_deliveries
            .inc(&[event, if ok { "ok" } else { "error" }]);
    }
}

impl Engine {
    /// The current metrics in the Prometheus text format.
    pub async fn metrics_text(&self) -> Result<String, EngineError> {
        let status = self.status().await?;
        let live = self.live();
        let m = &self.inner.metrics;
        let mut out = Writer::default();

        out.family("apollo_build_info", "gauge", "Apollo's version.");
        out.sample(
            "apollo_build_info",
            &[("version", env!("CARGO_PKG_VERSION"))],
            1.0,
        );

        out.family(
            "apollo_config_generation",
            "gauge",
            "Configs applied since start (1 = the startup config).",
        );
        out.sample("apollo_config_generation", &[], live.generation as f64);
        out.family(
            "apollo_config_loaded_timestamp_seconds",
            "gauge",
            "When the running config was applied (unix seconds).",
        );
        out.sample(
            "apollo_config_loaded_timestamp_seconds",
            &[],
            unix_secs(live.loaded_at),
        );

        out.counter_vec(
            "apollo_submissions_total",
            "Submissions, by outcome (accepted, existing, invalid, overloaded, error).",
            &["outcome"],
            &m.submissions,
        );
        out.counter_vec(
            "apollo_items_finished_total",
            "Inputs that finished, by final state.",
            &["state"],
            &m.items_finished,
        );

        out.family(
            "apollo_tasks",
            "gauge",
            "Tasks in storage, by state (until retention purges them).",
        );
        let t = status.tasks;
        for (state, n) in [
            ("queued", t.queued),
            ("processing", t.processing),
            ("completed", t.completed),
            ("failed", t.failed),
            ("cancelled", t.cancelled),
        ] {
            out.sample("apollo_tasks", &[("state", state)], n as f64);
        }

        out.gauge(
            "apollo_items_in_flight",
            "Inputs accepted and not finished.",
            status.items_in_flight as f64,
        );
        out.gauge(
            "apollo_max_pending",
            "Limit on inputs in flight before submissions are rejected (0 = none).",
            status.max_pending as f64,
        );
        out.gauge(
            "apollo_slots",
            "Inputs that may be processed at once ([app].max_concurrent).",
            status.slots_total as f64,
        );
        out.gauge(
            "apollo_slots_busy",
            "Inputs being processed.",
            status.slots_busy as f64,
        );
        out.gauge(
            "apollo_slots_waiting",
            "Inputs waiting for a slot.",
            status.slots_waiting as f64,
        );
        if let Some(used) = status.memory_bytes {
            out.gauge(
                "apollo_memory_bytes",
                "Memory the process uses (physical footprint on macOS, RSS elsewhere).",
                used as f64,
            );
        }
        if let Some(limit) = status.max_memory_bytes {
            out.gauge(
                "apollo_memory_limit_bytes",
                "The soft memory cap ([app].max_memory).",
                limit as f64,
            );
        }

        out.counter(
            "apollo_fetch_bytes_total",
            "Bytes fetched for remote and local-file inputs.",
            m.fetch_bytes.load(Ordering::Relaxed),
        );
        out.histogram(
            "apollo_fetch_seconds",
            "Time to fetch an input.",
            &[("", &m.fetch_seconds)],
        );
        out.counter_vec(
            "apollo_fetch_failures_total",
            "Failed fetches, by kind (transient ones are retried).",
            &["kind"],
            &m.fetch_failures,
        );
        out.counter(
            "apollo_cache_hits_total",
            "Model results served from the result cache.",
            m.cache_hits.load(Ordering::Relaxed),
        );

        if let Some(pending) = status.webhooks_pending {
            out.gauge(
                "apollo_webhooks_pending",
                "Finished inputs whose webhook event is not delivered yet.",
                pending as f64,
            );
        }
        out.counter_vec(
            "apollo_webhook_deliveries_total",
            "Webhook delivery attempts, by event and result.",
            &["event", "result"],
            &m.webhook_deliveries,
        );

        // Per model: gauges for the configured models, counters for every
        // model seen since start.
        out.family(
            "apollo_model_loaded",
            "gauge",
            "1 when the model's weights are in memory.",
        );
        for s in &status.models {
            let v = if s.loaded { 1.0 } else { 0.0 };
            out.sample("apollo_model_loaded", &[("model", &s.label)], v);
        }
        out.family(
            "apollo_model_queued_jobs",
            "gauge",
            "Requests waiting for the model's worker.",
        );
        for s in &status.models {
            let v = s.queued_jobs as f64;
            out.sample("apollo_model_queued_jobs", &[("model", &s.label)], v);
        }

        let models: Vec<(String, Arc<ModelMetrics>)> = m
            .models
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        let per_model =
            |out: &mut Writer, name: &str, help: &str, f: &dyn Fn(&ModelMetrics) -> u64| {
                out.family(name, "counter", help);
                for (model, mm) in &models {
                    out.sample(name, &[("model", model)], f(mm) as f64);
                }
            };
        per_model(
            &mut out,
            "apollo_model_inferences_total",
            "Images (and video frames) classified.",
            &|mm| mm.inferences.load(Ordering::Relaxed),
        );
        per_model(
            &mut out,
            "apollo_model_failures_total",
            "Images whose forward pass failed.",
            &|mm| mm.failures.load(Ordering::Relaxed),
        );
        per_model(
            &mut out,
            "apollo_model_loads_total",
            "Times the model's weights were loaded.",
            &|mm| mm.loads.load(Ordering::Relaxed),
        );
        per_model(
            &mut out,
            "apollo_scans_total",
            "Video scans finished.",
            &|mm| mm.scans.load(Ordering::Relaxed),
        );
        per_model(
            &mut out,
            "apollo_scan_frames_total",
            "Video frames classified by scans.",
            &|mm| mm.scan_frames.load(Ordering::Relaxed),
        );
        per_model(
            &mut out,
            "apollo_scan_early_exits_total",
            "Video scans that stopped early on a trigger.",
            &|mm| mm.scan_early_exits.load(Ordering::Relaxed),
        );
        let hist = |pick: fn(&ModelMetrics) -> &Histogram| -> Vec<(&str, &Histogram)> {
            models
                .iter()
                .map(|(name, mm)| (name.as_str(), pick(mm)))
                .collect()
        };
        out.histogram(
            "apollo_model_batch_size",
            "Images per forward pass.",
            &hist(|mm| &mm.batch_size),
        );
        out.histogram(
            "apollo_model_batch_seconds",
            "Duration of a forward pass.",
            &hist(|mm| &mm.batch_seconds),
        );
        out.histogram(
            "apollo_model_load_seconds",
            "Duration of a weight load, including download and warm-up.",
            &hist(|mm| &mm.load_seconds),
        );
        Ok(out.0)
    }
}

fn unix_secs(t: std::time::SystemTime) -> f64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs() as f64)
}

/// Builds the exposition text.
#[derive(Default)]
struct Writer(String);

impl Writer {
    fn family(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.0, "# HELP {name} {help}");
        let _ = writeln!(self.0, "# TYPE {name} {kind}");
    }

    fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: f64) {
        self.0.push_str(name);
        if !labels.is_empty() {
            self.0.push('{');
            for (i, (k, v)) in labels.iter().enumerate() {
                if i > 0 {
                    self.0.push(',');
                }
                let _ = write!(self.0, "{k}=\"{}\"", escape(v));
            }
            self.0.push('}');
        }
        let _ = writeln!(self.0, " {}", number(value));
    }

    fn gauge(&mut self, name: &str, help: &str, value: f64) {
        self.family(name, "gauge", help);
        self.sample(name, &[], value);
    }

    fn counter(&mut self, name: &str, help: &str, value: u64) {
        self.family(name, "counter", help);
        self.sample(name, &[], value as f64);
    }

    fn counter_vec(&mut self, name: &str, help: &str, keys: &[&str], values: &CounterVec) {
        self.family(name, "counter", help);
        for (labels, n) in values.snapshot() {
            let pairs: Vec<(&str, &str)> = keys.iter().copied().zip(labels).collect();
            self.sample(name, &pairs, n as f64);
        }
    }

    /// One histogram family; each entry is (model label value, or "" for
    /// none, histogram).
    fn histogram(&mut self, name: &str, help: &str, entries: &[(&str, &Histogram)]) {
        self.family(name, "histogram", help);
        for (model, h) in entries {
            let s = h.snapshot();
            let base: Vec<(&str, &str)> = if model.is_empty() {
                Vec::new()
            } else {
                vec![("model", model)]
            };
            let mut cumulative = 0;
            for (bound, n) in h.bounds.iter().zip(&s.counts) {
                cumulative += n;
                let le = number(*bound);
                let mut labels = base.clone();
                labels.push(("le", &le));
                self.sample(&format!("{name}_bucket"), &labels, cumulative as f64);
            }
            let mut labels = base.clone();
            labels.push(("le", "+Inf"));
            self.sample(&format!("{name}_bucket"), &labels, s.count as f64);
            self.sample(&format!("{name}_sum"), &base, s.sum);
            self.sample(&format!("{name}_count"), &base, s.count as f64);
        }
    }
}

/// A sample value: integers without a fraction, `+Inf`/`-Inf`/`NaN` as
/// Prometheus spells them.
fn number(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 { "+Inf" } else { "-Inf" }.into()
    } else if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

/// Escape a label value: backslash, double quote and newline.
fn escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histograms_render_cumulative_buckets() {
        let h = Histogram::new(&[1.0, 2.0]);
        for v in [0.5, 1.5, 1.7, 9.0] {
            h.observe(v);
        }
        let mut w = Writer::default();
        w.histogram("x_seconds", "X.", &[("m\"1", &h)]);
        let text = w.0;
        assert!(text.contains("# TYPE x_seconds histogram"), "{text}");
        assert!(
            text.contains("x_seconds_bucket{model=\"m\\\"1\",le=\"1\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("x_seconds_bucket{model=\"m\\\"1\",le=\"2\"} 3"),
            "{text}"
        );
        assert!(
            text.contains("x_seconds_bucket{model=\"m\\\"1\",le=\"+Inf\"} 4"),
            "{text}"
        );
        assert!(
            text.contains("x_seconds_sum{model=\"m\\\"1\"} 12.7"),
            "{text}"
        );
        assert!(
            text.contains("x_seconds_count{model=\"m\\\"1\"} 4"),
            "{text}"
        );
    }

    #[test]
    fn numbers_and_counters() {
        assert_eq!(number(3.0), "3");
        assert_eq!(number(0.25), "0.25");
        assert_eq!(number(f64::INFINITY), "+Inf");
        let c = CounterVec::default();
        c.inc(&["finished", "ok"]);
        c.inc(&["finished", "ok"]);
        c.inc(&["progress", "error"]);
        let mut w = Writer::default();
        w.counter_vec("d_total", "D.", &["event", "result"], &c);
        assert!(
            w.0.contains("d_total{event=\"finished\",result=\"ok\"} 2"),
            "{}",
            w.0
        );
        assert!(
            w.0.contains("d_total{event=\"progress\",result=\"error\"} 1"),
            "{}",
            w.0
        );
    }
}
