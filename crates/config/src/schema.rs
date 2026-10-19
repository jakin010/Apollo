//! Serde structs mirroring the TOML config (see `config.example.toml`).
//!
//! `architecture` is the model discriminator. Almost every field is optional via
//! [`crate::defaults`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::defaults;
use apollo_domain::Architecture;

/// Top-level configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub app: AppConfig,
    #[serde(default)]
    pub webhook: Option<WebhookConfig>,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub database: DatabaseConfig,
    #[serde(default)]
    pub cache: Option<CacheConfig>,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub strategies: BTreeMap<String, StrategyConfig>,
    #[serde(default)]
    pub pipelines: BTreeMap<String, PipelineConfig>,
    #[serde(default)]
    pub models: BTreeMap<String, ModelConfig>,
}

/// Authentication for the `Inference` service — **on by default**. Every
/// Inference RPC must carry a valid PASETO v4 token signed by the matching secret
/// key (mint tokens with `apollo token`). Health and reflection stay open.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// Require a token on every Inference RPC. Defaults to `true`; set `false`
    /// only on a trusted network (anyone who can reach the port can then submit
    /// work).
    #[serde(default = "crate::defaults::auth_enabled")]
    pub enabled: bool,
    /// PASERK-encoded v4 **public** key (`k4.public.…`) used to verify tokens.
    /// When unset, the server uses — creating on first start — a keypair kept in
    /// an owner-only `auth.key` file in its state directory, which `apollo token`
    /// also reads.
    #[serde(default)]
    pub public_key: Option<String>,
    /// Token subjects (`sub`) that may read and cancel every task. Everyone else
    /// only sees the tasks their own subject created.
    #[serde(default)]
    pub admin_subjects: Vec<String>,
    /// File listing revoked token ids and subjects (written by `apollo revoke`,
    /// re-read by running servers within seconds). Defaults to
    /// `revoked_tokens` in the server's state directory. Relative paths resolve
    /// from the config file's directory.
    #[serde(default)]
    pub revocation_file: Option<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: defaults::auth_enabled(),
            public_key: None,
            admin_subjects: Vec::new(),
            revocation_file: None,
        }
    }
}

/// `[tls]` — serve the gRPC API over TLS. Omit the section to serve plaintext
/// (e.g. behind a TLS-terminating proxy). Relative paths resolve from the config
/// file's directory. Certificates are read at startup; restart to pick up a
/// renewed certificate.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// PEM certificate chain (leaf first) presented to clients.
    pub cert_file: String,
    /// PEM private key for `cert_file`.
    pub key_file: String,
    /// PEM CA bundle. When set, clients must present a certificate signed by it
    /// (mutual TLS), in addition to any `[auth]` token.
    #[serde(default)]
    pub client_ca_file: Option<String>,
}

/// Result cache (optional). When present and `enabled`, model outputs are cached
/// by content hash (with a url→content-hash hint) so identical inputs skip
/// inference. A bare `[cache]` section turns it on.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    /// Master switch. Defaults to true so `[cache]` alone enables caching.
    #[serde(default = "crate::defaults::cache_enabled")]
    pub enabled: bool,
    /// Freshness window in seconds; entries older than this are ignored (and
    /// eligible for purge). Omit for entries that never expire.
    #[serde(default)]
    pub ttl_secs: Option<u64>,
}

/// `[metrics]` — a Prometheus scrape endpoint (`GET /metrics`) on its own
/// listener, separate from the gRPC port. Off by default; read at startup.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Bind host/IP. Loopback by default: the metrics are not authenticated.
    #[serde(default = "crate::defaults::metrics_endpoint")]
    pub endpoint: String,
    #[serde(default = "crate::defaults::metrics_port")]
    pub port: u16,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: defaults::metrics_endpoint(),
            port: defaults::metrics_port(),
        }
    }
}

/// `[app]` — application-wide settings.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default = "crate::defaults::endpoint")]
    pub endpoint: String,
    #[serde(default = "crate::defaults::port")]
    pub port: u16,
    #[serde(default)]
    pub cache_dir: Option<String>,
    /// Global ceiling on concurrent in-flight inferences (VRAM budget).
    #[serde(default = "crate::defaults::global_max_concurrent")]
    pub max_concurrent: u32,
    /// Seconds a model stays resident while idle before being unloaded, unless
    /// the model sets `keep_in_memory`.
    #[serde(default = "crate::defaults::idle_timeout")]
    pub idle_timeout: u32,
    /// Log verbosity: one of `trace`, `debug`, `info`, `warn`, `error`. The
    /// `RUST_LOG` environment variable, if set, overrides this at startup.
    #[serde(default = "crate::defaults::log_level")]
    pub log_level: String,
    /// Canonical path the running daemon treats as authoritative (for writes and
    /// reloads). Does not locate the file initially — that's `--config` or the
    /// built-in default in [`crate::load`].
    #[serde(default)]
    pub config_file: Option<String>,
    /// Soft ceiling on process memory (e.g. `4gb`, `512mb`; `0` = off): the
    /// physical footprint on macOS (including Metal buffers), resident set size
    /// elsewhere.
    /// New work is rejected with RESOURCE_EXHAUSTED while usage is above this.
    #[serde(default = "crate::defaults::max_memory")]
    pub max_memory: String,
    /// Max items queued or in-flight before submissions are rejected with
    /// RESOURCE_EXHAUSTED (backpressure). `0` disables the limit.
    #[serde(default = "crate::defaults::max_pending")]
    pub max_pending: u32,
    /// Max times a failed item is retried before it is marked failed. `0` disables
    /// retries.
    #[serde(default = "crate::defaults::max_retries")]
    pub max_retries: u32,
    /// Compute backend for the model workers. `auto` picks CUDA → Metal → CPU.
    #[serde(default)]
    pub device: DevicePreference,
}

/// Which compute backend to run models on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DevicePreference {
    /// The best backend that initializes: CUDA, then Metal, then CPU.
    #[default]
    Auto,
    /// Always the CPU, even when a GPU is available.
    Cpu,
    /// Metal (macOS); falls back to CPU if it cannot be initialized.
    Metal,
    /// CUDA (`cuda` builds); falls back to CPU if it cannot be initialized.
    Cuda,
}

impl AppConfig {
    /// `max_memory` parsed to bytes; `None` means no limit.
    pub fn max_memory_bytes(&self) -> Option<u64> {
        crate::units::parse_size(&self.max_memory)
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            endpoint: defaults::endpoint(),
            port: defaults::port(),
            cache_dir: None,
            max_concurrent: defaults::global_max_concurrent(),
            idle_timeout: defaults::idle_timeout(),
            log_level: defaults::log_level(),
            config_file: None,
            max_memory: defaults::max_memory(),
            max_pending: defaults::max_pending(),
            max_retries: defaults::max_retries(),
            device: DevicePreference::Auto,
        }
    }
}

/// `[webhook]` — outbound delivery target. Omit the section to disable.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookConfig {
    /// gRPC target; scheme selects TLS (`https`) vs plaintext (`http`). Any path
    /// component is ignored — the gRPC method path is fixed by the service.
    pub url: String,
    /// Optional shared secret. When set, each delivery carries an
    /// `x-apollo-webhook-signature` metadata header: lowercase-hex HMAC-SHA256 of
    /// the task id, letting the receiver verify the call came from this server.
    #[serde(default)]
    pub secret: Option<String>,
    /// How often (seconds) the background loop retries deliveries left pending by
    /// a failure. `0` disables periodic redelivery (still retried on restart).
    #[serde(default = "crate::defaults::redelivery_secs")]
    pub redelivery_secs: u32,
}

/// Safety limits applied when fetching remote inputs (SSRF and resource guards).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    /// Max bytes downloaded for a single input (e.g. `512mb`; `0` = unlimited).
    pub max_download: String,
    /// Reject videos longer than this many seconds (`0` = unlimited).
    pub max_video_seconds: u32,
    /// Reject hosts resolving to private / loopback / link-local addresses.
    pub block_private_ips: bool,
    /// URL schemes permitted for remote fetches.
    pub allowed_schemes: Vec<String>,
    /// Allow `file://` and bare local paths as inputs. Off by default; even when
    /// on, a path must resolve inside one of `local_roots`.
    pub allow_local_files: bool,
    /// Directories a local-file input may resolve inside (after canonicalization,
    /// so `..`/symlinks cannot escape). Empty means no local path is permitted.
    pub local_roots: Vec<String>,
    /// Max decoded pixels (`width * height`) for an image, guarding against
    /// decompression bombs. `0` = unlimited.
    pub max_pixels: u64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_download: defaults::max_download(),
            max_video_seconds: defaults::max_video_seconds(),
            block_private_ips: defaults::block_private_ips(),
            allowed_schemes: defaults::allowed_schemes(),
            allow_local_files: defaults::allow_local_files(),
            local_roots: defaults::local_roots(),
            max_pixels: defaults::max_pixels(),
        }
    }
}

impl LimitsConfig {
    /// `max_download` parsed to bytes; `None` means unlimited.
    pub fn max_download_bytes(&self) -> Option<u64> {
        crate::units::parse_size(&self.max_download)
    }
}

/// `[database]` — persistence backend selection plus per-backend config.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    #[serde(default)]
    pub backend: Backend,
    /// How long to keep finished tasks (e.g. "30d"); `None` keeps forever.
    #[serde(default)]
    pub retention: Option<String>,
    #[serde(default)]
    pub sqlite: Option<SqliteConfig>,
    #[serde(default)]
    pub postgres: Option<PostgresConfig>,
    #[serde(default)]
    pub surrealdb: Option<SurrealdbConfig>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            backend: Backend::Sqlite,
            retention: None,
            sqlite: None,
            postgres: None,
            surrealdb: None,
        }
    }
}

/// Database backend. New backends are added here and in `apollo-storage`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum Backend {
    #[default]
    Sqlite,
    Postgres,
    Surrealdb,
}

/// `[database.sqlite]`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteConfig {
    #[serde(default = "crate::defaults::sqlite_path")]
    pub path: String,
    #[serde(default = "crate::defaults::wal")]
    pub wal: bool,
    #[serde(default = "crate::defaults::busy_timeout")]
    pub busy_timeout: u32,
    #[serde(default = "crate::defaults::sqlite_max_connections")]
    pub max_connections: u32,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: defaults::sqlite_path(),
            wal: defaults::wal(),
            busy_timeout: defaults::busy_timeout(),
            max_connections: defaults::sqlite_max_connections(),
        }
    }
}

/// `[database.postgres]` — future backend.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    pub host: String,
    #[serde(default = "crate::defaults::pg_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: Option<String>,
    pub dbname: String,
    #[serde(default)]
    pub sslmode: Option<String>,
    #[serde(default = "crate::defaults::pg_max_connections")]
    pub max_connections: u32,
}

/// `[database.surrealdb]` — remote SurrealDB backend.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SurrealdbConfig {
    pub url: String,
    pub namespace: String,
    pub database: String,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

/// `[strategies.<name>]` — how an image-classifier is applied to a video.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyConfig {
    /// How per-frame scores roll up into one video result.
    #[serde(default = "crate::defaults::aggregation")]
    pub aggregation: Aggregation,
    /// Whether this strategy stops as soon as a model's trigger fires. Requires
    /// the model to define `early_exit.labels`, else it has no effect.
    #[serde(default)]
    pub early_exit: bool,
    /// Don't trust a single frame: one reaching the model's `early_exit`
    /// threshold only counts as a trigger if at least this many *other* frames
    /// within `confirm_window` seconds of it reach it too (frames are sampled
    /// there when the plan has none). An unconfirmed frame doesn't stop the scan
    /// and doesn't flag the video. `0` (the default) trusts single frames.
    #[serde(default)]
    pub confirm: u32,
    /// Seconds either side of a triggering frame that `confirm` looks in.
    #[serde(default = "crate::defaults::confirm_window")]
    pub confirm_window: f64,
    /// Most frames the sampling (and `refine`) steps may classify per video,
    /// for a predictable worst case. Steps take frames in order; the step that
    /// does not fit is thinned (its strongest scene cuts, or an even spread),
    /// and `refine` gets what is left. Frames sampled to `confirm` a trigger
    /// do not count. Unset: no limit beyond the built-in safety cap.
    #[serde(default)]
    pub max_frames: Option<u32>,
    /// Ordered sampling methods (run in `step` order). Must be non-empty.
    #[serde(default)]
    pub sampling: Vec<SamplingStep>,
}

/// Aggregation across classified frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Aggregation {
    Max,
    #[serde(alias = "average")]
    Mean,
}

/// One sampling step: a method plus its parameters. The required parameter
/// depends on `method` (validated in [`crate::validate`]).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SamplingStep {
    /// Execution order; lower runs first. Unique within a strategy.
    pub step: u32,
    pub method: SamplingKind,
    /// Frames per second (`method = "fps"`, and the density around each
    /// frame `method = "refine"` looks closer at).
    #[serde(default)]
    pub fps: Option<f64>,
    /// Total frames evenly spaced (`method = "uniform"`).
    #[serde(default)]
    pub count: Option<u32>,
    /// Take every Nth frame (`method = "every_nth"`).
    #[serde(default)]
    pub nth: Option<u32>,
    /// Scene-change score threshold 0..1 (`method = "scene"`).
    #[serde(default)]
    pub threshold: Option<f64>,
    /// Minimum seconds between scene frames (`method = "scene"`): a cut closer
    /// than this to the previously kept cut is skipped. Caps the cost on
    /// fast-cut video. Default 0 (keep every cut).
    #[serde(default)]
    pub min_gap: Option<f64>,
    /// Lowest trigger-label score that counts as ambiguous (`method = "refine"`):
    /// frames scoring from here up to the model's `early_exit` threshold get a
    /// closer look.
    #[serde(default)]
    pub min_score: Option<f32>,
    /// Seconds either side of an ambiguous frame to sample (`method = "refine"`).
    #[serde(default)]
    pub window: Option<f64>,
}

/// Frame-sampling method. New methods are added here and in `apollo-media`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SamplingKind {
    Iframes,
    Fps,
    Uniform,
    EveryNth,
    Scene,
    /// Look closer: after every other step has run, sample densely around the
    /// frames whose trigger-label score was ambiguous. Planned from the scores,
    /// so it must come after all other steps.
    Refine,
}

/// `[models.<label>]` — a registered model.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    pub architecture: Architecture,
    /// Hugging Face repo. Required for `vit` and `siglip`; `pdq` has none.
    #[serde(default)]
    pub repo: String,
    #[serde(default = "crate::defaults::revision")]
    pub revision: String,
    #[serde(default = "crate::defaults::enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub keep_in_memory: bool,
    /// GPU batch size for this model's worker.
    #[serde(default = "crate::defaults::model_max_concurrent")]
    pub max_concurrent: u32,
    /// Max processing seconds per (input, model); queue wait excluded.
    #[serde(default = "crate::defaults::timeout")]
    pub timeout: u32,
    /// Scheduling priority: higher is admitted from the queue ahead of
    /// earlier-submitted lower-priority work. Defaults to 0; may be negative.
    #[serde(default)]
    pub priority: i32,
    /// Opts an image-classifier into video input via the named strategy.
    #[serde(default)]
    pub video_strategy: Option<String>,
    /// Early-exit trigger for video scans (model-specific labels + threshold).
    #[serde(default)]
    pub early_exit: Option<EarlyExit>,
    /// Candidate labels for open-vocabulary architectures (siglip). Each is
    /// embedded via the text tower and scored against the image; ignored by
    /// fixed-head architectures (vit) that get their labels from the weights.
    #[serde(default)]
    pub labels: Vec<String>,
    /// Optional prompt template wrapping each label before encoding, e.g.
    /// `"a photo of a {}"` (a `{}` placeholder is substituted; otherwise the
    /// template is prefixed). The bare label is what's returned in results.
    #[serde(default)]
    pub prompt_template: Option<String>,
    /// Keep labels scoring at or above this (siglip sigmoid probability). For a
    /// video frame scan, set this low so true peaks survive into the temporal
    /// pool. Defaults to 0.5 when unset.
    #[serde(default)]
    pub score_threshold: Option<f32>,
    /// Cap on the number of labels returned (highest-scoring first). `None`
    /// returns every label above the threshold.
    #[serde(default)]
    pub max_results: Option<usize>,
    /// Path to a taxonomy file (TOML) defining grouped, prompt-backed categories
    /// for a siglip model. Relative paths resolve from the config file's
    /// directory. Mutually exclusive with `labels`.
    #[serde(default)]
    pub taxonomy_file: Option<String>,
    /// `pdq`: the block list, one `<hash> <category id>` per line (`#`
    /// comments allowed). Relative paths resolve from the config file's
    /// directory. Re-read when the model reloads.
    #[serde(default)]
    pub hash_list: Option<String>,
    /// `pdq`: category names by id, e.g. `categories = { 1 = "csam" }`. The
    /// ids are what the list's entries carry and predictions report.
    #[serde(default)]
    pub categories: BTreeMap<String, String>,
    /// `pdq`: the most bits (of 256) an image's hash may differ from a listed
    /// one and still match. Defaults to 31.
    #[serde(default)]
    pub max_distance: Option<u32>,
    /// `pdq`: images whose hash quality (0–100) is below this never match;
    /// flat or nearly blank images hash unreliably. Defaults to 50.
    #[serde(default)]
    pub min_quality: Option<u32>,
    /// `pdq`: also match rotated (90°, 180°, 270°) and mirrored copies, by
    /// comparing all 8 of an image's dihedral hashes. Defaults to true; it
    /// also makes matching independent of EXIF orientation.
    #[serde(default)]
    pub match_rotations: Option<bool>,
}

/// `[models.<label>.early_exit]` — what counts as a trigger for this model.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EarlyExit {
    /// Stop the scan when any of these category ids crosses `threshold` on a
    /// frame (a class index for vit; a label index or taxonomy child id for
    /// siglip).
    pub labels: Vec<u32>,
    #[serde(default = "crate::defaults::early_exit_threshold")]
    pub threshold: f32,
}

/// `[pipelines.<name>]` — an ordered, gated sequence of models for one input.
/// Steps run in ascending `order`, each step's models concurrently; if a step's
/// optional `stop_if` trigger fires on its gating model's output, the remaining
/// steps are skipped and the task completes normally (firing the task webhook).
/// A step *failure* (inference error) instead fails the whole pipeline, routing
/// the item through the normal retry + dead-letter path.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub steps: Vec<PipelineStep>,
}

impl PipelineConfig {
    /// The steps in run order (ascending `order`).
    pub fn ordered_steps(&self) -> Vec<&PipelineStep> {
        let mut steps: Vec<&PipelineStep> = self.steps.iter().collect();
        steps.sort_by_key(|s| s.order);
        steps
    }

    /// Every model of the pipeline, in run order.
    pub fn models(&self) -> Vec<String> {
        self.ordered_steps()
            .into_iter()
            .flat_map(|s| s.models().iter().cloned())
            .collect()
    }
}

/// One step of a pipeline: its model(s), its position in the order, and an
/// optional gate evaluated once the step finishes.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineStep {
    /// The model label to run, or a list of them to run concurrently (each
    /// must be defined under `[models.*]`).
    pub model: StepModels,
    /// Execution position; steps run in ascending `order`, unique per pipeline.
    pub order: u32,
    /// If set and the gating model's output triggers it (any listed id at or
    /// above the threshold), the pipeline early-exits and later steps are
    /// skipped. Independent of a model's own `[models.<l>.early_exit]` (which
    /// governs video frame scans).
    #[serde(default)]
    pub stop_if: Option<StopIf>,
}

impl PipelineStep {
    /// The step's models, in the order written.
    pub fn models(&self) -> &[String] {
        self.model.names()
    }

    /// The model whose output `stop_if` checks: `stop_if.model`, or the step's
    /// only model. `None` without a gate, or when a multi-model step's gate
    /// names none (rejected by validation).
    pub fn gate_model(&self) -> Option<&str> {
        let stop = self.stop_if.as_ref()?;
        match (&stop.model, self.models()) {
            (Some(m), _) => Some(m),
            (None, [only]) => Some(only),
            (None, _) => None,
        }
    }
}

/// A step's `model`: one name or a list.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum StepModels {
    One(String),
    Many(Vec<String>),
}

impl StepModels {
    pub fn names(&self) -> &[String] {
        match self {
            StepModels::One(name) => std::slice::from_ref(name),
            StepModels::Many(names) => names,
        }
    }
}

/// A pipeline step's gate: stop when `model`'s output has any of `labels` at or
/// above `threshold` (on the aggregated result for a video scan).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StopIf {
    /// Whose output is checked; required when the step runs several models.
    #[serde(default)]
    pub model: Option<String>,
    pub labels: Vec<u32>,
    #[serde(default = "crate::defaults::early_exit_threshold")]
    pub threshold: f32,
}
