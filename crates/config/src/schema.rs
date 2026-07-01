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
    pub auth: Option<AuthConfig>,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub database: DatabaseConfig,
    #[serde(default)]
    pub cache: Option<CacheConfig>,
    #[serde(default)]
    pub strategies: BTreeMap<String, StrategyConfig>,
    #[serde(default)]
    pub pipelines: BTreeMap<String, PipelineConfig>,
    #[serde(default)]
    pub models: BTreeMap<String, ModelConfig>,
}

/// Authentication for the `Inference` service. When this section is present,
/// every Inference RPC must carry a valid PASETO v4 token signed by the matching
/// secret key (mint tokens with `apollo token`). Health and reflection stay open.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct AuthConfig {
    /// PASERK-encoded v4 **public** key (`k4.public.…`) used to verify tokens.
    pub public_key: String,
}

/// Result cache (optional). When present and `enabled`, model outputs are cached
/// by content hash (with a url→content-hash hint) so identical inputs skip
/// inference. A bare `[cache]` section turns it on.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct CacheConfig {
    /// Master switch. Defaults to true so `[cache]` alone enables caching.
    #[serde(default = "crate::defaults::cache_enabled")]
    pub enabled: bool,
    /// Freshness window in seconds; entries older than this are ignored (and
    /// eligible for purge). Omit for entries that never expire.
    #[serde(default)]
    pub ttl_secs: Option<u64>,
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
    /// Soft ceiling on resident process memory (e.g. `4gb`, `512mb`; `0` = off).
    /// New work is rejected with RESOURCE_EXHAUSTED while usage is above this.
    #[serde(default = "crate::defaults::max_memory")]
    pub max_memory: String,
    /// Max items queued or in-flight before submissions are rejected with
    /// RESOURCE_EXHAUSTED (backpressure). `0` disables the limit.
    #[serde(default = "crate::defaults::max_pending")]
    pub max_pending: u32,
    /// Max times a failed item is retried before it is marked failed (and reported
    /// to the webhook via the `Webhook.ItemFailed` dead-letter call). `0` disables
    /// retries.
    #[serde(default = "crate::defaults::max_retries")]
    pub max_retries: u32,
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
#[serde(default)]
pub struct LimitsConfig {
    /// Max bytes downloaded for a single input (e.g. `512mb`; `0` = unlimited).
    pub max_download: String,
    /// Reject videos longer than this many seconds (`0` = unlimited).
    pub max_video_seconds: u32,
    /// Reject hosts resolving to private / loopback / link-local addresses.
    pub block_private_ips: bool,
    /// URL schemes permitted for remote fetches.
    pub allowed_schemes: Vec<String>,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_download: defaults::max_download(),
            max_video_seconds: defaults::max_video_seconds(),
            block_private_ips: defaults::block_private_ips(),
            allowed_schemes: defaults::allowed_schemes(),
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
pub enum Backend {
    Sqlite,
    Postgres,
    Surrealdb,
}

impl Default for Backend {
    fn default() -> Self {
        Backend::Sqlite
    }
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

/// `[database.surrealdb]` — future backend.
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
    /// Frames per second (`method = "fps"`).
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
}

/// `[models.<label>]` — a registered model.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    pub architecture: Architecture,
    /// Hugging Face repo (the only required field).
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
/// Steps run in ascending `order`; if a step's optional `stop_if` trigger fires
/// on that model's output, the remaining steps are skipped and the task
/// completes normally (firing the task webhook). A step *failure* (inference
/// error) instead fails the whole pipeline, routing the item through the normal
/// retry + dead-letter path.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub steps: Vec<PipelineStep>,
}

/// One step of a pipeline: a model, its position in the order, and an optional
/// gate condition evaluated on that model's output.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineStep {
    /// The model label to run (must be defined under `[models.*]`).
    pub model: String,
    /// Execution position; steps run in ascending `order`, unique per pipeline.
    pub order: u32,
    /// If set and this model's output triggers it (any listed id at or above the
    /// threshold), the pipeline early-exits and later steps are skipped. Reuses
    /// the `EarlyExit` shape (`labels` + `threshold`) but is independent of a
    /// model's own `[models.<l>.early_exit]` (which governs video frame scans).
    #[serde(default)]
    pub stop_if: Option<EarlyExit>,
}
