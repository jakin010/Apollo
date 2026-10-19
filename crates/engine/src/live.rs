//! The reloadable part of the engine, and [`Engine::reload`].
//!
//! A reload validates the new config, keeps the model workers whose weights
//! do not depend on anything that changed (see `registry::LoadKey`), and swaps
//! in a new [`Live`] snapshot. Tasks already running keep the snapshot they
//! started with, so they finish on one config, and a removed model's worker
//! exits once they are done. Settings that only take effect at startup are
//! kept at their running values and reported, so the config the engine shows
//! is always the one in effect.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use apollo_config::Config;
use apollo_media::FetchLimits;

use crate::registry::{Registry, RegistryChanges, WorkerEnv};
use crate::{Engine, EngineError, WebhookSink};

/// One config and everything derived from it.
pub(crate) struct Live {
    pub(crate) config: Arc<Config>,
    pub(crate) registry: Registry,
    pub(crate) webhook: Option<Arc<dyn WebhookSink>>,
    /// SSRF / resource limits applied to remote input fetches.
    pub(crate) fetch_limits: FetchLimits,
    /// Max decoded pixels (`width * height`) per image; `None` disables the cap.
    pub(crate) max_pixels: Option<u64>,
    /// Soft memory cap in bytes (see [`crate::memory`]); `None` disables it.
    pub(crate) max_memory_bytes: Option<u64>,
    /// Max items queued or in flight before submissions are rejected; `0` = off.
    pub(crate) max_pending: usize,
    /// 1 for the startup config, then one more per reload.
    pub(crate) generation: u64,
    pub(crate) loaded_at: SystemTime,
}

impl Live {
    pub(crate) fn new(
        config: Arc<Config>,
        registry: Registry,
        webhook: Option<Arc<dyn WebhookSink>>,
        generation: u64,
    ) -> Self {
        let limits = &config.limits;
        let fetch_limits = FetchLimits {
            allowed_schemes: limits.allowed_schemes.clone(),
            block_private_ips: limits.block_private_ips,
            max_download_bytes: limits.max_download_bytes(),
            allow_local_files: limits.allow_local_files,
            local_roots: limits.local_roots.iter().map(PathBuf::from).collect(),
        };
        Self {
            fetch_limits,
            max_pixels: (limits.max_pixels != 0).then_some(limits.max_pixels),
            max_memory_bytes: config.app.max_memory_bytes(),
            max_pending: config.app.max_pending as usize,
            registry,
            webhook,
            generation,
            loaded_at: SystemTime::now(),
            config,
        }
    }
}

/// `[app].idle_timeout` in milliseconds.
pub(crate) fn idle_ms(config: &Config) -> u64 {
    u64::from(config.app.idle_timeout) * 1000
}

/// What a reload did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadReport {
    /// The new config's generation (the startup config is 1).
    pub generation: u64,
    pub models: RegistryChanges,
    /// Settings that changed but only take effect after a restart (they keep
    /// their running values until then), e.g. `app.port`.
    pub restart_required: Vec<String>,
}

impl Engine {
    /// Apply `config` (with `webhook`, the sink built for its `[webhook]`
    /// section) to the running engine. The config is validated first; an
    /// invalid one is rejected and the running config stays. Reloads are
    /// serialized.
    pub async fn reload(
        &self,
        config: Config,
        webhook: Option<Arc<dyn WebhookSink>>,
    ) -> Result<ReloadReport, EngineError> {
        config
            .validate()
            .map_err(|e| EngineError::Config(e.to_string()))?;
        if !config.has_models() {
            return Err(EngineError::Config(
                "no models configured — nothing to serve".into(),
            ));
        }
        let _serialized = self.inner.reloading.lock().await;
        let current = Arc::clone(&self.inner.live.read().unwrap());
        let (config, restart_required) = keep_restart_only(&current.config, config);

        self.inner
            .idle_ms
            .store(idle_ms(&config), Ordering::Relaxed);
        let env = WorkerEnv {
            kind: self.inner.device,
            cache_dir: self.inner.cache_dir.as_ref(),
            idle_ms: &self.inner.idle_ms,
            metrics: &self.inner.metrics,
        };
        let (registry, models) = Registry::build(&config, &env, Some(&current.registry));
        let generation = current.generation + 1;
        let live = Live::new(Arc::new(config), registry, webhook, generation);
        *self.inner.live.write().unwrap() = Arc::new(live);

        tracing::info!(
            generation,
            added = ?models.added,
            reloaded = ?models.reloaded,
            removed = ?models.removed,
            "config reloaded"
        );
        if !restart_required.is_empty() {
            tracing::warn!(
                settings = ?restart_required,
                "changed settings take effect after a restart"
            );
        }
        Ok(ReloadReport {
            generation,
            models,
            restart_required,
        })
    }

    /// The running config's generation and when it was applied.
    pub fn config_generation(&self) -> (u64, SystemTime) {
        let live = self.live();
        (live.generation, live.loaded_at)
    }
}

/// `new` with every restart-only setting set back to its value in `running`,
/// and the names of those that differed.
fn keep_restart_only(running: &Config, mut new: Config) -> (Config, Vec<String>) {
    let mut changed = Vec::new();
    macro_rules! keep {
        ($($field:ident).+, $name:expr) => {
            if new.$($field).+ != running.$($field).+ {
                changed.push($name.to_string());
                new.$($field).+ = running.$($field).+.clone();
            }
        };
    }
    keep!(app.endpoint, "app.endpoint");
    keep!(app.port, "app.port");
    keep!(app.device, "app.device");
    keep!(app.cache_dir, "app.cache_dir");
    keep!(app.max_concurrent, "app.max_concurrent");
    keep!(app.config_file, "app.config_file");
    keep!(tls, "tls");
    keep!(auth, "auth");
    keep!(database.backend, "database.backend");
    keep!(database.sqlite, "database.sqlite");
    keep!(database.surrealdb, "database.surrealdb");
    keep!(database.postgres, "database.postgres");
    keep!(metrics, "metrics");
    (new, changed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_only_settings_keep_their_running_values() {
        let running: Config = apollo_config::load::from_str("[app]\nport = 8080\n").unwrap();
        let mut new = running.clone();
        new.app.port = 9090;
        new.app.max_retries = running.app.max_retries + 1;
        new.app.max_concurrent = 99;
        let (applied, changed) = keep_restart_only(&running, new);
        assert_eq!(changed, ["app.port", "app.max_concurrent"]);
        assert_eq!(applied.app.port, 8080);
        assert_eq!(applied.app.max_concurrent, running.app.max_concurrent);
        // Live settings are applied.
        assert_eq!(applied.app.max_retries, running.app.max_retries + 1);
    }
}
