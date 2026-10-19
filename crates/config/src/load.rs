//! Config discovery, parsing, and run-only overrides.
//!
//! Resolution order for *locating* the file: the `--config` flag, else the
//! built-in [`default_path`]. `[app].config_file` records the canonical path
//! inside a loaded config (used for writes/reloads); it cannot locate the file
//! initially, since a file can't name the path used to find it.

use std::path::{Path, PathBuf};

use crate::error::ConfigError;
use crate::schema::Config;

/// Built-in default config path: the per-user application config directory
/// (`~/.config/apollo/config.toml` on Linux, `~/Library/Application
/// Support/apollo/config.toml` on macOS, `%APPDATA%\apollo\config.toml` on
/// Windows). Falls back to `./apollo/config.toml` when no home is known.
/// Override with `--config`.
pub fn default_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("apollo")
        .join("config.toml")
}

/// Parse a config from TOML text (no path resolution).
pub fn from_str(text: &str) -> Result<Config, ConfigError> {
    toml::from_str::<Config>(text).map_err(|e| ConfigError::Parse(e.to_string()))
}

/// Read and parse a config from a specific file.
pub fn load_from(path: &Path) -> Result<Config, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|e| ConfigError::Io(path.to_path_buf(), e))?;
    let mut config = from_str(&text)?;
    if let Some(base) = path.parent() {
        config.resolve_paths(base);
    }
    Ok(config)
}

/// Resolve the config path (CLI flag wins, else the built-in default) and load it.
pub fn load(cli_config: Option<&Path>) -> Result<Config, ConfigError> {
    let path = cli_config
        .map(Path::to_path_buf)
        .unwrap_or_else(default_path);
    load_from(&path)
}

/// Run-only overrides supplied on the `start` command line. These do not persist
/// to the file (that is what `config set` is for). `webhook_url` replaces
/// `[webhook].url`, or enables delivery to that URL when the section is absent.
#[derive(Debug, Default, Clone)]
pub struct Overrides {
    pub endpoint: Option<String>,
    pub port: Option<u16>,
    pub webhook_url: Option<String>,
}

impl Config {
    /// Resolve relative file paths (model `taxonomy_file` and `hash_list`, the `[tls]` files, and
    /// `[auth].revocation_file`) against `base` (the directory of the config
    /// file), so downstream consumers can open them directly.
    fn resolve_paths(&mut self, base: &Path) {
        let resolve = |path: &mut String| {
            if Path::new(path.as_str()).is_relative() {
                *path = base.join(path.as_str()).to_string_lossy().into_owned();
            }
        };
        for model in self.models.values_mut() {
            if let Some(tf) = model.taxonomy_file.as_mut() {
                resolve(tf);
            }
            if let Some(list) = model.hash_list.as_mut() {
                resolve(list);
            }
        }
        if let Some(tls) = self.tls.as_mut() {
            resolve(&mut tls.cert_file);
            resolve(&mut tls.key_file);
            if let Some(ca) = tls.client_ca_file.as_mut() {
                resolve(ca);
            }
        }
        if let Some(file) = self.auth.revocation_file.as_mut() {
            resolve(file);
        }
    }

    /// Apply run-only overrides in place.
    pub fn apply_overrides(&mut self, o: &Overrides) {
        if let Some(e) = &o.endpoint {
            self.app.endpoint = e.clone();
        }
        if let Some(p) = o.port {
            self.app.port = p;
        }
        if let Some(u) = &o.webhook_url {
            match self.webhook.as_mut() {
                Some(w) => w.url = u.clone(),
                // No [webhook] section: the flag alone enables delivery for this
                // run (unsigned, default redelivery interval).
                None => {
                    self.webhook = Some(crate::schema::WebhookConfig {
                        url: u.clone(),
                        secret: None,
                        redelivery_secs: crate::defaults::redelivery_secs(),
                    })
                }
            }
        }
    }
}
