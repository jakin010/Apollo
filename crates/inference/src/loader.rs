//! Hugging Face Hub access (cached) and `config.json` label parsing.

use std::path::{Path, PathBuf};

use hf_hub::api::sync::{Api, ApiBuilder, ApiRepo};
use hf_hub::{Repo, RepoType};

use apollo_config::ModelConfig;

use crate::error::InferenceError;

/// A handle to a model repo on the Hub, backed by the local cache.
pub(crate) struct Hub {
    repo: ApiRepo,
}

impl Hub {
    /// Open the repo named by `cfg` (at its revision), using `cache_dir` if given.
    pub(crate) fn open(
        cfg: &ModelConfig,
        cache_dir: Option<&Path>,
    ) -> Result<Self, InferenceError> {
        let api = match cache_dir {
            Some(dir) => ApiBuilder::new()
                .with_cache_dir(dir.to_path_buf())
                .build()
                .map_err(|e| InferenceError::Hub(e.to_string()))?,
            None => Api::new().map_err(|e| InferenceError::Hub(e.to_string()))?,
        };
        let repo = api.repo(Repo::with_revision(
            cfg.repo.clone(),
            RepoType::Model,
            cfg.revision.clone(),
        ));
        Ok(Self { repo })
    }

    /// Resolve a file in the repo to a local path, downloading if not cached.
    pub(crate) fn file(&self, name: &str) -> Result<PathBuf, InferenceError> {
        self.repo
            .get(name)
            .map_err(|e| InferenceError::Hub(format!("{name}: {e}")))
    }
}

/// Read `id2label` from a model `config.json`, returning labels ordered by index.
pub(crate) fn labels_from_config(bytes: &[u8]) -> Result<Vec<String>, InferenceError> {
    let v: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|e| InferenceError::Config(format!("config.json: {e}")))?;
    let Some(map) = v.get("id2label").and_then(|m| m.as_object()) else {
        return Err(InferenceError::Config("config.json has no id2label".into()));
    };
    let mut labels = vec![String::new(); map.len()];
    for (k, val) in map {
        let idx: usize = k
            .parse()
            .map_err(|_| InferenceError::Config(format!("id2label key '{k}' is not an index")))?;
        if let Some(slot) = labels.get_mut(idx) {
            *slot = val.as_str().unwrap_or_default().to_string();
        }
    }
    Ok(labels)
}
