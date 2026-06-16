//! Input retrieval. Tries `main`, then `fallback`; local paths and `file://` are
//! used in place, `http(s)://` is downloaded to a temp file.
//!
//! Note: local-filesystem inputs are not sandboxed here — restricting which paths
//! a caller may reference is a policy decision for the server/engine layer.

use std::path::{Path, PathBuf};

use apollo_domain::Url;

use crate::error::MediaError;

/// A locally-available media file. If it was downloaded, the temp dir is held
/// open until this value drops.
pub struct LocalMedia {
    path: PathBuf,
    _temp: Option<tempfile::TempDir>,
}

impl LocalMedia {
    /// Path to the file on local disk.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read the whole file into memory (used for images).
    pub fn read_bytes(&self) -> Result<Vec<u8>, MediaError> {
        std::fs::read(&self.path)
            .map_err(|e| MediaError::Io(format!("reading {:?}: {e}", self.path)))
    }
}

/// Resolve a [`Url`], trying `main` then `fallback`.
pub async fn fetch(url: &Url) -> Result<LocalMedia, MediaError> {
    let mut errors = Vec::new();
    match resolve(&url.main).await {
        Ok(m) => return Ok(m),
        Err(e) => errors.push(format!("main <{}>: {e}", url.main)),
    }
    if let Some(fallback) = &url.fallback {
        match resolve(fallback).await {
            Ok(m) => return Ok(m),
            Err(e) => errors.push(format!("fallback <{fallback}>: {e}")),
        }
    }
    Err(MediaError::AllSourcesFailed {
        input: url.main.clone(),
        errors,
    })
}

async fn resolve(src: &str) -> Result<LocalMedia, MediaError> {
    if let Some(rest) = src.strip_prefix("file://") {
        return local(rest);
    }
    if src.starts_with("http://") || src.starts_with("https://") {
        return download(src).await;
    }
    local(src)
}

fn local(path: &str) -> Result<LocalMedia, MediaError> {
    let p = PathBuf::from(path);
    if !p.is_file() {
        return Err(MediaError::NotFound(path.to_string()));
    }
    Ok(LocalMedia { path: p, _temp: None })
}

async fn download(url: &str) -> Result<LocalMedia, MediaError> {
    let resp = reqwest::get(url)
        .await
        .map_err(|e| MediaError::Http(format!("requesting {url}: {e}")))?
        .error_for_status()
        .map_err(|e| MediaError::Http(format!("{url}: {e}")))?;
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| MediaError::Http(format!("reading body of {url}: {e}")))?;

    let dir = tempfile::tempdir().map_err(|e| MediaError::Io(format!("temp dir: {e}")))?;
    let name = filename_hint(url).unwrap_or_else(|| "download".to_string());
    let path = dir.path().join(name);
    std::fs::write(&path, &bytes).map_err(|e| MediaError::Io(format!("writing {:?}: {e}", path)))?;

    Ok(LocalMedia {
        path,
        _temp: Some(dir),
    })
}

/// Best-effort filename (with extension) from a URL, so ffmpeg/image can sniff by
/// extension if useful.
fn filename_hint(url: &str) -> Option<String> {
    let no_query = url.split(['?', '#']).next().unwrap_or(url);
    let last = no_query.rsplit('/').next().unwrap_or("");
    (!last.is_empty()).then(|| last.to_string())
}
