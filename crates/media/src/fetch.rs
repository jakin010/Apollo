//! Input retrieval. Tries `main`, then `fallback`; local paths and `file://` are
//! used in place, `http(s)://` is downloaded to a temp file under the configured
//! [`FetchLimits`] (allowed schemes, a private-address / SSRF guard, and a byte
//! cap).
//!
//! Note: local-filesystem inputs are not sandboxed here — restricting which paths
//! a caller may reference is a policy decision for the server/engine layer.

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

use apollo_domain::Url;

use crate::error::MediaError;

/// Limits applied to remote (`http(s)://`) fetches.
#[derive(Debug, Clone)]
pub struct FetchLimits {
    /// URL schemes permitted for remote fetches (e.g. `http`, `https`).
    pub allowed_schemes: Vec<String>,
    /// Reject hosts that resolve to private / loopback / link-local addresses,
    /// and pin the connection to a vetted public address (anti-rebinding).
    pub block_private_ips: bool,
    /// Hard cap on downloaded bytes; the download aborts once exceeded. `None`
    /// means unlimited.
    pub max_download_bytes: Option<u64>,
}

impl Default for FetchLimits {
    fn default() -> Self {
        FetchLimits {
            allowed_schemes: vec!["http".to_string(), "https".to_string()],
            block_private_ips: true,
            max_download_bytes: Some(512 * 1024 * 1024),
        }
    }
}

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

    /// Adopt an existing local file (e.g. a `ClassifyStream` upload) without
    /// fetching. Not owned by a temp dir, so it is never removed on drop — the
    /// caller owns its lifecycle.
    pub fn adopt(path: PathBuf) -> Self {
        Self { path, _temp: None }
    }

    /// Read the whole file into memory (used for images).
    pub fn read_bytes(&self) -> Result<Vec<u8>, MediaError> {
        std::fs::read(&self.path)
            .map_err(|e| MediaError::Io(format!("reading {:?}: {e}", self.path)))
    }
}

/// Resolve a [`Url`], trying `main` then `fallback`, subject to `limits`.
pub async fn fetch(url: &Url, limits: &FetchLimits) -> Result<LocalMedia, MediaError> {
    let mut errors = Vec::new();
    match resolve(&url.main, limits).await {
        Ok(m) => return Ok(m),
        Err(e) => errors.push(format!("main <{}>: {e}", url.main)),
    }
    if let Some(fallback) = &url.fallback {
        match resolve(fallback, limits).await {
            Ok(m) => return Ok(m),
            Err(e) => errors.push(format!("fallback <{fallback}>: {e}")),
        }
    }
    Err(MediaError::AllSourcesFailed {
        input: url.main.clone(),
        errors,
    })
}

async fn resolve(src: &str, limits: &FetchLimits) -> Result<LocalMedia, MediaError> {
    if let Some(rest) = src.strip_prefix("file://") {
        return local(rest);
    }
    if let Some((scheme, _)) = src.split_once("://") {
        // A URL with an explicit scheme: it must be permitted, then downloaded.
        if !limits
            .allowed_schemes
            .iter()
            .any(|s| s.eq_ignore_ascii_case(scheme))
        {
            return Err(MediaError::Http(format!("scheme '{scheme}' is not allowed")));
        }
        return download(src, limits).await;
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

async fn download(url_str: &str, limits: &FetchLimits) -> Result<LocalMedia, MediaError> {
    let url = reqwest::Url::parse(url_str)
        .map_err(|e| MediaError::Http(format!("invalid URL {url_str}: {e}")))?;
    let host = url
        .host_str()
        .ok_or_else(|| MediaError::Http(format!("URL has no host: {url_str}")))?
        .to_string();

    // Redirects are disabled so a 30x cannot bounce us to an internal address.
    let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
    if limits.block_private_ips {
        let port = url.port_or_known_default().unwrap_or(443);
        let addr = vet_host(&host, port).await?;
        // Pin the host to the vetted address (defeats DNS rebinding to internal IPs).
        builder = builder.resolve(&host, addr);
    }
    let client = builder
        .build()
        .map_err(|e| MediaError::Http(format!("building http client: {e}")))?;

    let mut resp = client
        .get(url.clone())
        .send()
        .await
        .map_err(|e| MediaError::Http(format!("requesting {url_str}: {e}")))?
        .error_for_status()
        .map_err(|e| MediaError::Http(format!("{url_str}: {e}")))?;

    if let (Some(max), Some(len)) = (limits.max_download_bytes, resp.content_length()) {
        if len > max {
            return Err(MediaError::Http(format!(
                "{url_str}: content-length {len} exceeds limit of {max} bytes"
            )));
        }
    }

    let mut bytes: Vec<u8> = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| MediaError::Http(format!("reading body of {url_str}: {e}")))?
    {
        if let Some(max) = limits.max_download_bytes {
            if bytes.len() as u64 + chunk.len() as u64 > max {
                return Err(MediaError::Http(format!(
                    "{url_str}: download exceeds limit of {max} bytes"
                )));
            }
        }
        bytes.extend_from_slice(&chunk);
    }

    let dir = tempfile::tempdir().map_err(|e| MediaError::Io(format!("temp dir: {e}")))?;
    let name = filename_hint(url_str).unwrap_or_else(|| "download".to_string());
    let path = dir.path().join(name);
    std::fs::write(&path, &bytes).map_err(|e| MediaError::Io(format!("writing {:?}: {e}", path)))?;
    Ok(LocalMedia {
        path,
        _temp: Some(dir),
    })
}

/// Resolve `host`, rejecting any private/loopback/link-local result, and return a
/// vetted public socket address to pin the connection to.
async fn vet_host(host: &str, port: u16) -> Result<SocketAddr, MediaError> {
    if host.eq_ignore_ascii_case("localhost") {
        return Err(MediaError::Http("refusing to fetch from 'localhost'".into()));
    }
    let mut chosen: Option<SocketAddr> = None;
    let addrs = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| MediaError::Http(format!("resolving {host}: {e}")))?;
    for addr in addrs {
        if is_blocked_ip(addr.ip()) {
            return Err(MediaError::Http(format!(
                "refusing to fetch from non-public address {} ({host})",
                addr.ip()
            )));
        }
        chosen.get_or_insert(addr);
    }
    chosen.ok_or_else(|| MediaError::Http(format!("{host} did not resolve to any address")))
}

/// Whether an address is private, loopback, link-local, or otherwise non-public.
fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.octets()[0] == 0
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 0x40) // 100.64/10 CGNAT
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // fc00::/7 unique-local
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
                || v6
                    .to_ipv4_mapped()
                    .map_or(false, |m| is_blocked_ip(IpAddr::V4(m)))
        }
    }
}

/// Best-effort filename (with extension) from a URL, so ffmpeg/image can sniff by
/// extension if useful.
fn filename_hint(url: &str) -> Option<String> {
    let no_query = url.split(['?', '#']).next().unwrap_or(url);
    let last = no_query.rsplit('/').next().unwrap_or("");
    (!last.is_empty()).then(|| last.to_string())
}
