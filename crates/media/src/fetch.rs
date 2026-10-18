//! Input retrieval. Tries `main`, then `fallback`; local paths and `file://` are
//! used in place, `http(s)://` is downloaded to a temp file under the configured
//! [`FetchLimits`] (allowed schemes, a private-address / SSRF guard, and a byte
//! cap). Downloads stream straight to disk, so the byte cap bounds disk use, not
//! memory.
//!
//! Local-filesystem inputs are gated by [`FetchLimits::allow_local_files`] and
//! must canonicalize inside one of [`FetchLimits::local_roots`].

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt;

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
    /// Allow `file://` and bare local-path inputs. When `false`, both are refused
    /// (remote `http(s)` fetches are unaffected).
    pub allow_local_files: bool,
    /// Directories a local path must canonicalize inside to be accepted (resolving
    /// `..` and symlinks). Empty means no local path is permitted, even when
    /// `allow_local_files` is set.
    pub local_roots: Vec<PathBuf>,
}

impl Default for FetchLimits {
    fn default() -> Self {
        FetchLimits {
            allowed_schemes: vec!["http".to_string(), "https".to_string()],
            block_private_ips: true,
            max_download_bytes: Some(512 * 1024 * 1024),
            allow_local_files: false,
            local_roots: Vec::new(),
        }
    }
}

/// A locally-available media file. If it was downloaded, the temp dir is held
/// open until this value drops.
pub struct LocalMedia {
    path: PathBuf,
    used_fallback: bool,
    _temp: Option<tempfile::TempDir>,
}

impl LocalMedia {
    /// Path to the file on local disk.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether this content came from the `fallback` source rather than `main`.
    pub fn used_fallback(&self) -> bool {
        self.used_fallback
    }

    /// Adopt an existing local file (e.g. a `ClassifyStream` upload) without
    /// fetching. Not owned by a temp dir, so it is never removed on drop — the
    /// caller owns its lifecycle.
    pub fn adopt(path: PathBuf) -> Self {
        Self {
            path,
            used_fallback: false,
            _temp: None,
        }
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
    let mut transient = false;
    match resolve(&url.main, limits).await {
        Ok(m) => return Ok(m),
        Err(e) => {
            transient |= e.is_transient();
            errors.push(format!("main <{}>: {e}", url.main));
        }
    }
    if let Some(fallback) = &url.fallback {
        match resolve(fallback, limits).await {
            Ok(mut m) => {
                m.used_fallback = true;
                return Ok(m);
            }
            Err(e) => {
                transient |= e.is_transient();
                errors.push(format!("fallback <{fallback}>: {e}"));
            }
        }
    }
    Err(MediaError::AllSourcesFailed {
        input: url.main.clone(),
        errors,
        transient,
    })
}

async fn resolve(src: &str, limits: &FetchLimits) -> Result<LocalMedia, MediaError> {
    if let Some(rest) = src.strip_prefix("file://") {
        return local(rest, limits);
    }
    if let Some((scheme, _)) = src.split_once("://") {
        // A URL with an explicit scheme: it must be permitted, then downloaded.
        if !limits
            .allowed_schemes
            .iter()
            .any(|s| s.eq_ignore_ascii_case(scheme))
        {
            return Err(MediaError::Refused(format!(
                "scheme '{scheme}' is not allowed"
            )));
        }
        return download(src, limits).await;
    }
    local(src, limits)
}

/// Resolve a local-path input under the local-file policy: access is off unless
/// `allow_local_files` is set, and even then the path must canonicalize (resolving
/// `..` and symlinks) inside one of `local_roots`. This is the gate that keeps an
/// untrusted request from reading arbitrary files (e.g. `file:///etc/passwd`).
///
/// A missing file and a file outside the roots produce the same error, so a
/// caller cannot use the error text to probe which paths exist on the server.
fn local(path: &str, limits: &FetchLimits) -> Result<LocalMedia, MediaError> {
    if !limits.allow_local_files {
        return Err(MediaError::Refused(
            "local file inputs are disabled (enable limits.allow_local_files and set limits.local_roots)"
                .into(),
        ));
    }
    let denied = || {
        MediaError::Refused(format!(
            "local path {path:?} does not exist or is outside the permitted roots"
        ))
    };
    let canonical = std::fs::canonicalize(path).map_err(|_| denied())?;
    let inside_root = limits.local_roots.iter().any(|root| {
        std::fs::canonicalize(root)
            .map(|r| canonical.starts_with(&r))
            .unwrap_or(false)
    });
    if !inside_root || !canonical.is_file() {
        return Err(denied());
    }
    Ok(LocalMedia {
        path: canonical,
        used_fallback: false,
        _temp: None,
    })
}

async fn download(url_str: &str, limits: &FetchLimits) -> Result<LocalMedia, MediaError> {
    let url = reqwest::Url::parse(url_str)
        .map_err(|e| MediaError::Refused(format!("invalid URL {url_str}: {e}")))?;
    let host = url
        .host_str()
        .ok_or_else(|| MediaError::Refused(format!("URL has no host: {url_str}")))?
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
        .map_err(|e| MediaError::Http(format!("requesting {url_str}: {e}")))?;

    let status = resp.status();
    if !status.is_success() {
        let msg = format!("{url_str}: HTTP {status}");
        // 5xx, 408 and 429 may succeed later; anything else (404, 403, a 30x we
        // refuse to follow, ...) will not.
        let retryable = status.is_server_error()
            || status == reqwest::StatusCode::REQUEST_TIMEOUT
            || status == reqwest::StatusCode::TOO_MANY_REQUESTS;
        return Err(if retryable {
            MediaError::Http(msg)
        } else {
            MediaError::Refused(msg)
        });
    }

    if let (Some(max), Some(len)) = (limits.max_download_bytes, resp.content_length())
        && len > max
    {
        return Err(MediaError::Refused(format!(
            "{url_str}: content-length {len} exceeds limit of {max} bytes"
        )));
    }

    // Stream the body to disk so a large download never sits in memory. The
    // file name is fixed and only carries an allow-listed media extension: the
    // URL's own name could otherwise steer ffmpeg's format detection (e.g. an
    // `.m3u8` playlist) or contain path separators.
    let dir = tempfile::tempdir().map_err(|e| MediaError::Io(format!("temp dir: {e}")))?;
    let path = dir.path().join(safe_file_name(url.path()));
    let mut file = tokio::fs::File::create(&path)
        .await
        .map_err(|e| MediaError::Io(format!("creating {:?}: {e}", path)))?;
    let mut written: u64 = 0;
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| MediaError::Http(format!("reading body of {url_str}: {e}")))?
    {
        written += chunk.len() as u64;
        if let Some(max) = limits.max_download_bytes
            && written > max
        {
            return Err(MediaError::Refused(format!(
                "{url_str}: download exceeds limit of {max} bytes"
            )));
        }
        file.write_all(&chunk)
            .await
            .map_err(|e| MediaError::Io(format!("writing {:?}: {e}", path)))?;
    }
    file.flush()
        .await
        .map_err(|e| MediaError::Io(format!("flushing {:?}: {e}", path)))?;

    Ok(LocalMedia {
        path,
        used_fallback: false,
        _temp: Some(dir),
    })
}

/// Media extensions a downloaded file may keep (as a decoding hint). Anything
/// else — notably playlist / concat formats such as `m3u8` or `ffconcat` — is
/// dropped.
const SAFE_EXTENSIONS: &[&str] = &[
    "3gp", "avi", "bmp", "flv", "gif", "jpeg", "jpg", "m4v", "mkv", "mov", "mp4", "mpeg", "mpg",
    "png", "tif", "tiff", "ts", "webm", "webp", "wmv",
];

/// The local file name for a download: always `input`, plus the URL path's
/// extension when it is a known media extension.
fn safe_file_name(url_path: &str) -> String {
    let last = url_path.rsplit('/').next().unwrap_or("");
    let ext = last
        .rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .filter(|e| SAFE_EXTENSIONS.contains(&e.as_str()));
    match ext {
        Some(e) => format!("input.{e}"),
        None => "input".to_string(),
    }
}

/// Resolve `host`, rejecting any private/loopback/link-local result, and return a
/// vetted public socket address to pin the connection to.
async fn vet_host(host: &str, port: u16) -> Result<SocketAddr, MediaError> {
    if host.eq_ignore_ascii_case("localhost") {
        return Err(MediaError::Refused(
            "refusing to fetch from 'localhost'".into(),
        ));
    }
    let mut chosen: Option<SocketAddr> = None;
    let addrs = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| MediaError::Http(format!("resolving {host}: {e}")))?;
    for addr in addrs {
        if is_blocked_ip(addr.ip()) {
            return Err(MediaError::Refused(format!(
                "refusing to fetch from non-public address ({host})"
            )));
        }
        chosen.get_or_insert(addr);
    }
    chosen.ok_or_else(|| MediaError::Http(format!("{host} did not resolve to any address")))
}

/// Whether an address is private, loopback, link-local, or otherwise non-public.
fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_blocked_v4(v4),
        IpAddr::V6(v6) => is_blocked_v6(v6),
    }
}

fn is_blocked_v4(v4: Ipv4Addr) -> bool {
    let [a, b, c, _] = v4.octets();
    v4.is_private()
        || v4.is_loopback()
        || v4.is_link_local()
        || v4.is_unspecified()
        || v4.is_broadcast()
        || v4.is_documentation()
        || v4.is_multicast()
        || a == 0 // 0.0.0.0/8 "this network"
        || a >= 240 // 240.0.0.0/4 reserved
        || (a == 100 && (b & 0xc0) == 0x40) // 100.64.0.0/10 CGNAT
        || (a == 198 && (b & 0xfe) == 18) // 198.18.0.0/15 benchmarking
        || (a == 192 && b == 0 && c == 0) // 192.0.0.0/24 IETF protocol assignments
}

fn is_blocked_v6(v6: Ipv6Addr) -> bool {
    let s = v6.segments();
    // An IPv4 address embedded in IPv6 is judged as that IPv4 address.
    let embedded_v4 =
        |hi: u16, lo: u16| Ipv4Addr::new((hi >> 8) as u8, hi as u8, (lo >> 8) as u8, lo as u8);
    v6.is_loopback()
        || v6.is_unspecified()
        || v6.is_multicast()
        || (s[0] & 0xfe00) == 0xfc00 // fc00::/7 unique-local
        || (s[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
        || (s[0] & 0xffc0) == 0xfec0 // fec0::/10 site-local (deprecated)
        || (s[0] == 0x2001 && s[1] == 0x0db8) // 2001:db8::/32 documentation
        || (s[0] == 0x2001 && s[1] == 0x0000) // 2001::/32 Teredo (obfuscated IPv4)
        // ::ffff:a.b.c.d (mapped) and ::a.b.c.d (compatible)
        || v6.to_ipv4().is_some_and(is_blocked_v4)
        // 64:ff9b::/96 NAT64
        || (s[0] == 0x0064 && s[1] == 0xff9b && s[2..6] == [0, 0, 0, 0]
            && is_blocked_v4(embedded_v4(s[6], s[7])))
        // 2002::/16 6to4
        || (s[0] == 0x2002 && is_blocked_v4(embedded_v4(s[1], s[2])))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blocked(s: &str) -> bool {
        is_blocked_ip(s.parse().unwrap())
    }

    #[test]
    fn blocks_non_public_addresses() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "0.0.0.0",
            "100.64.0.1",
            "198.18.0.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
            "192.0.0.8",
            "::1",
            "::",
            "fc00::1",
            "fd00:ec2::254",
            "fe80::1",
            "::ffff:127.0.0.1",
            "::127.0.0.1",
            "64:ff9b::a9fe:a9fe", // NAT64 of 169.254.169.254
            "2002:7f00:1::",      // 6to4 of 127.0.0.1
            "2001::1",
            "2001:db8::1",
            "ff02::1",
        ] {
            assert!(blocked(ip), "{ip} should be blocked");
        }
    }

    #[test]
    fn allows_public_addresses() {
        for ip in [
            "1.1.1.1",
            "8.8.8.8",
            "93.184.216.34",
            "2606:4700:4700::1111",
            "64:ff9b::808:808", // NAT64 of 8.8.8.8
            "2002:808:808::",   // 6to4 of 8.8.8.8
        ] {
            assert!(!blocked(ip), "{ip} should be allowed");
        }
    }

    #[test]
    fn download_names_keep_only_safe_extensions() {
        assert_eq!(safe_file_name("/videos/clip.MP4"), "input.mp4");
        assert_eq!(safe_file_name("/a/b/photo.jpeg"), "input.jpeg");
        assert_eq!(safe_file_name("/live/index.m3u8"), "input");
        assert_eq!(safe_file_name("/list.ffconcat"), "input");
        assert_eq!(safe_file_name("/..\\..\\evil.mp4"), "input.mp4");
        assert_eq!(safe_file_name("/"), "input");
        assert_eq!(safe_file_name(""), "input");
    }

    #[tokio::test]
    async fn local_files_are_gated_and_errors_do_not_leak_existence() {
        let root = tempfile::tempdir().unwrap();
        let inside = root.path().join("a.png");
        std::fs::write(&inside, b"x").unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();

        let off = FetchLimits::default();
        let err = local(inside.to_str().unwrap(), &off).err().unwrap();
        assert!(!err.is_transient());

        let on = FetchLimits {
            allow_local_files: true,
            local_roots: vec![root.path().to_path_buf()],
            ..FetchLimits::default()
        };
        assert!(local(inside.to_str().unwrap(), &on).is_ok());

        // "Missing" and "exists but outside the roots" read identically.
        let missing_path = "/definitely/not/here.png";
        let outside_path = outside.path().to_str().unwrap();
        let missing = local(missing_path, &on).err().unwrap().to_string();
        let outside = local(outside_path, &on).err().unwrap().to_string();
        assert_eq!(
            missing.replace(missing_path, "<p>"),
            outside.replace(outside_path, "<p>")
        );
    }

    #[tokio::test]
    async fn refused_schemes_are_not_transient() {
        let url = Url {
            main: "ftp://example.com/a.png".into(),
            fallback: Some("gopher://example.com/b.png".into()),
        };
        let err = fetch(&url, &FetchLimits::default()).await.err().unwrap();
        assert!(!err.is_transient());
    }
}
