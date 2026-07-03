//! Thin ffprobe/ffmpeg wrapper. Requires `ffmpeg` and `ffprobe` on `PATH`.
//!
//! Frame extraction seeks per timestamp (`-ss` before `-i`, accurate on modern
//! ffmpeg). A single-pass `select`-filter extraction would decode the video only
//! once and is the natural optimization; per-seek is used here because it keeps
//! the code simple and the behavior easy to reason about for resume.

use std::path::Path;

use tokio::process::Command;

use apollo_domain::DecodedImage;

use crate::decode::decode_image;
use crate::error::MediaError;

/// Video metadata from `ffprobe`.
#[derive(Debug, Clone)]
pub struct VideoInfo {
    /// Duration in seconds.
    pub duration: f64,
    /// Average frame rate (frames per second).
    pub fps: f64,
    /// Total frame count, if ffprobe reported it.
    pub frame_count: Option<u64>,
    pub width: u32,
    pub height: u32,
}

/// Probe a video file for duration, frame rate, and dimensions.
pub async fn probe(path: &Path) -> Result<VideoInfo, MediaError> {
    let mut cmd = Command::new("ffprobe");
    cmd.args([
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
    ])
    .arg(path);
    let stdout = capture("ffprobe", &mut cmd).await?;
    parse_probe(&stdout)
}

/// Extract a single frame at `timestamp` seconds, decoded to RGB8.
pub async fn extract_frame(
    path: &Path,
    timestamp: f64,
    max_pixels: Option<u64>,
) -> Result<DecodedImage, MediaError> {
    // Primary: fast input-seek to the requested time.
    if let Some(bytes) = grab_frame(path, &["-ss".into(), format!("{timestamp}")]).await? {
        return decode_image(&bytes, max_pixels);
    }
    // The seek landed past the last decodable frame (imprecise duration / VFR);
    // fall back to the file's final frame so a near-end sample still yields an
    // image instead of failing the whole scan.
    if let Some(bytes) = grab_frame(path, &["-sseof".into(), "-1".into()]).await? {
        return decode_image(&bytes, max_pixels);
    }
    Err(MediaError::Ffmpeg(format!(
        "ffmpeg produced no frame at t={timestamp} (no final frame either)"
    )))
}

/// Run ffmpeg with the given pre-input seek args and grab a single PNG frame.
/// `Ok(None)` means ffmpeg exited cleanly but produced no frame (seek past the end).
async fn grab_frame(path: &Path, seek: &[String]) -> Result<Option<Vec<u8>>, MediaError> {
    let mut cmd = Command::new("ffmpeg");
    cmd.args(["-v", "error"])
        .args(seek)
        .arg("-i")
        .arg(path)
        .args([
            "-frames:v",
            "1",
            "-f",
            "image2pipe",
            "-vcodec",
            "png",
            "pipe:1",
        ]);
    let stdout = capture("ffmpeg", &mut cmd).await?;
    Ok((!stdout.is_empty()).then_some(stdout))
}

/// Extract frames at each `timestamp` (one ffmpeg seek per timestamp).
pub async fn extract_frames(
    path: &Path,
    timestamps: &[f64],
    max_pixels: Option<u64>,
) -> Result<Vec<DecodedImage>, MediaError> {
    let mut frames = Vec::with_capacity(timestamps.len());
    for &t in timestamps {
        frames.push(extract_frame(path, t, max_pixels).await?);
    }
    Ok(frames)
}

/// Presentation timestamps of all keyframes (I-frames). Backs `iframes` sampling.
pub async fn keyframe_timestamps(path: &Path) -> Result<Vec<f64>, MediaError> {
    let mut cmd = Command::new("ffprobe");
    cmd.args([
        "-v",
        "error",
        "-skip_frame",
        "nokey",
        "-select_streams",
        "v:0",
        "-show_entries",
        "frame=pts_time",
        "-of",
        "csv=print_section=0",
    ])
    .arg(path);
    let stdout = capture("ffprobe", &mut cmd).await?;
    Ok(parse_float_lines(&stdout))
}

/// Timestamps of scene changes above `threshold` (0..1). Backs `scene` sampling.
pub async fn scene_timestamps(path: &Path, threshold: f64) -> Result<Vec<f64>, MediaError> {
    // Defensive: config validation already enforces 0.0..=1.0, but never
    // interpolate a non-finite or out-of-range value into the filter expression.
    let threshold = if threshold.is_nan() {
        0.0
    } else {
        threshold.clamp(0.0, 1.0)
    };
    let mut cmd = Command::new("ffmpeg");
    cmd.args(["-v", "info", "-i"])
        .arg(path)
        .arg("-vf")
        .arg(format!("select='gt(scene,{threshold})',showinfo"))
        .args(["-an", "-f", "null", "-"]);
    // showinfo reports via stderr; a successful run still exits 0.
    let stderr = capture_stderr("ffmpeg", &mut cmd).await?;
    Ok(parse_showinfo_pts(&stderr))
}

// -------------------------------- internals --------------------------------

async fn capture(name: &str, cmd: &mut Command) -> Result<Vec<u8>, MediaError> {
    let out = cmd
        .output()
        .await
        .map_err(|e| MediaError::Ffmpeg(format!("spawning {name}: {e}")))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        return Err(MediaError::Ffmpeg(format!(
            "{name} failed ({}): {}",
            out.status,
            err.trim()
        )));
    }
    Ok(out.stdout)
}

/// Like [`capture`] but returns stderr (for filters that report there).
async fn capture_stderr(name: &str, cmd: &mut Command) -> Result<Vec<u8>, MediaError> {
    let out = cmd
        .output()
        .await
        .map_err(|e| MediaError::Ffmpeg(format!("spawning {name}: {e}")))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        return Err(MediaError::Ffmpeg(format!(
            "{name} failed ({}): {}",
            out.status,
            err.trim()
        )));
    }
    Ok(out.stderr)
}

fn parse_probe(stdout: &[u8]) -> Result<VideoInfo, MediaError> {
    let v: serde_json::Value = serde_json::from_slice(stdout)
        .map_err(|e| MediaError::Parse(format!("ffprobe json: {e}")))?;

    let video = v
        .get("streams")
        .and_then(|s| s.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|s| s.get("codec_type").and_then(|c| c.as_str()) == Some("video"))
        })
        .ok_or_else(|| MediaError::Parse("ffprobe: no video stream".into()))?;

    let width = video.get("width").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
    let height = video.get("height").and_then(|x| x.as_u64()).unwrap_or(0) as u32;

    let fps = video
        .get("avg_frame_rate")
        .and_then(|x| x.as_str())
        .and_then(parse_rational)
        .or_else(|| {
            video
                .get("r_frame_rate")
                .and_then(|x| x.as_str())
                .and_then(parse_rational)
        })
        .unwrap_or(0.0);

    let duration = v
        .get("format")
        .and_then(|f| f.get("duration"))
        .and_then(|d| d.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .or_else(|| {
            video
                .get("duration")
                .and_then(|d| d.as_str())
                .and_then(|s| s.parse::<f64>().ok())
        })
        .unwrap_or(0.0);

    let frame_count = video
        .get("nb_frames")
        .and_then(|n| n.as_str())
        .and_then(|s| s.parse::<u64>().ok());

    // Reject still images handed to a video field: ffmpeg will happily "probe" a
    // PNG/JPEG as a 1-frame stream, but it is not a video.
    let format_name = v
        .get("format")
        .and_then(|f| f.get("format_name"))
        .and_then(|s| s.as_str())
        .unwrap_or("");
    let codec = video
        .get("codec_name")
        .and_then(|c| c.as_str())
        .unwrap_or("");
    if is_image_only(format_name, codec, duration, frame_count) {
        return Err(MediaError::Ffmpeg(format!(
            "expected a video but got a still image (format '{format_name}', codec '{codec}')"
        )));
    }

    Ok(VideoInfo {
        duration,
        fps,
        frame_count,
        width,
        height,
    })
}

/// Whether an ffprobe result describes a still image rather than a video. Image
/// demuxers (`image2`, `*_pipe`, `apng`) are conclusive; otherwise a known
/// still-image codec with at most one frame and no real duration is treated as an
/// image. Animated formats (e.g. multi-frame GIF) have >1 frame and pass.
fn is_image_only(format_name: &str, codec: &str, duration: f64, frame_count: Option<u64>) -> bool {
    let image_format = format_name
        .split(',')
        .any(|f| f == "image2" || f == "apng" || f.ends_with("_pipe"));
    if image_format {
        return true;
    }
    const STILL_CODECS: &[&str] = &["png", "mjpeg", "bmp", "tiff", "webp", "gif"];
    let single_frame = frame_count.map_or(true, |n| n <= 1);
    STILL_CODECS.contains(&codec) && single_frame && duration <= 0.0
}

/// Parse a `"num/den"` rational (an ffmpeg frame rate) into f64.
fn parse_rational(s: &str) -> Option<f64> {
    let mut it = s.split('/');
    let num: f64 = it.next()?.parse().ok()?;
    let den: f64 = it.next().unwrap_or("1").parse().ok()?;
    (den != 0.0).then_some(num / den)
}

/// One float per non-empty line (ffprobe csv of `pts_time`).
fn parse_float_lines(bytes: &[u8]) -> Vec<f64> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|l| l.trim().parse::<f64>().ok())
        .collect()
}

/// Scan ffmpeg `showinfo` stderr for `pts_time:<n>` and collect the times.
fn parse_showinfo_pts(bytes: &[u8]) -> Vec<f64> {
    let text = String::from_utf8_lossy(bytes);
    let mut out = Vec::new();
    for chunk in text.split("pts_time:").skip(1) {
        let num: String = chunk
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == '.')
            .collect();
        if let Ok(t) = num.parse::<f64>() {
            out.push(t);
        }
    }
    out
}
