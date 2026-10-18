//! Thin ffprobe/ffmpeg wrapper. Requires `ffmpeg` and `ffprobe` on `PATH`.
//!
//! Frame extraction seeks per timestamp (`-ss` before `-i`, accurate on modern
//! ffmpeg). A single-pass `select`-filter extraction would decode the video only
//! once and is the natural optimization; per-seek is used here because it keeps
//! the code simple and the behavior easy to reason about for resume.
//!
//! Inputs are untrusted, so every invocation:
//! * restricts ffmpeg to the `file` protocol (`-protocol_whitelist file`), so a
//!   crafted playlist or reference file can never make ffmpeg open a network
//!   URL, and
//! * runs under a deadline and is killed when it expires, so a pathological file
//!   cannot hang a scan forever.

use std::path::Path;
use std::time::Duration;

use tokio::process::Command;

use apollo_domain::DecodedImage;

use crate::decode::decode_image;
use crate::error::MediaError;

/// Deadline for a probe or a single-frame extraction.
const SHORT_TIMEOUT: Duration = Duration::from_secs(60);
/// Deadline for a pass that decodes the whole video (keyframe / scene scans).
const FULL_PASS_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Input options applied to every ffmpeg / ffprobe call (see module docs).
pub(crate) const INPUT_GUARD: [&str; 2] = ["-protocol_whitelist", "file"];

/// Video metadata from `ffprobe`.
#[derive(Debug, Clone)]
pub struct VideoInfo {
    /// Duration in seconds.
    pub duration: f64,
    /// Presentation time of the first frame, in seconds (non-zero e.g. for many
    /// MPEG-TS files). Frame timestamps reported by ffmpeg are absolute; sampling
    /// works in time relative to this.
    pub start_time: f64,
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
    cmd.args(INPUT_GUARD)
        .args([
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
        ])
        .arg(path);
    let stdout = capture("ffprobe", &mut cmd, SHORT_TIMEOUT).await?;
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
        .args(INPUT_GUARD)
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
    let stdout = capture("ffmpeg", &mut cmd, SHORT_TIMEOUT).await?;
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
    cmd.args(INPUT_GUARD)
        .args([
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
    let stdout = capture("ffprobe", &mut cmd, FULL_PASS_TIMEOUT).await?;
    Ok(parse_float_lines(&stdout))
}

/// Decoder options for the scene-detection pass: skip non-reference frames and
/// the deblocking filter. Cut detection does not need full-quality frames; on
/// the test set this took a third off the decode at 2 threads and still found
/// every spliced-in clip's cut (94% of all cuts).
const FAST_SCENE_DECODE: [&str; 4] = ["-skip_frame", "noref", "-skip_loop_filter", "all"];

/// Scene changes above `threshold` (0..1), as `(timestamp, scene score)`.
/// Backs `scene` sampling.
pub async fn scene_cuts(path: &Path, threshold: f64) -> Result<Vec<(f64, f32)>, MediaError> {
    // Defensive: config validation already enforces 0.0..=1.0, but never
    // interpolate a non-finite or out-of-range value into the filter expression.
    let threshold = if threshold.is_nan() {
        0.0
    } else {
        threshold.clamp(0.0, 1.0)
    };
    let mut cmd = Command::new("ffmpeg");
    cmd.args(["-v", "info"])
        .args(INPUT_GUARD)
        .args(FAST_SCENE_DECODE)
        .arg("-i")
        .arg(path)
        .arg("-vf")
        .arg(format!(
            "select='gt(scene,{threshold})',metadata=print:key=lavfi.scene_score"
        ))
        .args(["-an", "-f", "null", "-"]);
    // The metadata filter reports via stderr; a successful run still exits 0.
    let stderr = capture_stderr("ffmpeg", &mut cmd, FULL_PASS_TIMEOUT).await?;
    Ok(parse_scene_cuts(&stderr))
}

// -------------------------------- internals --------------------------------

async fn capture(name: &str, cmd: &mut Command, timeout: Duration) -> Result<Vec<u8>, MediaError> {
    Ok(run(name, cmd, timeout).await?.stdout)
}

/// Like [`capture`] but returns stderr (for filters that report there).
async fn capture_stderr(
    name: &str,
    cmd: &mut Command,
    timeout: Duration,
) -> Result<Vec<u8>, MediaError> {
    Ok(run(name, cmd, timeout).await?.stderr)
}

/// Run `cmd` to completion within `timeout`, killing it if the deadline passes,
/// and fail on a non-zero exit.
pub(crate) async fn run(
    name: &str,
    cmd: &mut Command,
    timeout: Duration,
) -> Result<std::process::Output, MediaError> {
    cmd.kill_on_drop(true);
    let out = match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(out) => out.map_err(|e| MediaError::Ffmpeg(format!("spawning {name}: {e}")))?,
        Err(_) => {
            return Err(MediaError::Ffmpeg(format!(
                "{name} timed out after {}s",
                timeout.as_secs()
            )));
        }
    };
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        return Err(MediaError::Ffmpeg(format!(
            "{name} failed ({}): {}",
            out.status,
            err.trim()
        )));
    }
    Ok(out)
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
        .filter(|f| f.is_finite() && *f > 0.0)
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
        // Container metadata is untrusted: a NaN / infinite duration would make
        // the sampling loops run forever, so treat it as unknown.
        .filter(|d| d.is_finite() && *d >= 0.0)
        .unwrap_or(0.0);

    let start_time = v
        .get("format")
        .and_then(|f| f.get("start_time"))
        .and_then(|s| s.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|s| s.is_finite())
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
        start_time,
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
    let single_frame = frame_count.is_none_or(|n| n <= 1);
    STILL_CODECS.contains(&codec) && single_frame && duration <= 0.0
}

/// Parse a `"num/den"` rational (an ffmpeg frame rate) into f64.
fn parse_rational(s: &str) -> Option<f64> {
    let mut it = s.split('/');
    let num: f64 = it.next()?.parse().ok()?;
    let den: f64 = it.next().unwrap_or("1").parse().ok()?;
    (den != 0.0).then_some(num / den)
}

/// One float per non-empty line (ffprobe csv of `pts_time`). Only the first csv
/// field is read: newer ffprobe versions append a trailing separator to some
/// rows (e.g. `0.000000,` for a frame carrying side data).
fn parse_float_lines(bytes: &[u8]) -> Vec<f64> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|l| l.split(',').next()?.trim().parse::<f64>().ok())
        .collect()
}

/// Scan ffmpeg `showinfo` stderr for `pts_time:<n>` and collect the times.
pub(crate) fn parse_showinfo_pts(bytes: &[u8]) -> Vec<f64> {
    let text = String::from_utf8_lossy(bytes);
    let mut out = Vec::new();
    for chunk in text.split("pts_time:").skip(1) {
        let num: String = chunk
            .trim_start()
            .chars()
            .enumerate()
            .take_while(|(i, c)| c.is_ascii_digit() || *c == '.' || (*i == 0 && *c == '-'))
            .map(|(_, c)| c)
            .collect();
        if let Ok(t) = num.parse::<f64>() {
            out.push(t);
        }
    }
    out
}

/// Pair each `pts_time` the metadata filter prints with the
/// `lavfi.scene_score` that follows it.
pub(crate) fn parse_scene_cuts(bytes: &[u8]) -> Vec<(f64, f32)> {
    let text = String::from_utf8_lossy(bytes);
    let mut out = Vec::new();
    let mut time = None;
    for line in text.lines() {
        if let Some(t) = parse_showinfo_pts(line.as_bytes()).first() {
            time = Some(*t);
        } else if let Some(score) = line.split("lavfi.scene_score=").nth(1)
            && let (Some(t), Ok(score)) = (time.take(), score.trim().parse::<f32>())
        {
            out.push((t, score));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{parse_float_lines, parse_probe};

    #[test]
    fn scene_cuts_pair_times_with_scores() {
        let err = b"[Parsed_metadata_1 @ 0x1] frame:0    pts:32768   pts_time:2.133333\n\
[Parsed_metadata_1 @ 0x1] lavfi.scene_score=0.300658\n\
noise line\n\
[Parsed_metadata_1 @ 0x1] frame:1    pts:105984  pts_time:6.9\n\
[Parsed_metadata_1 @ 0x1] lavfi.scene_score=0.454537\n";
        assert_eq!(
            super::parse_scene_cuts(err),
            vec![(2.133333, 0.300658), (6.9, 0.454537)]
        );
    }

    #[test]
    fn showinfo_times_parse_including_negative() {
        let err = b"n:   0 pts:  153600 pts_time:10      duration: 512 duration_time:0.0333\n\
                    n:   1 pts:   -1024 pts_time:-0.0667 pos: 1\n";
        assert_eq!(super::parse_showinfo_pts(err), vec![10.0, -0.0667]);
    }

    #[test]
    fn probe_reads_start_time() {
        let json =
            br#"{"streams":[{"codec_type":"video","codec_name":"h264","width":64,"height":64,
            "avg_frame_rate":"25/1"}],
            "format":{"format_name":"mpegts","duration":"10.0","start_time":"1.400000"}}"#;
        let info = parse_probe(json).unwrap();
        assert_eq!(info.start_time, 1.4);
    }

    #[test]
    fn probe_rejects_non_finite_metadata() {
        let json =
            br#"{"streams":[{"codec_type":"video","codec_name":"h264","width":64,"height":64,
            "avg_frame_rate":"1/0","r_frame_rate":"0/0"}],
            "format":{"format_name":"mov,mp4","duration":"inf"}}"#;
        let info = parse_probe(json).unwrap();
        assert_eq!(info.duration, 0.0);
        assert_eq!(info.fps, 0.0);
    }

    #[test]
    fn keyframe_lines_tolerate_trailing_separator() {
        let out = b"0.000000,\n2.000000\n\n4.500000\nN/A\n";
        assert_eq!(parse_float_lines(out), vec![0.0, 2.0, 4.5]);
    }
}
