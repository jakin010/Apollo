//! Batched frame extraction: many sampled frames per ffmpeg process.
//!
//! Extracting one frame per ffmpeg process (seek, decode from the previous
//! keyframe, PNG-encode at full resolution, decode again in Rust) costs a process
//! start-up plus a keyframe-to-target decode for *every* frame. Here a group of
//! nearby targets shares one process and one sequential decode:
//!
//! * a `select` filter keeps, for each target, the first frame at or after it
//!   (exactly what an accurate `-ss` seek returns, pixel for pixel);
//! * `showinfo` reports each kept frame's timestamp, which maps frames back to
//!   targets (a frame can satisfy several targets in a low-frame-rate video);
//! * frames are scaled by ffmpeg straight to the model's input size (matching
//!   the still-image resize closely, see [`SCALE_FLAGS`]) and streamed as raw
//!   RGB, so no PNG is encoded or decoded;
//! * keyframe targets are decoded with `-skip_frame nokey`, skipping every other
//!   frame.
//!
//! Targets further apart than [`MAX_RUN_GAP`] go to separate processes, so sparse
//! sampling of a long video never decodes the stretches in between. Any target
//! the batch cannot deliver falls back to the single-frame path.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::process::Command;

use apollo_domain::DecodedImage;

use crate::error::MediaError;
use crate::ffmpeg::{self, VideoInfo};

/// Targets closer than this (seconds) are decoded in one sequential pass rather
/// than separate seeks. Decoding the gap costs about as much as a new process
/// plus seek at ~4 s of 1080p video.
pub const MAX_RUN_GAP: f64 = 4.0;

/// A frame within this many seconds *before* a target still counts as reaching
/// it: timestamps from ffprobe are rounded to microseconds, so a frame exactly at
/// a target can appear a hair earlier.
const TARGET_EPSILON: f64 = 0.001;

/// swscale settings for the downscale to the model size. Bilinear (whose
/// kernel widens when downscaling) with full-resolution chroma and accurate
/// rounding comes closest to the `image` crate's Triangle resize that still
/// images go through: ~1/255 mean difference per channel, against ~3/255 for
/// plain `area` or `bilinear`.
const SCALE_FLAGS: &str = "bilinear+full_chroma_int+accurate_rnd";

/// Deadline for one batched extraction process.
const RUN_TIMEOUT: Duration = Duration::from_secs(120);

/// Set once ffmpeg has rejected `-fps_mode` (ffmpeg < 5.1); `-vsync` is used
/// from then on.
static LEGACY_VSYNC: AtomicBool = AtomicBool::new(false);

/// A frame to extract.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FrameRequest {
    /// Seconds from the start of the video (relative to its start time).
    pub timestamp: f64,
    /// The target is a keyframe, so it can be reached decoding keyframes only.
    pub keyframe: bool,
}

/// Extract the requested frames as `size`×`size` RGB images, returned in the
/// order requested.
pub async fn extract_frames_scaled(
    path: &Path,
    info: &VideoInfo,
    requests: &[FrameRequest],
    size: u32,
) -> Result<Vec<DecodedImage>, MediaError> {
    let mut out: Vec<Option<DecodedImage>> = vec![None; requests.len()];
    for run in group_runs(requests) {
        let times: Vec<f64> = run.iter().map(|&i| requests[i].timestamp).collect();
        let keyframes = run.iter().all(|&i| requests[i].keyframe);
        // A failed batch is not fatal: every target it misses falls back below.
        let output = Output {
            width: size,
            height: size,
            flags: SCALE_FLAGS,
        };
        if let Ok(frames) = extract_run(path, info, &times, keyframes, &output).await {
            for (slot, frame) in run.iter().zip(frames) {
                out[*slot] = frame;
            }
        }
    }
    let mut images = Vec::with_capacity(requests.len());
    for (req, frame) in requests.iter().zip(out) {
        images.push(match frame {
            Some(img) => img,
            None => fallback(path, req.timestamp, size).await?,
        });
    }
    Ok(images)
}

/// Group request indices into runs: keyframe targets together, other targets in
/// ascending time with gaps of at most [`MAX_RUN_GAP`].
fn group_runs(requests: &[FrameRequest]) -> Vec<Vec<usize>> {
    let mut order: Vec<usize> = (0..requests.len()).collect();
    order.sort_by(|&a, &b| requests[a].timestamp.total_cmp(&requests[b].timestamp));
    let (keyframes, others): (Vec<usize>, Vec<usize>) =
        order.into_iter().partition(|&i| requests[i].keyframe);

    let mut runs = Vec::new();
    if !keyframes.is_empty() {
        runs.push(keyframes);
    }
    let mut current: Vec<usize> = Vec::new();
    for i in others {
        if let Some(&last) = current.last()
            && requests[i].timestamp - requests[last].timestamp > MAX_RUN_GAP
        {
            runs.push(std::mem::take(&mut current));
        }
        current.push(i);
    }
    if !current.is_empty() {
        runs.push(current);
    }
    runs
}

/// Output frame geometry and swscale flags.
#[derive(Debug, Clone, Copy)]
struct Output<'a> {
    width: u32,
    height: u32,
    flags: &'a str,
}

/// One ffmpeg pass over `[first, last]` returning, for each (ascending) target,
/// the first frame at or after it scaled per `output` — `None` where the video
/// has no such frame.
async fn extract_run(
    path: &Path,
    info: &VideoInfo,
    targets: &[f64],
    keyframes_only: bool,
    output: &Output<'_>,
) -> Result<Vec<Option<DecodedImage>>, MediaError> {
    let Output { width, height, .. } = *output;
    let (Some(&first), Some(&last)) = (targets.first(), targets.last()) else {
        return Ok(Vec::new());
    };
    // `-ss`/`-to` are positions relative to the file start; with `-copyts` the
    // filter's `t` is the absolute presentation time, so targets are shifted by
    // the stream's start time inside the expression.
    let seek = (first - 0.25).max(0.0);
    let stop = last + 1.0;
    let expr = select_expr(targets.iter().map(|t| t + info.start_time));

    let out = loop {
        let legacy = LEGACY_VSYNC.load(Ordering::Relaxed);
        let spec = RunSpec {
            seek,
            stop,
            expr: &expr,
            keyframes_only,
            output,
        };
        let mut cmd = run_command(path, &spec, legacy);
        match ffmpeg::run("ffmpeg", &mut cmd, RUN_TIMEOUT).await {
            // ffmpeg < 5.1 has no `-fps_mode`; retry with `-vsync`.
            Err(MediaError::Ffmpeg(msg)) if !legacy && msg.contains("fps_mode") => {
                LEGACY_VSYNC.store(true, Ordering::Relaxed);
            }
            other => break other?,
        }
    };

    let times: Vec<f64> = ffmpeg::parse_showinfo_pts(&out.stderr)
        .into_iter()
        .map(|t| t - info.start_time)
        .collect();
    let frame_len = width as usize * height as usize * 3;
    if out.stdout.len() != times.len() * frame_len {
        return Err(MediaError::Ffmpeg(format!(
            "batched extraction returned {} bytes for {} frames",
            out.stdout.len(),
            times.len()
        )));
    }
    let frames: Vec<DecodedImage> = out
        .stdout
        .chunks_exact(frame_len)
        .map(|px| DecodedImage {
            width,
            height,
            data: px.to_vec(),
        })
        .collect();

    // Each target gets the first kept frame at or after it.
    Ok(targets
        .iter()
        .map(|&t| {
            times
                .iter()
                .position(|&ft| ft >= t - TARGET_EPSILON)
                .map(|i| frames[i].clone())
        })
        .collect())
}

/// Parameters of one extraction run.
struct RunSpec<'a> {
    /// Input seek position (relative seconds).
    seek: f64,
    /// Stop reading at this position (relative seconds).
    stop: f64,
    /// `select` expression choosing the frames.
    expr: &'a str,
    /// Decode keyframes only.
    keyframes_only: bool,
    output: &'a Output<'a>,
}

/// The ffmpeg invocation for one run: seek, decode to `stop`, keep the frames
/// the expression selects (reporting each one's original timestamp via
/// `showinfo`), scale them, and stream raw RGB. Kept frames pass through with
/// their own timestamps; frame-rate conversion would duplicate frames to fill
/// the gaps between them.
fn run_command(path: &Path, spec: &RunSpec<'_>, legacy_vsync: bool) -> Command {
    let mut cmd = Command::new("ffmpeg");
    cmd.args(["-hide_banner", "-nostats", "-v", "info"])
        .args(ffmpeg::INPUT_GUARD);
    if spec.keyframes_only {
        cmd.args(["-skip_frame", "nokey"]);
    }
    cmd.arg("-ss")
        .arg(format!("{:.6}", spec.seek))
        .arg("-to")
        .arg(format!("{:.6}", spec.stop))
        .arg("-copyts")
        .arg("-i")
        .arg(path)
        .arg("-vf")
        .arg(format!(
            "select='{}',showinfo,scale={}:{}:flags={}",
            spec.expr, spec.output.width, spec.output.height, spec.output.flags
        ))
        .args(if legacy_vsync {
            ["-vsync", "passthrough"]
        } else {
            ["-fps_mode", "passthrough"]
        })
        .args(["-f", "image2pipe", "-c:v", "rawvideo"])
        .args(["-pix_fmt", "rgb24", "pipe:1"]);
    cmd
}

/// `select` expression keeping the first frame at or after each target: a frame
/// at time `t` (previous frame at `prev_t`) is kept for target `x` when
/// `prev_t < x <= t`. `prev_t` is NaN for the first frame, which compares false,
/// so the first frame is kept if it already reaches a target.
fn select_expr(targets: impl Iterator<Item = f64>) -> String {
    targets
        .map(|t| {
            let x = t - TARGET_EPSILON;
            format!("gte(t\\,{x:.6})*not(gte(prev_t\\,{x:.6}))")
        })
        .collect::<Vec<_>>()
        .join("+")
}

/// Single-frame path (one process, PNG), scaled to `size` in Rust.
async fn fallback(path: &Path, timestamp: f64, size: u32) -> Result<DecodedImage, MediaError> {
    // The frame is resized right away, so no extra pixel cap is applied here
    // beyond the resolution check done when the video was probed.
    let img = ffmpeg::extract_frame(path, timestamp, None).await?;
    tokio::task::spawn_blocking(move || resize(&img, size))
        .await
        .map_err(|e| MediaError::Decode(format!("resize task failed: {e}")))?
}

/// Resize an RGB image to `size`×`size`.
fn resize(img: &DecodedImage, size: u32) -> Result<DecodedImage, MediaError> {
    if img.width == size && img.height == size {
        return Ok(img.clone());
    }
    let view = image::ImageBuffer::<image::Rgb<u8>, &[u8]>::from_raw(
        img.width,
        img.height,
        img.data.as_slice(),
    )
    .ok_or_else(|| MediaError::Decode("frame buffer does not match its dimensions".into()))?;
    let resized = image::imageops::resize(&view, size, size, image::imageops::FilterType::Triangle);
    Ok(DecodedImage {
        width: size,
        height: size,
        data: resized.into_raw(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(timestamp: f64, keyframe: bool) -> FrameRequest {
        FrameRequest {
            timestamp,
            keyframe,
        }
    }

    #[test]
    fn runs_split_on_gaps_and_keep_keyframes_together() {
        let requests = [
            req(30.0, false),
            req(0.0, true),
            req(10.0, false),
            req(12.0, false),
            req(50.0, true),
            req(11.0, false),
        ];
        let runs = group_runs(&requests);
        // Keyframes (any distance apart) form one run; 10/11/12 are within the
        // gap; 30 is too far from 12.
        assert_eq!(runs, vec![vec![1, 4], vec![2, 5, 3], vec![0]]);
    }

    #[test]
    fn expression_keeps_first_frame_at_or_after_each_target() {
        let e = select_expr([1.5, 2.0].into_iter());
        assert_eq!(
            e,
            "gte(t\\,1.499000)*not(gte(prev_t\\,1.499000))+gte(t\\,1.999000)*not(gte(prev_t\\,1.999000))"
        );
    }

    #[test]
    fn resize_is_a_no_op_at_the_target_size() {
        let img = DecodedImage {
            width: 2,
            height: 2,
            data: vec![7; 12],
        };
        assert_eq!(resize(&img, 2).unwrap(), img);
        let big = resize(&img, 4).unwrap();
        assert_eq!((big.width, big.height, big.data.len()), (4, 4, 48));
    }
}

/// End-to-end against the real ffmpeg: batched extraction returns exactly what
/// the single-frame path returns. Skipped when ffmpeg is not installed.
#[cfg(test)]
mod ffmpeg_tests {
    use super::*;

    fn have_ffmpeg() -> bool {
        std::process::Command::new("ffmpeg")
            .arg("-version")
            .output()
            .is_ok_and(|o| o.status.success())
    }

    /// 10 s of 64x48 test pattern at 10 fps with a keyframe every 2 s.
    fn sample_video(dir: &Path) -> std::path::PathBuf {
        let path = dir.join("clip.mp4");
        let ok = std::process::Command::new("ffmpeg")
            .args(["-v", "error", "-y", "-f", "lavfi", "-i"])
            .arg("testsrc2=size=64x48:rate=10")
            .args(["-t", "10", "-g", "20", "-pix_fmt", "yuv420p"])
            .arg(&path)
            .status()
            .is_ok_and(|s| s.success());
        assert!(ok, "could not create the sample video");
        path
    }

    /// Keyframes, a dense cluster, a sparse outlier, and two targets inside one
    /// frame interval (100 ms) that must both receive that frame.
    async fn requests(path: &Path, info: &VideoInfo) -> Vec<FrameRequest> {
        let keyframes = ffmpeg::keyframe_timestamps(path).await.unwrap();
        assert!(keyframes.len() >= 4, "{keyframes:?}");
        let mut out: Vec<FrameRequest> = keyframes
            .iter()
            .map(|&t| FrameRequest {
                timestamp: t - info.start_time,
                keyframe: true,
            })
            .collect();
        for t in [3.3, 3.5, 3.7, 4.05, 9.1, 6.02, 6.04] {
            out.push(FrameRequest {
                timestamp: t,
                keyframe: false,
            });
        }
        out
    }

    #[tokio::test]
    async fn batched_frames_are_pixel_identical_to_single_frame_extraction() {
        if !have_ffmpeg() {
            eprintln!("skipping: ffmpeg not installed");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = sample_video(dir.path());
        let info = ffmpeg::probe(&path).await.unwrap();
        let requests = requests(&path, &info).await;

        // At the native 64x48 size the scale filter is a no-op, so each frame
        // must equal what an accurate single-frame seek returns.
        for run in group_runs(&requests) {
            let times: Vec<f64> = run.iter().map(|&i| requests[i].timestamp).collect();
            let keyframes = run.iter().all(|&i| requests[i].keyframe);
            // At the native size a plain scale is a pass-through.
            let native = Output {
                width: 64,
                height: 48,
                flags: "area",
            };
            let frames = extract_run(&path, &info, &times, keyframes, &native)
                .await
                .unwrap();
            for (&t, frame) in times.iter().zip(frames) {
                let got = frame.unwrap_or_else(|| panic!("no frame for target {t}"));
                let want = ffmpeg::extract_frame(&path, t, None).await.unwrap();
                assert_eq!(got, want, "frame for target {t}");
            }
        }
    }

    #[tokio::test]
    async fn returns_square_frames_in_request_order() {
        if !have_ffmpeg() {
            eprintln!("skipping: ffmpeg not installed");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = sample_video(dir.path());
        let info = ffmpeg::probe(&path).await.unwrap();
        let requests = requests(&path, &info).await;
        let frames = extract_frames_scaled(&path, &info, &requests, 32)
            .await
            .unwrap();
        assert_eq!(frames.len(), requests.len());
        assert!(frames.iter().all(|f| f.width == 32 && f.height == 32));
        // 6.02 and 6.04 fall between the frames at 6.0 and 6.1: same frame.
        let n = requests.len();
        assert_eq!(frames[n - 2], frames[n - 1]);
        assert_ne!(frames[n - 3], frames[n - 2]);
    }
}
