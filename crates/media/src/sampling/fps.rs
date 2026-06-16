//! Fixed frames-per-second sampling.

use crate::ffmpeg::VideoInfo;

/// Timestamps at a fixed rate: 0, 1/fps, 2/fps, ... up to (not including) the
/// video duration.
pub fn timestamps(info: &VideoInfo, fps: f64) -> Vec<f64> {
    if fps <= 0.0 || info.duration <= 0.0 {
        return Vec::new();
    }
    let step = 1.0 / fps;
    let mut out = Vec::new();
    let mut t = 0.0;
    while t < info.duration {
        out.push(t);
        t += step;
    }
    out
}
