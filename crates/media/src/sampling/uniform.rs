//! N evenly-spaced frames across the whole video (segment midpoints).

use crate::ffmpeg::VideoInfo;

/// `count` timestamps at the midpoints of `count` equal segments.
pub fn timestamps(info: &VideoInfo, count: u32) -> Vec<f64> {
    if count == 0 || info.duration <= 0.0 {
        return Vec::new();
    }
    (0..count)
        .map(|i| (i as f64 + 0.5) / count as f64 * info.duration)
        .collect()
}
