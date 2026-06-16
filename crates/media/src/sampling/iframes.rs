//! Keyframe-only sampling — cheapest, no full decode (ffprobe `-skip_frame nokey`).

use std::path::Path;

use crate::error::MediaError;
use crate::ffmpeg;

/// Keyframe presentation timestamps.
pub async fn timestamps(path: &Path) -> Result<Vec<f64>, MediaError> {
    ffmpeg::keyframe_timestamps(path).await
}
