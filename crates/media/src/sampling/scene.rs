//! Scene-change sampling (ffmpeg `select=gt(scene,T)`).

use std::path::Path;

use crate::error::MediaError;
use crate::ffmpeg;

/// Timestamps of scene changes above `threshold` (0..1).
pub async fn timestamps(path: &Path, threshold: f64) -> Result<Vec<f64>, MediaError> {
    ffmpeg::scene_timestamps(path, threshold).await
}
