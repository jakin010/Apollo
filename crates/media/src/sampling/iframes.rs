//! Keyframe-only sampling — cheapest, no full decode (ffprobe `-skip_frame nokey`).

use crate::error::MediaError;
use crate::share::VideoShare;

/// Keyframe presentation timestamps (absolute).
pub async fn timestamps(share: &VideoShare) -> Result<Vec<f64>, MediaError> {
    share.keyframes().await
}
