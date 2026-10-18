//! Scene-change sampling (ffmpeg `select=gt(scene,T)`).

use std::path::Path;

use crate::error::MediaError;
use crate::ffmpeg;

/// Timestamps of scene changes above `threshold` (0..1), keeping a cut only if
/// it is at least `min_gap` seconds after the previously kept one.
pub async fn timestamps(path: &Path, threshold: f64, min_gap: f64) -> Result<Vec<f64>, MediaError> {
    Ok(thin(
        ffmpeg::scene_timestamps(path, threshold).await?,
        min_gap,
    ))
}

/// Drop cuts closer than `min_gap` to the last kept cut (the first of a burst
/// is kept). A non-positive gap keeps everything.
pub(crate) fn thin(mut cuts: Vec<f64>, min_gap: f64) -> Vec<f64> {
    if min_gap.is_nan() || min_gap <= 0.0 {
        return cuts;
    }
    cuts.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mut last = f64::NEG_INFINITY;
    cuts.retain(|&t| {
        let keep = t - last >= min_gap;
        if keep {
            last = t;
        }
        keep
    });
    cuts
}

#[cfg(test)]
mod tests {
    use super::thin;

    #[test]
    fn thin_keeps_first_of_each_burst() {
        let cuts = vec![1.0, 1.4, 2.2, 5.0, 5.9, 6.1];
        assert_eq!(thin(cuts.clone(), 1.0), vec![1.0, 2.2, 5.0, 6.1]);
        assert_eq!(thin(cuts.clone(), 0.0), cuts);
    }
}
