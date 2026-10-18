//! Scene-change sampling (ffmpeg `select=gt(scene,T)`).

use crate::error::MediaError;
use crate::share::VideoShare;

/// Scene cuts above `threshold` as `(absolute timestamp, scene score)`, thinned
/// so no two kept cuts are closer than `min_gap` seconds.
pub async fn cuts(
    share: &VideoShare,
    threshold: f64,
    min_gap: f64,
) -> Result<Vec<(f64, f32)>, MediaError> {
    Ok(thin(share.scene_cuts(threshold).await?, min_gap))
}

/// Keep, of cuts closer together than `min_gap`, the strongest (highest scene
/// score; the earlier on a tie). A non-positive gap keeps everything. Returned
/// in time order.
pub(crate) fn thin(mut cuts: Vec<(f64, f32)>, min_gap: f64) -> Vec<(f64, f32)> {
    let by_time =
        |a: &(f64, f32), b: &(f64, f32)| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal);
    if min_gap.is_nan() || min_gap <= 0.0 {
        cuts.sort_by(by_time);
        return cuts;
    }
    cuts.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| by_time(a, b))
    });
    let mut kept: Vec<(f64, f32)> = Vec::new();
    for cut in cuts {
        if kept.iter().all(|k| (k.0 - cut.0).abs() >= min_gap) {
            kept.push(cut);
        }
    }
    kept.sort_by(by_time);
    kept
}

#[cfg(test)]
mod tests {
    use super::thin;

    #[test]
    fn thin_keeps_the_strongest_cut_of_each_close_group() {
        let cuts = vec![
            (1.0, 0.5),
            (1.4, 0.9),
            (2.2, 0.4),
            (5.0, 0.6),
            (5.9, 0.3),
            (6.1, 0.7),
        ];
        let times = |c: Vec<(f64, f32)>| c.into_iter().map(|(t, _)| t).collect::<Vec<_>>();
        assert_eq!(times(thin(cuts.clone(), 1.0)), vec![1.4, 5.0, 6.1]);
        assert_eq!(times(thin(cuts.clone(), 0.0)).len(), cuts.len());
    }
}
