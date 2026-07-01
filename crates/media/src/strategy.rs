//! Multi-step planning + aggregation + early-exit decision.
//!
//! [`plan`] runs the sampling steps in `step` order, drops any frame within
//! [`DEDUPE_TOLERANCE`] of one already chosen, and assigns indices in processing
//! order (cheap steps first). The engine classifies in that order in batches and
//! stops on the first [`triggered`] result; [`aggregate`] rolls the kept frames
//! into one result.

use std::cmp::Ordering;
use std::path::Path;

use apollo_config::{Aggregation, SamplingStep};
use apollo_domain::{select_top, Classification, Prediction};

use crate::error::MediaError;
use crate::ffmpeg::VideoInfo;
use crate::sampling::{self, FrameRef};

/// Frames within this many seconds of an already-chosen frame are treated as
/// duplicates across steps.
pub const DEDUPE_TOLERANCE: f64 = 0.05;

/// Build the ordered, de-duplicated frame plan for a video.
pub async fn plan(
    path: &Path,
    info: &VideoInfo,
    steps: &[SamplingStep],
) -> Result<Vec<FrameRef>, MediaError> {
    let mut ordered = steps.to_vec();
    ordered.sort_by_key(|s| s.step);

    // Never seek at or past the last decodable frame: imprecise container
    // durations and variable frame rates mean a timestamp at (or fractionally
    // beyond) `duration` decodes to nothing. Clamp every sample to at least one
    // frame-duration short of the end.
    let guard = if info.fps > 0.0 { (1.0 / info.fps).max(1e-3) } else { 0.05 };
    let max_seek = (info.duration - guard).max(0.0);

    let mut chosen: Vec<f64> = Vec::new();
    for step in &ordered {
        let mut times = sampling::step_timestamps(path, info, step).await?;
        times.sort_by(cmp_f64);
        for t in times {
            let t = t.clamp(0.0, max_seek);
            if !chosen.iter().any(|&c| (c - t).abs() <= DEDUPE_TOLERANCE) {
                chosen.push(t);
            }
        }
    }

    Ok(chosen
        .into_iter()
        .enumerate()
        .map(|(i, timestamp)| FrameRef {
            index: i as u32,
            timestamp,
        })
        .collect())
}

/// Roll per-frame classifications into one. Predictions are pooled by `max` or
/// `mean` of each label's score across all frames (taxonomy child scores pool
/// the same way, since they are now just flat predictions keyed by child id),
/// then the standard top-5 ∪ >0.90 selection is applied. `mean` divides by the
/// total frame count, so a category present in only a few frames pools to a low,
/// prevalence-weighted score.
pub fn aggregate(per_frame: &[Classification], how: Aggregation) -> Classification {
    use std::collections::BTreeMap;

    let denom = per_frame.len().max(1) as f32;

    // Pool flat predictions by label id across all frames.
    let mut flat: BTreeMap<u32, f32> = BTreeMap::new();
    for frame in per_frame {
        for p in &frame.predictions {
            let entry = flat.entry(p.label).or_insert(match how {
                Aggregation::Max => f32::MIN,
                Aggregation::Mean => 0.0,
            });
            match how {
                Aggregation::Max => *entry = entry.max(p.score),
                Aggregation::Mean => *entry += p.score,
            }
        }
    }

    let predictions = flat
        .into_iter()
        .map(|(label, value)| Prediction {
            label,
            score: match how {
                Aggregation::Max => value,
                Aggregation::Mean => value / denom,
            },
        })
        .collect::<Vec<_>>();

    // Taxonomy results are flat predictions like any other model now, so there is
    // a single rollup: the standard top-5 ∪ >0.90 selection.
    Classification {
        predictions: select_top(predictions),
    }
}

/// Whether any trigger label is predicted at or above `threshold` — the
/// early-exit condition for a video scan.
pub fn triggered(class: &Classification, labels: &[u32], threshold: f32) -> bool {
    class
        .predictions
        .iter()
        .any(|p| p.score >= threshold && labels.contains(&p.label))
}

fn cmp_f64(a: &f64, b: &f64) -> Ordering {
    a.partial_cmp(b).unwrap_or(Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apollo_config::SamplingKind;

    fn info(duration: f64, fps: f64) -> VideoInfo {
        VideoInfo { duration, fps, frame_count: None, width: 0, height: 0 }
    }

    fn step(n: u32, method: SamplingKind, count: Option<u32>, fps: Option<f64>) -> SamplingStep {
        SamplingStep { step: n, method, fps, count, nth: None, threshold: None }
    }

    fn classification(preds: &[(u32, f32)]) -> Classification {
        Classification {
            predictions: preds
                .iter()
                .map(|(l, s)| Prediction { label: *l, score: *s })
                .collect(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn plan_dedupes_overlapping_steps() {
        // Two identical uniform steps: the second is fully de-duplicated.
        let steps = vec![
            step(1, SamplingKind::Uniform, Some(3), None),
            step(2, SamplingKind::Uniform, Some(3), None),
        ];
        let frames = plan(Path::new("unused"), &info(9.0, 30.0), &steps)
            .await
            .unwrap();
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].index, 0);
        assert!((frames[0].timestamp - 1.5).abs() < 1e-9);
        assert!((frames[2].timestamp - 7.5).abs() < 1e-9);
    }

    #[tokio::test]
    async fn plan_orders_cheap_step_first_then_merges() {
        // step 1: 2 uniform frames at [2,6]; step 2: fps=1 at 0..7, minus dupes.
        let steps = vec![
            step(1, SamplingKind::Uniform, Some(2), None),
            step(2, SamplingKind::Fps, None, Some(1.0)),
        ];
        let frames = plan(Path::new("unused"), &info(8.0, 30.0), &steps)
            .await
            .unwrap();
        // step1 -> [2,6]; step2 -> 0..7 drop 2 and 6 -> [0,1,3,4,5,7]; total 8
        assert_eq!(frames.len(), 8);
        assert!((frames[0].timestamp - 2.0).abs() < 1e-9); // cheap step first
        assert!((frames[1].timestamp - 6.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn plan_never_seeks_past_last_frame() {
        // Dense uniform sampling would otherwise place a frame essentially at the
        // very end of the clip, where ffmpeg can decode nothing. Every planned
        // timestamp must stay at least one frame-duration short of the end.
        let frames = plan(
            Path::new("unused"),
            &info(60.0, 30.0),
            &[step(1, SamplingKind::Uniform, Some(2000), None)],
        )
            .await
            .unwrap();
        let max_seek = 60.0 - 1.0 / 30.0;
        assert!(!frames.is_empty());
        assert!(frames.iter().all(|f| f.timestamp <= max_seek + 1e-9));
    }

    #[test]
    fn aggregate_mean_and_max() {
        let frames = vec![
            classification(&[(1, 0.9), (2, 0.1)]),
            classification(&[(1, 0.5)]),
        ];

        let mean = aggregate(&frames, Aggregation::Mean);
        let cat = mean.predictions.iter().find(|p| p.label == 1).unwrap();
        assert!((cat.score - 0.7).abs() < 1e-6);
        let dog = mean.predictions.iter().find(|p| p.label == 2).unwrap();
        assert!((dog.score - 0.05).abs() < 1e-6);

        let max = aggregate(&frames, Aggregation::Max);
        let cat = max.predictions.iter().find(|p| p.label == 1).unwrap();
        assert!((cat.score - 0.9).abs() < 1e-6);
    }

    #[test]
    fn triggered_respects_labels_and_threshold() {
        let c = classification(&[(1, 0.95), (2, 0.05)]);
        assert!(triggered(&c, &[1], 0.85));
        assert!(!triggered(&c, &[1], 0.99));
        assert!(!triggered(&c, &[3], 0.5));
    }
}