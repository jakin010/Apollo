//! Multi-step planning + aggregation + early-exit decision.
//!
//! [`plan`] runs the sampling steps in `step` order, drops any frame within
//! [`DEDUPE_TOLERANCE`] of one already chosen, and assigns indices in processing
//! order (cheap steps first). The engine classifies in that order in batches and
//! stops on the first [`triggered`] result. If none triggers, [`refine_plan`]
//! adds the `refine` steps' frames around ambiguous scores, indexed after the
//! plan. [`aggregate`] rolls the kept frames into one result.

use std::cmp::Ordering;
use std::path::Path;

use apollo_config::{Aggregation, SamplingKind, SamplingStep};
use apollo_domain::{Classification, Prediction, select_top};

use crate::error::MediaError;
use crate::ffmpeg::VideoInfo;
use crate::sampling::{self, FrameRef};

/// Frames within this many seconds of an already-chosen frame are treated as
/// duplicates across steps.
pub const DEDUPE_TOLERANCE: f64 = 0.05;

/// Upper bound on frames planned for one video. Keyframe and scene lists come
/// from the (untrusted) file, so the plan is capped rather than allowed to grow
/// without limit. One hour at 5 fps is 18 000 frames.
pub const MAX_PLANNED_FRAMES: usize = 100_000;

/// Build the ordered, de-duplicated frame plan for a video. `refine` steps are
/// left out: they are planned from the scores ([`refine_plan`]).
pub async fn plan(
    path: &Path,
    info: &VideoInfo,
    steps: &[SamplingStep],
) -> Result<Vec<FrameRef>, MediaError> {
    let mut ordered: Vec<&SamplingStep> = steps
        .iter()
        .filter(|s| s.method != SamplingKind::Refine)
        .collect();
    ordered.sort_by_key(|s| s.step);

    let max_seek = max_seek(info);
    let mut picker = Picker::default();
    'steps: for step in ordered {
        let mut times = sampling::step_timestamps(path, info, step).await?;
        times.sort_by(cmp_f64);
        let keyframe = step.method == SamplingKind::Iframes;
        for t in times {
            if t.is_finite() {
                picker.add(t.clamp(0.0, max_seek), keyframe);
            }
            if picker.chosen.len() >= MAX_PLANNED_FRAMES {
                break 'steps;
            }
        }
    }
    Ok(picker.into_frames(0))
}

/// Frames the strategy's `refine` steps add once every planned frame has been
/// classified without a trigger: for each planned frame whose trigger score is
/// ambiguous (`min_score <= score < threshold`), frames every `1 / fps` seconds
/// within `window` seconds either side.
///
/// `scored` holds the planned frames with their trigger score
/// ([`trigger_score`]). New frames skip anything within [`DEDUPE_TOLERANCE`] of a
/// planned frame or of each other, come in time order, and are indexed from
/// `next_index`. A pure function of its inputs, so a resumed scan plans exactly
/// the same frames.
pub fn refine_plan(
    info: &VideoInfo,
    steps: &[SamplingStep],
    scored: &[(FrameRef, f32)],
    threshold: f32,
    next_index: u32,
) -> Vec<FrameRef> {
    let max_seek = max_seek(info);
    let mut picker = Picker::default();
    for (frame, _) in scored {
        picker.seed(frame.timestamp);
    }
    let mut refine: Vec<&SamplingStep> = steps
        .iter()
        .filter(|s| s.method == SamplingKind::Refine)
        .collect();
    refine.sort_by_key(|s| s.step);

    'steps: for step in refine {
        let (Some(min_score), Some(window), Some(fps)) = (step.min_score, step.window, step.fps)
        else {
            continue;
        };
        if fps.is_nan() || window.is_nan() || fps <= 0.0 || window <= 0.0 {
            continue;
        }
        let reach = (window * fps).floor() as i64;
        let mut times: Vec<f64> = scored
            .iter()
            .filter(|(_, score)| (min_score..threshold).contains(score))
            .flat_map(|(frame, _)| (-reach..=reach).map(move |k| frame.timestamp + k as f64 / fps))
            .collect();
        times.sort_by(cmp_f64);
        for t in times {
            if t.is_finite() {
                picker.add(t.clamp(0.0, max_seek), false);
            }
            if scored.len() + picker.chosen.len() >= MAX_PLANNED_FRAMES {
                break 'steps;
            }
        }
    }
    picker.into_frames(next_index)
}

/// A frame's score for the trigger labels: the highest of their scores, or 0
/// when none of them is predicted.
pub fn trigger_score(class: &Classification, labels: &[u32]) -> f32 {
    class
        .predictions
        .iter()
        .filter(|p| labels.contains(&p.label))
        .map(|p| p.score)
        .fold(0.0, f32::max)
}

/// The latest timestamp worth seeking to. Imprecise container durations and
/// variable frame rates mean a timestamp at (or fractionally beyond) `duration`
/// decodes to nothing, so every sample stays at least one frame-duration short
/// of the end.
fn max_seek(info: &VideoInfo) -> f64 {
    let guard = if info.fps > 0.0 {
        (1.0 / info.fps).max(1e-3)
    } else {
        0.05
    };
    (info.duration - guard).max(0.0)
}

/// Frame timestamps in processing order, dropping any within
/// [`DEDUPE_TOLERANCE`] of one already taken. `sorted` mirrors every taken (and
/// seeded) timestamp in time order, so the duplicate check is a binary search
/// against the two nearest neighbours.
#[derive(Default)]
struct Picker {
    chosen: Vec<(f64, bool)>,
    sorted: Vec<f64>,
}

impl Picker {
    /// Count `t` as taken without emitting it (frames planned earlier).
    fn seed(&mut self, t: f64) {
        let pos = self.sorted.partition_point(|&c| c < t);
        self.sorted.insert(pos, t);
    }

    /// Take `t` unless it duplicates a taken or seeded timestamp.
    fn add(&mut self, t: f64, keyframe: bool) {
        let pos = self.sorted.partition_point(|&c| c < t);
        let near_prev = pos > 0 && t - self.sorted[pos - 1] <= DEDUPE_TOLERANCE;
        let near_next = pos < self.sorted.len() && self.sorted[pos] - t <= DEDUPE_TOLERANCE;
        if !(near_prev || near_next) {
            self.sorted.insert(pos, t);
            self.chosen.push((t, keyframe));
        }
    }

    fn into_frames(self, first_index: u32) -> Vec<FrameRef> {
        self.chosen
            .into_iter()
            .enumerate()
            .map(|(i, (timestamp, keyframe))| FrameRef {
                index: first_index + i as u32,
                timestamp,
                keyframe,
            })
            .collect()
    }
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
        VideoInfo {
            duration,
            start_time: 0.0,
            fps,
            frame_count: None,
            width: 0,
            height: 0,
        }
    }

    fn step(n: u32, method: SamplingKind, count: Option<u32>, fps: Option<f64>) -> SamplingStep {
        SamplingStep {
            step: n,
            method,
            fps,
            count,
            nth: None,
            threshold: None,
            min_gap: None,
            min_score: None,
            window: None,
        }
    }

    fn refine_step(n: u32, min_score: f32, window: f64, fps: f64) -> SamplingStep {
        SamplingStep {
            min_score: Some(min_score),
            window: Some(window),
            ..step(n, SamplingKind::Refine, None, Some(fps))
        }
    }

    fn frame(index: u32, timestamp: f64) -> FrameRef {
        FrameRef {
            index,
            timestamp,
            keyframe: false,
        }
    }

    fn classification(preds: &[(u32, f32)]) -> Classification {
        Classification {
            predictions: preds
                .iter()
                .map(|(l, s)| Prediction {
                    label: *l,
                    score: *s,
                })
                .collect(),
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

    #[tokio::test]
    async fn plan_is_capped() {
        // 200k well-separated frames would all survive dedupe; the cap holds.
        let frames = plan(
            Path::new("unused"),
            &info(10_000_000.0, 30.0),
            &[step(1, SamplingKind::Uniform, Some(200_000), None)],
        )
        .await
        .unwrap();
        assert_eq!(frames.len(), MAX_PLANNED_FRAMES);
    }

    #[tokio::test]
    async fn plan_leaves_refine_steps_out() {
        let steps = vec![
            step(1, SamplingKind::Uniform, Some(2), None),
            refine_step(2, 0.1, 1.0, 5.0),
        ];
        let frames = plan(Path::new("unused"), &info(8.0, 30.0), &steps)
            .await
            .unwrap();
        assert_eq!(frames.len(), 2);
    }

    #[test]
    fn refine_looks_closer_only_around_ambiguous_frames() {
        // Planned frames at 2s (confident clean), 5s (ambiguous), 8s (above the
        // trigger: the scan would have stopped, but refine must not anchor on it).
        let scored = [
            (frame(0, 2.0), 0.001),
            (frame(1, 5.0), 0.4),
            (frame(2, 8.0), 0.95),
        ];
        let steps = [refine_step(4, 0.1, 1.0, 2.0)];
        let extra = refine_plan(&info(20.0, 30.0), &steps, &scored, 0.85, 3);
        let times: Vec<f64> = extra.iter().map(|f| f.timestamp).collect();
        // +-1s at 2 fps around 5s, minus 5s itself (already planned).
        assert_eq!(times, vec![4.0, 4.5, 5.5, 6.0]);
        assert_eq!(
            extra.iter().map(|f| f.index).collect::<Vec<_>>(),
            vec![3, 4, 5, 6]
        );
    }

    #[test]
    fn refine_merges_overlapping_windows_and_stays_in_bounds() {
        // Two nearby ambiguous frames share their windows; one at the start
        // clamps to 0 rather than seeking before the video.
        let scored = [
            (frame(0, 0.2), 0.3),
            (frame(1, 10.0), 0.3),
            (frame(2, 10.5), 0.3),
        ];
        let steps = [refine_step(2, 0.1, 1.0, 2.0)];
        let extra = refine_plan(&info(11.0, 30.0), &steps, &scored, 0.85, 3);
        let times: Vec<f64> = extra.iter().map(|f| f.timestamp).collect();
        let max_seek = 11.0 - 1.0 / 30.0;
        assert!(times.iter().all(|&t| (0.0..=max_seek).contains(&t)));
        // No two frames (planned or new) closer than the dedupe tolerance.
        let mut all: Vec<f64> = times.iter().copied().chain([0.2, 10.0, 10.5]).collect();
        all.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert!(all.windows(2).all(|w| w[1] - w[0] > DEDUPE_TOLERANCE));
        // Identical inputs plan identical frames (resume relies on this).
        assert_eq!(
            extra,
            refine_plan(&info(11.0, 30.0), &steps, &scored, 0.85, 3)
        );
    }

    #[test]
    fn refine_adds_nothing_when_every_score_is_confident() {
        let scored = [(frame(0, 1.0), 0.0001), (frame(1, 3.0), 0.02)];
        let steps = [refine_step(2, 0.1, 2.0, 5.0)];
        assert!(refine_plan(&info(10.0, 30.0), &steps, &scored, 0.85, 2).is_empty());
    }

    #[test]
    fn trigger_score_takes_the_highest_trigger_label() {
        let c = classification(&[(0, 0.7), (1, 0.2), (3, 0.6)]);
        assert!((trigger_score(&c, &[1, 3]) - 0.6).abs() < 1e-6);
        assert_eq!(trigger_score(&c, &[9]), 0.0);
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
