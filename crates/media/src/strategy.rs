//! Multi-step planning + aggregation + early-exit decision.
//!
//! A [`Planner`] works out the sampling steps one at a time in `step` order —
//! the engine classifies each step's frames before planning the next, so a scan
//! that triggers early never pays for a later step's probe — dropping any frame
//! within [`DEDUPE_TOLERANCE`] of one already chosen and assigning indices in
//! processing order (cheap steps first). ([`plan`] gives all steps at once.) If
//! nothing triggers, [`refine_plan`] adds the `refine` steps' frames around
//! ambiguous scores, indexed after the plan. [`aggregate`] rolls the kept frames
//! into one result.

use std::cmp::Ordering;

use apollo_config::{Aggregation, SamplingKind, SamplingStep};
use apollo_domain::{Classification, Prediction, select_top};

use crate::error::MediaError;
use crate::ffmpeg::VideoInfo;
use crate::sampling::{self, FrameRef};
use crate::share::VideoShare;

/// Frames within this many seconds of an already-chosen frame are treated as
/// duplicates across steps.
pub const DEDUPE_TOLERANCE: f64 = 0.05;

/// Upper bound on frames planned for one video. Keyframe and scene lists come
/// from the (untrusted) file, so the plan is capped rather than allowed to grow
/// without limit. One hour at 5 fps is 18 000 frames.
pub const MAX_PLANNED_FRAMES: usize = 100_000;

/// Build the whole ordered, de-duplicated frame plan for a video at once — the
/// frames a [`Planner`] gives over all its steps. `refine` steps are left out:
/// they are planned from the scores ([`refine_plan`]).
pub async fn plan(
    share: &VideoShare,
    steps: &[SamplingStep],
    budget: Option<usize>,
) -> Result<Vec<FrameRef>, MediaError> {
    let mut planner = Planner::new(share.info(), steps, budget);
    let mut frames = Vec::new();
    while let Some(step) = planner.next_step(share).await? {
        frames.extend(step);
    }
    Ok(frames)
}

/// Plans a strategy's sampling steps one at a time, in `step` order, so a step's
/// frames — and any ffmpeg probe they need, like the full decode behind scene
/// detection — are only worked out once a scan actually reaches that step.
/// `refine` steps are left out ([`refine_plan`]).
///
/// Frames close to one an earlier step chose are skipped, and indices run on
/// across steps in processing order, exactly as planning everything up front
/// would give: the result depends only on the video and the steps, so a resumed
/// scan plans the same frames.
///
/// `budget` caps the frames planned ([`MAX_PLANNED_FRAMES`] always applies).
/// Steps take frames in order; the step that does not fit is thinned to what is
/// left — keeping its strongest scene cuts, or otherwise spreading its frames
/// evenly across the video — and later steps get none.
pub struct Planner {
    steps: Vec<SamplingStep>,
    next: usize,
    max_seek: f64,
    remaining: usize,
    picker: Picker,
}

impl Planner {
    pub fn new(info: &VideoInfo, steps: &[SamplingStep], budget: Option<usize>) -> Self {
        let mut steps: Vec<SamplingStep> = steps
            .iter()
            .filter(|s| s.method != SamplingKind::Refine)
            .cloned()
            .collect();
        steps.sort_by_key(|s| s.step);
        Self {
            steps,
            next: 0,
            max_seek: max_seek(info),
            remaining: budget.map_or(MAX_PLANNED_FRAMES, |b| b.min(MAX_PLANNED_FRAMES)),
            picker: Picker::default(),
        }
    }

    /// The step the next [`Planner::next_step`] call plans, if there is one
    /// and budget left for it (to start its probe early).
    pub fn upcoming(&self) -> Option<&SamplingStep> {
        self.steps.get(self.next).filter(|_| self.remaining > 0)
    }

    /// Frames planned so far — also the index the next one gets.
    pub fn planned(&self) -> usize {
        self.picker.chosen.len()
    }

    /// Plan the next step: its new frames in processing order, indexed after
    /// those already planned (possibly none, when they all duplicate earlier
    /// frames). `None` once every step is planned or the budget is spent.
    pub async fn next_step(
        &mut self,
        share: &VideoShare,
    ) -> Result<Option<Vec<FrameRef>>, MediaError> {
        if self.remaining == 0 || self.next >= self.steps.len() {
            return Ok(None);
        }
        let step = &self.steps[self.next];
        self.next += 1;
        let mut samples: Vec<(f64, Option<f32>)> = sampling::step_samples(share, step)
            .await?
            .into_iter()
            .filter(|(t, _)| t.is_finite())
            .map(|(t, score)| (t.clamp(0.0, self.max_seek), score))
            .collect();
        samples.sort_by(|a, b| cmp_f64(&a.0, &b.0));
        // What this step would add: not near an earlier step's frame or another
        // of its own (time order, so only the previous one can be near).
        let mut fresh: Vec<(f64, Option<f32>)> = Vec::new();
        for (t, score) in samples {
            let near_last = fresh
                .last()
                .is_some_and(|&(u, _)| t - u <= DEDUPE_TOLERANCE);
            if !near_last && !self.picker.is_dup(t) {
                fresh.push((t, score));
            }
        }
        let keyframe = step.method == SamplingKind::Iframes;
        let kept = within_budget(fresh, self.remaining);
        self.remaining -= kept.len();
        let first = self.picker.chosen.len();
        for (t, _) in kept {
            self.picker.add(t, keyframe);
        }
        Ok(Some(
            self.picker.chosen[first..]
                .iter()
                .enumerate()
                .map(|(i, &(timestamp, keyframe))| FrameRef {
                    index: (first + i) as u32,
                    timestamp,
                    keyframe,
                })
                .collect(),
        ))
    }
}

/// The frames of one step that fit in `budget`, in time order: all of them if
/// they fit; else the highest-priority ones (scene cuts by score), or, without
/// priorities, an even spread across the step's frames.
fn within_budget(fresh: Vec<(f64, Option<f32>)>, budget: usize) -> Vec<(f64, Option<f32>)> {
    if fresh.len() <= budget {
        return fresh;
    }
    if budget == 0 {
        return Vec::new();
    }
    if fresh.iter().all(|(_, score)| score.is_some()) {
        let mut idx: Vec<usize> = (0..fresh.len()).collect();
        idx.sort_by(|&a, &b| {
            fresh[b]
                .1
                .partial_cmp(&fresh[a].1)
                .unwrap_or(Ordering::Equal)
                .then(a.cmp(&b))
        });
        idx.truncate(budget);
        idx.sort_unstable();
        idx.into_iter().map(|i| fresh[i]).collect()
    } else {
        let n = fresh.len() as f64;
        (0..budget)
            .map(|i| fresh[((i as f64 + 0.5) * n / budget as f64) as usize])
            .collect()
    }
}

/// Frames the strategy's `refine` steps add once every planned frame has been
/// classified without a trigger: for each planned frame whose trigger score is
/// ambiguous (`min_score <= score < threshold`), frames every `1 / fps` seconds
/// within `window` seconds either side.
///
/// `scored` holds the planned frames with their trigger score
/// ([`trigger_score`]). At most `limit` frames are added: the most ambiguous
/// frames (highest score) are looked at first, nearest frames first. New frames
/// skip anything within [`DEDUPE_TOLERANCE`] of a planned frame or of each
/// other and are indexed from `next_index` in that order. A pure function of its
/// inputs, so a resumed scan plans exactly the same frames.
pub fn refine_plan(
    info: &VideoInfo,
    steps: &[SamplingStep],
    scored: &[(FrameRef, f32)],
    threshold: f32,
    next_index: u32,
    limit: usize,
) -> Vec<FrameRef> {
    let max_seek = max_seek(info);
    let limit = limit.min(MAX_PLANNED_FRAMES.saturating_sub(scored.len()));
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
        let mut anchors: Vec<(f64, f32)> = scored
            .iter()
            .filter(|(_, score)| (min_score..threshold).contains(score))
            .map(|(frame, score)| (frame.timestamp, *score))
            .collect();
        anchors.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(Ordering::Equal)
                .then(cmp_f64(&a.0, &b.0))
        });
        for (anchor, _) in anchors {
            for k in 1..=reach {
                for sign in [-1.0, 1.0] {
                    if picker.chosen.len() >= limit {
                        break 'steps;
                    }
                    let t = anchor + sign * k as f64 / fps;
                    if t.is_finite() {
                        picker.add(t.clamp(0.0, max_seek), false);
                    }
                }
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
pub(crate) fn max_seek(info: &VideoInfo) -> f64 {
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

    /// Whether `t` is within [`DEDUPE_TOLERANCE`] of a taken or seeded timestamp.
    fn is_dup(&self, t: f64) -> bool {
        let pos = self.sorted.partition_point(|&c| c < t);
        (pos > 0 && t - self.sorted[pos - 1] <= DEDUPE_TOLERANCE)
            || (pos < self.sorted.len() && self.sorted[pos] - t <= DEDUPE_TOLERANCE)
    }

    /// Take `t` unless it duplicates a taken or seeded timestamp.
    fn add(&mut self, t: f64, keyframe: bool) {
        if !self.is_dup(t) {
            let pos = self.sorted.partition_point(|&c| c < t);
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

    fn share(info: VideoInfo) -> VideoShare {
        VideoShare::new("unused".into(), info)
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
        let frames = plan(&share(info(9.0, 30.0)), &steps, None).await.unwrap();
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
        let frames = plan(&share(info(8.0, 30.0)), &steps, None).await.unwrap();
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
            &share(info(60.0, 30.0)),
            &[step(1, SamplingKind::Uniform, Some(2000), None)],
            None,
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
            &share(info(10_000_000.0, 30.0)),
            &[step(1, SamplingKind::Uniform, Some(200_000), None)],
            None,
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
        let frames = plan(&share(info(8.0, 30.0)), &steps, None).await.unwrap();
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
        let extra = refine_plan(&info(20.0, 30.0), &steps, &scored, 0.85, 3, usize::MAX);
        let times: Vec<f64> = extra.iter().map(|f| f.timestamp).collect();
        // +-1s at 2 fps around 5s (nearest first), minus 5s itself (planned).
        assert_eq!(times, vec![4.5, 5.5, 4.0, 6.0]);
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
        let extra = refine_plan(&info(11.0, 30.0), &steps, &scored, 0.85, 3, usize::MAX);
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
            refine_plan(&info(11.0, 30.0), &steps, &scored, 0.85, 3, usize::MAX)
        );
    }

    #[tokio::test]
    async fn budget_keeps_earlier_steps_and_thins_the_one_that_overflows() {
        // uniform 4 on 8s -> [1,3,5,7]; fps 1 would add [0,2,4,6]. A budget of 6
        // keeps all of step 1 and 2 of step 2, spread evenly (the middle of each
        // half: 2 and 6), and step 3 gets nothing.
        let steps = vec![
            step(1, SamplingKind::Uniform, Some(4), None),
            step(2, SamplingKind::Fps, None, Some(1.0)),
            step(3, SamplingKind::Uniform, Some(3), None),
        ];
        let frames = plan(&share(info(8.0, 30.0)), &steps, Some(6))
            .await
            .unwrap();
        let times: Vec<f64> = frames.iter().map(|f| f.timestamp).collect();
        assert_eq!(times, vec![1.0, 3.0, 5.0, 7.0, 2.0, 6.0]);
    }

    #[test]
    fn budget_keeps_the_strongest_scene_cuts() {
        let cuts = vec![
            (1.0, Some(0.4)),
            (2.0, Some(0.9)),
            (3.0, Some(0.35)),
            (4.0, Some(0.6)),
        ];
        let kept: Vec<f64> = within_budget(cuts, 2).into_iter().map(|(t, _)| t).collect();
        assert_eq!(kept, vec![2.0, 4.0]);
    }

    #[test]
    fn refine_spends_a_limit_on_the_most_ambiguous_frame_first() {
        let scored = [(frame(0, 2.0), 0.2), (frame(1, 8.0), 0.7)];
        let steps = [refine_step(4, 0.1, 1.0, 2.0)];
        let extra = refine_plan(&info(20.0, 30.0), &steps, &scored, 0.85, 2, 3);
        let times: Vec<f64> = extra.iter().map(|f| f.timestamp).collect();
        // 0.7 at 8s is looked at first, nearest first; the limit stops at 3.
        assert_eq!(times, vec![7.5, 8.5, 7.0]);
    }

    #[test]
    fn refine_adds_nothing_when_every_score_is_confident() {
        let scored = [(frame(0, 1.0), 0.0001), (frame(1, 3.0), 0.02)];
        let steps = [refine_step(2, 0.1, 2.0, 5.0)];
        assert!(refine_plan(&info(10.0, 30.0), &steps, &scored, 0.85, 2, usize::MAX).is_empty());
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
