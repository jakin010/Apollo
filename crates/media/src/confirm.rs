//! Confirming triggers. A single frame reaching a model's trigger threshold can
//! be the model flickering on one frame rather than the content. With a
//! strategy's `confirm = N`, a frame only counts as a trigger if at least `N`
//! *other* frames within `confirm_window` seconds of it reach the threshold too;
//! when the scan has no frames there, [`targets`] says where to sample.
//!
//! Everything here is a pure function of the frames and their scores, so a
//! resumed scan reaches the same decisions.

use apollo_config::StrategyConfig;

use crate::ffmpeg::VideoInfo;
use crate::sampling::FrameRef;
use crate::strategy::{DEDUPE_TOLERANCE, max_seek};

/// Frame indices at and above this are confirmation frames: the `j`-th frame
/// sampled around the frame with index `c` gets
/// `CONFIRM_INDEX_BASE + c * MAX_TARGETS + j`, so a resumed scan recognises the
/// ones it already has. Planned frames stay far below
/// ([`crate::strategy::MAX_PLANNED_FRAMES`]).
pub const CONFIRM_INDEX_BASE: u32 = 1 << 31;

/// Most frames sampled around one candidate: `confirm` either side, so twice
/// the largest `confirm` the config allows.
pub const MAX_TARGETS: u32 = 16;

/// A strategy's confirmation rule.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Confirm {
    /// Other frames within `window` that must also reach the threshold.
    pub frames: u32,
    /// Seconds either side of the candidate.
    pub window: f64,
}

impl Confirm {
    /// The strategy's rule, or `None` when it trusts single frames.
    pub fn from_strategy(strategy: &StrategyConfig) -> Option<Self> {
        (strategy.confirm > 0).then_some(Self {
            frames: strategy.confirm.min(MAX_TARGETS / 2),
            window: strategy.confirm_window,
        })
    }
}

/// Whether `index` belongs to a frame sampled to confirm another one (such
/// frames are never candidates themselves, so confirmation does not cascade).
pub fn is_confirmation_frame(index: u32) -> bool {
    index >= CONFIRM_INDEX_BASE
}

/// Where to sample to confirm `candidate`: `frames` points either side of it,
/// evenly spaced out to `window` seconds, kept inside the video and apart from
/// each other and the candidate. Each carries its derived index.
pub fn targets(info: &VideoInfo, candidate: &FrameRef, rule: Confirm) -> Vec<FrameRef> {
    let n = rule.frames.min(MAX_TARGETS / 2);
    if n == 0 || is_confirmation_frame(candidate.index) {
        return Vec::new();
    }
    let max_seek = max_seek(info);
    let base = CONFIRM_INDEX_BASE + candidate.index.saturating_mul(MAX_TARGETS);
    let mut out: Vec<FrameRef> = Vec::with_capacity(2 * n as usize);
    for k in 1..=n {
        for (side, sign) in [(0, -1.0), (1, 1.0)] {
            let t = (candidate.timestamp + sign * rule.window * k as f64 / n as f64)
                .clamp(0.0, max_seek);
            let clash = (t - candidate.timestamp).abs() <= DEDUPE_TOLERANCE
                || out
                    .iter()
                    .any(|f| (f.timestamp - t).abs() <= DEDUPE_TOLERANCE);
            if !clash {
                out.push(FrameRef {
                    index: base + 2 * (k - 1) + side,
                    timestamp: t,
                    keyframe: false,
                });
            }
        }
    }
    out
}

/// Whether the frame `i` of `scored` (timestamp, trigger score) is a confirmed
/// trigger: it reaches `threshold` and at least `rule.frames` other frames
/// within `rule.window` seconds do too.
pub fn is_confirmed(scored: &[(f64, f32)], i: usize, threshold: f32, rule: Confirm) -> bool {
    let (t, score) = scored[i];
    score >= threshold
        && scored
            .iter()
            .enumerate()
            .filter(|&(j, &(u, s))| j != i && (u - t).abs() <= rule.window && s >= threshold)
            .count()
            >= rule.frames as usize
}

/// Whether any frame of `scored` is a confirmed trigger.
pub fn any_confirmed(scored: &[(f64, f32)], threshold: f32, rule: Confirm) -> bool {
    (0..scored.len()).any(|i| is_confirmed(scored, i, threshold, rule))
}

/// Trigger scores after confirmation, for the video-level result. A frame at or
/// above `threshold` that is not confirmed counts as its surroundings: the
/// highest score below the threshold among the other frames within the window
/// (0 when there are none). Every other frame keeps its score.
pub fn confirmed_scores(scored: &[(f64, f32)], threshold: f32, rule: Confirm) -> Vec<f32> {
    (0..scored.len())
        .map(|i| {
            let (t, score) = scored[i];
            if score < threshold || is_confirmed(scored, i, threshold, rule) {
                return score;
            }
            scored
                .iter()
                .enumerate()
                .filter(|&(j, &(u, s))| j != i && (u - t).abs() <= rule.window && s < threshold)
                .map(|(_, &(_, s))| s)
                .fold(0.0, f32::max)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const RULE: Confirm = Confirm {
        frames: 1,
        window: 0.25,
    };

    fn info(duration: f64) -> VideoInfo {
        VideoInfo {
            duration,
            start_time: 0.0,
            fps: 30.0,
            frame_count: None,
            width: 0,
            height: 0,
        }
    }

    fn frame(index: u32, timestamp: f64) -> FrameRef {
        FrameRef {
            index,
            timestamp,
            keyframe: false,
        }
    }

    #[test]
    fn targets_sit_either_side_with_derived_indices() {
        let two = Confirm {
            frames: 2,
            window: 0.5,
        };
        let t = targets(&info(60.0), &frame(7, 10.0), two);
        let times: Vec<f64> = t.iter().map(|f| f.timestamp).collect();
        assert_eq!(times, vec![9.75, 10.25, 9.5, 10.5]);
        let base = CONFIRM_INDEX_BASE + 7 * MAX_TARGETS;
        assert_eq!(
            t.iter().map(|f| f.index).collect::<Vec<_>>(),
            vec![base, base + 1, base + 2, base + 3]
        );
        // Same input, same frames: a resumed scan recognises them.
        assert_eq!(t, targets(&info(60.0), &frame(7, 10.0), two));
    }

    #[test]
    fn targets_stay_in_the_video_and_never_cascade() {
        // At the very start the earlier target clamps onto the candidate and is
        // dropped; the later one remains.
        let t = targets(&info(60.0), &frame(0, 0.0), RULE);
        assert_eq!(t.len(), 1);
        assert!((t[0].timestamp - 0.25).abs() < 1e-9);
        // A confirmation frame never asks for confirmation itself.
        assert!(targets(&info(60.0), &frame(CONFIRM_INDEX_BASE + 3, 5.0), RULE).is_empty());
    }

    #[test]
    fn a_lone_spike_is_not_confirmed_and_counts_as_its_surroundings() {
        // 0.9 at 5.0 with neighbours at 0.2 / 0.05: unconfirmed, scores as 0.2.
        let scored = [(4.75, 0.2), (5.0, 0.9), (5.25, 0.05), (9.0, 0.6)];
        assert!(!is_confirmed(&scored, 1, 0.85, RULE));
        assert!(!any_confirmed(&scored, 0.85, RULE));
        let eff = confirmed_scores(&scored, 0.85, RULE);
        assert_eq!(eff, vec![0.2, 0.2, 0.05, 0.6]);
    }

    #[test]
    fn a_supported_trigger_is_confirmed_and_keeps_its_score() {
        // Two adjacent frames above the threshold confirm each other; a frame
        // just outside the window does not count.
        let scored = [(5.0, 0.95), (5.2, 0.9), (5.6, 0.99)];
        assert!(is_confirmed(&scored, 0, 0.85, RULE));
        assert!(is_confirmed(&scored, 1, 0.85, RULE));
        assert!(!is_confirmed(&scored, 2, 0.85, RULE));
        let eff = confirmed_scores(&scored, 0.85, RULE);
        assert_eq!(&eff[..2], &[0.95, 0.9]);
        assert!(eff[2] < 0.85);
    }
}
