//! Result and lifecycle-state assembly.

use apollo_config::Aggregation;
use apollo_domain::{Classification, Frame, FrameScan, ItemState, Task, TaskState};
use apollo_media::Confirm;

/// Whether an item has reached a terminal state.
pub(crate) fn item_terminal(state: ItemState) -> bool {
    matches!(
        state,
        ItemState::Completed | ItemState::Failed | ItemState::Cancelled
    )
}

/// Whether a task has reached a terminal (final) state.
pub(crate) fn task_terminal(state: TaskState) -> bool {
    matches!(
        state,
        TaskState::Completed | TaskState::Failed | TaskState::Cancelled
    )
}

/// The task state implied by its items: `Completed` once every item is terminal,
/// otherwise `Processing`. The single source of truth for the task-completion
/// invariant — the scheduler rolls the task up through this as each item finishes.
pub(crate) fn task_state_for(task: &Task) -> TaskState {
    if task.items.iter().all(|it| item_terminal(it.state)) {
        TaskState::Completed
    } else {
        TaskState::Processing
    }
}

/// The trigger a frame scan confirms: the model's trigger labels and threshold,
/// and the strategy's confirmation rule.
pub(crate) struct Confirming<'a> {
    pub labels: &'a [u32],
    pub threshold: f32,
    pub rule: Confirm,
}

/// Assemble a [`FrameScan`] from classified frames: order by index, then roll up
/// via the strategy aggregation (delegates to [`apollo_media::aggregate`]).
///
/// With `confirming`, an unconfirmed frame's trigger-label scores count as its
/// surroundings in the roll-up ([`apollo_media::confirm::confirmed_scores`]), so a
/// lone spike cannot flag the video; the per-frame results stay as classified.
pub(crate) fn frame_scan(
    mut frames: Vec<Frame>,
    aggregation: Aggregation,
    confirming: Option<Confirming<'_>>,
) -> FrameScan {
    frames.sort_by_key(|f| f.index);
    let mut per_frame: Vec<Classification> =
        frames.iter().map(|f| f.classification.clone()).collect();
    if let Some(c) = confirming {
        let scored: Vec<(f64, f32)> = frames
            .iter()
            .map(|f| {
                let score = apollo_media::trigger_score(&f.classification, c.labels);
                (f.timestamp, score)
            })
            .collect();
        let confirmed = apollo_media::confirm::confirmed_scores(&scored, c.threshold, c.rule);
        for ((class, (_, raw)), cap) in per_frame.iter_mut().zip(&scored).zip(confirmed) {
            if cap < *raw {
                for p in &mut class.predictions {
                    if c.labels.contains(&p.label) {
                        p.score = p.score.min(cap);
                    }
                }
            }
        }
    }
    let aggregated = apollo_media::aggregate(&per_frame, aggregation);
    FrameScan { aggregated, frames }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apollo_domain::Prediction;

    fn frame(index: u32, timestamp: f64, nsfw: f32) -> Frame {
        Frame {
            timestamp,
            index,
            classification: Classification {
                predictions: vec![
                    Prediction {
                        label: 0,
                        score: 1.0 - nsfw,
                    },
                    Prediction {
                        label: 1,
                        score: nsfw,
                    },
                ],
            },
        }
    }

    fn nsfw(scan: &FrameScan) -> f32 {
        let p = scan.aggregated.predictions.iter().find(|p| p.label == 1);
        p.map_or(0.0, |p| p.score)
    }

    fn confirming() -> Option<Confirming<'static>> {
        Some(Confirming {
            labels: &[1],
            threshold: 0.85,
            rule: Confirm {
                frames: 1,
                window: 0.25,
            },
        })
    }

    #[test]
    fn a_lone_spike_does_not_flag_the_video_when_confirming() {
        // One frame at 0.9; the frames sampled either side of it score low.
        let frames = vec![
            frame(0, 2.0, 0.01),
            frame(1, 5.0, 0.9),
            frame(2, 4.75, 0.3),
            frame(3, 5.25, 0.02),
        ];
        let plain = frame_scan(frames.clone(), Aggregation::Max, None);
        assert!((nsfw(&plain) - 0.9).abs() < 1e-6);

        let confirmed = frame_scan(frames, Aggregation::Max, confirming());
        // The spike counts as its surroundings (0.3), so the video is not flagged.
        assert!((nsfw(&confirmed) - 0.3).abs() < 1e-6);
        // Per-frame results are reported as classified.
        let spike = confirmed.frames.iter().find(|f| f.index == 1).unwrap();
        assert!((spike.classification.predictions[1].score - 0.9).abs() < 1e-6);
    }

    #[test]
    fn a_supported_trigger_still_flags_the_video() {
        let frames = vec![frame(0, 5.0, 0.95), frame(1, 5.2, 0.9)];
        let scan = frame_scan(frames, Aggregation::Max, confirming());
        assert!((nsfw(&scan) - 0.95).abs() < 1e-6);
    }
}
