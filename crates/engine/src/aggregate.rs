//! Result and lifecycle-state assembly.

use apollo_config::Aggregation;
use apollo_domain::{Classification, ItemState, Sample, Scan, Segment, Task, TaskError, TaskState};
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

/// The task state implied by its items: once every item is terminal, `Failed`
/// when every item failed and `Completed` otherwise; until then `Processing`.
/// The single source of truth for the task-completion invariant — the
/// scheduler rolls the task up through this as each item finishes.
pub(crate) fn task_state_for(task: &Task) -> TaskState {
    if !task.items.iter().all(|it| item_terminal(it.state)) {
        TaskState::Processing
    } else if !task.items.is_empty() && task.items.iter().all(|it| it.state == ItemState::Failed) {
        TaskState::Failed
    } else {
        TaskState::Completed
    }
}

/// Why a task failed: its (first) failed item's error.
pub fn task_error(task: &Task) -> Option<&TaskError> {
    if task.state != TaskState::Failed {
        return None;
    }
    task.items
        .iter()
        .filter(|it| it.state == ItemState::Failed)
        .find_map(|it| it.error.as_ref())
}

/// What counts as flagged in a scan: the model's trigger labels and threshold,
/// and the strategy's confirmation rule (if any).
pub(crate) struct Flagging<'a> {
    pub labels: &'a [u32],
    pub threshold: f32,
    pub rule: Option<Confirm>,
}

/// Assemble a [`Scan`] from classified samples: order them by time, roll them
/// up via the strategy aggregation (delegates to [`apollo_media::aggregate`]),
/// and, with `flagging`, find the [`Segment`]s where a trigger label reached its
/// threshold. `duration` is the media's length, the last segment's outer bound.
///
/// With a confirmation rule, an unconfirmed sample's trigger-label scores count
/// as its surroundings in the roll-up and the segments
/// ([`apollo_media::confirm::confirmed_scores`]), so a lone spike cannot flag the
/// video; the per-sample results stay as classified.
pub(crate) fn scan(
    mut samples: Vec<Sample>,
    aggregation: Aggregation,
    flagging: Option<Flagging<'_>>,
    duration: f64,
) -> Scan {
    samples.sort_by(|a, b| a.start.total_cmp(&b.start).then(a.index.cmp(&b.index)));
    let mut effective: Vec<Classification> =
        samples.iter().map(|s| s.classification.clone()).collect();
    let mut segments = Vec::new();
    if let Some(f) = flagging {
        if let Some(rule) = f.rule {
            let scored: Vec<(f64, f32)> = samples
                .iter()
                .map(|s| {
                    let score = apollo_media::trigger_score(&s.classification, f.labels);
                    (s.start, score)
                })
                .collect();
            let confirmed = apollo_media::confirm::confirmed_scores(&scored, f.threshold, rule);
            for ((class, (_, raw)), cap) in effective.iter_mut().zip(&scored).zip(confirmed) {
                if cap < *raw {
                    for p in &mut class.predictions {
                        if f.labels.contains(&p.label) {
                            p.score = p.score.min(cap);
                        }
                    }
                }
            }
        }
        segments = find_segments(&samples, &effective, f.labels, f.threshold, duration);
    }
    let aggregated = apollo_media::aggregate(&effective, aggregation);
    Scan {
        aggregated,
        segments,
        samples,
    }
}

/// Per trigger label, merge time-adjacent samples scoring at or above
/// `threshold` (by `scores`, parallel to the time-ordered `samples`) into
/// segments. A segment's bounds are the nearest sample below the threshold on
/// either side, or the media's edges. Ordered by start, then label.
fn find_segments(
    samples: &[Sample],
    scores: &[Classification],
    labels: &[u32],
    threshold: f32,
    duration: f64,
) -> Vec<Segment> {
    let mut out = Vec::new();
    for &label in labels {
        let mut open: Option<Segment> = None;
        // End of the last sample below the threshold: the next segment's bound.
        let mut last_clear = 0.0;
        for (sample, class) in samples.iter().zip(scores) {
            let flagged = class
                .predictions
                .iter()
                .find(|p| p.label == label && p.score >= threshold);
            match flagged {
                Some(p) => {
                    let seg = open.get_or_insert_with(|| Segment {
                        label,
                        name: p.name.clone(),
                        start: sample.start,
                        end: sample.end(),
                        peak: f32::MIN,
                        peak_at: sample.start,
                        samples: 0,
                        bound_start: last_clear,
                        bound_end: duration,
                    });
                    seg.end = sample.end();
                    seg.samples += 1;
                    if p.score > seg.peak {
                        seg.peak = p.score;
                        seg.peak_at = sample.start;
                    }
                }
                None => {
                    if let Some(mut seg) = open.take() {
                        seg.bound_end = sample.start;
                        out.push(seg);
                    }
                    last_clear = sample.end();
                }
            }
        }
        if let Some(mut seg) = open {
            seg.bound_end = seg.bound_end.max(seg.end);
            out.push(seg);
        }
    }
    out.sort_by(|a, b| a.start.total_cmp(&b.start).then(a.label.cmp(&b.label)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use apollo_domain::Prediction;

    fn sample(index: u32, timestamp: f64, nsfw: f32) -> Sample {
        Sample::frame(
            index,
            timestamp,
            Classification {
                predictions: vec![
                    Prediction::new(0, "sfw", 1.0 - nsfw),
                    Prediction::new(1, "nsfw", nsfw),
                ],
            },
        )
    }

    fn nsfw(scan: &Scan) -> f32 {
        let p = scan.aggregated.predictions.iter().find(|p| p.label == 1);
        p.map_or(0.0, |p| p.score)
    }

    fn flagging(rule: Option<Confirm>) -> Option<Flagging<'static>> {
        Some(Flagging {
            labels: &[1],
            threshold: 0.85,
            rule,
        })
    }

    const CONFIRM: Option<Confirm> = Some(Confirm {
        frames: 1,
        window: 0.25,
    });

    #[test]
    fn a_lone_spike_does_not_flag_the_video_when_confirming() {
        // One frame at 0.9; the frames sampled either side of it score low.
        let samples = vec![
            sample(0, 2.0, 0.01),
            sample(1, 5.0, 0.9),
            sample(2, 4.75, 0.3),
            sample(3, 5.25, 0.02),
        ];
        let plain = scan(samples.clone(), Aggregation::Max, None, 10.0);
        assert!((nsfw(&plain) - 0.9).abs() < 1e-6);

        let confirmed = scan(samples, Aggregation::Max, flagging(CONFIRM), 10.0);
        // The spike counts as its surroundings (0.3), so the video is not flagged.
        assert!((nsfw(&confirmed) - 0.3).abs() < 1e-6);
        assert!(confirmed.segments.is_empty());
        // Per-sample results are reported as classified.
        let spike = confirmed.samples.iter().find(|s| s.index == 1).unwrap();
        assert!((spike.classification.predictions[1].score - 0.9).abs() < 1e-6);
    }

    #[test]
    fn a_supported_trigger_still_flags_the_video() {
        let samples = vec![sample(0, 5.0, 0.95), sample(1, 5.2, 0.9)];
        let scan = scan(samples, Aggregation::Max, flagging(CONFIRM), 10.0);
        assert!((nsfw(&scan) - 0.95).abs() < 1e-6);
        assert_eq!(scan.segments.len(), 1);
    }

    #[test]
    fn samples_come_back_in_time_order() {
        // A refine frame (high index) sampled between earlier ones.
        let samples = vec![
            sample(0, 0.0, 0.1),
            sample(1, 4.0, 0.1),
            sample(2, 2.0, 0.1),
        ];
        let scan = scan(samples, Aggregation::Max, None, 5.0);
        let order: Vec<u32> = scan.samples.iter().map(|s| s.index).collect();
        assert_eq!(order, [0, 2, 1]);
    }

    #[test]
    fn flagged_samples_merge_into_segments_bounded_by_clear_ones() {
        let samples = vec![
            sample(0, 0.0, 0.1),
            sample(1, 10.0, 0.9),
            sample(2, 20.0, 0.97),
            sample(3, 30.0, 0.2),
            sample(4, 40.0, 0.88),
            // Sampled later (refine), but in time between 1 and 2: still one
            // segment, since it is flagged too.
            sample(5, 15.0, 0.86),
        ];
        let scan = scan(samples, Aggregation::Max, flagging(None), 45.0);
        assert_eq!(scan.segments.len(), 2);

        let first = &scan.segments[0];
        assert_eq!((first.label, first.name.as_str()), (1, "nsfw"));
        assert_eq!((first.start, first.end), (10.0, 20.0));
        assert_eq!((first.bound_start, first.bound_end), (0.0, 30.0));
        assert_eq!(first.samples, 3);
        assert!((first.peak - 0.97).abs() < 1e-6);
        assert_eq!(first.peak_at, 20.0);

        // The last one runs to the end of the media.
        let last = &scan.segments[1];
        assert_eq!((last.start, last.end), (40.0, 40.0));
        assert_eq!((last.bound_start, last.bound_end), (30.0, 45.0));
    }

    #[test]
    fn a_clear_sample_splits_a_segment() {
        let samples = vec![
            sample(0, 1.0, 0.9),
            sample(1, 2.0, 0.5),
            sample(2, 3.0, 0.9),
        ];
        let scan = scan(samples, Aggregation::Max, flagging(None), 4.0);
        let spans: Vec<_> = scan
            .segments
            .iter()
            .map(|s| (s.bound_start, s.start, s.end, s.bound_end))
            .collect();
        assert_eq!(spans, [(0.0, 1.0, 1.0, 2.0), (2.0, 3.0, 3.0, 4.0)]);
    }

    #[test]
    fn no_trigger_means_no_segments() {
        let samples = vec![sample(0, 1.0, 0.99)];
        assert!(
            scan(samples, Aggregation::Max, None, 2.0)
                .segments
                .is_empty()
        );
    }

    #[test]
    fn a_task_whose_only_item_failed_is_failed() {
        use apollo_domain::{Input, Item, Url};
        let mut item = Item::new(
            Input::Image(Url {
                main: "x".into(),
                fallback: None,
            }),
            vec!["m".into()],
            None,
        );
        item.state = ItemState::Failed;
        item.error = Some(TaskError::fetch("unreachable"));
        let mut task = Task::new("t", vec![item.clone()], None);
        assert_eq!(task_state_for(&task), TaskState::Failed);
        task.state = TaskState::Failed;
        assert_eq!(task_error(&task).unwrap().message, "unreachable");

        item.state = ItemState::Completed;
        task.items.push(item);
        assert_eq!(task_state_for(&task), TaskState::Completed);
    }
}
