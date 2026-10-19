//! Result shapes: `Prediction`, `Classification`, and `Scan` (`aggregated`, the
//! flagged `Segment`s, and per-sample `Sample`s with times).

use serde::{Deserialize, Serialize};

/// A single (category, score) prediction. `label` is an integer id — a class
/// index (vit), a label-list index (siglip with plain labels), or a taxonomy
/// child-category id (siglip with a taxonomy) — and `name` its readable name.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Prediction {
    pub label: u32,
    /// Empty for results stored before names were recorded.
    #[serde(default)]
    pub name: String,
    pub score: f32,
}

impl Prediction {
    pub fn new(label: u32, name: impl Into<String>, score: f32) -> Self {
        Self {
            label,
            name: name.into(),
            score,
        }
    }
}

/// One of a model's labels: the id predictions carry, and its readable name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Label {
    pub id: u32,
    pub name: String,
}

impl Label {
    /// Labels whose ids are their positions in `names`.
    pub fn indexed(names: &[String]) -> Vec<Label> {
        names
            .iter()
            .enumerate()
            .map(|(i, name)| Label {
                id: i as u32,
                name: name.clone(),
            })
            .collect()
    }

    pub fn predict(&self, score: f32) -> Prediction {
        Prediction::new(self.id, self.name.clone(), score)
    }
}

/// A set of predictions. The returned set is the top 5 unioned with any label
/// scoring above 0.90 (assembled in `apollo-engine`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Classification {
    /// Flat predictions. For a siglip taxonomy model the `label` is the child
    /// category id (the per-parent grouping is left to the caller).
    pub predictions: Vec<Prediction>,
}

/// One classified sample of a scan: a video frame, where `start == end`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    /// Position in processing order.
    pub index: u32,
    /// Seconds into the media.
    #[serde(alias = "timestamp")]
    pub start: f64,
    /// Seconds into the media; equal to `start` for a frame (stored frames
    /// from before samples had an end read back as 0 and mean `start`).
    #[serde(default)]
    pub end: f64,
    pub classification: Classification,
}

impl Sample {
    /// A video frame at `timestamp`.
    pub fn frame(index: u32, timestamp: f64, classification: Classification) -> Self {
        Self {
            index,
            start: timestamp,
            end: timestamp,
            classification,
        }
    }

    /// The sample's end, never before its start.
    pub fn end(&self) -> f64 {
        self.end.max(self.start)
    }
}

/// A stretch where a trigger label reached its threshold on consecutive
/// samples (in time order, with no classified sample below it in between).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Segment {
    pub label: u32,
    pub name: String,
    /// First and last flagged sample, in seconds.
    pub start: f64,
    pub end: f64,
    /// Highest score in the segment, and where it was.
    pub peak: f32,
    pub peak_at: f64,
    /// Flagged samples in the segment.
    pub samples: u32,
    /// The nearest sample below the threshold on either side (or the media's
    /// start / end): how far the segment may extend.
    pub bound_start: f64,
    pub bound_end: f64,
}

/// An image classifier applied to a video's sampled frames.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Scan {
    /// Rolled up across `samples` via the strategy aggregation (max / mean).
    pub aggregated: Classification,
    /// Where the model's trigger labels fired.
    #[serde(default)]
    pub segments: Vec<Segment>,
    /// Every sample actually classified, in time order.
    #[serde(alias = "frames")]
    pub samples: Vec<Sample>,
}

/// The output of a completed model run — the API `oneof`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ModelOutput {
    /// Image input, or a whole-clip video-classifier.
    Classification(Classification),
    /// An image-classifier run over a video.
    #[serde(alias = "FrameScan")]
    Scan(Scan),
}

/// Select the predictions to return: the top 5 by score, unioned with any label
/// scoring above 0.90. Input need not be sorted; output is sorted high-to-low.
pub fn select_top(mut preds: Vec<Prediction>) -> Vec<Prediction> {
    preds.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    preds
        .into_iter()
        .enumerate()
        .filter(|(i, p)| *i < 5 || p.score > 0.90)
        .map(|(_, p)| p)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(label: u32, score: f32) -> Prediction {
        Prediction::new(label, "", score)
    }

    #[test]
    fn reads_results_stored_before_the_rename() {
        let old = r#"{"FrameScan":{"aggregated":{"predictions":[{"label":1,"score":0.9}]},
            "frames":[{"timestamp":2.5,"index":0,"classification":{"predictions":[]}}]}}"#;
        let ModelOutput::Scan(scan) = serde_json::from_str::<ModelOutput>(old).unwrap() else {
            panic!("expected a scan");
        };
        assert_eq!(scan.aggregated.predictions[0].name, "");
        assert_eq!(scan.samples[0].start, 2.5);
        assert_eq!(scan.samples[0].end(), 2.5);
        assert!(scan.segments.is_empty());
    }

    #[test]
    fn keeps_five_by_default() {
        let preds = vec![
            p(1, 0.8),
            p(2, 0.7),
            p(3, 0.6),
            p(4, 0.5),
            p(5, 0.4),
            p(6, 0.3),
            p(7, 0.2),
        ];
        let out = select_top(preds);
        assert_eq!(out.len(), 5);
        assert_eq!(out[0].label, 1);
    }

    #[test]
    fn unions_high_confidence_beyond_five() {
        let preds = vec![
            p(1, 0.95),
            p(2, 0.94),
            p(3, 0.93),
            p(4, 0.92),
            p(5, 0.91),
            p(6, 0.905),
        ];
        // all six exceed 0.90, so all six are returned even though that is > 5
        assert_eq!(select_top(preds).len(), 6);
    }
}
