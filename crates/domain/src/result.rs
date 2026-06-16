//! Result shapes: `Prediction`, `Classification`, and `FrameScan` (`aggregated`
//! plus per-frame `Frame`s with timestamps).

use serde::{Deserialize, Serialize};

/// A single (label, score) prediction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Prediction {
    pub label: String,
    pub score: f32,
}

/// A set of predictions. The returned set is the top 5 unioned with any label
/// scoring above 0.90 (assembled in `apollo-engine`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Classification {
    pub predictions: Vec<Prediction>,
}

/// One classified video frame.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Frame {
    /// Seconds into the video.
    pub timestamp: f64,
    /// Ordinal among sampled frames.
    pub index: u32,
    pub classification: Classification,
}

/// An image-classifier applied to a video, frame by frame.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FrameScan {
    /// Rolled up across `frames` via the strategy aggregation (max / mean).
    pub aggregated: Classification,
    /// Only the frames actually classified (early exit truncates this list).
    pub frames: Vec<Frame>,
}

/// The output of a completed model run — the API `oneof`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ModelOutput {
    /// Image input, or a whole-clip video-classifier.
    Classification(Classification),
    /// An image-classifier run over a video.
    FrameScan(FrameScan),
}

/// Select the predictions to return: the top 5 by score, unioned with any label
/// scoring above 0.90. Input need not be sorted; output is sorted high-to-low.
pub fn select_top(mut preds: Vec<Prediction>) -> Vec<Prediction> {
    preds.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
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

    fn p(label: &str, score: f32) -> Prediction {
        Prediction { label: label.into(), score }
    }

    #[test]
    fn keeps_five_by_default() {
        let preds = vec![
            p("a", 0.8), p("b", 0.7), p("c", 0.6),
            p("d", 0.5), p("e", 0.4), p("f", 0.3), p("g", 0.2),
        ];
        let out = select_top(preds);
        assert_eq!(out.len(), 5);
        assert_eq!(out[0].label, "a");
    }

    #[test]
    fn unions_high_confidence_beyond_five() {
        let preds = vec![
            p("a", 0.95), p("b", 0.94), p("c", 0.93),
            p("d", 0.92), p("e", 0.91), p("f", 0.905),
        ];
        // all six exceed 0.90, so all six are returned even though that is > 5
        assert_eq!(select_top(preds).len(), 6);
    }
}
