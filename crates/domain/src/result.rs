//! Result shapes: `Prediction`, `Classification`, and `FrameScan` (`aggregated`
//! plus per-frame `Frame`s with timestamps).

use serde::{Deserialize, Serialize};

/// A single (category-id, score) prediction. `label` is an integer id — a class
/// index (vit), a label-list index (siglip with plain labels), or a taxonomy
/// child-category id (siglip with a taxonomy) — never a name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Prediction {
    pub label: u32,
    pub score: f32,
}

/// A set of predictions. The returned set is the top 5 unioned with any label
/// scoring above 0.90 (assembled in `apollo-engine`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Classification {
    /// Flat predictions. For a siglip taxonomy model the `label` is the child
    /// category id (the per-parent grouping is left to the caller).
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
        Prediction { label, score }
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
