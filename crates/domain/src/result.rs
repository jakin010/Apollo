//! Result shapes: `Prediction`, `Classification`, `Scan` (`aggregated`, the
//! flagged `Segment`s, and per-sample `Sample`s with times), and `TextOutput`
//! (text a model read, with where it appears in a video).

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

/// Text a model read (OCR) from an image or a video.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TextOutput {
    /// All of it: an image's text, or a video's parts joined by newlines.
    pub text: String,
    /// The language, when the model reports one (empty for OCR).
    #[serde(default)]
    pub language: String,
    /// For a video, where each part of the text appears, in time order;
    /// empty for an image.
    #[serde(default)]
    pub parts: Vec<TimedText>,
    /// For a video, the times of the sampled frames on which no text was
    /// read, in time order. Kept so that classifying the parts can tell a
    /// stretch without text from one that was not sampled (not part of the
    /// API).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blank: Vec<f64>,
}

/// Text seen from `start` to `end`: on consecutive sampled frames of a video.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimedText {
    /// First and last sample it was read on, in seconds.
    pub start: f64,
    pub end: f64,
    pub text: String,
}

impl TextOutput {
    /// A still image's text.
    pub fn still(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            ..Self::default()
        }
    }

    /// The text read on a video's sampled frames, given as (time, text) in
    /// time order: consecutive frames with the same text (whitespace,
    /// line breaks included, aside) become one part, laid out as on its first
    /// frame. Frames without text are listed in `blank`; they end a part, so
    /// text that disappears and comes back is two parts.
    pub fn from_frames<'a>(frames: impl IntoIterator<Item = (f64, &'a str)>) -> Self {
        let mut parts: Vec<TimedText> = Vec::new();
        let mut blank = Vec::new();
        // The words of the part the previous frame continued (none after a
        // frame without text).
        let mut open: Option<String> = None;
        for (at, text) in frames {
            let text = normalize_whitespace(text);
            if text.is_empty() {
                blank.push(at);
                open = None;
                continue;
            }
            let words = text.split_whitespace().collect::<Vec<_>>().join(" ");
            match parts.last_mut() {
                Some(last) if open.as_deref() == Some(words.as_str()) => last.end = at,
                _ => parts.push(TimedText {
                    start: at,
                    end: at,
                    text,
                }),
            }
            open = Some(words);
        }
        let text = parts
            .iter()
            .map(|p| p.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        Self {
            text,
            language: String::new(),
            parts,
            blank,
        }
    }
}

/// `text` with each run of whitespace made one space (a newline stays one
/// newline), trimmed.
fn normalize_whitespace(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for line in text.lines() {
        let line = line.split_whitespace().collect::<Vec<_>>().join(" ");
        if line.is_empty() {
            continue;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&line);
    }
    out
}

/// The output of a completed model run — the API `oneof`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ModelOutput {
    /// Image input, or a whole-clip video-classifier.
    Classification(Classification),
    /// An image-classifier run over a video.
    #[serde(alias = "FrameScan")]
    Scan(Scan),
    /// Text a model read (OCR).
    Text(TextOutput),
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
    fn video_text_merges_repeats_into_parts() {
        let out = TextOutput::from_frames([
            (0.0, "  SALE  today "),
            (1.0, "SALE today"),
            (2.0, ""),
            (3.0, "SALE today"),
            (4.0, "Line one\n\n  line   two"),
            (5.0, "Line one\nline two"),
        ]);
        let parts: Vec<(f64, f64, &str)> = out
            .parts
            .iter()
            .map(|p| (p.start, p.end, p.text.as_str()))
            .collect();
        assert_eq!(
            parts,
            vec![
                (0.0, 1.0, "SALE today"),
                // A frame without text ends a part.
                (3.0, 3.0, "SALE today"),
                (4.0, 5.0, "Line one\nline two"),
            ]
        );
        assert_eq!(out.text, "SALE today\nSALE today\nLine one\nline two");
        assert_eq!(out.blank, [2.0]);
        let none = TextOutput::from_frames([(0.0, " ")]);
        assert!(none.parts.is_empty());
        assert_eq!(none.blank, [0.0]);
        // A line break is whitespace too: the same caption wrapped
        // differently on the next frame is the same part, laid out as first.
        let wrapped = TextOutput::from_frames([(0.0, "SALE today"), (2.0, "SALE\ntoday")]);
        assert_eq!(wrapped.parts.len(), 1);
        assert_eq!(
            (wrapped.parts[0].end, wrapped.parts[0].text.as_str()),
            (2.0, "SALE today")
        );
        // Stored text from before `blank` existed reads back without it.
        let old: TextOutput = serde_json::from_str(r#"{"text":"a","parts":[]}"#).unwrap();
        assert!(old.blank.is_empty());
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
