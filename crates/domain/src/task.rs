//! Task lifecycle: `Task`, `Item`, `ModelResult`, and their state enums.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::model::Modality;
use crate::result::ModelOutput;

/// A content reference: a primary URL plus an optional fallback, tried only if
/// `main` cannot be fetched or decoded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Url {
    /// Local path, `file://`, or `http(s)://`.
    pub main: String,
    /// Optional backup source, used only when `main` fails.
    pub fallback: Option<String>,
}

/// A submitted input, tagged by modality. URL-bearing inputs carry a [`Url`]
/// (with optional fallback); `Text` is inline and has neither.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Input {
    Image(Url),
    Video(Url),
    /// Inline content (future).
    Text(String),
    /// (future)
    Audio(Url),
    /// Raw content bytes streamed in via `ClassifyStream` and staged to a local
    /// file. `video` selects the processing path (false = image, true = video).
    /// The file is removed once the owning task reaches a terminal state.
    Bytes {
        path: PathBuf,
        video: bool,
    },
}

impl Input {
    /// The modality this input represents.
    pub fn modality(&self) -> Modality {
        match self {
            Input::Image(_) => Modality::Image,
            Input::Video(_) => Modality::Video,
            Input::Text(_) => Modality::Text,
            Input::Audio(_) => Modality::Audio,
            Input::Bytes { video: false, .. } => Modality::Image,
            Input::Bytes { video: true, .. } => Modality::Video,
        }
    }
}

/// Lifecycle state of a whole task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    Queued,
    Processing,
    /// Every item reached a terminal state (not necessarily success).
    Completed,
    /// Failed before any item could run.
    Failed,
    /// Cancelled by a client request before completing.
    Cancelled,
}

/// Lifecycle state of one input within a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemState {
    Queued,
    Processing,
    /// A previous attempt failed but retries remain under the configured cap, so
    /// the item is queued to run again.
    Retrying,
    Completed,
    Failed,
    /// Cancelled by a client request before completing.
    Cancelled,
}

/// Lifecycle state of one (item, model) unit of work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelState {
    Queued,
    Processing,
    Done,
    Failed,
    /// Not run because an earlier pipeline step's gate fired.
    Skipped,
}

/// The string in a stored state column matched no known variant of that state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseStateError {
    kind: &'static str,
    value: String,
}

impl fmt::Display for ParseStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown {} state {:?}", self.kind, self.value)
    }
}

impl std::error::Error for ParseStateError {}

impl TaskState {
    /// The stable lowercase token used to persist this state.
    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Queued => "queued",
            TaskState::Processing => "processing",
            TaskState::Completed => "completed",
            TaskState::Failed => "failed",
            TaskState::Cancelled => "cancelled",
        }
    }
}

impl FromStr for TaskState {
    type Err = ParseStateError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "queued" => TaskState::Queued,
            "processing" => TaskState::Processing,
            "completed" => TaskState::Completed,
            "failed" => TaskState::Failed,
            "cancelled" => TaskState::Cancelled,
            _ => {
                return Err(ParseStateError {
                    kind: "task",
                    value: s.to_string(),
                });
            }
        })
    }
}

impl ItemState {
    /// The stable lowercase token used to persist this state.
    pub fn as_str(self) -> &'static str {
        match self {
            ItemState::Queued => "queued",
            ItemState::Processing => "processing",
            ItemState::Retrying => "retrying",
            ItemState::Completed => "completed",
            ItemState::Failed => "failed",
            ItemState::Cancelled => "cancelled",
        }
    }
}

impl FromStr for ItemState {
    type Err = ParseStateError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "queued" => ItemState::Queued,
            "processing" => ItemState::Processing,
            "retrying" => ItemState::Retrying,
            "completed" => ItemState::Completed,
            "failed" => ItemState::Failed,
            "cancelled" => ItemState::Cancelled,
            _ => {
                return Err(ParseStateError {
                    kind: "item",
                    value: s.to_string(),
                });
            }
        })
    }
}

impl ModelState {
    /// The stable lowercase token used to persist this state.
    pub fn as_str(self) -> &'static str {
        match self {
            ModelState::Queued => "queued",
            ModelState::Processing => "processing",
            ModelState::Done => "done",
            ModelState::Failed => "failed",
            ModelState::Skipped => "skipped",
        }
    }
}

impl FromStr for ModelState {
    type Err = ParseStateError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "queued" => ModelState::Queued,
            "processing" => ModelState::Processing,
            "done" => ModelState::Done,
            "failed" => ModelState::Failed,
            "skipped" => ModelState::Skipped,
            _ => {
                return Err(ParseStateError {
                    kind: "model",
                    value: s.to_string(),
                });
            }
        })
    }
}

/// A category for a failure, for programmatic handling by clients.
/// `Unspecified` means an uncategorized/custom error carried by its message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    Unspecified,
    Fetch,
    Decode,
    Inference,
    Timeout,
    Cancelled,
    ModelUnavailable,
    Internal,
}

/// A structured error attached to a failed item or model result: a machine-
/// readable `kind` plus a human-readable `message`. A custom error uses
/// `ErrorKind::Unspecified` with the text in `message`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskError {
    pub kind: ErrorKind,
    pub message: String,
}

impl TaskError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
    /// An uncategorized error carrying only a message.
    pub fn custom(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unspecified, message)
    }
    pub fn fetch(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Fetch, message)
    }
    pub fn inference(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Inference, message)
    }
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Cancelled, message)
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message)
    }
}

/// Result of running one model on one input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelResult {
    pub state: ModelState,
    /// Present only when `state == ModelState::Done`.
    pub output: Option<ModelOutput>,
    /// Present only when `state == ModelState::Failed`.
    pub error: Option<TaskError>,
}

impl ModelResult {
    pub fn queued() -> Self {
        Self {
            state: ModelState::Queued,
            output: None,
            error: None,
        }
    }

    pub fn processing() -> Self {
        Self {
            state: ModelState::Processing,
            output: None,
            error: None,
        }
    }

    pub fn done(output: ModelOutput) -> Self {
        Self {
            state: ModelState::Done,
            output: Some(output),
            error: None,
        }
    }

    pub fn failed(error: TaskError) -> Self {
        Self {
            state: ModelState::Failed,
            output: None,
            error: Some(error),
        }
    }

    pub fn skipped() -> Self {
        Self {
            state: ModelState::Skipped,
            output: None,
            error: None,
        }
    }
}

/// One input within a task: its request spec (needed to run and to resume) plus
/// per-model progress.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Item {
    pub input: Input,
    /// Model labels requested for this input. For a pipeline item these are the
    /// pipeline's step models, in execution order.
    pub models: Vec<String>,
    /// Name of the pipeline to run this item through, if any. When set, `models`
    /// runs as an ordered, gated sequence rather than as a parallel set.
    #[serde(default)]
    pub pipeline: Option<String>,
    pub state: ItemState,
    /// Per-model results, keyed by model label.
    pub results: BTreeMap<String, ModelResult>,
    /// Set on item-level failure (e.g. the input could not be fetched).
    pub error: Option<TaskError>,
    /// Times this item has been retried after a failed attempt (resets nothing;
    /// compared against `[app].max_retries`).
    #[serde(default)]
    pub retries: u32,
}

/// A unit of work: one or more inputs submitted together under a single id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub state: TaskState,
    /// Aligned to the submitted inputs.
    pub items: Vec<Item>,
}
