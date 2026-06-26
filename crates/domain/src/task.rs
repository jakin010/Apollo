//! Task lifecycle: `Task`, `Item`, `ModelResult`, and their state enums.

use std::collections::BTreeMap;

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
}

impl Input {
    /// The modality this input represents.
    pub fn modality(&self) -> Modality {
        match self {
            Input::Image(_) => Modality::Image,
            Input::Video(_) => Modality::Video,
            Input::Text(_) => Modality::Text,
            Input::Audio(_) => Modality::Audio,
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
}

/// Result of running one model on one input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelResult {
    pub state: ModelState,
    /// Present only when `state == ModelState::Done`.
    pub output: Option<ModelOutput>,
    /// Present only when `state == ModelState::Failed`.
    pub error: Option<String>,
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

    pub fn failed(error: impl Into<String>) -> Self {
        Self {
            state: ModelState::Failed,
            output: None,
            error: Some(error.into()),
        }
    }
}

/// One input within a task: its request spec (needed to run and to resume) plus
/// per-model progress.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Item {
    pub input: Input,
    /// Model labels requested for this input.
    pub models: Vec<String>,
    pub state: ItemState,
    /// Per-model results, keyed by model label.
    pub results: BTreeMap<String, ModelResult>,
    /// Set on item-level failure (e.g. the input could not be fetched).
    pub error: Option<String>,
}

/// A unit of work: one or more inputs submitted together under a single id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub state: TaskState,
    /// Aligned to the submitted inputs.
    pub items: Vec<Item>,
}
