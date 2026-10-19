//! `apollo-domain` — shared runtime types used across the workspace.
//!
//! These are the internal representation, deliberately independent of the
//! generated protobuf types in `apollo-proto`; `apollo-server` converts between them.

pub mod image;
pub mod model;
pub mod result;
pub mod task;

pub use image::DecodedImage;
pub use model::{Architecture, Modality, ModelInput};
pub use result::{
    Classification, Label, ModelOutput, Prediction, Sample, Scan, Segment, select_top,
};
pub use task::{
    ErrorKind, Input, Item, ItemState, MediaKind, ModelResult, ModelState, ParseStateError, Task,
    TaskError, TaskState, Url,
};
