//! `apollo-media` — input fetch/decode and video frame sampling.
//!
//! - [`fetch`]    — resolve a [`apollo_domain::Url`] (`main`, then `fallback`) to a local file: local paths and `file://` in place, `http(s)://` downloaded to a temp file.
//! - [`decode`]   — decode image bytes to [`apollo_domain::DecodedImage`] (RGB8).
//! - [`ffmpeg`]   — thin ffprobe/ffmpeg wrapper: probe metadata, extract a frame at a timestamp, list keyframes / scene changes. Requires `ffmpeg` and `ffprobe` on `PATH`.
//! - [`extract`]  — batched extraction: many sampled frames per ffmpeg process, scaled to the model's input size.
//! - [`sampling`] — the five sampling methods (the plug-point for new ones); each is deterministic w.r.t. the input, which makes frame-level resume exact.
//! - [`strategy`] — the ordered, de-duplicated multi-step frame plan, plus the aggregation and early-exit helpers.
//!
//! Boundary: this crate stays free of the inference and storage layers. The engine
//! drives the scan — pull the plan from [`strategy::plan`], extract and classify
//! frames in batches, persist, and stop on early-exit — using the helpers here.

pub mod confirm;
pub mod decode;
pub mod error;
pub mod extract;
pub mod fetch;
pub mod ffmpeg;
pub mod sampling;
pub mod share;
pub mod strategy;

pub use confirm::Confirm;
pub use decode::{decode_image, decode_image_as_stored};
pub use error::MediaError;
pub use extract::{FrameRequest, extract_frames_scaled};
pub use fetch::{FetchLimits, LocalMedia, fetch};
pub use ffmpeg::{VideoInfo, extract_frame, extract_frames, probe};
pub use sampling::FrameRef;
pub use share::{ShareStats, VideoShare, Warmup};
pub use strategy::{
    DEDUPE_TOLERANCE, Planner, aggregate, plan, refine_plan, trigger_score, triggered,
};
