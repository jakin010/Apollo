//! `apollo-media` — input fetch/decode and video frame sampling.
//!
//! - [`fetch`]    — resolve a [`apollo_domain::Url`] (`main`, then `fallback`) to
//!                  a local file: local paths and `file://` in place, `http(s)://`
//!                  downloaded to a temp file.
//! - [`decode`]   — decode image bytes to [`apollo_domain::DecodedImage`] (RGB8).
//! - [`ffmpeg`]   — thin ffprobe/ffmpeg wrapper: probe metadata, extract a frame
//!                  at a timestamp, list keyframes / scene changes. Requires
//!                  `ffmpeg` and `ffprobe` on `PATH`.
//! - [`sampling`] — the five sampling methods (the plug-point for new ones); each
//!                  is deterministic w.r.t. the input, which makes frame-level
//!                  resume exact.
//! - [`strategy`] — the ordered, de-duplicated multi-step frame plan, plus the
//!                  aggregation and early-exit helpers.
//!
//! Boundary: this crate stays free of the inference and storage layers. The engine
//! drives the scan — pull the plan from [`strategy::plan`], extract and classify
//! frames in batches, persist, and stop on early-exit — using the helpers here.

pub mod decode;
pub mod error;
pub mod fetch;
pub mod ffmpeg;
pub mod sampling;
pub mod strategy;

pub use decode::decode_image;
pub use error::MediaError;
pub use fetch::{fetch, LocalMedia};
pub use ffmpeg::{extract_frame, extract_frames, probe, VideoInfo};
pub use sampling::FrameRef;
pub use strategy::{aggregate, plan, triggered, DEDUPE_TOLERANCE};
