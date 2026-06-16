//! Sampling-method dispatch. Each method is deterministic w.r.t. the input video,
//! which is what makes frame-level resume exact. Pure methods (`uniform`, `fps`,
//! `every_nth`) are computed from [`VideoInfo`]; `iframes` and `scene` probe the
//! file with ffmpeg.

pub mod every_nth;
pub mod fps;
pub mod iframes;
pub mod scene;
pub mod uniform;

use std::path::Path;

use apollo_config::{SamplingKind, SamplingStep};

use crate::error::MediaError;
use crate::ffmpeg::VideoInfo;

/// A frame to classify: its ordinal `index` (assigned after cross-step dedupe in
/// [`crate::strategy::plan`]) and `timestamp` in seconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FrameRef {
    pub index: u32,
    pub timestamp: f64,
}

/// The (unindexed) timestamps a single configured sampling step selects.
pub async fn step_timestamps(
    path: &Path,
    info: &VideoInfo,
    step: &SamplingStep,
) -> Result<Vec<f64>, MediaError> {
    match step.method {
        SamplingKind::Uniform => Ok(uniform::timestamps(info, req(step.count, "count")?)),
        SamplingKind::Fps => Ok(fps::timestamps(info, req(step.fps, "fps")?)),
        SamplingKind::EveryNth => Ok(every_nth::timestamps(info, req(step.nth, "nth")?)),
        SamplingKind::Iframes => iframes::timestamps(path).await,
        SamplingKind::Scene => scene::timestamps(path, req(step.threshold, "threshold")?).await,
    }
}

fn req<T>(value: Option<T>, name: &str) -> Result<T, MediaError> {
    value.ok_or_else(|| MediaError::MisconfiguredStep(format!("step missing '{name}'")))
}
