//! Sampling-method dispatch. Each method is deterministic w.r.t. the input video,
//! which is what makes frame-level resume exact. Pure methods (`uniform`, `fps`,
//! `every_nth`) are computed from [`VideoInfo`]; `iframes` and `scene` probe the
//! file with ffmpeg. `refine` is planned later, from the classified frames'
//! scores ([`crate::strategy::refine_plan`]), and is deterministic given those.

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
/// [`crate::strategy::plan`]), `timestamp` in seconds, and whether it is a
/// keyframe (chosen by the `iframes` step), which makes it cheap to extract.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FrameRef {
    pub index: u32,
    pub timestamp: f64,
    pub keyframe: bool,
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
        // ffmpeg reports these as absolute presentation times; sampling works
        // relative to the stream start (like `duration` and the other methods).
        SamplingKind::Iframes => Ok(relative(info, iframes::timestamps(path).await?)),
        SamplingKind::Scene => Ok(relative(
            info,
            scene::timestamps(
                path,
                req(step.threshold, "threshold")?,
                step.min_gap.unwrap_or(0.0),
            )
            .await?,
        )),
        // Planned from the scores of the other steps' frames, after they are
        // classified — see [`crate::strategy::refine_plan`].
        SamplingKind::Refine => Ok(Vec::new()),
    }
}

fn relative(info: &VideoInfo, times: Vec<f64>) -> Vec<f64> {
    times
        .into_iter()
        .map(|t| (t - info.start_time).max(0.0))
        .collect()
}

fn req<T>(value: Option<T>, name: &str) -> Result<T, MediaError> {
    value.ok_or_else(|| MediaError::MisconfiguredStep(format!("step missing '{name}'")))
}
