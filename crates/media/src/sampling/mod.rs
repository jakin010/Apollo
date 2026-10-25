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

use apollo_config::{SamplingKind, SamplingStep};

use crate::error::MediaError;
use crate::ffmpeg::VideoInfo;
use crate::share::VideoShare;

/// A frame to classify: its ordinal `index` (assigned after cross-step dedupe in
/// [`crate::strategy::plan`]), `timestamp` in seconds, and whether it is a
/// keyframe (chosen by the `iframes` step), which makes it cheap to extract.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FrameRef {
    pub index: u32,
    pub timestamp: f64,
    pub keyframe: bool,
}

/// The (unindexed) frames a single configured sampling step selects, as
/// `(timestamp, priority)`: scene cuts carry their scene score (used to keep the
/// strongest when a frame budget cannot fit them all); other methods none.
pub async fn step_samples(
    share: &VideoShare,
    step: &SamplingStep,
    seed: u64,
) -> Result<Vec<(f64, Option<f32>)>, MediaError> {
    let info = share.info();
    let plain = |times: Vec<f64>| times.into_iter().map(|t| (t, None)).collect();
    match step.method {
        SamplingKind::Uniform => {
            // A random step's times depend on the scan's seed and the step.
            let seed = step.random.then(|| step_seed(seed, step.step));
            Ok(plain(uniform::timestamps(
                info,
                req(step.count, "count")?,
                seed,
            )))
        }
        SamplingKind::Fps => Ok(plain(fps::timestamps(info, req(step.fps, "fps")?))),
        SamplingKind::EveryNth => Ok(plain(every_nth::timestamps(info, req(step.nth, "nth")?))),
        // ffmpeg reports these as absolute presentation times; sampling works
        // relative to the stream start (like `duration` and the other methods).
        SamplingKind::Iframes => Ok(plain(relative(info, iframes::timestamps(share).await?))),
        SamplingKind::Scene => {
            let cuts = scene::cuts(
                share,
                req(step.threshold, "threshold")?,
                step.min_gap.unwrap_or(0.0),
            )
            .await?;
            Ok(cuts
                .into_iter()
                .map(|(t, score)| ((t - info.start_time).max(0.0), Some(score)))
                .collect())
        }
        // Planned from the scores of the other steps' frames, after they are
        // classified — see [`crate::strategy::refine_plan`].
        SamplingKind::Refine => Ok(Vec::new()),
    }
}

/// A step's own seed from the scan's, so two random steps of a strategy
/// differ.
fn step_seed(seed: u64, step: u32) -> u64 {
    seed ^ u64::from(step).wrapping_mul(0x9E37_79B9_7F4A_7C15)
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
