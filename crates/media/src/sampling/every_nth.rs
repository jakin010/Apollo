//! Every Nth decoded frame, converted to timestamps via the video frame rate.

use crate::ffmpeg::VideoInfo;

/// Highest frame rate believed from a container. The rate comes from the
/// (untrusted) file, so an absurd value is clamped rather than allowed to blow up
/// the number of timestamps generated.
pub const MAX_PLAUSIBLE_FPS: f64 = 240.0;

/// Timestamps for frames 0, nth, 2·nth, ... using the video's frame rate.
pub fn timestamps(info: &VideoInfo, nth: u32) -> Vec<f64> {
    // NaN / non-positive rates and NaN / infinite / non-positive durations would
    // make the loop below meaningless or endless.
    let usable = info.fps.is_finite() && info.fps > 0.0 && info.duration.is_finite();
    if nth == 0 || !usable || info.duration <= 0.0 {
        return Vec::new();
    }
    let fps = info.fps.min(MAX_PLAUSIBLE_FPS);
    let mut out = Vec::new();
    let mut frame = 0u64;
    loop {
        let t = frame as f64 / fps;
        if t >= info.duration {
            break;
        }
        out.push(t);
        frame += nth as u64;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absurd_frame_rates_are_clamped() {
        let info = VideoInfo {
            duration: 10.0,
            start_time: 0.0,
            fps: 1.0e9,
            frame_count: None,
            width: 0,
            height: 0,
        };
        // At most 10s * 240fps / nth frames, not 10s * 1e9fps.
        assert_eq!(timestamps(&info, 10).len(), 240);
    }
}
