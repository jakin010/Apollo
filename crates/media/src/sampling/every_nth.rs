//! Every Nth decoded frame, converted to timestamps via the video frame rate.

use crate::ffmpeg::VideoInfo;

/// Timestamps for frames 0, nth, 2·nth, ... using the video's frame rate.
pub fn timestamps(info: &VideoInfo, nth: u32) -> Vec<f64> {
    if nth == 0 || info.fps <= 0.0 || info.duration <= 0.0 {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut frame = 0u64;
    loop {
        let t = frame as f64 / info.fps;
        if t >= info.duration {
            break;
        }
        out.push(t);
        frame += nth as u64;
    }
    out
}
