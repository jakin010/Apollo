//! N frames spread evenly across the whole video: one per equal stretch, at
//! its midpoint, or at a random time within it.

use crate::ffmpeg::VideoInfo;

/// `count` timestamps, one in each of `count` equal stretches of the video:
/// the stretch's midpoint, or — with a `seed` — a random time within it. All
/// are chosen at once; the same seed always gives the same times.
pub fn timestamps(info: &VideoInfo, count: u32, seed: Option<u64>) -> Vec<f64> {
    if count == 0 || info.duration <= 0.0 {
        return Vec::new();
    }
    let mut rng = seed.map(SplitMix64);
    (0..count)
        .map(|i| {
            let offset = rng.as_mut().map_or(0.5, SplitMix64::unit);
            (i as f64 + offset) / count as f64 * info.duration
        })
        .collect()
}

/// SplitMix64: small, seedable and well mixed, so neighbouring seeds give
/// unrelated sequences.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn video(duration: f64) -> VideoInfo {
        VideoInfo {
            duration,
            start_time: 0.0,
            fps: 25.0,
            frame_count: None,
            width: 64,
            height: 48,
        }
    }

    #[test]
    fn midpoints_without_a_seed() {
        assert_eq!(
            timestamps(&video(60.0), 4, None),
            vec![7.5, 22.5, 37.5, 52.5]
        );
        assert!(timestamps(&video(60.0), 0, None).is_empty());
        assert!(timestamps(&video(0.0), 4, Some(1)).is_empty());
    }

    #[test]
    fn a_seed_puts_one_random_time_in_each_stretch() {
        let times = timestamps(&video(60.0), 8, Some(42));
        assert_eq!(times.len(), 8);
        for (i, t) in times.iter().enumerate() {
            let (start, end) = (i as f64 * 7.5, (i + 1) as f64 * 7.5);
            assert!((start..end).contains(t), "{t} outside stretch {i}");
        }
        // Not the midpoints.
        assert_ne!(times, timestamps(&video(60.0), 8, None));
    }

    #[test]
    fn the_same_seed_gives_the_same_times() {
        let a = timestamps(&video(60.0), 8, Some(7));
        assert_eq!(a, timestamps(&video(60.0), 8, Some(7)));
        assert_ne!(a, timestamps(&video(60.0), 8, Some(8)));
    }

    #[test]
    fn random_times_cover_each_stretch() {
        // Over many seeds, the offsets within a stretch spread over all of it.
        let mut buckets = [0u32; 10];
        for seed in 0..2000 {
            let t = timestamps(&video(10.0), 1, Some(seed))[0];
            buckets[(t as usize).min(9)] += 1;
        }
        assert!(buckets.iter().all(|&n| n > 120), "{buckets:?}");
    }
}
