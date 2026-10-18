//! Work shared by everything that scans one video: one [`VideoShare`] per
//! fetched video, used by every model of an item (and by its retries), so each
//! probe runs once and each frame is decoded once.
//!
//! - Probes (keyframes, scene cuts) are computed on first use; callers asking
//!   at the same time wait for that one run instead of starting their own.
//! - Frames are keyed by timestamp and size. Whoever asks first decodes them
//!   (several per ffmpeg pass, as before) and publishes them; anyone asking for
//!   the same frame meanwhile waits for it, and decodes it themselves if that
//!   decode was cancelled or failed. Recently decoded frames are kept for later
//!   askers, up to [`KEPT_FRAMES`].

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use apollo_config::{SamplingKind, SamplingStep};
use apollo_domain::DecodedImage;
use tokio::sync::{Mutex as AsyncMutex, OnceCell, OwnedMutexGuard};

use crate::error::MediaError;
use crate::extract::{FrameRequest, extract_frames_scaled};
use crate::ffmpeg::{self, VideoInfo};

/// Decoded frames kept for later askers, per video (most recent first out).
/// At a 224 px model input a frame is ~150 KB, so this is ~19 MB.
pub const KEPT_FRAMES: usize = 128;

/// A frame: its timestamp in milliseconds, and its square size.
type FrameKey = (i64, u32);
/// `None` until decoded; whoever holds the lock is (or was) decoding it.
type FrameSlot = Arc<AsyncMutex<Option<Arc<DecodedImage>>>>;
/// Scene cuts as `(timestamp, score)`, detected once.
type SceneCell = Arc<OnceCell<Vec<(f64, f32)>>>;

/// How much work sharing saved, for logging and measurement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShareStats {
    /// ffmpeg probe passes run (keyframes, scene detection).
    pub probes_run: u64,
    /// Probe results served without running one.
    pub probes_shared: u64,
    /// Frames decoded.
    pub frames_decoded: u64,
    /// Frames served without decoding them again.
    pub frames_shared: u64,
}

/// A probe started early by [`VideoShare::warm`]; dropping it cancels the probe
/// if it is still running.
pub struct Warmup(tokio::task::JoinHandle<()>);

impl Drop for Warmup {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Shared probes and frames for one video file.
pub struct VideoShare {
    path: PathBuf,
    info: VideoInfo,
    keyframes: OnceCell<Vec<f64>>,
    /// Keyed by the threshold's bits.
    scenes: Mutex<HashMap<u64, SceneCell>>,
    frames: Mutex<FrameStore>,
    probes_run: AtomicU64,
    probes_shared: AtomicU64,
    frames_decoded: AtomicU64,
    frames_shared: AtomicU64,
}

#[derive(Default)]
struct FrameStore {
    slots: HashMap<FrameKey, FrameSlot>,
    /// Decoded keys, oldest first, for eviction.
    kept: VecDeque<FrameKey>,
}

impl VideoShare {
    pub fn new(path: PathBuf, info: VideoInfo) -> Self {
        Self {
            path,
            info,
            keyframes: OnceCell::new(),
            scenes: Mutex::new(HashMap::new()),
            frames: Mutex::new(FrameStore::default()),
            probes_run: AtomicU64::new(0),
            probes_shared: AtomicU64::new(0),
            frames_decoded: AtomicU64::new(0),
            frames_shared: AtomicU64::new(0),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn info(&self) -> &VideoInfo {
        &self.info
    }

    pub fn stats(&self) -> ShareStats {
        ShareStats {
            probes_run: self.probes_run.load(Ordering::Relaxed),
            probes_shared: self.probes_shared.load(Ordering::Relaxed),
            frames_decoded: self.frames_decoded.load(Ordering::Relaxed),
            frames_shared: self.frames_shared.load(Ordering::Relaxed),
        }
    }

    /// Keyframe presentation timestamps (absolute), probed once.
    pub async fn keyframes(&self) -> Result<Vec<f64>, MediaError> {
        self.probe_once(&self.keyframes, || ffmpeg::keyframe_timestamps(&self.path))
            .await
    }

    /// Scene cuts above `threshold` as `(absolute timestamp, scene score)`,
    /// detected once per threshold.
    pub async fn scene_cuts(&self, threshold: f64) -> Result<Vec<(f64, f32)>, MediaError> {
        let cell = {
            let mut scenes = self.scenes.lock().unwrap_or_else(|e| e.into_inner());
            Arc::clone(scenes.entry(threshold.to_bits()).or_default())
        };
        self.probe_once(&cell, || ffmpeg::scene_cuts(&self.path, threshold))
            .await
    }

    /// Whether planning `step` would not wait on a probe: it needs none, or its
    /// probe has already finished.
    pub fn ready(&self, step: &SamplingStep) -> bool {
        match step.method {
            SamplingKind::Iframes => self.keyframes.initialized(),
            SamplingKind::Scene => step.threshold.is_none_or(|t| {
                let scenes = self.scenes.lock().unwrap_or_else(|e| e.into_inner());
                scenes
                    .get(&t.to_bits())
                    .is_some_and(|cell| cell.initialized())
            }),
            _ => true,
        }
    }

    /// Start the probe `step` will need (keyframes, scene detection) in the
    /// background, so it is ready, or under way, when the step is planned.
    /// `None` for steps that need none. Dropping the returned [`Warmup`] cancels
    /// it — killing its ffmpeg — e.g. when the scan stops before the step.
    pub fn warm(self: &Arc<Self>, step: &SamplingStep) -> Option<Warmup> {
        let share = Arc::clone(self);
        let task = match step.method {
            SamplingKind::Iframes => tokio::spawn(async move {
                let _ = share.keyframes().await;
            }),
            SamplingKind::Scene => {
                let threshold = step.threshold?;
                tokio::spawn(async move {
                    let _ = share.scene_cuts(threshold).await;
                })
            }
            _ => return None,
        };
        Some(Warmup(task))
    }

    async fn probe_once<T: Clone, F, Fut>(
        &self,
        cell: &OnceCell<T>,
        run: F,
    ) -> Result<T, MediaError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, MediaError>>,
    {
        let mut ran = false;
        let value = cell
            .get_or_try_init(|| {
                ran = true;
                run()
            })
            .await?;
        let counter = if ran {
            &self.probes_run
        } else {
            &self.probes_shared
        };
        counter.fetch_add(1, Ordering::Relaxed);
        Ok(value.clone())
    }

    /// The requested frames as `size` x `size` RGB images, in request order,
    /// decoding only those nobody has decoded (or is decoding) already.
    pub async fn frames(
        &self,
        requests: &[FrameRequest],
        size: u32,
    ) -> Result<Vec<Arc<DecodedImage>>, MediaError> {
        let mut out: Vec<Option<Arc<DecodedImage>>> = vec![None; requests.len()];
        let mut mine: Vec<(usize, OwnedMutexGuard<Option<Arc<DecodedImage>>>)> = Vec::new();
        let mut theirs: Vec<(usize, FrameSlot)> = Vec::new();
        {
            let mut store = self.frames.lock().unwrap_or_else(|e| e.into_inner());
            for (i, r) in requests.iter().enumerate() {
                let key = frame_key(r.timestamp, size);
                if let Some(slot) = store.slots.get(&key) {
                    theirs.push((i, Arc::clone(slot)));
                } else {
                    let slot: FrameSlot = Arc::new(AsyncMutex::new(None));
                    // Fresh and unshared, so the lock is free.
                    let guard = Arc::clone(&slot)
                        .try_lock_owned()
                        .expect("a new slot is unlocked");
                    store.slots.insert(key, slot);
                    mine.push((i, guard));
                }
            }
        }

        // Decode ours first and publish them, releasing their locks, before
        // waiting on anyone else's: two scans waiting on each other's frames
        // therefore never deadlock.
        self.decode_into(requests, size, &mut mine, &mut out)
            .await?;
        let mut missing = Vec::new();
        for (i, slot) in theirs {
            let guard = slot.lock_owned().await;
            match guard.as_ref() {
                Some(img) => {
                    out[i] = Some(Arc::clone(img));
                    self.frames_shared.fetch_add(1, Ordering::Relaxed);
                }
                // Its decoder was cancelled or failed: decode it here.
                None => missing.push((i, guard)),
            }
        }
        self.decode_into(requests, size, &mut missing, &mut out)
            .await?;

        Ok(out
            .into_iter()
            .map(|img| img.expect("every requested frame was filled"))
            .collect())
    }

    /// Decode the frames whose locks are in `slots` in one extraction, store
    /// them in their slots (and `out`), and keep them for later askers.
    async fn decode_into(
        &self,
        requests: &[FrameRequest],
        size: u32,
        slots: &mut Vec<(usize, OwnedMutexGuard<Option<Arc<DecodedImage>>>)>,
        out: &mut [Option<Arc<DecodedImage>>],
    ) -> Result<(), MediaError> {
        if slots.is_empty() {
            return Ok(());
        }
        let wanted: Vec<FrameRequest> = slots.iter().map(|(i, _)| requests[*i]).collect();
        // On error the locks are released with the slots still empty, so
        // anyone waiting decodes the frames themselves.
        let images = extract_frames_scaled(&self.path, &self.info, &wanted, size).await?;
        self.frames_decoded
            .fetch_add(images.len() as u64, Ordering::Relaxed);
        let mut decoded = Vec::with_capacity(images.len());
        for ((i, guard), img) in slots.iter_mut().zip(images) {
            let img = Arc::new(img);
            **guard = Some(Arc::clone(&img));
            out[*i] = Some(img);
            decoded.push(frame_key(requests[*i].timestamp, size));
        }
        slots.clear();

        let mut store = self.frames.lock().unwrap_or_else(|e| e.into_inner());
        for key in decoded {
            store.kept.push_back(key);
        }
        while store.kept.len() > KEPT_FRAMES {
            if let Some(old) = store.kept.pop_front() {
                store.slots.remove(&old);
            }
        }
        Ok(())
    }
}

fn frame_key(timestamp: f64, size: u32) -> FrameKey {
    ((timestamp * 1000.0).round() as i64, size)
}

#[cfg(test)]
mod tests_support {
    use super::*;

    pub(super) fn have_ffmpeg() -> bool {
        std::process::Command::new("ffmpeg")
            .arg("-version")
            .output()
            .is_ok_and(|o| o.status.success())
    }

    /// 6 s of 64x48 test pattern at 10 fps.
    pub(super) async fn sample(dir: &Path) -> Arc<VideoShare> {
        let path = dir.join("clip.mp4");
        let ok = std::process::Command::new("ffmpeg")
            .args(["-v", "error", "-y", "-f", "lavfi", "-i"])
            .arg("testsrc2=size=64x48:rate=10")
            .args(["-t", "6", "-g", "20", "-pix_fmt", "yuv420p"])
            .arg(&path)
            .status()
            .is_ok_and(|s| s.success());
        assert!(ok, "could not create the sample video");
        let info = ffmpeg::probe(&path).await.unwrap();
        Arc::new(VideoShare::new(path, info))
    }
}

#[cfg(test)]
mod tests {
    use super::tests_support::*;
    use super::*;

    fn at(times: &[f64]) -> Vec<FrameRequest> {
        times
            .iter()
            .map(|&timestamp| FrameRequest {
                timestamp,
                keyframe: false,
            })
            .collect()
    }

    #[tokio::test]
    async fn concurrent_scans_decode_each_frame_once() {
        if !have_ffmpeg() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let share = sample(dir.path()).await;
        // Two scans asking at once, overlapping on 2.0 and 3.0.
        let (first, second) = (at(&[1.0, 2.0, 3.0]), at(&[2.0, 3.0, 4.0]));
        let (a, b) = tokio::join!(share.frames(&first, 32), share.frames(&second, 32));
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!((a.len(), b.len()), (3, 3));
        // The overlapping frames are the very same images.
        assert!(Arc::ptr_eq(&a[1], &b[0]) && Arc::ptr_eq(&a[2], &b[1]));
        let stats = share.stats();
        assert_eq!(stats.frames_decoded, 4);
        assert_eq!(stats.frames_shared, 2);

        // Asked again later, a kept frame is served without decoding.
        let again = share.frames(&at(&[4.0]), 32).await.unwrap();
        assert!(Arc::ptr_eq(&again[0], &b[2]));
        assert_eq!(share.stats().frames_decoded, 4);
        // A different size is a different frame.
        share.frames(&at(&[4.0]), 16).await.unwrap();
        assert_eq!(share.stats().frames_decoded, 5);
    }

    #[tokio::test]
    async fn concurrent_probes_run_once() {
        if !have_ffmpeg() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let share = sample(dir.path()).await;
        let (a, b) = tokio::join!(share.scene_cuts(0.3), share.scene_cuts(0.3));
        assert_eq!(a.unwrap(), b.unwrap());
        let (k1, k2) = tokio::join!(share.keyframes(), share.keyframes());
        assert_eq!(k1.unwrap(), k2.unwrap());
        let stats = share.stats();
        assert_eq!((stats.probes_run, stats.probes_shared), (2, 2));
        // Another threshold is another probe.
        share.scene_cuts(0.5).await.unwrap();
        assert_eq!(share.stats().probes_run, 3);
    }
}

#[cfg(test)]
mod planner_tests {
    use super::tests_support::*;
    use super::*;
    use crate::strategy::Planner;
    use apollo_config::SamplingStep;

    fn step(
        n: u32,
        method: SamplingKind,
        count: Option<u32>,
        threshold: Option<f64>,
    ) -> SamplingStep {
        SamplingStep {
            step: n,
            method,
            fps: None,
            count,
            nth: None,
            threshold,
            min_gap: None,
            min_score: None,
            window: None,
        }
    }

    fn steps() -> Vec<SamplingStep> {
        vec![
            step(1, SamplingKind::Uniform, Some(2), None),
            step(2, SamplingKind::Scene, None, Some(0.3)),
        ]
    }

    #[tokio::test]
    async fn a_later_steps_probe_only_runs_when_the_scan_reaches_it() {
        if !have_ffmpeg() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let share = sample(dir.path()).await;
        let mut planner = Planner::new(share.info(), &steps(), None);

        let first = planner.next_step(&share).await.unwrap().unwrap();
        assert_eq!(first.len(), 2);
        // Planning the uniform step ran no scene detection.
        assert_eq!(share.stats().probes_run, 0);
        assert_eq!(
            planner.upcoming().map(|s| s.method),
            Some(SamplingKind::Scene)
        );

        planner.next_step(&share).await.unwrap().unwrap();
        assert_eq!(share.stats().probes_run, 1);
        assert!(planner.next_step(&share).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_warmed_probe_runs_once_and_frames_match_planning_up_front() {
        if !have_ffmpeg() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let share = sample(dir.path()).await;
        let mut planner = Planner::new(share.info(), &steps(), None);
        let mut lazy = planner.next_step(&share).await.unwrap().unwrap();
        let warm = share.warm(planner.upcoming().unwrap());
        assert!(warm.is_some());
        lazy.extend(planner.next_step(&share).await.unwrap().unwrap());
        // Warm-up and planner asked for the same probe; it ran once (whichever
        // got there first ran it, the other waited for it).
        assert_eq!(share.stats().probes_run, 1);
        drop(warm);
        // Same frames and indices as planning every step at once.
        let eager = crate::strategy::plan(&share, &steps(), None).await.unwrap();
        assert_eq!(lazy, eager);
    }
}
