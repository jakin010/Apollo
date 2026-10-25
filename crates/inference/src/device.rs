//! Compute-device selection.
//!
//! At runtime this picks the best backend that actually initializes, in the
//! order CUDA → Metal → CPU, considering only backends compiled into the binary:
//!
//! * **CUDA** — compiled in only with `--features cuda` (Linux/Windows + NVIDIA
//!   toolkit). Selected at runtime only if a CUDA device initializes.
//! * **Metal** — compiled in automatically on macOS. Selected only if a Metal
//!   device initializes.
//! * **CPU** — always available, the final fallback.
//!
//! `[app].device` can pin one backend instead (`cpu`, `metal`, `cuda`), e.g. to
//! keep a GPU free or to measure CPU-only performance on a GPU machine.
//!
//! `select_device()` therefore drops to CPU whenever the preferred backend is
//! compiled in but cannot bring up a device — no GPU present, driver mismatch,
//! device busy. On macOS it runs regardless, since Metal ships with the OS.
//!
//! IMPORTANT — this fallback only covers a backend that is *present but
//! unusable*. It does NOT make a CUDA build portable. candle links the CUDA
//! runtime at load time (cudarc's `dynamic-linking` feature), so a
//! `--features cuda` binary carries hard `NEEDED` entries for
//! `libcuda`/`libcudart`/`libcublas`/`libcurand`/`libnvrtc`; if those are absent
//! the dynamic loader aborts the process *before* `main()` and none of this code
//! runs. To run where CUDA may be missing, ship the CPU-only build (compiled
//! without `--features cuda`) — it links nothing CUDA-related and starts
//! anywhere, selecting CPU here.

use apollo_config::DevicePreference;
use candle_core::Device;

/// Which backend a device represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceKind {
    Cuda,
    Metal,
    Cpu,
}

impl std::fmt::Display for DeviceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DeviceKind::Cuda => "cuda",
            DeviceKind::Metal => "metal",
            DeviceKind::Cpu => "cpu",
        })
    }
}

/// Open a fresh, independent device of `kind`, falling back to CPU if it cannot
/// be initialized.
///
/// Each model worker must get its own: candle's Metal (and CUDA) devices keep
/// their command stream and buffer pool behind shared handles, so clones of one
/// device used from several threads at once interleave each other's work and
/// can return corrupted (NaN) results.
pub fn open_device(kind: DeviceKind) -> Device {
    match try_open(kind) {
        Some(Ok(device)) => device,
        Some(Err(e)) => {
            tracing::warn!(error = %e, device = %kind, "could not open a device; using CPU");
            Device::Cpu
        }
        None => Device::Cpu,
    }
}

/// Pick the device to run on. `Auto` tries CUDA, then Metal, then CPU; an
/// explicit backend is tried alone and falls back to CPU if it cannot start.
/// Only backends compiled into this binary are attempted.
pub fn select_device(preference: DevicePreference) -> (Device, DeviceKind) {
    init_cpu_threads();
    let candidates: &[DeviceKind] = match preference {
        DevicePreference::Auto => &[DeviceKind::Cuda, DeviceKind::Metal],
        DevicePreference::Cuda => &[DeviceKind::Cuda],
        DevicePreference::Metal => &[DeviceKind::Metal],
        DevicePreference::Cpu => &[],
    };
    for &kind in candidates {
        match try_open(kind) {
            Some(Ok(device)) => {
                tracing::info!(device = %kind, "selected device 0");
                return (device, kind);
            }
            Some(Err(e)) => {
                tracing::warn!(error = %e, device = %kind, "device did not initialize; trying the next backend");
            }
            None if preference != DevicePreference::Auto => {
                tracing::warn!(device = %kind, "this build has no support for the configured device; using CPU");
            }
            None => {}
        }
    }
    tracing::info!("selected CPU device");
    (Device::Cpu, DeviceKind::Cpu)
}

/// Size rayon's global pool (candle's CPU matmuls, tokenizers) to candle's CPU
/// thread count: one per physical core the process may use, within its
/// affinity mask and container CPU quota. Left alone, rayon starts one thread
/// per logical CPU (hyperthreads, efficiency cores) while candle splits each
/// matmul into one part per core, so the parts land on slower or shared CPUs.
/// `RAYON_NUM_THREADS` / `CANDLE_NUM_THREADS` override the count. A no-op if
/// the pool already exists.
fn init_cpu_threads() {
    candle_core::utils::init_global_threadpool();
    tracing::info!(
        threads = candle_core::utils::get_num_threads(),
        "CPU threads for inference"
    );
}

/// Try to open device 0 of `kind`; `None` when that backend is not compiled
/// into this binary.
fn try_open(kind: DeviceKind) -> Option<candle_core::Result<Device>> {
    match kind {
        DeviceKind::Cpu => Some(Ok(Device::Cpu)),
        #[cfg(feature = "cuda")]
        DeviceKind::Cuda => Some(Device::new_cuda(0)),
        #[cfg(target_os = "macos")]
        DeviceKind::Metal => Some(Device::new_metal(0)),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}
