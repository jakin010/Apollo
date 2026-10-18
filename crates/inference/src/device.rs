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
    let opened = match kind {
        DeviceKind::Cpu => return Device::Cpu,
        #[cfg(feature = "cuda")]
        DeviceKind::Cuda => Device::new_cuda(0),
        #[cfg(target_os = "macos")]
        DeviceKind::Metal => Device::new_metal(0),
        #[allow(unreachable_patterns)]
        _ => return Device::Cpu,
    };
    opened.unwrap_or_else(|e| {
        tracing::warn!(error = %e, device = %kind, "could not open a device; using CPU");
        Device::Cpu
    })
}

/// Pick the best device available right now, trying CUDA, then Metal, then CPU.
/// Only backends compiled into this binary are attempted.
pub fn select_device() -> (Device, DeviceKind) {
    #[cfg(feature = "cuda")]
    {
        match Device::new_cuda(0) {
            Ok(device) => {
                tracing::info!("selected CUDA device 0");
                return (device, DeviceKind::Cuda);
            }
            Err(e) => {
                tracing::warn!(error = %e, "CUDA is compiled in but no device initialized; trying next backend");
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        match Device::new_metal(0) {
            Ok(device) => {
                tracing::info!("selected Metal device 0");
                return (device, DeviceKind::Metal);
            }
            Err(e) => {
                tracing::warn!(error = %e, "Metal did not initialize; falling back to CPU");
            }
        }
    }

    tracing::info!("selected CPU device");
    (Device::Cpu, DeviceKind::Cpu)
}
