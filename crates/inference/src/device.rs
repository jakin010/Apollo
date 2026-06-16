//! Compute-device selection.
//!
//! A single per-platform binary adapts to the machine it runs on. The backends
//! compiled in depend on the build target:
//!
//! * **CUDA** — compiled in only with `--features cuda` (Linux/Windows + NVIDIA
//!   toolkit). Used at runtime only if a CUDA device initializes.
//! * **Metal** — compiled in automatically on macOS. Used at runtime only if a
//!   Metal device initializes.
//! * **CPU** — always available, the final fallback.
//!
//! Selection is therefore "look at what's actually here": a CUDA build on a host
//! with no GPU quietly drops to CPU; a macOS build uses Metal when present.

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
