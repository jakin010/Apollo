//! Best-effort memory use of the current process, for the soft memory cap and
//! `apollo status`.
//!
//! On macOS this is the kernel's `phys_footprint` — the figure Activity Monitor
//! and `footprint(1)` show, and the one jetsam acts on. Unlike the resident set
//! size it includes GPU allocations: on Apple Silicon, Metal buffers (model
//! weights, activations) live in unified memory outside RSS, so RSS can read a
//! fraction of what the process really holds. Elsewhere it is the resident set
//! size.

/// Memory the current process is using, in bytes, or `None` when the platform
/// probe is unavailable (in which case the memory cap is not enforced).
pub(crate) fn current_memory_bytes() -> Option<u64> {
    imp::current_memory_bytes()
}

#[cfg(target_os = "macos")]
mod imp {
    use std::mem::size_of;

    /// `TASK_VM_INFO` from `<mach/task_info.h>` (not exported by `libc`).
    const TASK_VM_INFO: libc::task_flavor_t = 22;

    /// The leading fields of `struct task_vm_info` from `<mach/task_info.h>`, up
    /// to `phys_footprint` (revision 1). The kernel fills as many revisions as
    /// the passed count allows and reports how many it wrote, so a prefix is
    /// enough. Every field is naturally aligned, so `repr(C)` matches the
    /// header's `#pragma pack(4)` layout.
    #[repr(C)]
    #[derive(Default)]
    struct TaskVmInfoRev1 {
        virtual_size: u64,
        region_count: i32,
        page_size: i32,
        resident_size: u64,
        resident_size_peak: u64,
        device: u64,
        device_peak: u64,
        internal: u64,
        internal_peak: u64,
        external: u64,
        external_peak: u64,
        reusable: u64,
        reusable_peak: u64,
        purgeable_volatile_pmap: u64,
        purgeable_volatile_resident: u64,
        purgeable_volatile_virtual: u64,
        compressed: u64,
        compressed_peak: u64,
        compressed_lifetime: u64,
        phys_footprint: u64,
    }

    /// `TASK_VM_INFO_REV1_COUNT`: the struct's size in `natural_t` units.
    const REV1_COUNT: libc::mach_msg_type_number_t =
        (size_of::<TaskVmInfoRev1>() / size_of::<libc::natural_t>()) as _;

    pub(super) fn current_memory_bytes() -> Option<u64> {
        let mut info = TaskVmInfoRev1::default();
        let mut count = REV1_COUNT;
        // SAFETY: `info` is a writable buffer of `count` natural_t's laid out as
        // the kernel's task_vm_info prefix; the kernel writes at most `count`
        // of them and updates `count` to the number written.
        // `mach_task_self` is deprecated in `libc` in favour of the `mach2`
        // crate, which is not otherwise a dependency; the binding is the same.
        #[allow(deprecated)]
        let kr = unsafe {
            libc::task_info(
                libc::mach_task_self(),
                TASK_VM_INFO,
                (&raw mut info).cast(),
                &mut count,
            )
        };
        // A kernel too old to fill `phys_footprint` reports a smaller count.
        (kr == libc::KERN_SUCCESS && count >= REV1_COUNT).then_some(info.phys_footprint)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn layout_matches_the_header() {
            // Values of TASK_VM_INFO_REV1_COUNT and
            // offsetof(task_vm_info_data_t, phys_footprint) in the macOS SDK.
            assert_eq!(REV1_COUNT, 38);
            assert_eq!(std::mem::offset_of!(TaskVmInfoRev1, phys_footprint), 144);
        }
    }
}

#[cfg(not(target_os = "macos"))]
mod imp {
    pub(super) fn current_memory_bytes() -> Option<u64> {
        memory_stats::memory_stats().map(|m| m.physical_mem as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_memory_that_grows_with_allocation() {
        let before = current_memory_bytes().expect("memory probe available");
        assert!(before > 0);
        // Touch 64 MiB so it is actually committed, then check it is counted.
        let mut buf = vec![0u8; 64 << 20];
        for page in buf.chunks_mut(4096) {
            page[0] = 1;
        }
        let after = current_memory_bytes().unwrap();
        std::hint::black_box(&buf);
        assert!(
            after >= before + (32 << 20),
            "before {before}, after {after}"
        );
    }
}
