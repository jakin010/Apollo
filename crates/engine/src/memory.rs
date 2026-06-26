//! Best-effort current-process resident memory, for the soft memory cap.

/// Resident (physical) memory of the current process in bytes, or `None` when the
/// platform probe is unavailable (in which case the memory cap is not enforced).
pub(crate) fn current_rss_bytes() -> Option<u64> {
    memory_stats::memory_stats().map(|m| m.physical_mem as u64)
}
