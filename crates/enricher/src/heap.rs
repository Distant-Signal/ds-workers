//! Keeps the batch path's short-lived peaks from becoming the pod's
//! resident size.
//!
//! The binary uses the system allocator: glibc malloc in the image
//! (`debian:bookworm-slim`). A batch step allocates tens of MB at once (a
//! batch's request bodies, its results) and frees it a moment later, and
//! glibc keeps freed memory by default:
//!
//! - Small chunks go back to the arena's free lists. Only the very top of
//!   the main arena is ever returned on its own, and the tokio workers
//!   allocate from their own arenas, so a stage's freed heap stayed
//!   resident: prod 2026-10-14, ~230 MiB after one 929-incident batch,
//!   against ~20 MiB before it (`DistantSignalComponentMemoryHigh`).
//!   [`release_freed_memory`] runs `malloc_trim(0)` after each batch step,
//!   which returns every arena's free pages.
//! - A chunk over the mmap threshold (128 KiB) is its own mapping, unmapped
//!   on free. But glibc raises that threshold to the size of every such
//!   chunk freed (up to 32 MiB), after which a batch's multi-MB upload
//!   body comes from a worker's arena instead, at the top of a heap that
//!   `malloc_trim` does not shrink. [`pin_mmap_threshold`] sets the
//!   threshold explicitly at startup, which turns that adjustment off.
//!
//! Measured on 2026-10-14 by submitting and reading back 2,000-incident
//! primary and adversarial batches against an out-of-process mock:
//! anonymous RSS returned to its ~5 MiB baseline after each step with both,
//! and stayed at 12-50 MiB with either alone (168 MiB with neither, before
//! the batch path stopped holding its requests as `Value` trees).
//!
//! Both are no-ops off glibc.

/// The mmap threshold [`pin_mmap_threshold`] pins: glibc's own default.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const MMAP_THRESHOLD: libc::c_int = 128 * 1024;

/// Pins glibc's mmap threshold at its default, so allocations over it are
/// always their own mapping, returned to the kernel when freed. Call once,
/// at startup. (An explicit `MALLOC_MMAP_THRESHOLD_` in the environment
/// does the same; this makes it the binary's own behaviour.)
pub(crate) fn pin_mmap_threshold() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // SAFETY: `mallopt` takes two integers and changes a glibc malloc
        // parameter under its own lock; any thread may call it at any time.
        #[expect(unsafe_code, reason = "an FFI call into glibc; see SAFETY above")]
        let ok = unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, MMAP_THRESHOLD) };
        if ok != 1 {
            tracing::warn!("could not pin the malloc mmap threshold");
        }
    }
}

/// Hands the heap a batch step just freed back to the kernel (see the
/// module doc). It runs once per ended batch stage or batch-submitting
/// sweep, so its cost (a walk of the arenas, about a millisecond at this
/// size) does not matter.
pub(crate) fn release_freed_memory() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // SAFETY: `malloc_trim` takes no pointers and only walks glibc's
        // own arenas under their locks; any thread may call it at any time.
        #[expect(unsafe_code, reason = "an FFI call into glibc; see SAFETY above")]
        let released = unsafe { libc::malloc_trim(0) };
        tracing::debug!(released, "returned freed heap memory to the kernel");
    }
}
