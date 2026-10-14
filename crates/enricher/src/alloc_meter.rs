//! (A copy of poller-stations' meter, plus [`Mark`].)
//!
//! Test-only global allocator that tracks, per thread, how many bytes are
//! live and the high-water mark above a baseline -- so a test can assert
//! how much memory a code path needs without other tests' allocations
//! (running concurrently on other threads) skewing the number.

#![expect(
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "test code: casts of small known test values"
)]
#![expect(unsafe_code, reason = "a GlobalAlloc impl is unsafe by definition")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct Meter;

thread_local! {
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn record(delta: isize) {
    // `try_with`: the thread-locals may already be gone during thread
    // teardown, when allocations must still succeed.
    let _ = LIVE.try_with(|live| {
        let now = live.get() + delta;
        live.set(now);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
    });
}

// SAFETY: delegates every operation to `System` unchanged; the bookkeeping
// only touches const-initialized thread-locals, which never allocate.
unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwards the caller's `alloc` contract to `System` unchanged.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record(layout.size() as isize);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwards the caller's `alloc_zeroed` contract to `System`.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System` via this allocator with `layout`.
        unsafe { System.dealloc(ptr, layout) };
        record(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `ptr` came from `System` via this allocator with `layout`.
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            // Counted as the worst case, a copy: both blocks live at once.
            record(new_size as isize);
            record(-(layout.size() as isize));
        }
        new
    }
}

#[global_allocator]
static METER: Meter = Meter;

/// Runs `f` and returns its result together with the most bytes this
/// thread had allocated at once during `f`, above what was live before it.
pub(crate) fn peak_during<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let baseline = LIVE.with(Cell::get);
    PEAK.with(|peak| peak.set(baseline));
    let result = f();
    let peak = PEAK.with(Cell::get);
    (result, (peak - baseline).max(0) as usize)
}

/// A baseline for code that awaits, so can't run inside [`peak_during`]:
/// what this thread had live when it was taken, with the high-water mark
/// reset to it. The awaited code must run on this thread (a current-thread
/// runtime, as `#[tokio::test]` uses); work on other threads (a mock
/// server's) is not counted.
pub(crate) struct Mark(isize);

impl Mark {
    pub(crate) fn new() -> Self {
        let live = LIVE.with(Cell::get);
        PEAK.with(|peak| peak.set(live));
        Self(live)
    }

    /// The most bytes this thread had live at once since the mark, above it.
    pub(crate) fn peak(&self) -> usize {
        (PEAK.with(Cell::get) - self.0).max(0) as usize
    }

    /// Bytes this thread has live now, above the mark.
    pub(crate) fn held(&self) -> usize {
        (LIVE.with(Cell::get) - self.0).max(0) as usize
    }
}
