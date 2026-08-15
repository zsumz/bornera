//! Explicit count and retained-byte limits for one ordered writer.

use core::num::NonZeroUsize;

use calandria::RetainedBytes;

/// Resource bounds for complete frames retained in one connection epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteQueueLimits {
    max_frames: NonZeroUsize,
    max_retained_bytes: RetainedBytes,
}

impl WriteQueueLimits {
    /// Creates explicit frame-count and retained-byte limits.
    pub const fn new(max_frames: NonZeroUsize, max_retained_bytes: RetainedBytes) -> Self {
        Self {
            max_frames,
            max_retained_bytes,
        }
    }

    /// Returns the maximum complete frames retained by the writer.
    pub const fn max_frames(self) -> usize {
        self.max_frames.get()
    }

    /// Returns the maximum variable bytes retained by all frames.
    pub const fn max_retained_bytes(self) -> RetainedBytes {
        self.max_retained_bytes
    }
}
