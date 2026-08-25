//! Stable diagnostic view of private writer ownership.

use std::fmt;

use super::WriteQueue;

impl<F> fmt::Debug for WriteQueue<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WriteQueue")
            .field("epoch", &self.epoch)
            .field("limits", &self.limits)
            .field("queued_frames", &self.frames.len())
            .field("retained_bytes", &self.retained_budget.used())
            .finish_non_exhaustive()
    }
}
