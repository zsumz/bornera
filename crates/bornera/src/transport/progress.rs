//! Measured work returned by one transport-local progression call.

use core::num::NonZeroUsize;

use super::TransportBudget;

/// Exact bounded work performed inside one transport progression call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportProgress {
    operations: usize,
    read_bytes: usize,
    written_bytes: usize,
}

impl TransportProgress {
    /// No transport-local work was performed.
    pub const IDLE: Self = Self {
        operations: 0,
        read_bytes: 0,
        written_bytes: 0,
    };

    /// Creates an exact report of logical operations and raw byte movement.
    pub const fn new(operations: NonZeroUsize, read_bytes: usize, written_bytes: usize) -> Self {
        Self {
            operations: operations.get(),
            read_bytes,
            written_bytes,
        }
    }

    /// Reports one operation without raw byte movement.
    pub const fn operation() -> Self {
        Self::new(NonZeroUsize::MIN, 0, 0)
    }

    /// Returns the completed logical operation count.
    pub const fn operations(self) -> usize {
        self.operations
    }

    /// Returns raw bytes acquired during this call.
    pub const fn read_bytes(self) -> usize {
        self.read_bytes
    }

    /// Returns raw bytes written to the underlying I/O capability during this call.
    pub const fn written_bytes(self) -> usize {
        self.written_bytes
    }

    /// Returns whether this report fits the supplied hard budget.
    pub const fn fits(self, budget: TransportBudget) -> bool {
        self.operations <= budget.operations().get()
            && self.read_bytes <= budget.read_bytes().get()
            && self.written_bytes <= budget.write_bytes().get()
    }

    /// Returns whether the transport reported no progress.
    pub const fn is_idle(self) -> bool {
        self.operations == 0 && self.read_bytes == 0 && self.written_bytes == 0
    }
}
