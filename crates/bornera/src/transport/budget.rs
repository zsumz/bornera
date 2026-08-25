//! Explicit work and byte bounds for one transport-local progression call.

use core::num::NonZeroUsize;

/// Hard bounds supplied to one transport progression call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportBudget {
    operations: NonZeroUsize,
    read_bytes: NonZeroUsize,
    write_bytes: NonZeroUsize,
}

impl TransportBudget {
    /// Creates explicit operation and raw-I/O byte bounds.
    pub const fn new(
        operations: NonZeroUsize,
        read_bytes: NonZeroUsize,
        write_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            operations,
            read_bytes,
            write_bytes,
        }
    }

    /// Returns the maximum logical operations this call may report.
    pub const fn operations(self) -> NonZeroUsize {
        self.operations
    }

    /// Returns the maximum raw bytes this call may acquire.
    pub const fn read_bytes(self) -> NonZeroUsize {
        self.read_bytes
    }

    /// Returns the maximum raw bytes this call may write to underlying I/O.
    pub const fn write_bytes(self) -> NonZeroUsize {
        self.write_bytes
    }
}
