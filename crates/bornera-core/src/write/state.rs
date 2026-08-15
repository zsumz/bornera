//! Immutable admission, slice, progress, and discard observations.

use calandria::RetainedBytes;

use crate::{ConnectionEpoch, Delivery, EffectId, OperationId};

/// Whether one progress report crossed the conservative delivery boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteBoundary {
    /// The operation had already crossed or no byte progressed.
    Unchanged,
    /// The first positive byte progress occurred in this report.
    Crossed,
}

/// Complete frame ownership accepted by the ordered writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteAccepted {
    /// Owning epoch.
    pub epoch: ConnectionEpoch,
    /// Owning operation.
    pub operation: OperationId,
    /// Write effect required by progress reports.
    pub effect: EffectId,
    /// Complete contiguous frame bytes.
    pub frame_bytes: usize,
    /// Variable memory retained by the frame.
    pub retained_bytes: RetainedBytes,
}

impl WriteAccepted {
    /// Returns certainty before any positive write progress.
    pub const fn delivery(self) -> Delivery {
        Delivery::NotSent
    }
}

/// Borrowed bytes from only the FIFO queue front.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteSlice<'a> {
    /// Owning epoch.
    pub epoch: ConnectionEpoch,
    /// Owning operation.
    pub operation: OperationId,
    /// Write effect required by the corresponding progress report.
    pub effect: EffectId,
    /// Next contiguous bytes, capped by the caller's requested size.
    pub bytes: &'a [u8],
}

/// Result of applying exact progress to the FIFO front.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteProgress<F> {
    /// The same frame remains at the FIFO front.
    Pending {
        /// Owning operation.
        operation: OperationId,
        /// Owning effect.
        effect: EffectId,
        /// Bytes remaining in the complete frame.
        remaining: usize,
        /// Whether this report crossed the delivery boundary.
        boundary: WriteBoundary,
        /// Current conservative delivery certainty.
        delivery: Delivery,
    },
    /// The complete frame left write ownership.
    Complete {
        /// Owning operation.
        operation: OperationId,
        /// Owning effect.
        effect: EffectId,
        /// Original complete frame.
        frame: F,
        /// Whether this report crossed the delivery boundary.
        boundary: WriteBoundary,
        /// Current conservative delivery certainty.
        delivery: Delivery,
    },
}

/// One frame removed before normal write completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscardedWrite<F> {
    /// Owning operation.
    pub operation: OperationId,
    /// Owning effect.
    pub effect: EffectId,
    /// Original complete frame.
    pub frame: F,
    /// Exact bytes that had progressed before removal.
    pub written: usize,
    /// Conservative delivery certainty at removal.
    pub delivery: Delivery,
}

/// Bounded frames removed together when an epoch closes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscardedWrites<F> {
    pub(crate) writes: Vec<DiscardedWrite<F>>,
    pub(crate) retained_bytes: RetainedBytes,
}

impl<F> DiscardedWrites<F> {
    /// Returns discarded writes in original wire order.
    pub fn writes(&self) -> &[DiscardedWrite<F>] {
        &self.writes
    }

    /// Returns variable memory released by the discarded frames.
    pub const fn retained_bytes(&self) -> RetainedBytes {
        self.retained_bytes
    }

    /// Recovers discarded frames in original wire order.
    pub fn into_writes(self) -> Vec<DiscardedWrite<F>> {
        self.writes
    }
}
