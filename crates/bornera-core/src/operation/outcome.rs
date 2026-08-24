//! Mechanical operation outcomes without protocol retry meaning.

use crate::{CloseReason, Delivery};

/// The transport-owned phase of an accepted operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OperationPhase {
    /// The complete frame is queued but transport write ownership has not begun.
    Queued,
    /// At least one byte was written, but the writer still owns an unwritten tail.
    Writing,
    /// The complete frame left local write ownership and a reply may arrive.
    AwaitingReply,
    /// One terminal outcome has been emitted.
    Terminal,
}

/// A mechanical failure observed by the connection owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OperationFailure {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The owning connection epoch closed.
    ConnectionClosed(CloseReason),
    /// The received reply named another live operation instead of the FIFO front.
    MatchKeyMismatch {
        /// Match key required by the FIFO front.
        expected: crate::MatchKey,
        /// Match key carried by the received reply.
        received: crate::MatchKey,
    },
}

/// Exactly one terminal outcome for an accepted operation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OperationOutcome<F> {
    /// The matching discipline assigned a reply to this operation.
    Reply(F),
    /// The complete frame left local write ownership without awaiting a reply.
    WriteComplete {
        /// What local transport ownership can prove.
        delivery: Delivery,
    },
    /// Mechanical failure with conservative delivery certainty.
    Failed {
        /// The observed failure.
        failure: OperationFailure,
        /// What local transport ownership can prove.
        delivery: Delivery,
    },
    /// Explicit local cancellation with conservative delivery certainty.
    Cancelled {
        /// What local transport ownership can prove.
        delivery: Delivery,
    },
}

impl<F: calandria::Retained> calandria::Retained for OperationOutcome<F> {
    fn retained_bytes(&self) -> calandria::RetainedBytes {
        match self {
            Self::Reply(frame) => frame.retained_bytes(),
            Self::WriteComplete { .. } | Self::Failed { .. } | Self::Cancelled { .. } => {
                calandria::RetainedBytes::ZERO
            }
        }
    }
}
