//! Public fail-closed errors for aggregate ownership disagreement.

use core::fmt;

use crate::{EffectId, InputDisposition, OperationId, WriteProgressError};

/// Fatal disagreement between the aggregate policy and frame owners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectionCoreInvariant {
    /// A safe frame implementation changed its byte-view shape after commit.
    FrameContractViolation(crate::FrameContractViolation),
    /// Policy required a frame that the aggregate writer did not own.
    MissingWrite {
        /// Accepted operation that should own the frame.
        operation: OperationId,
        /// Exact write identity expected by policy.
        effect: EffectId,
    },
    /// A discarded frame belonged to a different operation than policy named.
    DiscardedWriteMismatch {
        /// Operation named by policy.
        expected: OperationId,
        /// Operation retained by the writer.
        actual: OperationId,
    },
    /// Policy and writer disagree about one accepted operation's write identity.
    WriteIdentityMismatch {
        /// Accepted operation whose identity diverged.
        operation: OperationId,
        /// Exact effect retained by policy.
        expected: EffectId,
        /// Different effect retained by the writer.
        actual: EffectId,
    },
    /// The writer retained a frame with no matching policy ownership.
    UnexpectedWrite {
        /// Operation named by the unexpected frame.
        operation: OperationId,
        /// Effect named by the unexpected frame.
        effect: EffectId,
    },
    /// Exact writer progress could not be applied to the corresponding policy record.
    WritePolicyMismatch {
        /// Operation whose frame progressed.
        operation: OperationId,
        /// Write identity whose frame progressed.
        effect: EffectId,
        /// Policy classification that rejected the progress.
        disposition: InputDisposition,
    },
    /// Reservation accounting detected impossible release or commit state.
    ReservationAccounting,
    /// Outbound retained-byte accounting detected an impossible release.
    WriteAccounting,
    /// A transport reported positive progress beyond the exact supplied write slice.
    WriteProgressContract {
        /// Impossible byte count reported by the transport.
        written: usize,
        /// Bytes present in the exact supplied slice.
        remaining: usize,
    },
    /// Preallocated recovery-journal capacity disagreed with configured ownership bounds.
    RecoveryJournalCapacity,
    /// Explicit recovery permanently consumed this fixed-epoch owner.
    Recovered,
}

/// Fatal deterministic aggregate failure requiring explicit owner recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectionCoreError {
    /// Exact frame progress violated FIFO or accounting ownership.
    Write(WriteProgressError),
    /// Policy and frame ownership diverged.
    Invariant(ConnectionCoreInvariant),
    /// A prior aggregate failure poisoned this fixed epoch.
    Poisoned(ConnectionCoreInvariant),
}

impl fmt::Display for ConnectionCoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Write(source) => source.fmt(formatter),
            Self::Invariant(_) => formatter.write_str("connection ownership diverged"),
            Self::Poisoned(_) => formatter.write_str("connection owner is poisoned"),
        }
    }
}

impl core::error::Error for ConnectionCoreError {}
