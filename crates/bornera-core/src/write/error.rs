//! Ownership-preserving admission rejection and exact-progress faults.

use core::fmt;

use calandria::RetainedBytes;

use crate::{ConnectionEpoch, Delivery, EffectId};

/// Pending identity category that a new frame attempted to reuse.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteIdentityKind {
    /// Accepted operation identity.
    Operation,
    /// Write effect identity.
    Effect,
}

/// Why a complete frame could not enter the bounded writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteAdmissionFailure {
    /// The frame names an epoch other than the writer's fixed epoch.
    StaleEpoch {
        /// Epoch owned by the writer.
        expected: ConnectionEpoch,
        /// Epoch supplied with the frame.
        received: ConnectionEpoch,
    },
    /// An operation or effect identity is already retained.
    IdentityInUse(WriteIdentityKind),
    /// The configured frame-count capacity was reached.
    FrameCapacityReached {
        /// Configured maximum frames.
        limit: usize,
    },
    /// Retaining the frame would exceed the configured byte bound.
    RetainedByteCapacity {
        /// Bytes already retained by the writer.
        retained: RetainedBytes,
        /// Bytes retained by the rejected frame.
        incoming: RetainedBytes,
        /// Configured retained-byte maximum.
        limit: RetainedBytes,
    },
}

/// Rejected admission that preserves the exact unsent frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteAdmissionError<F> {
    failure: WriteAdmissionFailure,
    frame: F,
}

impl<F> WriteAdmissionError<F> {
    pub(crate) const fn new(failure: WriteAdmissionFailure, frame: F) -> Self {
        Self { failure, frame }
    }

    /// Returns the mechanical admission failure.
    pub const fn failure(&self) -> WriteAdmissionFailure {
        self.failure
    }

    /// Returns certainty for a frame never accepted by the writer.
    pub const fn delivery(&self) -> Delivery {
        Delivery::NotSent
    }

    /// Recovers the exact unadmitted frame.
    pub fn into_frame(self) -> F {
        self.frame
    }
}

impl<F> fmt::Display for WriteAdmissionError<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.failure.fmt(formatter)
    }
}

impl<F: fmt::Debug> core::error::Error for WriteAdmissionError<F> {}

impl fmt::Display for WriteAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::StaleEpoch { .. } => "frame belongs to another connection epoch",
            Self::IdentityInUse(_) => "operation or effect identity is already queued",
            Self::FrameCapacityReached { .. } => "write-frame capacity is exhausted",
            Self::RetainedByteCapacity { .. } => "write retained-byte capacity is exhausted",
        })
    }
}

/// Why reported transport progress could not mutate the FIFO front.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteProgressError {
    /// The progress event belongs to another epoch.
    StaleEpoch {
        /// Epoch owned by the writer.
        expected: ConnectionEpoch,
        /// Epoch supplied with progress.
        received: ConnectionEpoch,
    },
    /// No frame currently awaits progress.
    NoPendingWrite,
    /// Progress named an effect other than the FIFO front.
    OutOfOrderEffect {
        /// Effect at the FIFO front.
        expected: EffectId,
        /// Effect supplied with progress.
        received: EffectId,
    },
    /// Reported bytes exceed the frame's remaining bytes.
    ExceedsRemaining {
        /// Bytes reported written.
        written: usize,
        /// Bytes remaining before the report.
        remaining: usize,
    },
    /// Internal retained-byte release exceeded the writer's accounted total.
    RetainedAccountingUnderflow {
        /// Bytes accounted before release.
        retained: RetainedBytes,
        /// Bytes the completed or discarded frame required releasing.
        released: RetainedBytes,
    },
}

impl fmt::Display for WriteProgressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::StaleEpoch { .. } => "write progress belongs to another epoch",
            Self::NoPendingWrite => "no ordered write is pending",
            Self::OutOfOrderEffect { .. } => "write progress does not name the FIFO front",
            Self::ExceedsRemaining { .. } => "write progress exceeds remaining frame bytes",
            Self::RetainedAccountingUnderflow { .. } => {
                "write retained-byte accounting underflowed"
            }
        })
    }
}

impl core::error::Error for WriteProgressError {}
