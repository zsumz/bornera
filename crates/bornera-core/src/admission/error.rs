//! Admission and commit failures that preserve pre-publication ownership.

use core::fmt;

use crate::{Delivery, WriteAdmissionFailure};

use super::OperationPermit;

/// Why an operation reservation was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReserveError {
    /// The connection is not accepting this class of work.
    AdmissionClosed,
    /// The absolute deadline is already due.
    DeadlineElapsed,
    /// The accepted-operation count is full.
    OperationCapacity,
    /// Semantic retained-byte capacity is full.
    RetainedByteCapacity,
    /// Write-frame count or retained-memory capacity is full.
    WriteCapacity,
    /// No match key is currently available.
    MatchKeyExhausted,
    /// A generated fixed-width identity is exhausted.
    IdentityExhausted,
    /// A prior invariant failure poisoned this fixed-epoch owner.
    OwnerPoisoned,
}

impl fmt::Display for ReserveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AdmissionClosed => "connection admission is closed",
            Self::DeadlineElapsed => "operation deadline is already elapsed",
            Self::OperationCapacity => "operation count capacity is exhausted",
            Self::RetainedByteCapacity => "retained byte capacity is exhausted",
            Self::WriteCapacity => "write-frame count or retained-memory capacity is exhausted",
            Self::MatchKeyExhausted => "match key space is exhausted",
            Self::IdentityExhausted => "operation or effect identity is exhausted",
            Self::OwnerPoisoned => "connection owner is poisoned",
        })
    }
}

impl core::error::Error for ReserveError {}

/// Why a reserved operation could not be committed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CommitErrorKind {
    /// The permit belongs to another machine or epoch.
    ForeignPermit,
    /// Admission closed after reservation and before commit.
    AdmissionClosed,
    /// The frame's cached retained footprint exceeds its reserved write-memory capacity.
    FrameTooLarge,
    /// A prior invariant failure poisoned this fixed-epoch owner.
    OwnerPoisoned,
}

/// Why an atomic frame commit could not transfer ownership to the writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FrameCommitFailure {
    /// Connection policy rejected the still-affine permit.
    Policy(CommitErrorKind),
    /// The bounded write owner rejected the complete frame.
    Writer(WriteAdmissionFailure),
}

/// Failed atomic commit preserving both the permit and exact unsent frame.
pub struct FrameCommitError<F> {
    failure: FrameCommitFailure,
    permit: OperationPermit,
    frame: F,
}

impl<F> FrameCommitError<F> {
    pub(crate) const fn new(
        failure: FrameCommitFailure,
        permit: OperationPermit,
        frame: F,
    ) -> Self {
        Self {
            failure,
            permit,
            frame,
        }
    }

    /// Returns the mechanical rejection reason.
    pub const fn failure(&self) -> FrameCommitFailure {
        self.failure
    }

    /// Returns certainty for a frame never accepted by the writer.
    pub const fn delivery(&self) -> Delivery {
        Delivery::NotSent
    }

    /// Recovers the still-affine permit and exact unadmitted frame.
    pub fn into_parts(self) -> (OperationPermit, F) {
        (self.permit, self.frame)
    }
}

impl<F> fmt::Debug for FrameCommitError<F>
where
    F: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FrameCommitError")
            .field("failure", &self.failure)
            .field("permit", &self.permit)
            .field("frame", &self.frame)
            .finish()
    }
}

impl<F> fmt::Display for FrameCommitError<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.failure.fmt(formatter)
    }
}

impl<F: fmt::Debug> core::error::Error for FrameCommitError<F> {}

impl fmt::Display for FrameCommitFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Policy(kind) => formatter.write_str(match kind {
                CommitErrorKind::ForeignPermit => "operation permit belongs to another epoch",
                CommitErrorKind::AdmissionClosed => "admission closed before operation commit",
                CommitErrorKind::FrameTooLarge => {
                    "frame retained footprint exceeds reserved write memory"
                }
                CommitErrorKind::OwnerPoisoned => "connection owner is poisoned",
            }),
            Self::Writer(failure) => failure.fmt(formatter),
        }
    }
}
