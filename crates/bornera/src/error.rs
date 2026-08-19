//! Construction, owner-integrity, and ownership-preserving commit failures.

use core::fmt;
use std::io;

use bornera_core::{ConnectionCoreError, FrameCommitError, FrameDecodeError};
use calandria::{EventBatchFailure, TimerScheduleFailure};
use calandria_mio::MioError;

use crate::OwnerFailure;

/// Failure before one production connection owner becomes observable.
#[derive(Debug)]
pub enum ConnectError<E> {
    /// The operating system rejected creation of the nonblocking stream.
    Io(io::Error),
    /// The Mio readiness adapter could not be created or registered.
    Mio(MioError),
    /// The supplied decoder began outside its retained-memory contract.
    Decoder(FrameDecodeError<E>),
    /// The single transport could not enter its preallocated resource slot.
    ResourceAdmission,
}

/// Fatal divergence inside an otherwise bounded production owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineInvariant {
    /// A Calandria resource token no longer named the engine's live capability.
    ResourceToken,
    /// An aggregate-internal frame discard escaped into the capability interpreter.
    UnexpectedDiscardEffect,
    /// Unit-input policy unexpectedly attempted to publish a reply payload.
    UnexpectedUnitReply,
    /// The bounded deadline-token index diverged from core operation capacity.
    DeadlineIndexCapacity,
    /// A timer could not retain a deadline already reserved by core policy.
    DeadlineSchedule(TimerScheduleFailure),
    /// A terminal result could not enter its pre-reserved publication capacity.
    OutcomePublication(EventBatchFailure),
    /// A rejected terminal result could not enter the bounded recovery owner.
    RecoveryOutcomePublication(EventBatchFailure),
    /// A lifecycle edge could not enter its separately bounded publication stream.
    LifecyclePublication(EventBatchFailure),
    /// A rejected lifecycle edge could not enter the bounded recovery owner.
    RecoveryLifecyclePublication(EventBatchFailure),
    /// The fixed-width lifecycle sequence could not advance without aliasing.
    EventSequenceExhausted,
    /// Closing policy did not retain the mechanical reason required for publication.
    MissingCloseReason,
}

/// Fatal owner or readiness-adapter failure returned by a bounded turn.
///
/// Once returned, normal driving must stop and the engine must be consumed by
/// [`crate::ConnectionEngine::try_recover`].
#[derive(Debug)]
pub enum EngineError {
    /// Mio registration, polling, or deregistration failed.
    Mio(MioError),
    /// The deterministic aggregate requires explicit recovery.
    Core(ConnectionCoreError),
    /// An internal ownership invariant diverged.
    Invariant(EngineInvariant),
    /// A prior fatal error permanently stopped normal owner mutation.
    OwnerFailed(OwnerFailure),
}

/// Commit either rejects before publication or reports fatal owner divergence.
#[derive(Debug)]
pub enum EngineCommitError<F> {
    /// Policy or writer admission rejected and preserved both permit and frame.
    Rejected(Box<FrameCommitError<F>>),
    /// The accepted operation exposed a fatal owner invariant while publishing effects.
    Owner {
        /// Operation already accepted before the fatal owner divergence.
        operation: bornera_core::OperationId,
        /// Fatal owner failure.
        source: EngineError,
    },
    /// A prior or newly observed fatal error rejected the affine permit and frame.
    OwnerFailed {
        /// Latched mechanical owner-failure category.
        reason: OwnerFailure,
        /// Permit that never transferred into accepted operation ownership.
        permit: bornera_core::OperationPermit,
        /// Exact frame that never transferred into write ownership.
        frame: F,
    },
}

impl<E: fmt::Display> fmt::Display for ConnectError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(source) => source.fmt(formatter),
            Self::Mio(source) => source.fmt(formatter),
            Self::Decoder(source) => source.fmt(formatter),
            Self::ResourceAdmission => {
                formatter.write_str("transport resource admission unexpectedly failed")
            }
        }
    }
}

impl<E> core::error::Error for ConnectError<E> where E: core::error::Error + 'static {}

impl fmt::Display for EngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mio(source) => source.fmt(formatter),
            Self::Core(source) => source.fmt(formatter),
            Self::Invariant(source) => source.fmt(formatter),
            Self::OwnerFailed(_) => formatter.write_str("connection owner previously failed"),
        }
    }
}

impl core::error::Error for EngineError {}

impl fmt::Display for EngineInvariant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ResourceToken => "the transport resource token is stale or absent",
            Self::UnexpectedDiscardEffect => "an internal write discard escaped the aggregate",
            Self::UnexpectedUnitReply => "unit policy transition published a reply",
            Self::DeadlineIndexCapacity => "deadline index exceeded operation capacity",
            Self::DeadlineSchedule(_) => "reserved operation deadline could not be scheduled",
            Self::OutcomePublication(_) => "reserved terminal outcome could not be published",
            Self::RecoveryOutcomePublication(_) => {
                "terminal outcome exceeded bounded recovery ownership"
            }
            Self::LifecyclePublication(_) => "bounded lifecycle event publication failed",
            Self::RecoveryLifecyclePublication(_) => {
                "lifecycle edge exceeded bounded recovery ownership"
            }
            Self::EventSequenceExhausted => "connection event sequence is exhausted",
            Self::MissingCloseReason => "closing connection retained no mechanical reason",
        })
    }
}

impl<F: fmt::Debug> fmt::Display for EngineCommitError<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(source) => source.fmt(formatter),
            Self::Owner { source, .. } => source.fmt(formatter),
            Self::OwnerFailed { .. } => formatter.write_str("connection owner previously failed"),
        }
    }
}

impl<F: fmt::Debug> core::error::Error for EngineCommitError<F> {}

impl<E> From<io::Error> for ConnectError<E> {
    fn from(source: io::Error) -> Self {
        Self::Io(source)
    }
}

impl<E> From<MioError> for ConnectError<E> {
    fn from(source: MioError) -> Self {
        Self::Mio(source)
    }
}

impl From<MioError> for EngineError {
    fn from(source: MioError) -> Self {
        Self::Mio(source)
    }
}
