//! Construction, owner-integrity, and ownership-preserving commit failures.

use core::fmt;
use std::io;

use bornera_core::{ConnectionCoreError, FrameCommitError, FrameDecodeError};
use calandria::{EventBatchFailure, TimerScheduleFailure};
use calandria_mio::MioError;

use crate::OwnerFailure;

/// Failure before one production connection owner becomes observable.
#[derive(Debug)]
#[non_exhaustive]
pub enum ConnectError<E> {
    /// The operating system rejected creation of the nonblocking stream.
    Io(io::Error),
    /// The Mio readiness adapter could not be created or registered.
    Mio(MioError),
    /// The supplied decoder began outside its retained-memory contract.
    Decoder(FrameDecodeError<E>),
    /// The bounded connection set has no free resource slot.
    ResourceAdmission,
    /// The shared selector owner had already failed permanently.
    OwnerFailed(OwnerFailure),
}

/// Fatal divergence inside an otherwise bounded production owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EngineInvariant {
    /// A resource generation already proven live disappeared internally.
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
    /// A newer core emitted an effect this production owner cannot interpret.
    UnsupportedCoreEffect,
    /// A safe transport implementation reported more bytes than the supplied read buffer.
    TransportReadContract {
        /// Buffer length supplied to the transport.
        capacity: usize,
        /// Impossible byte count reported by the transport.
        reported: usize,
    },
    /// A safe transport reported work outside the supplied hard budget.
    TransportProgressContract {
        /// Hard bounds supplied for the call.
        budget: crate::TransportBudget,
        /// Impossible progress reported by the transport.
        reported: crate::TransportProgress,
    },
    /// A transport claimed application readiness before accepting establishment policy.
    TransportOpenedBeforeEstablishment,
    /// A transport advertised immediate work but performed none.
    TransportNoProgress,
}

/// Fatal slot or readiness-adapter failure returned by a bounded operation.
///
/// A connection-local invariant fences that slot. Any post-admission Mio
/// lifecycle or poll failure fences the shared selector and every live slot.
#[derive(Debug)]
#[non_exhaustive]
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
#[non_exhaustive]
pub enum EngineCommitError<F> {
    /// Policy or writer admission rejected and preserved both permit and frame.
    Rejected(Box<FrameCommitError<F>>),
    /// The operation was accepted before effect publication exposed fatal owner divergence.
    ///
    /// The caller must publish any reserved semantic context for `operation`, must not
    /// retry the frame, and must recover the failed owner.
    AcceptedOwnerFailure {
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
            Self::ResourceAdmission => formatter.write_str("connection set capacity is exhausted"),
            Self::OwnerFailed(_) => formatter.write_str("shared selector owner previously failed"),
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
            Self::ResourceToken => "a proven-live transport resource disappeared",
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
            Self::UnsupportedCoreEffect => "connection core emitted an unsupported effect",
            Self::TransportReadContract { .. } => {
                "transport reported a read larger than the supplied buffer"
            }
            Self::TransportProgressContract { .. } => {
                "transport progression exceeded its supplied budget"
            }
            Self::TransportOpenedBeforeEstablishment => {
                "transport opened before bounded establishment accepted its policy"
            }
            Self::TransportNoProgress => {
                "transport advertised immediate work without making progress"
            }
        })
    }
}

impl<F: fmt::Debug> fmt::Display for EngineCommitError<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(source) => source.fmt(formatter),
            Self::AcceptedOwnerFailure { source, .. } => source.fmt(formatter),
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
