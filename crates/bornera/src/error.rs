//! Construction, owner-integrity, and ownership-preserving commit failures.

use std::io;

use bornera_core::{ConnectionCoreError, FrameCommitError, FrameDecodeError};
use calandria::{EventBatchFailure, TimerScheduleFailure};
use calandria_mio::MioError;

use crate::{OwnerFailure, TransportPressure};

mod display;

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
    /// The transport exceeded the slot's retained bound during construction or registration.
    TransportCapacity {
        /// Configured aggregate transport retained-memory bound.
        limit: calandria::RetainedBytes,
        /// Auditable pressure reported during construction or registration.
        reported: TransportPressure,
    },
    /// The adapter's declared pressure ceiling was incompatible with its slot binding.
    TransportLimit {
        /// Configured slot ceiling or previously bound adapter ceiling.
        limit: calandria::RetainedBytes,
        /// Stable ceiling declared by the returned transport.
        reported: calandria::RetainedBytes,
    },
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
    /// An ordered drain reached physical closure without its absolute shutdown bound.
    MissingShutdownDeadline,
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
    /// A transport exceeded its configured retained-memory capacity.
    TransportRetainedCapacity {
        /// Configured aggregate transport retained-memory bound.
        limit: calandria::RetainedBytes,
        /// Auditable pressure observed while the transport was live.
        reported: TransportPressure,
    },
    /// A selector-free adapter declared an incompatible or unstable pressure ceiling.
    TransportLimitContract {
        /// Configured slot ceiling or previously bound adapter ceiling.
        limit: calandria::RetainedBytes,
        /// Stable ceiling declared by the supplied transport.
        reported: calandria::RetainedBytes,
    },
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
