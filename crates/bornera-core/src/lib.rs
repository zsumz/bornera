//! Deterministic, sans-I/O connection policy for native protocol clients.

mod admission;
mod connection;
mod delivery;
mod framing;
mod identity;
mod limits;
mod matching;
mod operation;
mod write;

pub use admission::{
    AdmissionClass, AdmissionGate, CommitErrorKind, CompletionMode, FrameCommitError,
    FrameCommitFailure, OperationOptions, OperationPermit, ReserveError,
};
pub use calandria::{Deadline, Moment, RetainedBytes};
pub use connection::{
    CancelOutcome, CloseReason, ConnectionCore, ConnectionCoreError, ConnectionCoreInvariant,
    ConnectionEffect, ConnectionInput, ConnectionMachine, ConnectionPhase, ConnectionRecovery,
    ConnectionSnapshot, ConnectionTransition, InboundReply, InputDisposition, RecoveredOperation,
};
pub use delivery::Delivery;
pub use framing::{FrameDecodeError, FrameDecoder, FrameDriver};
pub use identity::{
    ConnectionEpoch, ConnectionId, EffectId, EndpointId, IdentitySeeds, LaneId, MatchKey,
    OperationId,
};
pub use limits::{ConnectionLimits, LimitsError, MatchKeySpace};
pub use matching::OrderedVerified;
pub use operation::{OperationFailure, OperationOutcome, OperationPhase};
pub use write::{
    DiscardedWrite, DiscardedWrites, FrameContractViolation, FrameMeasure, WriteAdmissionFailure,
    WriteFrame, WriteIdentityKind, WriteProgressError, WriteSlice,
};
