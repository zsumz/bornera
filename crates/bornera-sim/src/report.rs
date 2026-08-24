//! Data-only observations emitted by exact deterministic replay.

use bornera_core::{
    ConnectionCoreError, ConnectionRecovery, ConnectionSnapshot, ConnectionTransition,
    FrameCommitFailure, MatchKey, OperationId, ReserveError,
};

use crate::{OperationIndex, SimFrame};

/// Result of applying one trace action.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StepResult {
    /// One operation was reserved and committed under a trace-local identity.
    Submitted {
        /// Stable trace-local reference used by later actions.
        index: OperationIndex,
        /// Core-assigned operation identity.
        operation: OperationId,
        /// Core-assigned protocol match key.
        match_key: MatchKey,
        /// Required effects emitted by commit.
        transition: ConnectionTransition,
    },
    /// Reservation was rejected without accepting an operation.
    ReserveRejected(ReserveError),
    /// Frame commit was rejected and both affine inputs were recovered.
    CommitRejected(FrameCommitFailure),
    /// A command without an inbound reply produced this transition.
    UnitTransition(ConnectionTransition),
    /// One inbound reply produced this frame-owning transition.
    ReplyTransition(ConnectionTransition<SimFrame>),
    /// Exact transport write progression produced this transition.
    WriteTransition {
        /// Operation owning the write effect.
        operation: OperationId,
        /// Byte count reported by the simulated transport.
        bytes: usize,
        /// Required effects emitted by progression.
        transition: ConnectionTransition,
    },
    /// No frame currently belongs to transport write ownership.
    NoPendingWrite,
    /// The trace-local identity does not name an accepted operation.
    UnknownOperation(OperationIndex),
    /// Core policy failed closed while applying the action.
    CoreFailed(ConnectionCoreError),
    /// Aggregate recovery transferred every remaining owned observation.
    Recovered(ConnectionRecovery<SimFrame>),
}

/// One action result paired with the complete post-action owner snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct StepObservation {
    /// Zero-based action position in the replayed trace.
    pub action: usize,
    /// Mechanical result of applying the action.
    pub result: StepResult,
    /// Aggregate state immediately after the action.
    pub snapshot: ConnectionSnapshot,
}

/// Complete replay result suitable for byte-for-byte equality comparison.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ReplayReport {
    observations: Vec<StepObservation>,
    final_snapshot: ConnectionSnapshot,
}

impl ReplayReport {
    pub(crate) const fn new(
        observations: Vec<StepObservation>,
        final_snapshot: ConnectionSnapshot,
    ) -> Self {
        Self {
            observations,
            final_snapshot,
        }
    }

    /// Borrows observations in exact action order.
    pub fn observations(&self) -> &[StepObservation] {
        &self.observations
    }

    /// Returns the final aggregate snapshot.
    pub const fn final_snapshot(&self) -> ConnectionSnapshot {
        self.final_snapshot
    }
}

pub(crate) fn unit_result(result: Result<ConnectionTransition, ConnectionCoreError>) -> StepResult {
    result.map_or_else(StepResult::CoreFailed, StepResult::UnitTransition)
}

pub(crate) fn reply_result(
    result: Result<ConnectionTransition<SimFrame>, ConnectionCoreError>,
) -> StepResult {
    result.map_or_else(StepResult::CoreFailed, StepResult::ReplyTransition)
}
