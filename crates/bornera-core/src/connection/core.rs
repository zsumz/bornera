//! Aggregate deterministic ownership of policy and complete outbound frames.

use core::num::NonZeroUsize;

use calandria::{Moment, RetainedBytes};

use crate::{
    ConnectionCoreInvariant, ConnectionEpoch, ConnectionId, ConnectionLimits, ConnectionMachine,
    ConnectionSnapshot, ConnectionTransition, EffectId, EndpointId, FrameCommitError,
    FrameCommitFailure, IdentitySeeds, LaneId, OperationId, OperationOptions, OperationPermit,
    OrderedVerified, ReserveError, WriteFrame, WriteQueue, WriteSlice,
};

use super::journal::RecoveryJournal;

/// Sole deterministic mutation owner for one exact connection epoch.
#[derive(Debug)]
pub struct ConnectionCore<F> {
    pub(super) machine: ConnectionMachine,
    pub(super) writes: WriteQueue<F>,
    pub(super) journal: RecoveryJournal<F>,
    pub(super) poisoned: Option<ConnectionCoreInvariant>,
}

impl<F: WriteFrame> ConnectionCore<F> {
    /// Creates a live fixed-epoch owner with session-only admission.
    pub fn new(
        endpoint: EndpointId,
        lane: LaneId,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        limits: ConnectionLimits,
    ) -> Self {
        Self {
            machine: ConnectionMachine::new(endpoint, lane, connection, epoch, limits),
            writes: WriteQueue::new(epoch, limits.write_queue_limits()),
            journal: RecoveryJournal::new(limits.max_operations(), limits.max_write_frames()),
            poisoned: None,
        }
    }

    /// Creates a fixed epoch with deterministic identity seeds.
    pub fn with_identity_seeds(
        endpoint: EndpointId,
        lane: LaneId,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        limits: ConnectionLimits,
        seeds: IdentitySeeds,
    ) -> Self {
        Self {
            machine: ConnectionMachine::with_identity_seeds(
                endpoint, lane, connection, epoch, limits, seeds,
            ),
            writes: WriteQueue::new(epoch, limits.write_queue_limits()),
            journal: RecoveryJournal::new(limits.max_operations(), limits.max_write_frames()),
            poisoned: None,
        }
    }

    /// Atomically reserves all operation resources before frame preparation.
    pub fn reserve(
        &mut self,
        now: Moment,
        options: OperationOptions,
    ) -> Result<OperationPermit, ReserveError> {
        if self
            .ensure_healthy()
            .and_then(|()| self.verify_ownership())
            .is_err()
        {
            return Err(ReserveError::OwnerPoisoned);
        }
        self.machine.reserve(now, options)
    }

    /// Atomically transfers an affine permit and exact complete frame.
    pub fn commit(
        &mut self,
        permit: OperationPermit,
        frame: F,
    ) -> Result<(OperationId, ConnectionTransition), FrameCommitError<F>> {
        if self
            .ensure_healthy()
            .and_then(|()| self.verify_ownership())
            .is_err()
        {
            return Err(FrameCommitError::new(
                FrameCommitFailure::Policy(crate::CommitErrorKind::OwnerPoisoned),
                permit,
                frame,
            ));
        }
        let frame_bytes = frame.retained_bytes();
        if let Some(kind) = self.machine.commit_failure(&permit, frame_bytes) {
            return Err(FrameCommitError::new(
                FrameCommitFailure::Policy(kind),
                permit,
                frame,
            ));
        }
        if let Err(error) = self
            .writes
            .admit(permit.epoch, permit.operation, permit.effect, frame)
        {
            return Err(FrameCommitError::new(
                FrameCommitFailure::Writer(error.failure()),
                permit,
                error.into_frame(),
            ));
        }
        Ok(self.machine.commit_permit(permit, frame_bytes))
    }

    /// Borrows at most `maximum` bytes from the exact FIFO write front.
    pub fn front_write(&self, maximum: NonZeroUsize) -> Option<WriteSlice<'_>> {
        self.writes.front(maximum)
    }

    /// Returns the exact epoch permanently owned by this aggregate.
    pub const fn epoch(&self) -> ConnectionEpoch {
        self.machine.epoch()
    }

    /// Returns current deterministic policy state.
    pub fn snapshot(&self) -> ConnectionSnapshot {
        self.machine.snapshot()
    }

    /// Returns immutable ordered-matching state.
    pub const fn matching(&self) -> &OrderedVerified {
        self.machine.matching()
    }

    /// Returns read-only access to the underlying policy state for diagnostics.
    pub const fn policy(&self) -> &ConnectionMachine {
        &self.machine
    }

    /// Returns read-only access to the bounded frame owner for diagnostics.
    pub const fn writes(&self) -> &WriteQueue<F> {
        &self.writes
    }

    /// Returns complete frames still awaiting transport completion.
    pub fn queued_write_frames(&self) -> usize {
        self.writes.queued_frames()
    }

    /// Returns the internal write identity retained for an accepted operation.
    pub fn write_effect(&self, operation: OperationId) -> Option<EffectId> {
        self.writes.effect_for(operation)
    }

    /// Returns bytes retained by complete outbound frames.
    pub const fn buffered_write_bytes(&self) -> RetainedBytes {
        self.writes.retained_bytes()
    }
}
