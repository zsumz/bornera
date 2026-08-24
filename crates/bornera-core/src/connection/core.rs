//! Aggregate deterministic ownership of policy and complete outbound frames.

use core::{cell::Cell, num::NonZeroUsize};

use calandria::{Moment, RetainedBytes};

use crate::{
    ConnectionCoreInvariant, ConnectionEpoch, ConnectionId, ConnectionLimits, ConnectionMachine,
    ConnectionSnapshot, ConnectionTransition, EffectId, EndpointId, FrameCommitError,
    FrameCommitFailure, FrameMeasure, IdentitySeeds, LaneId, OperationId, OperationOptions,
    OperationPermit, OrderedVerified, ReserveError, WriteFrame, WriteSlice,
};

use super::journal::RecoveryJournal;
use crate::write::WriteQueue;

/// Sole deterministic mutation owner for one exact connection epoch.
#[derive(Debug)]
pub struct ConnectionCore<F> {
    pub(super) machine: ConnectionMachine,
    pub(super) writes: WriteQueue<F>,
    pub(super) journal: RecoveryJournal<F>,
    pub(super) poisoned: Cell<Option<ConnectionCoreInvariant>>,
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
            poisoned: Cell::new(None),
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
            poisoned: Cell::new(None),
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
            .and_then(|()| self.verify_ownership_on_hot_path())
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
            .and_then(|()| self.verify_ownership_on_hot_path())
            .is_err()
        {
            return Err(FrameCommitError::new(
                FrameCommitFailure::Policy(crate::CommitErrorKind::OwnerPoisoned),
                permit,
                frame,
            ));
        }
        let measure = FrameMeasure::capture(&frame);
        if let Some(kind) = self
            .machine
            .commit_failure(&permit, measure.retained_bytes())
        {
            return Err(FrameCommitError::new(
                FrameCommitFailure::Policy(kind),
                permit,
                frame,
            ));
        }
        if let Err(error) = self.writes.admit(
            permit.epoch,
            permit.operation,
            permit.effect,
            measure,
            frame,
        ) {
            return Err(FrameCommitError::new(
                FrameCommitFailure::Writer(error.failure()),
                permit,
                error.into_frame(),
            ));
        }
        Ok(self.machine.commit_permit(permit, measure.retained_bytes()))
    }

    /// Borrows at most `maximum` bytes from the exact FIFO write front.
    pub fn front_write(
        &self,
        maximum: NonZeroUsize,
    ) -> Result<Option<WriteSlice<'_>>, crate::ConnectionCoreError> {
        if let Some(invariant) = self.poisoned.get() {
            return Err(crate::ConnectionCoreError::Poisoned(invariant));
        }
        if self.machine.ledger.borrow().poisoned() {
            let invariant = ConnectionCoreInvariant::ReservationAccounting;
            self.poisoned.set(Some(invariant));
            return Err(crate::ConnectionCoreError::Invariant(invariant));
        }
        self.writes.front(maximum).map_err(|violation| {
            let invariant = ConnectionCoreInvariant::FrameContractViolation(violation);
            if self.poisoned.get().is_none() {
                self.poisoned.set(Some(invariant));
            }
            crate::ConnectionCoreError::Invariant(invariant)
        })
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

    /// Returns complete frames still awaiting transport completion.
    pub fn queued_write_frames(&self) -> usize {
        self.writes.queued_frames()
    }

    /// Returns whether the FIFO front can complete without transport progress.
    pub fn front_write_is_empty(&self) -> bool {
        self.writes.front_is_empty()
    }

    /// Returns the internal write identity retained for an accepted operation.
    pub fn write_effect(&self, operation: OperationId) -> Option<EffectId> {
        self.writes.effect_for(operation)
    }

    /// Returns bytes retained by complete outbound frames.
    pub const fn buffered_write_retained_bytes(&self) -> RetainedBytes {
        self.writes.retained_bytes()
    }
}
