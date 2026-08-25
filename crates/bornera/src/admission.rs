//! Owner-local admission and atomic frame-commit ownership.

use bornera_core::{
    CommitErrorKind, FrameCommitFailure, FrameDecoder, OperationId, OperationOptions,
    OperationPermit, ReserveError,
};
use calandria::{Moment, Retained};

use crate::{ConnectionSlot, EngineCommitError, InboundClassifier, OutboundFrame, OwnerFailure};

impl<D, C> ConnectionSlot<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    /// Reserves policy and publication capacity before protocol frame preparation.
    pub fn reserve(
        &mut self,
        now: Moment,
        options: OperationOptions,
    ) -> Result<OperationPermit, ReserveError> {
        if self.state.failure().is_some() {
            return Err(ReserveError::OwnerPoisoned);
        }
        let permit = match self.core.reserve(now, options) {
            Ok(permit) => permit,
            Err(error) => {
                if error == ReserveError::OwnerPoisoned {
                    self.latch_owner_failure(OwnerFailure::Core);
                }
                return Err(error);
            }
        };
        let owned = self.core.snapshot().owned_operations;
        if self.outcomes.len().saturating_add(owned) > self.limits.operation_capacity().get() {
            drop(permit);
            return Err(ReserveError::OperationCapacity);
        }
        Ok(permit)
    }

    /// Atomically transfers a permit and complete frame to this owner.
    ///
    /// [`EngineCommitError::AcceptedOwnerFailure`] means ownership transferred and the
    /// returned operation must not be retried even though effect publication failed.
    pub fn commit(
        &mut self,
        permit: OperationPermit,
        frame: OutboundFrame,
    ) -> Result<OperationId, EngineCommitError<OutboundFrame>> {
        if let Some(reason) = self.state.failure() {
            return Err(EngineCommitError::OwnerFailed {
                reason,
                permit,
                frame,
            });
        }
        let (operation, transition) = match self.core.commit(permit, frame) {
            Ok(committed) => committed,
            Err(error)
                if matches!(
                    error.failure(),
                    FrameCommitFailure::Policy(CommitErrorKind::OwnerPoisoned)
                ) =>
            {
                let (permit, frame) = error.into_parts();
                self.latch_owner_failure(OwnerFailure::Core);
                return Err(EngineCommitError::OwnerFailed {
                    reason: OwnerFailure::Core,
                    permit,
                    frame,
                });
            }
            Err(error) => return Err(EngineCommitError::Rejected(Box::new(error))),
        };
        if let Err(source) = self.interpret_unit(transition) {
            self.latch_failure(&source);
            return Err(EngineCommitError::AcceptedOwnerFailure { operation, source });
        }
        Ok(operation)
    }
}
