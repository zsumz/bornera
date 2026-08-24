//! Bounded data-only terminal publications from the connection owner.

use bornera_core::{ConnectionEpoch, OperationId, OperationOutcome};
use calandria::{Retained, RetainedBytes};

/// One terminal outcome labeled with its exact connection lifetime and operation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct EngineOutcome<F> {
    epoch: ConnectionEpoch,
    operation: OperationId,
    outcome: OperationOutcome<F>,
}

impl<F> EngineOutcome<F> {
    pub(crate) const fn new(
        epoch: ConnectionEpoch,
        operation: OperationId,
        outcome: OperationOutcome<F>,
    ) -> Self {
        Self {
            epoch,
            operation,
            outcome,
        }
    }

    /// Returns the exact socket lifetime that accepted the operation.
    pub const fn epoch(&self) -> ConnectionEpoch {
        self.epoch
    }

    /// Returns the accepted operation identity.
    pub const fn operation(&self) -> OperationId {
        self.operation
    }

    /// Borrows the mechanical terminal result.
    pub const fn outcome(&self) -> &OperationOutcome<F> {
        &self.outcome
    }

    /// Recovers the mechanical terminal result.
    pub fn into_outcome(self) -> OperationOutcome<F> {
        self.outcome
    }
}

impl<F: Retained> Retained for EngineOutcome<F> {
    fn retained_bytes(&self) -> RetainedBytes {
        self.outcome.retained_bytes()
    }
}
