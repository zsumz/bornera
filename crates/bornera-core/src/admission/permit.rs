//! Affine capacity permits returned before protocol frame preparation.

use core::fmt;
use std::{cell::RefCell, rc::Rc};

use calandria::Deadline;

use crate::{AdmissionClass, CompletionMode, ConnectionEpoch, EffectId, MatchKey, OperationId};

use super::{Reservation, ReservationLedger};

/// An atomic reservation that rolls back all capacity when dropped.
pub struct OperationPermit {
    pub(crate) ledger: Rc<RefCell<ReservationLedger>>,
    pub(crate) epoch: ConnectionEpoch,
    pub(crate) operation: OperationId,
    pub(crate) effect: EffectId,
    pub(crate) deadline: Deadline,
    pub(crate) class: AdmissionClass,
    pub(crate) completion: CompletionMode,
    pub(crate) reservation: Reservation,
    pub(crate) active: bool,
}

impl OperationPermit {
    /// Returns the reserved operation identity.
    pub const fn operation_id(&self) -> OperationId {
        self.operation
    }

    /// Returns the protocol-visible match key reserved for encoding.
    pub const fn match_key(&self) -> MatchKey {
        self.reservation.match_key
    }

    /// Returns the original absolute deadline.
    pub const fn deadline(&self) -> Deadline {
        self.deadline
    }

    pub(crate) fn commit(&mut self, actual: calandria::RetainedBytes) {
        self.ledger.borrow_mut().commit(self.reservation, actual);
        self.reservation.write_retained_bytes = actual;
        self.active = false;
    }
}

impl fmt::Debug for OperationPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OperationPermit")
            .field("epoch", &self.epoch)
            .field("operation", &self.operation)
            .field("effect", &self.effect)
            .field("deadline", &self.deadline)
            .field("class", &self.class)
            .field("completion", &self.completion)
            .field("match_key", &self.reservation.match_key)
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

impl Drop for OperationPermit {
    fn drop(&mut self) {
        if self.active {
            self.ledger.borrow_mut().rollback_permit(self.reservation);
            self.active = false;
        }
    }
}
