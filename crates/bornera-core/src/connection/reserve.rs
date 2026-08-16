//! Atomic admission before protocol frame preparation.

use std::rc::Rc;

use calandria::Moment;

use crate::{ConnectionMachine, OperationOptions, OperationPermit, ReserveError};

impl ConnectionMachine {
    /// Atomically reserves every bounded resource needed to prepare one operation.
    pub(crate) fn reserve(
        &mut self,
        now: Moment,
        options: OperationOptions,
    ) -> Result<OperationPermit, ReserveError> {
        if !self.gate.admits(options.class()) {
            return Err(ReserveError::AdmissionClosed);
        }
        if options.deadline().is_elapsed_at(now) {
            return Err(ReserveError::DeadlineElapsed);
        }
        if !self.identities.available() {
            return Err(ReserveError::IdentityExhausted);
        }

        let reservation = self
            .ledger
            .borrow_mut()
            .reserve(options.retained(), options.write())?;
        let Some((operation, effect)) = self.identities.take() else {
            self.ledger.borrow_mut().rollback_permit(reservation);
            return Err(ReserveError::IdentityExhausted);
        };

        Ok(OperationPermit {
            ledger: Rc::clone(&self.ledger),
            epoch: self.epoch,
            operation,
            effect,
            deadline: options.deadline(),
            class: options.class(),
            reservation,
            active: true,
        })
    }
}
