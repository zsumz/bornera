//! Ownership transfer from an affine permit into the epoch machine.

use std::rc::Rc;

use calandria::RetainedBytes;

use crate::{
    CommitErrorKind, ConnectionEffect, ConnectionMachine, ConnectionTransition, Delivery,
    InputDisposition, OperationId, OperationPhase, operation::OperationRecord,
};

impl ConnectionMachine {
    pub(crate) fn commit_failure(
        &self,
        permit: &crate::OperationPermit,
        frame_bytes: RetainedBytes,
    ) -> Option<CommitErrorKind> {
        if !Rc::ptr_eq(&self.ledger, &permit.ledger) || permit.epoch != self.epoch {
            return Some(CommitErrorKind::ForeignPermit);
        }
        if !self.gate.admits(permit.class) {
            return Some(CommitErrorKind::AdmissionClosed);
        }
        if frame_bytes > permit.reservation.write_bytes {
            return Some(CommitErrorKind::FrameTooLarge);
        }
        None
    }

    pub(crate) fn commit_permit(
        &mut self,
        mut permit: crate::OperationPermit,
        frame_bytes: RetainedBytes,
    ) -> (OperationId, ConnectionTransition) {
        permit.commit(frame_bytes);
        let operation = permit.operation;
        let record = OperationRecord {
            id: operation,
            effect: permit.effect,
            deadline: permit.deadline,
            reservation: permit.reservation,
            phase: OperationPhase::Queued,
            delivery: Delivery::NotSent,
            write_held: true,
        };
        self.matching.push(record);

        let mut transition = ConnectionTransition::new(InputDisposition::Applied);
        transition.push(ConnectionEffect::ScheduleDeadline {
            epoch: self.epoch,
            operation,
            deadline: record.deadline,
        });
        (operation, transition)
    }
}
