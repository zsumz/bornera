//! Absolute-deadline handling without relative-time restart.

use crate::{
    CloseReason, ConnectionEffect, ConnectionMachine, ConnectionTransition, Delivery,
    InputDisposition, OperationFailure,
};

impl ConnectionMachine {
    pub(super) fn deadline_elapsed(
        &mut self,
        epoch: crate::ConnectionEpoch,
        operation: crate::OperationId,
        now: crate::Moment,
    ) -> ConnectionTransition {
        if epoch != self.epoch {
            return ConnectionTransition::new(InputDisposition::IgnoredStaleEpoch);
        }
        let Some(record) = self.matching.get(operation).copied() else {
            return ConnectionTransition::new(InputDisposition::IgnoredUnknownOperation);
        };
        if !record.deadline.is_elapsed_at(now) {
            let mut transition = ConnectionTransition::new(InputDisposition::Applied);
            transition.push(ConnectionEffect::ScheduleDeadline {
                epoch: self.epoch,
                operation,
                deadline: record.deadline,
            });
            return transition;
        }
        if record.delivery == Delivery::PossiblySent {
            let mut transition = ConnectionTransition::new(InputDisposition::Applied);
            self.close_into(CloseReason::DeadlineAfterPossibleSend, &mut transition);
            return transition;
        }

        let Some(record) = self.matching.remove(operation) else {
            return ConnectionTransition::new(InputDisposition::IgnoredUnknownOperation);
        };

        let mut transition = ConnectionTransition::new(InputDisposition::Applied);
        if record.write_held {
            transition.push(ConnectionEffect::DiscardWrite {
                effect: record.effect,
                epoch: self.epoch,
                operation,
            });
        }
        transition.push(ConnectionEffect::CancelDeadline {
            epoch: self.epoch,
            operation,
        });
        transition.push(ConnectionEffect::failed(
            self.epoch,
            operation,
            OperationFailure::DeadlineElapsed,
            Delivery::NotSent,
        ));
        self.ledger
            .borrow_mut()
            .release_operation(record.reservation, record.write_held);
        self.finish_drain(&mut transition);
        transition
    }
}
