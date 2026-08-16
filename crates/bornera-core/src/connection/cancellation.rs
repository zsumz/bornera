//! Explicit cancellation before and after transport write ownership.

use crate::{
    CancelOutcome, ConnectionEffect, ConnectionMachine, ConnectionTransition, Delivery,
    InputDisposition, OperationOutcome, OperationPhase,
};

impl ConnectionMachine {
    pub(super) fn cancel(
        &mut self,
        epoch: crate::ConnectionEpoch,
        operation: crate::OperationId,
    ) -> ConnectionTransition {
        if epoch != self.epoch {
            return ConnectionTransition::new(InputDisposition::IgnoredStaleEpoch)
                .cancelled(CancelOutcome::AlreadyTerminal);
        }
        let Some(record) = self.matching.get(operation).copied() else {
            return ConnectionTransition::new(InputDisposition::IgnoredUnknownOperation)
                .cancelled(CancelOutcome::AlreadyTerminal);
        };
        if record.phase == OperationPhase::Terminal {
            return ConnectionTransition::new(InputDisposition::AlreadyTerminal)
                .cancelled(CancelOutcome::AlreadyTerminal);
        }

        let mut transition = ConnectionTransition::new(InputDisposition::Applied);
        if record.delivery == Delivery::NotSent {
            let Some(record) = self.matching.remove(operation) else {
                return ConnectionTransition::new(InputDisposition::IgnoredUnknownOperation)
                    .cancelled(CancelOutcome::AlreadyTerminal);
            };
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
            transition.push(ConnectionEffect::PublishOutcome {
                epoch: self.epoch,
                operation,
                outcome: OperationOutcome::Cancelled {
                    delivery: Delivery::NotSent,
                },
            });
            self.ledger
                .borrow_mut()
                .release_operation(record.reservation, record.write_held);
            transition = transition.cancelled(CancelOutcome::CancelledNotSent);
            self.finish_drain(&mut transition);
            return transition;
        }

        transition.push(ConnectionEffect::PublishOutcome {
            epoch: self.epoch,
            operation,
            outcome: OperationOutcome::Cancelled {
                delivery: Delivery::PossiblySent,
            },
        });
        if let Some(record) = self.matching.get_mut(operation) {
            record.phase = OperationPhase::Terminal;
        }
        transition.cancelled(CancelOutcome::ObservationCancelled {
            delivery: Delivery::PossiblySent,
        })
    }
}
