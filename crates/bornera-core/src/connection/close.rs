//! Ordered epoch closure and drain completion.

use crate::{
    AdmissionGate, CloseReason, ConnectionEffect, ConnectionMachine, ConnectionPhase,
    ConnectionTransition, OperationFailure, OperationPhase,
};

impl ConnectionMachine {
    pub(super) fn close_into<F>(
        &mut self,
        reason: CloseReason,
        transition: &mut ConnectionTransition<F>,
    ) {
        self.close_with_failure_into(reason, None, transition);
    }

    pub(super) fn close_with_failure_into<F>(
        &mut self,
        reason: CloseReason,
        operation_failure: Option<(crate::OperationId, OperationFailure)>,
        transition: &mut ConnectionTransition<F>,
    ) {
        if self.phase != ConnectionPhase::Live {
            return;
        }

        self.gate = AdmissionGate::Closed;
        self.phase = ConnectionPhase::Closing;
        self.close_reason = Some(reason);
        transition.push(ConnectionEffect::CloseEpoch {
            epoch: self.epoch,
            reason,
        });
        while let Some(record) = self.matching.pop_front() {
            transition.push(ConnectionEffect::CancelDeadline {
                epoch: self.epoch,
                operation: record.id,
            });
            if record.write_held {
                transition.push(ConnectionEffect::DiscardWrite {
                    effect: record.effect,
                    epoch: self.epoch,
                    operation: record.id,
                });
            }
            if record.phase != OperationPhase::Terminal {
                let failure = operation_failure
                    .filter(|(operation, _)| *operation == record.id)
                    .map_or(
                        OperationFailure::ConnectionClosed(reason),
                        |(_, failure)| failure,
                    );
                transition.push(ConnectionEffect::failed(
                    self.epoch,
                    record.id,
                    failure,
                    record.delivery,
                ));
            }
            self.ledger
                .borrow_mut()
                .release_operation(record.reservation, record.write_held);
        }
    }

    pub(super) fn finish_drain<F>(&mut self, transition: &mut ConnectionTransition<F>) {
        if self.gate == AdmissionGate::Draining && self.matching.is_empty() {
            self.close_into(CloseReason::Drained, transition);
        }
    }
}
