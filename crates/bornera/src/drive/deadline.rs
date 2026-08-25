//! Bounded connect and operation deadline progression.

use bornera_core::{CloseReason, ConnectionInput, FrameDecoder};
use calandria::{Moment, Retained};

use crate::{
    ConnectionSlot, EngineError, InboundClassifier, TransportDiagnostic, TransportFailureKind,
    TransportFailurePhase,
};

impl<D, C> ConnectionSlot<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    pub(super) fn drive_deadlines(
        &mut self,
        now: Moment,
        budget: usize,
    ) -> Result<usize, EngineError> {
        let mut work = 0;
        while work < budget {
            let operation = self.timers.next_deadline();
            let drain_first = self.drain_deadline.is_some_and(|deadline| {
                deadline.is_elapsed_at(now)
                    && operation.is_none_or(|operation| deadline <= operation)
                    && (!self.is_connecting() || deadline <= self.connect_deadline)
            });
            if drain_first {
                self.record_transport_failure(TransportDiagnostic::new(
                    TransportFailurePhase::Shutdown,
                    TransportFailureKind::TimedOut,
                    std::io::ErrorKind::TimedOut,
                    None,
                ));
                self.drain_deadline = None;
                self.close_for(CloseReason::Requested)?;
                work += 1;
                break;
            }
            let connect_first = self.is_connecting()
                && self.connect_deadline.is_elapsed_at(now)
                && operation.is_none_or(|deadline| self.connect_deadline <= deadline);
            if connect_first {
                self.close_for(CloseReason::ConnectTimedOut)?;
                work += 1;
                break;
            }
            let Some(timer) = self.timers.pop_due(now) else {
                break;
            };
            let token = timer.token();
            if let Some(index) = self.deadlines.iter().position(|entry| entry.token == token) {
                self.deadlines.swap_remove(index);
            }
            let event = timer.into_value();
            let transition = self
                .core
                .apply(ConnectionInput::DeadlineElapsed {
                    epoch: event.epoch,
                    operation: event.operation,
                    now,
                })
                .map_err(EngineError::Core)?;
            self.interpret_unit(transition)?;
            work += 1;
            if self.close_request.is_some() {
                break;
            }
        }
        Ok(work)
    }

    pub(super) fn has_due_deadline(&self, now: Moment) -> bool {
        (self.is_connecting() && self.connect_deadline.is_elapsed_at(now))
            || self
                .drain_deadline
                .is_some_and(|deadline| deadline.is_elapsed_at(now))
            || self
                .timers
                .next_deadline()
                .is_some_and(|deadline| deadline.is_elapsed_at(now))
    }
}
