//! FIFO reply verification and opaque frame ownership transfer.

use crate::{
    CloseReason, ConnectionEffect, ConnectionMachine, ConnectionPhase, ConnectionTransition,
    InputDisposition, OperationFailure, OperationOutcome, OperationPhase,
};

impl ConnectionMachine {
    /// Applies one complete opaque frame after protocol-specific reply classification.
    pub(crate) fn apply_reply<F>(
        &mut self,
        reply: crate::InboundReply<F>,
    ) -> ConnectionTransition<F> {
        let crate::InboundReply {
            epoch,
            key: received,
            frame,
        } = reply;
        if epoch != self.epoch {
            return ConnectionTransition::new(InputDisposition::IgnoredStaleEpoch);
        }
        if self.phase != ConnectionPhase::Live {
            return ConnectionTransition::new(InputDisposition::IgnoredInvalidPhase);
        }
        let Some(front) = self.matching.front().copied() else {
            let mut transition = ConnectionTransition::new(InputDisposition::Fault);
            self.close_into(CloseReason::UnexpectedReply, &mut transition);
            return transition;
        };
        if !matches!(
            front.phase,
            OperationPhase::AwaitingReply | OperationPhase::Terminal
        ) || front.write_held
        {
            let mut transition = ConnectionTransition::new(InputDisposition::Fault);
            self.close_into(CloseReason::UnexpectedReply, &mut transition);
            return transition;
        }
        let expected = front.reservation.match_key;
        if received != expected {
            let reason = CloseReason::MatchKeyMismatch { expected, received };
            let failure = OperationFailure::MatchKeyMismatch { expected, received };
            let mut transition = ConnectionTransition::new(InputDisposition::Fault);
            self.close_with_failure_into(reason, Some((front.id, failure)), &mut transition);
            return transition;
        }

        let Some(completed) = self.matching.pop_front() else {
            return ConnectionTransition::new(InputDisposition::IgnoredUnknownOperation);
        };
        let mut transition = ConnectionTransition::new(InputDisposition::Applied);
        transition.push(ConnectionEffect::CancelDeadline {
            epoch: self.epoch,
            operation: completed.id,
        });
        if completed.phase != OperationPhase::Terminal {
            transition.push(ConnectionEffect::PublishOutcome {
                epoch: self.epoch,
                operation: completed.id,
                outcome: OperationOutcome::Reply(frame),
            });
        }
        self.ledger
            .borrow_mut()
            .release_operation(completed.reservation, false);
        self.finish_drain(&mut transition);
        transition
    }

    pub(super) fn reply_malformed(
        &mut self,
        epoch: crate::ConnectionEpoch,
    ) -> ConnectionTransition {
        if epoch != self.epoch {
            return ConnectionTransition::new(InputDisposition::IgnoredStaleEpoch);
        }
        if self.phase != ConnectionPhase::Live {
            return ConnectionTransition::new(InputDisposition::IgnoredInvalidPhase);
        }
        let mut transition = ConnectionTransition::new(InputDisposition::Fault);
        self.close_into(CloseReason::MalformedReply, &mut transition);
        transition
    }
}
