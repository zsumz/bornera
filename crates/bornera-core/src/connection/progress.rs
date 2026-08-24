//! Aggregate application of inputs, replies, and exact write progress.

use crate::write::{WriteBoundary, WriteProgress};
use crate::{
    ConnectionCore, ConnectionCoreError, ConnectionCoreInvariant, ConnectionInput, ConnectionPhase,
    ConnectionTransition, Delivery, EffectId, InboundReply, InputDisposition, OperationPhase,
    WriteFrame,
};

impl<F: WriteFrame> ConnectionCore<F> {
    /// Applies one epoch-fenced command, timer, or transport observation.
    pub fn apply(
        &mut self,
        input: ConnectionInput,
    ) -> Result<ConnectionTransition, ConnectionCoreError> {
        self.ensure_healthy()?;
        self.verify_ownership_on_hot_path()?;
        self.prepare_input_journal(input)?;
        let transition = self.machine.apply(input);
        let result = self.reconcile(transition);
        match result {
            Ok(transition) => {
                self.ensure_healthy()?;
                self.journal.clear();
                Ok(transition)
            }
            Err(error) => Err(error),
        }
    }

    /// Applies one complete opaque reply through the same aggregate owner.
    pub fn apply_reply<R>(
        &mut self,
        reply: InboundReply<R>,
    ) -> Result<ConnectionTransition<R>, ConnectionCoreError> {
        self.ensure_healthy()?;
        self.verify_ownership_on_hot_path()?;
        self.prepare_reply_journal(&reply)?;
        let transition = self.machine.apply_reply(reply);
        let result = self.reconcile(transition);
        match result {
            Ok(transition) => {
                self.ensure_healthy()?;
                self.journal.clear();
                Ok(transition)
            }
            Err(error) => Err(error),
        }
    }

    /// Applies exact transport progress to both frame and policy ownership.
    pub fn advance_write(
        &mut self,
        epoch: crate::ConnectionEpoch,
        effect: EffectId,
        written: usize,
    ) -> Result<ConnectionTransition, ConnectionCoreError> {
        self.ensure_healthy()?;
        // The writer and policy both validate the exact FIFO operation/effect
        // below, so partial progress needs no whole-epoch ownership snapshot.
        self.begin_write_recovery_journal();
        let progress = match self.writes.advance(epoch, effect, written) {
            Ok(progress) => progress,
            Err(
                crate::WriteProgressError::RetainedAccountingUnderflow { .. }
                | crate::WriteProgressError::ProgressAccountingOverflow { .. },
            ) => {
                return self.poison(ConnectionCoreInvariant::WriteAccounting);
            }
            Err(crate::WriteProgressError::ExceedsRemaining { written, remaining }) => {
                return self
                    .poison(ConnectionCoreInvariant::WriteProgressContract { written, remaining });
            }
            Err(source) => {
                self.journal.clear();
                return Err(ConnectionCoreError::Write(source));
            }
        };
        let (operation, boundary, complete) = match progress {
            WriteProgress::Pending {
                operation,
                boundary,
                ..
            } => (operation, boundary, false),
            WriteProgress::Complete {
                operation,
                effect,
                frame,
                measure,
                boundary,
                delivery,
            } => {
                let written = measure.wire_bytes();
                if !self.journal.retain_write(crate::DiscardedWrite {
                    operation,
                    effect,
                    frame,
                    measure,
                    written,
                    delivery,
                }) {
                    return self.poison(ConnectionCoreInvariant::RecoveryJournalCapacity);
                }
                let Some((wire_index, record)) = self.machine.matching.get_indexed(operation)
                else {
                    return self
                        .poison(ConnectionCoreInvariant::UnexpectedWrite { operation, effect });
                };
                if !self.journal.retain_operation(record, wire_index) {
                    return self.poison(ConnectionCoreInvariant::RecoveryJournalCapacity);
                }
                (operation, boundary, true)
            }
        };
        let mut combined = ConnectionTransition::new(InputDisposition::Applied);
        if boundary == WriteBoundary::Crossed {
            let transition = self.machine.write_started(epoch, operation, effect);
            self.absorb_write_transition(&mut combined, transition, operation, effect)?;
        }
        if complete {
            let transition = self.machine.write_completed(epoch, operation, effect);
            self.absorb_write_transition(&mut combined, transition, operation, effect)?;
        }
        self.ensure_healthy()?;
        self.journal.clear();
        Ok(combined)
    }

    fn prepare_input_journal(&mut self, input: ConnectionInput) -> Result<(), ConnectionCoreError> {
        match input {
            ConnectionInput::CloseRequested { epoch, .. }
            | ConnectionInput::ReplyMalformed { epoch }
                if epoch == self.machine.epoch && self.machine.phase == ConnectionPhase::Live =>
            {
                self.begin_recovery_journal()
            }
            ConnectionInput::Cancel { epoch, operation }
                if epoch == self.machine.epoch
                    && self
                        .machine
                        .matching
                        .get(operation)
                        .is_some_and(|record| record.phase != OperationPhase::Terminal) =>
            {
                self.begin_operation_recovery_journal(operation)
            }
            ConnectionInput::DeadlineElapsed {
                epoch,
                operation,
                now,
            } if epoch == self.machine.epoch => {
                let Some(record) = self.machine.matching.get(operation).copied() else {
                    return Ok(());
                };
                if !record.deadline.is_elapsed_at(now) {
                    return Ok(());
                }
                if record.delivery == Delivery::PossiblySent {
                    self.begin_recovery_journal()
                } else {
                    self.begin_operation_recovery_journal(operation)
                }
            }
            _ => Ok(()),
        }
    }

    fn prepare_reply_journal<R>(
        &mut self,
        reply: &InboundReply<R>,
    ) -> Result<(), ConnectionCoreError> {
        if reply.epoch != self.machine.epoch || self.machine.phase != ConnectionPhase::Live {
            return Ok(());
        }
        let Some(front) = self.machine.matching.front().copied() else {
            return self.begin_recovery_journal();
        };
        let valid_phase = matches!(
            front.phase,
            OperationPhase::AwaitingReply | OperationPhase::Terminal
        ) && !front.write_held;
        let expected = front.reservation.match_key;
        if !valid_phase || reply.key != expected {
            self.begin_recovery_journal()
        } else {
            self.begin_operation_recovery_journal(front.id)
        }
    }
}
