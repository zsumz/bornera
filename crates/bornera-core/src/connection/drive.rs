//! Deterministic application of commands, timers, and transport observations.

use crate::{
    AdmissionGate, CompletionMode, ConnectionEffect, ConnectionInput, ConnectionMachine,
    ConnectionPhase, ConnectionTransition, InputDisposition, OperationOutcome, OperationPhase,
};

impl ConnectionMachine {
    /// Applies one data-only input without acquiring time or I/O capabilities.
    pub(crate) fn apply(&mut self, input: ConnectionInput) -> ConnectionTransition {
        match input {
            ConnectionInput::OpenAdmission { epoch } => self.open_admission(epoch),
            ConnectionInput::Cancel { epoch, operation } => self.cancel(epoch, operation),
            ConnectionInput::DeadlineElapsed {
                epoch,
                operation,
                now,
            } => self.deadline_elapsed(epoch, operation, now),
            ConnectionInput::ReplyMalformed { epoch } => self.reply_malformed(epoch),
            ConnectionInput::BeginDrain { epoch } => self.begin_drain(epoch),
            ConnectionInput::CloseRequested { epoch, reason } => {
                self.close_requested(epoch, reason)
            }
            ConnectionInput::EpochClosed { epoch } => self.epoch_closed(epoch),
        }
    }

    pub(super) fn open_admission(&mut self, epoch: crate::ConnectionEpoch) -> ConnectionTransition {
        if epoch != self.epoch {
            return ConnectionTransition::new(InputDisposition::IgnoredStaleEpoch);
        }
        if self.phase == ConnectionPhase::Live && self.gate == AdmissionGate::SessionOnly {
            self.gate = AdmissionGate::Open;
            ConnectionTransition::new(InputDisposition::Applied)
        } else {
            ConnectionTransition::new(InputDisposition::IgnoredInvalidPhase)
        }
    }

    pub(super) fn write_started(
        &mut self,
        epoch: crate::ConnectionEpoch,
        operation: crate::OperationId,
        effect: crate::EffectId,
    ) -> ConnectionTransition {
        if epoch != self.epoch {
            return ConnectionTransition::new(InputDisposition::IgnoredStaleEpoch);
        }
        let Some(record) = self.matching.get_mut(operation) else {
            return ConnectionTransition::new(InputDisposition::IgnoredUnknownOperation);
        };
        if record.phase == OperationPhase::Terminal {
            return ConnectionTransition::new(InputDisposition::AlreadyTerminal);
        }
        if record.effect != effect {
            return ConnectionTransition::new(InputDisposition::IgnoredStaleEffect);
        }
        if record.phase != OperationPhase::Queued {
            return ConnectionTransition::new(InputDisposition::IgnoredInvalidPhase);
        }
        record.delivery = record.delivery.possibly_sent();
        record.phase = OperationPhase::Writing;
        ConnectionTransition::new(InputDisposition::Applied)
    }

    pub(super) fn write_completed(
        &mut self,
        epoch: crate::ConnectionEpoch,
        operation: crate::OperationId,
        effect: crate::EffectId,
    ) -> ConnectionTransition {
        if epoch != self.epoch {
            return ConnectionTransition::new(InputDisposition::IgnoredStaleEpoch);
        }
        let Some(record) = self.matching.get(operation).copied() else {
            return ConnectionTransition::new(InputDisposition::IgnoredUnknownOperation);
        };
        if record.effect != effect {
            return ConnectionTransition::new(InputDisposition::IgnoredStaleEffect);
        }
        let completes_empty_frame = record.phase == OperationPhase::Queued;
        if (!matches!(
            record.phase,
            OperationPhase::Writing | OperationPhase::Terminal
        ) && !completes_empty_frame)
            || !record.write_held
        {
            return ConnectionTransition::new(InputDisposition::IgnoredInvalidPhase);
        }
        self.ledger
            .borrow_mut()
            .release_write(record.reservation.write_retained_bytes);
        if record.completion == CompletionMode::ReplyExpected {
            let Some(record) = self.matching.get_mut(operation) else {
                return ConnectionTransition::new(InputDisposition::IgnoredUnknownOperation);
            };
            if record.phase != OperationPhase::Terminal {
                record.phase = OperationPhase::AwaitingReply;
            }
            record.write_held = false;
            return ConnectionTransition::new(InputDisposition::Applied);
        }

        let Some(completed) = self.matching.remove(operation) else {
            return ConnectionTransition::new(InputDisposition::IgnoredUnknownOperation);
        };
        let mut transition = ConnectionTransition::new(InputDisposition::Applied);
        transition.push(ConnectionEffect::CancelDeadline {
            epoch: self.epoch,
            operation,
        });
        if completed.phase != OperationPhase::Terminal {
            transition.push(ConnectionEffect::PublishOutcome {
                epoch: self.epoch,
                operation,
                outcome: OperationOutcome::WriteComplete {
                    delivery: completed.delivery,
                },
            });
        }
        self.ledger
            .borrow_mut()
            .release_operation(completed.reservation, false);
        self.finish_drain(&mut transition);
        transition
    }
}
