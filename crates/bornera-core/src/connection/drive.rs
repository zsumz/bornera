//! Deterministic application of commands, timers, and transport observations.

use crate::{
    AdmissionGate, ConnectionInput, ConnectionMachine, ConnectionPhase, ConnectionTransition,
    InputDisposition, OperationPhase,
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
        let Some(record) = self.matching.get_mut(operation) else {
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
        if record.phase != OperationPhase::Terminal {
            record.phase = OperationPhase::AwaitingReply;
        }
        self.ledger
            .borrow_mut()
            .release_write(record.reservation.write_bytes);
        record.write_held = false;
        ConnectionTransition::new(InputDisposition::Applied)
    }
}
