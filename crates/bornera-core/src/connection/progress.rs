//! Aggregate application of inputs, replies, and exact write progress.

use crate::{
    ConnectionCore, ConnectionCoreError, ConnectionCoreInvariant, ConnectionInput,
    ConnectionTransition, EffectId, InboundReply, InputDisposition, WriteBoundary, WriteFrame,
    WriteProgress,
};

impl<F: WriteFrame> ConnectionCore<F> {
    /// Applies one epoch-fenced command, timer, or transport observation.
    pub fn apply(
        &mut self,
        input: ConnectionInput,
    ) -> Result<ConnectionTransition, ConnectionCoreError> {
        self.ensure_healthy()?;
        self.verify_ownership()?;
        self.begin_recovery_journal()?;
        let transition = self.machine.apply(input);
        let result = self.reconcile(transition);
        if result.is_ok() {
            self.journal.clear();
        }
        result
    }

    /// Applies one complete opaque reply through the same aggregate owner.
    pub fn apply_reply<R>(
        &mut self,
        reply: InboundReply<R>,
    ) -> Result<ConnectionTransition<R>, ConnectionCoreError> {
        self.ensure_healthy()?;
        self.verify_ownership()?;
        self.begin_recovery_journal()?;
        let transition = self.machine.apply_reply(reply);
        let result = self.reconcile(transition);
        if result.is_ok() {
            self.journal.clear();
        }
        result
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
            Err(crate::WriteProgressError::RetainedAccountingUnderflow { .. }) => {
                return self.poison(ConnectionCoreInvariant::WriteAccounting);
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
                boundary,
                delivery,
            } => {
                let written = frame.bytes().len();
                if !self.journal.retain_write(crate::DiscardedWrite {
                    operation,
                    effect,
                    frame,
                    written,
                    delivery,
                }) {
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
        self.journal.clear();
        Ok(combined)
    }
}
