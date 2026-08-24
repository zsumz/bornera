//! Aggregate ownership audit, effect reconciliation, and poison latching.

use crate::{
    ConnectionCore, ConnectionCoreError, ConnectionCoreInvariant, ConnectionEffect,
    ConnectionTransition, EffectId, InputDisposition, OperationId, WriteFrame, WriteProgressError,
};

impl<F: WriteFrame> ConnectionCore<F> {
    pub(super) fn begin_recovery_journal(&mut self) -> Result<(), ConnectionCoreError> {
        if self.journal.begin(self.machine.matching.records()) {
            Ok(())
        } else {
            self.poison(ConnectionCoreInvariant::RecoveryJournalCapacity)
        }
    }

    pub(super) fn begin_operation_recovery_journal(
        &mut self,
        operation: OperationId,
    ) -> Result<(), ConnectionCoreError> {
        let Some((wire_index, record)) = self.machine.matching.get_indexed(operation) else {
            return Ok(());
        };
        if self.journal.begin_operation(record, wire_index) {
            Ok(())
        } else {
            self.poison(ConnectionCoreInvariant::RecoveryJournalCapacity)
        }
    }

    pub(super) fn begin_write_recovery_journal(&mut self) {
        self.journal.begin_write_progress();
    }

    pub(super) fn verify_ownership(&mut self) -> Result<(), ConnectionCoreError> {
        let violation = if self.writes.accounting_is_valid() {
            self.ownership_merge_violation()
        } else {
            Some(ConnectionCoreInvariant::WriteAccounting)
        };
        match violation {
            Some(source) => self.poison(source),
            None => Ok(()),
        }
    }

    pub(super) fn verify_ownership_on_hot_path(&mut self) -> Result<(), ConnectionCoreError> {
        if cfg!(debug_assertions) {
            self.verify_ownership()
        } else {
            Ok(())
        }
    }

    fn ownership_merge_violation(&self) -> Option<ConnectionCoreInvariant> {
        let mut writes = self.writes.identities().peekable();
        for record in self.machine.matching.records() {
            if !record.write_held {
                continue;
            }
            match writes.next() {
                None => {
                    return Some(ConnectionCoreInvariant::MissingWrite {
                        operation: record.id,
                        effect: record.effect,
                    });
                }
                Some((operation, effect)) if operation != record.id => {
                    return Some(ConnectionCoreInvariant::UnexpectedWrite { operation, effect });
                }
                Some((_, actual)) if actual != record.effect => {
                    return Some(ConnectionCoreInvariant::WriteIdentityMismatch {
                        operation: record.id,
                        expected: record.effect,
                        actual,
                    });
                }
                Some(_) => {}
            }
        }
        writes.next().map(
            |(operation, effect)| ConnectionCoreInvariant::UnexpectedWrite { operation, effect },
        )
    }

    pub(super) fn reconcile<R>(
        &mut self,
        transition: ConnectionTransition<R>,
    ) -> Result<ConnectionTransition<R>, ConnectionCoreError> {
        let disposition = transition.disposition();
        let cancelled = transition.cancel_outcome();
        let mut output = ConnectionTransition::new(disposition);
        if let Some(cancelled) = cancelled {
            output = output.cancelled(cancelled);
        }
        for effect in transition.into_effects() {
            match effect {
                ConnectionEffect::DiscardWrite {
                    effect,
                    epoch,
                    operation,
                } => {
                    let discarded = match self.writes.discard(epoch, effect) {
                        Ok(discarded) => discarded,
                        Err(WriteProgressError::RetainedAccountingUnderflow { .. }) => {
                            return self.poison(ConnectionCoreInvariant::WriteAccounting);
                        }
                        Err(source) => return Err(ConnectionCoreError::Write(source)),
                    };
                    let Some(discarded) = discarded else {
                        return self
                            .poison(ConnectionCoreInvariant::MissingWrite { operation, effect });
                    };
                    if discarded.operation != operation {
                        let actual = discarded.operation;
                        let _retained = self.journal.retain_write(discarded);
                        return self.poison(ConnectionCoreInvariant::DiscardedWriteMismatch {
                            expected: operation,
                            actual,
                        });
                    }
                    if !self.journal.retain_write(discarded) {
                        return self.poison(ConnectionCoreInvariant::RecoveryJournalCapacity);
                    }
                }
                effect => output.push(effect),
            }
        }
        Ok(output)
    }

    pub(super) fn absorb_write_transition(
        &mut self,
        combined: &mut ConnectionTransition,
        transition: ConnectionTransition,
        operation: OperationId,
        effect: EffectId,
    ) -> Result<(), ConnectionCoreError> {
        if transition.disposition() != InputDisposition::Applied {
            return self.poison(ConnectionCoreInvariant::WritePolicyMismatch {
                operation,
                effect,
                disposition: transition.disposition(),
            });
        }
        let transition = self.reconcile(transition)?;
        for effect in transition.into_effects() {
            combined.push(effect);
        }
        Ok(())
    }

    pub(super) fn ensure_healthy(&mut self) -> Result<(), ConnectionCoreError> {
        if self.machine.ledger.borrow().poisoned() {
            return self.poison(ConnectionCoreInvariant::ReservationAccounting);
        }
        match self.poisoned.get() {
            Some(source) => Err(ConnectionCoreError::Poisoned(source)),
            None => Ok(()),
        }
    }

    pub(super) fn poison<T>(
        &mut self,
        source: ConnectionCoreInvariant,
    ) -> Result<T, ConnectionCoreError> {
        if self.poisoned.get().is_none() {
            self.poisoned.set(Some(source));
        }
        Err(ConnectionCoreError::Invariant(source))
    }
}
