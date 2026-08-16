//! Aggregate reconciliation and fail-closed ownership errors.

use core::fmt;

use crate::{
    ConnectionCore, ConnectionEffect, ConnectionTransition, EffectId, InputDisposition,
    OperationId, WriteFrame, WriteProgressError,
};

/// Fatal disagreement between the aggregate policy and frame owners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionCoreInvariant {
    /// Policy required a frame that the aggregate writer did not own.
    MissingWrite {
        /// Accepted operation that should own the frame.
        operation: OperationId,
        /// Exact write identity expected by policy.
        effect: EffectId,
    },
    /// A discarded frame belonged to a different operation than policy named.
    DiscardedWriteMismatch {
        /// Operation named by policy.
        expected: OperationId,
        /// Operation retained by the writer.
        actual: OperationId,
    },
    /// Policy and writer disagree about one accepted operation's write identity.
    WriteIdentityMismatch {
        /// Accepted operation whose identity diverged.
        operation: OperationId,
        /// Exact effect retained by policy.
        expected: EffectId,
        /// Different effect retained by the writer.
        actual: EffectId,
    },
    /// The writer retained a frame with no matching policy ownership.
    UnexpectedWrite {
        /// Operation named by the unexpected frame.
        operation: OperationId,
        /// Effect named by the unexpected frame.
        effect: EffectId,
    },
    /// Exact writer progress could not be applied to the corresponding policy record.
    WritePolicyMismatch {
        /// Operation whose frame progressed.
        operation: OperationId,
        /// Write identity whose frame progressed.
        effect: EffectId,
        /// Policy classification that rejected the progress.
        disposition: InputDisposition,
    },
    /// Reservation accounting detected impossible release or commit state.
    ReservationAccounting,
    /// Outbound retained-byte accounting detected an impossible release.
    WriteAccounting,
    /// Preallocated recovery-journal capacity disagreed with configured ownership bounds.
    RecoveryJournalCapacity,
    /// Explicit recovery permanently consumed this fixed-epoch owner.
    Recovered,
}

/// Fatal deterministic aggregate failure requiring explicit owner recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionCoreError {
    /// Exact frame progress violated FIFO or accounting ownership.
    Write(WriteProgressError),
    /// Policy and frame ownership diverged.
    Invariant(ConnectionCoreInvariant),
    /// A prior aggregate failure poisoned this fixed epoch.
    Poisoned(ConnectionCoreInvariant),
}

impl<F: WriteFrame> ConnectionCore<F> {
    pub(super) fn begin_recovery_journal(&mut self) -> Result<(), ConnectionCoreError> {
        if self.journal.begin(self.machine.matching.records()) {
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
            self.policy_write_violation()
                .or_else(|| self.writer_policy_violation())
        } else {
            Some(ConnectionCoreInvariant::WriteAccounting)
        };
        match violation {
            Some(source) => self.poison(source),
            None => Ok(()),
        }
    }

    fn policy_write_violation(&self) -> Option<ConnectionCoreInvariant> {
        for record in self.machine.matching.records() {
            match (record.write_held, self.writes.effect_for(record.id)) {
                (true, None) => {
                    return Some(ConnectionCoreInvariant::MissingWrite {
                        operation: record.id,
                        effect: record.effect,
                    });
                }
                (true, Some(actual)) if actual != record.effect => {
                    return Some(ConnectionCoreInvariant::WriteIdentityMismatch {
                        operation: record.id,
                        expected: record.effect,
                        actual,
                    });
                }
                (false, Some(effect)) => {
                    return Some(ConnectionCoreInvariant::UnexpectedWrite {
                        operation: record.id,
                        effect,
                    });
                }
                _ => {}
            }
        }
        None
    }

    fn writer_policy_violation(&self) -> Option<ConnectionCoreInvariant> {
        self.writes
            .identities()
            .find(|(operation, effect)| {
                self.machine
                    .matching
                    .get(*operation)
                    .is_none_or(|record| !record.write_held || record.effect != *effect)
            })
            .map(
                |(operation, effect)| ConnectionCoreInvariant::UnexpectedWrite {
                    operation,
                    effect,
                },
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
        match self.poisoned {
            Some(source) => Err(ConnectionCoreError::Poisoned(source)),
            None => Ok(()),
        }
    }

    pub(super) fn poison<T>(
        &mut self,
        source: ConnectionCoreInvariant,
    ) -> Result<T, ConnectionCoreError> {
        self.poisoned.get_or_insert(source);
        Err(ConnectionCoreError::Invariant(source))
    }
}

impl fmt::Display for ConnectionCoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Write(source) => source.fmt(formatter),
            Self::Invariant(_) => formatter.write_str("connection ownership diverged"),
            Self::Poisoned(_) => formatter.write_str("connection owner is poisoned"),
        }
    }
}

impl core::error::Error for ConnectionCoreError {}
