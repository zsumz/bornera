//! Conservative ownership recovery after a deterministic owner can no longer be driven.

use crate::{
    ConnectionCore, ConnectionEpoch, Delivery, DiscardedWrite, EffectId, OperationId,
    OperationPhase, WriteFrame,
};

use super::journal::JournalOperation;
use crate::operation::OperationRecord;

/// One nonterminal accepted operation recovered from a failed owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredOperation<F> {
    /// Accepted operation identity.
    pub operation: OperationId,
    /// Conservative delivery certainty at recovery.
    pub delivery: Delivery,
    /// Exact complete frame when write ownership had not released it.
    pub frame: Option<F>,
}

/// Bounded recovery contents for one fixed connection epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionRecovery<F> {
    /// Exact epoch that permanently owned the operations.
    pub epoch: ConnectionEpoch,
    /// Nonterminal operations in original wire order.
    pub operations: Vec<RecoveredOperation<F>>,
    /// Writer-owned frames that had no exact policy record.
    pub unmatched_writes: Vec<DiscardedWrite<F>>,
    /// Policy records and frame ownership disagreed during recovery.
    pub ownership_diverged: bool,
}

impl<F: WriteFrame> ConnectionCore<F> {
    /// Extracts every nonterminal operation and makes this aggregate unusable.
    pub fn recover(&mut self) -> ConnectionRecovery<F> {
        let epoch = self.machine.epoch;
        let journal_armed = self.journal.armed();
        let mut writes = self.writes.discard_all().into_writes();
        writes.extend(self.journal.take_writes());
        let journal_operations = self.journal.take_operations();
        let mut current_records = Vec::with_capacity(self.machine.matching.pending_operations());
        while let Some(record) = self.machine.matching.pop_front() {
            current_records.push(Some(record));
        }
        let capacity = current_records
            .len()
            .saturating_add(journal_operations.len());
        let mut operations = Vec::with_capacity(capacity);
        let mut ownership_diverged = journal_armed || !journal_operations.is_empty();

        if journal_armed {
            for journal_record in journal_operations {
                let current = current_records
                    .iter_mut()
                    .find(|record| {
                        record
                            .as_ref()
                            .is_some_and(|record| record.id == journal_record.operation)
                    })
                    .and_then(Option::take);
                if let Some(current) = current {
                    if current.effect != journal_record.effect {
                        ownership_diverged = true;
                    }
                    self.recover_current_operation(
                        current,
                        &mut writes,
                        &mut operations,
                        &mut ownership_diverged,
                    );
                } else {
                    recover_journal_operation(
                        journal_record,
                        &mut writes,
                        &mut operations,
                        &mut ownership_diverged,
                    );
                }
            }
        }
        for record in current_records.into_iter().flatten() {
            self.recover_current_operation(
                record,
                &mut writes,
                &mut operations,
                &mut ownership_diverged,
            );
        }
        if !writes.is_empty() {
            ownership_diverged = true;
        }
        if self.machine.ledger.borrow().poisoned() {
            ownership_diverged = true;
        }
        self.poisoned
            .get_or_insert(crate::ConnectionCoreInvariant::Recovered);
        ConnectionRecovery {
            epoch,
            operations,
            unmatched_writes: writes,
            ownership_diverged,
        }
    }

    fn recover_current_operation(
        &mut self,
        record: OperationRecord,
        writes: &mut Vec<DiscardedWrite<F>>,
        operations: &mut Vec<RecoveredOperation<F>>,
        ownership_diverged: &mut bool,
    ) {
        let write = take_write(writes, record.id, record.effect);
        if record.write_held != write.is_some() {
            *ownership_diverged = true;
        }
        let (delivery, frame) = write.map_or((record.delivery, None), |write| {
            (weaken(record.delivery, write.delivery), Some(write.frame))
        });
        if record.phase != OperationPhase::Terminal {
            operations.push(RecoveredOperation {
                operation: record.id,
                delivery,
                frame,
            });
        }
        self.machine
            .ledger
            .borrow_mut()
            .release_operation(record.reservation, record.write_held);
    }
}

fn recover_journal_operation<F>(
    record: JournalOperation,
    writes: &mut Vec<crate::DiscardedWrite<F>>,
    operations: &mut Vec<RecoveredOperation<F>>,
    ownership_diverged: &mut bool,
) {
    if record.phase == OperationPhase::Terminal {
        return;
    }
    let write = take_write(writes, record.operation, record.effect);
    if record.write_held != write.is_some() {
        *ownership_diverged = true;
    }
    let (delivery, frame) = write.map_or((record.delivery, None), |write| {
        (weaken(record.delivery, write.delivery), Some(write.frame))
    });
    operations.push(RecoveredOperation {
        operation: record.operation,
        delivery,
        frame,
    });
}

fn take_write<F>(
    writes: &mut Vec<crate::DiscardedWrite<F>>,
    operation: OperationId,
    effect: EffectId,
) -> Option<crate::DiscardedWrite<F>> {
    let index = writes
        .iter()
        .position(|write| write.operation == operation && write.effect == effect)?;
    Some(writes.remove(index))
}

const fn weaken(policy: Delivery, writer: Delivery) -> Delivery {
    if matches!(policy, Delivery::PossiblySent) || matches!(writer, Delivery::PossiblySent) {
        Delivery::PossiblySent
    } else {
        Delivery::NotSent
    }
}
