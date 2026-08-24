//! Conservative ownership recovery after a deterministic owner can no longer be driven.

use crate::{
    ConnectionCore, ConnectionEpoch, Delivery, DiscardedWrite, OperationId, OperationPhase,
    WriteFrame,
};

use super::journal::JournalOperation;
use super::recovery_item::{recover_journal_operation, take_write, weaken};
use crate::operation::OperationRecord;

/// One nonterminal accepted operation recovered from a failed owner.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
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
#[non_exhaustive]
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

        self.recover_in_wire_order(
            journal_operations,
            &mut current_records,
            &mut writes,
            &mut operations,
            &mut ownership_diverged,
        );
        if !writes.is_empty() {
            ownership_diverged = true;
        }
        if self.machine.ledger.borrow().poisoned() {
            ownership_diverged = true;
        }
        if self.poisoned.get().is_none() {
            self.poisoned
                .set(Some(crate::ConnectionCoreInvariant::Recovered));
        }
        ConnectionRecovery {
            epoch,
            operations,
            unmatched_writes: writes,
            ownership_diverged,
        }
    }

    fn recover_in_wire_order(
        &mut self,
        journal_operations: Vec<JournalOperation>,
        current_records: &mut [Option<OperationRecord>],
        writes: &mut Vec<DiscardedWrite<F>>,
        operations: &mut Vec<RecoveredOperation<F>>,
        ownership_diverged: &mut bool,
    ) {
        let journal_ids: Vec<_> = journal_operations
            .iter()
            .map(|record| record.operation)
            .collect();
        let missing = journal_operations
            .iter()
            .filter(|journal| {
                !current_records.iter().any(|current| {
                    current
                        .as_ref()
                        .is_some_and(|current| current.id == journal.operation)
                })
            })
            .count();
        let original_len = current_records.len().saturating_add(missing);
        if journal_operations
            .iter()
            .any(|record| record.wire_index >= original_len)
        {
            *ownership_diverged = true;
        }
        let mut journal: Vec<_> = journal_operations.into_iter().map(Some).collect();

        for wire_index in 0..original_len {
            let mut recovered_journal = false;
            while let Some(index) = journal.iter().position(|record| {
                record
                    .as_ref()
                    .is_some_and(|record| record.wire_index == wire_index)
            }) {
                if recovered_journal {
                    *ownership_diverged = true;
                }
                recovered_journal = true;
                let Some(journal_record) = journal[index].take() else {
                    continue;
                };
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
                        *ownership_diverged = true;
                    }
                    self.recover_current_operation(current, writes, operations, ownership_diverged);
                } else {
                    recover_journal_operation(
                        journal_record,
                        writes,
                        operations,
                        ownership_diverged,
                    );
                }
            }
            if recovered_journal {
                continue;
            }
            let current = current_records
                .iter_mut()
                .find(|record| {
                    record
                        .as_ref()
                        .is_some_and(|record| !journal_ids.contains(&record.id))
                })
                .and_then(Option::take);
            let Some(current) = current else {
                *ownership_diverged = true;
                continue;
            };
            self.recover_current_operation(current, writes, operations, ownership_diverged);
        }

        for current in current_records.iter_mut().filter_map(Option::take) {
            *ownership_diverged = true;
            self.recover_current_operation(current, writes, operations, ownership_diverged);
        }
        for journal_record in journal.into_iter().flatten() {
            *ownership_diverged = true;
            recover_journal_operation(journal_record, writes, operations, ownership_diverged);
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
