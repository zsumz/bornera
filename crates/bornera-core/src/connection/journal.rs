//! Preallocated fallback ownership retained across reconciliation failure.

use core::fmt;

use crate::{Delivery, DiscardedWrite, EffectId, OperationId, OperationPhase};

use crate::operation::OperationRecord;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct JournalOperation {
    pub(super) wire_index: usize,
    pub(super) operation: OperationId,
    pub(super) effect: EffectId,
    pub(super) delivery: Delivery,
    pub(super) phase: OperationPhase,
    pub(super) write_held: bool,
}

pub(super) struct RecoveryJournal<F> {
    operations: Vec<Option<JournalOperation>>,
    operation_len: usize,
    writes: Vec<Option<DiscardedWrite<F>>>,
    write_len: usize,
    overflow_write: Option<DiscardedWrite<F>>,
    armed: bool,
}

impl<F> fmt::Debug for RecoveryJournal<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveryJournal")
            .field("operation_slots", &self.operations.len())
            .field("used_operations", &self.operation_len)
            .field("write_slots", &self.writes.len())
            .field("used_writes", &self.write_len)
            .field("armed", &self.armed)
            .finish_non_exhaustive()
    }
}

impl<F> RecoveryJournal<F> {
    pub(super) fn new(max_operations: usize, max_writes: usize) -> Self {
        Self {
            operations: std::iter::repeat_n(None, max_operations).collect(),
            operation_len: 0,
            writes: std::iter::repeat_with(|| None).take(max_writes).collect(),
            write_len: 0,
            overflow_write: None,
            armed: false,
        }
    }

    pub(super) fn begin<'a>(&mut self, records: impl Iterator<Item = &'a OperationRecord>) -> bool {
        self.clear();
        for (wire_index, record) in records.enumerate() {
            if !self.retain_operation(record, wire_index) {
                self.clear();
                return false;
            }
        }
        self.armed = true;
        true
    }

    pub(super) fn begin_write_progress(&mut self) {
        self.clear();
        self.armed = true;
    }

    pub(super) fn begin_operation(&mut self, record: &OperationRecord, wire_index: usize) -> bool {
        self.clear();
        self.armed = true;
        self.retain_operation(record, wire_index)
    }

    pub(super) fn retain_operation(&mut self, record: &OperationRecord, wire_index: usize) -> bool {
        let Some(slot) = self.operations.get_mut(self.operation_len) else {
            return false;
        };
        *slot = Some(JournalOperation {
            wire_index,
            operation: record.id,
            effect: record.effect,
            delivery: record.delivery,
            phase: record.phase,
            write_held: record.write_held,
        });
        self.operation_len += 1;
        true
    }

    pub(super) fn retain_write(&mut self, write: DiscardedWrite<F>) -> bool {
        if let Some(slot) = self.writes.get_mut(self.write_len) {
            *slot = Some(write);
            self.write_len += 1;
            return true;
        }
        if self.overflow_write.is_none() {
            self.overflow_write = Some(write);
        }
        false
    }

    pub(super) fn clear(&mut self) {
        self.operations[..self.operation_len]
            .iter_mut()
            .for_each(|slot| *slot = None);
        self.operation_len = 0;
        self.writes[..self.write_len]
            .iter_mut()
            .for_each(|slot| *slot = None);
        self.write_len = 0;
        self.overflow_write = None;
        self.armed = false;
    }

    pub(super) const fn armed(&self) -> bool {
        self.armed
    }

    pub(super) fn take_operations(&mut self) -> Vec<JournalOperation> {
        let operations = self.operations[..self.operation_len]
            .iter_mut()
            .filter_map(Option::take)
            .collect();
        self.operation_len = 0;
        operations
    }

    pub(super) fn take_writes(&mut self) -> Vec<DiscardedWrite<F>> {
        let mut writes: Vec<_> = self.writes[..self.write_len]
            .iter_mut()
            .filter_map(Option::take)
            .collect();
        self.write_len = 0;
        writes.extend(self.overflow_write.take());
        self.armed = false;
        writes
    }
}
