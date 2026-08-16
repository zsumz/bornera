//! Bounded FIFO ownership for replies carrying verifiable match keys.

use std::collections::VecDeque;

use crate::{MatchKey, MatchKeySpace, OperationId, operation::OperationRecord};

/// FIFO matching state for replies whose key must equal the wire-order front.
#[derive(Debug)]
pub struct OrderedVerified {
    key_space: MatchKeySpace,
    capacity: usize,
    pending: VecDeque<OperationRecord>,
}

impl OrderedVerified {
    pub(crate) fn new(key_space: MatchKeySpace, capacity: usize) -> Self {
        Self {
            key_space,
            capacity,
            pending: VecDeque::new(),
        }
    }

    /// Returns the inclusive key space used for reserved operations.
    pub const fn key_space(&self) -> MatchKeySpace {
        self.key_space
    }

    /// Returns the maximum pending operations in this discipline.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns operations currently retaining wire-order ownership.
    pub fn pending_operations(&self) -> usize {
        self.pending.len()
    }

    /// Returns the match key at the wire-order front.
    pub fn front_match_key(&self) -> Option<MatchKey> {
        self.pending
            .front()
            .map(|record| record.reservation.match_key)
    }

    pub(crate) fn push(&mut self, record: OperationRecord) {
        // The originating permit holds one slot from this same capacity.
        self.pending.push_back(record);
    }

    pub(crate) fn get(&self, id: OperationId) -> Option<&OperationRecord> {
        self.pending.iter().find(|record| record.id == id)
    }

    pub(crate) fn get_mut(&mut self, id: OperationId) -> Option<&mut OperationRecord> {
        self.pending.iter_mut().find(|record| record.id == id)
    }

    pub(crate) fn front(&self) -> Option<&OperationRecord> {
        self.pending.front()
    }

    pub(crate) fn records(&self) -> impl Iterator<Item = &OperationRecord> {
        self.pending.iter()
    }

    pub(crate) fn pop_front(&mut self) -> Option<OperationRecord> {
        self.pending.pop_front()
    }

    pub(crate) fn remove(&mut self, id: OperationId) -> Option<OperationRecord> {
        let index = self.pending.iter().position(|record| record.id == id)?;
        self.pending.remove(index)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(crate) fn terminal_len(&self) -> usize {
        self.pending
            .iter()
            .filter(|record| record.phase == crate::OperationPhase::Terminal)
            .count()
    }

    pub(crate) fn active_len(&self) -> usize {
        self.pending
            .iter()
            .filter(|record| record.phase != crate::OperationPhase::Terminal)
            .count()
    }
}
