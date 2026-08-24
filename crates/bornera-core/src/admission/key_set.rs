//! Preallocated deterministic membership for active protocol match keys.

use crate::MatchKey;

#[derive(Debug)]
pub(super) struct ActiveKeySet {
    slots: Vec<Option<MatchKey>>,
    len: usize,
}

impl ActiveKeySet {
    pub(super) fn new(max_keys: usize) -> Self {
        let capacity = max_keys
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .unwrap_or(max_keys)
            .max(1);
        Self {
            slots: std::iter::repeat_n(None, capacity).collect(),
            len: 0,
        }
    }

    pub(super) const fn len(&self) -> usize {
        self.len
    }

    pub(super) fn contains(&self, key: MatchKey) -> bool {
        self.find(key).is_some()
    }

    pub(super) fn insert(&mut self, key: MatchKey) -> bool {
        let mut index = self.home(key);
        for _ in 0..self.slots.len() {
            match self.slots[index] {
                Some(existing) if existing == key => return false,
                Some(_) => index = self.next(index),
                None => {
                    self.slots[index] = Some(key);
                    self.len += 1;
                    return true;
                }
            }
        }
        false
    }

    pub(super) fn remove(&mut self, key: MatchKey) -> bool {
        let Some(index) = self.find(key) else {
            return false;
        };
        self.slots[index] = None;
        self.len -= 1;

        let mut cursor = self.next(index);
        while let Some(displaced) = self.slots[cursor].take() {
            self.len -= 1;
            if !self.insert(displaced) {
                return false;
            }
            cursor = self.next(cursor);
        }
        true
    }

    fn find(&self, key: MatchKey) -> Option<usize> {
        let mut index = self.home(key);
        for _ in 0..self.slots.len() {
            match self.slots[index] {
                Some(existing) if existing == key => return Some(index),
                Some(_) => index = self.next(index),
                None => return None,
            }
        }
        None
    }

    fn home(&self, key: MatchKey) -> usize {
        let mixed = key.get().wrapping_mul(0x9e37_79b1);
        usize::try_from(mixed).unwrap_or(0) % self.slots.len()
    }

    const fn next(&self, index: usize) -> usize {
        if index + 1 == self.slots.len() {
            0
        } else {
            index + 1
        }
    }
}
