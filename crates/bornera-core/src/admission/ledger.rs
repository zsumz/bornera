//! Single-owner accounting behind operation permits and accepted operations.

use calandria::RetainedBytes;

use crate::{ConnectionLimits, MatchKey, ReserveError};

use super::key_set::ActiveKeySet;

/// Capacity held by one permit or accepted operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Reservation {
    pub(crate) retained_bytes: RetainedBytes,
    pub(crate) write_retained_bytes: RetainedBytes,
    pub(crate) match_key: MatchKey,
}

/// Bounded resource accounting shared with affine permits on one owner thread.
#[derive(Debug)]
pub(crate) struct ReservationLedger {
    limits: ConnectionLimits,
    operations: usize,
    permits: usize,
    retained_bytes: RetainedBytes,
    write_frames: usize,
    write_retained_bytes: RetainedBytes,
    active_keys: ActiveKeySet,
    next_key: MatchKey,
    poisoned: bool,
}

impl ReservationLedger {
    pub(crate) fn new(limits: ConnectionLimits) -> Self {
        Self {
            limits,
            operations: 0,
            permits: 0,
            retained_bytes: RetainedBytes::ZERO,
            write_frames: 0,
            write_retained_bytes: RetainedBytes::ZERO,
            active_keys: ActiveKeySet::new(limits.max_operations()),
            next_key: limits.match_keys().first(),
            poisoned: false,
        }
    }

    pub(crate) fn reserve(
        &mut self,
        retained_bytes: RetainedBytes,
        write_retained_bytes: RetainedBytes,
    ) -> Result<Reservation, ReserveError> {
        if self.poisoned {
            return Err(ReserveError::OwnerPoisoned);
        }
        if self.operations == self.limits.max_operations() {
            return Err(ReserveError::OperationCapacity);
        }
        let Some(next_retained) = self.retained_bytes.checked_add(retained_bytes) else {
            return Err(ReserveError::RetainedByteCapacity);
        };
        if next_retained > self.limits.max_retained_bytes() {
            return Err(ReserveError::RetainedByteCapacity);
        }
        if self.write_frames == self.limits.max_write_frames() {
            return Err(ReserveError::WriteCapacity);
        }
        let Some(next_write) = self.write_retained_bytes.checked_add(write_retained_bytes) else {
            return Err(ReserveError::WriteCapacity);
        };
        if next_write > self.limits.max_write_retained_bytes() {
            return Err(ReserveError::WriteCapacity);
        }
        let key = self.allocate_key()?;
        if !self.active_keys.insert(key) {
            self.poisoned = true;
            return Err(ReserveError::OwnerPoisoned);
        }

        self.operations += 1;
        self.permits += 1;
        self.retained_bytes = next_retained;
        self.write_frames += 1;
        self.write_retained_bytes = next_write;
        Ok(Reservation {
            retained_bytes,
            write_retained_bytes,
            match_key: key,
        })
    }

    fn allocate_key(&mut self) -> Result<MatchKey, ReserveError> {
        let Some(attempts) = self.active_keys.len().checked_add(1) else {
            self.poisoned = true;
            return Err(ReserveError::OwnerPoisoned);
        };
        let mut candidate = self.next_key;
        for _ in 0..attempts {
            if !self.active_keys.contains(candidate) {
                self.next_key = self.limits.match_keys().next(candidate);
                return Ok(candidate);
            }
            candidate = self.limits.match_keys().next(candidate);
        }
        Err(ReserveError::MatchKeyExhausted)
    }

    pub(crate) fn rollback_permit(&mut self, reservation: Reservation) {
        self.release_all(reservation, true, true);
    }

    pub(crate) fn commit(&mut self, reservation: Reservation, actual: RetainedBytes) {
        let Some(permits) = self.permits.checked_sub(1) else {
            self.poisoned = true;
            return;
        };
        let Some(unused) = reservation.write_retained_bytes.checked_sub(actual) else {
            self.poisoned = true;
            return;
        };
        let Some(write_retained_bytes) = self.write_retained_bytes.checked_sub(unused) else {
            self.poisoned = true;
            return;
        };
        self.permits = permits;
        self.write_retained_bytes = write_retained_bytes;
    }

    pub(crate) fn release_operation(&mut self, reservation: Reservation, write_held: bool) {
        self.release_all(reservation, write_held, false);
    }

    fn release_all(&mut self, reservation: Reservation, write_held: bool, release_permit: bool) {
        let Some(operations) = self.operations.checked_sub(1) else {
            self.poisoned = true;
            return;
        };
        let Some(retained_bytes) = self.retained_bytes.checked_sub(reservation.retained_bytes)
        else {
            self.poisoned = true;
            return;
        };
        let permits = if release_permit {
            let Some(permits) = self.permits.checked_sub(1) else {
                self.poisoned = true;
                return;
            };
            permits
        } else {
            self.permits
        };
        let (write_frames, write_retained_bytes) = if write_held {
            let Some(write_frames) = self.write_frames.checked_sub(1) else {
                self.poisoned = true;
                return;
            };
            let Some(write_retained_bytes) = self
                .write_retained_bytes
                .checked_sub(reservation.write_retained_bytes)
            else {
                self.poisoned = true;
                return;
            };
            (write_frames, write_retained_bytes)
        } else {
            (self.write_frames, self.write_retained_bytes)
        };
        if !self.active_keys.remove(reservation.match_key) {
            self.poisoned = true;
            return;
        }

        self.operations = operations;
        self.permits = permits;
        self.retained_bytes = retained_bytes;
        self.write_frames = write_frames;
        self.write_retained_bytes = write_retained_bytes;
    }

    pub(crate) fn release_write(&mut self, write_retained_bytes: RetainedBytes) {
        let Some(write_frames) = self.write_frames.checked_sub(1) else {
            self.poisoned = true;
            return;
        };
        let Some(retained) = self.write_retained_bytes.checked_sub(write_retained_bytes) else {
            self.poisoned = true;
            return;
        };
        self.write_frames = write_frames;
        self.write_retained_bytes = retained;
    }

    pub(crate) const fn operations(&self) -> usize {
        self.operations
    }

    pub(crate) const fn permits(&self) -> usize {
        self.permits
    }

    pub(crate) const fn retained_bytes(&self) -> RetainedBytes {
        self.retained_bytes
    }

    pub(crate) const fn write_frames(&self) -> usize {
        self.write_frames
    }

    pub(crate) const fn write_retained_bytes(&self) -> RetainedBytes {
        self.write_retained_bytes
    }

    pub(crate) fn active_keys(&self) -> usize {
        self.active_keys.len()
    }

    pub(crate) const fn poisoned(&self) -> bool {
        self.poisoned
    }
}
