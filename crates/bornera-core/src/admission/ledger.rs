//! Single-owner accounting behind operation permits and accepted operations.

use calandria::RetainedBytes;

use crate::{ConnectionLimits, MatchKey, ReserveError};

/// Capacity held by one permit or accepted operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Reservation {
    pub(crate) retained_bytes: RetainedBytes,
    pub(crate) write_bytes: RetainedBytes,
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
    write_bytes: RetainedBytes,
    active_keys: Vec<MatchKey>,
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
            write_bytes: RetainedBytes::ZERO,
            active_keys: Vec::new(),
            next_key: limits.match_keys().first(),
            poisoned: false,
        }
    }

    pub(crate) fn reserve(
        &mut self,
        retained_bytes: RetainedBytes,
        write_bytes: RetainedBytes,
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
        let Some(next_write) = self.write_bytes.checked_add(write_bytes) else {
            return Err(ReserveError::WriteCapacity);
        };
        if next_write > self.limits.max_write_bytes() {
            return Err(ReserveError::WriteCapacity);
        }
        let key = self.allocate_key()?;

        self.operations += 1;
        self.permits += 1;
        self.retained_bytes = next_retained;
        self.write_frames += 1;
        self.write_bytes = next_write;
        self.active_keys.push(key);

        Ok(Reservation {
            retained_bytes,
            write_bytes,
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
            if !self.active_keys.contains(&candidate) {
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
        let Some(unused) = reservation.write_bytes.checked_sub(actual) else {
            self.poisoned = true;
            return;
        };
        let Some(write_bytes) = self.write_bytes.checked_sub(unused) else {
            self.poisoned = true;
            return;
        };
        self.permits = permits;
        self.write_bytes = write_bytes;
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
        let (write_frames, write_bytes) = if write_held {
            let Some(write_frames) = self.write_frames.checked_sub(1) else {
                self.poisoned = true;
                return;
            };
            let Some(write_bytes) = self.write_bytes.checked_sub(reservation.write_bytes) else {
                self.poisoned = true;
                return;
            };
            (write_frames, write_bytes)
        } else {
            (self.write_frames, self.write_bytes)
        };
        let Some(index) = self
            .active_keys
            .iter()
            .position(|active| *active == reservation.match_key)
        else {
            self.poisoned = true;
            return;
        };

        self.operations = operations;
        self.permits = permits;
        self.retained_bytes = retained_bytes;
        self.write_frames = write_frames;
        self.write_bytes = write_bytes;
        self.active_keys.swap_remove(index);
    }

    pub(crate) fn release_write(&mut self, write_bytes: RetainedBytes) {
        let Some(write_frames) = self.write_frames.checked_sub(1) else {
            self.poisoned = true;
            return;
        };
        let Some(retained) = self.write_bytes.checked_sub(write_bytes) else {
            self.poisoned = true;
            return;
        };
        self.write_frames = write_frames;
        self.write_bytes = retained;
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

    pub(crate) const fn write_bytes(&self) -> RetainedBytes {
        self.write_bytes
    }

    pub(crate) fn active_keys(&self) -> usize {
        self.active_keys.len()
    }

    pub(crate) const fn poisoned(&self) -> bool {
        self.poisoned
    }
}
