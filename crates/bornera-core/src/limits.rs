//! Count, byte, and match-key limits for one connection epoch.

use core::fmt;
use core::num::NonZeroUsize;

use calandria::RetainedBytes;

use crate::{MatchKey, WriteQueueLimits};

/// An inclusive range of protocol-visible match keys.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MatchKeySpace {
    first: MatchKey,
    last: MatchKey,
}

impl MatchKeySpace {
    /// Creates a nonempty inclusive match-key range.
    pub const fn new(first: u32, last: u32) -> Result<Self, LimitsError> {
        if first > last {
            return Err(LimitsError::EmptyMatchKeySpace);
        }
        Ok(Self {
            first: MatchKey::new(first),
            last: MatchKey::new(last),
        })
    }

    /// Returns the first key in the range.
    pub const fn first(self) -> MatchKey {
        self.first
    }

    /// Returns the last key in the range.
    pub const fn last(self) -> MatchKey {
        self.last
    }

    /// Returns the number of keys in the range.
    pub fn capacity(self) -> u64 {
        u64::from(self.last.get()) - u64::from(self.first.get()) + 1
    }

    pub(crate) const fn next(self, current: MatchKey) -> MatchKey {
        if current.get() == self.last.get() {
            self.first
        } else {
            MatchKey::new(current.get() + 1)
        }
    }
}

/// Bounded resources owned by one connection epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionLimits {
    max_operations: NonZeroUsize,
    max_retained_bytes: RetainedBytes,
    max_write_frames: NonZeroUsize,
    max_write_bytes: RetainedBytes,
    match_keys: MatchKeySpace,
}

impl ConnectionLimits {
    /// Creates limits with nonzero count capacities.
    pub const fn new(
        max_operations: usize,
        max_retained_bytes: RetainedBytes,
        max_write_frames: usize,
        max_write_bytes: RetainedBytes,
        match_keys: MatchKeySpace,
    ) -> Result<Self, LimitsError> {
        let Some(max_operations) = NonZeroUsize::new(max_operations) else {
            return Err(LimitsError::ZeroOperationCapacity);
        };
        let Some(max_write_frames) = NonZeroUsize::new(max_write_frames) else {
            return Err(LimitsError::ZeroWriteCapacity);
        };
        Ok(Self {
            max_operations,
            max_retained_bytes,
            max_write_frames,
            max_write_bytes,
            match_keys,
        })
    }

    /// Returns the maximum accepted and reserved operations.
    pub const fn max_operations(self) -> usize {
        self.max_operations.get()
    }

    /// Returns the maximum semantic retained bytes.
    pub const fn max_retained_bytes(self) -> RetainedBytes {
        self.max_retained_bytes
    }

    /// Returns the maximum queued and reserved write frames.
    pub const fn max_write_frames(self) -> usize {
        self.max_write_frames.get()
    }

    /// Returns the maximum queued and reserved write bytes.
    pub const fn max_write_bytes(self) -> RetainedBytes {
        self.max_write_bytes
    }

    /// Returns the match-key space.
    pub const fn match_keys(self) -> MatchKeySpace {
        self.match_keys
    }

    /// Returns the matching bounded-writer limits for this reservation ledger.
    pub const fn write_queue_limits(self) -> WriteQueueLimits {
        WriteQueueLimits::new(self.max_write_frames, self.max_write_bytes)
    }
}

/// Invalid connection-limit configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LimitsError {
    /// No operation can be admitted.
    ZeroOperationCapacity,
    /// No write frame can be reserved.
    ZeroWriteCapacity,
    /// The inclusive match-key range is reversed.
    EmptyMatchKeySpace,
}

impl fmt::Display for LimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroOperationCapacity => "operation capacity must be nonzero",
            Self::ZeroWriteCapacity => "write-frame capacity must be nonzero",
            Self::EmptyMatchKeySpace => "match-key range must be nonempty",
        })
    }
}

impl core::error::Error for LimitsError {}
