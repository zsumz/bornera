//! Rejected production-slot trace stimuli with exact ownership recovery.

use bornera_core::Moment;

use crate::SlotAction;

/// Why one production-slot trace action was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SlotTraceAdmissionFailure {
    /// The trace reached its configured action count.
    ActionCapacity,
    /// The trace exceeded or overflowed its retained-byte bound.
    ByteCapacity,
}

/// Rejected trace stimulus preserving exact action ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlotTraceAdmissionError {
    at: Moment,
    action: SlotAction,
    failure: SlotTraceAdmissionFailure,
}

impl SlotTraceAdmissionError {
    pub(crate) const fn new(
        at: Moment,
        action: SlotAction,
        failure: SlotTraceAdmissionFailure,
    ) -> Self {
        Self {
            at,
            action,
            failure,
        }
    }

    /// Returns the hard bound that rejected the stimulus.
    pub const fn failure(&self) -> SlotTraceAdmissionFailure {
        self.failure
    }

    /// Recovers the absolute moment and exact rejected action.
    pub fn into_parts(self) -> (Moment, SlotAction) {
        (self.at, self.action)
    }
}

impl core::fmt::Display for SlotTraceAdmissionError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self.failure {
            SlotTraceAdmissionFailure::ActionCapacity => "slot trace action capacity is exhausted",
            SlotTraceAdmissionFailure::ByteCapacity => {
                "slot trace retained-byte capacity is exhausted"
            }
        })
    }
}

impl core::error::Error for SlotTraceAdmissionError {}
