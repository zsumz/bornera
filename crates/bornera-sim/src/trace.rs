//! Count- and byte-bounded deterministic simulation actions.

use core::num::NonZeroUsize;

use bornera_core::{CloseReason, Moment, OperationOptions, RetainedBytes};

use crate::SimFrame;

/// Trace-local identity assigned to each successfully submitted operation.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationIndex(usize);

impl OperationIndex {
    /// Creates a trace-local operation reference.
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    /// Returns the trace-local fixed-width value.
    pub const fn get(self) -> usize {
        self.0
    }
}

/// Whether an action targets the live epoch or a deterministic stale epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EpochTarget {
    /// Use the simulator's exact current epoch.
    Current,
    /// Use a distinct epoch that must be ignored by policy.
    Stale,
}

/// One replayable external action with no hidden time or I/O acquisition.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TraceAction {
    /// Reserves and immediately commits one immutable complete frame.
    Submit {
        /// Authoritative reservation moment.
        now: Moment,
        /// Mechanical operation policy.
        options: OperationOptions,
        /// Complete simulated outbound frame.
        frame: SimFrame,
    },
    /// Reports exact write progress for the current FIFO front.
    AdvanceWrite {
        /// Bytes reported by the simulated transport.
        bytes: usize,
    },
    /// Delivers a reply carrying the selected accepted operation's match key.
    Reply {
        /// Trace-local operation whose key is placed in the reply.
        operation: OperationIndex,
        /// Opaque immutable reply frame.
        frame: SimFrame,
        /// Epoch fence used by the delivered frame.
        epoch: EpochTarget,
    },
    /// Cancels local observation of one submitted operation.
    Cancel {
        /// Trace-local submitted operation.
        operation: OperationIndex,
        /// Epoch fence used by the command.
        epoch: EpochTarget,
    },
    /// Delivers an absolute operation deadline observation.
    Deadline {
        /// Trace-local submitted operation.
        operation: OperationIndex,
        /// Authoritative current moment.
        now: Moment,
        /// Epoch fence used by the timer event.
        epoch: EpochTarget,
    },
    /// Opens regular admission after simulated session establishment.
    OpenAdmission {
        /// Epoch fence used by the command.
        epoch: EpochTarget,
    },
    /// Closes admission and begins ordered draining.
    BeginDrain {
        /// Epoch fence used by the command.
        epoch: EpochTarget,
    },
    /// Requests deterministic epoch closure.
    Close {
        /// Epoch fence used by the command.
        epoch: EpochTarget,
        /// Mechanical close cause supplied to policy.
        reason: CloseReason,
    },
    /// Confirms that the simulated physical capability is closed.
    EpochClosed {
        /// Epoch fence used by the transport observation.
        epoch: EpochTarget,
    },
    /// Consumes all remaining aggregate ownership through recovery.
    Recover,
}

impl TraceAction {
    fn retained_bytes(&self) -> RetainedBytes {
        match self {
            Self::Submit { frame, .. } | Self::Reply { frame, .. } => frame.retained(),
            Self::AdvanceWrite { .. }
            | Self::Cancel { .. }
            | Self::Deadline { .. }
            | Self::OpenAdmission { .. }
            | Self::BeginDrain { .. }
            | Self::Close { .. }
            | Self::EpochClosed { .. }
            | Self::Recover => RetainedBytes::ZERO,
        }
    }
}

/// Hard count and retained-byte limits for one replay trace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TraceLimits {
    actions: NonZeroUsize,
    retained_bytes: RetainedBytes,
}

impl TraceLimits {
    /// Creates explicit trace count and owned-byte bounds.
    pub const fn new(actions: NonZeroUsize, retained_bytes: RetainedBytes) -> Self {
        Self {
            actions,
            retained_bytes,
        }
    }
}

/// Owned deterministic action sequence admitted under fixed bounds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Trace {
    limits: TraceLimits,
    actions: Vec<TraceAction>,
    retained_bytes: RetainedBytes,
}

impl Trace {
    /// Creates an empty trace with all action storage preallocated.
    pub fn new(limits: TraceLimits) -> Self {
        Self {
            limits,
            actions: Vec::with_capacity(limits.actions.get()),
            retained_bytes: RetainedBytes::ZERO,
        }
    }

    /// Admits one action without exceeding the trace's hard limits.
    pub fn try_push(&mut self, action: TraceAction) -> Result<(), TraceAdmissionError> {
        if self.actions.len() >= self.limits.actions.get() {
            return Err(TraceAdmissionError::new(
                action,
                TraceAdmissionFailure::ActionCapacity,
            ));
        }
        let incoming = action.retained_bytes();
        let Some(retained) = self.retained_bytes.checked_add(incoming) else {
            return Err(TraceAdmissionError::new(
                action,
                TraceAdmissionFailure::ByteCapacity,
            ));
        };
        if retained > self.limits.retained_bytes {
            return Err(TraceAdmissionError::new(
                action,
                TraceAdmissionFailure::ByteCapacity,
            ));
        }
        self.actions.push(action);
        self.retained_bytes = retained;
        Ok(())
    }

    /// Borrows actions in exact replay order.
    pub fn actions(&self) -> &[TraceAction] {
        &self.actions
    }

    /// Returns bytes retained by all frame-bearing actions.
    pub const fn retained_bytes(&self) -> RetainedBytes {
        self.retained_bytes
    }
}

/// Why bounded trace admission rejected an action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TraceAdmissionFailure {
    /// The trace already owns its maximum action count.
    ActionCapacity,
    /// The trace would retain more frame bytes than configured.
    ByteCapacity,
}

/// Rejected trace action preserving exact ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceAdmissionError {
    action: TraceAction,
    failure: TraceAdmissionFailure,
}

impl TraceAdmissionError {
    const fn new(action: TraceAction, failure: TraceAdmissionFailure) -> Self {
        Self { action, failure }
    }

    /// Returns the hard limit that rejected the action.
    pub const fn failure(&self) -> TraceAdmissionFailure {
        self.failure
    }

    /// Recovers the exact rejected action.
    pub fn into_action(self) -> TraceAction {
        self.action
    }
}

impl core::fmt::Display for TraceAdmissionError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self.failure {
            TraceAdmissionFailure::ActionCapacity => "trace action capacity is exhausted",
            TraceAdmissionFailure::ByteCapacity => "trace retained-byte capacity is exhausted",
        })
    }
}

impl core::error::Error for TraceAdmissionError {}
