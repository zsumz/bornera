//! Bounded external stimuli for production-slot replay.

use core::num::NonZeroUsize;

use bornera::OwnerFailure;
use bornera_core::{CloseReason, Moment, OperationOptions, RetainedBytes};
use calandria::Retained;

use crate::{OperationIndex, SimFrame, SlotTraceAdmissionError, SlotTraceAdmissionFailure};

/// One transport, control, or protocol stimulus for the production slot.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SlotAction {
    /// Makes the nonblocking connection attempt ready to complete.
    ConnectReady,
    /// Requests regular operation admission.
    OpenAdmission,
    /// Reserves and commits one complete outbound frame.
    Submit {
        /// Mechanical operation policy.
        options: OperationOptions,
        /// Complete opaque outbound bytes.
        frame: SimFrame,
    },
    /// Makes at most this many outbound bytes writable.
    WriteReady {
        /// Exact simulated write allowance.
        bytes: usize,
    },
    /// Injects one framed reply through transport, decoder, and classifier ownership.
    Reply {
        /// Trace-local operation whose match key is encoded.
        operation: OperationIndex,
        /// Opaque reply payload.
        payload: SimFrame,
    },
    /// Cancels local observation for one accepted operation.
    Cancel {
        /// Trace-local accepted operation.
        operation: OperationIndex,
    },
    /// Closes admission and begins ordered draining.
    BeginDrain,
    /// Requests deterministic local closure.
    Close {
        /// Mechanical close reason.
        reason: CloseReason,
    },
    /// Makes the next read observe peer EOF.
    PeerClosed,
    /// Confirms physical transport release after a close request.
    SettleTransport,
    /// Runs one bounded owner quantum without another stimulus.
    Drive,
    /// Transfers every remaining owner observation and ends slot replay.
    Recover {
        /// Mechanical recovery category supplied by the host.
        reason: OwnerFailure,
    },
}

impl SlotAction {
    pub(crate) fn retained_bytes(&self) -> RetainedBytes {
        match self {
            Self::Submit { frame, .. } => Retained::retained_bytes(frame),
            Self::Reply { payload, .. } => Retained::retained_bytes(payload),
            Self::ConnectReady
            | Self::OpenAdmission
            | Self::WriteReady { .. }
            | Self::Cancel { .. }
            | Self::BeginDrain
            | Self::Close { .. }
            | Self::PeerClosed
            | Self::SettleTransport
            | Self::Drive
            | Self::Recover { .. } => RetainedBytes::ZERO,
        }
    }
}

/// Hard count and retained-byte limits for one production-slot trace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotTraceLimits {
    actions: NonZeroUsize,
    retained_bytes: RetainedBytes,
}

impl SlotTraceLimits {
    /// Creates explicit action-count and retained-byte limits.
    pub const fn new(actions: NonZeroUsize, retained_bytes: RetainedBytes) -> Self {
        Self {
            actions,
            retained_bytes,
        }
    }

    pub(crate) const fn retained_bytes(self) -> RetainedBytes {
        self.retained_bytes
    }
}

/// One bounded trace replayed through Calandria virtual time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlotTrace {
    limits: SlotTraceLimits,
    actions: Vec<TimedSlotAction>,
    retained_bytes: RetainedBytes,
}

impl SlotTrace {
    /// Creates an empty trace with action storage preallocated to its hard bound.
    pub fn new(limits: SlotTraceLimits) -> Self {
        Self {
            limits,
            actions: Vec::with_capacity(limits.actions.get()),
            retained_bytes: RetainedBytes::ZERO,
        }
    }

    /// Admits one virtual-time stimulus without exceeding trace ownership bounds.
    pub fn try_push(
        &mut self,
        at: Moment,
        action: SlotAction,
    ) -> Result<(), SlotTraceAdmissionError> {
        if self.actions.len() >= self.limits.actions.get() {
            return Err(SlotTraceAdmissionError::new(
                at,
                action,
                SlotTraceAdmissionFailure::ActionCapacity,
            ));
        }
        let incoming = action.retained_bytes();
        let Some(next) = self.retained_bytes.checked_add(incoming) else {
            return Err(SlotTraceAdmissionError::new(
                at,
                action,
                SlotTraceAdmissionFailure::ByteCapacity,
            ));
        };
        if next > self.limits.retained_bytes {
            return Err(SlotTraceAdmissionError::new(
                at,
                action,
                SlotTraceAdmissionFailure::ByteCapacity,
            ));
        }
        self.actions.push(TimedSlotAction { at, action });
        self.retained_bytes = next;
        Ok(())
    }

    /// Borrows admitted stimuli in trace insertion order.
    pub fn actions(&self) -> &[TimedSlotAction] {
        &self.actions
    }

    /// Returns all variable bytes retained by the trace.
    pub const fn retained_bytes(&self) -> RetainedBytes {
        self.retained_bytes
    }

    pub(crate) const fn limits(&self) -> SlotTraceLimits {
        self.limits
    }
}

/// One trace stimulus paired with its absolute virtual moment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimedSlotAction {
    at: Moment,
    action: SlotAction,
}

impl TimedSlotAction {
    /// Returns the absolute virtual action moment.
    pub const fn at(&self) -> Moment {
        self.at
    }

    /// Borrows the owned action.
    pub const fn action(&self) -> &SlotAction {
        &self.action
    }

    pub(crate) fn into_action(self) -> SlotAction {
        self.action
    }
}
