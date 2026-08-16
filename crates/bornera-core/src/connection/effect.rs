//! Data-only effects emitted by the deterministic connection machine.

use calandria::Deadline;

use crate::{
    ConnectionEpoch, Delivery, EffectId, MatchKey, OperationFailure, OperationId, OperationOutcome,
};

/// Why an epoch must close.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseReason {
    /// Accepted work drained after admission closed.
    Drained,
    /// The owner explicitly requested closure.
    Requested,
    /// A possibly-sent operation reached its deadline.
    DeadlineAfterPossibleSend,
    /// The transport reported loss of the connection.
    TransportLost,
    /// A reply arrived when no operation could own it.
    UnexpectedReply,
    /// The adapter rejected a complete inbound frame as malformed.
    MalformedReply,
    /// A complete inbound frame exceeded its configured retained-byte bound.
    InboundRetainedCapacity,
    /// A reply key did not equal the wire-order front.
    MatchKeyMismatch {
        /// Match key required by the FIFO front.
        expected: MatchKey,
        /// Match key carried by the received reply.
        received: MatchKey,
    },
}

/// A capability action or terminal publication requested by core policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectionEffect<F = ()> {
    /// Schedule the original absolute deadline.
    ScheduleDeadline {
        /// Owning epoch.
        epoch: ConnectionEpoch,
        /// Owning operation.
        operation: OperationId,
        /// Absolute, never-restarted deadline.
        deadline: Deadline,
    },
    /// Remove a frame that never entered transport write ownership.
    DiscardWrite {
        /// Original write effect.
        effect: EffectId,
        /// Owning epoch.
        epoch: ConnectionEpoch,
        /// Owning operation.
        operation: OperationId,
    },
    /// Remove the deadline owned by an operation.
    CancelDeadline {
        /// Owning epoch.
        epoch: ConnectionEpoch,
        /// Owning operation.
        operation: OperationId,
    },
    /// Close the physical capability for this exact epoch.
    CloseEpoch {
        /// Epoch that must close.
        epoch: ConnectionEpoch,
        /// Mechanical closure reason.
        reason: CloseReason,
    },
    /// Publish one terminal operation outcome outside the owner path.
    PublishOutcome {
        /// Owning epoch.
        epoch: ConnectionEpoch,
        /// Terminal operation.
        operation: OperationId,
        /// Mechanical terminal outcome. Replies enter with matching in Milestone 3.
        outcome: OperationOutcome<F>,
    },
}

impl<F> ConnectionEffect<F> {
    pub(crate) fn failed(
        epoch: ConnectionEpoch,
        operation: OperationId,
        failure: OperationFailure,
        delivery: Delivery,
    ) -> Self {
        Self::PublishOutcome {
            epoch,
            operation,
            outcome: OperationOutcome::Failed { failure, delivery },
        }
    }
}
