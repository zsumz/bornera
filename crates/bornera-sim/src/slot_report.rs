//! Exact observations from replay through the production connection slot.

use bornera::{
    ConnectionEvent, ConnectionSlotSnapshot, EngineOutcome, OutboundFrame, OwnerFailure,
    RecoveryReport, SlotProgress, TcpSocketPolicy,
};
use bornera_core::{
    CancelOutcome, FrameCommitFailure, InputDisposition, MatchKey, Moment, OperationId,
    ReserveError,
};
use calandria_sim::ScheduleFailure;

use crate::{OperationIndex, SimReply};

/// Result of one externally supplied production-slot action.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SlotActionResult {
    /// Connect readiness was injected.
    ConnectReady,
    /// Admission opening returned this policy disposition.
    Admission(InputDisposition),
    /// One operation was accepted under a trace-local identity.
    Submitted {
        /// Stable reference used by later trace actions.
        index: OperationIndex,
        /// Slot-assigned operation identity.
        operation: OperationId,
        /// Protocol match key encoded into simulated replies.
        match_key: MatchKey,
    },
    /// Commit accepted the operation before slot interpretation failed closed.
    SubmittedOwnerFailed {
        /// Stable reference retained for later recovery-oriented actions.
        index: OperationIndex,
        /// Accepted operation identity reported by the slot.
        operation: OperationId,
        /// Protocol match key reserved for the accepted operation.
        match_key: MatchKey,
        /// Fatal owner category observed after acceptance.
        reason: OwnerFailure,
    },
    /// Write readiness was injected with this exact byte allowance.
    WriteReady(usize),
    /// One framed reply was injected for this accepted operation.
    ReplyInjected(OperationIndex),
    /// Cancellation returned this mechanical observation state.
    Cancelled(CancelOutcome),
    /// Ordered draining returned this policy disposition.
    Drain(InputDisposition),
    /// Local closure returned this policy disposition.
    Close(InputDisposition),
    /// Peer EOF readiness was injected.
    PeerClosed,
    /// Physical close settlement found or did not find a pending directive.
    TransportSettled(bool),
    /// One explicit bounded drive was requested.
    Driven,
    /// The action was rejected without losing accepted operation ownership.
    Rejected(SlotActionFailure),
    /// All remaining slot ownership transferred and replay stopped.
    Recovered,
}

/// Stable classification of an action failure without retaining backend errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SlotActionFailure {
    /// The trace-local reference does not name an accepted operation.
    UnknownOperation(OperationIndex),
    /// Reservation was rejected before operation acceptance.
    Reserve(ReserveError),
    /// Commit preserved the permit and frame under this rejection.
    Commit(FrameCommitFailure),
    /// Slot driving or interpretation failed closed under this owner category.
    Owner(OwnerFailure),
    /// Simulated reply encoding exceeded its fixed-width bound.
    ReplyEncoding,
    /// Complete outbound bytes could not enter the production frame type.
    FrameEncoding,
    /// Simulated reply input exceeded the fixture's fixed preallocated capacity.
    SimulatedInputCapacity,
    /// A prior recovery permanently ended this replay owner.
    SlotRecovered,
}

/// Whether an observation came from a trace action or a scheduled slot deadline.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SlotObservationKind {
    /// One trace action in insertion order.
    Action {
        /// Zero-based trace insertion position.
        index: usize,
        /// Mechanical action result.
        result: SlotActionResult,
    },
    /// Calandria virtual time reached the slot's earliest deadline.
    Deadline,
}

/// Complete post-step production orchestration observation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SlotStepObservation {
    /// Authoritative Calandria virtual moment.
    pub at: Moment,
    /// External or internal stimulus that produced the step.
    pub kind: SlotObservationKind,
    /// Bounded slot work completed after the stimulus.
    pub progress: Option<SlotProgress>,
    /// Fatal owner category returned while driving after the stimulus.
    pub drive_failure: Option<SlotActionFailure>,
    /// Post-step slot state, absent after recovery consumed it.
    pub snapshot: Option<ConnectionSlotSnapshot>,
    /// Ownership transferred by this step, present only for `Recovered`.
    pub recovery: Option<RecoveryReport<OutboundFrame, SimReply>>,
    /// Terminal outcomes drained after this step.
    pub outcomes: Vec<EngineOutcome<SimReply>>,
    /// Lifecycle events drained after this step.
    pub events: Vec<ConnectionEvent>,
}

/// Complete exact replay report for one production slot.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SlotReplayReport {
    observations: Vec<SlotStepObservation>,
    final_snapshot: Option<ConnectionSlotSnapshot>,
    outbound: Vec<u8>,
    applied_policy: Option<TcpSocketPolicy>,
}

impl SlotReplayReport {
    pub(crate) const fn new(
        observations: Vec<SlotStepObservation>,
        final_snapshot: Option<&ConnectionSlotSnapshot>,
        outbound: Vec<u8>,
        applied_policy: Option<TcpSocketPolicy>,
    ) -> Self {
        Self {
            observations,
            final_snapshot: final_snapshot.copied(),
            outbound,
            applied_policy,
        }
    }

    /// Borrows observations in deterministic virtual-time order.
    pub fn observations(&self) -> &[SlotStepObservation] {
        &self.observations
    }

    /// Returns final slot state, absent when recovery consumed the owner.
    pub const fn final_snapshot(&self) -> Option<ConnectionSlotSnapshot> {
        self.final_snapshot
    }

    /// Borrows all bytes accepted by the simulated transport in wire order.
    pub fn outbound_bytes(&self) -> &[u8] {
        &self.outbound
    }

    /// Returns the exact socket policy applied after simulated establishment.
    pub const fn applied_policy(&self) -> Option<TcpSocketPolicy> {
        self.applied_policy
    }
}

/// Why a production-slot trace could not be constructed or scheduled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SlotReplayError {
    /// Adding one internal deadline slot overflowed the event-count domain.
    TimelineCapacityOverflow,
    /// Calandria rejected a trace or internal deadline event.
    Schedule(ScheduleFailure),
    /// The production decoder began outside its retained-memory contract.
    SlotConstruction,
    /// The simulated transport could not preallocate within its configured bound.
    TransportConstruction,
}

impl core::fmt::Display for SlotReplayError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::TimelineCapacityOverflow => {
                formatter.write_str("slot simulation timeline capacity overflowed")
            }
            Self::Schedule(failure) => failure.fmt(formatter),
            Self::SlotConstruction => {
                formatter.write_str("production slot simulation construction failed")
            }
            Self::TransportConstruction => {
                formatter.write_str("simulated transport construction failed")
            }
        }
    }
}

impl core::error::Error for SlotReplayError {}
