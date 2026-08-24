//! Calandria virtual-time replay through Bornera's production connection slot.

use core::num::NonZeroUsize;

use bornera::{ConnectionSlot, SlotProgress};
use bornera_core::{MatchKey, Moment, OperationId, RetainedBytes};
use calandria::Retained;
use calandria_sim::{EventToken, Timeline, TimelineLimits};

use crate::{
    SimClassifier, SimDecoder, SimReply, SimTransport, SlotAction, SlotActionFailure,
    SlotObservationKind, SlotReplayError, SlotReplayReport, SlotSimulationConfig,
    SlotStepObservation, SlotTrace,
};

pub(crate) type SimSlot = ConnectionSlot<SimDecoder, SimClassifier>;

/// Stateless replay factory for production decoder, classifier, deadline, and publication logic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotSimulator {
    config: SlotSimulationConfig,
}

impl SlotSimulator {
    /// Creates a production-slot replay factory.
    pub const fn new(config: SlotSimulationConfig) -> Self {
        Self { config }
    }

    /// Replays one bounded trace from fresh state under Calandria virtual time.
    pub fn replay(&self, trace: &SlotTrace) -> Result<SlotReplayReport, SlotReplayError> {
        let capacity = trace
            .actions()
            .len()
            .checked_add(1)
            .and_then(NonZeroUsize::new)
            .ok_or(SlotReplayError::TimelineCapacityOverflow)?;
        let limits = TimelineLimits::new(capacity, trace.limits().retained_bytes());
        let mut timeline = Timeline::new(self.config.timeline(), limits);
        for (index, timed) in trace.actions().iter().cloned().enumerate() {
            timeline
                .schedule_at(
                    timed.at(),
                    ReplayEvent::Action {
                        index,
                        action: timed.into_action(),
                    },
                )
                .map_err(|error| SlotReplayError::Schedule(error.failure()))?;
        }
        let horizon = trace.actions().iter().map(crate::TimedSlotAction::at).max();
        let observation_capacity = trace
            .actions()
            .len()
            .checked_mul(2)
            .and_then(|capacity| capacity.checked_add(1))
            .ok_or(SlotReplayError::TimelineCapacityOverflow)?;
        let decoder = SimDecoder::new(self.config.limits().reply_retained_bytes());
        let slot = ConnectionSlot::new(
            self.config.slot(),
            self.config.limits(),
            decoder,
            SimClassifier,
        )
        .map_err(|_| SlotReplayError::SlotConstruction)?;
        let mut owner =
            ReplayOwner::new(slot, trace.actions().len(), observation_capacity, horizon);
        owner.sync_deadline(&mut timeline)?;

        while let Some(delivery) = timeline.pop_next() {
            let (token, event) = delivery.into_parts();
            owner.apply_event(token, event);
            owner.sync_deadline(&mut timeline)?;
        }
        Ok(owner.report())
    }
}

pub(crate) struct ReplayOwner {
    pub(crate) slot: Option<SimSlot>,
    pub(crate) transport: SimTransport,
    pub(crate) accepted: Vec<Accepted>,
    observations: Vec<SlotStepObservation>,
    deadline: Option<EventToken>,
    horizon: Option<Moment>,
}

impl ReplayOwner {
    fn new(
        slot: SimSlot,
        action_capacity: usize,
        observation_capacity: usize,
        horizon: Option<Moment>,
    ) -> Self {
        Self {
            slot: Some(slot),
            transport: SimTransport::new(),
            accepted: Vec::with_capacity(action_capacity),
            observations: Vec::with_capacity(observation_capacity),
            deadline: None,
            horizon,
        }
    }

    fn apply_event(&mut self, token: EventToken, event: ReplayEvent) {
        let at = token.at();
        let kind = match event {
            ReplayEvent::Action { index, action } => SlotObservationKind::Action {
                index,
                result: self.apply_action(at, action),
            },
            ReplayEvent::Deadline => {
                if self.deadline == Some(token) {
                    self.deadline = None;
                }
                SlotObservationKind::Deadline
            }
        };
        let (progress, drive_failure) = self.drive(at);
        let (snapshot, outcomes, events) = self.observe();
        self.observations.push(SlotStepObservation {
            at,
            kind,
            progress,
            drive_failure,
            snapshot,
            outcomes,
            events,
        });
    }

    fn drive(&mut self, now: Moment) -> (Option<SlotProgress>, Option<SlotActionFailure>) {
        let Some(slot) = self.slot.as_mut() else {
            return (None, None);
        };
        match slot.drive_quantum(now, Some(&mut self.transport)) {
            Ok(progress) => (Some(progress), None),
            Err(error) => (
                None,
                Some(SlotActionFailure::Owner(bornera::OwnerFailure::from(
                    &error,
                ))),
            ),
        }
    }

    fn observe(
        &mut self,
    ) -> (
        Option<bornera::ConnectionSlotSnapshot>,
        Vec<bornera::EngineOutcome<SimReply>>,
        Vec<bornera::ConnectionEvent>,
    ) {
        let Some(slot) = self.slot.as_mut() else {
            return (None, Vec::new(), Vec::new());
        };
        let snapshot = slot.snapshot();
        let outcomes = slot.drain_outcomes().collect();
        let events = slot.drain_events().collect();
        (Some(snapshot), outcomes, events)
    }

    fn sync_deadline(
        &mut self,
        timeline: &mut Timeline<ReplayEvent>,
    ) -> Result<(), SlotReplayError> {
        let desired = self.slot.as_ref().and_then(|slot| {
            (slot.snapshot().owner_failure.is_none())
                .then(|| slot.next_deadline())
                .flatten()
                .map(bornera_core::Deadline::moment)
                .filter(|at| self.horizon.is_some_and(|horizon| *at <= horizon))
        });
        if self.deadline.map(EventToken::at) == desired {
            return Ok(());
        }
        if let Some(token) = self.deadline.take() {
            let _cancelled = timeline.cancel(token);
        }
        if let Some(at) = desired {
            let token = timeline
                .schedule_at(at.max(timeline.now()), ReplayEvent::Deadline)
                .map_err(|error| SlotReplayError::Schedule(error.failure()))?;
            self.deadline = Some(token);
        }
        Ok(())
    }

    fn report(self) -> SlotReplayReport {
        let final_snapshot = self.slot.as_ref().map(ConnectionSlot::snapshot);
        SlotReplayReport::new(
            self.observations,
            final_snapshot,
            self.transport.outbound().to_vec(),
            self.transport.applied_policy(),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Accepted {
    pub(crate) operation: OperationId,
    pub(crate) key: MatchKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReplayEvent {
    Action { index: usize, action: SlotAction },
    Deadline,
}

impl Retained for ReplayEvent {
    fn retained_bytes(&self) -> RetainedBytes {
        match self {
            Self::Action { action, .. } => action.retained_bytes(),
            Self::Deadline => RetainedBytes::ZERO,
        }
    }
}
