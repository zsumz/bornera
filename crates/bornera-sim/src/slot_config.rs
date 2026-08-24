//! Production-slot and Calandria timeline configuration for simulated replay.

use bornera::{ConnectionSlotConfig, ConnectionSlotLimits};
use calandria_sim::TimelineId;

/// Immutable construction facts shared by every production-slot replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotSimulationConfig {
    slot: ConnectionSlotConfig,
    limits: ConnectionSlotLimits,
    timeline: TimelineId,
}

impl SlotSimulationConfig {
    /// Creates a deterministic slot replay configuration.
    pub const fn new(
        slot: ConnectionSlotConfig,
        limits: ConnectionSlotLimits,
        timeline: TimelineId,
    ) -> Self {
        Self {
            slot,
            limits,
            timeline,
        }
    }

    pub(crate) const fn slot(self) -> ConnectionSlotConfig {
        self.slot
    }

    pub(crate) const fn limits(self) -> ConnectionSlotLimits {
        self.limits
    }

    pub(crate) const fn timeline(self) -> TimelineId {
        self.timeline
    }
}
