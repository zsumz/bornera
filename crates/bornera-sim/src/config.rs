//! Fixed identity and ownership bounds for exact trace replay.

use bornera_core::{ConnectionEpoch, ConnectionId, ConnectionLimits, EndpointId, LaneId};

/// Immutable construction facts for every replay of one trace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimulationConfig {
    endpoint: EndpointId,
    lane: LaneId,
    connection: ConnectionId,
    epoch: ConnectionEpoch,
    limits: ConnectionLimits,
}

impl SimulationConfig {
    /// Creates a deterministic fixed-epoch simulation owner.
    pub const fn new(
        endpoint: EndpointId,
        lane: LaneId,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        limits: ConnectionLimits,
    ) -> Self {
        Self {
            endpoint,
            lane,
            connection,
            epoch,
            limits,
        }
    }

    pub(crate) const fn endpoint(self) -> EndpointId {
        self.endpoint
    }

    pub(crate) const fn lane(self) -> LaneId {
        self.lane
    }

    pub(crate) const fn connection(self) -> ConnectionId {
        self.connection
    }

    pub(crate) const fn epoch(self) -> ConnectionEpoch {
        self.epoch
    }

    pub(crate) const fn limits(self) -> ConnectionLimits {
        self.limits
    }
}
