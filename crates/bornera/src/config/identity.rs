//! Stable identity domains and one exact connection-attempt policy.

use std::net::SocketAddr;

use bornera_core::{ConnectionEpoch, ConnectionId, EndpointId, LaneId};
use calandria::{Deadline, ResourceOwnerId, TimerOwnerId};

use crate::TcpSocketPolicy;

/// Stable logical identities attached to one exact socket lifetime.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ConnectionIdentity {
    endpoint: EndpointId,
    lane: LaneId,
    connection: ConnectionId,
    epoch: ConnectionEpoch,
}

impl ConnectionIdentity {
    /// Creates the identity tuple for one exact connection epoch.
    pub const fn new(
        endpoint: EndpointId,
        lane: LaneId,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
    ) -> Self {
        Self {
            endpoint,
            lane,
            connection,
            epoch,
        }
    }

    /// Returns the logical endpoint identity.
    pub const fn endpoint(self) -> EndpointId {
        self.endpoint
    }

    /// Returns the traffic lane identity.
    pub const fn lane(self) -> LaneId {
        self.lane
    }

    /// Returns the physical connection-slot identity.
    pub const fn connection(self) -> ConnectionId {
        self.connection
    }

    /// Returns the exact socket lifetime.
    pub const fn epoch(self) -> ConnectionEpoch {
        self.epoch
    }
}

/// Selector-independent identity, timing, and socket policy for one connection epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionSlotConfig {
    identity: ConnectionIdentity,
    connect_deadline: Deadline,
    timer_owner: TimerOwnerId,
    socket_policy: TcpSocketPolicy,
}

impl ConnectionSlotConfig {
    /// Creates one exact connection epoch with an absolute connect deadline.
    pub const fn new(
        identity: ConnectionIdentity,
        connect_deadline: Deadline,
        timer_owner: TimerOwnerId,
    ) -> Self {
        Self {
            identity,
            connect_deadline,
            timer_owner,
            socket_policy: TcpSocketPolicy::DEFAULT,
        }
    }

    /// Replaces the post-connect TCP socket policy.
    #[must_use]
    pub const fn socket_policy(mut self, policy: TcpSocketPolicy) -> Self {
        self.socket_policy = policy;
        self
    }

    pub(crate) const fn identity(self) -> ConnectionIdentity {
        self.identity
    }

    pub(crate) const fn connect_deadline(self) -> Deadline {
        self.connect_deadline
    }

    pub(crate) const fn timer_owner(self) -> TimerOwnerId {
        self.timer_owner
    }

    pub(crate) const fn tcp_policy(self) -> TcpSocketPolicy {
        self.socket_policy
    }
}

/// One already-resolved TCP address paired with selector-independent slot policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionConfig {
    slot: ConnectionSlotConfig,
    address: SocketAddr,
}

impl ConnectionConfig {
    /// Creates one exact address attempt with an absolute connection deadline.
    pub const fn new(
        identity: ConnectionIdentity,
        address: SocketAddr,
        connect_deadline: Deadline,
        timer_owner: TimerOwnerId,
    ) -> Self {
        Self {
            slot: ConnectionSlotConfig::new(identity, connect_deadline, timer_owner),
            address,
        }
    }

    /// Replaces the post-connect TCP socket policy.
    #[must_use]
    pub const fn socket_policy(mut self, policy: TcpSocketPolicy) -> Self {
        self.slot = self.slot.socket_policy(policy);
        self
    }

    pub(crate) const fn identity(self) -> ConnectionIdentity {
        self.slot.identity()
    }

    pub(crate) const fn address(self) -> SocketAddr {
        self.address
    }

    pub(crate) const fn slot(self) -> ConnectionSlotConfig {
        self.slot
    }
}

/// Identity configuration for one shared selector/resource owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionSetConfig {
    resource_owner: ResourceOwnerId,
}

impl ConnectionSetConfig {
    /// Creates a shared owner with one exact resource-token domain.
    pub const fn new(resource_owner: ResourceOwnerId) -> Self {
        Self { resource_owner }
    }

    pub(crate) const fn resource_owner(self) -> ResourceOwnerId {
        self.resource_owner
    }
}

/// Combined configuration for the capacity-one convenience owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StandaloneConnectionConfig {
    set: ConnectionSetConfig,
    connection: ConnectionConfig,
}

impl StandaloneConnectionConfig {
    /// Combines selector ownership with one exact connection attempt.
    pub const fn new(set: ConnectionSetConfig, connection: ConnectionConfig) -> Self {
        Self { set, connection }
    }

    pub(crate) const fn set(self) -> ConnectionSetConfig {
        self.set
    }

    pub(crate) const fn connection(self) -> ConnectionConfig {
        self.connection
    }
}
