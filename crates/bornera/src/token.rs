//! Opaque generation-fenced identity for one connection-set slot.

use bornera_core::{ConnectionEpoch, ConnectionId};
use calandria::ResourceToken;

use crate::ConnectionIdentity;

/// Exact live generation of one connection admitted to a [`crate::ConnectionSet`].
///
/// A token remains safe to retain after its connection closes. Every set
/// operation validates both its Calandria resource generation and the complete
/// Bornera connection identity before exposing mutable state.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ConnectionToken {
    resource: ResourceToken,
    identity: ConnectionIdentity,
}

impl ConnectionToken {
    pub(crate) const fn new(resource: ResourceToken, identity: ConnectionIdentity) -> Self {
        Self { resource, identity }
    }

    pub(crate) const fn resource(self) -> ResourceToken {
        self.resource
    }

    /// Returns the complete stable identity attached to this generation.
    pub const fn identity(self) -> ConnectionIdentity {
        self.identity
    }

    /// Returns the logical connection-slot identity.
    pub const fn connection(self) -> ConnectionId {
        self.identity.connection()
    }

    /// Returns the exact socket lifetime fenced by this token.
    pub const fn epoch(self) -> ConnectionEpoch {
        self.identity.epoch()
    }
}
