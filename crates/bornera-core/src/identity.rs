//! Domain-specific identities owned by the connection policy.

/// A logical remote endpoint.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EndpointId(u64);

impl EndpointId {
    /// Creates an endpoint identity.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the fixed-width representation.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// An opaque traffic lane within an endpoint.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LaneId(u32);

impl LaneId {
    /// Creates a lane identity.
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the fixed-width representation.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// A physical connection slot.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionId(u64);

impl ConnectionId {
    /// Creates a connection identity.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the fixed-width representation.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// One exact lifetime of a physical connection.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionEpoch(u64);

impl ConnectionEpoch {
    /// Creates an epoch identity.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the fixed-width representation.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// An accepted operation owned by one epoch.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationId(u64);

impl OperationId {
    /// Creates an operation identity.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the fixed-width representation.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A transport effect whose completion may return later.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EffectId(u64);

impl EffectId {
    /// Creates an effect identity.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the fixed-width representation.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A protocol-visible response matching identity.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MatchKey(u32);

impl MatchKey {
    /// Creates a match key.
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the fixed-width representation.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Initial deterministic values for generated identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IdentitySeeds {
    operation: u64,
    effect: u64,
}

impl IdentitySeeds {
    /// Starts both generated identity domains at zero.
    pub const ZERO: Self = Self::new(0, 0);

    /// Creates deterministic identity seeds.
    pub const fn new(operation: u64, effect: u64) -> Self {
        Self { operation, effect }
    }

    /// Returns the first operation identity.
    pub const fn operation(self) -> u64 {
        self.operation
    }

    /// Returns the first effect identity.
    pub const fn effect(self) -> u64 {
        self.effect
    }
}

#[derive(Debug)]
pub(crate) struct IdentityGenerator {
    operation: Option<u64>,
    effect: Option<u64>,
}

impl IdentityGenerator {
    pub(crate) const fn new(seeds: IdentitySeeds) -> Self {
        Self {
            operation: Some(seeds.operation()),
            effect: Some(seeds.effect()),
        }
    }

    pub(crate) const fn available(&self) -> bool {
        self.operation.is_some() && self.effect.is_some()
    }

    pub(crate) fn take(&mut self) -> Option<(OperationId, EffectId)> {
        let operation = self.operation?;
        let effect = self.effect?;
        self.operation = operation.checked_add(1);
        self.effect = effect.checked_add(1);
        Some((OperationId::new(operation), EffectId::new(effect)))
    }
}
