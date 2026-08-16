//! Fixed-epoch owner state and construction.

use std::{cell::RefCell, rc::Rc};

use crate::{
    AdmissionGate, ConnectionEpoch, ConnectionId, ConnectionLimits, EndpointId, IdentitySeeds,
    LaneId, OrderedVerified, admission::ReservationLedger, identity::IdentityGenerator,
};

/// Physical lifecycle of one fixed connection epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionPhase {
    /// The fixed-epoch policy owner exists; transport establishment may still be pending.
    Live,
    /// Closure was requested and awaits capability confirmation.
    Closing,
    /// The physical capability confirmed closure.
    Closed,
}

/// Deterministic single-owner policy for one exact connection epoch.
#[derive(Debug)]
pub struct ConnectionMachine {
    pub(super) endpoint: EndpointId,
    pub(super) lane: LaneId,
    pub(super) connection: ConnectionId,
    pub(super) epoch: ConnectionEpoch,
    pub(super) phase: ConnectionPhase,
    pub(super) gate: AdmissionGate,
    pub(super) close_reason: Option<crate::CloseReason>,
    pub(super) identities: IdentityGenerator,
    pub(super) ledger: Rc<RefCell<ReservationLedger>>,
    pub(super) matching: OrderedVerified,
}

impl ConnectionMachine {
    /// Creates an active epoch with session-only admission and zero-based identities.
    pub(crate) fn new(
        endpoint: EndpointId,
        lane: LaneId,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        limits: ConnectionLimits,
    ) -> Self {
        Self::with_identity_seeds(
            endpoint,
            lane,
            connection,
            epoch,
            limits,
            IdentitySeeds::ZERO,
        )
    }

    /// Creates an epoch with deterministic seeds for replay and exhaustion proofs.
    pub(crate) fn with_identity_seeds(
        endpoint: EndpointId,
        lane: LaneId,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        limits: ConnectionLimits,
        seeds: IdentitySeeds,
    ) -> Self {
        Self {
            endpoint,
            lane,
            connection,
            epoch,
            phase: ConnectionPhase::Live,
            gate: AdmissionGate::SessionOnly,
            close_reason: None,
            identities: IdentityGenerator::new(seeds),
            ledger: Rc::new(RefCell::new(ReservationLedger::new(limits))),
            matching: OrderedVerified::new(limits.match_keys(), limits.max_operations()),
        }
    }

    /// Returns the exact epoch owned by this machine.
    pub const fn epoch(&self) -> ConnectionEpoch {
        self.epoch
    }

    /// Returns immutable state for the configured matching discipline.
    pub const fn matching(&self) -> &OrderedVerified {
        &self.matching
    }
}
