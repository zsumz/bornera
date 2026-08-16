//! Immutable observation of current connection ownership and capacity.

use calandria::RetainedBytes;

use crate::{
    AdmissionGate, ConnectionEpoch, ConnectionId, ConnectionMachine, ConnectionPhase, EndpointId,
    LaneId,
};

/// Immutable, data-only state for observation outside the owner path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionSnapshot {
    /// Logical endpoint.
    pub endpoint: EndpointId,
    /// Opaque traffic lane.
    pub lane: LaneId,
    /// Physical connection slot.
    pub connection: ConnectionId,
    /// Exact physical lifetime.
    pub epoch: ConnectionEpoch,
    /// Physical lifecycle phase.
    pub phase: ConnectionPhase,
    /// Mechanical admission gate.
    pub gate: AdmissionGate,
    /// Mechanical reason retained once this epoch begins closing.
    pub close_reason: Option<crate::CloseReason>,
    /// Affine reservations not yet committed or dropped.
    pub reserved_permits: usize,
    /// Accepted and reserved operation capacity currently owned.
    pub owned_operations: usize,
    /// Accepted operations still capable of producing an effect.
    pub active_operations: usize,
    /// FIFO entries retaining a terminal observation tombstone.
    pub terminal_slots: usize,
    /// Match keys unavailable in the current epoch.
    pub active_match_keys: usize,
    /// Semantic bytes retained by permits and active operations.
    pub retained_bytes: RetainedBytes,
    /// Frames retained by reservations or the write owner.
    pub buffered_write_frames: usize,
    /// Bytes retained by reservations or the write owner.
    pub buffered_write_bytes: RetainedBytes,
}

impl ConnectionMachine {
    /// Returns an immutable observation snapshot.
    pub fn snapshot(&self) -> ConnectionSnapshot {
        let ledger = self.ledger.borrow();
        ConnectionSnapshot {
            endpoint: self.endpoint,
            lane: self.lane,
            connection: self.connection,
            epoch: self.epoch,
            phase: self.phase,
            gate: self.gate,
            close_reason: self.close_reason,
            reserved_permits: ledger.permits(),
            owned_operations: ledger.operations(),
            active_operations: self.matching.active_len(),
            terminal_slots: self.matching.terminal_len(),
            active_match_keys: ledger.active_keys(),
            retained_bytes: ledger.retained_bytes(),
            buffered_write_frames: ledger.write_frames(),
            buffered_write_bytes: ledger.write_bytes(),
        }
    }
}
