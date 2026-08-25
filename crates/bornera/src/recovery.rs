//! Explicit conservative recovery after a connection owner cannot continue.

use core::fmt;

use bornera_core::{DiscardedWrite, FrameDecoder, RecoveredOperation};
use calandria::Retained;

use crate::{
    ConnectionEvent, EngineError, EngineOutcome, InboundClassifier, OutboundFrame,
    RegisteredTransport, StandaloneConnection, TcpTransport, TransportDiagnostic,
    TransportPressure,
};

/// Mechanical category explaining why normal owner finalization was abandoned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OwnerFailure {
    /// Deterministic aggregate ownership diverged or was poisoned.
    Core,
    /// The readiness backend failed.
    Readiness,
    /// A bounded production-owner invariant failed.
    OwnerInvariant,
}

impl From<&EngineError> for OwnerFailure {
    fn from(error: &EngineError) -> Self {
        match error {
            EngineError::Core(_) => Self::Core,
            EngineError::Mio(_) => Self::Readiness,
            EngineError::Invariant(_) => Self::OwnerInvariant,
            EngineError::OwnerFailed(reason) => *reason,
        }
    }
}

/// Bounded owner contents transferred to the parent after fatal failure.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RecoveryReport<F, R> {
    /// Exact failed transport lifetime.
    pub epoch: bornera_core::ConnectionEpoch,
    /// Mechanical fatal-owner category.
    pub reason: OwnerFailure,
    /// Nonterminal operations in original wire order.
    pub operations: Vec<RecoveredOperation<F>>,
    /// Outbound frames with no exact policy record at recovery.
    pub unmatched_writes: Vec<DiscardedWrite<F>>,
    /// Terminal outcomes published before recovery but not yet drained.
    pub outcomes: Vec<EngineOutcome<R>>,
    /// Lifecycle edges published before recovery but not yet drained.
    pub events: Vec<ConnectionEvent>,
    /// Last bounded transport diagnostic observed before recovery.
    pub transport_diagnostic: Option<TransportDiagnostic>,
    /// Last accounted pressure, or `None` before observation or after ownership was lost.
    pub transport_pressure: Option<TransportPressure>,
    /// Stable retained-memory bound declared by the adapter, when observed.
    pub transport_retained_limit: Option<calandria::RetainedBytes>,
    /// Configured slot ceiling for adapter-owned memory, when the slot remained identifiable.
    pub transport_retained_ceiling: Option<calandria::RetainedBytes>,
    /// Policy, frame, transport accounting, or cleanup ownership disagreed.
    pub ownership_diverged: bool,
}

/// Rejected recovery attempt that still owns the healthy connection.
pub struct RecoveryWhileRunning<D, C, T = TcpTransport>
where
    D: FrameDecoder,
    T: RegisteredTransport,
{
    connection: Box<StandaloneConnection<D, C, T>>,
}

impl<D, C, T> fmt::Debug for RecoveryWhileRunning<D, C, T>
where
    D: FrameDecoder,
    T: RegisteredTransport,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveryWhileRunning")
            .finish_non_exhaustive()
    }
}

impl<D, C, T> RecoveryWhileRunning<D, C, T>
where
    D: FrameDecoder,
    T: RegisteredTransport,
{
    /// Returns immutable access to the still-running capacity-one owner.
    pub fn connection(&self) -> &StandaloneConnection<D, C, T> {
        &self.connection
    }

    /// Recovers the still-running capacity-one owner intact.
    pub fn into_connection(self) -> StandaloneConnection<D, C, T> {
        *self.connection
    }
}

impl<D, C, T> StandaloneConnection<D, C, T>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
    T: RegisteredTransport,
{
    /// Tries to consume a failed connection and transfer all recoverable ownership.
    ///
    /// A running owner is returned intact. A failed fixed epoch cannot be
    /// resumed or reused after successful recovery.
    pub fn try_recover(
        self,
    ) -> Result<RecoveryReport<OutboundFrame, D::Frame>, RecoveryWhileRunning<D, C, T>> {
        let failure = match self.set.entry(self.connection) {
            Ok(entry) => entry.slot.state.failure(),
            Err(_) => Some(
                self.set
                    .owner_failure
                    .unwrap_or(OwnerFailure::OwnerInvariant),
            ),
        };
        let Some(reason) = failure else {
            return Err(RecoveryWhileRunning {
                connection: Box::new(self),
            });
        };
        Ok(self.recover_owned(reason))
    }

    /// Explicitly abandons a running owner or recovers its latched failure.
    pub fn abandon(self, requested: OwnerFailure) -> RecoveryReport<OutboundFrame, D::Frame> {
        let reason = self
            .set
            .entry(self.connection)
            .ok()
            .and_then(|entry| entry.slot.state.failure())
            .unwrap_or(requested);
        self.recover_owned(reason)
    }

    fn recover_owned(mut self, reason: OwnerFailure) -> RecoveryReport<OutboundFrame, D::Frame> {
        let resource = self.connection.resource();
        let epoch = self.connection.epoch();
        let (cleanup_failed, pressure_failed) = {
            let (poller, resources) = (&mut self.set.poller, &mut self.set.resources);
            let Ok((_, entry)) = resources.get_mut(resource) else {
                return empty_diverged(epoch, reason);
            };
            if let Some(transport) = entry.transport.as_mut() {
                let failed = poller.deregister(transport, resource).is_err();
                let pressure_failed = entry.slot.capture_transport_pressure(transport).is_err();
                (failed, pressure_failed)
            } else {
                (false, false)
            }
        };
        self.set.ready.retain(|token| *token != resource);
        let Ok((_, mut entry)) = self.set.resources.remove(resource) else {
            return empty_diverged(epoch, reason);
        };
        entry
            .slot
            .recover_owned(reason, cleanup_failed || pressure_failed)
    }
}

impl<D, C> crate::ConnectionSlot<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    /// Explicitly transfers all remaining ownership from this slot.
    ///
    /// The caller must first release any physical transport capability. The
    /// recovered slot is permanently unusable after this operation.
    pub fn recover(mut self, reason: OwnerFailure) -> RecoveryReport<OutboundFrame, D::Frame> {
        self.recover_owned(reason, false)
    }

    pub(crate) fn recover_owned(
        &mut self,
        reason: OwnerFailure,
        cleanup_failed: bool,
    ) -> RecoveryReport<OutboundFrame, D::Frame> {
        let recovery = self.core.recover();
        let mut outcomes: Vec<_> = self.outcomes.drain().collect();
        outcomes.extend(self.recovery_outcomes.drain());
        let mut events: Vec<_> = self.lifecycle.drain().collect();
        events.extend(self.recovery_events.drain());
        RecoveryReport {
            epoch: recovery.epoch,
            reason,
            operations: recovery.operations,
            unmatched_writes: recovery.unmatched_writes,
            outcomes,
            events,
            transport_diagnostic: self.transport_diagnostic,
            transport_pressure: self.transport_pressure,
            transport_retained_limit: self.transport_retained_limit,
            transport_retained_ceiling: Some(self.limits.transport_retained_bytes()),
            ownership_diverged: recovery.ownership_diverged
                || cleanup_failed
                || self.transport_contract_diverged,
        }
    }
}

fn empty_diverged<F, R>(
    epoch: bornera_core::ConnectionEpoch,
    reason: OwnerFailure,
) -> RecoveryReport<F, R> {
    RecoveryReport {
        epoch,
        reason,
        operations: Vec::new(),
        unmatched_writes: Vec::new(),
        outcomes: Vec::new(),
        events: Vec::new(),
        transport_diagnostic: None,
        transport_pressure: None,
        transport_retained_limit: None,
        transport_retained_ceiling: None,
        ownership_diverged: true,
    }
}
