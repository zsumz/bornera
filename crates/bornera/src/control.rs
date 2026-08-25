//! Selector-free slot control, outcome drains, and observations.

use bornera_core::{
    CancelOutcome, CloseReason, ConnectionInput, FrameDecoder, InputDisposition, OperationId,
};
use calandria::{Deadline, EventBatchDrain, Retained};

use crate::{
    ConnectionEvent, ConnectionSlot, ConnectionSlotSnapshot, EngineError, EngineOutcome,
    InboundClassifier, TransportDiagnostic, TransportState,
};

impl<D, C> ConnectionSlot<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    /// Opens regular admission after the caller has established its protocol session.
    ///
    /// Bornera mechanically requires an open application transport but does not validate
    /// protocol negotiation. [`ConnectionEvent::AdmissionOpened`] is authoritative.
    pub fn open_admission(&mut self) -> Result<InputDisposition, EngineError> {
        self.ensure_running()?;
        let result = self.open_admission_inner();
        self.latch(result)
    }

    /// Cancels local observation for one accepted operation.
    pub fn cancel(&mut self, operation: OperationId) -> Result<CancelOutcome, EngineError> {
        self.ensure_running()?;
        let result = self.cancel_inner(operation);
        self.latch(result)
    }

    /// Closes admission and begins ordered draining through an absolute deadline.
    ///
    /// The deadline spans accepted operations and bounded transport-local graceful
    /// shutdown. Reaching it forces physical release without waiting for a peer.
    pub fn begin_drain(&mut self, deadline: Deadline) -> Result<InputDisposition, EngineError> {
        self.ensure_running()?;
        let result = self.begin_drain_inner(deadline);
        self.latch(result)
    }

    /// Forces mechanical closure for this exact connection epoch.
    ///
    /// This preempts pending transport-local graceful shutdown while retaining the
    /// close reason already established by core policy.
    pub fn finalize(&mut self, reason: CloseReason) -> Result<InputDisposition, EngineError> {
        self.ensure_running()?;
        let result = self.finalize_inner(reason);
        self.latch(result)
    }

    /// Drains terminal operation outcomes in publication order.
    pub fn drain_outcomes(&mut self) -> EventBatchDrain<'_, EngineOutcome<D::Frame>> {
        self.outcomes.drain()
    }

    /// Drains connection lifecycle events in sequence order.
    pub fn drain_events(&mut self) -> EventBatchDrain<'_, ConnectionEvent> {
        self.lifecycle.drain()
    }

    /// Returns immutable selector-independent slot state.
    pub fn snapshot(&self) -> ConnectionSlotSnapshot {
        ConnectionSlotSnapshot {
            connection: self.core.snapshot(),
            owner_failure: self.state.failure(),
            transport: self.transport_state,
            transport_release_ready: self
                .close_request
                .is_some_and(crate::CloseDirective::settlement_ready),
            shutdown_deadline: self
                .close_request
                .and_then(crate::CloseDirective::shutdown_deadline)
                .or(self.drain_deadline),
            transport_diagnostic: self.transport_diagnostic,
            transport_pressure: self.transport_pressure,
            transport_retained_limit: self.transport_retained_limit,
            transport_retained_ceiling: self.limits.transport_retained_bytes(),
            queued_write_frames: self.core.queued_write_frames(),
            buffered_write_retained_bytes: self.core.buffered_write_retained_bytes(),
            buffered_read_bytes: self.decoder.retained_bytes(),
            pending_outcomes: self.outcomes.len() + self.recovery_outcomes.len(),
            pending_events: self.lifecycle.len() + self.recovery_events.len(),
            event_sequence: self.event_sequence,
            pending_deadlines: self.timers.len(),
        }
    }

    /// Returns whether transport establishment completed.
    pub const fn is_transport_open(&self) -> bool {
        matches!(self.transport_state, TransportState::Open)
    }

    /// Returns whether the host may release the physical capability and settle closure.
    pub fn transport_release_ready(&self) -> bool {
        self.close_request
            .is_some_and(crate::CloseDirective::settlement_ready)
    }

    pub(crate) const fn is_connecting(&self) -> bool {
        matches!(self.transport_state, TransportState::Connecting)
    }

    /// Returns the earliest connect, operation, drain, or shutdown deadline.
    pub fn next_deadline(&self) -> Option<Deadline> {
        [
            self.timers.next_deadline(),
            self.is_connecting().then_some(self.connect_deadline),
            self.drain_deadline,
            self.close_request
                .and_then(crate::CloseDirective::shutdown_deadline),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    pub(crate) fn record_transport_failure(&mut self, diagnostic: TransportDiagnostic) {
        self.transport_diagnostic = Some(diagnostic);
    }

    fn cancel_inner(&mut self, operation: OperationId) -> Result<CancelOutcome, EngineError> {
        let transition = self
            .core
            .apply(ConnectionInput::Cancel {
                epoch: self.core.epoch(),
                operation,
            })
            .map_err(EngineError::Core)?;
        let outcome = transition
            .cancel_outcome()
            .unwrap_or(CancelOutcome::AlreadyTerminal);
        self.interpret_unit(transition)?;
        Ok(outcome)
    }

    fn begin_drain_inner(&mut self, deadline: Deadline) -> Result<InputDisposition, EngineError> {
        let transition = self
            .core
            .apply(ConnectionInput::BeginDrain {
                epoch: self.core.epoch(),
            })
            .map_err(EngineError::Core)?;
        let disposition = transition.disposition();
        if disposition == InputDisposition::Applied {
            self.drain_deadline = Some(deadline);
        }
        self.interpret_unit(transition)?;
        Ok(disposition)
    }

    fn finalize_inner(&mut self, reason: CloseReason) -> Result<InputDisposition, EngineError> {
        if self.force_shutdown() {
            return Ok(InputDisposition::Applied);
        }
        let transition = self
            .core
            .apply(ConnectionInput::CloseRequested {
                epoch: self.core.epoch(),
                reason,
            })
            .map_err(EngineError::Core)?;
        let disposition = transition.disposition();
        self.interpret_unit(transition)?;
        Ok(disposition)
    }

    fn open_admission_inner(&mut self) -> Result<InputDisposition, EngineError> {
        if !self.is_transport_open() {
            return Ok(InputDisposition::IgnoredInvalidPhase);
        }
        let transition = self
            .core
            .apply(ConnectionInput::OpenAdmission {
                epoch: self.core.epoch(),
            })
            .map_err(EngineError::Core)?;
        let disposition = transition.disposition();
        self.interpret_unit(transition)?;
        if disposition == InputDisposition::Applied {
            self.publish_admission_opened()?;
        }
        Ok(disposition)
    }
}
