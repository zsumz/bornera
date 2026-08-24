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
    /// Opens regular operation admission after transport establishment.
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

    /// Closes admission and begins ordered draining.
    pub fn begin_drain(&mut self) -> Result<InputDisposition, EngineError> {
        self.ensure_running()?;
        let result = self.begin_drain_inner();
        self.latch(result)
    }

    /// Requests mechanical closure for this exact connection epoch.
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
            transport_diagnostic: self.transport_diagnostic,
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

    pub(crate) const fn is_connecting(&self) -> bool {
        matches!(self.transport_state, TransportState::Connecting)
    }

    /// Returns the earliest connect or operation deadline owned by this slot.
    pub fn next_deadline(&self) -> Option<Deadline> {
        let operation = self.timers.next_deadline();
        if self.is_connecting() {
            Some(operation.map_or(self.connect_deadline, |deadline| {
                if deadline <= self.connect_deadline {
                    deadline
                } else {
                    self.connect_deadline
                }
            }))
        } else {
            operation
        }
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

    fn begin_drain_inner(&mut self) -> Result<InputDisposition, EngineError> {
        let transition = self
            .core
            .apply(ConnectionInput::BeginDrain {
                epoch: self.core.epoch(),
            })
            .map_err(EngineError::Core)?;
        let disposition = transition.disposition();
        self.interpret_unit(transition)?;
        Ok(disposition)
    }

    fn finalize_inner(&mut self, reason: CloseReason) -> Result<InputDisposition, EngineError> {
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
