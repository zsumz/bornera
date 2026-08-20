//! Owner-local control, hosting, outcome, and observation surface.

use bornera_core::{
    CancelOutcome, CloseReason, ConnectionInput, FrameDecoder, InputDisposition, OperationId,
};
use calandria::{
    EventBatchDrain, Moment, PollReport, Retained, Span, Turn, WaitOutcome, WakeHandle,
};

use crate::{
    ConnectionEngine, ConnectionEvent, EngineError, EngineOutcome, EnginePort, EngineSnapshot,
    InboundClassifier, TransportState, to_u64,
};

impl<D, C> ConnectionEngine<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    /// Opens regular admission after the protocol-owned session lane succeeds.
    pub fn open_admission(&mut self) -> Result<InputDisposition, EngineError> {
        self.ensure_running()?;
        let result = self.open_admission_inner();
        self.latch(result)
    }

    /// Explicitly cancels local ownership of one operation.
    pub fn cancel(&mut self, operation: OperationId) -> Result<CancelOutcome, EngineError> {
        self.ensure_running()?;
        let result = self.cancel_inner(operation);
        self.latch(result)
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

    /// Closes admission and drains already accepted work in wire order.
    pub fn begin_drain(&mut self) -> Result<InputDisposition, EngineError> {
        self.ensure_running()?;
        let result = self.begin_drain_inner();
        self.latch(result)
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

    /// Forces mechanical closure of this exact connection epoch.
    pub fn close(&mut self) -> Result<InputDisposition, EngineError> {
        self.finalize(CloseReason::Requested)
    }

    /// Drives explicit terminal closure and publication for this exact epoch.
    pub fn finalize(&mut self, reason: CloseReason) -> Result<InputDisposition, EngineError> {
        self.ensure_running()?;
        let result = self.finalize_inner(reason);
        self.latch(result)
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

    /// Polls only the Calandria-owned readiness capability for a bounded duration.
    pub fn poll_io(&mut self, maximum: Span) -> Result<WaitOutcome, EngineError> {
        self.ensure_running()?;
        let result = self.poll_io_inner(maximum);
        self.latch(result)
    }

    /// Performs one bounded component turn for explicit composition in a protocol duty.
    pub fn turn_component(&mut self, now: Moment) -> Result<Turn, EngineError> {
        self.ensure_running()?;
        let result = self.drive_turn(now);
        self.latch(result)
    }

    /// Drains terminal data in deterministic publication order.
    pub fn drain_outcomes(&mut self) -> EventBatchDrain<'_, EngineOutcome<D::Frame>> {
        self.outcomes.drain()
    }

    /// Drains separately bounded lifecycle edges in monotonic sequence order.
    pub fn drain_events(&mut self) -> EventBatchDrain<'_, ConnectionEvent> {
        self.lifecycle.drain()
    }

    /// Creates another coalesced wake handle for this engine's Mio selector.
    pub fn wake_handle(&self) -> WakeHandle {
        self.poller.wake_handle()
    }

    /// Creates another producer for the bounded mechanical command mailbox.
    pub fn port(&self) -> EnginePort {
        self.port.clone()
    }

    /// Returns whether the private plaintext capability is established.
    pub fn is_transport_open(&self) -> bool {
        self.transport
            .and_then(|token| self.resources.get(token).ok())
            .is_some_and(|(_, transport)| transport.is_open())
    }

    /// Returns immutable connection, queue, timer, and stale-event state.
    pub fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            connection: self.core.snapshot(),
            owner_failure: self.state.failure(),
            transport: self.transport_state(),
            queued_write_frames: self.core.queued_write_frames(),
            buffered_write_bytes: self.core.buffered_write_bytes(),
            buffered_read_bytes: self.decoder.retained_bytes(),
            pending_outcomes: self.outcomes.len() + self.recovery_outcomes.len(),
            pending_events: self.lifecycle.len() + self.recovery_events.len(),
            event_sequence: self.event_sequence,
            pending_deadlines: self.timers.len(),
            commands: self.commands.snapshot(),
            stale_backend_events: self.stale_backend_events,
            stale_resource_events: self.stale_resource_events,
            stale_commands: self.stale_commands,
        }
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

    fn poll_io_inner(&mut self, maximum: Span) -> Result<WaitOutcome, EngineError> {
        let report = self.poller.poll(maximum, &mut self.poll_events)?;
        self.observe_poll(report);
        Ok(if report.observed() == 0 {
            WaitOutcome::Idle
        } else {
            WaitOutcome::Notified
        })
    }

    pub(crate) fn observe_poll(&mut self, report: PollReport) {
        self.poll_saturated = report.saturated();
        self.stale_backend_events = self
            .stale_backend_events
            .saturating_add(to_u64(report.stale()));
    }

    fn transport_state(&self) -> TransportState {
        self.transport
            .and_then(|token| self.resources.get(token).ok())
            .map_or(TransportState::Closed, |(_, transport)| transport.state())
    }
}
