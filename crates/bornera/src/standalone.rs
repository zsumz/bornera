//! Capacity-one convenience wrapper around the shared-selector owner.

use bornera_core::{
    CancelOutcome, CloseReason, FrameDecoder, InputDisposition, OperationId, OperationOptions,
    OperationPermit,
};
use calandria::{Duty, EventBatchDrain, Moment, Next, Retained, Span, Turn, WaitOutcome};

use crate::{
    ConnectError, ConnectionCommitError, ConnectionEvent, ConnectionPort, ConnectionSet,
    ConnectionSetLimits, ConnectionSetSnapshot, ConnectionSlotLimits, ConnectionSlotSnapshot,
    ConnectionToken, EngineCommitError, EngineError, EngineOutcome, InboundClassifier,
    OutboundFrame, OwnerFailure, StandaloneConnectionConfig, TransportState,
};

/// Dedicated capacity-one owner implemented by the same bounded connection set.
#[derive(Debug)]
pub struct StandaloneConnection<D, C>
where
    D: FrameDecoder,
{
    pub(crate) set: ConnectionSet<D, C>,
    pub(crate) connection: ConnectionToken,
}

impl<D, C> StandaloneConnection<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    /// Begins one exact nonblocking connection in a capacity-one set.
    pub fn connect(
        config: StandaloneConnectionConfig,
        limits: ConnectionSlotLimits,
        decoder: D,
        classifier: C,
    ) -> Result<Self, ConnectError<D::Error>> {
        let set_limits = ConnectionSetLimits::standalone(limits);
        let mut set = ConnectionSet::new(config.set(), set_limits).map_err(ConnectError::Mio)?;
        let connection = set.connect(config.connection(), limits, decoder, classifier)?;
        Ok(Self { set, connection })
    }

    /// Returns the generation-fenced identity of the sole connection.
    pub const fn token(&self) -> ConnectionToken {
        self.connection
    }

    /// Returns a bounded command producer for the sole connection.
    pub fn port(&self) -> ConnectionPort {
        ConnectionPort::new(self.connection, self.set.sender.clone())
    }

    /// Creates an independent coalesced wake domain for the owned selector.
    pub fn wake_handle(&self) -> calandria::WakeHandle {
        self.set.wake_handle()
    }

    /// Reserves bounded operation ownership.
    pub fn reserve(
        &mut self,
        now: Moment,
        options: OperationOptions,
    ) -> Result<OperationPermit, bornera_core::ReserveError> {
        match self.set.reserve(self.connection, now, options) {
            Ok(permit) => Ok(permit),
            Err(crate::ConnectionReserveError::Rejected(error)) => Err(error),
            Err(crate::ConnectionReserveError::StaleConnection) => {
                Err(bornera_core::ReserveError::OwnerPoisoned)
            }
        }
    }

    /// Atomically transfers a permit and complete frame to write ownership.
    pub fn commit(
        &mut self,
        permit: OperationPermit,
        frame: OutboundFrame,
    ) -> Result<OperationId, EngineCommitError<OutboundFrame>> {
        match self.set.commit(self.connection, permit, frame) {
            Ok(operation) => Ok(operation),
            Err(ConnectionCommitError::Connection(error)) => Err(error),
            Err(ConnectionCommitError::StaleConnection { permit, frame }) => {
                Err(EngineCommitError::OwnerFailed {
                    reason: OwnerFailure::OwnerInvariant,
                    permit,
                    frame,
                })
            }
        }
    }

    /// Opens regular admission synchronously after establishment.
    pub fn open_admission(&mut self) -> Result<InputDisposition, EngineError> {
        self.set.open_admission(self.connection)
    }

    /// Cancels local observation synchronously.
    pub fn cancel(&mut self, operation: OperationId) -> Result<CancelOutcome, EngineError> {
        self.set.cancel(self.connection, operation)
    }

    /// Begins ordered draining synchronously.
    pub fn begin_drain(&mut self) -> Result<InputDisposition, EngineError> {
        self.set.begin_drain(self.connection)
    }

    /// Requests mechanical closure synchronously.
    pub fn finalize(&mut self, reason: CloseReason) -> Result<InputDisposition, EngineError> {
        self.set.finalize(self.connection, reason)
    }

    /// Drains terminal outcomes.
    pub fn drain_outcomes(
        &mut self,
    ) -> Result<EventBatchDrain<'_, EngineOutcome<D::Frame>>, EngineError> {
        self.set.drain_outcomes(self.connection)
    }

    /// Drains lifecycle events.
    pub fn drain_events(&mut self) -> Result<EventBatchDrain<'_, ConnectionEvent>, EngineError> {
        self.set.drain_events(self.connection)
    }

    /// Returns immutable state for the sole connection.
    pub fn snapshot(&self) -> Result<ConnectionSlotSnapshot, EngineError> {
        self.set.connection_snapshot(self.connection)
    }

    /// Returns immutable pressure state for the capacity-one set machinery.
    pub fn set_snapshot(&self) -> ConnectionSetSnapshot {
        self.set.snapshot()
    }

    /// Returns whether the TCP capability completed establishment.
    pub fn is_transport_open(&self) -> Result<bool, EngineError> {
        self.set.is_transport_open(self.connection)
    }

    /// Performs one bounded capacity-one owner turn.
    pub fn turn_component(&mut self, now: Moment) -> Result<Turn, EngineError> {
        self.ensure_running()?;
        let turn = self.set.turn_component(now)?;
        let snapshot = self.set.connection_snapshot(self.connection)?;
        if let Some(reason) = snapshot.owner_failure {
            return Err(EngineError::OwnerFailed(reason));
        }
        Ok(if snapshot.transport == TransportState::Closed {
            Turn::new(turn.work(), Next::Stop)
        } else {
            turn
        })
    }

    /// Polls the capacity-one set's selector.
    pub fn poll_io(&mut self, maximum: Span) -> Result<WaitOutcome, EngineError> {
        self.ensure_running()?;
        self.set.poll_io(maximum)
    }

    fn ensure_running(&self) -> Result<(), EngineError> {
        let snapshot = self.set.connection_snapshot(self.connection)?;
        snapshot
            .owner_failure
            .map_or(Ok(()), |reason| Err(EngineError::OwnerFailed(reason)))
    }
}

impl<D, C> Duty for StandaloneConnection<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    type Error = EngineError;

    fn turn(&mut self, now: Moment) -> Result<Turn, Self::Error> {
        self.turn_component(now)
    }
}
