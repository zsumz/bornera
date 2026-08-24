//! Direct generation-fenced access to connections in a shared set.

use bornera_core::{
    CancelOutcome, CloseReason, FrameDecoder, InputDisposition, OperationId, OperationOptions,
    OperationPermit,
};
use calandria::{EventBatchDrain, Moment, Retained};

use crate::{
    ConnectionAccessError, ConnectionCommitError, ConnectionEvent, ConnectionReserveError,
    ConnectionRetireError, ConnectionSet, ConnectionSlotSnapshot, ConnectionToken, EngineOutcome,
    InboundClassifier, OutboundFrame, TransportState,
};

impl<D, C> ConnectionSet<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    /// Reserves bounded operation ownership in one exact connection.
    pub fn reserve(
        &mut self,
        connection: ConnectionToken,
        now: Moment,
        options: OperationOptions,
    ) -> Result<OperationPermit, ConnectionReserveError> {
        let Ok(entry) = self.entry_mut(connection) else {
            return Err(ConnectionReserveError::StaleConnection);
        };
        entry
            .slot
            .reserve(now, options)
            .map_err(ConnectionReserveError::Rejected)
    }

    /// Atomically transfers a permit and prepared frame to one connection.
    pub fn commit(
        &mut self,
        connection: ConnectionToken,
        permit: OperationPermit,
        frame: OutboundFrame,
    ) -> Result<OperationId, ConnectionCommitError<OutboundFrame>> {
        let resource = connection.resource();
        let result = match self.entry_mut(connection) {
            Ok(entry) => entry
                .slot
                .commit(permit, frame)
                .map_err(ConnectionCommitError::Connection),
            Err(_) => Err(ConnectionCommitError::StaleConnection { permit, frame }),
        };
        self.enqueue(resource);
        match result {
            Ok(operation) => match self.settle_connection(resource) {
                Ok(_) => Ok(operation),
                Err(source) => Err(ConnectionCommitError::Connection(
                    crate::EngineCommitError::Owner { operation, source },
                )),
            },
            Err(error) => {
                let _settled = self.settle_connection(resource);
                Err(error)
            }
        }
    }

    /// Applies admission opening synchronously to one exact generation.
    pub fn open_admission(
        &mut self,
        connection: ConnectionToken,
    ) -> Result<InputDisposition, ConnectionAccessError> {
        self.apply_and_enqueue(connection, ConnectionPortAction::OpenAdmission)
    }

    /// Applies local observation cancellation synchronously.
    pub fn cancel(
        &mut self,
        connection: ConnectionToken,
        operation: OperationId,
    ) -> Result<CancelOutcome, ConnectionAccessError> {
        let resource = connection.resource();
        let result = self.entry_mut(connection)?.slot.cancel(operation);
        self.enqueue(resource);
        match result {
            Ok(outcome) => self
                .settle_connection(resource)
                .map(|_| outcome)
                .map_err(ConnectionAccessError::Owner),
            Err(error) => {
                let _settled = self.settle_connection(resource);
                Err(ConnectionAccessError::Owner(error))
            }
        }
    }

    /// Closes admission and begins ordered draining synchronously.
    pub fn begin_drain(
        &mut self,
        connection: ConnectionToken,
    ) -> Result<InputDisposition, ConnectionAccessError> {
        self.apply_and_enqueue(connection, ConnectionPortAction::BeginDrain)
    }

    /// Requests mechanical closure synchronously.
    pub fn finalize(
        &mut self,
        connection: ConnectionToken,
        reason: CloseReason,
    ) -> Result<InputDisposition, ConnectionAccessError> {
        let resource = connection.resource();
        let result = self.entry_mut(connection)?.slot.finalize(reason);
        self.enqueue(resource);
        match result {
            Ok(disposition) => self
                .settle_connection(resource)
                .map(|_| disposition)
                .map_err(ConnectionAccessError::Owner),
            Err(error) => {
                let _settled = self.settle_connection(resource);
                Err(ConnectionAccessError::Owner(error))
            }
        }
    }

    /// Drains terminal outcomes retained by one exact connection.
    pub fn drain_outcomes(
        &mut self,
        connection: ConnectionToken,
    ) -> Result<EventBatchDrain<'_, EngineOutcome<D::Frame>>, ConnectionAccessError> {
        Ok(self.entry_mut(connection)?.slot.drain_outcomes())
    }

    /// Drains lifecycle publications retained by one exact connection.
    pub fn drain_events(
        &mut self,
        connection: ConnectionToken,
    ) -> Result<EventBatchDrain<'_, ConnectionEvent>, ConnectionAccessError> {
        Ok(self.entry_mut(connection)?.slot.drain_events())
    }

    /// Returns immutable state for one exact connection generation.
    pub fn connection_snapshot(
        &self,
        connection: ConnectionToken,
    ) -> Result<ConnectionSlotSnapshot, ConnectionAccessError> {
        Ok(self.entry(connection)?.slot.snapshot())
    }

    /// Returns whether one exact connection has established its transport.
    pub fn is_transport_open(
        &self,
        connection: ConnectionToken,
    ) -> Result<bool, ConnectionAccessError> {
        Ok(self.entry(connection)?.slot.is_transport_open())
    }

    /// Retires a clean closed generation after all publications are drained.
    pub fn retire(&mut self, connection: ConnectionToken) -> Result<(), ConnectionRetireError> {
        let entry = self
            .entry(connection)
            .map_err(|_| ConnectionRetireError::StaleConnection)?;
        if let Some(reason) = entry.slot.state.failure() {
            return Err(ConnectionRetireError::OwnerFailed(reason));
        }
        let snapshot = entry.slot.snapshot();
        if entry.transport.is_some() || snapshot.transport != TransportState::Closed {
            return Err(ConnectionRetireError::TransportLive);
        }
        if snapshot.pending_outcomes != 0 || snapshot.pending_events != 0 {
            return Err(ConnectionRetireError::PublicationsPending);
        }
        self.ready.retain(|token| *token != connection.resource());
        self.resources
            .remove(connection.resource())
            .map_err(|_| ConnectionRetireError::StaleConnection)?;
        Ok(())
    }

    fn apply_and_enqueue(
        &mut self,
        connection: ConnectionToken,
        action: ConnectionPortAction,
    ) -> Result<InputDisposition, ConnectionAccessError> {
        let resource = connection.resource();
        let entry = self.entry_mut(connection)?;
        let result = match action {
            ConnectionPortAction::OpenAdmission => entry.slot.open_admission(),
            ConnectionPortAction::BeginDrain => entry.slot.begin_drain(),
        };
        self.enqueue(resource);
        match result {
            Ok(disposition) => self
                .settle_connection(resource)
                .map(|_| disposition)
                .map_err(ConnectionAccessError::Owner),
            Err(error) => {
                let _settled = self.settle_connection(resource);
                Err(ConnectionAccessError::Owner(error))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectionPortAction {
    OpenAdmission,
    BeginDrain,
}
