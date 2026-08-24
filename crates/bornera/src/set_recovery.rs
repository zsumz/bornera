//! Per-generation recovery inside a shared selector owner.

use bornera_core::FrameDecoder;
use calandria::Retained;

use crate::{
    ConnectionRecoveryError, ConnectionSet, ConnectionToken, InboundClassifier, OutboundFrame,
    OwnerFailure, RecoveryReport, RegisteredTransport,
};

impl<D, C, T> ConnectionSet<D, C, T>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
    T: RegisteredTransport,
{
    /// Transfers one failed generation without consuming peer ownership.
    ///
    /// A connection-local failure leaves healthy peers live. A set-wide
    /// readiness failure makes each peer independently recoverable. Successful
    /// recovery retires this resource generation, so its tokens become stale.
    pub fn try_recover(
        &mut self,
        connection: ConnectionToken,
    ) -> Result<RecoveryReport<OutboundFrame, D::Frame>, ConnectionRecoveryError> {
        let reason = self
            .entry(connection)
            .map_err(|_| ConnectionRecoveryError::StaleConnection)?
            .slot
            .state
            .failure()
            .ok_or(ConnectionRecoveryError::OwnerRunning)?;
        self.recover_connection(connection, reason)
    }

    /// Explicitly abandons one generation without consuming healthy peers.
    pub fn abandon(
        &mut self,
        connection: ConnectionToken,
        requested: OwnerFailure,
    ) -> Result<RecoveryReport<OutboundFrame, D::Frame>, ConnectionRecoveryError> {
        let reason = self
            .entry(connection)
            .map_err(|_| ConnectionRecoveryError::StaleConnection)?
            .slot
            .state
            .failure()
            .unwrap_or(requested);
        self.recover_connection(connection, reason)
    }

    fn recover_connection(
        &mut self,
        connection: ConnectionToken,
        reason: OwnerFailure,
    ) -> Result<RecoveryReport<OutboundFrame, D::Frame>, ConnectionRecoveryError> {
        let resource = connection.resource();
        let cleanup_error = {
            let (poller, resources) = (&mut self.poller, &mut self.resources);
            let (identity, entry) = resources
                .get_mut(resource)
                .map_err(|_| ConnectionRecoveryError::StaleConnection)?;
            if *identity != connection.identity() {
                return Err(ConnectionRecoveryError::StaleConnection);
            }
            entry
                .transport
                .as_mut()
                .and_then(|transport| poller.deregister(transport, resource).err())
        };
        if let Some(error) = cleanup_error.as_ref() {
            self.latch_selector_failure(error);
        }
        self.ready.retain(|token| *token != resource);
        let (_, mut entry) = self
            .resources
            .remove(resource)
            .map_err(|_| ConnectionRecoveryError::StaleConnection)?;
        Ok(entry.slot.recover_owned(reason, cleanup_error.is_some()))
    }
}
