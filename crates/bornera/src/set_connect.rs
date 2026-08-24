//! Capacity-first acquisition for one TCP connection in a shared selector.

use bornera_core::FrameDecoder;
use calandria::Retained;

use crate::{
    ConnectError, ConnectionConfig, ConnectionEntry, ConnectionSet, ConnectionSlot,
    ConnectionSlotLimits, ConnectionToken, InboundClassifier, PlaintextTransport,
};

impl<D, C> ConnectionSet<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    /// Begins and registers one exact nonblocking TCP connection attempt.
    pub fn connect(
        &mut self,
        config: ConnectionConfig,
        limits: ConnectionSlotLimits,
        decoder: D,
        classifier: C,
    ) -> Result<ConnectionToken, ConnectError<D::Error>> {
        self.connect_with(
            config,
            limits,
            decoder,
            classifier,
            PlaintextTransport::connect,
        )
    }

    pub(crate) fn connect_with(
        &mut self,
        config: ConnectionConfig,
        limits: ConnectionSlotLimits,
        decoder: D,
        classifier: C,
        connector: fn(std::net::SocketAddr) -> std::io::Result<PlaintextTransport>,
    ) -> Result<ConnectionToken, ConnectError<D::Error>> {
        if let Some(reason) = self.owner_failure {
            return Err(ConnectError::OwnerFailed(reason));
        }
        let slot = ConnectionSlot::new(config.slot(), limits, decoder, classifier)
            .map_err(ConnectError::Decoder)?;
        let identity = config.identity();
        let resource = self
            .resources
            .admit(
                identity,
                ConnectionEntry {
                    slot,
                    transport: None,
                    ready_queued: false,
                },
            )
            .map_err(|_| ConnectError::ResourceAdmission)?;
        let transport = match connector(config.address()) {
            Ok(transport) => transport,
            Err(source) => {
                let _removed = self.resources.remove(resource);
                return Err(ConnectError::Io(source));
            }
        };
        let (_, entry) = self
            .resources
            .get_mut(resource)
            .map_err(|_| ConnectError::ResourceAdmission)?;
        entry.transport = Some(transport);
        let registration = self
            .poller
            .register(
                entry
                    .transport
                    .as_mut()
                    .ok_or(ConnectError::ResourceAdmission)?,
                resource,
                calandria::Interest::READ_WRITE,
            )
            .map_err(ConnectError::Mio);
        if let Err(error) = registration {
            let _removed = self.resources.remove(resource);
            return Err(error);
        }
        let token = ConnectionToken::new(resource, identity);
        self.enqueue(resource);
        Ok(token)
    }
}
