//! Capacity-first acquisition for one registered transport in a shared selector.

use bornera_core::FrameDecoder;
use calandria::Retained;

use crate::{
    ConnectError, ConnectionConfig, ConnectionEntry, ConnectionSet, ConnectionSlot,
    ConnectionSlotLimits, ConnectionToken, InboundClassifier, RegisteredTransport, TcpTransport,
    TransportConnector,
};

impl<D, C> ConnectionSet<D, C, TcpTransport>
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
        self.connect_with(config, limits, decoder, classifier, TcpConnector)
    }
}

impl<D, C, T> ConnectionSet<D, C, T>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
    T: RegisteredTransport,
{
    /// Capacity-first construction and registration of one exact transport attempt.
    pub fn connect_with<K>(
        &mut self,
        config: ConnectionConfig,
        limits: ConnectionSlotLimits,
        decoder: D,
        classifier: C,
        connector: K,
    ) -> Result<ConnectionToken, ConnectError<D::Error>>
    where
        K: TransportConnector<Transport = T>,
    {
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
                    interest: calandria::Interest::READ_WRITE,
                    ready_queued: false,
                },
            )
            .map_err(|_| ConnectError::ResourceAdmission)?;
        let transport = match connector.connect(config.address()) {
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
        entry.interest = entry.slot.desired_interest(&transport);
        entry.transport = Some(transport);
        let registration = self
            .poller
            .register(
                entry
                    .transport
                    .as_mut()
                    .ok_or(ConnectError::ResourceAdmission)?,
                resource,
                entry.interest,
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

#[derive(Clone, Copy, Debug)]
struct TcpConnector;

impl TransportConnector for TcpConnector {
    type Transport = TcpTransport;

    fn connect(self, address: std::net::SocketAddr) -> std::io::Result<Self::Transport> {
        TcpTransport::connect(address)
    }
}
