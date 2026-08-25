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
        let transport = match connector.connect(config.address(), limits.transport_limits()) {
            Ok(transport) => transport,
            Err(source) => {
                let _removed = self.resources.remove(resource);
                return Err(ConnectError::Io(source));
            }
        };
        let pressure = transport.pressure();
        let limit = limits.transport_retained_bytes();
        let transport_limit = transport.pressure_limit().retained_bytes();
        if transport_limit > limit {
            let _removed = self.resources.remove(resource);
            return Err(ConnectError::TransportLimit {
                limit,
                reported: transport_limit,
            });
        }
        if pressure.total() > transport_limit {
            let _removed = self.resources.remove(resource);
            return Err(ConnectError::TransportCapacity {
                limit: transport_limit,
                reported: pressure,
            });
        }
        let (registration, post_registration_pressure, post_registration_limit) = {
            let (poller, resources) = (&mut self.poller, &mut self.resources);
            let (_, entry) = resources
                .get_mut(resource)
                .map_err(|_| ConnectError::ResourceAdmission)?;
            entry.interest = entry.slot.desired_interest(&transport);
            entry.slot.transport_pressure = Some(pressure);
            entry.slot.transport_retained_limit = Some(transport_limit);
            entry.transport = Some(transport);
            let transport = entry
                .transport
                .as_mut()
                .ok_or(ConnectError::ResourceAdmission)?;
            let registration = poller.register(transport, resource, entry.interest);
            let pressure = entry.slot.observe_transport_pressure(transport);
            let limit = transport.pressure_limit().retained_bytes();
            (registration, pressure, limit)
        };
        if let Err(source) = registration {
            let _removed = self.resources.remove(resource);
            return Err(ConnectError::Mio(source));
        }
        let limit_changed = post_registration_limit != transport_limit;
        if limit_changed || post_registration_pressure.total() > transport_limit {
            let cleanup = {
                let (poller, resources) = (&mut self.poller, &mut self.resources);
                let (_, entry) = resources
                    .get_mut(resource)
                    .map_err(|_| ConnectError::ResourceAdmission)?;
                entry.slot.transport_pressure = Some(post_registration_pressure);
                let transport = entry
                    .transport
                    .as_mut()
                    .ok_or(ConnectError::ResourceAdmission)?;
                let cleanup = poller.deregister(transport, resource);
                entry.slot.observe_transport_pressure(transport);
                cleanup
            };
            if let Err(source) = cleanup {
                self.latch_selector_failure(&source);
                let _removed = self.resources.remove(resource);
                return Err(ConnectError::Mio(source));
            }
            let _removed = self.resources.remove(resource);
            if limit_changed {
                return Err(ConnectError::TransportLimit {
                    limit: transport_limit,
                    reported: post_registration_limit,
                });
            }
            return Err(ConnectError::TransportCapacity {
                limit: transport_limit,
                reported: post_registration_pressure,
            });
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

    fn connect(
        self,
        address: std::net::SocketAddr,
        _limits: crate::TransportLimits,
    ) -> std::io::Result<Self::Transport> {
        TcpTransport::connect(address)
    }
}
