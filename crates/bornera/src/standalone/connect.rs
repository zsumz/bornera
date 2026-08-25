//! Capacity-one construction through plaintext or custom registered transports.

use bornera_core::FrameDecoder;
use calandria::Retained;

use super::StandaloneConnection;
use crate::{
    ConnectError, ConnectionSet, ConnectionSetLimits, ConnectionSlotLimits, InboundClassifier,
    RegisteredTransport, StandaloneConnectionConfig, TcpTransport, TransportConnector,
};

impl<D, C> StandaloneConnection<D, C, TcpTransport>
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
}

impl<D, C, T> StandaloneConnection<D, C, T>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
    T: RegisteredTransport,
{
    /// Capacity-first construction through one custom registered-transport connector.
    pub fn connect_with<K>(
        config: StandaloneConnectionConfig,
        limits: ConnectionSlotLimits,
        decoder: D,
        classifier: C,
        connector: K,
    ) -> Result<Self, ConnectError<D::Error>>
    where
        K: TransportConnector<Transport = T>,
    {
        let set_limits = ConnectionSetLimits::standalone(limits);
        let mut set = ConnectionSet::new(config.set(), set_limits).map_err(ConnectError::Mio)?;
        let connection =
            set.connect_with(config.connection(), limits, decoder, classifier, connector)?;
        Ok(Self { set, connection })
    }
}
