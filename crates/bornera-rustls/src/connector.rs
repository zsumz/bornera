//! Capacity-first Bornera connector for exact rustls client attempts.

use std::{io, net::SocketAddr};

use bornera::{TransportConnector, TransportLimits};

use crate::{RustlsConnectError, RustlsTransport, RustlsTransportConfig};

/// Cloneable per-attempt connector retaining TLS identity and bounded configuration.
#[derive(Clone, Debug)]
pub struct RustlsConnector {
    config: RustlsTransportConfig,
}

impl RustlsConnector {
    /// Creates a connector for one logical server identity and client configuration.
    pub const fn new(config: RustlsTransportConfig) -> Self {
        Self { config }
    }

    /// Returns the immutable transport configuration.
    pub const fn config(&self) -> &RustlsTransportConfig {
        &self.config
    }
}

impl TransportConnector for RustlsConnector {
    type Transport = RustlsTransport;

    fn connect(self, address: SocketAddr, limits: TransportLimits) -> io::Result<Self::Transport> {
        RustlsTransport::connect(address, &self.config, limits).map_err(connect_io_error)
    }
}

fn connect_io_error(error: RustlsConnectError) -> io::Error {
    match error {
        RustlsConnectError::Io(source) => source,
        RustlsConnectError::Transport(diagnostic) => {
            io::Error::new(diagnostic.kind, RustlsConnectError::Transport(diagnostic))
        }
        other @ (RustlsConnectError::Capacity { .. } | RustlsConnectError::Tls(_)) => {
            io::Error::new(io::ErrorKind::InvalidInput, other)
        }
    }
}
