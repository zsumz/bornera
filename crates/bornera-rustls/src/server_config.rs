//! Shared configuration and capacity-first construction failures for server sessions.

use std::sync::Arc;

use bornera::TransportDiagnostic;
use calandria::RetainedBytes;
use rustls::ServerConfig;

use crate::RustlsTransportLimits;

/// Shared rustls identity and exact per-session retained-memory policy.
#[derive(Clone, Debug)]
pub struct RustlsServerConfig {
    server: Arc<ServerConfig>,
    limits: RustlsTransportLimits,
}

impl RustlsServerConfig {
    /// Creates a socket-free server-session configuration.
    pub const fn new(server: Arc<ServerConfig>, limits: RustlsTransportLimits) -> Self {
        Self { server, limits }
    }

    /// Returns the shared rustls server identity and protocol policy.
    pub const fn server_config(&self) -> &Arc<ServerConfig> {
        &self.server
    }

    /// Returns the exact per-session transport bounds.
    pub const fn limits(&self) -> RustlsTransportLimits {
        self.limits
    }
}

/// Failure before one socket-free rustls server session becomes observable.
#[derive(Debug)]
#[non_exhaustive]
pub enum RustlsServerSessionError {
    /// The configured session charge exceeds its caller-supplied ceiling.
    Capacity {
        /// Charge required by this rustls session configuration.
        required: RetainedBytes,
        /// Ceiling supplied by the connection owner.
        supplied: RetainedBytes,
    },
    /// Rustls's initial logical state exceeded a configured bound.
    Transport(TransportDiagnostic),
    /// Rustls rejected construction of the server connection.
    Tls(rustls::Error),
}
