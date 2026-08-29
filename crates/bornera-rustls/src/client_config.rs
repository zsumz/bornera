//! Capacity-first construction failures for socket-free client sessions.

use bornera::TransportDiagnostic;
use calandria::RetainedBytes;

/// Failure before one socket-free rustls client session becomes observable.
#[derive(Debug)]
#[non_exhaustive]
pub enum RustlsClientSessionError {
    /// The configured session charge exceeds its caller-supplied ceiling.
    Capacity {
        /// Charge required by this rustls session configuration.
        required: RetainedBytes,
        /// Ceiling supplied by the connection owner.
        supplied: RetainedBytes,
    },
    /// Rustls's initial logical state exceeded a configured bound.
    Transport(TransportDiagnostic),
    /// Rustls rejected construction of the client connection.
    Tls(rustls::Error),
}
