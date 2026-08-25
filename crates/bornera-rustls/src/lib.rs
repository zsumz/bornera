//! Bounded rustls client transports for Bornera connection owners.
//!
//! The adapter keeps TCP establishment, socket policy, TLS handshake work,
//! encrypted I/O, and graceful close progression inside Bornera's existing
//! per-slot budgets. Application writes become irreversible when rustls accepts
//! their plaintext; ciphertext may remain locally buffered afterward.
//!
//! [`RustlsTransportLimits`] mechanically bounds application-write buffering and
//! observable TLS egress/plaintext. Its [`bornera::TransportPressure`] is a stable,
//! caller-audited conservative charge because rustls does not expose allocation
//! capacities for all private protocol and cryptographic-provider state. Shared
//! [`rustls::ClientConfig`] ownership and operating-system socket buffers remain
//! outside the per-connection charge.
#![doc = include_str!("sizing.md")]
mod application;
mod config;
mod config_display;
mod connector;
mod diagnostic;
mod establish;
mod limited;
mod port;
mod source;
mod tls_io;
mod transport;

pub use config::{
    RustlsConfigError, RustlsConnectError, RustlsTransportConfig, RustlsTransportLimits,
    RustlsTransportLimitsError,
};
pub use connector::RustlsConnector;
pub use diagnostic::RustlsDiagnostic;
pub use transport::RustlsTransport;
