//! Bounded rustls transports and socket-free TLS sessions for Bornera owners.
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
//!
//! [`RustlsClientSession`] and [`RustlsServerSession`] apply the same limits to
//! caller-driven rustls state. They own no socket, readiness source, clock, task,
//! or runtime: an external connection owner supplies bounded ciphertext slices,
//! drains bounded TLS output, and decides when those bytes reach its transport.
#![doc = include_str!("sizing.md")]
mod application;
mod client_config;
mod client_io;
mod client_session;
mod client_status;
mod config;
mod config_display;
mod connector;
mod diagnostic;
mod establish;
mod limited;
mod port;
mod server_config;
mod server_io;
mod server_session;
mod server_status;
mod source;
mod tls_io;
mod transport;

pub use client_config::RustlsClientSessionError;
pub use client_session::RustlsClientSession;
pub use client_status::RustlsClientStatus;
pub use config::{
    RustlsConfigError, RustlsConnectError, RustlsTransportConfig, RustlsTransportLimits,
    RustlsTransportLimitsError,
};
pub use connector::RustlsConnector;
pub use diagnostic::RustlsDiagnostic;
pub use server_config::{RustlsServerConfig, RustlsServerSessionError};
pub use server_session::RustlsServerSession;
pub use server_status::{RustlsPeerClosure, RustlsServerStatus};
pub use transport::RustlsTransport;
