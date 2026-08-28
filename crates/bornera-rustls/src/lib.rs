//! Bounded rustls transports and socket-free server sessions for Bornera owners.
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
//! [`RustlsServerSession`] applies the same limits to a caller-driven
//! [`rustls::ServerConnection`]. It owns no socket, readiness source, clock, task,
//! or runtime: an external connection owner supplies bounded ciphertext slices,
//! drains bounded TLS output, and decides when those bytes reach its transport.
#![doc = include_str!("sizing.md")]
mod application;
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
