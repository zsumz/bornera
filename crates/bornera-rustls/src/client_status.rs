//! Data-only observations of a socket-free client session.

use bornera::TransportDiagnostic;

pub(crate) const STATUS_HANDSHAKING: u8 = 1 << 0;
pub(crate) const STATUS_OPEN: u8 = 1 << 1;
pub(crate) const STATUS_WANTS_READ: u8 = 1 << 2;
pub(crate) const STATUS_WANTS_WRITE: u8 = 1 << 3;
pub(crate) const STATUS_CLOSE_NOTIFY_SENT: u8 = 1 << 4;

/// Exact bounded state needed by an external client transport and lifecycle owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RustlsClientStatus {
    pub(crate) flags: u8,
    pub(crate) readable_plaintext_bytes: usize,
    pub(crate) tls_egress_bytes: usize,
    pub(crate) peer_closure: crate::RustlsPeerClosure,
    pub(crate) failure: Option<TransportDiagnostic>,
}

impl RustlsClientStatus {
    /// Returns whether rustls is still negotiating the initial handshake.
    pub const fn is_handshaking(self) -> bool {
        self.flags & STATUS_HANDSHAKING != 0
    }

    /// Returns whether handshake completion and required egress are externally visible.
    pub const fn is_open(self) -> bool {
        self.flags & STATUS_OPEN != 0
    }

    /// Returns whether further encrypted peer input can progress the session.
    pub const fn wants_read(self) -> bool {
        self.flags & STATUS_WANTS_READ != 0
    }

    /// Returns whether [`crate::RustlsClientSession::drain_tls`] can produce output.
    pub const fn wants_write(self) -> bool {
        self.flags & STATUS_WANTS_WRITE != 0
    }

    /// Returns authenticated application bytes immediately available to read.
    pub const fn readable_plaintext_bytes(self) -> usize {
        self.readable_plaintext_bytes
    }

    /// Returns encrypted output observed by the last successful rustls transition.
    pub const fn tls_egress_bytes(self) -> usize {
        self.tls_egress_bytes
    }

    /// Returns the authenticated or raw peer-input termination state.
    pub const fn peer_closure(self) -> crate::RustlsPeerClosure {
        self.peer_closure
    }

    /// Returns whether local graceful TLS shutdown has started.
    pub const fn close_notify_sent(self) -> bool {
        self.flags & STATUS_CLOSE_NOTIFY_SENT != 0
    }

    /// Returns the first fatal bounded diagnostic, if progression has stopped.
    pub const fn failure(self) -> Option<TransportDiagnostic> {
        self.failure
    }
}
