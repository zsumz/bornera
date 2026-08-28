//! Socket-free bounded rustls server progression driven by an external owner.

use bornera::{
    TransportDiagnostic, TransportError, TransportFailurePhase, TransportLimits, TransportPressure,
};
use rustls::ServerConnection;

use crate::{
    RustlsPeerClosure, RustlsServerConfig, RustlsServerSessionError, RustlsServerStatus,
    RustlsTransportLimits,
    diagnostic::protocol_error,
    server_status::{
        STATUS_CLOSE_NOTIFY_SENT, STATUS_HANDSHAKING, STATUS_OPEN, STATUS_WANTS_READ,
        STATUS_WANTS_WRITE,
    },
};

/// One bounded rustls server connection without a socket, clock, task, or runtime.
#[derive(Debug)]
pub struct RustlsServerSession {
    pub(crate) tls: ServerConnection,
    pub(crate) limits: RustlsTransportLimits,
    pub(crate) plaintext_bytes: usize,
    pub(crate) tls_egress_bytes: usize,
    pub(crate) peer_closure: RustlsPeerClosure,
    pub(crate) opened: bool,
    pub(crate) close_notify_sent: bool,
    pub(crate) failure: Option<TransportDiagnostic>,
}

impl RustlsServerSession {
    /// Constructs a session only when its stable charge fits the supplied ceiling.
    pub fn new(
        config: &RustlsServerConfig,
        supplied: TransportLimits,
    ) -> Result<Self, RustlsServerSessionError> {
        let limits = config.limits();
        let required = limits.transport_limits().retained_bytes();
        if required > supplied.retained_bytes() {
            return Err(RustlsServerSessionError::Capacity {
                required,
                supplied: supplied.retained_bytes(),
            });
        }
        let mut tls = ServerConnection::new(config.server_config().clone())
            .map_err(RustlsServerSessionError::Tls)?;
        tls.set_buffer_limit(Some(limits.application_write_buffer_bytes().get()));
        let state = tls
            .process_new_packets()
            .map_err(RustlsServerSessionError::Tls)?;
        limits
            .validate_state(&state, TransportFailurePhase::Establishment)
            .map_err(RustlsServerSessionError::Transport)?;
        Ok(Self {
            plaintext_bytes: state.plaintext_bytes_to_read(),
            tls_egress_bytes: state.tls_bytes_to_write(),
            tls,
            limits,
            peer_closure: RustlsPeerClosure::Open,
            opened: false,
            close_notify_sent: false,
            failure: None,
        })
    }

    /// Returns the exact externally actionable session state.
    pub fn status(&self) -> RustlsServerStatus {
        let failed = self.failure.is_some();
        let wants_read =
            self.tls.wants_read() && self.peer_closure == RustlsPeerClosure::Open && !failed;
        let flags = (u8::from(self.tls.is_handshaking()) * STATUS_HANDSHAKING)
            | (u8::from(self.opened && !self.close_notify_sent && !failed) * STATUS_OPEN)
            | (u8::from(wants_read) * STATUS_WANTS_READ)
            | (u8::from(self.tls.wants_write()) * STATUS_WANTS_WRITE)
            | (u8::from(self.close_notify_sent) * STATUS_CLOSE_NOTIFY_SENT);
        RustlsServerStatus {
            flags,
            readable_plaintext_bytes: self.plaintext_bytes,
            tls_egress_bytes: self.tls_egress_bytes,
            peer_closure: self.peer_closure,
            failure: self.failure,
        }
    }

    /// Returns the exact stable charge reserved for this session.
    pub const fn pressure(&self) -> TransportPressure {
        self.limits.pressure()
    }

    /// Returns the logical and accounting limits enforced by this session.
    pub const fn limits(&self) -> RustlsTransportLimits {
        self.limits
    }

    /// Returns the negotiated ALPN identifier without copying it.
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.tls.alpn_protocol()
    }

    /// Returns the offered `ClientHello` SNI value without copying it.
    pub fn server_name(&self) -> Option<&str> {
        self.tls.server_name()
    }

    pub(crate) fn refresh(&mut self, phase: TransportFailurePhase) -> Result<(), TransportError> {
        let state = self
            .tls
            .process_new_packets()
            .map_err(|error| protocol_error(phase, &error))?;
        self.limits
            .validate_state(&state, phase)
            .map_err(TransportError::new)?;
        self.plaintext_bytes = state.plaintext_bytes_to_read();
        self.tls_egress_bytes = state.tls_bytes_to_write();
        if state.peer_has_closed() {
            self.peer_closure = RustlsPeerClosure::Clean;
        }
        if !self.tls.is_handshaking() && !self.tls.wants_write() {
            self.opened = true;
        }
        Ok(())
    }

    pub(crate) fn ensure_progressing(&self) -> Result<(), TransportError> {
        match self.failure {
            Some(diagnostic) => Err(TransportError::new(diagnostic)),
            None => Ok(()),
        }
    }

    pub(crate) fn latch(&mut self, diagnostic: TransportDiagnostic) -> TransportError {
        self.record_failure(diagnostic);
        TransportError::new(diagnostic)
    }

    pub(crate) fn record_failure(&mut self, diagnostic: TransportDiagnostic) {
        self.failure.get_or_insert(diagnostic);
    }

    pub(crate) fn read_phase(&self) -> TransportFailurePhase {
        if self.tls.is_handshaking() {
            TransportFailurePhase::Establishment
        } else {
            TransportFailurePhase::TransportRead
        }
    }

    pub(crate) fn write_phase(&self) -> TransportFailurePhase {
        if self.tls.is_handshaking() {
            TransportFailurePhase::Establishment
        } else if self.close_notify_sent {
            TransportFailurePhase::Shutdown
        } else {
            TransportFailurePhase::TransportWrite
        }
    }

    pub(crate) fn ingress_limit(&self) -> usize {
        match usize::try_from(self.limits.pressure().inbound().get()) {
            Ok(limit) => limit,
            Err(_) => usize::MAX,
        }
    }
}
