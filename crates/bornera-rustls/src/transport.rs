//! Concrete nonblocking TCP and rustls client connection ownership.

use std::net::SocketAddr;

use bornera::{
    TransportDiagnostic, TransportError, TransportFailureKind, TransportFailurePhase,
    TransportLimits,
};
use calandria::Readiness;
use mio::net::TcpStream;
use rustls::ClientConnection;

use crate::{RustlsConnectError, RustlsTransportConfig};

/// One registered TCP source with a bounded rustls client connection.
#[derive(Debug)]
pub struct RustlsTransport {
    pub(crate) stream: TcpStream,
    pub(crate) tls: ClientConnection,
    pub(crate) phase: TransportPhase,
    pub(crate) readiness: Readiness,
    pub(crate) preference: TlsPreference,
    pub(crate) plaintext_bytes: usize,
    pub(crate) eof: EofState,
    pub(crate) shutdown: ShutdownState,
    pub(crate) limits: crate::RustlsTransportLimits,
    pub(crate) pending_error: Option<TransportDiagnostic>,
}

impl RustlsTransport {
    /// Initiates one exact nonblocking TCP and rustls client attempt.
    ///
    /// TLS construction and memory-capacity validation occur before socket acquisition.
    pub fn connect(
        address: SocketAddr,
        config: &RustlsTransportConfig,
        supplied: TransportLimits,
    ) -> Result<Self, RustlsConnectError> {
        let limits = config.limits();
        let required = limits.transport_limits().retained_bytes();
        if required > supplied.retained_bytes() {
            return Err(RustlsConnectError::Capacity {
                required,
                supplied: supplied.retained_bytes(),
            });
        }
        let mut tls =
            ClientConnection::new(config.client_config().clone(), config.server_name().clone())
                .map_err(RustlsConnectError::Tls)?;
        tls.set_buffer_limit(Some(limits.application_write_buffer_bytes().get()));
        let state = tls.process_new_packets().map_err(RustlsConnectError::Tls)?;
        limits
            .validate_state(&state, TransportFailurePhase::Establishment)
            .map_err(RustlsConnectError::Transport)?;
        let stream = TcpStream::connect(address).map_err(RustlsConnectError::Io)?;
        Ok(Self {
            stream,
            tls,
            phase: TransportPhase::Connecting,
            readiness: Readiness::EMPTY,
            preference: TlsPreference::Write,
            plaintext_bytes: state.plaintext_bytes_to_read(),
            eof: EofState::Live,
            shutdown: ShutdownState::NotStarted,
            limits,
            pending_error: None,
        })
    }

    pub(crate) fn observe(&mut self, readiness: Readiness) {
        self.readiness |= readiness;
    }

    pub(crate) fn refresh_state(
        &mut self,
        phase: TransportFailurePhase,
    ) -> Result<(), TransportError> {
        let state = self
            .tls
            .process_new_packets()
            .map_err(|error| crate::diagnostic::protocol_error(phase, &error))?;
        self.limits
            .validate_state(&state, phase)
            .map_err(TransportError::new)?;
        self.plaintext_bytes = state.plaintext_bytes_to_read();
        if state.peer_has_closed() {
            self.eof = EofState::Clean;
        }
        Ok(())
    }

    pub(crate) fn record_pending_error(&mut self, diagnostic: TransportDiagnostic) {
        self.pending_error.get_or_insert(diagnostic);
    }

    pub(crate) fn take_pending_error(&mut self) -> Option<TransportError> {
        self.pending_error.take().map(TransportError::new)
    }
}

impl crate::RustlsTransportLimits {
    pub(crate) fn validate_state(
        self,
        state: &rustls::IoState,
        phase: TransportFailurePhase,
    ) -> Result<(), TransportDiagnostic> {
        if state.tls_bytes_to_write() > self.max_tls_egress_bytes().get()
            || state.plaintext_bytes_to_read() > self.max_plaintext_bytes().get()
        {
            return Err(TransportDiagnostic::new(
                phase,
                TransportFailureKind::Capacity,
                std::io::ErrorKind::OutOfMemory,
                Some(crate::RustlsDiagnostic::Capacity.code()),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportPhase {
    Connecting,
    NoDelay,
    Keepalive,
    Handshaking,
    Open,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TlsPreference {
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EofState {
    Live,
    Clean,
    Truncated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShutdownState {
    NotStarted,
    Started,
}
