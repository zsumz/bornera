//! TCP completion, socket policy, and transition into TLS handshaking.

use std::io;

use bornera::{
    TcpNoDelay, TcpSocketPolicy, TransportError, TransportFailurePhase, TransportProgress,
};
use calandria::Readiness;
use socket2::{SockRef, TcpKeepalive};

use crate::transport::{RustlsTransport, TransportPhase};

impl RustlsTransport {
    pub(crate) fn complete_connect(&mut self) -> Result<TransportProgress, TransportError> {
        if let Some(source) = self
            .stream
            .take_error()
            .map_err(|source| TransportError::from_io(TransportFailurePhase::Connect, source))?
        {
            return Err(TransportError::from_io(
                TransportFailurePhase::Connect,
                source,
            ));
        }
        match self.stream.peer_addr() {
            Ok(_) => {
                self.phase = TransportPhase::NoDelay;
                self.readiness = self.readiness.remove(Readiness::ERROR);
                Ok(TransportProgress::operation())
            }
            Err(source) if source.kind() == io::ErrorKind::NotConnected => {
                self.clear_connect();
                Ok(TransportProgress::operation())
            }
            Err(source) => Err(TransportError::from_io(
                TransportFailurePhase::Connect,
                source,
            )),
        }
    }

    pub(crate) fn apply_no_delay(
        &mut self,
        policy: TcpSocketPolicy,
    ) -> Result<TransportProgress, TransportError> {
        self.stream
            .set_nodelay(policy.no_delay() == TcpNoDelay::Enabled)
            .map_err(|source| {
                TransportError::from_io(TransportFailurePhase::SocketPolicy, source)
            })?;
        self.phase = if policy.keepalive_policy().is_some() {
            TransportPhase::Keepalive
        } else {
            TransportPhase::Handshaking
        };
        Ok(TransportProgress::operation())
    }

    pub(crate) fn apply_keepalive(
        &mut self,
        policy: TcpSocketPolicy,
    ) -> Result<TransportProgress, TransportError> {
        let Some(keepalive) = policy.keepalive_policy() else {
            self.phase = TransportPhase::Handshaking;
            return Ok(TransportProgress::operation());
        };
        let settings = TcpKeepalive::new().with_time(keepalive.idle().as_duration());
        SockRef::from(&self.stream)
            .set_tcp_keepalive(&settings)
            .map_err(|source| {
                TransportError::from_io(TransportFailurePhase::SocketPolicy, source)
            })?;
        self.phase = TransportPhase::Handshaking;
        Ok(TransportProgress::operation())
    }

    pub(crate) fn can_connect(&self) -> bool {
        self.readiness.intersects(
            Readiness::WRITABLE
                .union(Readiness::ERROR)
                .union(Readiness::READ_CLOSED)
                .union(Readiness::WRITE_CLOSED),
        )
    }

    pub(crate) fn clear_connect(&mut self) {
        self.readiness = self.readiness.remove(
            Readiness::WRITABLE
                .union(Readiness::ERROR)
                .union(Readiness::READ_CLOSED)
                .union(Readiness::WRITE_CLOSED),
        );
    }
}
