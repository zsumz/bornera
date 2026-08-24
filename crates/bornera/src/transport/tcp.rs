//! Native Mio TCP capability and explicit nonblocking connect lifecycle.

use std::{io, net::SocketAddr};

use calandria::{Interest, Readiness};
use mio::net::TcpStream;
use socket2::{SockRef, TcpKeepalive};

use crate::{
    TcpNoDelay, TcpSocketPolicy, TransportError, TransportFailurePhase, TransportProgress,
};

/// Native nonblocking TCP transport registered by a [`crate::ConnectionSet`].
#[derive(Debug)]
pub struct TcpTransport {
    pub(super) stream: TcpStream,
    pub(super) phase: TransportPhase,
    pub(super) readiness: Readiness,
}

impl TcpTransport {
    /// Initiates one exact nonblocking TCP connection attempt.
    pub fn connect(address: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            stream: TcpStream::connect(address)?,
            phase: TransportPhase::Connecting,
            readiness: Readiness::EMPTY,
        })
    }

    pub(crate) fn observe(&mut self, readiness: Readiness) {
        self.readiness |= readiness;
    }

    pub(super) fn complete_connect(&mut self) -> Result<TransportProgress, TransportError> {
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

    pub(super) fn apply_no_delay(
        &mut self,
        policy: TcpSocketPolicy,
    ) -> Result<TransportProgress, TransportError> {
        self.stream
            .set_nodelay(policy.no_delay() == TcpNoDelay::Enabled)
            .map_err(|source| {
                TransportError::from_io(TransportFailurePhase::SocketPolicy, source)
            })?;
        if policy.keepalive_policy().is_some() {
            self.phase = TransportPhase::Keepalive;
        } else {
            self.phase = TransportPhase::Open;
        }
        Ok(TransportProgress::operation())
    }

    pub(super) fn apply_keepalive(
        &mut self,
        policy: TcpSocketPolicy,
    ) -> Result<TransportProgress, TransportError> {
        let Some(keepalive) = policy.keepalive_policy() else {
            self.phase = TransportPhase::Open;
            return Ok(TransportProgress::operation());
        };
        let settings = TcpKeepalive::new().with_time(keepalive.idle().as_duration());
        SockRef::from(&self.stream)
            .set_tcp_keepalive(&settings)
            .map_err(|source| {
                TransportError::from_io(TransportFailurePhase::SocketPolicy, source)
            })?;
        self.phase = TransportPhase::Open;
        Ok(TransportProgress::operation())
    }

    pub(crate) fn can_establish(&self) -> bool {
        match self.phase {
            TransportPhase::Connecting => self.readiness.intersects(
                Readiness::WRITABLE
                    .union(Readiness::ERROR)
                    .union(Readiness::READ_CLOSED)
                    .union(Readiness::WRITE_CLOSED),
            ),
            TransportPhase::NoDelay | TransportPhase::Keepalive => true,
            TransportPhase::Open => false,
        }
    }

    pub(crate) fn is_open(&self) -> bool {
        self.phase == TransportPhase::Open
    }

    pub(crate) fn can_read(&self) -> bool {
        self.is_open()
            && self.readiness.intersects(
                Readiness::READABLE
                    .union(Readiness::READ_CLOSED)
                    .union(Readiness::ERROR),
            )
    }

    pub(crate) fn can_write(&self) -> bool {
        self.is_open()
            && self.readiness.intersects(
                Readiness::WRITABLE
                    .union(Readiness::WRITE_CLOSED)
                    .union(Readiness::ERROR),
            )
    }

    pub(crate) fn clear_read(&mut self) {
        self.readiness = self.readiness.remove(
            Readiness::READABLE
                .union(Readiness::READ_CLOSED)
                .union(Readiness::ERROR),
        );
    }

    pub(crate) fn clear_write(&mut self) {
        self.readiness = self.readiness.remove(
            Readiness::WRITABLE
                .union(Readiness::WRITE_CLOSED)
                .union(Readiness::ERROR),
        );
    }

    pub(crate) fn clear_connect(&mut self) {
        self.readiness = self.readiness.remove(
            Readiness::WRITABLE
                .union(Readiness::ERROR)
                .union(Readiness::READ_CLOSED)
                .union(Readiness::WRITE_CLOSED),
        );
    }

    pub(crate) fn desired_interest(&self, has_writes: bool) -> Interest {
        if self.phase == TransportPhase::Connecting || has_writes {
            Interest::READ_WRITE
        } else {
            Interest::READABLE
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TransportPhase {
    Connecting,
    NoDelay,
    Keepalive,
    Open,
}
