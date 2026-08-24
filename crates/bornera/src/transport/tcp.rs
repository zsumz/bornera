//! Native Mio TCP capability and explicit nonblocking connect lifecycle.

use std::{
    io::{self, Read, Write},
    net::SocketAddr,
};

use calandria::{Interest, Readiness};
use mio::{Registry, Token, event::Source, net::TcpStream};
use socket2::{SockRef, TcpKeepalive};

use crate::{ConnectProgress, RegisteredTransport, SlotTransport, TcpNoDelay, TcpSocketPolicy};

/// Native nonblocking TCP transport registered by a [`crate::ConnectionSet`].
#[derive(Debug)]
pub struct TcpTransport {
    stream: TcpStream,
    phase: TransportPhase,
    readiness: Readiness,
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

    pub(crate) fn finish_connect(&mut self) -> io::Result<ConnectProgress> {
        if self.phase == TransportPhase::Open {
            return Ok(ConnectProgress::AlreadyOpen);
        }
        if let Some(source) = self.stream.take_error()? {
            return Err(source);
        }
        match self.stream.peer_addr() {
            Ok(_) => {
                self.phase = TransportPhase::Open;
                self.readiness = self.readiness.remove(Readiness::ERROR);
                Ok(ConnectProgress::Opened)
            }
            Err(source) if source.kind() == io::ErrorKind::NotConnected => {
                self.clear_connect();
                Ok(ConnectProgress::Pending)
            }
            Err(source) => Err(source),
        }
    }

    pub(crate) fn apply_policy(&self, policy: TcpSocketPolicy) -> io::Result<()> {
        self.stream
            .set_nodelay(policy.no_delay() == TcpNoDelay::Enabled)?;
        if let Some(keepalive) = policy.keepalive_policy() {
            let settings = TcpKeepalive::new().with_time(keepalive.idle().as_duration());
            SockRef::from(&self.stream).set_tcp_keepalive(&settings)?;
        }
        Ok(())
    }

    pub(crate) fn can_finish_connect(&self) -> bool {
        self.phase == TransportPhase::Connecting
            && self.readiness.intersects(
                Readiness::WRITABLE
                    .union(Readiness::ERROR)
                    .union(Readiness::READ_CLOSED)
                    .union(Readiness::WRITE_CLOSED),
            )
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

impl Read for TcpTransport {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buffer)
    }
}

impl Write for TcpTransport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.stream.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl SlotTransport for TcpTransport {
    fn finish_connect(&mut self) -> io::Result<ConnectProgress> {
        Self::finish_connect(self)
    }

    fn apply_policy(&mut self, policy: TcpSocketPolicy) -> io::Result<()> {
        Self::apply_policy(self, policy)
    }

    fn can_finish_connect(&self) -> bool {
        Self::can_finish_connect(self)
    }

    fn is_open(&self) -> bool {
        Self::is_open(self)
    }

    fn can_read(&self) -> bool {
        Self::can_read(self)
    }

    fn can_write(&self) -> bool {
        Self::can_write(self)
    }

    fn desired_interest(&self, has_writes: bool) -> Interest {
        Self::desired_interest(self, has_writes)
    }

    fn clear_read(&mut self) {
        Self::clear_read(self);
    }

    fn clear_write(&mut self) {
        Self::clear_write(self);
    }
}

impl RegisteredTransport for TcpTransport {
    fn observe_readiness(&mut self, readiness: Readiness) {
        self.observe(readiness);
    }
}

impl Source for TcpTransport {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.stream.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.stream.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        self.stream.deregister(registry)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransportPhase {
    Connecting,
    Open,
}
