//! Private Mio TCP capability and explicit nonblocking connect lifecycle.

use std::{
    io::{self, Read, Write},
    net::SocketAddr,
};

use calandria::{Interest, Readiness};
use mio::{Registry, Token, event::Source, net::TcpStream};

use crate::TransportState;

#[derive(Debug)]
pub(crate) struct PlaintextTransport {
    stream: TcpStream,
    phase: TransportPhase,
    readiness: Readiness,
    interest: Interest,
}

impl PlaintextTransport {
    pub(crate) fn connect(address: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            stream: TcpStream::connect(address)?,
            phase: TransportPhase::Connecting,
            readiness: Readiness::EMPTY,
            interest: Interest::READ_WRITE,
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

    pub(crate) const fn interest(&self) -> Interest {
        self.interest
    }

    pub(crate) fn set_interest(&mut self, interest: Interest) {
        self.interest = interest;
    }

    pub(crate) const fn state(&self) -> TransportState {
        match self.phase {
            TransportPhase::Connecting => TransportState::Connecting,
            TransportPhase::Open => TransportState::Open,
        }
    }
}

impl Read for PlaintextTransport {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buffer)
    }
}

impl Write for PlaintextTransport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.stream.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl Source for PlaintextTransport {
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
pub(crate) enum ConnectProgress {
    Pending,
    Opened,
    AlreadyOpen,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransportPhase {
    Connecting,
    Open,
}
