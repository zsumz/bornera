//! Standard I/O, slot, readiness, and Mio wiring for native TCP.

use std::io::{self, Read, Write};

use calandria::Readiness;
use mio::{Registry, Token, event::Source};

use super::{TcpTransport, tcp::TransportPhase};
use crate::{
    RegisteredTransport, SlotTransport, TcpSocketPolicy, TransportBudget, TransportError,
    TransportProgress,
};

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
    fn drive_establishment(
        &mut self,
        policy: TcpSocketPolicy,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        match self.phase {
            TransportPhase::Connecting => self.complete_connect(),
            TransportPhase::NoDelay => self.apply_no_delay(policy),
            TransportPhase::Keepalive => self.apply_keepalive(policy),
            TransportPhase::Open => Ok(TransportProgress::IDLE),
        }
    }

    fn drive_transport(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        Ok(TransportProgress::IDLE)
    }

    fn can_establish(&self) -> bool {
        Self::can_establish(self)
    }

    fn has_transport_work(&self) -> bool {
        false
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

    fn desired_interest(&self, has_writes: bool) -> calandria::Interest {
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
