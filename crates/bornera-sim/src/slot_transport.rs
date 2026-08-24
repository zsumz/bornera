//! Deterministic in-memory implementation of Bornera's production transport port.

use std::{collections::VecDeque, io};

use bornera::{
    SlotTransport, TcpSocketPolicy, TransportBudget, TransportError, TransportFailurePhase,
    TransportProgress,
};
use calandria::Interest;

#[derive(Debug)]
pub(crate) struct SimTransport {
    phase: Phase,
    connect_ready: bool,
    inbound: VecDeque<u8>,
    peer_closed: bool,
    write_credit: usize,
    outbound: Vec<u8>,
    applied_policy: Option<TcpSocketPolicy>,
}

impl SimTransport {
    pub(crate) fn new() -> Self {
        Self {
            phase: Phase::Connecting,
            connect_ready: false,
            inbound: VecDeque::new(),
            peer_closed: false,
            write_credit: 0,
            outbound: Vec::new(),
            applied_policy: None,
        }
    }

    pub(crate) fn observe_connect_ready(&mut self) {
        self.connect_ready = true;
    }

    pub(crate) fn inject_read(&mut self, bytes: Vec<u8>) {
        self.inbound.extend(bytes);
    }

    pub(crate) fn observe_peer_closed(&mut self) {
        self.peer_closed = true;
    }

    pub(crate) fn allow_write(&mut self, bytes: usize) {
        self.write_credit = bytes;
    }

    pub(crate) fn close(&mut self) {
        self.phase = Phase::Closed;
        self.connect_ready = false;
        self.inbound.clear();
        self.write_credit = 0;
    }

    pub(crate) fn outbound(&self) -> &[u8] {
        &self.outbound
    }

    pub(crate) const fn applied_policy(&self) -> Option<TcpSocketPolicy> {
        self.applied_policy
    }
}

impl io::Read for SimTransport {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.phase != Phase::Open {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "simulated transport closed",
            ));
        }
        let count = buffer.len().min(self.inbound.len());
        for destination in buffer.iter_mut().take(count) {
            let Some(byte) = self.inbound.pop_front() else {
                return Err(io::Error::other("simulated inbound accounting diverged"));
            };
            *destination = byte;
        }
        if count != 0 {
            return Ok(count);
        }
        if self.peer_closed {
            return Ok(0);
        }
        Err(io::Error::from(io::ErrorKind::WouldBlock))
    }
}

impl io::Write for SimTransport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.phase != Phase::Open {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "simulated transport closed",
            ));
        }
        let written = buffer.len().min(self.write_credit);
        if written == 0 {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        self.outbound.extend_from_slice(&buffer[..written]);
        self.write_credit = self.write_credit.saturating_sub(written);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SlotTransport for SimTransport {
    fn drive_establishment(
        &mut self,
        policy: TcpSocketPolicy,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        match self.phase {
            Phase::Open => Ok(TransportProgress::IDLE),
            Phase::Closed => Err(TransportError::from_io(
                TransportFailurePhase::Connect,
                io::Error::new(io::ErrorKind::NotConnected, "simulated transport closed"),
            )),
            Phase::Connecting if self.connect_ready => {
                self.phase = Phase::Open;
                self.connect_ready = false;
                self.applied_policy = Some(policy);
                Ok(TransportProgress::operation())
            }
            Phase::Connecting => Ok(TransportProgress::operation()),
        }
    }

    fn drive_transport(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        Ok(TransportProgress::IDLE)
    }

    fn can_establish(&self) -> bool {
        self.phase == Phase::Connecting && self.connect_ready
    }

    fn has_transport_work(&self) -> bool {
        false
    }

    fn is_open(&self) -> bool {
        self.phase == Phase::Open
    }

    fn can_read(&self) -> bool {
        self.phase == Phase::Open && (!self.inbound.is_empty() || self.peer_closed)
    }

    fn can_write(&self) -> bool {
        self.phase == Phase::Open && self.write_credit != 0
    }

    fn desired_interest(&self, has_writes: bool) -> Interest {
        if self.phase == Phase::Connecting || has_writes {
            Interest::READ_WRITE
        } else {
            Interest::READABLE
        }
    }

    fn clear_read(&mut self) {}

    fn clear_write(&mut self) {
        self.write_credit = 0;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Connecting,
    Open,
    Closed,
}
