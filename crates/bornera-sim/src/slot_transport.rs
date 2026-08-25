//! Deterministic in-memory implementation of Bornera's production transport port.

use std::{collections::VecDeque, io};

use bornera::{
    SlotTransport, TcpSocketPolicy, TransportBudget, TransportError, TransportFailurePhase,
    TransportLimits, TransportPressure, TransportProgress,
};
use calandria::Interest;

use crate::{Phase, ShutdownState, transport_pressure};

#[derive(Debug)]
pub(crate) struct SimTransport {
    phase: Phase,
    connect_ready: bool,
    inbound: VecDeque<u8>,
    peer_closed: bool,
    write_credit: usize,
    outbound: Vec<u8>,
    limits: TransportLimits,
    applied_policy: Option<TcpSocketPolicy>,
    shutdown: ShutdownState,
}

impl SimTransport {
    pub(crate) fn new(limits: TransportLimits) -> Result<Self, ()> {
        let total = usize::try_from(limits.retained_bytes().get()).map_err(|_| ())?;
        let inbound_limit = total / 2;
        let outbound_limit = total.saturating_sub(inbound_limit);
        let mut inbound = VecDeque::new();
        inbound.try_reserve_exact(inbound_limit).map_err(|_| ())?;
        let mut outbound = Vec::new();
        outbound.try_reserve_exact(outbound_limit).map_err(|_| ())?;
        let pressure = transport_pressure(&inbound, &outbound);
        if pressure.total() > limits.retained_bytes() {
            return Err(());
        }
        Ok(Self {
            phase: Phase::Connecting,
            connect_ready: false,
            inbound,
            peer_closed: false,
            write_credit: 0,
            outbound,
            limits: TransportLimits::new(pressure.total()),
            applied_policy: None,
            shutdown: ShutdownState::NotStarted,
        })
    }

    pub(crate) fn observe_connect_ready(&mut self) {
        self.connect_ready = true;
    }

    pub(crate) fn inject_read(&mut self, bytes: Vec<u8>) -> Result<(), ()> {
        if bytes.len() > self.inbound.capacity().saturating_sub(self.inbound.len()) {
            return Err(());
        }
        self.inbound.extend(bytes);
        Ok(())
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
        self.shutdown = ShutdownState::Complete;
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
        let available = self.outbound.capacity().saturating_sub(self.outbound.len());
        let written = buffer.len().min(self.write_credit).min(available);
        if written == 0 {
            if available == 0 && !buffer.is_empty() {
                return Err(io::Error::other(
                    "simulated transport output capacity exhausted",
                ));
            }
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
        if self.shutdown != ShutdownState::Flushing {
            return Ok(TransportProgress::IDLE);
        }
        self.shutdown = ShutdownState::Complete;
        Ok(TransportProgress::new(core::num::NonZeroUsize::MIN, 0, 1))
    }

    fn begin_shutdown(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        self.shutdown = ShutdownState::Flushing;
        Ok(TransportProgress::operation())
    }

    fn can_establish(&self) -> bool {
        self.phase == Phase::Connecting && self.connect_ready
    }

    fn has_transport_work(&self) -> bool {
        self.shutdown == ShutdownState::Flushing
    }

    fn is_shutdown_complete(&self) -> bool {
        self.shutdown == ShutdownState::Complete
    }

    fn is_open(&self) -> bool {
        self.phase == Phase::Open
    }

    fn can_read(&self) -> bool {
        self.phase == Phase::Open
            && self.shutdown == ShutdownState::NotStarted
            && (!self.inbound.is_empty() || self.peer_closed)
    }

    fn can_write(&self) -> bool {
        self.phase == Phase::Open
            && self.shutdown == ShutdownState::NotStarted
            && self.write_credit != 0
    }

    fn desired_interest(&self, has_writes: bool) -> Interest {
        if self.shutdown == ShutdownState::Flushing {
            Interest::WRITABLE
        } else if self.phase == Phase::Connecting || has_writes {
            Interest::READ_WRITE
        } else {
            Interest::READABLE
        }
    }

    fn pressure(&self) -> TransportPressure {
        transport_pressure(&self.inbound, &self.outbound)
    }

    fn pressure_limit(&self) -> TransportLimits {
        self.limits
    }

    fn clear_read(&mut self) {}

    fn clear_write(&mut self) {
        self.write_credit = 0;
    }
}
