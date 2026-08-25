//! Bornera transport lifecycle and readiness contract implementation.

use bornera::{
    RegisteredTransport, SlotTransport, TcpSocketPolicy, TransportBudget, TransportError,
    TransportFailurePhase, TransportPressure, TransportProgress,
};
use calandria::{Interest, Readiness};

use crate::{
    RustlsTransport,
    transport::{EofState, ShutdownState, TransportPhase},
};

impl SlotTransport for RustlsTransport {
    fn drive_establishment(
        &mut self,
        policy: TcpSocketPolicy,
        budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        let progress = match self.phase {
            TransportPhase::Connecting => self.complete_connect(),
            TransportPhase::NoDelay => self.apply_no_delay(policy),
            TransportPhase::Keepalive => self.apply_keepalive(policy),
            TransportPhase::Handshaking => self.drive_tls_once(
                budget,
                TransportFailurePhase::Establishment,
                TransportFailurePhase::Establishment,
            ),
            TransportPhase::Open => Ok(TransportProgress::IDLE),
        }?;
        if self.phase == TransportPhase::Handshaking
            && !self.tls.is_handshaking()
            && !self.tls.wants_write()
        {
            self.phase = TransportPhase::Open;
        }
        Ok(progress)
    }

    fn drive_transport(
        &mut self,
        budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        if self.shutdown == ShutdownState::Started {
            return self.drive_tls_write(budget, TransportFailurePhase::Shutdown);
        }
        self.drive_tls_once(
            budget,
            TransportFailurePhase::TransportRead,
            TransportFailurePhase::TransportWrite,
        )
    }

    fn begin_shutdown(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        self.tls.send_close_notify();
        self.shutdown = ShutdownState::Started;
        self.refresh_state(TransportFailurePhase::Shutdown)?;
        Ok(TransportProgress::operation())
    }

    fn can_establish(&self) -> bool {
        match self.phase {
            TransportPhase::Connecting => self.can_connect(),
            TransportPhase::NoDelay | TransportPhase::Keepalive => true,
            TransportPhase::Handshaking => self.can_tls_progress(),
            TransportPhase::Open => false,
        }
    }

    fn has_transport_work(&self) -> bool {
        if self.shutdown == ShutdownState::Started {
            self.can_tls_write()
        } else {
            self.can_tls_progress()
        }
    }

    fn is_shutdown_complete(&self) -> bool {
        self.shutdown == ShutdownState::Started && !self.tls.wants_write()
    }

    fn is_open(&self) -> bool {
        self.phase == TransportPhase::Open
    }

    fn can_read(&self) -> bool {
        self.is_open() && self.can_read_plaintext()
    }

    fn can_write(&self) -> bool {
        self.is_open() && self.shutdown == ShutdownState::NotStarted && self.can_accept_plaintext()
    }

    fn desired_interest(&self, has_writes: bool) -> Interest {
        match self.phase {
            TransportPhase::Connecting | TransportPhase::NoDelay | TransportPhase::Keepalive => {
                Interest::READ_WRITE
            }
            TransportPhase::Handshaking | TransportPhase::Open => {
                let read = self.shutdown == ShutdownState::NotStarted
                    && self.eof == EofState::Live
                    && self.tls.wants_read();
                let write = self.tls.wants_write()
                    || (self.phase == TransportPhase::Open
                        && self.shutdown == ShutdownState::NotStarted
                        && has_writes);
                match (read, write) {
                    (true, true) => Interest::READ_WRITE,
                    (false, true) => Interest::WRITABLE,
                    _ => Interest::READABLE,
                }
            }
        }
    }

    fn pressure(&self) -> TransportPressure {
        self.limits.pressure()
    }

    fn pressure_limit(&self) -> bornera::TransportLimits {
        self.limits.transport_limits()
    }

    fn clear_read(&mut self) {
        self.clear_raw_read();
    }

    fn clear_write(&mut self) {
        self.clear_raw_write();
    }
}

impl RegisteredTransport for RustlsTransport {
    fn observe_readiness(&mut self, readiness: Readiness) {
        self.observe(readiness);
    }
}
