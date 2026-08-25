//! One-operation raw TLS ingress or egress progression.

use std::{io, num::NonZeroUsize};

use bornera::{TransportBudget, TransportError, TransportFailurePhase, TransportProgress};
use calandria::Readiness;

use crate::{
    diagnostic::truncated_error,
    limited::{LimitedReader, LimitedWriter},
    transport::{EofState, RustlsTransport, TlsPreference},
};

impl RustlsTransport {
    pub(crate) fn drive_tls_once(
        &mut self,
        budget: TransportBudget,
        read_phase: TransportFailurePhase,
        write_phase: TransportFailurePhase,
    ) -> Result<TransportProgress, TransportError> {
        if let Some(error) = self.take_pending_error() {
            return Err(error);
        }
        if self.eof == EofState::Truncated && self.plaintext_bytes == 0 {
            return Err(truncated_error(read_phase));
        }
        let read = self.can_tls_read();
        let write = self.can_tls_write();
        match (self.preference, read, write) {
            (TlsPreference::Read, true, _) | (_, true, false) => {
                self.drive_tls_read(budget, read_phase)
            }
            (TlsPreference::Write, _, true) | (_, false, true) => {
                self.drive_tls_write(budget, write_phase)
            }
            _ => Ok(TransportProgress::IDLE),
        }
    }

    pub(crate) fn drive_tls_write(
        &mut self,
        budget: TransportBudget,
        phase: TransportFailurePhase,
    ) -> Result<TransportProgress, TransportError> {
        let mut writer = LimitedWriter::new(&mut self.stream, budget.write_bytes().get());
        let written = match self.tls.write_tls(&mut writer) {
            Ok(0) => {
                return Err(TransportError::from_io(
                    phase,
                    io::Error::from(io::ErrorKind::WriteZero),
                ));
            }
            Ok(written) => written,
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                self.clear_raw_write();
                return Ok(TransportProgress::operation());
            }
            Err(source) => return Err(TransportError::from_io(phase, source)),
        };
        self.preference = TlsPreference::Read;
        self.refresh_state(phase)?;
        Ok(TransportProgress::new(NonZeroUsize::MIN, 0, written))
    }

    fn drive_tls_read(
        &mut self,
        budget: TransportBudget,
        phase: TransportFailurePhase,
    ) -> Result<TransportProgress, TransportError> {
        let mut reader = LimitedReader::new(&mut self.stream, budget.read_bytes().get());
        let read = match self.tls.read_tls(&mut reader) {
            Ok(read) => read,
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                self.clear_raw_read();
                return Ok(TransportProgress::operation());
            }
            Err(source) => return Err(TransportError::from_io(phase, source)),
        };
        if read == 0 {
            self.clear_raw_read();
            self.eof = if self.eof == EofState::Clean {
                EofState::Clean
            } else {
                EofState::Truncated
            };
            if self.plaintext_bytes == 0 && self.eof == EofState::Truncated {
                return Err(truncated_error(phase));
            }
            return Ok(TransportProgress::operation());
        }
        self.refresh_state(phase)?;
        self.preference = TlsPreference::Write;
        Ok(TransportProgress::new(NonZeroUsize::MIN, read, 0))
    }

    pub(crate) fn can_tls_progress(&self) -> bool {
        (self.eof == EofState::Truncated && self.plaintext_bytes == 0)
            || self.pending_error.is_some()
            || self.can_tls_read()
            || self.can_tls_write()
    }

    pub(crate) fn can_tls_read(&self) -> bool {
        self.eof == EofState::Live
            && self.tls.wants_read()
            && self.readiness.intersects(
                Readiness::READABLE
                    .union(Readiness::READ_CLOSED)
                    .union(Readiness::ERROR),
            )
    }

    pub(crate) fn can_tls_write(&self) -> bool {
        self.tls.wants_write()
            && self.readiness.intersects(
                Readiness::WRITABLE
                    .union(Readiness::WRITE_CLOSED)
                    .union(Readiness::ERROR),
            )
    }

    pub(crate) fn clear_raw_read(&mut self) {
        self.readiness = self.readiness.remove(
            Readiness::READABLE
                .union(Readiness::READ_CLOSED)
                .union(Readiness::ERROR),
        );
    }

    pub(crate) fn clear_raw_write(&mut self) {
        self.readiness = self.readiness.remove(
            Readiness::WRITABLE
                .union(Readiness::WRITE_CLOSED)
                .union(Readiness::ERROR),
        );
    }
}
