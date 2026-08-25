//! Application plaintext `Read` and irreversible-acceptance `Write` semantics.

use std::io::{self, Read, Write};

use crate::{RustlsTransport, transport::EofState};

impl Read for RustlsTransport {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.tls.reader().read(buffer)?;
        self.plaintext_bytes = self.plaintext_bytes.saturating_sub(read);
        Ok(read)
    }
}

impl Write for RustlsTransport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if !self.can_accept_plaintext() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        match self.tls.writer().write(buffer)? {
            0 => Err(io::ErrorKind::WouldBlock.into()),
            written => {
                if let Err(error) = self.refresh_state(bornera::TransportFailurePhase::Write) {
                    self.record_pending_error(error.diagnostic());
                }
                Ok(written)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.tls.wants_write() {
            Err(io::ErrorKind::WouldBlock.into())
        } else {
            Ok(())
        }
    }
}

impl RustlsTransport {
    pub(crate) fn can_read_plaintext(&self) -> bool {
        self.plaintext_bytes != 0 || self.eof == EofState::Clean
    }

    pub(crate) fn can_accept_plaintext(&self) -> bool {
        !self.tls.wants_write() && self.can_raw_write()
    }

    pub(crate) fn can_raw_write(&self) -> bool {
        self.readiness.intersects(
            calandria::Readiness::WRITABLE
                .union(calandria::Readiness::WRITE_CLOSED)
                .union(calandria::Readiness::ERROR),
        )
    }
}
