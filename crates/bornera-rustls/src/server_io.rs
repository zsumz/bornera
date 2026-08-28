//! Caller-driven encrypted and plaintext progression for server sessions.

use std::io::{self, Read, Write};

use bornera::{TransportError, TransportFailurePhase};

use crate::{
    RustlsPeerClosure, RustlsServerSession,
    diagnostic::{caller_state_error, truncated_error},
};

impl RustlsServerSession {
    /// Copies caller-owned ciphertext into rustls and returns its exact consumed count.
    pub fn ingest_tls(&mut self, ciphertext: &[u8]) -> Result<usize, TransportError> {
        if ciphertext.is_empty() {
            return Ok(0);
        }
        self.ensure_progressing()?;
        if self.peer_closure != RustlsPeerClosure::Open {
            return Err(caller_state_error(
                self.read_phase(),
                io::ErrorKind::InvalidInput,
            ));
        }
        let length = ciphertext.len().min(self.ingress_limit());
        let mut source = &ciphertext[..length];
        let phase = self.read_phase();
        let read = self
            .tls
            .read_tls(&mut source)
            .map_err(|error| TransportError::from_io(phase, error))?;
        if let Err(error) = self.refresh(phase) {
            return Err(self.latch(error.diagnostic()));
        }
        Ok(read)
    }

    /// Reads authenticated plaintext and returns the exact copied count.
    pub fn read_plaintext(&mut self, output: &mut [u8]) -> Result<usize, TransportError> {
        if output.is_empty() {
            return Ok(0);
        }
        self.ensure_progressing()?;
        if self.peer_closure == RustlsPeerClosure::Truncated && self.plaintext_bytes == 0 {
            let error = truncated_error(self.read_phase());
            return Err(self.latch(error.diagnostic()));
        }
        match self.tls.reader().read(output) {
            Ok(read) => {
                self.plaintext_bytes = self.plaintext_bytes.saturating_sub(read);
                Ok(read)
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                self.peer_closure = RustlsPeerClosure::Truncated;
                let error = truncated_error(self.read_phase());
                Err(self.latch(error.diagnostic()))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Err(caller_state_error(
                TransportFailurePhase::Read,
                io::ErrorKind::WouldBlock,
            )),
            Err(error) => {
                let error = TransportError::from_io(TransportFailurePhase::Read, error);
                Err(self.latch(error.diagnostic()))
            }
        }
    }

    /// Irreversibly accepts bounded response plaintext and returns its exact count.
    pub fn write_plaintext(&mut self, plaintext: &[u8]) -> Result<usize, TransportError> {
        if plaintext.is_empty() {
            return Ok(0);
        }
        self.ensure_progressing()?;
        if !self.opened || self.close_notify_sent {
            return Err(caller_state_error(
                TransportFailurePhase::Write,
                io::ErrorKind::InvalidInput,
            ));
        }
        let written = self
            .tls
            .writer()
            .write(plaintext)
            .map_err(|error| TransportError::from_io(TransportFailurePhase::Write, error))?;
        if let Err(error) = self.refresh(TransportFailurePhase::Write) {
            self.record_failure(error.diagnostic());
        }
        Ok(written)
    }

    /// Copies pending TLS records into a caller buffer and returns the exact produced count.
    pub fn drain_tls(&mut self, output: &mut [u8]) -> Result<usize, TransportError> {
        if output.is_empty() || !self.tls.wants_write() {
            return Ok(0);
        }
        let phase = self.write_phase();
        let mut sink = output;
        let written = self
            .tls
            .write_tls(&mut sink)
            .map_err(|error| TransportError::from_io(phase, error))?;
        if self.failure.is_none() {
            if let Err(error) = self.refresh(phase) {
                self.record_failure(error.diagnostic());
            }
        } else {
            self.tls_egress_bytes = self.tls_egress_bytes.saturating_sub(written);
        }
        Ok(written)
    }

    /// Records raw input EOF, deferring truncation until authenticated plaintext drains.
    pub fn finish_input(&mut self) -> Result<(), TransportError> {
        self.ensure_progressing()?;
        if self.peer_closure == RustlsPeerClosure::Clean {
            return Ok(());
        }
        if self.peer_closure == RustlsPeerClosure::Open {
            let mut eof = io::empty();
            let phase = self.read_phase();
            let _eof = self
                .tls
                .read_tls(&mut eof)
                .map_err(|error| TransportError::from_io(phase, error))?;
            if let Err(error) = self.refresh(phase) {
                return Err(self.latch(error.diagnostic()));
            }
            if self.peer_closure != RustlsPeerClosure::Clean {
                self.peer_closure = RustlsPeerClosure::Truncated;
            }
        }
        if self.plaintext_bytes == 0 {
            let error = truncated_error(self.read_phase());
            return Err(self.latch(error.diagnostic()));
        }
        Ok(())
    }

    /// Starts graceful local TLS shutdown; repeated calls are idempotent.
    pub fn send_close_notify(&mut self) -> Result<(), TransportError> {
        self.ensure_progressing()?;
        if self.close_notify_sent {
            return Ok(());
        }
        if !self.opened {
            return Err(caller_state_error(
                TransportFailurePhase::Shutdown,
                io::ErrorKind::InvalidInput,
            ));
        }
        self.tls.send_close_notify();
        self.close_notify_sent = true;
        match self.refresh(TransportFailurePhase::Shutdown) {
            Ok(()) => Ok(()),
            Err(error) => Err(self.latch(error.diagnostic())),
        }
    }
}
