//! Retained-byte enforcement around one protocol-owned decoder.

use core::fmt;

use calandria::RetainedBytes;

use super::{FrameDecodeError, FrameDecoder};

/// Bounded, terminal-on-malformation driver for one incremental decoder.
pub struct FrameDriver<D> {
    decoder: D,
    max_retained_bytes: RetainedBytes,
    failed: bool,
}

impl<D: FrameDecoder> FrameDriver<D> {
    /// Wraps a decoder if its initial retained memory is within the bound.
    pub fn new(
        decoder: D,
        max_retained_bytes: RetainedBytes,
    ) -> Result<Self, FrameDecodeError<D::Error>> {
        let retained = decoder.retained_bytes();
        if retained > max_retained_bytes {
            return Err(FrameDecodeError::RetainedContractViolation {
                retained,
                limit: max_retained_bytes,
            });
        }
        Ok(Self {
            decoder,
            max_retained_bytes,
            failed: false,
        })
    }

    /// Admits a borrowed byte chunk or leaves it entirely with the caller.
    pub fn feed(&mut self, bytes: &[u8]) -> Result<(), FrameDecodeError<D::Error>> {
        if self.failed {
            return Err(FrameDecodeError::DecoderFailed);
        }
        let incoming = RetainedBytes::try_from(bytes.len())
            .map_err(|_| FrameDecodeError::InputSizeOverflow)?;
        let retained = self.decoder.retained_bytes();
        let Some(accepted) = retained.checked_add(incoming) else {
            return Err(FrameDecodeError::RetainedByteCapacity {
                retained,
                incoming: bytes.len(),
                limit: self.max_retained_bytes,
            });
        };
        if accepted > self.max_retained_bytes {
            return Err(FrameDecodeError::RetainedByteCapacity {
                retained,
                incoming: bytes.len(),
                limit: self.max_retained_bytes,
            });
        }
        if let Err(error) = self.decoder.feed(bytes) {
            self.failed = true;
            return Err(FrameDecodeError::Decoder(error));
        }
        self.validate_retained()
    }

    /// Extracts at most one complete opaque frame.
    pub fn next_frame(&mut self) -> Result<Option<D::Frame>, FrameDecodeError<D::Error>> {
        if self.failed {
            return Err(FrameDecodeError::DecoderFailed);
        }
        let frame = match self.decoder.next_frame() {
            Ok(frame) => frame,
            Err(error) => {
                self.failed = true;
                return Err(FrameDecodeError::Decoder(error));
            }
        };
        self.validate_retained()?;
        Ok(frame)
    }

    /// Returns variable memory retained by the adapter decoder.
    pub fn retained_bytes(&self) -> RetainedBytes {
        self.decoder.retained_bytes()
    }

    /// Returns whether a terminal adapter or accounting failure occurred.
    pub const fn is_failed(&self) -> bool {
        self.failed
    }

    /// Recovers the protocol-owned decoder.
    pub fn into_decoder(self) -> D {
        self.decoder
    }

    fn validate_retained(&mut self) -> Result<(), FrameDecodeError<D::Error>> {
        let retained = self.decoder.retained_bytes();
        if retained > self.max_retained_bytes {
            self.failed = true;
            return Err(FrameDecodeError::RetainedContractViolation {
                retained,
                limit: self.max_retained_bytes,
            });
        }
        Ok(())
    }
}

impl<D: FrameDecoder> fmt::Debug for FrameDriver<D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FrameDriver")
            .field("max_retained_bytes", &self.max_retained_bytes)
            .field("retained_bytes", &self.decoder.retained_bytes())
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}
