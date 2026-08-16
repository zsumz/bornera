//! Narrow protocol-adapter port for incremental frame decoding.

use calandria::RetainedBytes;

/// Protocol-owned incremental decoding of complete inbound frames.
///
/// Implementations retain one bounded stream accumulation rather than an
/// unreported queue of decoded frames. Callers drain `next_frame` after each
/// accepted input before admitting more bytes.
pub trait FrameDecoder {
    /// Complete opaque frame emitted to inbound classification.
    type Frame;
    /// Protocol-specific terminal decoder error.
    type Error;

    /// Admits bytes into protocol-specific accumulation.
    fn feed(&mut self, bytes: &[u8]) -> Result<(), Self::Error>;

    /// Extracts at most one complete frame while retaining later bytes.
    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error>;

    /// Reports all variable memory currently retained by the decoder.
    fn retained_bytes(&self) -> RetainedBytes;
}
