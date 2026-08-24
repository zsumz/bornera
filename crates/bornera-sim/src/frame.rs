//! Immutable exact-length bytes for simulated writes and replies.

use bornera_core::{RetainedBytes, WriteFrame};

/// Immutable bytes with mechanically exact retained-memory accounting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimFrame {
    bytes: Box<[u8]>,
    retained: RetainedBytes,
}

impl SimFrame {
    /// Copies bytes into an exact-length immutable allocation.
    pub fn copy_from_slice(bytes: &[u8]) -> Result<Self, SimFrameError> {
        let retained = RetainedBytes::try_from(bytes.len()).map_err(|_| SimFrameError::TooLarge)?;
        Ok(Self {
            bytes: bytes.into(),
            retained,
        })
    }

    /// Borrows the complete immutable byte view.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the exact allocation size retained by this frame.
    pub const fn retained(&self) -> RetainedBytes {
        self.retained
    }
}

impl WriteFrame for SimFrame {
    fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn retained_bytes(&self) -> RetainedBytes {
        self.retained
    }
}

impl calandria::Retained for SimFrame {
    fn retained_bytes(&self) -> RetainedBytes {
        self.retained
    }
}

/// A simulated frame could not fit fixed-width retained-byte accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SimFrameError {
    /// The platform slice length exceeded the fixed-width byte domain.
    TooLarge,
}

impl core::fmt::Display for SimFrameError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("simulated frame exceeds fixed-width retained bytes")
    }
}

impl core::error::Error for SimFrameError {}
