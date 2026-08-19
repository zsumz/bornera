//! Complete outbound byte ownership with explicit retained-memory accounting.

use bornera_core::{RetainedBytes, WriteFrame};
use bytes::Bytes;

/// One complete contiguous frame prepared by a protocol adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboundFrame {
    bytes: Bytes,
    retained: RetainedBytes,
}

impl OutboundFrame {
    /// Wraps complete bytes when `retained` accounts for their full allocation.
    pub fn new(bytes: Bytes, retained: RetainedBytes) -> Result<Self, OutboundFrameError> {
        let visible =
            RetainedBytes::try_from(bytes.len()).map_err(|_| OutboundFrameError::LengthOverflow)?;
        if retained < visible {
            return Err(OutboundFrameError::Underreported { visible, retained });
        }
        Ok(Self { bytes, retained })
    }

    /// Copies one complete borrowed frame into exactly measured owned bytes.
    pub fn copy_from_slice(bytes: &[u8]) -> Result<Self, OutboundFrameError> {
        let retained =
            RetainedBytes::try_from(bytes.len()).map_err(|_| OutboundFrameError::LengthOverflow)?;
        Ok(Self {
            bytes: Bytes::copy_from_slice(bytes),
            retained,
        })
    }

    /// Borrows the complete transport bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Recovers the owned byte container.
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

impl WriteFrame for OutboundFrame {
    fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn retained_bytes(&self) -> RetainedBytes {
        self.retained
    }
}

/// Invalid retained-memory declaration for a complete outbound frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundFrameError {
    /// The platform frame length exceeded fixed-width byte accounting.
    LengthOverflow,
    /// Reported retained bytes were smaller than the visible byte slice.
    Underreported {
        /// Visible complete-frame bytes.
        visible: RetainedBytes,
        /// Adapter-reported retained allocation.
        retained: RetainedBytes,
    },
}

impl core::fmt::Display for OutboundFrameError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::LengthOverflow => "frame length exceeds fixed-width retained-byte accounting",
            Self::Underreported { .. } => "frame retained bytes are smaller than visible bytes",
        })
    }
}

impl core::error::Error for OutboundFrameError {}
