//! Fixed-width correlated request and reply framing for TLS integration tests.

use core::fmt;

use bornera::{FrameDecoder, InboundClassifier};
use bornera_core::MatchKey;
use calandria::{Retained, RetainedBytes};

pub(crate) const FRAME_BYTES: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Frame([u8; FRAME_BYTES]);

impl Frame {
    pub(crate) const fn from_bytes(bytes: [u8; FRAME_BYTES]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn bytes(&self) -> &[u8; FRAME_BYTES] {
        &self.0
    }
}

impl Retained for Frame {
    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::ZERO
    }
}

#[derive(Debug)]
pub(crate) struct Decoder {
    bytes: Vec<u8>,
}

impl Decoder {
    pub(crate) fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(FRAME_BYTES * 2),
        }
    }
}

impl FrameDecoder for Decoder {
    type Frame = Frame;
    type Error = DecodeError;

    fn feed(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        if self.bytes.len().saturating_add(bytes.len()) > self.bytes.capacity() {
            return Err(DecodeError);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error> {
        if self.bytes.len() < FRAME_BYTES {
            return Ok(None);
        }
        let mut frame = [0_u8; FRAME_BYTES];
        frame.copy_from_slice(&self.bytes[..FRAME_BYTES]);
        self.bytes.drain(..FRAME_BYTES);
        Ok(Some(Frame(frame)))
    }

    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::try_from(self.bytes.capacity()).unwrap_or(RetainedBytes::new(u64::MAX))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DecodeError;

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("test frame decoder capacity exceeded")
    }
}

impl core::error::Error for DecodeError {}

#[derive(Debug)]
pub(crate) struct Classifier;

impl InboundClassifier<Frame> for Classifier {
    type Error = DecodeError;

    fn reply_key(&mut self, frame: &Frame) -> Result<MatchKey, Self::Error> {
        let key = u32::from_be_bytes(frame.bytes()[..4].try_into().map_err(|_| DecodeError)?);
        Ok(MatchKey::new(key))
    }
}

pub(crate) fn request(key: MatchKey, value: u32) -> [u8; FRAME_BYTES] {
    let mut frame = [0_u8; FRAME_BYTES];
    frame[..4].copy_from_slice(&key.get().to_be_bytes());
    frame[4..].copy_from_slice(&value.to_be_bytes());
    frame
}
