//! Opaque fixed-width framing fixtures shared by production tests.

use std::{error::Error, fmt};

use bornera::InboundClassifier;
use bornera_core::{FrameDecoder, MatchKey, RetainedBytes};
use calandria::Retained;

pub(crate) const FRAME_BYTES: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TestFrame(pub Vec<u8>);

impl Retained for TestFrame {
    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::try_from(self.0.capacity()).unwrap_or(RetainedBytes::new(u64::MAX))
    }
}

#[derive(Debug)]
pub(crate) struct FixedDecoder {
    pub(crate) bytes: Vec<u8>,
}

impl FrameDecoder for FixedDecoder {
    type Frame = TestFrame;
    type Error = DecodeError;

    fn feed(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error> {
        if self.bytes.len() < FRAME_BYTES {
            return Ok(None);
        }
        Ok(Some(TestFrame(self.bytes.drain(..FRAME_BYTES).collect())))
    }

    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::try_from(self.bytes.capacity()).unwrap_or(RetainedBytes::new(u64::MAX))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DecodeError;

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("fixed decoder failed")
    }
}

impl Error for DecodeError {}

#[derive(Debug)]
pub(crate) struct KeyClassifier;

impl InboundClassifier<TestFrame> for KeyClassifier {
    type Error = DecodeError;

    fn reply_key(&mut self, frame: &TestFrame) -> Result<MatchKey, Self::Error> {
        let bytes: [u8; 4] = frame
            .0
            .get(..4)
            .ok_or(DecodeError)?
            .try_into()
            .map_err(|_| DecodeError)?;
        Ok(MatchKey::new(u32::from_be_bytes(bytes)))
    }
}

pub(crate) fn request(key: MatchKey, value: u32) -> [u8; FRAME_BYTES] {
    let mut frame = [0_u8; FRAME_BYTES];
    frame[..4].copy_from_slice(&key.get().to_be_bytes());
    frame[4..].copy_from_slice(&value.to_be_bytes());
    frame
}
