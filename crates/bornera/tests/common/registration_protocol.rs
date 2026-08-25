//! Minimal protocol fixture shared by registered-transport tests.

use std::convert::Infallible;

use bornera::InboundClassifier;
use bornera_core::{FrameDecoder, MatchKey, RetainedBytes};

#[derive(Debug)]
pub(crate) struct Decoder;

impl FrameDecoder for Decoder {
    type Frame = ();
    type Error = Infallible;

    fn feed(&mut self, _bytes: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }

    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error> {
        Ok(None)
    }

    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::ZERO
    }
}

#[derive(Debug)]
pub(crate) struct Classifier;

impl InboundClassifier<()> for Classifier {
    type Error = Infallible;

    fn reply_key(&mut self, _frame: &()) -> Result<MatchKey, Self::Error> {
        Ok(MatchKey::new(0))
    }
}
