//! Bounded test protocol that exercises production decode and classify orchestration.

use core::fmt;

use bornera::InboundClassifier;
use bornera_core::{FrameDecoder, MatchKey, RetainedBytes};
use calandria::Retained;

use crate::SimFrame;

const HEADER_BYTES: usize = 8;

/// One decoded simulated reply with protocol correlation identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimReply {
    key: MatchKey,
    payload: Box<[u8]>,
}

impl SimReply {
    /// Returns the correlation key encoded on the simulated wire.
    pub const fn match_key(&self) -> MatchKey {
        self.key
    }

    /// Borrows the opaque reply payload.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl Retained for SimReply {
    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::try_from(self.payload.len()).unwrap_or(RetainedBytes::new(u64::MAX))
    }
}

#[derive(Debug)]
pub(crate) struct SimDecoder {
    maximum_payload: RetainedBytes,
    header: [u8; HEADER_BYTES],
    header_used: usize,
    key: Option<MatchKey>,
    payload: Option<Box<[u8]>>,
    payload_used: usize,
    ready: Option<SimReply>,
}

impl SimDecoder {
    pub(crate) const fn new(maximum_payload: RetainedBytes) -> Self {
        Self {
            maximum_payload,
            header: [0; HEADER_BYTES],
            header_used: 0,
            key: None,
            payload: None,
            payload_used: 0,
            ready: None,
        }
    }

    fn accept_header(&mut self, input: &mut &[u8]) -> Result<(), SimWireError> {
        let needed = HEADER_BYTES.saturating_sub(self.header_used);
        let accepted = needed.min(input.len());
        let end = self.header_used.saturating_add(accepted);
        self.header[self.header_used..end].copy_from_slice(&input[..accepted]);
        self.header_used = end;
        *input = &input[accepted..];
        if self.header_used != HEADER_BYTES {
            return Ok(());
        }

        let key = MatchKey::new(u32::from_be_bytes([
            self.header[0],
            self.header[1],
            self.header[2],
            self.header[3],
        ]));
        let length = u32::from_be_bytes([
            self.header[4],
            self.header[5],
            self.header[6],
            self.header[7],
        ]) as usize;
        let retained =
            RetainedBytes::try_from(length).map_err(|_| SimWireError::PayloadTooLarge)?;
        if retained > self.maximum_payload {
            return Err(SimWireError::PayloadTooLarge);
        }
        if length == 0 {
            self.ready = Some(SimReply {
                key,
                payload: Box::default(),
            });
            self.header_used = 0;
        } else {
            self.key = Some(key);
            self.payload = Some(core::iter::repeat_n(0, length).collect());
        }
        Ok(())
    }

    fn accept_payload(&mut self, input: &mut &[u8]) -> Result<(), SimWireError> {
        let Some(payload) = self.payload.as_mut() else {
            return Ok(());
        };
        let needed = payload.len().saturating_sub(self.payload_used);
        let accepted = needed.min(input.len());
        let end = self.payload_used.saturating_add(accepted);
        payload[self.payload_used..end].copy_from_slice(&input[..accepted]);
        self.payload_used = end;
        *input = &input[accepted..];
        if self.payload_used != payload.len() {
            return Ok(());
        }
        let key = self.key.take().ok_or(SimWireError::StateDiverged)?;
        let payload = self.payload.take().ok_or(SimWireError::StateDiverged)?;
        self.ready = Some(SimReply { key, payload });
        self.header_used = 0;
        self.payload_used = 0;
        Ok(())
    }
}

impl FrameDecoder for SimDecoder {
    type Frame = SimReply;
    type Error = SimWireError;

    fn feed(&mut self, mut bytes: &[u8]) -> Result<(), Self::Error> {
        if self.ready.is_some() {
            return Err(SimWireError::UndrainedFrame);
        }
        while !bytes.is_empty() {
            if self.payload.is_some() {
                self.accept_payload(&mut bytes)?;
            } else {
                self.accept_header(&mut bytes)?;
            }
            if self.ready.is_some() && !bytes.is_empty() {
                return Err(SimWireError::MultipleFrames);
            }
        }
        Ok(())
    }

    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error> {
        Ok(self.ready.take())
    }

    fn retained_bytes(&self) -> RetainedBytes {
        self.payload.as_ref().map_or_else(
            || {
                self.ready
                    .as_ref()
                    .map_or(RetainedBytes::ZERO, Retained::retained_bytes)
            },
            |payload| {
                RetainedBytes::try_from(payload.len()).unwrap_or(RetainedBytes::new(u64::MAX))
            },
        )
    }
}

#[derive(Debug)]
pub(crate) struct SimClassifier;

impl InboundClassifier<SimReply> for SimClassifier {
    type Error = SimWireError;

    fn reply_key(&mut self, frame: &SimReply) -> Result<MatchKey, Self::Error> {
        Ok(frame.key)
    }
}

pub(crate) fn encode_reply(key: MatchKey, payload: &SimFrame) -> Result<Vec<u8>, SimWireError> {
    let length =
        u32::try_from(payload.as_bytes().len()).map_err(|_| SimWireError::PayloadTooLarge)?;
    let mut encoded = Vec::with_capacity(HEADER_BYTES.saturating_add(payload.as_bytes().len()));
    encoded.extend_from_slice(&key.get().to_be_bytes());
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(payload.as_bytes());
    Ok(encoded)
}

/// Failure in the bounded simulated wire protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SimWireError {
    /// The encoded payload exceeded configured or fixed-width bounds.
    PayloadTooLarge,
    /// A caller fed another frame before draining the complete prior frame.
    UndrainedFrame,
    /// One input chunk contained more than one complete simulated frame.
    MultipleFrames,
    /// Internal streaming state disagreed while completing a frame.
    StateDiverged,
}

impl fmt::Display for SimWireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::PayloadTooLarge => "simulated reply payload exceeds its bound",
            Self::UndrainedFrame => "simulated decoder frame was not drained",
            Self::MultipleFrames => "simulated input contained multiple frames",
            Self::StateDiverged => "simulated decoder state diverged",
        })
    }
}

impl core::error::Error for SimWireError {}
