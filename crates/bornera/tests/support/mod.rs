//! Opaque framing fixtures shared by production integration tests.

use std::{error::Error, fmt, net::SocketAddr, num::NonZeroUsize};

use bornera::{
    ConnectionEngine, DecoderLimits, EngineConfig, EngineLimits, InboundClassifier,
    PublicationLimits, TurnLimits,
};
use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, EndpointId, FrameDecoder, LaneId, MatchKey,
    MatchKeySpace,
};
use calandria::{ResourceOwnerId, Retained, RetainedBytes, TimerOwnerId};

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

pub(crate) type TestEngine = ConnectionEngine<FixedDecoder, KeyClassifier>;

pub(crate) fn engine(address: SocketAddr) -> Result<TestEngine, Box<dyn Error>> {
    let (config, limits) = engine_parts(address)?;
    Ok(ConnectionEngine::connect(
        config,
        limits,
        FixedDecoder { bytes: Vec::new() },
        KeyClassifier,
    )?)
}

pub(crate) fn engine_parts(
    address: SocketAddr,
) -> Result<(EngineConfig, EngineLimits), Box<dyn Error>> {
    engine_parts_with_events(address, nonzero(8)?)
}

pub(crate) fn engine_parts_with_events(
    address: SocketAddr,
    lifecycle_events: NonZeroUsize,
) -> Result<(EngineConfig, EngineLimits), Box<dyn Error>> {
    let connection = ConnectionLimits::new(
        4,
        RetainedBytes::new(4_096),
        4,
        RetainedBytes::new(4_096),
        MatchKeySpace::new(0, 32)?,
    )?;
    let limits = EngineLimits::new(
        connection,
        DecoderLimits::new(RetainedBytes::new(64), RetainedBytes::new(64)),
        TurnLimits::new(nonzero(16)?, nonzero(8)?, nonzero(8)?, nonzero(4)?),
        PublicationLimits::new(lifecycle_events),
    )?;
    let config = EngineConfig {
        endpoint: EndpointId::new(1),
        lane: LaneId::new(2),
        connection: ConnectionId::new(3),
        epoch: ConnectionEpoch::new(4),
        address,
        resource_owner: ResourceOwnerId::new(5),
        timer_owner: TimerOwnerId::new(6),
    };
    Ok((config, limits))
}

pub(crate) fn request(key: MatchKey, value: u32) -> [u8; FRAME_BYTES] {
    let mut frame = [0_u8; FRAME_BYTES];
    frame[..4].copy_from_slice(&key.get().to_be_bytes());
    frame[4..].copy_from_slice(&value.to_be_bytes());
    frame
}

fn nonzero(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value)
        .ok_or_else(|| std::io::Error::other("test bound must be nonzero").into())
}
