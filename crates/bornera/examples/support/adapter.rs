//! Small opaque framing adapter shared only by the hosting examples.

use std::{error::Error, fmt, net::SocketAddr, num::NonZeroUsize};

use bornera::{
    ConnectionConfig, ConnectionIdentity, ConnectionSetConfig, ConnectionSlotLimits, DecoderLimits,
    InboundClassifier, IoLimits, OutboundFrame, PublicationLimits, StandaloneConnection,
    StandaloneConnectionConfig,
};
use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, EndpointId, FrameDecoder, LaneId, MatchKey,
    MatchKeySpace, OperationOptions,
};
use calandria::{Deadline, Moment, ResourceOwnerId, Retained, RetainedBytes, TimerOwnerId};

pub(crate) const FRAME_BYTES: usize = 8;
pub(crate) type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExampleFrame(pub(crate) Vec<u8>);

impl Retained for ExampleFrame {
    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::try_from(self.0.capacity()).unwrap_or(RetainedBytes::new(u64::MAX))
    }
}

#[derive(Debug)]
pub(crate) struct ExampleDecoder {
    bytes: Vec<u8>,
}

impl FrameDecoder for ExampleDecoder {
    type Frame = ExampleFrame;
    type Error = ExampleDecodeError;

    fn feed(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error> {
        if self.bytes.len() < FRAME_BYTES {
            return Ok(None);
        }
        Ok(Some(ExampleFrame(
            self.bytes.drain(..FRAME_BYTES).collect(),
        )))
    }

    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::try_from(self.bytes.capacity()).unwrap_or(RetainedBytes::new(u64::MAX))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExampleDecodeError;

impl fmt::Display for ExampleDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("example frame is malformed")
    }
}

impl Error for ExampleDecodeError {}

#[derive(Debug)]
pub(crate) struct ExampleClassifier;

impl InboundClassifier<ExampleFrame> for ExampleClassifier {
    type Error = ExampleDecodeError;

    fn reply_key(&mut self, frame: &ExampleFrame) -> Result<MatchKey, Self::Error> {
        let bytes: [u8; 4] = frame
            .0
            .get(..4)
            .ok_or(ExampleDecodeError)?
            .try_into()
            .map_err(|_| ExampleDecodeError)?;
        Ok(MatchKey::new(u32::from_be_bytes(bytes)))
    }
}

type ExampleEngine = StandaloneConnection<ExampleDecoder, ExampleClassifier>;

pub(crate) fn prepared_engine(
    address: SocketAddr,
) -> Result<(ExampleEngine, [u8; FRAME_BYTES]), BoxError> {
    let connection = ConnectionLimits::new(
        4,
        RetainedBytes::new(4_096),
        4,
        RetainedBytes::new(4_096),
        MatchKeySpace::new(0, 32)?,
    )?;
    let limits = ConnectionSlotLimits::new(
        connection,
        DecoderLimits::new(RetainedBytes::new(64), RetainedBytes::new(64)),
        IoLimits::new(nonzero(8)?, nonzero(8)?),
        PublicationLimits::new(nonzero(8)?),
    )?;
    let identity = ConnectionIdentity::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
    );
    let connection = ConnectionConfig::new(
        identity,
        address,
        Deadline::at(Moment::from_nanos(u64::MAX)),
        TimerOwnerId::new(6),
    );
    let config = StandaloneConnectionConfig::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(5)),
        connection,
    );
    let mut engine = StandaloneConnection::connect(
        config,
        limits,
        ExampleDecoder { bytes: Vec::new() },
        ExampleClassifier,
    )?;
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(u64::MAX)))
        .retained_bytes(RetainedBytes::new(8))
        .write_retained_bytes(RetainedBytes::new(8))
        .session();
    let permit = engine.reserve(Moment::ORIGIN, options)?;
    let mut frame = [0_u8; FRAME_BYTES];
    frame[..4].copy_from_slice(&permit.match_key().get().to_be_bytes());
    frame[4..].copy_from_slice(&41_u32.to_be_bytes());
    engine
        .commit(permit, OutboundFrame::copy_from_slice(&frame)?)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok((engine, frame))
}

fn nonzero(value: usize) -> Result<NonZeroUsize, BoxError> {
    NonZeroUsize::new(value)
        .ok_or_else(|| std::io::Error::other("example bound must be nonzero").into())
}
