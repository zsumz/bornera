//! Selector-free slot and alternate-transport fixtures.

use std::convert::Infallible;
use std::error::Error;
use std::io;
use std::num::NonZeroUsize;

use bornera::{
    ConnectionIdentity, ConnectionSlot, ConnectionSlotConfig, ConnectionSlotLimits, DecoderLimits,
    InboundClassifier, IoLimits, PublicationLimits, TransportLimits,
};
use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId, FrameDecoder, LaneId,
    MatchKey, MatchKeySpace, Moment, RetainedBytes,
};
use calandria::{Retained, TimerOwnerId};

#[path = "slot_transport.rs"]
mod slot_transport;
pub(crate) use slot_transport::TestTransport;

pub(crate) fn slot() -> Result<ConnectionSlot<Decoder, Classifier>, Box<dyn Error>> {
    slot_with_decoder(Decoder::retaining(0))
}

pub(crate) fn slot_with_decoder(
    decoder: Decoder,
) -> Result<ConnectionSlot<Decoder, Classifier>, Box<dyn Error>> {
    slot_with_io_operations(decoder, 4)
}

pub(crate) fn slot_with_io_operations(
    decoder: Decoder,
    operations: usize,
) -> Result<ConnectionSlot<Decoder, Classifier>, Box<dyn Error>> {
    let core = ConnectionLimits::new(
        4,
        RetainedBytes::new(64),
        4,
        RetainedBytes::new(64),
        MatchKeySpace::new(0, 3)?,
    )?;
    let limits = ConnectionSlotLimits::new(
        core,
        DecoderLimits::new(RetainedBytes::new(8), RetainedBytes::new(8)),
        IoLimits::new(nonzero(operations)?, nonzero(8)?),
        TransportLimits::new(RetainedBytes::ZERO),
        PublicationLimits::new(nonzero(8)?),
    )?;
    let identity = ConnectionIdentity::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
    );
    Ok(ConnectionSlot::new(
        ConnectionSlotConfig::new(
            identity,
            Deadline::at(Moment::from_nanos(100)),
            TimerOwnerId::new(5),
        ),
        limits,
        decoder,
        Classifier,
    )?)
}

#[derive(Debug)]
pub(crate) struct Decoder {
    retained: u64,
}

impl Decoder {
    pub(crate) const fn retaining(bytes: u64) -> Self {
        Self { retained: bytes }
    }
}

impl FrameDecoder for Decoder {
    type Frame = Frame;
    type Error = Infallible;

    fn feed(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.retained = self
            .retained
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error> {
        Ok(None)
    }

    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::new(self.retained)
    }
}

#[derive(Debug)]
pub(crate) struct Frame;

impl Retained for Frame {
    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::ZERO
    }
}

#[derive(Debug)]
pub(crate) struct Classifier;

impl InboundClassifier<Frame> for Classifier {
    type Error = Infallible;

    fn reply_key(&mut self, _frame: &Frame) -> Result<MatchKey, Self::Error> {
        Ok(MatchKey::new(0))
    }
}

fn nonzero(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value).ok_or_else(|| io::Error::other("test bound must be nonzero").into())
}
