//! Minimal protocol and slot configuration for shutdown tests.

use std::{convert::Infallible, error::Error, io, num::NonZeroUsize};

use bornera::{
    ConnectionIdentity, ConnectionSlot, ConnectionSlotConfig, ConnectionSlotLimits, DecoderLimits,
    InboundClassifier, IoLimits, PublicationLimits, TransportLimits,
};
use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId, FrameDecoder, LaneId,
    MatchKey, MatchKeySpace, Moment, RetainedBytes,
};
use calandria::TimerOwnerId;

pub(crate) fn slot(
    operations: usize,
    transport_bytes: RetainedBytes,
) -> Result<ConnectionSlot<Decoder, Classifier>, Box<dyn Error>> {
    let operations = NonZeroUsize::new(operations)
        .ok_or_else(|| io::Error::other("test operation bound must be nonzero"))?;
    let connection = ConnectionLimits::new(
        4,
        RetainedBytes::new(64),
        4,
        RetainedBytes::new(64),
        MatchKeySpace::new(0, 3)?,
    )?;
    let limits = ConnectionSlotLimits::new(
        connection,
        DecoderLimits::new(RetainedBytes::new(8), RetainedBytes::new(8)),
        IoLimits::new(operations, NonZeroUsize::MIN),
        TransportLimits::new(transport_bytes),
        PublicationLimits::new(NonZeroUsize::MIN.saturating_add(7)),
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
        Decoder,
        Classifier,
    )?)
}

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
