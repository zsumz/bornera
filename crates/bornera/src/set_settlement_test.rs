//! Synchronous settlement must surface failures observed during physical closure.

use std::convert::Infallible;
use std::error::Error;
use std::io;
use std::num::NonZeroUsize;

use bornera_core::{
    CloseReason, ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId,
    FrameDecoder, LaneId, MatchKey, MatchKeySpace, Moment, RetainedBytes,
};
use calandria::{ResourceOwnerId, Retained, TimerOwnerId};

use crate::{
    ConnectionEntry, ConnectionEvent, ConnectionIdentity, ConnectionSet, ConnectionSetConfig,
    ConnectionSetLimits, ConnectionSlot, ConnectionSlotConfig, ConnectionSlotLimits,
    ConnectionToken, DecoderLimits, EngineError, InboundClassifier, IoLimits, OwnerFailure,
    PublicationLimits,
};

#[test]
fn closed_publication_failure_is_returned_by_synchronous_finalize() -> Result<(), Box<dyn Error>> {
    let identity = ConnectionIdentity::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
    );
    let limits = slot_limits()?;
    let slot = ConnectionSlot::new(
        ConnectionSlotConfig::new(
            identity,
            Deadline::at(Moment::from_nanos(100)),
            TimerOwnerId::new(5),
        ),
        limits,
        Decoder,
        Classifier,
    )?;
    let mut set = ConnectionSet::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(6)),
        ConnectionSetLimits::standalone(limits),
    )?;
    let resource = set
        .resources
        .admit(
            identity,
            ConnectionEntry {
                slot,
                transport: None,
                ready_queued: false,
            },
        )
        .map_err(|_| io::Error::other("fixture resource admission failed"))?;
    let connection = ConnectionToken::new(resource, identity);

    assert!(matches!(
        set.finalize(connection, CloseReason::Requested),
        Err(EngineError::OwnerFailed(OwnerFailure::OwnerInvariant))
    ));
    assert_eq!(
        set.connection_snapshot(connection)?.owner_failure,
        Some(OwnerFailure::OwnerInvariant)
    );
    let report = set.try_recover(connection)?;
    assert_eq!(report.reason, OwnerFailure::OwnerInvariant);
    assert!(matches!(
        report.events.as_slice(),
        [
            ConnectionEvent::Closing {
                reason: CloseReason::Requested,
                ..
            },
            ConnectionEvent::Closed {
                reason: CloseReason::Requested,
                ..
            }
        ]
    ));
    Ok(())
}

fn slot_limits() -> Result<ConnectionSlotLimits, Box<dyn Error>> {
    let one = NonZeroUsize::MIN;
    Ok(ConnectionSlotLimits::new(
        ConnectionLimits::new(
            1,
            RetainedBytes::new(8),
            1,
            RetainedBytes::new(8),
            MatchKeySpace::new(0, 0)?,
        )?,
        DecoderLimits::new(RetainedBytes::new(8), RetainedBytes::new(8)),
        IoLimits::new(one, one),
        PublicationLimits::new(one),
    )?)
}

#[derive(Debug)]
struct Decoder;

impl FrameDecoder for Decoder {
    type Frame = Frame;
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
struct Frame;

impl Retained for Frame {
    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::ZERO
    }
}

#[derive(Debug)]
struct Classifier;

impl InboundClassifier<Frame> for Classifier {
    type Error = Infallible;

    fn reply_key(&mut self, _frame: &Frame) -> Result<MatchKey, Self::Error> {
        Ok(MatchKey::new(0))
    }
}
