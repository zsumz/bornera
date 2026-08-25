//! Readiness callbacks cannot escape transport-pressure accounting.

use std::{convert::Infallible, error::Error, io, num::NonZeroUsize};

use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId, FrameDecoder, LaneId,
    MatchKey, MatchKeySpace, Moment, RetainedBytes,
};
use calandria::{Interest, Readiness, TimerOwnerId};
use mio::{Registry, Token, event::Source};

use super::observe_transport_readiness;
use crate::{
    ConnectionIdentity, ConnectionSlot, ConnectionSlotConfig, ConnectionSlotLimits, DecoderLimits,
    InboundClassifier, IoLimits, OwnerFailure, PublicationLimits, RegisteredTransport,
    SlotTransport, TcpSocketPolicy, TransportBudget, TransportError, TransportFailureKind,
    TransportLimits, TransportPressure, TransportProgress,
};

#[test]
fn readiness_mutation_is_sampled_before_slot_progress() -> Result<(), Box<dyn Error>> {
    let mut slot = slot()?;
    let mut transport = ReadinessPressureTransport {
        pressure: TransportPressure::ZERO,
    };

    observe_transport_readiness(&mut slot, &mut transport, Readiness::READABLE);

    let snapshot = slot.snapshot();
    assert_eq!(snapshot.owner_failure, Some(OwnerFailure::OwnerInvariant));
    assert_eq!(snapshot.transport_pressure, Some(output_pressure(5)?));
    assert_eq!(
        snapshot.transport_diagnostic.map(|value| value.failure),
        Some(TransportFailureKind::Capacity)
    );
    Ok(())
}

fn slot() -> Result<ConnectionSlot<Decoder, Classifier>, Box<dyn Error>> {
    let one = NonZeroUsize::MIN;
    let limits = ConnectionSlotLimits::new(
        ConnectionLimits::new(
            1,
            RetainedBytes::new(8),
            1,
            RetainedBytes::new(8),
            MatchKeySpace::new(0, 0)?,
        )?,
        DecoderLimits::new(RetainedBytes::new(8), RetainedBytes::new(8)),
        IoLimits::new(one, one),
        TransportLimits::new(RetainedBytes::new(4)),
        PublicationLimits::new(one),
    )?;
    Ok(ConnectionSlot::new(
        ConnectionSlotConfig::new(
            ConnectionIdentity::new(
                EndpointId::new(1),
                LaneId::new(2),
                ConnectionId::new(3),
                ConnectionEpoch::new(4),
            ),
            Deadline::at(Moment::from_nanos(100)),
            TimerOwnerId::new(5),
        ),
        limits,
        Decoder,
        Classifier,
    )?)
}

fn output_pressure(bytes: u64) -> Result<TransportPressure, Box<dyn Error>> {
    Ok(TransportPressure::new(
        RetainedBytes::ZERO,
        RetainedBytes::new(bytes),
        RetainedBytes::ZERO,
        RetainedBytes::ZERO,
    )?)
}

#[derive(Debug)]
struct Decoder;

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
struct Classifier;

impl InboundClassifier<()> for Classifier {
    type Error = Infallible;

    fn reply_key(&mut self, _frame: &()) -> Result<MatchKey, Self::Error> {
        Ok(MatchKey::new(0))
    }
}

#[derive(Debug)]
struct ReadinessPressureTransport {
    pressure: TransportPressure,
}

impl io::Read for ReadinessPressureTransport {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }
}

impl io::Write for ReadinessPressureTransport {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SlotTransport for ReadinessPressureTransport {
    fn drive_establishment(
        &mut self,
        _policy: TcpSocketPolicy,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        Ok(TransportProgress::operation())
    }

    fn drive_transport(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        Ok(TransportProgress::IDLE)
    }

    fn begin_shutdown(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        Ok(TransportProgress::operation())
    }

    fn can_establish(&self) -> bool {
        false
    }

    fn has_transport_work(&self) -> bool {
        false
    }

    fn is_shutdown_complete(&self) -> bool {
        true
    }

    fn is_open(&self) -> bool {
        false
    }

    fn can_read(&self) -> bool {
        false
    }

    fn can_write(&self) -> bool {
        false
    }

    fn desired_interest(&self, _has_writes: bool) -> Interest {
        Interest::READABLE
    }

    fn pressure(&self) -> TransportPressure {
        self.pressure
    }

    fn pressure_limit(&self) -> TransportLimits {
        TransportLimits::new(RetainedBytes::new(4))
    }

    fn clear_read(&mut self) {}

    fn clear_write(&mut self) {}
}

impl RegisteredTransport for ReadinessPressureTransport {
    fn observe_readiness(&mut self, _readiness: Readiness) {
        self.pressure = output_pressure(5).unwrap_or(TransportPressure::MAX);
    }
}

impl Source for ReadinessPressureTransport {
    fn register(
        &mut self,
        _registry: &Registry,
        _token: Token,
        _interests: mio::Interest,
    ) -> io::Result<()> {
        Ok(())
    }

    fn reregister(
        &mut self,
        _registry: &Registry,
        _token: Token,
        _interests: mio::Interest,
    ) -> io::Result<()> {
        Ok(())
    }

    fn deregister(&mut self, _registry: &Registry) -> io::Result<()> {
        Ok(())
    }
}
