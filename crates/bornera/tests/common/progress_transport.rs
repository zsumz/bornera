//! Focused selector-free fixture for transport progression and pressure tests.

use std::{convert::Infallible, error::Error, io, num::NonZeroUsize};

use bornera::{
    ConnectionIdentity, ConnectionSlot, ConnectionSlotConfig, ConnectionSlotLimits, DecoderLimits,
    InboundClassifier, IoLimits, PublicationLimits, SlotTransport, TcpSocketPolicy,
    TransportBudget, TransportError, TransportLimits, TransportPressure, TransportProgress,
};
use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId, FrameDecoder, LaneId,
    MatchKey, MatchKeySpace, Moment, RetainedBytes,
};
use calandria::{Interest, TimerOwnerId};

pub(crate) fn slot(
    operations: usize,
) -> Result<ConnectionSlot<Decoder, Classifier>, Box<dyn Error>> {
    slot_with_transport_limit(operations, RetainedBytes::new(64))
}

pub(crate) fn slot_with_transport_limit(
    operations: usize,
    retained_bytes: RetainedBytes,
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
        TransportLimits::new(retained_bytes),
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
        Decoder,
        Classifier,
    )?)
}

pub(crate) fn nonzero(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value).ok_or_else(|| io::Error::other("test bound must be nonzero").into())
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

#[derive(Debug)]
pub(crate) struct ProgressTransport {
    open: bool,
    progress: Option<TransportProgress>,
    write_ready: bool,
    pressure: TransportPressure,
    pressure_limit: TransportLimits,
    limit_after_establishment: Option<TransportLimits>,
    pressure_after_transport: Option<TransportPressure>,
    pressure_after_write: Option<TransportPressure>,
}

impl ProgressTransport {
    pub(crate) const fn new(progress: TransportProgress) -> Self {
        Self {
            open: false,
            progress: Some(progress),
            write_ready: false,
            pressure: TransportPressure::ZERO,
            pressure_limit: TransportLimits::new(RetainedBytes::new(64)),
            limit_after_establishment: None,
            pressure_after_transport: None,
            pressure_after_write: None,
        }
    }

    pub(crate) const fn buffered_write() -> Self {
        Self {
            open: false,
            progress: None,
            write_ready: true,
            pressure: TransportPressure::ZERO,
            pressure_limit: TransportLimits::new(RetainedBytes::new(64)),
            limit_after_establishment: None,
            pressure_after_transport: None,
            pressure_after_write: None,
        }
    }

    pub(crate) const fn preopened() -> Self {
        Self {
            open: true,
            progress: None,
            write_ready: false,
            pressure: TransportPressure::ZERO,
            pressure_limit: TransportLimits::new(RetainedBytes::new(64)),
            limit_after_establishment: None,
            pressure_after_transport: None,
            pressure_after_write: None,
        }
    }

    pub(crate) const fn with_pressure(pressure: TransportPressure) -> Self {
        Self {
            open: false,
            progress: None,
            write_ready: false,
            pressure,
            pressure_limit: TransportLimits::new(RetainedBytes::new(4)),
            limit_after_establishment: None,
            pressure_after_transport: None,
            pressure_after_write: None,
        }
    }

    pub(crate) const fn pressure_after_write(pressure: TransportPressure) -> Self {
        Self {
            open: false,
            progress: None,
            write_ready: true,
            pressure: TransportPressure::ZERO,
            pressure_limit: TransportLimits::new(RetainedBytes::new(4)),
            limit_after_establishment: None,
            pressure_after_transport: None,
            pressure_after_write: Some(pressure),
        }
    }

    pub(crate) const fn declared_limit(mut self, limit: RetainedBytes) -> Self {
        self.pressure_limit = TransportLimits::new(limit);
        self
    }

    pub(crate) const fn limit_after_establishment(mut self, limit: RetainedBytes) -> Self {
        self.limit_after_establishment = Some(TransportLimits::new(limit));
        self
    }

    pub(crate) const fn pressure_after_transport(mut self, pressure: TransportPressure) -> Self {
        self.pressure_limit = TransportLimits::new(RetainedBytes::new(4));
        self.pressure_after_transport = Some(pressure);
        self
    }
}

impl io::Read for ProgressTransport {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::from(io::ErrorKind::WouldBlock))
    }
}

impl io::Write for ProgressTransport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if !self.write_ready {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        self.write_ready = false;
        self.progress = Some(TransportProgress::operation());
        if let Some(pressure) = self.pressure_after_write.take() {
            self.pressure = pressure;
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SlotTransport for ProgressTransport {
    fn drive_establishment(
        &mut self,
        _policy: TcpSocketPolicy,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        self.open = true;
        if let Some(limit) = self.limit_after_establishment.take() {
            self.pressure_limit = limit;
        }
        Ok(TransportProgress::operation())
    }

    fn drive_transport(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        if let Some(pressure) = self.pressure_after_transport.take() {
            self.pressure = pressure;
        }
        Ok(self.progress.take().unwrap_or(TransportProgress::IDLE))
    }

    fn can_establish(&self) -> bool {
        !self.open
    }

    fn has_transport_work(&self) -> bool {
        self.progress.is_some()
    }

    fn is_open(&self) -> bool {
        self.open
    }

    fn can_read(&self) -> bool {
        false
    }

    fn can_write(&self) -> bool {
        self.open && self.write_ready
    }

    fn desired_interest(&self, _has_writes: bool) -> Interest {
        Interest::READABLE
    }

    fn pressure(&self) -> TransportPressure {
        self.pressure
    }

    fn pressure_limit(&self) -> TransportLimits {
        self.pressure_limit
    }

    fn clear_read(&mut self) {}

    fn clear_write(&mut self) {}
}
