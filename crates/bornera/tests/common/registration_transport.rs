//! Registered-transport construction and pressure-observation fixtures.

use std::{error::Error, io, net::SocketAddr, num::NonZeroUsize};

use bornera::{
    ConnectionConfig, ConnectionIdentity, ConnectionSet, ConnectionSetConfig, ConnectionSetLimits,
    ConnectionSlotLimits, DecoderLimits, InboundClassifier, IoLimits, PublicationLimits,
    RegisteredTransport, SlotTransport, TcpSocketPolicy, TransportBudget, TransportConnector,
    TransportError, TransportLimits, TransportPressure, TransportProgress,
};
use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId, FrameDecoder, LaneId,
    MatchKeySpace, Moment, RetainedBytes,
};
use calandria::{Interest, Readiness, ResourceOwnerId, Retained, TimerOwnerId};
use mio::{Registry, Token, event::Source};

use crate::registration_probe::RegistrationProbe;

pub(crate) type RegistrationSet<D, C> = ConnectionSet<D, C, PressureTransport>;

pub(crate) fn connection_set<D, C>(owner: u64) -> Result<RegistrationSet<D, C>, Box<dyn Error>>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    let one = NonZeroUsize::MIN;
    Ok(ConnectionSet::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(owner)),
        ConnectionSetLimits::new(one, one, one, one, one),
    )?)
}

pub(crate) fn slot_limits(retained: RetainedBytes) -> Result<ConnectionSlotLimits, Box<dyn Error>> {
    let connection = ConnectionLimits::new(
        1,
        RetainedBytes::new(8),
        1,
        RetainedBytes::new(8),
        MatchKeySpace::new(0, 0)?,
    )?;
    Ok(ConnectionSlotLimits::new(
        connection,
        DecoderLimits::new(RetainedBytes::new(8), RetainedBytes::new(8)),
        IoLimits::new(NonZeroUsize::MIN, NonZeroUsize::MIN),
        TransportLimits::new(retained),
        PublicationLimits::new(NonZeroUsize::MIN.saturating_add(3)),
    )?)
}

pub(crate) fn connection_config(timer_owner: u64) -> ConnectionConfig {
    ConnectionConfig::new(
        ConnectionIdentity::new(
            EndpointId::new(1),
            LaneId::new(1),
            ConnectionId::new(1),
            ConnectionEpoch::new(1),
        ),
        SocketAddr::from(([127, 0, 0, 1], 1)),
        Deadline::at(Moment::from_nanos(u64::MAX)),
        TimerOwnerId::new(timer_owner),
    )
}

#[derive(Clone, Debug)]
pub(crate) struct PressureConnector {
    probe: RegistrationProbe,
    initial: TransportPressure,
    script: PressureScript,
}

impl PressureConnector {
    pub(crate) const fn new(
        probe: RegistrationProbe,
        initial: TransportPressure,
        script: PressureScript,
    ) -> Self {
        Self {
            probe,
            initial,
            script,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PressureScript {
    pub(crate) declared_limit: Option<TransportLimits>,
    pub(crate) after_registration: Option<TransportPressure>,
    pub(crate) limit_after_registration: Option<TransportLimits>,
    pub(crate) after_reregistration: Option<TransportPressure>,
    pub(crate) after_deregistration: Option<TransportPressure>,
    pub(crate) limit_after_deregistration: Option<TransportLimits>,
    pub(crate) fail_reregistration: bool,
    pub(crate) fail_deregistration: bool,
}

impl PressureScript {
    pub(crate) const NONE: Self = Self {
        declared_limit: None,
        after_registration: None,
        limit_after_registration: None,
        after_reregistration: None,
        after_deregistration: None,
        limit_after_deregistration: None,
        fail_reregistration: false,
        fail_deregistration: false,
    };
}

impl TransportConnector for PressureConnector {
    type Transport = PressureTransport;

    fn connect(self, _address: SocketAddr, limits: TransportLimits) -> io::Result<Self::Transport> {
        self.probe.connected(limits);
        Ok(PressureTransport {
            probe: self.probe,
            pressure: self.initial,
            limits: self.script.declared_limit.unwrap_or(limits),
            after_registration: self.script.after_registration,
            limit_after_registration: self.script.limit_after_registration,
            after_reregistration: self.script.after_reregistration,
            after_deregistration: self.script.after_deregistration,
            limit_after_deregistration: self.script.limit_after_deregistration,
            fail_reregistration: self.script.fail_reregistration,
            fail_deregistration: self.script.fail_deregistration,
            open: false,
        })
    }
}

#[derive(Debug)]
pub(crate) struct PressureTransport {
    probe: RegistrationProbe,
    pressure: TransportPressure,
    limits: TransportLimits,
    after_registration: Option<TransportPressure>,
    limit_after_registration: Option<TransportLimits>,
    after_reregistration: Option<TransportPressure>,
    after_deregistration: Option<TransportPressure>,
    limit_after_deregistration: Option<TransportLimits>,
    fail_reregistration: bool,
    fail_deregistration: bool,
    open: bool,
}

impl io::Read for PressureTransport {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }
}

impl io::Write for PressureTransport {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SlotTransport for PressureTransport {
    fn drive_establishment(
        &mut self,
        _policy: TcpSocketPolicy,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        self.open = true;
        Ok(TransportProgress::operation())
    }

    fn drive_transport(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        Ok(TransportProgress::IDLE)
    }

    fn can_establish(&self) -> bool {
        !self.open
    }

    fn has_transport_work(&self) -> bool {
        false
    }

    fn is_open(&self) -> bool {
        self.open
    }

    fn can_read(&self) -> bool {
        false
    }

    fn can_write(&self) -> bool {
        false
    }

    fn desired_interest(&self, has_writes: bool) -> Interest {
        if self.open && has_writes {
            Interest::READ_WRITE
        } else {
            Interest::READABLE
        }
    }

    fn pressure(&self) -> TransportPressure {
        self.pressure
    }

    fn pressure_limit(&self) -> TransportLimits {
        self.limits
    }

    fn clear_read(&mut self) {}

    fn clear_write(&mut self) {}
}

impl RegisteredTransport for PressureTransport {
    fn observe_readiness(&mut self, _readiness: Readiness) {}
}

impl Source for PressureTransport {
    fn register(
        &mut self,
        _registry: &Registry,
        _token: Token,
        _interests: mio::Interest,
    ) -> io::Result<()> {
        self.probe.registered();
        if let Some(pressure) = self.after_registration.take() {
            self.pressure = pressure;
        }
        if let Some(limits) = self.limit_after_registration.take() {
            self.limits = limits;
        }
        Ok(())
    }

    fn reregister(
        &mut self,
        _registry: &Registry,
        _token: Token,
        _interests: mio::Interest,
    ) -> io::Result<()> {
        self.probe.reregistered();
        if let Some(pressure) = self.after_reregistration.take() {
            self.pressure = pressure;
        }
        if self.fail_reregistration {
            Err(io::Error::other("scripted reregistration failure"))
        } else {
            Ok(())
        }
    }

    fn deregister(&mut self, _registry: &Registry) -> io::Result<()> {
        self.probe.deregistered();
        if let Some(pressure) = self.after_deregistration.take() {
            self.pressure = pressure;
        }
        if let Some(limits) = self.limit_after_deregistration.take() {
            self.limits = limits;
        }
        if self.fail_deregistration {
            Err(io::Error::other("scripted deregistration failure"))
        } else {
            Ok(())
        }
    }
}
