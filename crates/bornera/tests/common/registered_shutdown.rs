//! Registered graceful-shutdown fixture with externally controlled writability.

use std::{
    io,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

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

pub(crate) type ShutdownSet<D, C> = ConnectionSet<D, C, ShutdownTransport>;

#[derive(Clone, Debug)]
pub(crate) struct ShutdownProbe {
    state: Arc<ProbeState>,
}

#[derive(Debug)]
struct ProbeState {
    writable: AtomicBool,
    shutdown_writable_interest: AtomicBool,
    begins: AtomicUsize,
    drives: AtomicUsize,
    registrations: AtomicUsize,
    reregistrations: AtomicUsize,
    deregistrations: AtomicUsize,
}

impl ShutdownProbe {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(ProbeState {
                writable: AtomicBool::new(false),
                shutdown_writable_interest: AtomicBool::new(false),
                begins: AtomicUsize::new(0),
                drives: AtomicUsize::new(0),
                registrations: AtomicUsize::new(0),
                reregistrations: AtomicUsize::new(0),
                deregistrations: AtomicUsize::new(0),
            }),
        }
    }

    pub(crate) fn allow_write(&self) {
        self.state.writable.store(true, Ordering::Relaxed);
    }

    pub(crate) fn begins(&self) -> usize {
        self.state.begins.load(Ordering::Relaxed)
    }

    pub(crate) fn drives(&self) -> usize {
        self.state.drives.load(Ordering::Relaxed)
    }

    pub(crate) fn reregistrations(&self) -> usize {
        self.state.reregistrations.load(Ordering::Relaxed)
    }

    pub(crate) fn deregistrations(&self) -> usize {
        self.state.deregistrations.load(Ordering::Relaxed)
    }

    pub(crate) fn saw_shutdown_writable_interest(&self) -> bool {
        self.state
            .shutdown_writable_interest
            .load(Ordering::Relaxed)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ShutdownConnector(pub(crate) ShutdownProbe);

impl TransportConnector for ShutdownConnector {
    type Transport = ShutdownTransport;

    fn connect(self, _address: SocketAddr, limits: TransportLimits) -> io::Result<Self::Transport> {
        Ok(ShutdownTransport {
            probe: self.0,
            limits,
            open: false,
            began: false,
            pending: false,
        })
    }
}

#[derive(Debug)]
pub(crate) struct ShutdownTransport {
    probe: ShutdownProbe,
    limits: TransportLimits,
    open: bool,
    began: bool,
    pending: bool,
}

impl io::Read for ShutdownTransport {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }
}

impl io::Write for ShutdownTransport {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SlotTransport for ShutdownTransport {
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
        self.probe.state.drives.fetch_add(1, Ordering::Relaxed);
        self.probe.state.writable.store(false, Ordering::Relaxed);
        self.pending = false;
        Ok(TransportProgress::operation())
    }

    fn begin_shutdown(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        self.probe.state.begins.fetch_add(1, Ordering::Relaxed);
        self.began = true;
        self.pending = true;
        Ok(TransportProgress::operation())
    }

    fn can_establish(&self) -> bool {
        !self.open
    }

    fn has_transport_work(&self) -> bool {
        self.pending && self.probe.state.writable.load(Ordering::Relaxed)
    }

    fn is_shutdown_complete(&self) -> bool {
        self.began && !self.pending
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
        if !self.open || self.pending || has_writes {
            Interest::READ_WRITE
        } else {
            Interest::READABLE
        }
    }

    fn pressure(&self) -> TransportPressure {
        TransportPressure::ZERO
    }

    fn pressure_limit(&self) -> TransportLimits {
        self.limits
    }

    fn clear_read(&mut self) {}

    fn clear_write(&mut self) {}
}

impl RegisteredTransport for ShutdownTransport {
    fn observe_readiness(&mut self, readiness: Readiness) {
        if readiness.is_writable() {
            self.probe.state.writable.store(true, Ordering::Relaxed);
        }
    }
}

impl Source for ShutdownTransport {
    fn register(
        &mut self,
        _registry: &Registry,
        _token: Token,
        _interests: mio::Interest,
    ) -> io::Result<()> {
        self.probe
            .state
            .registrations
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn reregister(
        &mut self,
        _registry: &Registry,
        _token: Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.probe
            .state
            .reregistrations
            .fetch_add(1, Ordering::Relaxed);
        if self.pending && interests.is_writable() {
            self.probe
                .state
                .shutdown_writable_interest
                .store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    fn deregister(&mut self, _registry: &Registry) -> io::Result<()> {
        self.probe
            .state
            .deregistrations
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

pub(crate) fn connection_set<D, C>() -> Result<ShutdownSet<D, C>, calandria_mio::MioError>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    let one = NonZeroUsize::MIN;
    ConnectionSet::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(71)),
        ConnectionSetLimits::new(one, one, one, one, one),
    )
}

pub(crate) fn connection_config() -> ConnectionConfig {
    ConnectionConfig::new(
        ConnectionIdentity::new(
            EndpointId::new(1),
            LaneId::new(2),
            ConnectionId::new(3),
            ConnectionEpoch::new(4),
        ),
        SocketAddr::from(([127, 0, 0, 1], 1)),
        Deadline::at(Moment::from_nanos(100)),
        TimerOwnerId::new(72),
    )
}

pub(crate) fn slot_limits() -> Result<ConnectionSlotLimits, Box<dyn std::error::Error>> {
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
        TransportLimits::new(RetainedBytes::ZERO),
        PublicationLimits::new(NonZeroUsize::MIN.saturating_add(4)),
    )?)
}
