//! Capacity-first connector fault capability for set tests.

use std::{
    convert::Infallible,
    error::Error,
    io,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, Ordering},
};

use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId, FrameDecoder, LaneId,
    MatchKey, MatchKeySpace, Moment, RetainedBytes,
};
use calandria::{Interest, Readiness, ResourceOwnerId, Retained, Span, TimerOwnerId, WaitOutcome};
use mio::{Registry, Token, event::Source};

use crate::{
    ConnectionConfig, ConnectionIdentity, ConnectionSet, ConnectionSetConfig, ConnectionSetLimits,
    ConnectionSlotLimits, DecoderLimits, InboundClassifier, IoLimits, PublicationLimits,
    RegisteredTransport, SlotTransport, TcpSocketPolicy, TcpTransport, TransportBudget,
    TransportConnector, TransportError, TransportProgress,
};

static SOCKET_ATTEMPTED: AtomicBool = AtomicBool::new(false);

pub(crate) fn reset_socket_attempt() {
    SOCKET_ATTEMPTED.store(false, Ordering::Relaxed);
}

pub(crate) fn socket_attempted() -> bool {
    SOCKET_ATTEMPTED.load(Ordering::Relaxed)
}

#[derive(Debug)]
pub(crate) struct RecordSocketAttempt;

impl TransportConnector for RecordSocketAttempt {
    type Transport = TcpTransport;

    fn connect(self, _address: SocketAddr) -> io::Result<Self::Transport> {
        SOCKET_ATTEMPTED.store(true, Ordering::Relaxed);
        Err(io::Error::other("capacity check acquired a socket"))
    }
}

#[test]
fn custom_transport_registers_its_initial_interest() -> Result<(), Box<dyn Error>> {
    let mut set: ConnectionSet<Decoder, Classifier, TestRegisteredTransport> = ConnectionSet::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(81)),
        set_limits(),
    )?;
    let token = set.connect_with(
        connection_config(),
        slot_limits()?,
        Decoder,
        Classifier,
        TestConnector,
    )?;
    let transport = set
        .entry(token)?
        .transport
        .as_ref()
        .ok_or_else(|| io::Error::other("registered transport disappeared"))?;
    assert!(transport.registered_readable);
    assert!(!transport.registered_writable);
    Ok(())
}

#[test]
fn pulse_handle_notifies_the_owned_selector() -> Result<(), Box<dyn Error>> {
    let mut set: ConnectionSet<Decoder, Classifier> = ConnectionSet::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(82)),
        set_limits(),
    )?;
    set.pulse_handle().pulse()?;
    assert_eq!(set.poll_io(Span::ZERO)?, WaitOutcome::Notified);
    Ok(())
}

#[derive(Debug)]
struct TestConnector;

impl TransportConnector for TestConnector {
    type Transport = TestRegisteredTransport;

    fn connect(self, _address: SocketAddr) -> io::Result<Self::Transport> {
        Ok(TestRegisteredTransport {
            open: false,
            registered_readable: false,
            registered_writable: false,
        })
    }
}

#[derive(Debug)]
struct TestRegisteredTransport {
    open: bool,
    registered_readable: bool,
    registered_writable: bool,
}

impl io::Read for TestRegisteredTransport {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }
}

impl io::Write for TestRegisteredTransport {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SlotTransport for TestRegisteredTransport {
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

    fn desired_interest(&self, _has_writes: bool) -> Interest {
        Interest::READABLE
    }

    fn clear_read(&mut self) {}

    fn clear_write(&mut self) {}
}

impl RegisteredTransport for TestRegisteredTransport {
    fn observe_readiness(&mut self, _readiness: Readiness) {}
}

impl Source for TestRegisteredTransport {
    fn register(
        &mut self,
        _registry: &Registry,
        _token: Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.registered_readable = interests.is_readable();
        self.registered_writable = interests.is_writable();
        Ok(())
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.register(registry, token, interests)
    }

    fn deregister(&mut self, _registry: &Registry) -> io::Result<()> {
        Ok(())
    }
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

fn set_limits() -> ConnectionSetLimits {
    ConnectionSetLimits::new(
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
    )
}

fn slot_limits() -> Result<ConnectionSlotLimits, Box<dyn Error>> {
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
        PublicationLimits::new(NonZeroUsize::MIN),
    )?)
}

fn connection_config() -> ConnectionConfig {
    let identity = ConnectionIdentity::new(
        EndpointId::new(1),
        LaneId::new(1),
        ConnectionId::new(1),
        ConnectionEpoch::new(1),
    );
    ConnectionConfig::new(
        identity,
        SocketAddr::from(([127, 0, 0, 1], 1)),
        Deadline::at(Moment::from_nanos(u64::MAX)),
        TimerOwnerId::new(81),
    )
}
