//! Generation fencing and exact pending-connect lifetime in a shared set.

use std::{error::Error, net::TcpListener, num::NonZeroUsize};

use bornera::{
    ConnectionConfig, ConnectionIdentity, ConnectionSet, ConnectionSetConfig, ConnectionSetLimits,
    ConnectionSlotLimits, DecoderLimits, IoLimits, PublicationLimits, SocketPolicyError,
    TcpKeepalivePolicy, TcpNoDelay, TcpSocketPolicy, TransportState,
};
use bornera_core::{
    CloseReason, ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId,
    FrameDecoder, LaneId, MatchKey, MatchKeySpace, Moment, RetainedBytes,
};
use calandria::{Next, ResourceOwnerId, Retained, Span, TimerOwnerId};

type TestSet = ConnectionSet<Decoder, Classifier>;

#[test]
fn retired_generation_commands_cannot_reach_its_replacement() -> Result<(), Box<dyn Error>> {
    let old_listener = TcpListener::bind("127.0.0.1:0")?;
    let new_listener = TcpListener::bind("127.0.0.1:0")?;
    let mut set = connection_set()?;
    let old = set.connect(
        connection_config(old_listener.local_addr()?, 10, 20, 30, far_deadline()),
        slot_limits()?,
        Decoder,
        Classifier,
    )?;
    let stale_port = set.port(old)?;
    set.finalize(old, CloseReason::Requested)?;
    drop(set.drain_events(old)?);
    set.retire(old)?;

    let replacement = set.connect(
        connection_config(new_listener.local_addr()?, 10, 21, 31, far_deadline()),
        slot_limits()?,
        Decoder,
        Classifier,
    )?;
    stale_port.close()?;
    let _turn = set.turn_component(Moment::ORIGIN)?;
    if connection_state(&set, replacement) == TransportState::Closed {
        return Err(std::io::Error::other("stale command closed the replacement").into());
    }
    assert_eq!(set.snapshot().stale_commands, 1);
    set.finalize(replacement, CloseReason::Requested)?;
    Ok(())
}

#[test]
fn pending_connect_closes_at_its_exact_deadline() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut set = connection_set()?;
    let connection = set.connect(
        connection_config(
            listener.local_addr()?,
            10,
            20,
            30,
            Deadline::at(Moment::ORIGIN),
        ),
        slot_limits()?,
        Decoder,
        Classifier,
    )?;
    let turn = set.turn_component(Moment::ORIGIN)?;
    if turn.next() == Next::Now {
        return Err(std::io::Error::other("elapsed connect deadline requested more work").into());
    }
    let snapshot = set.connection_snapshot(connection)?;
    assert_eq!(snapshot.transport, TransportState::Closed);
    assert_eq!(
        snapshot.connection.close_reason,
        Some(CloseReason::ConnectTimedOut)
    );
    assert!(snapshot.transport_diagnostic.is_none());
    Ok(())
}

#[test]
fn explicit_tcp_policy_is_applied_before_transport_open_publication() -> Result<(), Box<dyn Error>>
{
    assert!(matches!(
        TcpKeepalivePolicy::new(Span::ZERO),
        Err(SocketPolicyError::ZeroKeepaliveIdle)
    ));
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let keepalive = TcpKeepalivePolicy::new(Span::from_nanos(60_000_000_000))?;
    let policy = TcpSocketPolicy::new(TcpNoDelay::Disabled).keepalive(keepalive);
    let mut set = connection_set()?;
    let connection = set.connect(
        connection_config(listener.local_addr()?, 10, 20, 30, far_deadline()).socket_policy(policy),
        slot_limits()?,
        Decoder,
        Classifier,
    )?;
    for _ in 0..128 {
        set.turn_component(Moment::ORIGIN)?;
        if connection_state(&set, connection) == TransportState::Open {
            set.finalize(connection, CloseReason::Requested)?;
            return Ok(());
        }
        set.poll_io(Span::from_nanos(10_000_000))?;
    }
    Err(std::io::Error::other("socket policy connection did not open within bounded turns").into())
}

fn connection_set() -> Result<TestSet, Box<dyn Error>> {
    Ok(ConnectionSet::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(90)),
        ConnectionSetLimits::new(nz(2)?, nz(2)?, nz(8)?, nz(8)?, nz(1)?),
    )?)
}

fn slot_limits() -> Result<ConnectionSlotLimits, Box<dyn Error>> {
    let core = ConnectionLimits::new(
        4,
        RetainedBytes::new(4_096),
        4,
        RetainedBytes::new(4_096),
        MatchKeySpace::new(0, 32)?,
    )?;
    Ok(ConnectionSlotLimits::new(
        core,
        DecoderLimits::new(RetainedBytes::new(64), RetainedBytes::new(64)),
        IoLimits::new(nz(8)?, nz(8)?),
        PublicationLimits::new(nz(8)?),
    )?)
}

fn connection_config(
    address: std::net::SocketAddr,
    connection: u64,
    epoch: u64,
    timer: u64,
    deadline: Deadline,
) -> ConnectionConfig {
    ConnectionConfig::new(
        ConnectionIdentity::new(
            EndpointId::new(1),
            LaneId::new(2),
            ConnectionId::new(connection),
            ConnectionEpoch::new(epoch),
        ),
        address,
        deadline,
        TimerOwnerId::new(timer),
    )
}

fn far_deadline() -> Deadline {
    Deadline::at(Moment::from_nanos(u64::MAX))
}

fn connection_state(set: &TestSet, connection: bornera::ConnectionToken) -> TransportState {
    set.connection_snapshot(connection)
        .map_or(TransportState::Closed, |snapshot| snapshot.transport)
}

fn nz(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value)
        .ok_or_else(|| std::io::Error::other("test bound must be nonzero").into())
}

#[derive(Debug)]
struct Decoder;

impl FrameDecoder for Decoder {
    type Frame = Frame;
    type Error = std::convert::Infallible;

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

impl bornera::InboundClassifier<Frame> for Classifier {
    type Error = std::convert::Infallible;

    fn reply_key(&mut self, _frame: &Frame) -> Result<MatchKey, Self::Error> {
        Ok(MatchKey::new(0))
    }
}
