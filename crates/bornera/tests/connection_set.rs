//! Shared-selector fairness and connection-local protocol-failure isolation.

#[path = "common/framing.rs"]
mod framing;

use std::{
    error::Error,
    io::{Read, Write},
    net::TcpListener,
    num::NonZeroUsize,
    sync::mpsc,
    thread,
};

use bornera::{
    ConnectionConfig, ConnectionIdentity, ConnectionSet, ConnectionSetConfig, ConnectionSetLimits,
    ConnectionSlotLimits, DecoderLimits, IoLimits, OutboundFrame, PublicationLimits,
    TransportLimits, TransportState,
};
use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, EndpointId, LaneId, MatchKeySpace,
    OperationFailure, OperationOptions, OperationOutcome, RetainedBytes,
};
use calandria::{Deadline, Moment, Next, ResourceOwnerId, Span, TimerOwnerId};

use framing::{FixedDecoder, KeyClassifier, TestFrame, request};

type TestSet = ConnectionSet<FixedDecoder, KeyClassifier>;
#[test]
fn two_connections_progress_fairly_through_one_selector() -> Result<(), Box<dyn Error>> {
    let first = TcpListener::bind("127.0.0.1:0")?;
    let second = TcpListener::bind("127.0.0.1:0")?;
    let first_address = first.local_addr()?;
    let second_address = second.local_addr()?;
    let first_peer = echo_peer(first);
    let second_peer = echo_peer(second);

    let mut set = connection_set()?;
    let first = set.connect(
        connection_config(first_address, 10, 20, 30, far_deadline()),
        slot_limits()?,
        decoder(),
        KeyClassifier,
    )?;
    let second = set.connect(
        connection_config(second_address, 11, 21, 31, far_deadline()),
        slot_limits()?,
        decoder(),
        KeyClassifier,
    )?;
    assert_eq!(set.snapshot().poller.registrations(), 2);
    assert_eq!(set.snapshot().connections.active(), 2);

    commit_request(&mut set, first, 41)?;
    commit_request(&mut set, second, 42)?;
    drive_until(&mut set, |set| {
        pending_outcomes(set, first) == 1 && pending_outcomes(set, second) == 1
    })?;

    let first_outcomes: Vec<_> = set.drain_outcomes(first)?.collect();
    let second_outcomes: Vec<_> = set.drain_outcomes(second)?.collect();
    assert!(matches!(
        first_outcomes.as_slice(),
        [outcome] if matches!(outcome.outcome(), OperationOutcome::Reply(TestFrame(bytes)) if bytes[4..] == 41_u32.to_be_bytes())
    ));
    assert!(matches!(
        second_outcomes.as_slice(),
        [outcome] if matches!(outcome.outcome(), OperationOutcome::Reply(TestFrame(bytes)) if bytes[4..] == 42_u32.to_be_bytes())
    ));
    join(first_peer)?;
    join(second_peer)?;
    Ok(())
}

#[test]
fn one_protocol_failure_does_not_close_an_independent_connection() -> Result<(), Box<dyn Error>> {
    let first = TcpListener::bind("127.0.0.1:0")?;
    let second = TcpListener::bind("127.0.0.1:0")?;
    let first_address = first.local_addr()?;
    let second_address = second.local_addr()?;
    let wrong_peer = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = first.accept()?;
        let mut frame = [0_u8; framing::FRAME_BYTES];
        stream.read_exact(&mut frame)?;
        frame[..4].copy_from_slice(&u32::MAX.to_be_bytes());
        stream.write_all(&frame)
    });
    let (release, hold) = mpsc::channel();
    let healthy_peer = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = second.accept()?;
        let mut frame = [0_u8; framing::FRAME_BYTES];
        stream.read_exact(&mut frame)?;
        stream.write_all(&frame)?;
        hold.recv()
            .map_err(|_| std::io::Error::other("healthy peer release was dropped"))
    });

    let mut set = connection_set()?;
    let failed = set.connect(
        connection_config(first_address, 10, 20, 30, far_deadline()),
        slot_limits()?,
        decoder(),
        KeyClassifier,
    )?;
    let healthy = set.connect(
        connection_config(second_address, 11, 21, 31, far_deadline()),
        slot_limits()?,
        decoder(),
        KeyClassifier,
    )?;
    commit_request(&mut set, failed, 1)?;
    commit_request(&mut set, healthy, 2)?;
    drive_until(&mut set, |set| {
        connection_state(set, failed) == TransportState::Closed
            && pending_outcomes(set, healthy) == 1
    })?;

    assert_eq!(connection_state(&set, healthy), TransportState::Open);
    let failed_outcomes: Vec<_> = set.drain_outcomes(failed)?.collect();
    assert!(matches!(
        failed_outcomes.as_slice(),
        [outcome] if matches!(outcome.outcome(), OperationOutcome::Failed { failure: OperationFailure::MatchKeyMismatch { .. }, .. })
    ));
    assert!(matches!(
        set.drain_outcomes(healthy)?
            .next()
            .map(bornera::EngineOutcome::into_outcome),
        Some(OperationOutcome::Reply(_))
    ));
    release.send(())?;
    join(wrong_peer)?;
    join(healthy_peer)?;
    Ok(())
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
        TransportLimits::new(RetainedBytes::ZERO),
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
    let identity = ConnectionIdentity::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(connection),
        ConnectionEpoch::new(epoch),
    );
    ConnectionConfig::new(identity, address, deadline, TimerOwnerId::new(timer))
}

fn commit_request(
    set: &mut TestSet,
    connection: bornera::ConnectionToken,
    value: u32,
) -> Result<(), Box<dyn Error>> {
    let permit = set.reserve(connection, Moment::ORIGIN, options())?;
    let bytes = request(permit.match_key(), value);
    set.commit(connection, permit, OutboundFrame::copy_from_slice(&bytes)?)?;
    Ok(())
}

fn options() -> OperationOptions {
    OperationOptions::until(far_deadline())
        .session()
        .retained_bytes(RetainedBytes::new(8))
        .write_retained_bytes(RetainedBytes::new(8))
}

fn far_deadline() -> Deadline {
    Deadline::at(Moment::from_nanos(u64::MAX))
}

fn decoder() -> FixedDecoder {
    FixedDecoder { bytes: Vec::new() }
}

fn pending_outcomes(set: &TestSet, connection: bornera::ConnectionToken) -> usize {
    set.connection_snapshot(connection)
        .map_or(0, |snapshot| snapshot.pending_outcomes)
}

fn connection_state(set: &TestSet, connection: bornera::ConnectionToken) -> TransportState {
    set.connection_snapshot(connection)
        .map_or(TransportState::Closed, |snapshot| snapshot.transport)
}

fn drive_until(
    set: &mut TestSet,
    mut complete: impl FnMut(&TestSet) -> bool,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..256 {
        let turn = set.turn_component(Moment::ORIGIN)?;
        if complete(set) {
            return Ok(());
        }
        if turn.next() != Next::Now {
            set.poll_io(Span::from_nanos(10_000_000))?;
        }
    }
    Err(std::io::Error::other("connection set made no bounded progress").into())
}

fn echo_peer(listener: TcpListener) -> thread::JoinHandle<std::io::Result<()>> {
    thread::spawn(move || {
        let (mut stream, _) = listener.accept()?;
        let mut frame = [0_u8; framing::FRAME_BYTES];
        stream.read_exact(&mut frame)?;
        stream.write_all(&frame)
    })
}

fn join(handle: thread::JoinHandle<std::io::Result<()>>) -> Result<(), Box<dyn Error>> {
    Ok(handle
        .join()
        .map_err(|_| std::io::Error::other("loopback peer panicked"))??)
}

fn nz(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value)
        .ok_or_else(|| std::io::Error::other("test bound must be nonzero").into())
}
