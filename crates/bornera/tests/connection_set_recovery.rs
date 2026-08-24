//! Connection-local recovery leaves healthy shared-selector peers intact.

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
    ConnectionConfig, ConnectionIdentity, ConnectionRecoveryError, ConnectionSet,
    ConnectionSetConfig, ConnectionSetLimits, ConnectionSlotLimits, DecoderLimits, EngineError,
    EngineInvariant, IoLimits, OutboundFrame, OwnerFailure, PublicationLimits, TransportState,
};
use bornera_core::{
    CloseReason, ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId, LaneId,
    MatchKeySpace, Moment, OperationOptions, OperationOutcome, RetainedBytes,
};
use calandria::{Next, ResourceOwnerId, Span, TimerOwnerId};

use framing::{FixedDecoder, KeyClassifier, request};

type TestSet = ConnectionSet<FixedDecoder, KeyClassifier>;

#[test]
fn failed_generation_recovery_does_not_consume_a_healthy_peer() -> Result<(), Box<dyn Error>> {
    let failed_listener = TcpListener::bind("127.0.0.1:0")?;
    let healthy_listener = TcpListener::bind("127.0.0.1:0")?;
    let (release_failed, failed_peer) = holding_peer(failed_listener, false)?;
    let (release_healthy, healthy_peer) = holding_peer(healthy_listener, true)?;
    let mut set = connection_set()?;
    let failed = set.connect(
        connection_config(failed_peer.address, 10, 20, 30),
        slot_limits()?,
        decoder(),
        KeyClassifier,
    )?;
    let healthy = set.connect(
        connection_config(healthy_peer.address, 11, 21, 31),
        slot_limits()?,
        decoder(),
        KeyClassifier,
    )?;
    let stale_port = set.port(failed)?;
    drive_until_open(&mut set, failed, healthy)?;

    set.open_admission(failed)?;
    let fatal = set
        .finalize(failed, CloseReason::Requested)
        .err()
        .ok_or_else(|| std::io::Error::other("full lifecycle owner accepted closure"))?;
    assert!(matches!(
        fatal,
        EngineError::Invariant(EngineInvariant::LifecyclePublication(_))
    ));
    assert!(matches!(
        set.try_recover(healthy),
        Err(ConnectionRecoveryError::OwnerRunning)
    ));

    let report = set.try_recover(failed)?;
    assert_eq!(report.reason, OwnerFailure::OwnerInvariant);
    assert_eq!(set.snapshot().connections.active(), 1);
    assert_eq!(set.snapshot().poller.registrations(), 1);
    assert!(set.connection_snapshot(failed).is_err());
    assert_eq!(
        set.connection_snapshot(healthy)?.transport,
        TransportState::Open
    );
    let permit = set.reserve(healthy, Moment::ORIGIN, operation_options())?;
    let bytes = request(permit.match_key(), 17);
    set.commit(healthy, permit, OutboundFrame::copy_from_slice(&bytes)?)?;
    drive_until_outcome(&mut set, healthy)?;
    assert!(matches!(
        set.drain_outcomes(healthy)?
            .next()
            .map(bornera::EngineOutcome::into_outcome),
        Some(OperationOutcome::Reply(_))
    ));

    stale_port.close()?;
    let _turn = set.turn_component(Moment::ORIGIN)?;
    assert_eq!(set.snapshot().stale_commands, 1);
    let healthy_report = set.abandon(healthy, OwnerFailure::OwnerInvariant)?;
    assert_eq!(healthy_report.reason, OwnerFailure::OwnerInvariant);
    assert_eq!(set.snapshot().connections.active(), 0);

    release_failed.send(())?;
    release_healthy.send(())?;
    join(failed_peer.handle)?;
    join(healthy_peer.handle)?;
    Ok(())
}

#[test]
fn hosted_turn_localizes_a_fatal_close_publication() -> Result<(), Box<dyn Error>> {
    let failed_listener = TcpListener::bind("127.0.0.1:0")?;
    let healthy_listener = TcpListener::bind("127.0.0.1:0")?;
    let (release_failed, failed_peer) = holding_peer(failed_listener, false)?;
    let (release_healthy, healthy_peer) = holding_peer(healthy_listener, true)?;
    let mut set = connection_set()?;
    let failed = set.connect(
        connection_config(failed_peer.address, 10, 20, 30),
        slot_limits()?,
        decoder(),
        KeyClassifier,
    )?;
    let healthy = set.connect(
        connection_config(healthy_peer.address, 11, 21, 31),
        slot_limits()?,
        decoder(),
        KeyClassifier,
    )?;
    drive_until_open(&mut set, failed, healthy)?;

    set.port(failed)?.close()?;
    let permit = set.reserve(healthy, Moment::ORIGIN, operation_options())?;
    let bytes = request(permit.match_key(), 23);
    set.commit(healthy, permit, OutboundFrame::copy_from_slice(&bytes)?)?;
    drive_until_outcome(&mut set, healthy)?;

    assert_eq!(set.snapshot().owner_failure, None);
    let failed_snapshot = set.connection_snapshot(failed)?;
    assert_eq!(
        failed_snapshot.owner_failure,
        Some(OwnerFailure::OwnerInvariant)
    );
    assert_eq!(failed_snapshot.transport, TransportState::Closed);
    assert_eq!(
        set.connection_snapshot(healthy)?.transport,
        TransportState::Open
    );
    assert!(matches!(
        set.drain_outcomes(healthy)?
            .next()
            .map(bornera::EngineOutcome::into_outcome),
        Some(OperationOutcome::Reply(_))
    ));
    assert_eq!(
        set.try_recover(failed)?.reason,
        OwnerFailure::OwnerInvariant
    );
    let _healthy = set.abandon(healthy, OwnerFailure::OwnerInvariant)?;

    release_failed.send(())?;
    release_healthy.send(())?;
    join(failed_peer.handle)?;
    join(healthy_peer.handle)?;
    Ok(())
}

struct Peer {
    address: std::net::SocketAddr,
    handle: thread::JoinHandle<std::io::Result<()>>,
}

fn holding_peer(listener: TcpListener, echo: bool) -> std::io::Result<(mpsc::Sender<()>, Peer)> {
    let address = listener.local_addr()?;
    let (release, hold) = mpsc::channel();
    let handle = thread::spawn(move || {
        let mut stream = listener.accept()?.0;
        if echo {
            let mut frame = [0_u8; framing::FRAME_BYTES];
            stream.read_exact(&mut frame)?;
            stream.write_all(&frame)?;
        }
        hold.recv()
            .map_err(|_| std::io::Error::other("peer release was dropped"))
    });
    Ok((release, Peer { address, handle }))
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
        RetainedBytes::new(64),
        4,
        RetainedBytes::new(64),
        MatchKeySpace::new(0, 3)?,
    )?;
    Ok(ConnectionSlotLimits::new(
        core,
        DecoderLimits::new(RetainedBytes::new(16), RetainedBytes::new(16)),
        IoLimits::new(nz(4)?, nz(4)?),
        PublicationLimits::new(nz(2)?),
    )?)
}

fn connection_config(
    address: std::net::SocketAddr,
    connection: u64,
    epoch: u64,
    timer: u64,
) -> ConnectionConfig {
    ConnectionConfig::new(
        ConnectionIdentity::new(
            EndpointId::new(1),
            LaneId::new(2),
            ConnectionId::new(connection),
            ConnectionEpoch::new(epoch),
        ),
        address,
        Deadline::at(Moment::from_nanos(u64::MAX)),
        TimerOwnerId::new(timer),
    )
}

fn decoder() -> FixedDecoder {
    FixedDecoder { bytes: Vec::new() }
}

fn operation_options() -> OperationOptions {
    OperationOptions::until(Deadline::at(Moment::from_nanos(u64::MAX)))
        .session()
        .retained_bytes(RetainedBytes::new(8))
        .write_retained_bytes(RetainedBytes::new(8))
}

fn drive_until_open(
    set: &mut TestSet,
    first: bornera::ConnectionToken,
    second: bornera::ConnectionToken,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..128 {
        let turn = set.turn_component(Moment::ORIGIN)?;
        if set.is_transport_open(first)? && set.is_transport_open(second)? {
            return Ok(());
        }
        if turn.next() != Next::Now {
            set.poll_io(Span::from_nanos(10_000_000))?;
        }
    }
    Err(std::io::Error::other("connections did not open within bounded turns").into())
}

fn drive_until_outcome(
    set: &mut TestSet,
    connection: bornera::ConnectionToken,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..128 {
        let turn = set.turn_component(Moment::ORIGIN)?;
        if set.connection_snapshot(connection)?.pending_outcomes != 0 {
            return Ok(());
        }
        if turn.next() != Next::Now {
            set.poll_io(Span::from_nanos(10_000_000))?;
        }
    }
    Err(std::io::Error::other("healthy peer made no bounded progress").into())
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
