//! Set-wide selector failure fencing and recovery.

use std::convert::Infallible;
use std::error::Error;
use std::io;
use std::net::TcpListener;
use std::num::NonZeroUsize;

use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId, LaneId, MatchKey,
    MatchKeySpace, Moment, RetainedBytes,
};
use calandria::{ResourceOwnerId, Retained, Span, TimerOwnerId};
use calandria_mio::MioError;

use crate::set_connect_test::{RecordSocketAttempt, reset_socket_attempt, socket_attempted};
use crate::{
    ConnectError, ConnectionConfig, ConnectionIdentity, ConnectionRecoveryError, ConnectionSet,
    ConnectionSetConfig, ConnectionSetLimits, ConnectionSlotLimits, DecoderLimits, EngineError,
    InboundClassifier, IoLimits, OwnerFailure, PublicationLimits, TransportLimits,
};

#[test]
fn full_set_rejects_before_acquiring_another_socket() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut set = ConnectionSet::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(40)),
        ConnectionSetLimits::new(nz(1)?, nz(1)?, nz(2)?, nz(2)?, nz(1)?),
    )?;
    let _first = connect_fixture(&mut set, &listener, 1)?;
    reset_socket_attempt();

    let result = set.connect_with(
        connection_config(listener.local_addr()?, 2, 12, 22),
        slot_limits()?,
        Decoder,
        Classifier,
        RecordSocketAttempt,
    );

    assert!(matches!(result, Err(ConnectError::ResourceAdmission)));
    assert!(!socket_attempted());
    Ok(())
}

#[test]
fn poll_failure_fences_every_slot_and_preserves_per_connection_recovery()
-> Result<(), Box<dyn Error>> {
    let first_listener = TcpListener::bind("127.0.0.1:0")?;
    let second_listener = TcpListener::bind("127.0.0.1:0")?;
    let mut set = connection_set()?;
    let first = set.connect(
        connection_config(first_listener.local_addr()?, 1, 11, 21),
        slot_limits()?,
        Decoder,
        Classifier,
    )?;
    let second = set.connect(
        connection_config(second_listener.local_addr()?, 2, 12, 22),
        slot_limits()?,
        Decoder,
        Classifier,
    )?;
    let queued_port = set.port(first)?;

    let failure = set.resolve_selector_poll(Err(MioError::from(io::Error::other(
        "injected selector poll failure",
    ))));
    assert!(matches!(failure, Err(EngineError::Mio(_))));
    assert_eq!(set.snapshot().owner_failure, Some(OwnerFailure::Readiness));
    assert_eq!(set.snapshot().ready_connections, 0);
    assert_slot_failed(&set, first)?;
    assert_slot_failed(&set, second)?;
    assert!(queued_port.close().is_err());
    assert!(matches!(
        set.poll_io(Span::ZERO),
        Err(EngineError::OwnerFailed(OwnerFailure::Readiness))
    ));
    assert!(matches!(
        set.turn_component(Moment::ORIGIN),
        Err(EngineError::OwnerFailed(OwnerFailure::Readiness))
    ));
    assert!(matches!(
        set.connect(
            connection_config(first_listener.local_addr()?, 3, 13, 23),
            slot_limits()?,
            Decoder,
            Classifier,
        ),
        Err(ConnectError::OwnerFailed(OwnerFailure::Readiness))
    ));

    assert_eq!(set.try_recover(first)?.reason, OwnerFailure::Readiness);
    assert_eq!(set.try_recover(second)?.reason, OwnerFailure::Readiness);
    assert_eq!(set.snapshot().connections.active(), 0);
    assert!(matches!(
        set.try_recover(first),
        Err(ConnectionRecoveryError::StaleConnection)
    ));
    Ok(())
}

#[test]
fn lifecycle_failure_fences_the_shared_selector_owner() -> Result<(), Box<dyn Error>> {
    let first_listener = TcpListener::bind("127.0.0.1:0")?;
    let second_listener = TcpListener::bind("127.0.0.1:0")?;
    let mut set = connection_set()?;
    let first = connect_fixture(&mut set, &first_listener, 1)?;
    let second = connect_fixture(&mut set, &second_listener, 2)?;
    remove_backend_registration(&mut set, first)?;
    set.port(first)?.close()?;

    assert!(matches!(
        set.turn_component(Moment::ORIGIN),
        Err(EngineError::Mio(_))
    ));
    assert_eq!(set.snapshot().owner_failure, Some(OwnerFailure::Readiness));
    assert_slot_failure(&set, first)?;
    assert_slot_failure(&set, second)?;
    assert!(set.try_recover(first)?.ownership_diverged);
    assert_eq!(set.try_recover(second)?.reason, OwnerFailure::Readiness);
    Ok(())
}

#[test]
fn failed_recovery_cleanup_fences_every_remaining_peer() -> Result<(), Box<dyn Error>> {
    let failed_listener = TcpListener::bind("127.0.0.1:0")?;
    let peer_listener = TcpListener::bind("127.0.0.1:0")?;
    let mut set = connection_set()?;
    let failed = connect_fixture(&mut set, &failed_listener, 1)?;
    let peer = connect_fixture(&mut set, &peer_listener, 2)?;
    remove_backend_registration(&mut set, failed)?;
    set.entry_mut(failed)?
        .slot
        .latch_owner_failure(OwnerFailure::OwnerInvariant);

    let report = set.try_recover(failed)?;
    assert_eq!(report.reason, OwnerFailure::OwnerInvariant);
    assert!(report.ownership_diverged);
    assert_eq!(set.snapshot().owner_failure, Some(OwnerFailure::Readiness));
    assert_slot_failure(&set, peer)?;
    assert_eq!(set.try_recover(peer)?.reason, OwnerFailure::Readiness);
    Ok(())
}

fn assert_slot_failed(
    set: &ConnectionSet<Decoder, Classifier>,
    connection: crate::ConnectionToken,
) -> Result<(), Box<dyn Error>> {
    let snapshot = set.connection_snapshot(connection)?;
    assert_slot_failure(set, connection)?;
    assert_eq!(
        snapshot
            .transport_diagnostic
            .map(|diagnostic| diagnostic.phase),
        Some(crate::TransportFailurePhase::Readiness)
    );
    Ok(())
}

fn assert_slot_failure(
    set: &ConnectionSet<Decoder, Classifier>,
    connection: crate::ConnectionToken,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        set.connection_snapshot(connection)?.owner_failure,
        Some(OwnerFailure::Readiness)
    );
    Ok(())
}

fn connection_set() -> Result<ConnectionSet<Decoder, Classifier>, Box<dyn Error>> {
    Ok(ConnectionSet::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(41)),
        ConnectionSetLimits::new(nz(2)?, nz(2)?, nz(4)?, nz(4)?, nz(2)?),
    )?)
}

fn connect_fixture(
    set: &mut ConnectionSet<Decoder, Classifier>,
    listener: &TcpListener,
    identity: u64,
) -> Result<crate::ConnectionToken, Box<dyn Error>> {
    Ok(set.connect(
        connection_config(
            listener.local_addr()?,
            identity,
            identity.saturating_add(10),
            identity.saturating_add(20),
        ),
        slot_limits()?,
        Decoder,
        Classifier,
    )?)
}

fn remove_backend_registration(
    set: &mut ConnectionSet<Decoder, Classifier>,
    connection: crate::ConnectionToken,
) -> Result<(), Box<dyn Error>> {
    let resource = connection.resource();
    let (poller, resources) = (&mut set.poller, &mut set.resources);
    let (_, entry) = resources
        .get_mut(resource)
        .map_err(|_| io::Error::other("fixture resource disappeared"))?;
    let transport = entry
        .transport
        .as_mut()
        .ok_or_else(|| io::Error::other("fixture transport disappeared"))?;
    poller.deregister(transport, resource)?;
    Ok(())
}

fn slot_limits() -> Result<ConnectionSlotLimits, Box<dyn Error>> {
    let core = ConnectionLimits::new(
        2,
        RetainedBytes::new(16),
        2,
        RetainedBytes::new(16),
        MatchKeySpace::new(0, 1)?,
    )?;
    Ok(ConnectionSlotLimits::new(
        core,
        DecoderLimits::new(RetainedBytes::new(8), RetainedBytes::new(8)),
        IoLimits::new(nz(2)?, nz(8)?),
        TransportLimits::new(RetainedBytes::ZERO),
        PublicationLimits::new(nz(4)?),
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

fn nz(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value)
        .ok_or_else(|| io::Error::other("test fixture bound must be nonzero").into())
}

#[derive(Debug)]
struct Decoder;

impl bornera_core::FrameDecoder for Decoder {
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
