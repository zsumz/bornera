//! Shared-selector TLS failure isolation and healthy-peer progression.

use std::{error::Error, num::NonZeroUsize, sync::mpsc, time::Duration};

use bornera::{
    ConnectionConfig, ConnectionIdentity, ConnectionSet, ConnectionSetConfig, ConnectionSetLimits,
    OutboundFrame, TransportFailureKind, TransportState,
};
use bornera_core::{
    ConnectionEpoch, ConnectionId, EndpointId, LaneId, OperationOptions, OperationOutcome,
    RetainedBytes,
};
use calandria::{Deadline, Moment, Next, ResourceOwnerId, Span, TimerOwnerId};

use super::{config, far_deadline, owner, protocol, tls};
use bornera_rustls::{RustlsDiagnostic, RustlsTransport};

type TlsSet = ConnectionSet<protocol::Decoder, protocol::Classifier, RustlsTransport>;

#[test]
fn tls_failure_isolated_while_healthy_peer_completes() -> Result<(), Box<dyn Error>> {
    let configs = tls::TlsConfigs::new()?;
    let limits = config::transport_limits()?;
    let (clean_sender, clean_receiver) = mpsc::channel();
    let healthy_server = tls::echo_server(configs.server.clone(), clean_sender)?;
    let failed_server = tls::rejecting_server(configs.server)?;
    let mut set = connection_set()?;
    let healthy = set.connect_with(
        connection_config(healthy_server.address(), 1, 11, 21),
        owner::slot_limits(limits, 2, 64)?,
        protocol::Decoder::new(),
        protocol::Classifier,
        config::connector(configs.client, limits)?,
    )?;
    let failed = set.connect_with(
        connection_config(failed_server.address(), 2, 12, 22),
        owner::slot_limits(limits, 2, 64)?,
        protocol::Decoder::new(),
        protocol::Classifier,
        config::connector(configs.untrusted_client, limits)?,
    )?;

    drive_until(&mut set, |set| {
        state(set, healthy) == TransportState::Open && state(set, failed) == TransportState::Closed
    })?;
    assert!(set.snapshot().owner_failure.is_none());
    assert!(matches!(
        set.connection_snapshot(failed)?.transport_diagnostic,
        Some(diagnostic)
            if diagnostic.failure == TransportFailureKind::Certificate
                && diagnostic.code == Some(RustlsDiagnostic::Certificate.code())
    ));

    set.open_admission(healthy)?;
    let options = OperationOptions::until(far_deadline())
        .retained_bytes(RetainedBytes::new(8))
        .write_retained_bytes(RetainedBytes::new(8));
    let permit = set.reserve(healthy, Moment::ORIGIN, options)?;
    let expected = protocol::request(permit.match_key(), 818);
    let operation = set.commit(healthy, permit, OutboundFrame::copy_from_slice(&expected)?)?;
    drive_until(&mut set, |set| pending_outcomes(set, healthy) == 1)?;
    let outcome = set
        .drain_outcomes(healthy)?
        .next()
        .ok_or_else(|| std::io::Error::other("healthy TLS operation had no outcome"))?;
    assert_eq!(outcome.operation(), operation);
    assert_eq!(
        outcome.into_outcome(),
        OperationOutcome::Reply(protocol::Frame::from_bytes(expected))
    );

    set.begin_drain(healthy, far_deadline())?;
    drive_until(&mut set, |set| {
        state(set, healthy) == TransportState::Closed
    })?;
    clean_receiver.recv_timeout(Duration::from_secs(2))?;
    healthy_server.join()?;
    failed_server.join()?;
    Ok(())
}

fn connection_set() -> Result<TlsSet, Box<dyn Error>> {
    Ok(ConnectionSet::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(30)),
        ConnectionSetLimits::new(nz(2)?, nz(2)?, nz(8)?, nz(8)?, nz(1)?),
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
            EndpointId::new(3),
            LaneId::new(4),
            ConnectionId::new(connection),
            ConnectionEpoch::new(epoch),
        ),
        address,
        Deadline::at(Moment::from_nanos(u64::MAX)),
        TimerOwnerId::new(timer),
    )
}

fn drive_until(
    set: &mut TlsSet,
    mut complete: impl FnMut(&TlsSet) -> bool,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..2_048 {
        let turn = set.turn_component(Moment::ORIGIN)?;
        if complete(set) {
            return Ok(());
        }
        if turn.next() != Next::Now {
            let _wait = set.poll_io(Span::from_nanos(10_000_000))?;
        }
    }
    Err(std::io::Error::other("shared TLS set made no bounded progress").into())
}

fn state(set: &TlsSet, connection: bornera::ConnectionToken) -> TransportState {
    set.connection_snapshot(connection)
        .map_or(TransportState::Closed, |snapshot| snapshot.transport)
}

fn pending_outcomes(set: &TlsSet, connection: bornera::ConnectionToken) -> usize {
    set.connection_snapshot(connection)
        .map_or(0, |snapshot| snapshot.pending_outcomes)
}

fn nz(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value)
        .ok_or_else(|| std::io::Error::other("test bound must be nonzero").into())
}
