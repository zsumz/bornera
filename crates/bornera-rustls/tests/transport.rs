//! End-to-end bounded rustls transport qualification over loopback TCP.

use std::{error::Error, io::Read, net::TcpListener, sync::mpsc, thread, time::Duration};

use bornera::{
    ConnectionEvent, OutboundFrame, TransportFailureKind, TransportFailurePhase, TransportState,
};
use bornera_core::{
    CloseReason, Deadline, Moment, OperationOptions, OperationOutcome, RetainedBytes,
};
use bornera_rustls::RustlsDiagnostic;
use calandria::{Next, Span};

#[path = "support/config.rs"]
mod config;
#[path = "transport/configuration.rs"]
mod configuration;
#[path = "support/owner.rs"]
mod owner;
#[path = "support/protocol.rs"]
mod protocol;
#[path = "transport/shared_set.rs"]
mod shared_set;
#[path = "support/tls.rs"]
mod tls;

#[test]
fn frame_acceptance_precedes_ciphertext_flush_and_echo_reply() -> Result<(), Box<dyn Error>> {
    let tls = tls::TlsConfigs::new()?;
    let limits = config::transport_limits()?;
    let (clean_sender, clean_receiver) = mpsc::channel();
    let server = tls::echo_server(tls.server, clean_sender)?;
    let connector = config::connector(tls.client, limits)?;
    let mut owner = owner::connect(server.address(), connector, limits, 1, 32)?;
    drive_until_open(&mut owner)?;
    owner.open_admission()?;

    let options = OperationOptions::until(far_deadline())
        .retained_bytes(RetainedBytes::new(8))
        .write_retained_bytes(RetainedBytes::new(8));
    let permit = owner.reserve(Moment::ORIGIN, options)?;
    let expected = protocol::request(permit.match_key(), 77);
    let operation = owner.commit(permit, OutboundFrame::copy_from_slice(&expected)?)?;
    drive_until_frame_released(&mut owner)?;

    let outcome = drive_until_outcome(&mut owner)?;
    assert_eq!(outcome.operation(), operation);
    assert_eq!(
        outcome.into_outcome(),
        OperationOutcome::Reply(protocol::Frame::from_bytes(expected))
    );
    assert_eq!(
        owner.snapshot()?.transport_retained_limit,
        Some(limits.transport_limits().retained_bytes())
    );

    owner.begin_drain(far_deadline())?;
    drive_until_closed(&mut owner)?;
    clean_receiver.recv_timeout(Duration::from_secs(2))?;
    server.join()?;
    let events: Vec<_> = owner.drain_events()?.collect();
    assert!(matches!(
        events.as_slice(),
        [
            ConnectionEvent::TransportOpened { sequence: 1, .. },
            ConnectionEvent::AdmissionOpened { sequence: 2, .. },
            ConnectionEvent::Closing { sequence: 3, .. },
            ConnectionEvent::Closed { sequence: 4, .. }
        ]
    ));
    Ok(())
}

#[test]
fn connect_deadline_covers_a_stalled_tls_handshake() -> Result<(), Box<dyn Error>> {
    let tls = tls::TlsConfigs::new()?;
    let limits = config::transport_limits()?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (hello_sender, hello_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let server = thread::spawn(move || -> std::io::Result<()> {
        let (mut socket, _) = listener.accept()?;
        let mut byte = [0_u8; 1];
        socket.read_exact(&mut byte)?;
        hello_sender
            .send(())
            .map_err(|_| std::io::Error::other("hello observer dropped"))?;
        release_receiver
            .recv()
            .map_err(|_| std::io::Error::other("stalled server release dropped"))
    });
    let connector = config::connector(tls.client, limits)?;
    let deadline = Deadline::at(Moment::from_nanos(5));
    let mut owner = owner::connect_until(address, connector, limits, deadline, 2, 64)?;
    let mut hello_observed = false;
    for _ in 0..1_024 {
        let turn = owner.turn_component(Moment::ORIGIN)?;
        if hello_receiver.try_recv().is_ok() {
            hello_observed = true;
            break;
        }
        poll_if_needed(&mut owner, turn.next())?;
    }
    assert!(hello_observed);
    assert!(owner.snapshot()?.transport == TransportState::Connecting);
    let _turn = owner.turn_component(Moment::from_nanos(5))?;
    assert_eq!(owner.snapshot()?.transport, TransportState::Closed);
    let events: Vec<_> = owner.drain_events()?.collect();
    assert!(matches!(
        events.as_slice(),
        [
            ConnectionEvent::Closing {
                reason: CloseReason::ConnectTimedOut,
                ..
            },
            ConnectionEvent::Closed { .. }
        ]
    ));
    release_sender.send(())?;
    server
        .join()
        .map_err(|_| std::io::Error::other("stalled TLS server panicked"))??;
    Ok(())
}

#[test]
fn raw_eof_without_close_notify_is_truncated() -> Result<(), Box<dyn Error>> {
    let tls = tls::TlsConfigs::new()?;
    let limits = config::transport_limits()?;
    let server = tls::truncating_server(tls.server)?;
    let connector = config::connector(tls.client, limits)?;
    let mut owner = owner::connect(server.address(), connector, limits, 2, 64)?;
    drive_until_closed(&mut owner)?;
    server.join()?;
    let snapshot = owner.snapshot()?;
    assert_eq!(snapshot.transport, TransportState::Closed);
    assert!(matches!(
        snapshot.transport_diagnostic,
        Some(diagnostic)
            if diagnostic.phase == TransportFailurePhase::TransportRead
                && diagnostic.failure == TransportFailureKind::Truncated
                && diagnostic.code == Some(RustlsDiagnostic::Truncated.code())
    ));
    Ok(())
}

#[test]
fn certificate_failure_is_bounded_and_connection_local() -> Result<(), Box<dyn Error>> {
    let tls = tls::TlsConfigs::new()?;
    let limits = config::transport_limits()?;
    let server = tls::rejecting_server(tls.server)?;
    let connector = config::connector(tls.untrusted_client, limits)?;
    let mut owner = owner::connect(server.address(), connector, limits, 2, 64)?;
    drive_until_closed(&mut owner)?;
    server.join()?;
    assert!(matches!(
        owner.snapshot()?.transport_diagnostic,
        Some(diagnostic)
            if diagnostic.phase == TransportFailurePhase::Establishment
                && diagnostic.failure == TransportFailureKind::Certificate
                && diagnostic.code == Some(RustlsDiagnostic::Certificate.code())
    ));
    Ok(())
}

#[test]
fn logical_server_name_failure_is_distinct_from_certificate_trust() -> Result<(), Box<dyn Error>> {
    let tls = tls::TlsConfigs::new()?;
    let limits = config::transport_limits()?;
    let server = tls::rejecting_server(tls.server)?;
    let connector =
        config::connector_for_server_name(tls.client, limits, "broker.invalid.example")?;
    let mut owner = owner::connect(server.address(), connector, limits, 2, 64)?;
    drive_until_closed(&mut owner)?;
    server.join()?;
    assert!(matches!(
        owner.snapshot()?.transport_diagnostic,
        Some(diagnostic)
            if diagnostic.phase == TransportFailurePhase::Establishment
                && diagnostic.failure == TransportFailureKind::ServerName
                && diagnostic.code == Some(RustlsDiagnostic::ServerName.code())
    ));
    Ok(())
}

#[test]
fn readable_plaintext_bound_fails_after_irreversible_frame_acceptance() -> Result<(), Box<dyn Error>>
{
    let tls = tls::TlsConfigs::new()?;
    let limits = config::transport_limits_with(1_024, 131_072, 4)?;
    let server = tls::replying_server(tls.server)?;
    let connector = config::connector(tls.client, limits)?;
    let mut owner = owner::connect(server.address(), connector, limits, 2, 64)?;
    drive_until_open(&mut owner)?;
    owner.open_admission()?;
    let options = OperationOptions::until(far_deadline())
        .retained_bytes(RetainedBytes::new(8))
        .write_retained_bytes(RetainedBytes::new(8));
    let permit = owner.reserve(Moment::ORIGIN, options)?;
    let frame = protocol::request(permit.match_key(), 91);
    let _operation = owner.commit(permit, OutboundFrame::copy_from_slice(&frame)?)?;
    drive_until_closed(&mut owner)?;
    server.join()?;
    let snapshot = owner.snapshot()?;
    assert!(matches!(
        snapshot.transport_diagnostic,
        Some(diagnostic)
            if diagnostic.phase == TransportFailurePhase::TransportRead
                && diagnostic.failure == TransportFailureKind::Capacity
                && diagnostic.code == Some(RustlsDiagnostic::Capacity.code())
    ));
    let outcome = owner
        .drain_outcomes()?
        .next()
        .ok_or_else(|| std::io::Error::other("accepted frame did not terminate"))?;
    assert!(matches!(
        outcome.into_outcome(),
        OperationOutcome::Failed {
            delivery: bornera_core::Delivery::PossiblySent,
            ..
        }
    ));
    Ok(())
}

fn drive_until_open(owner: &mut owner::TestOwner) -> Result<(), Box<dyn Error>> {
    for _ in 0..1_024 {
        let turn = owner.turn_component(Moment::ORIGIN)?;
        if owner.is_transport_open()? {
            return Ok(());
        }
        poll_if_needed(owner, turn.next())?;
    }
    Err(std::io::Error::other("TLS transport did not open within bounded turns").into())
}

fn drive_until_frame_released(owner: &mut owner::TestOwner) -> Result<(), Box<dyn Error>> {
    for _ in 0..1_024 {
        let turn = owner.turn_component(Moment::ORIGIN)?;
        if owner.snapshot()?.queued_write_frames == 0 {
            assert_eq!(turn.next(), Next::Now);
            assert!(owner.drain_outcomes()?.next().is_none());
            return Ok(());
        }
        poll_if_needed(owner, turn.next())?;
    }
    Err(std::io::Error::other("TLS frame never left Bornera write ownership").into())
}

fn drive_until_outcome(
    owner: &mut owner::TestOwner,
) -> Result<bornera::EngineOutcome<protocol::Frame>, Box<dyn Error>> {
    for _ in 0..1_024 {
        let turn = owner.turn_component(Moment::ORIGIN)?;
        if let Some(outcome) = owner.drain_outcomes()?.next() {
            return Ok(outcome);
        }
        poll_if_needed(owner, turn.next())?;
    }
    Err(std::io::Error::other("TLS reply did not terminate within bounded turns").into())
}

fn drive_until_closed(owner: &mut owner::TestOwner) -> Result<(), Box<dyn Error>> {
    for _ in 0..1_024 {
        let turn = owner.turn_component(Moment::ORIGIN)?;
        if owner.snapshot()?.transport == TransportState::Closed {
            return Ok(());
        }
        poll_if_needed(owner, turn.next())?;
    }
    Err(std::io::Error::other("TLS transport did not close within bounded turns").into())
}

fn poll_if_needed(owner: &mut owner::TestOwner, next: Next) -> Result<(), Box<dyn Error>> {
    if next != Next::Now && next != Next::Stop {
        let _wait = owner.poll_io(Span::from_nanos(10_000_000))?;
    }
    Ok(())
}

const fn far_deadline() -> Deadline {
    Deadline::at(Moment::from_nanos(u64::MAX))
}
