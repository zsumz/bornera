//! Deterministic qualification of the socket-free bounded rustls server session.

use std::{
    error::Error,
    io::{self, Read, Write},
};

use bornera::{TransportError, TransportFailureKind, TransportFailurePhase};
use bornera_rustls::{
    RustlsDiagnostic, RustlsPeerClosure, RustlsServerSession, RustlsServerSessionError,
};
use rustls::version::{TLS12, TLS13};

#[path = "server_session/support.rs"]
mod support;

#[test]
fn segmented_handshake_and_partial_io_preserve_exact_state() -> Result<(), Box<dyn Error>> {
    let limits = support::limits(1_024, 131_072, 131_072, 131_072)?;
    let mut pair = support::pair(limits, true)?;
    let evidence = support::drive_handshake(&mut pair, 1, 1)?;

    assert!(evidence.completed_with_egress);
    assert!(evidence.server_write_steps > 1);
    assert!(pair.server.status().is_open());
    assert_eq!(pair.server.alpn_protocol(), Some(&b"http/1.1"[..]));
    assert_eq!(pair.server.server_name(), Some("localhost"));
    assert_eq!(pair.server.pressure(), limits.pressure());

    support::send_client_plaintext(&mut pair, b"request", 1)?;
    assert_eq!(pair.server.status().readable_plaintext_bytes(), 7);
    let mut first = [0_u8; 3];
    let mut second = [0_u8; 4];
    assert_eq!(pair.server.read_plaintext(&mut first)?, first.len());
    assert_eq!(pair.server.read_plaintext(&mut second)?, second.len());
    assert_eq!(&first, b"req");
    assert_eq!(&second, b"uest");

    assert_eq!(pair.server.write_plaintext(b"response")?, 8);
    let mut response = [0_u8; 8];
    support::read_client_plaintext(&mut pair, &mut response, 1)?;
    assert_eq!(&response, b"response");
    Ok(())
}

#[test]
fn clean_close_notify_is_distinct_from_raw_eof() -> Result<(), Box<dyn Error>> {
    let limits = support::limits(1_024, 131_072, 131_072, 131_072)?;
    let mut pair = support::pair(limits, false)?;
    let _evidence = support::drive_handshake(&mut pair, 7, 11)?;

    pair.client.send_close_notify();
    assert!(support::client_to_server(&mut pair, 5)?);
    assert_eq!(
        pair.server.status().peer_closure(),
        RustlsPeerClosure::Clean
    );
    pair.server.finish_input()?;
    let mut byte = [0_u8; 1];
    assert_eq!(pair.server.read_plaintext(&mut byte)?, 0);

    pair.server.send_close_notify()?;
    assert!(pair.server.status().close_notify_sent());
    assert!(!pair.server.status().is_open());
    let steps = support::server_to_client(&mut pair, 1)?;
    assert!(steps > 1);
    assert_eq!(pair.client.reader().read(&mut byte)?, 0);
    Ok(())
}

#[test]
fn tls12_and_tls13_each_qualify_handshake_and_application_io() -> Result<(), Box<dyn Error>> {
    for version in [&TLS12, &TLS13] {
        let limits = support::limits(1_024, 131_072, 131_072, 131_072)?;
        let mut pair = support::pair_for_version(limits, version)?;
        let _evidence = support::drive_handshake(&mut pair, 5, 7)?;
        assert_eq!(pair.client.protocol_version(), Some(version.version));

        support::send_client_plaintext(&mut pair, b"ping", 2)?;
        let mut request = [0_u8; 4];
        assert_eq!(pair.server.read_plaintext(&mut request)?, 4);
        assert_eq!(&request, b"ping");
        assert_eq!(pair.server.write_plaintext(b"pong")?, 4);
        let mut response = [0_u8; 4];
        support::read_client_plaintext(&mut pair, &mut response, 3)?;
        assert_eq!(&response, b"pong");
    }
    Ok(())
}

#[test]
fn truncation_waits_for_authenticated_plaintext_to_drain() -> Result<(), Box<dyn Error>> {
    let limits = support::limits(1_024, 131_072, 131_072, 131_072)?;
    let mut pair = support::pair(limits, false)?;
    let _evidence = support::drive_handshake(&mut pair, 13, 17)?;
    support::send_client_plaintext(&mut pair, b"authenticated", 2)?;

    pair.server.finish_input()?;
    assert_eq!(
        pair.server.status().peer_closure(),
        RustlsPeerClosure::Truncated
    );
    assert_eq!(pair.server.status().failure(), None);
    let mut plaintext = [0_u8; 13];
    assert_eq!(pair.server.read_plaintext(&mut plaintext)?, 13);
    assert_eq!(&plaintext, b"authenticated");

    let mut byte = [0_u8; 1];
    let error = pair
        .server
        .read_plaintext(&mut byte)
        .err()
        .ok_or_else(|| io::Error::other("truncated input was accepted"))?;
    assert_diagnostic(
        &error,
        TransportFailurePhase::TransportRead,
        TransportFailureKind::Truncated,
        RustlsDiagnostic::Truncated,
    );
    assert_eq!(pair.server.status().failure(), Some(error.diagnostic()));
    Ok(())
}

#[test]
fn handshake_protocol_failure_is_stable_and_latched() -> Result<(), Box<dyn Error>> {
    let limits = support::limits(1_024, 131_072, 131_072, 131_072)?;
    let configs = support::configs(limits, false)?;
    let mut server = RustlsServerSession::new(&configs.server, limits.transport_limits())?;

    let error = server
        .ingest_tls(b"GET / HTTP/1.1\r\n")
        .err()
        .ok_or_else(|| io::Error::other("cleartext handshake input was accepted"))?;
    assert_diagnostic(
        &error,
        TransportFailurePhase::Establishment,
        TransportFailureKind::Protocol,
        RustlsDiagnostic::Protocol,
    );
    assert_eq!(server.status().failure(), Some(error.diagnostic()));
    let repeated = server
        .write_plaintext(b"forbidden")
        .err()
        .ok_or_else(|| io::Error::other("failed session accepted plaintext"))?;
    assert_eq!(repeated.diagnostic(), error.diagnostic());
    Ok(())
}

#[test]
fn capacity_is_checked_at_admission_and_after_tls_transitions() -> Result<(), Box<dyn Error>> {
    let limits = support::limits(1_024, 131_072, 4, 131_072)?;
    let configs = support::configs(limits, false)?;
    let admission = RustlsServerSession::new(&configs.server, support::empty_transport_limit())
        .err()
        .ok_or_else(|| io::Error::other("undersized session capacity was accepted"))?;
    assert!(matches!(
        admission,
        RustlsServerSessionError::Capacity { .. }
    ));

    let mut pair = support::pair(limits, false)?;
    let _evidence = support::drive_handshake(&mut pair, 31, 37)?;
    pair.client.writer().write_all(b"12345")?;
    let error = support::client_to_server(&mut pair, 131_072)
        .err()
        .ok_or_else(|| io::Error::other("oversized plaintext was accepted"))?;
    let error = transport_error(error.as_ref())?;
    assert_diagnostic(
        error,
        TransportFailurePhase::TransportRead,
        TransportFailureKind::Capacity,
        RustlsDiagnostic::Capacity,
    );
    assert_eq!(pair.server.status().failure(), Some(error.diagnostic()));
    Ok(())
}

#[test]
fn ingress_and_handshake_egress_obey_configured_capacity() -> Result<(), Box<dyn Error>> {
    let ingress_limits = support::limits(1_024, 131_072, 131_072, 1)?;
    let mut ingress_pair = support::pair(ingress_limits, false)?;
    let hello = support::take_client_tls(&mut ingress_pair.client)?;
    assert!(hello.len() > 1);
    assert_eq!(ingress_pair.server.ingest_tls(&hello)?, 1);

    let egress_limits = support::limits(1_024, 1, 131_072, 131_072)?;
    let mut egress_pair = support::pair(egress_limits, false)?;
    let error = support::client_to_server(&mut egress_pair, 131_072)
        .err()
        .ok_or_else(|| io::Error::other("oversized handshake egress was accepted"))?;
    let error = transport_error(error.as_ref())?;
    assert_diagnostic(
        error,
        TransportFailurePhase::Establishment,
        TransportFailureKind::Capacity,
        RustlsDiagnostic::Capacity,
    );
    Ok(())
}

#[test]
fn caller_state_misuse_has_a_distinct_stable_diagnostic() -> Result<(), Box<dyn Error>> {
    let limits = support::limits(1_024, 131_072, 131_072, 131_072)?;
    let mut pair = support::pair(limits, false)?;
    let error = pair
        .server
        .write_plaintext(b"before handshake")
        .err()
        .ok_or_else(|| io::Error::other("pre-handshake plaintext was accepted"))?;
    assert_diagnostic(
        &error,
        TransportFailurePhase::Write,
        TransportFailureKind::Contract,
        RustlsDiagnostic::CallerState,
    );
    assert_eq!(pair.server.status().failure(), None);
    Ok(())
}

fn assert_diagnostic(
    error: &TransportError,
    phase: TransportFailurePhase,
    failure: TransportFailureKind,
    code: RustlsDiagnostic,
) {
    let diagnostic = error.diagnostic();
    assert_eq!(diagnostic.phase, phase);
    assert_eq!(diagnostic.failure, failure);
    assert_eq!(diagnostic.code, Some(code.code()));
}

fn transport_error<'error>(
    error: &'error (dyn Error + 'static),
) -> Result<&'error TransportError, io::Error> {
    error
        .downcast_ref::<TransportError>()
        .ok_or_else(|| io::Error::other("expected bounded transport error"))
}
