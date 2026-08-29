//! Deterministic qualification of the socket-free bounded rustls client session.

use std::{
    error::Error,
    io::{self, Read, Write},
};

use bornera::{TransportFailureKind, TransportFailurePhase};
use bornera_rustls::{
    RustlsClientSession, RustlsClientSessionError, RustlsDiagnostic, RustlsPeerClosure,
    RustlsTransportConfig,
};
use rustls::ServerConnection;

#[path = "client_session/support.rs"]
mod support;

struct Pair {
    client: RustlsClientSession,
    server: ServerConnection,
}

#[test]
fn segmented_handshake_and_application_io_are_bounded() -> Result<(), Box<dyn Error>> {
    let limits = support::limits(1_024, 131_072, 131_072, 131_072)?;
    let mut pair = pair(limits, true)?;
    drive_handshake(&mut pair, 1, 1)?;

    assert!(pair.client.status().is_open());
    assert_eq!(pair.client.alpn_protocol(), Some(&b"http/1.1"[..]));
    assert_eq!(pair.client.pressure(), limits.pressure());

    pair.client.write_plaintext(b"request")?;
    let mut request = [0_u8; 7];
    client_to_server(&mut pair, 2)?;
    pair.server.reader().read_exact(&mut request)?;
    assert_eq!(&request, b"request");

    pair.server.writer().write_all(b"response")?;
    server_to_client(&mut pair, 3)?;
    let mut response = [0_u8; 8];
    assert_eq!(pair.client.read_plaintext(&mut response)?, response.len());
    assert_eq!(&response, b"response");
    Ok(())
}

#[test]
fn clean_close_and_raw_eof_remain_distinct() -> Result<(), Box<dyn Error>> {
    let limits = support::limits(1_024, 131_072, 131_072, 131_072)?;
    let mut clean = pair(limits, false)?;
    drive_handshake(&mut clean, 5, 7)?;
    clean.server.send_close_notify();
    server_to_client(&mut clean, 5)?;
    assert_eq!(
        clean.client.status().peer_closure(),
        RustlsPeerClosure::Clean
    );
    clean.client.finish_input()?;

    let mut truncated = pair(limits, false)?;
    drive_handshake(&mut truncated, 5, 7)?;
    let error = truncated
        .client
        .finish_input()
        .err()
        .ok_or_else(|| io::Error::other("raw TLS EOF was accepted"))?;
    assert_eq!(error.diagnostic().failure, TransportFailureKind::Truncated);
    assert_eq!(
        truncated.client.status().peer_closure(),
        RustlsPeerClosure::Truncated
    );
    Ok(())
}

#[test]
fn admission_and_transition_capacity_fail_closed() -> Result<(), Box<dyn Error>> {
    let limits = support::limits(1_024, 131_072, 4, 131_072)?;
    let configs = support::configs(limits, false)?;
    let client_config =
        RustlsTransportConfig::for_server_name(configs.client.clone(), "localhost", limits)?;
    let admission = RustlsClientSession::new(&client_config, support::empty_transport_limit())
        .err()
        .ok_or_else(|| io::Error::other("undersized client capacity was accepted"))?;
    assert!(matches!(
        admission,
        RustlsClientSessionError::Capacity { .. }
    ));

    let mut pair = pair(limits, false)?;
    drive_handshake(&mut pair, 31, 37)?;
    pair.server.writer().write_all(b"12345")?;
    let error = server_to_client(&mut pair, 131_072)
        .err()
        .ok_or_else(|| io::Error::other("oversized client plaintext was accepted"))?;
    let transport = error
        .downcast_ref::<bornera::TransportError>()
        .ok_or_else(|| io::Error::other("unexpected client capacity error"))?;
    assert_eq!(
        transport.diagnostic().phase,
        TransportFailurePhase::TransportRead
    );
    assert_eq!(
        transport.diagnostic().failure,
        TransportFailureKind::Capacity
    );
    assert_eq!(
        transport.diagnostic().code,
        Some(RustlsDiagnostic::Capacity.code())
    );
    assert_eq!(pair.client.status().failure(), Some(transport.diagnostic()));
    Ok(())
}

fn pair(limits: bornera_rustls::RustlsTransportLimits, alpn: bool) -> Result<Pair, Box<dyn Error>> {
    let configs = support::configs(limits, alpn)?;
    let config = RustlsTransportConfig::for_server_name(configs.client, "localhost", limits)?;
    Ok(Pair {
        client: RustlsClientSession::new(&config, limits.transport_limits())?,
        server: ServerConnection::new(configs.server.server_config().clone())?,
    })
}

fn drive_handshake(
    pair: &mut Pair,
    client_chunk: usize,
    server_chunk: usize,
) -> Result<(), Box<dyn Error>> {
    for _step in 0..128 {
        let client_progress = client_to_server(pair, client_chunk)?;
        let server_progress = server_to_client(pair, server_chunk)?;
        if pair.client.status().is_open() && !pair.server.is_handshaking() {
            return Ok(());
        }
        if !client_progress && !server_progress {
            return Err(io::Error::other("in-memory TLS handshake stalled").into());
        }
    }
    Err(io::Error::other("in-memory TLS handshake exceeded its step bound").into())
}

fn client_to_server(pair: &mut Pair, chunk: usize) -> Result<bool, Box<dyn Error>> {
    let mut progressed = false;
    let mut ciphertext: Vec<_> = std::iter::repeat_n(0_u8, chunk.max(1)).collect();
    while pair.client.status().wants_write() {
        let produced = pair.client.drain_tls(&mut ciphertext)?;
        if produced == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero).into());
        }
        let mut input = &ciphertext[..produced];
        let read = pair.server.read_tls(&mut input)?;
        if read == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
        }
        pair.server.process_new_packets()?;
        progressed = true;
    }
    Ok(progressed)
}

fn server_to_client(pair: &mut Pair, chunk: usize) -> Result<bool, Box<dyn Error>> {
    let mut progressed = false;
    let mut ciphertext: Vec<_> = std::iter::repeat_n(0_u8, chunk.max(1)).collect();
    while pair.server.wants_write() {
        let mut sink = &mut ciphertext[..];
        let produced = pair.server.write_tls(&mut sink)?;
        if produced == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero).into());
        }
        let consumed = pair.client.ingest_tls(&ciphertext[..produced])?;
        if consumed != produced {
            return Err(
                io::Error::other("client only partially consumed bounded TLS input").into(),
            );
        }
        progressed = true;
    }
    Ok(progressed)
}
