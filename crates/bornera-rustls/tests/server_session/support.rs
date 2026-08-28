//! In-memory rustls peers for deterministic server-session qualification.

use std::{
    error::Error,
    io::{self, Read, Write},
    num::NonZeroUsize,
    sync::Arc,
};

use bornera::{TransportLimits, TransportPressure};
use bornera_rustls::{RustlsServerConfig, RustlsServerSession, RustlsTransportLimits};
use calandria::RetainedBytes;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, SupportedProtocolVersion,
    pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
};

pub(crate) struct SessionConfigs {
    pub(crate) client: Arc<ClientConfig>,
    pub(crate) server: RustlsServerConfig,
}

pub(crate) struct Pair {
    pub(crate) client: ClientConnection,
    pub(crate) server: RustlsServerSession,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HandshakeEvidence {
    pub(crate) completed_with_egress: bool,
    pub(crate) server_write_steps: usize,
}

pub(crate) fn limits(
    writer: usize,
    egress: usize,
    plaintext: usize,
    inbound: usize,
) -> Result<RustlsTransportLimits, Box<dyn Error>> {
    let plaintext_charge = writer
        .checked_add(plaintext)
        .ok_or_else(|| io::Error::other("plaintext charge overflowed"))?;
    Ok(RustlsTransportLimits::new(
        nonzero(writer)?,
        nonzero(egress)?,
        nonzero(plaintext)?,
        TransportPressure::new(
            retained(inbound)?,
            retained(egress)?,
            retained(plaintext_charge)?,
            RetainedBytes::new(131_072),
        )?,
    )?)
}

pub(crate) fn configs(
    limits: RustlsTransportLimits,
    alpn: bool,
) -> Result<SessionConfigs, Box<dyn Error>> {
    configs_inner(limits, alpn, None)
}

fn configs_inner(
    limits: RustlsTransportLimits,
    alpn: bool,
    version: Option<&'static SupportedProtocolVersion>,
) -> Result<SessionConfigs, Box<dyn Error>> {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(Vec::from([String::from("localhost")]))?;
    let certificate = cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let server_builder = match version {
        Some(version) => ServerConfig::builder_with_protocol_versions(&[version]),
        None => ServerConfig::builder(),
    };
    let mut server = server_builder
        .with_no_client_auth()
        .with_single_cert(Vec::from([certificate.clone()]), key)?;
    let mut roots = RootCertStore::empty();
    roots.add(certificate)?;
    let client_builder = match version {
        Some(version) => ClientConfig::builder_with_protocol_versions(&[version]),
        None => ClientConfig::builder(),
    };
    let mut client = client_builder
        .with_root_certificates(roots)
        .with_no_client_auth();
    if alpn {
        server.alpn_protocols = Vec::from([Vec::from(&b"h2"[..]), Vec::from(&b"http/1.1"[..])]);
        client.alpn_protocols = Vec::from([Vec::from(&b"http/1.1"[..])]);
    }
    Ok(SessionConfigs {
        client: Arc::new(client),
        server: RustlsServerConfig::new(Arc::new(server), limits),
    })
}

pub(crate) fn pair(limits: RustlsTransportLimits, alpn: bool) -> Result<Pair, Box<dyn Error>> {
    let configs = configs(limits, alpn)?;
    pair_from_configs(configs, limits)
}

pub(crate) fn pair_for_version(
    limits: RustlsTransportLimits,
    version: &'static SupportedProtocolVersion,
) -> Result<Pair, Box<dyn Error>> {
    let configs = configs_inner(limits, false, Some(version))?;
    pair_from_configs(configs, limits)
}

fn pair_from_configs(
    configs: SessionConfigs,
    limits: RustlsTransportLimits,
) -> Result<Pair, Box<dyn Error>> {
    let client = ClientConnection::new(
        configs.client,
        ServerName::try_from(String::from("localhost"))?,
    )?;
    let server = RustlsServerSession::new(&configs.server, limits.transport_limits())?;
    Ok(Pair { client, server })
}

pub(crate) fn drive_handshake(
    pair: &mut Pair,
    ingress_chunk: usize,
    egress_chunk: usize,
) -> Result<HandshakeEvidence, Box<dyn Error>> {
    let mut evidence = HandshakeEvidence {
        completed_with_egress: false,
        server_write_steps: 0,
    };
    for _step in 0..128 {
        let client_progress = client_to_server(pair, ingress_chunk)?;
        let status = pair.server.status();
        if status.is_handshake_complete() && status.wants_write() {
            if status.is_open() {
                return Err(
                    io::Error::other("server opened before handshake egress drained").into(),
                );
            }
            evidence.completed_with_egress = true;
        }
        let server_steps = server_to_client(pair, egress_chunk)?;
        evidence.server_write_steps = evidence
            .server_write_steps
            .checked_add(server_steps)
            .ok_or_else(|| io::Error::other("server write-step count overflowed"))?;
        if pair.server.status().is_open() && !pair.client.is_handshaking() {
            return Ok(evidence);
        }
        if !client_progress && server_steps == 0 {
            return Err(io::Error::other("in-memory TLS handshake stalled").into());
        }
    }
    Err(io::Error::other("in-memory TLS handshake exceeded its step bound").into())
}

pub(crate) fn client_to_server(pair: &mut Pair, chunk: usize) -> Result<bool, Box<dyn Error>> {
    let ciphertext = take_client_tls(&mut pair.client)?;
    let progressed = !ciphertext.is_empty();
    let mut offset = 0;
    while offset < ciphertext.len() {
        let end = ciphertext.len().min(offset.saturating_add(chunk.max(1)));
        let consumed = pair.server.ingest_tls(&ciphertext[offset..end])?;
        if consumed == 0 {
            return Err(io::Error::other("server consumed no offered TLS input").into());
        }
        offset = offset.saturating_add(consumed);
    }
    Ok(progressed)
}

pub(crate) fn take_client_tls(client: &mut ClientConnection) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut ciphertext = Vec::new();
    while client.wants_write() {
        let written = client.write_tls(&mut ciphertext)?;
        if written == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero).into());
        }
    }
    Ok(ciphertext)
}

pub(crate) fn server_to_client(pair: &mut Pair, chunk: usize) -> Result<usize, Box<dyn Error>> {
    let mut steps = 0_usize;
    let mut ciphertext: Vec<_> = std::iter::repeat_n(0_u8, chunk.max(1)).collect();
    while pair.server.status().wants_write() {
        let produced = pair.server.drain_tls(&mut ciphertext)?;
        if produced == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero).into());
        }
        feed_client(&mut pair.client, &ciphertext[..produced])?;
        steps = steps
            .checked_add(1)
            .ok_or_else(|| io::Error::other("server write-step count overflowed"))?;
    }
    Ok(steps)
}

pub(crate) fn send_client_plaintext(
    pair: &mut Pair,
    plaintext: &[u8],
    chunk: usize,
) -> Result<(), Box<dyn Error>> {
    pair.client.writer().write_all(plaintext)?;
    if !client_to_server(pair, chunk)? {
        return Err(io::Error::other("client produced no application TLS output").into());
    }
    Ok(())
}

pub(crate) fn read_client_plaintext(
    pair: &mut Pair,
    output: &mut [u8],
    chunk: usize,
) -> Result<(), Box<dyn Error>> {
    let _steps = server_to_client(pair, chunk)?;
    pair.client.reader().read_exact(output)?;
    Ok(())
}

fn feed_client(client: &mut ClientConnection, ciphertext: &[u8]) -> Result<(), Box<dyn Error>> {
    let mut remaining = ciphertext;
    while !remaining.is_empty() {
        let consumed = client.read_tls(&mut remaining)?;
        if consumed == 0 {
            return Err(io::Error::other("client consumed no offered TLS input").into());
        }
        let _state = client.process_new_packets()?;
    }
    Ok(())
}

fn nonzero(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value)
        .ok_or_else(|| io::Error::other("TLS test bound must be nonzero").into())
}

fn retained(value: usize) -> Result<RetainedBytes, Box<dyn Error>> {
    Ok(RetainedBytes::new(u64::try_from(value)?))
}

pub(crate) const fn empty_transport_limit() -> TransportLimits {
    TransportLimits::new(RetainedBytes::ZERO)
}
