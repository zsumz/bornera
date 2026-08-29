//! TLS identities and conservative bounds for client-session qualification.

use std::{error::Error, io, num::NonZeroUsize, sync::Arc};

use bornera::{TransportLimits, TransportPressure};
use bornera_rustls::{RustlsServerConfig, RustlsTransportLimits};
use calandria::RetainedBytes;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
};

pub(crate) struct SessionConfigs {
    pub(crate) client: Arc<ClientConfig>,
    pub(crate) server: RustlsServerConfig,
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
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(Vec::from([String::from("localhost")]))?;
    let certificate = cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let mut server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(Vec::from([certificate.clone()]), key)?;
    let mut roots = RootCertStore::empty();
    roots.add(certificate)?;
    let mut client = ClientConfig::builder()
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

pub(crate) const fn empty_transport_limit() -> TransportLimits {
    TransportLimits::new(RetainedBytes::ZERO)
}

fn nonzero(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value).ok_or_else(|| io::Error::other("TLS bound must be nonzero").into())
}

fn retained(value: usize) -> Result<RetainedBytes, Box<dyn Error>> {
    Ok(RetainedBytes::new(u64::try_from(value)?))
}
