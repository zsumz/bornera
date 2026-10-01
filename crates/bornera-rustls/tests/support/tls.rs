//! Ephemeral trusted rustls configuration and blocking loopback servers.

use std::{
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener},
    sync::{Arc, mpsc},
    thread,
};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
    pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
};

use super::protocol::FRAME_BYTES;

pub(crate) struct TlsConfigs {
    pub(crate) client: Arc<ClientConfig>,
    pub(crate) untrusted_client: Arc<ClientConfig>,
    pub(crate) server: Arc<ServerConfig>,
}

impl TlsConfigs {
    pub(crate) fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_protocol_versions(rustls::DEFAULT_VERSIONS)
    }

    pub(crate) fn with_protocol_versions(
        versions: &[&'static rustls::SupportedProtocolVersion],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(Vec::from([String::from("localhost")]))?;
        let certificate = cert.der().clone();
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
        let server = ServerConfig::builder_with_protocol_versions(versions)
            .with_no_client_auth()
            .with_single_cert(Vec::from([certificate.clone()]), key)?;
        let mut roots = RootCertStore::empty();
        roots.add(certificate)?;
        let client = ClientConfig::builder_with_protocol_versions(versions)
            .with_root_certificates(roots)
            .with_no_client_auth();
        let untrusted_client = ClientConfig::builder_with_protocol_versions(versions)
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        Ok(Self {
            client: Arc::new(client),
            untrusted_client: Arc::new(untrusted_client),
            server: Arc::new(server),
        })
    }
}

pub(crate) struct ServerHandle {
    address: SocketAddr,
    join: thread::JoinHandle<io::Result<()>>,
}

impl ServerHandle {
    pub(crate) const fn address(&self) -> SocketAddr {
        self.address
    }

    pub(crate) fn join(self) -> Result<(), Box<dyn std::error::Error>> {
        self.join
            .join()
            .map_err(|_| io::Error::other("TLS server thread panicked"))??;
        Ok(())
    }
}

pub(crate) fn echo_server(
    config: Arc<ServerConfig>,
    clean_close: mpsc::Sender<()>,
) -> io::Result<ServerHandle> {
    spawn_server(config, move |mut stream| {
        let mut frame = [0_u8; FRAME_BYTES];
        stream.read_exact(&mut frame)?;
        stream.write_all(&frame)?;
        stream.flush()?;
        let mut eof = [0_u8; 1];
        if stream.read(&mut eof)? == 0 {
            clean_close
                .send(())
                .map_err(|_| io::Error::other("clean-close receiver dropped"))?;
        }
        Ok(())
    })
}

pub(crate) fn truncating_server(config: Arc<ServerConfig>) -> io::Result<ServerHandle> {
    spawn_server(config, |stream| stream.sock.shutdown(Shutdown::Both))
}

pub(crate) fn replying_server(config: Arc<ServerConfig>) -> io::Result<ServerHandle> {
    spawn_server(config, |mut stream| {
        let mut frame = [0_u8; FRAME_BYTES];
        stream.read_exact(&mut frame)?;
        stream.write_all(&frame)?;
        stream.flush()
    })
}

pub(crate) fn rejecting_server(config: Arc<ServerConfig>) -> io::Result<ServerHandle> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let join = thread::spawn(move || {
        let (socket, _) = listener.accept()?;
        let mut connection = ServerConnection::new(config)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let _rejected = connection.complete_io(&mut &socket);
        Ok(())
    });
    Ok(ServerHandle { address, join })
}

pub(crate) fn cross_epoch_server(config: Arc<ServerConfig>) -> io::Result<ServerHandle> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let join = thread::spawn(move || {
        let (mut socket, _) = listener.accept()?;
        socket.set_read_timeout(Some(std::time::Duration::from_secs(2)))?;
        socket.set_write_timeout(Some(std::time::Duration::from_secs(2)))?;
        let mut connection = ServerConnection::new(config)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        for _ in 0..16 {
            if connection.wants_write() {
                break;
            }
            if connection.read_tls(&mut socket)? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            connection
                .process_new_packets()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        }
        if connection.protocol_version() != Some(rustls::ProtocolVersion::TLSv1_3) {
            return Err(io::Error::other("hostile peer did not negotiate TLS 1.3"));
        }
        let mut flight = Vec::new();
        connection.write_tls(&mut flight)?;
        if flight.len() < 6 || flight[0] != 22 || flight[5] != 2 {
            return Err(io::Error::other(
                "server did not emit a plaintext ServerHello",
            ));
        }
        let hello_len = usize::from(u16::from_be_bytes([flight[3], flight[4]]));
        if flight.len() < 5 + hello_len {
            return Err(io::Error::other(
                "server emitted a partial ServerHello record",
            ));
        }
        flight.truncate(5 + hello_len);
        // RUSTSEC-2026-0285: a complete EncryptedExtensions must not share the
        // plaintext ServerHello record across its TLS 1.3 key change.
        flight.extend_from_slice(&[8, 0, 0, 2, 0, 0]);
        let record_len = u16::try_from(hello_len + 6)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        flight[3..5].copy_from_slice(&record_len.to_be_bytes());
        socket.write_all(&flight)?;
        socket.shutdown(Shutdown::Both)
    });
    Ok(ServerHandle { address, join })
}

fn spawn_server<F>(config: Arc<ServerConfig>, run: F) -> io::Result<ServerHandle>
where
    F: FnOnce(StreamOwned<ServerConnection, std::net::TcpStream>) -> io::Result<()>
        + Send
        + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let join = thread::spawn(move || {
        let (socket, _) = listener.accept()?;
        let connection = ServerConnection::new(config)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let mut stream = StreamOwned::new(connection, socket);
        while stream.conn.is_handshaking() {
            let _progress = stream.conn.complete_io(&mut stream.sock)?;
        }
        run(stream)
    });
    Ok(ServerHandle { address, join })
}
