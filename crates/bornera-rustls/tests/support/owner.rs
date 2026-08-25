//! Capacity-one Bornera owner configuration for rustls integration tests.

use std::{error::Error, net::SocketAddr, num::NonZeroUsize};

use bornera::{
    ConnectionConfig, ConnectionIdentity, ConnectionSetConfig, ConnectionSlotLimits, DecoderLimits,
    IoLimits, PublicationLimits, StandaloneConnection, StandaloneConnectionConfig,
};
use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, EndpointId, LaneId, MatchKeySpace,
};
use bornera_rustls::{RustlsConnector, RustlsTransport, RustlsTransportLimits};
use calandria::{Deadline, Moment, ResourceOwnerId, RetainedBytes, TimerOwnerId};

use super::protocol::{Classifier, Decoder};

pub(crate) type TestOwner = StandaloneConnection<Decoder, Classifier, RustlsTransport>;

pub(crate) fn connect(
    address: SocketAddr,
    connector: RustlsConnector,
    limits: RustlsTransportLimits,
    io_operations: usize,
    io_chunk_bytes: usize,
) -> Result<TestOwner, Box<dyn Error>> {
    connect_until(
        address,
        connector,
        limits,
        Deadline::at(Moment::from_nanos(u64::MAX)),
        io_operations,
        io_chunk_bytes,
    )
}

pub(crate) fn connect_until(
    address: SocketAddr,
    connector: RustlsConnector,
    limits: RustlsTransportLimits,
    connect_deadline: Deadline,
    io_operations: usize,
    io_chunk_bytes: usize,
) -> Result<TestOwner, Box<dyn Error>> {
    let slot = slot_limits(limits, io_operations, io_chunk_bytes)?;
    Ok(StandaloneConnection::connect_with(
        standalone_config(address, connect_deadline),
        slot,
        Decoder::new(),
        Classifier,
        connector,
    )?)
}

fn standalone_config(
    address: SocketAddr,
    connect_deadline: Deadline,
) -> StandaloneConnectionConfig {
    let identity = ConnectionIdentity::new(
        EndpointId::new(11),
        LaneId::new(12),
        ConnectionId::new(13),
        ConnectionEpoch::new(14),
    );
    let connection =
        ConnectionConfig::new(identity, address, connect_deadline, TimerOwnerId::new(15));
    StandaloneConnectionConfig::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(16)),
        connection,
    )
}

pub(crate) fn slot_limits(
    transport: RustlsTransportLimits,
    io_operations: usize,
    io_chunk_bytes: usize,
) -> Result<ConnectionSlotLimits, Box<dyn Error>> {
    let connection = ConnectionLimits::new(
        4,
        RetainedBytes::new(4_096),
        4,
        RetainedBytes::new(4_096),
        MatchKeySpace::new(0, 32)?,
    )?;
    Ok(ConnectionSlotLimits::new(
        connection,
        DecoderLimits::new(RetainedBytes::new(32), RetainedBytes::new(16)),
        IoLimits::new(nonzero(io_operations)?, nonzero(io_chunk_bytes)?),
        transport.transport_limits(),
        PublicationLimits::new(nonzero(8)?),
    )?)
}

fn nonzero(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value)
        .ok_or_else(|| std::io::Error::other("test bound must be nonzero").into())
}
