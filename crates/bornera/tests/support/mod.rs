//! Opaque framing fixtures shared by production integration tests.

#[path = "../common/framing.rs"]
pub(crate) mod framing;

use std::{error::Error, net::SocketAddr, num::NonZeroUsize};

use bornera::{
    ConnectionConfig, ConnectionIdentity, ConnectionSetConfig, ConnectionSlotLimits, DecoderLimits,
    IoLimits, PublicationLimits, StandaloneConnection, StandaloneConnectionConfig, TransportLimits,
};
use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, EndpointId, LaneId, MatchKeySpace,
};
use calandria::{Deadline, Moment, ResourceOwnerId, RetainedBytes, TimerOwnerId};
use framing::{FixedDecoder, KeyClassifier};

pub(crate) type TestEngine = StandaloneConnection<FixedDecoder, KeyClassifier>;

pub(crate) fn engine(address: SocketAddr) -> Result<TestEngine, Box<dyn Error>> {
    let (config, limits) = engine_parts(address)?;
    connect(config, limits)
}

fn connect(
    config: StandaloneConnectionConfig,
    limits: ConnectionSlotLimits,
) -> Result<TestEngine, Box<dyn Error>> {
    Ok(StandaloneConnection::connect(
        config,
        limits,
        FixedDecoder { bytes: Vec::new() },
        KeyClassifier,
    )?)
}

pub(crate) fn engine_parts(
    address: SocketAddr,
) -> Result<(StandaloneConnectionConfig, ConnectionSlotLimits), Box<dyn Error>> {
    engine_parts_with_events(address, nonzero(8)?)
}

pub(crate) fn engine_parts_with_events(
    address: SocketAddr,
    lifecycle_events: NonZeroUsize,
) -> Result<(StandaloneConnectionConfig, ConnectionSlotLimits), Box<dyn Error>> {
    engine_parts_with_bounds(address, nonzero(8)?, nonzero(4)?, lifecycle_events)
}

fn engine_parts_with_bounds(
    address: SocketAddr,
    io_operations: NonZeroUsize,
    io_chunk_bytes: NonZeroUsize,
    lifecycle_events: NonZeroUsize,
) -> Result<(StandaloneConnectionConfig, ConnectionSlotLimits), Box<dyn Error>> {
    let connection = ConnectionLimits::new(
        4,
        RetainedBytes::new(4_096),
        4,
        RetainedBytes::new(4_096),
        MatchKeySpace::new(0, 32)?,
    )?;
    let limits = ConnectionSlotLimits::new(
        connection,
        DecoderLimits::new(RetainedBytes::new(64), RetainedBytes::new(64)),
        IoLimits::new(io_operations, io_chunk_bytes),
        TransportLimits::new(RetainedBytes::ZERO),
        PublicationLimits::new(lifecycle_events),
    )?;
    let identity = ConnectionIdentity::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
    );
    let connection = ConnectionConfig::new(
        identity,
        address,
        Deadline::at(Moment::from_nanos(u64::MAX)),
        TimerOwnerId::new(6),
    );
    let config = StandaloneConnectionConfig::new(
        ConnectionSetConfig::new(ResourceOwnerId::new(5)),
        connection,
    );
    Ok((config, limits))
}

fn nonzero(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value)
        .ok_or_else(|| std::io::Error::other("test bound must be nonzero").into())
}
