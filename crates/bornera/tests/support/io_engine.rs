//! Deliberately narrow I/O quanta for partial-write production evidence.

use std::{error::Error, net::SocketAddr, num::NonZeroUsize};

use bornera::{
    ConnectionConfig, ConnectionIdentity, ConnectionSetConfig, ConnectionSlotLimits, DecoderLimits,
    IoLimits, PublicationLimits, StandaloneConnection, StandaloneConnectionConfig, TransportLimits,
};
use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, EndpointId, LaneId, MatchKeySpace,
};
use calandria::{Deadline, Moment, ResourceOwnerId, RetainedBytes, TimerOwnerId};

use crate::support::{TestEngine, framing::FixedDecoder, framing::KeyClassifier};

pub(crate) fn engine_with_io(
    address: SocketAddr,
    operations: NonZeroUsize,
    chunk_bytes: NonZeroUsize,
) -> Result<TestEngine, Box<dyn Error>> {
    let connection_limits = ConnectionLimits::new(
        4,
        RetainedBytes::new(4_096),
        4,
        RetainedBytes::new(4_096),
        MatchKeySpace::new(0, 32)?,
    )?;
    let slot_limits = ConnectionSlotLimits::new(
        connection_limits,
        DecoderLimits::new(RetainedBytes::new(64), RetainedBytes::new(64)),
        IoLimits::new(operations, chunk_bytes),
        TransportLimits::new(RetainedBytes::ZERO),
        PublicationLimits::new(nonzero(8)?),
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
    Ok(StandaloneConnection::connect(
        config,
        slot_limits,
        FixedDecoder { bytes: Vec::new() },
        KeyClassifier,
    )?)
}

fn nonzero(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value)
        .ok_or_else(|| std::io::Error::other("test bound must be nonzero").into())
}
