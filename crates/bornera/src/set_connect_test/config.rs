//! Compact connection and bound fixtures for selector registration tests.

use std::{error::Error, net::SocketAddr, num::NonZeroUsize};

use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId, LaneId, MatchKeySpace,
    Moment, RetainedBytes,
};
use calandria::TimerOwnerId;

use crate::{
    ConnectionConfig, ConnectionIdentity, ConnectionSetLimits, ConnectionSlotLimits, DecoderLimits,
    IoLimits, PublicationLimits, TransportLimits,
};

pub(super) const fn set_limits() -> ConnectionSetLimits {
    ConnectionSetLimits::new(
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
    )
}

pub(super) fn slot_limits() -> Result<ConnectionSlotLimits, Box<dyn Error>> {
    let connection = ConnectionLimits::new(
        1,
        RetainedBytes::new(8),
        1,
        RetainedBytes::new(8),
        MatchKeySpace::new(0, 0)?,
    )?;
    Ok(ConnectionSlotLimits::new(
        connection,
        DecoderLimits::new(RetainedBytes::new(8), RetainedBytes::new(8)),
        IoLimits::new(NonZeroUsize::MIN, NonZeroUsize::MIN),
        TransportLimits::new(RetainedBytes::ZERO),
        PublicationLimits::new(NonZeroUsize::MIN),
    )?)
}

pub(super) fn connection_config() -> ConnectionConfig {
    let identity = ConnectionIdentity::new(
        EndpointId::new(1),
        LaneId::new(1),
        ConnectionId::new(1),
        ConnectionEpoch::new(1),
    );
    ConnectionConfig::new(
        identity,
        SocketAddr::from(([127, 0, 0, 1], 1)),
        Deadline::at(Moment::from_nanos(u64::MAX)),
        TimerOwnerId::new(81),
    )
}
