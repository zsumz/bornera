//! Shared production-slot replay fixtures.

use std::{error::Error, num::NonZeroUsize};

use bornera::{
    ConnectionIdentity, ConnectionSlotConfig, ConnectionSlotLimits, DecoderLimits, IoLimits,
    PublicationLimits, TransportLimits,
};
use bornera_core::{
    CompletionMode, ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId, LaneId,
    MatchKeySpace, Moment, OperationOptions, RetainedBytes,
};
use bornera_sim::{SimFrame, SlotAction, SlotSimulationConfig, SlotTrace, SlotTraceLimits};
use calandria::TimerOwnerId;
use calandria_sim::TimelineId;

pub(crate) fn config_with_transport(
    transport_bytes: u64,
) -> Result<SlotSimulationConfig, Box<dyn Error>> {
    let core = ConnectionLimits::new(
        8,
        RetainedBytes::new(256),
        8,
        RetainedBytes::new(256),
        MatchKeySpace::new(10, 17)?,
    )?;
    let limits = ConnectionSlotLimits::new(
        core,
        DecoderLimits::new(RetainedBytes::new(128), RetainedBytes::new(64)),
        IoLimits::new(nz(8)?, nz(128)?),
        TransportLimits::new(RetainedBytes::new(transport_bytes)),
        PublicationLimits::new(nz(16)?),
    )?;
    let identity = ConnectionIdentity::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
    );
    let slot = ConnectionSlotConfig::new(
        identity,
        Deadline::at(Moment::from_nanos(100)),
        TimerOwnerId::new(5),
    );
    Ok(SlotSimulationConfig::new(slot, limits, TimelineId::new(6)))
}

pub(crate) fn trace(actions: usize, bytes: u64) -> Result<SlotTrace, Box<dyn Error>> {
    Ok(SlotTrace::new(SlotTraceLimits::new(
        nz(actions)?,
        RetainedBytes::new(bytes),
    )))
}

pub(crate) fn submit(
    trace: &mut SlotTrace,
    at: u64,
    mode: CompletionMode,
    deadline: u64,
    bytes: &[u8],
) -> Result<(), Box<dyn Error>> {
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(deadline)))
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(u64::try_from(bytes.len())?))
        .completion_mode(mode);
    push(
        trace,
        at,
        SlotAction::Submit {
            options,
            frame: SimFrame::copy_from_slice(bytes)?,
        },
    )
}

pub(crate) fn push(
    trace: &mut SlotTrace,
    at: u64,
    action: SlotAction,
) -> Result<(), Box<dyn Error>> {
    trace.try_push(Moment::from_nanos(at), action)?;
    Ok(())
}

fn nz(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value)
        .ok_or_else(|| std::io::Error::other("test bound must be nonzero").into())
}
