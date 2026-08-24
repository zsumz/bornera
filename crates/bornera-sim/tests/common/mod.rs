//! Shared deterministic simulation construction fixtures.

use std::{error::Error, num::NonZeroUsize};

use bornera_core::{
    CompletionMode, ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId, LaneId,
    MatchKeySpace, Moment, OperationOptions, RetainedBytes,
};
use bornera_sim::{SimFrame, SimulationConfig, Trace, TraceAction, TraceLimits};

pub(crate) fn simulator_config() -> Result<SimulationConfig, Box<dyn Error>> {
    let limits = ConnectionLimits::new(
        16,
        RetainedBytes::new(256),
        16,
        RetainedBytes::new(256),
        MatchKeySpace::new(100, 115)?,
    )?;
    Ok(SimulationConfig::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
        limits,
    ))
}

pub(crate) fn trace(action_capacity: usize, byte_capacity: u64) -> Result<Trace, Box<dyn Error>> {
    let actions = NonZeroUsize::new(action_capacity)
        .ok_or_else(|| std::io::Error::other("trace capacity must be nonzero"))?;
    Ok(Trace::new(TraceLimits::new(
        actions,
        RetainedBytes::new(byte_capacity),
    )))
}

pub(crate) fn options(
    mode: CompletionMode,
    bytes: usize,
) -> Result<OperationOptions, Box<dyn Error>> {
    Ok(
        OperationOptions::until(Deadline::at(Moment::from_nanos(1_000)))
            .session()
            .write_retained_bytes(RetainedBytes::new(u64::try_from(bytes)?))
            .completion_mode(mode),
    )
}

pub(crate) fn submit(
    trace: &mut Trace,
    mode: CompletionMode,
    bytes: &[u8],
) -> Result<(), Box<dyn Error>> {
    trace.try_push(TraceAction::Submit {
        now: Moment::ORIGIN,
        options: options(mode, bytes.len())?,
        frame: SimFrame::copy_from_slice(bytes)?,
    })?;
    Ok(())
}
