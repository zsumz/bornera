//! Fixed simulated transport phases and retained-memory observation.

use std::collections::VecDeque;

use bornera::TransportPressure;
use calandria::RetainedBytes;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Phase {
    Connecting,
    Open,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShutdownState {
    NotStarted,
    Flushing,
    Complete,
}

pub(crate) fn transport_pressure(inbound: &VecDeque<u8>, outbound: &Vec<u8>) -> TransportPressure {
    let inbound = retained_capacity(inbound.capacity());
    let outbound = retained_capacity(outbound.capacity());
    TransportPressure::new(inbound, outbound, RetainedBytes::ZERO, RetainedBytes::ZERO)
        .unwrap_or(TransportPressure::MAX)
}

fn retained_capacity(capacity: usize) -> RetainedBytes {
    RetainedBytes::try_from(capacity).unwrap_or(RetainedBytes::new(u64::MAX))
}
