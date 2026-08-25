//! Shared-selector graceful-shutdown readiness and settlement contracts.

use std::error::Error;

use bornera::TransportState;
use bornera_core::{Deadline, Moment};
use calandria::Next;

#[path = "common/registration_protocol.rs"]
mod protocol;
#[path = "common/registered_shutdown.rs"]
mod support;
use protocol::{Classifier, Decoder};
use support::{ShutdownConnector, ShutdownProbe, connection_config, connection_set, slot_limits};

#[test]
fn registered_shutdown_reregisters_waits_and_deregisters_exactly_once() -> Result<(), Box<dyn Error>>
{
    let probe = ShutdownProbe::new();
    let mut set = connection_set()?;
    let connection = set.connect_with(
        connection_config(),
        slot_limits()?,
        Decoder,
        Classifier,
        ShutdownConnector(probe.clone()),
    )?;
    let _established = set.turn_component(Moment::ORIGIN)?;
    assert_eq!(
        set.connection_snapshot(connection)?.transport,
        TransportState::Open
    );

    let deadline = Deadline::at(Moment::from_nanos(20));
    set.begin_drain(connection, deadline)?;
    let waiting = set.turn_component(Moment::from_nanos(1))?;
    assert_eq!(probe.begins(), 1);
    assert_eq!(probe.drives(), 0);
    assert!(probe.saw_shutdown_writable_interest());
    assert!(probe.reregistrations() >= 2);
    assert_eq!(probe.deregistrations(), 0);
    assert_eq!(waiting.next(), Next::WakeOr(deadline));
    assert_eq!(
        set.connection_snapshot(connection)?.transport,
        TransportState::Closing
    );

    probe.allow_write();
    let completed = set.turn_component(Moment::from_nanos(2))?;
    assert_eq!(probe.begins(), 1);
    assert_eq!(probe.drives(), 1);
    assert_eq!(probe.deregistrations(), 1);
    assert_eq!(completed.next(), Next::Wake);
    let snapshot = set.connection_snapshot(connection)?;
    assert_eq!(snapshot.transport, TransportState::Closed);
    assert!(!snapshot.transport_release_ready);

    let _idle = set.turn_component(Moment::from_nanos(3))?;
    assert_eq!(probe.deregistrations(), 1);
    Ok(())
}
