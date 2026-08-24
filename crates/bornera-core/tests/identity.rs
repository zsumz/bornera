//! Focused evidence for distinct fixed-width identities and exhaustion.

use std::error::Error;

use bornera_core::{
    ConnectionCore, ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EffectId,
    EndpointId, IdentitySeeds, LaneId, MatchKey, MatchKeySpace, Moment, OperationId,
    OperationOptions, ReserveError, RetainedBytes,
};

mod support;

use support::TestFrame;

#[test]
fn identity_domains_preserve_their_fixed_width_values() {
    assert_eq!(EndpointId::new(1).get(), 1);
    assert_eq!(LaneId::new(2).get(), 2);
    assert_eq!(ConnectionId::new(3).get(), 3);
    assert_eq!(ConnectionEpoch::new(4).get(), 4);
    assert_eq!(OperationId::new(5).get(), 5);
    assert_eq!(EffectId::new(6).get(), 6);
    assert_eq!(MatchKey::new(7).get(), 7);
}

#[test]
fn generated_identities_exhaust_without_alias() -> Result<(), Box<dyn Error>> {
    let limits = ConnectionLimits::new(
        2,
        RetainedBytes::new(20),
        2,
        RetainedBytes::new(20),
        MatchKeySpace::new(0, 1)?,
    )?;
    let mut machine: ConnectionCore<TestFrame> = ConnectionCore::with_identity_seeds(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
        limits,
        IdentitySeeds::new(u64::MAX, u64::MAX),
    );
    let permit = machine.reserve(
        Moment::ORIGIN,
        OperationOptions::until(Deadline::at(Moment::from_nanos(1)))
            .session()
            .write_retained_bytes(RetainedBytes::new(1)),
    )?;
    assert_eq!(permit.operation_id(), OperationId::new(u64::MAX));
    drop(permit);

    let error = machine
        .reserve(
            Moment::ORIGIN,
            OperationOptions::until(Deadline::at(Moment::from_nanos(1)))
                .session()
                .write_retained_bytes(RetainedBytes::new(1)),
        )
        .err();
    assert_eq!(error, Some(ReserveError::IdentityExhausted));
    Ok(())
}
