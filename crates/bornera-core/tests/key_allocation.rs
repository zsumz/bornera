//! Sustained match-key churn stays bounded and never aliases live work.

mod support;

use std::{collections::BTreeSet, error::Error};

use bornera_core::{
    CancelOutcome, ConnectionCore, ConnectionEpoch, ConnectionId, ConnectionInput,
    ConnectionLimits, Deadline, EndpointId, LaneId, MatchKeySpace, Moment, OperationOptions,
    RetainedBytes,
};

use support::TestFrame;

const CAPACITY: usize = 64;
const EPOCH: ConnectionEpoch = ConnectionEpoch::new(4);

#[test]
fn full_capacity_key_sets_survive_repeated_reverse_release() -> Result<(), Box<dyn Error>> {
    let mut core = ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        EPOCH,
        ConnectionLimits::new(
            CAPACITY,
            RetainedBytes::ZERO,
            CAPACITY,
            RetainedBytes::new(u64::try_from(CAPACITY)?),
            MatchKeySpace::new(100, 163)?,
        )?,
    );
    for _ in 0..32 {
        let mut operations = Vec::with_capacity(CAPACITY);
        let mut keys = BTreeSet::new();
        for byte in 0..CAPACITY {
            let permit = core.reserve(Moment::ORIGIN, options())?;
            assert!(keys.insert(permit.match_key()));
            let (operation, _transition) = core.commit(permit, TestFrame(vec_from(byte)?))?;
            operations.push(operation);
        }
        assert_eq!(keys.len(), CAPACITY);
        for operation in operations.into_iter().rev() {
            let transition = core.apply(ConnectionInput::Cancel {
                epoch: EPOCH,
                operation,
            })?;
            assert_eq!(
                transition.cancel_outcome(),
                Some(CancelOutcome::CancelledNotSent)
            );
        }
        assert_eq!(core.snapshot().owned_operations, 0);
        assert_eq!(core.snapshot().active_match_keys, 0);
    }
    Ok(())
}

#[test]
fn deleting_a_home_slot_relocates_colliding_live_keys() -> Result<(), Box<dyn Error>> {
    let mut core = ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        EPOCH,
        ConnectionLimits::new(
            4,
            RetainedBytes::ZERO,
            4,
            RetainedBytes::new(4),
            MatchKeySpace::new(0, 32)?,
        )?,
    );
    let keeper = submit(&mut core, 0)?;
    let mut collision = None;
    for expected in 1..=8 {
        let operation = submit(&mut core, expected)?;
        if expected == 8 {
            collision = Some(operation);
        } else {
            cancel(&mut core, operation)?;
        }
    }
    cancel(&mut core, keeper)?;
    assert_eq!(core.snapshot().active_match_keys, 1);
    cancel(
        &mut core,
        collision.ok_or_else(|| std::io::Error::other("collision operation was not retained"))?,
    )?;
    assert_eq!(core.snapshot().active_match_keys, 0);
    Ok(())
}

fn options() -> OperationOptions {
    OperationOptions::until(Deadline::at(Moment::from_nanos(u64::MAX)))
        .session()
        .write_retained_bytes(RetainedBytes::new(1))
}

fn vec_from(value: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    Ok(Vec::from([u8::try_from(value)?]))
}

fn submit(
    core: &mut ConnectionCore<TestFrame>,
    expected_key: u32,
) -> Result<bornera_core::OperationId, Box<dyn Error>> {
    let permit = core.reserve(Moment::ORIGIN, options())?;
    assert_eq!(permit.match_key().get(), expected_key);
    Ok(core.commit(permit, TestFrame(Vec::from([0])))?.0)
}

fn cancel(
    core: &mut ConnectionCore<TestFrame>,
    operation: bornera_core::OperationId,
) -> Result<(), Box<dyn Error>> {
    let transition = core.apply(ConnectionInput::Cancel {
        epoch: EPOCH,
        operation,
    })?;
    assert_eq!(
        transition.cancel_outcome(),
        Some(CancelOutcome::CancelledNotSent)
    );
    Ok(())
}
