//! Reply readiness requires the complete request to leave write ownership.

use std::error::Error;

use bornera_core::{
    CloseReason, ConnectionCore, ConnectionEffect, ConnectionEpoch, ConnectionId, ConnectionInput,
    ConnectionLimits, Deadline, EndpointId, InboundReply, InputDisposition, LaneId, MatchKey,
    MatchKeySpace, Moment, OperationOptions, OperationOutcome, RetainedBytes,
};

mod support;
use support::TestFrame;

fn core() -> Result<ConnectionCore<TestFrame>, Box<dyn Error>> {
    Ok(ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
        ConnectionLimits::new(
            2,
            RetainedBytes::new(16),
            2,
            RetainedBytes::new(16),
            MatchKeySpace::new(0, 1)?,
        )?,
    ))
}

fn commit(
    core: &mut ConnectionCore<TestFrame>,
) -> Result<(bornera_core::OperationId, bornera_core::EffectId, MatchKey), Box<dyn Error>> {
    let permit = core.reserve(
        Moment::ORIGIN,
        OperationOptions::until(Deadline::at(Moment::from_nanos(20)))
            .session()
            .retained_bytes(RetainedBytes::new(3))
            .write_bytes(RetainedBytes::new(3)),
    )?;
    let key = permit.match_key();
    let (operation, _) = core.commit(permit, TestFrame(Vec::from([1, 2, 3])))?;
    let effect = core
        .write_effect(operation)
        .ok_or_else(|| std::io::Error::other("accepted operation lost its write"))?;
    Ok((operation, effect, key))
}

fn is_unexpected_close(effect: &ConnectionEffect<TestFrame>) -> bool {
    matches!(
        effect,
        ConnectionEffect::CloseEpoch {
            reason: CloseReason::UnexpectedReply,
            ..
        }
    )
}

#[test]
fn matching_reply_during_partial_write_closes_without_success() -> Result<(), Box<dyn Error>> {
    let mut core = core()?;
    let (operation, effect, key) = commit(&mut core)?;
    core.advance_write(core.epoch(), effect, 1)?;

    let transition = core.apply_reply(InboundReply::new(
        core.epoch(),
        key,
        TestFrame(Vec::from([9])),
    ))?;

    assert_eq!(transition.disposition(), InputDisposition::Fault);
    assert!(transition.effects().iter().any(is_unexpected_close));
    assert!(!transition.effects().iter().any(|effect| matches!(
        effect,
        ConnectionEffect::PublishOutcome {
            operation: owner,
            outcome: OperationOutcome::Reply(_),
            ..
        } if *owner == operation
    )));
    Ok(())
}

#[test]
fn reply_to_cancelled_partial_write_closes_without_second_outcome() -> Result<(), Box<dyn Error>> {
    let mut core = core()?;
    let (operation, effect, key) = commit(&mut core)?;
    core.advance_write(core.epoch(), effect, 1)?;
    let cancelled = core.apply(ConnectionInput::Cancel {
        epoch: core.epoch(),
        operation,
    })?;
    assert_eq!(
        cancelled
            .effects()
            .iter()
            .filter(|effect| matches!(effect, ConnectionEffect::PublishOutcome { .. }))
            .count(),
        1
    );

    let transition = core.apply_reply(InboundReply::new(
        core.epoch(),
        key,
        TestFrame(Vec::from([9])),
    ))?;

    assert_eq!(transition.disposition(), InputDisposition::Fault);
    assert!(transition.effects().iter().any(is_unexpected_close));
    assert!(!transition.effects().iter().any(|effect| matches!(
        effect,
        ConnectionEffect::PublishOutcome { operation: owner, .. } if *owner == operation
    )));
    Ok(())
}
