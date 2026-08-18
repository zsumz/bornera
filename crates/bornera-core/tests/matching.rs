//! Focused evidence for protocol-neutral ordered verified matching.

use std::error::Error;

use bornera_core::{
    CancelOutcome, CloseReason, ConnectionCore, ConnectionEffect, ConnectionEpoch, ConnectionId,
    ConnectionInput, ConnectionLimits, ConnectionPhase, Deadline, Delivery, EffectId, EndpointId,
    InboundReply, InputDisposition, LaneId, MatchKey, MatchKeySpace, Moment, OperationFailure,
    OperationId, OperationOptions, OperationOutcome, RetainedBytes,
};

mod support;
use support::TestFrame;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReplyFrame(&'static str);

fn machine(
    first_key: u32,
    last_key: u32,
    capacity: usize,
) -> Result<ConnectionCore<TestFrame>, Box<dyn Error>> {
    Ok(ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
        ConnectionLimits::new(
            capacity,
            RetainedBytes::new(64),
            capacity,
            RetainedBytes::new(64),
            MatchKeySpace::new(first_key, last_key)?,
        )?,
    ))
}

fn front_identity(
    machine: &ConnectionCore<TestFrame>,
) -> Result<(OperationId, EffectId), std::io::Error> {
    machine
        .front_write(std::num::NonZeroUsize::MAX)
        .map(|front| (front.operation, front.effect))
        .ok_or_else(|| std::io::Error::other("aggregate retained no write front"))
}

fn commit_one(
    machine: &mut ConnectionCore<TestFrame>,
) -> Result<(OperationId, EffectId, MatchKey), Box<dyn Error>> {
    let permit = machine.reserve(
        Moment::ORIGIN,
        OperationOptions::until(Deadline::at(Moment::from_nanos(20)))
            .session()
            .retained_bytes(RetainedBytes::new(2))
            .write_bytes(RetainedBytes::new(3)),
    )?;
    let key = permit.match_key();
    let (operation, _) = machine.commit(permit, TestFrame(Vec::from([0; 3])))?;
    let effect = machine
        .write_effect(operation)
        .ok_or_else(|| std::io::Error::other("aggregate retained no operation write"))?;
    Ok((operation, effect, key))
}

fn mark_written(
    machine: &mut ConnectionCore<TestFrame>,
    operation: OperationId,
    effect: EffectId,
) -> Result<(), Box<dyn Error>> {
    let front = front_identity(machine)?;
    assert_eq!(front, (operation, effect));
    machine.advance_write(machine.epoch(), effect, 3)?;
    Ok(())
}

#[test]
fn matching_reply_completes_only_the_fifo_front_with_opaque_frame() -> Result<(), Box<dyn Error>> {
    let mut machine = machine(10, 13, 4)?;
    let (first, first_effect, first_key) = commit_one(&mut machine)?;
    let (second, second_effect, _) = commit_one(&mut machine)?;
    mark_written(&mut machine, first, first_effect)?;
    mark_written(&mut machine, second, second_effect)?;

    let reply = machine.apply_reply(InboundReply::new(
        machine.epoch(),
        first_key,
        ReplyFrame("opaque"),
    ))?;

    assert_eq!(reply.disposition(), InputDisposition::Applied);
    assert_eq!(
        reply.effects(),
        [
            ConnectionEffect::CancelDeadline {
                epoch: machine.epoch(),
                operation: first,
            },
            ConnectionEffect::PublishOutcome {
                epoch: machine.epoch(),
                operation: first,
                outcome: OperationOutcome::Reply(ReplyFrame("opaque")),
            },
        ]
    );
    assert_eq!(machine.matching().pending_operations(), 1);
    assert_eq!(machine.snapshot().active_match_keys, 1);
    Ok(())
}

#[test]
fn out_of_order_reply_poisoning_cannot_reassign_another_live_key() -> Result<(), Box<dyn Error>> {
    let mut machine = machine(10, 13, 4)?;
    let (first, first_effect, first_key) = commit_one(&mut machine)?;
    let (second, second_effect, second_key) = commit_one(&mut machine)?;
    mark_written(&mut machine, first, first_effect)?;
    mark_written(&mut machine, second, second_effect)?;

    let reply = machine.apply_reply(InboundReply::new(
        machine.epoch(),
        second_key,
        ReplyFrame("out-of-order"),
    ))?;

    assert_eq!(reply.disposition(), InputDisposition::Fault);
    assert!(matches!(
        reply.effects().first(),
        Some(ConnectionEffect::CloseEpoch {
            reason: CloseReason::MatchKeyMismatch { expected, received },
            ..
        }) if *expected == first_key && *received == second_key
    ));
    assert!(reply.effects().iter().any(|effect| matches!(
        effect,
        ConnectionEffect::PublishOutcome {
            operation,
            outcome: OperationOutcome::Failed {
                failure: OperationFailure::MatchKeyMismatch { expected, received },
                delivery: Delivery::PossiblySent,
            },
            ..
        } if *operation == first && *expected == first_key && *received == second_key
    )));
    assert!(reply.effects().iter().any(|effect| matches!(
        effect,
        ConnectionEffect::PublishOutcome {
            operation,
            outcome: OperationOutcome::Failed {
                failure: OperationFailure::ConnectionClosed(_),
                delivery: Delivery::PossiblySent,
            },
            ..
        } if *operation == second
    )));
    assert_eq!(machine.snapshot().phase, ConnectionPhase::Closing);
    assert_eq!(machine.snapshot().owned_operations, 0);
    Ok(())
}

#[test]
fn unsolicited_malformed_and_premature_replies_close_the_epoch() -> Result<(), Box<dyn Error>> {
    let mut idle = machine(0, 1, 2)?;
    let unsolicited = idle.apply_reply(InboundReply::new(
        idle.epoch(),
        MatchKey::new(0),
        ReplyFrame("unsolicited"),
    ))?;
    assert_eq!(unsolicited.disposition(), InputDisposition::Fault);
    assert!(matches!(
        unsolicited.effects().first(),
        Some(ConnectionEffect::CloseEpoch {
            reason: CloseReason::UnexpectedReply,
            ..
        })
    ));

    let mut malformed = machine(0, 1, 2)?;
    let malformed_epoch = malformed.epoch();
    let rejected = malformed.apply(ConnectionInput::ReplyMalformed {
        epoch: malformed_epoch,
    })?;
    assert_eq!(rejected.disposition(), InputDisposition::Fault);
    assert!(matches!(
        rejected.effects().first(),
        Some(ConnectionEffect::CloseEpoch {
            reason: CloseReason::MalformedReply,
            ..
        })
    ));

    let mut premature = machine(0, 1, 2)?;
    let (_, _, key) = commit_one(&mut premature)?;
    let reply = premature.apply_reply(InboundReply::new(
        premature.epoch(),
        key,
        ReplyFrame("before-write"),
    ))?;
    assert_eq!(reply.disposition(), InputDisposition::Fault);
    assert!(matches!(
        reply.effects().first(),
        Some(ConnectionEffect::CloseEpoch {
            reason: CloseReason::UnexpectedReply,
            ..
        })
    ));
    Ok(())
}

#[test]
fn draining_closes_only_after_the_fifo_reply_is_published() -> Result<(), Box<dyn Error>> {
    let mut machine = machine(0, 1, 2)?;
    let (operation, effect, key) = commit_one(&mut machine)?;
    mark_written(&mut machine, operation, effect)?;
    machine.apply(ConnectionInput::BeginDrain {
        epoch: machine.epoch(),
    })?;

    let reply = machine.apply_reply(InboundReply::new(machine.epoch(), key, ReplyFrame("last")))?;
    assert!(matches!(
        reply.effects().get(1),
        Some(ConnectionEffect::PublishOutcome {
            outcome: OperationOutcome::Reply(ReplyFrame("last")),
            ..
        })
    ));
    assert!(matches!(
        reply.effects().last(),
        Some(ConnectionEffect::CloseEpoch {
            reason: CloseReason::Drained,
            ..
        })
    ));
    Ok(())
}

#[test]
fn late_reply_for_cancelled_possible_send_is_consumed_without_second_outcome()
-> Result<(), Box<dyn Error>> {
    let mut machine = machine(0, 1, 2)?;
    let (operation, effect, key) = commit_one(&mut machine)?;
    machine.advance_write(machine.epoch(), effect, 1)?;
    let cancelled = machine.apply(ConnectionInput::Cancel {
        epoch: machine.epoch(),
        operation,
    })?;
    assert_eq!(
        cancelled.cancel_outcome(),
        Some(CancelOutcome::ObservationCancelled {
            delivery: Delivery::PossiblySent
        })
    );
    machine.advance_write(machine.epoch(), effect, 2)?;

    let late = machine.apply_reply(InboundReply::new(machine.epoch(), key, ReplyFrame("late")))?;
    assert_eq!(late.disposition(), InputDisposition::Applied);
    assert!(
        !late
            .effects()
            .iter()
            .any(|effect| matches!(effect, ConnectionEffect::PublishOutcome { .. }))
    );
    assert_eq!(machine.snapshot().owned_operations, 0);
    assert_eq!(machine.snapshot().active_match_keys, 0);
    Ok(())
}

#[test]
fn stale_epoch_reply_is_discarded_without_mutating_current_fifo() -> Result<(), Box<dyn Error>> {
    let mut machine = machine(0, 1, 2)?;
    let (_, _, key) = commit_one(&mut machine)?;
    let before = machine.snapshot();
    let stale = machine.apply_reply(InboundReply::new(
        ConnectionEpoch::new(3),
        key,
        ReplyFrame("stale"),
    ))?;
    assert_eq!(stale.disposition(), InputDisposition::IgnoredStaleEpoch);
    assert_eq!(stale.effects().len(), 0);
    assert_eq!(machine.snapshot(), before);
    Ok(())
}

#[test]
fn released_keys_wrap_within_the_configured_space_without_aliasing() -> Result<(), Box<dyn Error>> {
    let mut machine = machine(10, 11, 1)?;
    let mut observed = [MatchKey::new(0); 3];
    for key in &mut observed {
        let (operation, _, allocated) = commit_one(&mut machine)?;
        *key = allocated;
        machine.apply(ConnectionInput::Cancel {
            epoch: machine.epoch(),
            operation,
        })?;
    }
    assert_eq!(
        observed,
        [MatchKey::new(10), MatchKey::new(11), MatchKey::new(10)]
    );
    Ok(())
}
