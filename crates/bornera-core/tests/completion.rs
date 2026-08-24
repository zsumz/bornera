//! Write-complete operations release without waiting for impossible replies.

mod support;

use std::error::Error;

use bornera_core::{
    CancelOutcome, CloseReason, CompletionMode, ConnectionCore, ConnectionEffect, ConnectionEpoch,
    ConnectionId, ConnectionInput, ConnectionLimits, Deadline, Delivery, EndpointId, InboundReply,
    InputDisposition, LaneId, MatchKey, MatchKeySpace, Moment, OperationId, OperationOptions,
    OperationOutcome, RetainedBytes,
};

use support::TestFrame;

const EPOCH: ConnectionEpoch = ConnectionEpoch::new(8);

fn connection_core(
    max_operations: usize,
    max_writes: usize,
    max_retained: u64,
) -> Result<ConnectionCore<TestFrame>, Box<dyn Error>> {
    Ok(ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        EPOCH,
        ConnectionLimits::new(
            max_operations,
            RetainedBytes::ZERO,
            max_writes,
            RetainedBytes::new(max_retained),
            MatchKeySpace::new(0, u32::try_from(max_operations.saturating_sub(1))?)?,
        )?,
    ))
}

struct Accepted {
    operation: OperationId,
    effect: bornera_core::EffectId,
    key: MatchKey,
    bytes: usize,
}

fn accept(
    core: &mut ConnectionCore<TestFrame>,
    byte: u8,
    mode: CompletionMode,
) -> Result<Accepted, Box<dyn Error>> {
    let bytes = usize::from(byte != 0);
    let retained = RetainedBytes::new(u64::try_from(bytes)?);
    let permit = core.reserve(
        Moment::ORIGIN,
        OperationOptions::until(Deadline::at(Moment::from_nanos(100)))
            .session()
            .write_retained_bytes(retained)
            .completion_mode(mode),
    )?;
    let key = permit.match_key();
    let frame = if byte == 0 {
        TestFrame(Vec::new())
    } else {
        TestFrame(Vec::from([byte]))
    };
    let (operation, _transition) = core.commit(permit, frame)?;
    let effect = core
        .write_effect(operation)
        .ok_or_else(|| std::io::Error::other("accepted operation has no write"))?;
    Ok(Accepted {
        operation,
        effect,
        key,
        bytes,
    })
}

fn complete(
    core: &mut ConnectionCore<TestFrame>,
    accepted: &Accepted,
) -> Result<bornera_core::ConnectionTransition, Box<dyn Error>> {
    Ok(core.advance_write(EPOCH, accepted.effect, accepted.bytes)?)
}

fn assert_write_complete(
    transition: &bornera_core::ConnectionTransition,
    accepted: &Accepted,
    delivery: Delivery,
) {
    assert!(transition.effects().iter().any(|effect| {
        matches!(
            effect,
            ConnectionEffect::CancelDeadline { operation, .. }
                if *operation == accepted.operation
        )
    }));
    assert!(transition.effects().iter().any(|effect| {
        matches!(
            effect,
            ConnectionEffect::PublishOutcome {
                operation,
                outcome: OperationOutcome::WriteComplete { delivery: actual },
                ..
            } if *operation == accepted.operation && *actual == delivery
        )
    }));
}

#[test]
fn no_reply_operations_before_between_and_after_replies_leave_matching_exact()
-> Result<(), Box<dyn Error>> {
    let mut core = connection_core(8, 8, 64)?;
    let first = accept(&mut core, 1, CompletionMode::WriteComplete)?;
    let reply_one = accept(&mut core, 2, CompletionMode::ReplyExpected)?;
    let middle = accept(&mut core, 3, CompletionMode::WriteComplete)?;
    let reply_two = accept(&mut core, 4, CompletionMode::ReplyExpected)?;
    let last = accept(&mut core, 5, CompletionMode::WriteComplete)?;

    assert_write_complete(
        &complete(&mut core, &first)?,
        &first,
        Delivery::PossiblySent,
    );
    assert!(complete(&mut core, &reply_one)?.effects().is_empty());
    assert_write_complete(
        &complete(&mut core, &middle)?,
        &middle,
        Delivery::PossiblySent,
    );
    assert!(complete(&mut core, &reply_two)?.effects().is_empty());
    assert_write_complete(&complete(&mut core, &last)?, &last, Delivery::PossiblySent);

    assert_eq!(core.matching().pending_operations(), 2);
    assert_eq!(core.matching().front_match_key(), Some(reply_one.key));
    let first_reply = core.apply_reply(InboundReply::new(
        EPOCH,
        reply_one.key,
        TestFrame(Vec::from([9])),
    ))?;
    assert!(matches!(
        first_reply.effects().last(),
        Some(ConnectionEffect::PublishOutcome {
            operation,
            outcome: OperationOutcome::Reply(_),
            ..
        }) if *operation == reply_one.operation
    ));
    assert_eq!(core.matching().front_match_key(), Some(reply_two.key));
    let second_reply = core.apply_reply(InboundReply::new(
        EPOCH,
        reply_two.key,
        TestFrame(Vec::from([10])),
    ))?;
    assert!(matches!(
        second_reply.effects().last(),
        Some(ConnectionEffect::PublishOutcome {
            operation,
            outcome: OperationOutcome::Reply(_),
            ..
        }) if *operation == reply_two.operation
    ));
    assert_eq!(core.snapshot().owned_operations, 0);
    Ok(())
}

#[test]
fn partial_no_reply_cancellation_releases_silently_when_write_finishes()
-> Result<(), Box<dyn Error>> {
    let mut core = connection_core(2, 2, 16)?;
    let permit = core.reserve(
        Moment::ORIGIN,
        OperationOptions::until(Deadline::at(Moment::from_nanos(100)))
            .session()
            .write_retained_bytes(RetainedBytes::new(3))
            .completion_mode(CompletionMode::WriteComplete),
    )?;
    let (operation, _transition) = core.commit(permit, TestFrame(Vec::from([1, 2, 3])))?;
    let effect = core
        .write_effect(operation)
        .ok_or_else(|| std::io::Error::other("accepted operation has no write"))?;
    assert!(core.advance_write(EPOCH, effect, 1)?.effects().is_empty());

    let cancelled = core.apply(ConnectionInput::Cancel {
        epoch: EPOCH,
        operation,
    })?;
    assert_eq!(
        cancelled.cancel_outcome(),
        Some(CancelOutcome::ObservationCancelled {
            delivery: Delivery::PossiblySent
        })
    );
    assert_eq!(
        cancelled
            .effects()
            .iter()
            .filter(|effect| matches!(effect, ConnectionEffect::PublishOutcome { .. }))
            .count(),
        1
    );

    let finished = core.advance_write(EPOCH, effect, 2)?;
    assert!(finished.effects().iter().any(|effect| matches!(
        effect,
        ConnectionEffect::CancelDeadline {
            operation: actual,
            ..
        } if *actual == operation
    )));
    assert!(
        !finished
            .effects()
            .iter()
            .any(|effect| matches!(effect, ConnectionEffect::PublishOutcome { .. }))
    );
    assert_eq!(core.snapshot().owned_operations, 0);
    assert_eq!(
        core.apply(ConnectionInput::DeadlineElapsed {
            epoch: EPOCH,
            operation,
            now: Moment::from_nanos(100),
        })?
        .disposition(),
        InputDisposition::IgnoredUnknownOperation
    );
    Ok(())
}

#[test]
fn empty_no_reply_completion_is_not_fabricated_as_sent() -> Result<(), Box<dyn Error>> {
    let mut core = connection_core(1, 1, 1)?;
    let empty = accept(&mut core, 0, CompletionMode::WriteComplete)?;
    assert_write_complete(&complete(&mut core, &empty)?, &empty, Delivery::NotSent);
    assert_eq!(core.snapshot().owned_operations, 0);
    Ok(())
}

#[test]
fn draining_closes_after_the_last_no_reply_write() -> Result<(), Box<dyn Error>> {
    let mut core = connection_core(1, 1, 1)?;
    let accepted = accept(&mut core, 1, CompletionMode::WriteComplete)?;
    assert_eq!(
        core.apply(ConnectionInput::BeginDrain { epoch: EPOCH })?
            .disposition(),
        InputDisposition::Applied
    );
    let transition = complete(&mut core, &accepted)?;
    assert_write_complete(&transition, &accepted, Delivery::PossiblySent);
    assert!(transition.effects().iter().any(|effect| matches!(
        effect,
        ConnectionEffect::CloseEpoch {
            reason: CloseReason::Drained,
            ..
        }
    )));
    Ok(())
}
