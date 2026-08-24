//! Permit preparation order cannot override commit-defined wire ownership.

use std::error::Error;

use bornera_core::{
    CancelOutcome, ConnectionCore, ConnectionEffect, ConnectionEpoch, ConnectionId,
    ConnectionInput, ConnectionLimits, Deadline, EndpointId, InboundReply, LaneId, MatchKeySpace,
    Moment, OperationOptions, OperationOutcome, RetainedBytes, WriteFrame,
};

#[derive(Debug)]
struct Frame([u8; 1]);

impl WriteFrame for Frame {
    fn bytes(&self) -> &[u8] {
        &self.0
    }

    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::new(1)
    }
}

#[test]
fn reverse_permit_commit_order_defines_writes_and_replies() -> Result<(), Box<dyn Error>> {
    let mut core = fixture()?;
    let first = core.reserve(Moment::ORIGIN, options())?;
    let second = core.reserve(Moment::ORIGIN, options())?;
    let first_key = first.match_key();
    let second_key = second.match_key();
    let (second_operation, _) = core.commit(second, Frame([2]))?;
    let (first_operation, _) = core.commit(first, Frame([1]))?;

    let front = core
        .front_write(nonzero_one())?
        .ok_or_else(|| std::io::Error::other("reverse commit retained no write"))?;
    assert_eq!(front.operation, second_operation);
    let second_effect = front.effect;
    let _transition = core.advance_write(core.epoch(), second_effect, 1)?;
    let front = core
        .front_write(nonzero_one())?
        .ok_or_else(|| std::io::Error::other("second reverse write disappeared"))?;
    assert_eq!(front.operation, first_operation);
    let _transition = core.advance_write(core.epoch(), front.effect, 1)?;

    let second_reply = core.apply_reply(InboundReply::new(core.epoch(), second_key, 22_u8))?;
    assert!(published_reply(&second_reply, second_operation, 22));
    let first_reply = core.apply_reply(InboundReply::new(core.epoch(), first_key, 11_u8))?;
    assert!(published_reply(&first_reply, first_operation, 11));
    Ok(())
}

#[test]
fn cancellation_finds_a_lower_identity_behind_a_higher_wire_front() -> Result<(), Box<dyn Error>> {
    let mut core = fixture()?;
    let first = core.reserve(Moment::ORIGIN, options())?;
    let second = core.reserve(Moment::ORIGIN, options())?;
    let first_operation = first.operation_id();
    let _second = core.commit(second, Frame([2]))?;
    let _first = core.commit(first, Frame([1]))?;

    let transition = core.apply(ConnectionInput::Cancel {
        epoch: core.epoch(),
        operation: first_operation,
    })?;
    assert_eq!(
        transition.cancel_outcome(),
        Some(CancelOutcome::CancelledNotSent)
    );
    assert_eq!(core.snapshot().owned_operations, 1);
    assert_eq!(core.queued_write_frames(), 1);
    Ok(())
}

fn fixture() -> Result<ConnectionCore<Frame>, Box<dyn Error>> {
    let limits = ConnectionLimits::new(
        2,
        RetainedBytes::new(8),
        2,
        RetainedBytes::new(8),
        MatchKeySpace::new(0, 1)?,
    )?;
    Ok(ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
        limits,
    ))
}

fn options() -> OperationOptions {
    OperationOptions::until(Deadline::at(Moment::from_nanos(u64::MAX)))
        .session()
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(1))
}

fn published_reply(
    transition: &bornera_core::ConnectionTransition<u8>,
    operation: bornera_core::OperationId,
    reply: u8,
) -> bool {
    transition.effects().iter().any(|effect| {
        matches!(
            effect,
            ConnectionEffect::PublishOutcome {
                operation: published,
                outcome: OperationOutcome::Reply(value),
                ..
            } if *published == operation && *value == reply
        )
    })
}

const fn nonzero_one() -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::MIN
}
