//! Empty visible frames retain protocol-neutral ownership semantics.

use std::error::Error;

use bornera_core::{
    CancelOutcome, ConnectionCore, ConnectionEffect, ConnectionEpoch, ConnectionId,
    ConnectionInput, ConnectionLimits, Deadline, Delivery, EndpointId, InboundReply, LaneId,
    MatchKeySpace, Moment, OperationFailure, OperationOptions, OperationOutcome, RetainedBytes,
    WriteFrame,
};

#[derive(Debug, Eq, PartialEq)]
struct EmptyFrame {
    bytes: Vec<u8>,
}

impl EmptyFrame {
    fn retained(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
        }
    }
}

impl WriteFrame for EmptyFrame {
    fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::new(u64::try_from(self.bytes.capacity()).unwrap_or(u64::MAX))
    }
}

fn core() -> Result<ConnectionCore<EmptyFrame>, Box<dyn Error>> {
    Ok(ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
        ConnectionLimits::new(
            2,
            RetainedBytes::new(64),
            2,
            RetainedBytes::new(64),
            MatchKeySpace::new(0, 1)?,
        )?,
    ))
}

fn commit(
    core: &mut ConnectionCore<EmptyFrame>,
    deadline: Moment,
) -> Result<
    (
        bornera_core::OperationId,
        bornera_core::EffectId,
        bornera_core::MatchKey,
    ),
    Box<dyn Error>,
> {
    let retained = RetainedBytes::new(8);
    let permit = core.reserve(
        Moment::ORIGIN,
        OperationOptions::until(Deadline::at(deadline))
            .session()
            .retained_bytes(retained)
            .write_bytes(retained),
    )?;
    let key = permit.match_key();
    let (operation, _) = core.commit(permit, EmptyFrame::retained(8))?;
    let effect = core
        .write_effect(operation)
        .ok_or_else(|| std::io::Error::other("accepted empty frame lost its write"))?;
    Ok((operation, effect, key))
}

#[test]
fn empty_visible_frame_with_retained_allocation_completes_normally() -> Result<(), Box<dyn Error>> {
    let mut core = core()?;
    let (operation, effect, key) = commit(&mut core, Moment::from_nanos(20))?;
    core.advance_write(core.epoch(), effect, 0)?;
    assert_eq!(core.queued_write_frames(), 0);

    let reply = core.apply_reply(InboundReply::new(
        core.epoch(),
        key,
        EmptyFrame::retained(1),
    ))?;
    assert!(reply.effects().iter().any(|effect| matches!(
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
fn completed_empty_frame_cancels_not_sent_without_poison() -> Result<(), Box<dyn Error>> {
    let mut core = core()?;
    let (operation, effect, _) = commit(&mut core, Moment::from_nanos(20))?;
    core.advance_write(core.epoch(), effect, 0)?;

    let cancelled = core.apply(ConnectionInput::Cancel {
        epoch: core.epoch(),
        operation,
    })?;

    assert_eq!(
        cancelled.cancel_outcome(),
        Some(CancelOutcome::CancelledNotSent)
    );
    assert!(cancelled.effects().iter().any(|effect| matches!(
        effect,
        ConnectionEffect::PublishOutcome {
            outcome: OperationOutcome::Cancelled {
                delivery: Delivery::NotSent
            },
            ..
        }
    )));
    assert_eq!(core.snapshot().owned_operations, 0);
    Ok(())
}

#[test]
fn completed_empty_frame_deadline_fails_not_sent_without_poison() -> Result<(), Box<dyn Error>> {
    let mut core = core()?;
    let deadline = Moment::from_nanos(20);
    let (operation, effect, _) = commit(&mut core, deadline)?;
    core.advance_write(core.epoch(), effect, 0)?;

    let elapsed = core.apply(ConnectionInput::DeadlineElapsed {
        epoch: core.epoch(),
        operation,
        now: deadline,
    })?;

    assert!(elapsed.effects().iter().any(|effect| matches!(
        effect,
        ConnectionEffect::PublishOutcome {
            outcome: OperationOutcome::Failed {
                failure: OperationFailure::DeadlineElapsed,
                delivery: Delivery::NotSent,
            },
            ..
        }
    )));
    assert_eq!(core.snapshot().owned_operations, 0);
    Ok(())
}
