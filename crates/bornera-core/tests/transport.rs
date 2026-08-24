//! Integrated evidence for aggregate policy and bounded-frame ownership.

use std::error::Error;

use bornera_core::{
    CloseReason, ConnectionCore, ConnectionEffect, ConnectionEpoch, ConnectionInput,
    ConnectionLimits, Deadline, Delivery, EffectId, InboundReply, MatchKey, MatchKeySpace, Moment,
    OperationId, OperationOptions, OperationOutcome, RetainedBytes, WriteFrame,
};

mod support;

use support::TestFrame;

const EPOCH: ConnectionEpoch = ConnectionEpoch::new(4);

struct Accepted {
    operation: OperationId,
    effect: EffectId,
    key: MatchKey,
}

struct Harness {
    core: ConnectionCore<TestFrame>,
}

impl Harness {
    fn new() -> Result<Self, Box<dyn Error>> {
        let operations = 4;
        let retained = RetainedBytes::new(64);
        Ok(Self {
            core: ConnectionCore::new(
                bornera_core::EndpointId::new(1),
                bornera_core::LaneId::new(2),
                bornera_core::ConnectionId::new(3),
                EPOCH,
                ConnectionLimits::new(
                    operations,
                    retained,
                    operations,
                    retained,
                    MatchKeySpace::new(0, 3)?,
                )?,
            ),
        })
    }

    fn commit(&mut self, frame: TestFrame) -> Result<Accepted, Box<dyn Error>> {
        let bytes = frame.retained_bytes();
        let permit = self.core.reserve(
            Moment::ORIGIN,
            OperationOptions::until(Deadline::at(Moment::from_nanos(20)))
                .session()
                .retained_bytes(bytes)
                .write_retained_bytes(bytes),
        )?;
        let key = permit.match_key();
        let (operation, _) = self.core.commit(permit, frame)?;
        let (front, effect) = self
            .core
            .front_write(std::num::NonZeroUsize::MAX)
            .map_err(std::io::Error::other)?
            .map(|front| (front.operation, front.effect))
            .ok_or_else(|| std::io::Error::other("aggregate retained no write front"))?;
        assert_eq!(front, operation);
        Ok(Accepted {
            operation,
            effect,
            key,
        })
    }
}

fn outcome_delivery<F>(effects: &[ConnectionEffect<F>]) -> Option<Delivery> {
    effects.iter().find_map(|effect| match effect {
        ConnectionEffect::PublishOutcome {
            outcome:
                OperationOutcome::Failed { delivery, .. }
                | OperationOutcome::Cancelled { delivery },
            ..
        } => Some(*delivery),
        _ => None,
    })
}

#[test]
fn partial_writes_cross_one_delivery_boundary_before_opaque_matching() -> Result<(), Box<dyn Error>>
{
    let mut harness = Harness::new()?;
    let accepted = harness.commit(TestFrame(Vec::from([1, 2, 3])))?;

    let partial = harness.core.advance_write(EPOCH, accepted.effect, 1)?;
    assert!(partial.effects().is_empty());
    assert_eq!(harness.core.queued_write_frames(), 1);
    let complete = harness.core.advance_write(EPOCH, accepted.effect, 2)?;
    assert!(complete.effects().is_empty());
    assert_eq!(
        harness.core.buffered_write_retained_bytes(),
        RetainedBytes::ZERO
    );

    let reply = harness.core.apply_reply(InboundReply::new(
        EPOCH,
        accepted.key,
        TestFrame(Vec::from([9])),
    ))?;
    assert!(reply.effects().iter().any(|effect| matches!(
        effect,
        ConnectionEffect::PublishOutcome {
            operation,
            outcome: OperationOutcome::Reply(TestFrame(bytes)),
            ..
        } if *operation == accepted.operation && bytes == &[9]
    )));
    Ok(())
}

#[test]
fn reset_after_partial_progress_preserves_possible_send_and_discards_exact_write()
-> Result<(), Box<dyn Error>> {
    let mut harness = Harness::new()?;
    let accepted = harness.commit(TestFrame(Vec::from([1, 2, 3])))?;
    let _transition = harness.core.advance_write(EPOCH, accepted.effect, 1)?;

    let closed = harness.core.apply(ConnectionInput::CloseRequested {
        epoch: EPOCH,
        reason: CloseReason::TransportLost,
    })?;
    assert_eq!(harness.core.queued_write_frames(), 0);
    assert_eq!(
        harness.core.buffered_write_retained_bytes(),
        RetainedBytes::ZERO
    );
    assert_eq!(
        outcome_delivery(closed.effects()),
        Some(Delivery::PossiblySent)
    );
    Ok(())
}

#[test]
fn reset_after_complete_write_cannot_strengthen_delivery_certainty() -> Result<(), Box<dyn Error>> {
    let mut harness = Harness::new()?;
    let accepted = harness.commit(TestFrame(Vec::from([1, 2])))?;
    let _transition = harness.core.advance_write(EPOCH, accepted.effect, 2)?;

    let closed = harness.core.apply(ConnectionInput::CloseRequested {
        epoch: EPOCH,
        reason: CloseReason::TransportLost,
    })?;
    assert_eq!(
        outcome_delivery(closed.effects()),
        Some(Delivery::PossiblySent)
    );
    Ok(())
}

#[test]
fn empty_frame_completion_remains_not_sent_without_a_fabricated_write() -> Result<(), Box<dyn Error>>
{
    let mut harness = Harness::new()?;
    let accepted = harness.commit(TestFrame(Vec::new()))?;
    let _transition = harness.core.advance_write(EPOCH, accepted.effect, 0)?;

    let closed = harness.core.apply(ConnectionInput::CloseRequested {
        epoch: EPOCH,
        reason: CloseReason::TransportLost,
    })?;
    assert_eq!(outcome_delivery(closed.effects()), Some(Delivery::NotSent));
    Ok(())
}
