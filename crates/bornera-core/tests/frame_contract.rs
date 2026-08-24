//! Adversarial safe-frame implementations cannot escape cached bounds.

use std::{
    cell::Cell,
    error::Error,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    rc::Rc,
};

use bornera_core::{
    ConnectionCore, ConnectionCoreError, ConnectionCoreInvariant, ConnectionEpoch, ConnectionId,
    ConnectionLimits, Deadline, EndpointId, FrameContractViolation, LaneId, MatchKeySpace, Moment,
    OperationOptions, RetainedBytes, WriteFrame,
};

const EPOCH: ConnectionEpoch = ConnectionEpoch::new(1);

#[derive(Debug)]
struct AdversarialFrame {
    bytes: Vec<u8>,
    visible: Rc<Cell<usize>>,
    retained_calls: Rc<Cell<usize>>,
}

impl WriteFrame for AdversarialFrame {
    fn bytes(&self) -> &[u8] {
        self.bytes.get(..self.visible.get()).unwrap_or(&[])
    }

    fn retained_bytes(&self) -> RetainedBytes {
        let calls = self.retained_calls.get();
        self.retained_calls.set(calls.saturating_add(1));
        if calls == 0 {
            RetainedBytes::new(3)
        } else {
            RetainedBytes::new(10_000)
        }
    }
}

fn core() -> Result<ConnectionCore<AdversarialFrame>, Box<dyn Error>> {
    Ok(ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        EPOCH,
        ConnectionLimits::new(
            2,
            RetainedBytes::ZERO,
            2,
            RetainedBytes::new(3),
            MatchKeySpace::new(0, 1)?,
        )?,
    ))
}

fn commit(
    core: &mut ConnectionCore<AdversarialFrame>,
    visible: Rc<Cell<usize>>,
    retained_calls: Rc<Cell<usize>>,
) -> Result<bornera_core::EffectId, Box<dyn Error>> {
    let permit = core.reserve(
        Moment::ORIGIN,
        OperationOptions::until(Deadline::at(Moment::from_nanos(10)))
            .session()
            .write_retained_bytes(RetainedBytes::new(3)),
    )?;
    let frame = AdversarialFrame {
        bytes: Vec::from([1, 2, 3]),
        visible,
        retained_calls,
    };
    let (operation, _transition) = core.commit(permit, frame)?;
    core.write_effect(operation)
        .ok_or_else(|| std::io::Error::other("committed frame has no write effect").into())
}

#[test]
fn retained_memory_is_sampled_once_for_every_owner() -> Result<(), Box<dyn Error>> {
    let visible = Rc::new(Cell::new(3));
    let retained_calls = Rc::new(Cell::new(0));
    let mut core = core()?;
    let effect = commit(&mut core, Rc::clone(&visible), Rc::clone(&retained_calls))?;

    assert_eq!(retained_calls.get(), 1);
    assert_eq!(core.buffered_write_retained_bytes(), RetainedBytes::new(3));
    assert_eq!(
        core.front_write(NonZeroUsize::MAX)?
            .map(|front| front.bytes.len()),
        Some(3)
    );
    let _transition = core.advance_write(EPOCH, effect, 3)?;
    assert_eq!(retained_calls.get(), 1);
    assert_eq!(core.buffered_write_retained_bytes(), RetainedBytes::ZERO);
    Ok(())
}

#[test]
fn shrinking_after_partial_progress_is_checked_and_poisoned() -> Result<(), Box<dyn Error>> {
    let visible = Rc::new(Cell::new(3));
    let retained_calls = Rc::new(Cell::new(0));
    let mut core = core()?;
    let effect = commit(&mut core, Rc::clone(&visible), Rc::clone(&retained_calls))?;
    assert_eq!(
        core.front_write(NonZeroUsize::MAX)?
            .map(|front| front.bytes.len()),
        Some(3)
    );
    let _transition = core.advance_write(EPOCH, effect, 1)?;
    visible.set(1);

    let observed = catch_unwind(AssertUnwindSafe(|| core.front_write(NonZeroUsize::MAX)));
    let result = observed.map_err(|_| std::io::Error::other("checked frame access panicked"))?;
    assert!(matches!(
        result,
        Err(ConnectionCoreError::Invariant(
            ConnectionCoreInvariant::FrameContractViolation(
                FrameContractViolation::WireLengthChanged {
                    measured: 3,
                    observed: 1
                }
            )
        ))
    ));
    visible.set(3);
    assert!(matches!(
        core.front_write(NonZeroUsize::MAX),
        Err(ConnectionCoreError::Poisoned(
            ConnectionCoreInvariant::FrameContractViolation(_)
        ))
    ));
    assert_eq!(core.queued_write_frames(), 1);
    assert_eq!(core.recover().operations.len(), 1);
    assert!(matches!(
        core.front_write(NonZeroUsize::MAX),
        Err(ConnectionCoreError::Poisoned(
            ConnectionCoreInvariant::FrameContractViolation(_)
        ))
    ));
    Ok(())
}

#[test]
fn growing_byte_view_is_a_contract_violation_not_new_capacity() -> Result<(), Box<dyn Error>> {
    let visible = Rc::new(Cell::new(1));
    let retained_calls = Rc::new(Cell::new(0));
    let mut core = core()?;
    let _effect = commit(&mut core, Rc::clone(&visible), Rc::clone(&retained_calls))?;
    visible.set(3);

    assert!(matches!(
        core.front_write(NonZeroUsize::MAX),
        Err(ConnectionCoreError::Invariant(
            ConnectionCoreInvariant::FrameContractViolation(
                FrameContractViolation::WireLengthChanged {
                    measured: 1,
                    observed: 3
                }
            )
        ))
    ));
    assert_eq!(core.buffered_write_retained_bytes(), RetainedBytes::new(3));
    Ok(())
}
