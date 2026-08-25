//! Direct tests for the writer's aggregate retained-byte budget.

use std::{error::Error, num::NonZeroUsize};

use calandria::RetainedBytes;

use crate::{ConnectionEpoch, EffectId, OperationId};

use super::{FrameMeasure, WriteFrame, WriteProgress, WriteQueue, WriteQueueLimits};

const EPOCH: ConnectionEpoch = ConnectionEpoch::new(1);

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestFrame {
    bytes: [u8; 1],
    retained: RetainedBytes,
}

impl TestFrame {
    const fn new(retained: u64) -> Self {
        Self {
            bytes: [0],
            retained: RetainedBytes::new(retained),
        }
    }
}

impl WriteFrame for TestFrame {
    fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn retained_bytes(&self) -> RetainedBytes {
        self.retained
    }
}

fn admit(
    queue: &mut WriteQueue<TestFrame>,
    identity: u64,
    retained: u64,
) -> Result<(), Box<dyn Error>> {
    let frame = TestFrame::new(retained);
    queue
        .admit(
            EPOCH,
            OperationId::new(identity),
            EffectId::new(identity),
            FrameMeasure::capture(&frame),
            frame,
        )
        .map_err(Into::into)
}

#[test]
fn exact_cached_charges_drive_admission_completion_and_discard() -> Result<(), Box<dyn Error>> {
    let max_frames =
        NonZeroUsize::new(3).ok_or_else(|| std::io::Error::other("three must be nonzero"))?;
    let mut queue = WriteQueue::new(
        EPOCH,
        WriteQueueLimits::new(max_frames, RetainedBytes::new(5)),
    );

    admit(&mut queue, 1, 3)?;
    assert_eq!(queue.retained_bytes(), RetainedBytes::new(3));

    let rejected = TestFrame::new(3);
    let rejection = queue
        .admit(
            EPOCH,
            OperationId::new(2),
            EffectId::new(2),
            FrameMeasure::capture(&rejected),
            rejected.clone(),
        )
        .err()
        .ok_or_else(|| std::io::Error::other("over-budget frame was admitted"))?;
    assert_eq!(rejection.into_frame(), rejected);
    assert_eq!(queue.retained_bytes(), RetainedBytes::new(3));

    admit(&mut queue, 2, 2)?;
    assert_eq!(queue.retained_bytes(), RetainedBytes::new(5));

    assert!(matches!(
        queue.advance(EPOCH, EffectId::new(1), 1)?,
        WriteProgress::Complete { .. }
    ));
    assert_eq!(queue.retained_bytes(), RetainedBytes::new(2));

    assert!(queue.discard(EPOCH, EffectId::new(2))?.is_some());
    assert_eq!(queue.retained_bytes(), RetainedBytes::ZERO);

    admit(&mut queue, 3, 5)?;
    let discarded = queue.discard_all();
    assert_eq!(discarded.retained_bytes(), RetainedBytes::new(5));
    assert_eq!(queue.retained_bytes(), RetainedBytes::ZERO);
    Ok(())
}
