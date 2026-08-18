//! Focused evidence for generic bounded incremental frame driving.

use std::{error::Error, fmt};

use bornera_core::{FrameDecodeError, FrameDecoder, FrameDriver, RetainedBytes};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TestDecodeError;

impl fmt::Display for TestDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("test decoder rejected input")
    }
}

impl Error for TestDecodeError {}

#[derive(Debug)]
struct TinyDecoder {
    buffered: Vec<u8>,
}

impl TinyDecoder {
    fn new() -> Self {
        Self {
            buffered: Vec::new(),
        }
    }
}

impl FrameDecoder for TinyDecoder {
    type Frame = Vec<u8>;
    type Error = TestDecodeError;

    fn feed(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.buffered.extend_from_slice(bytes);
        Ok(())
    }

    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error> {
        let Some(length) = self.buffered.first().copied() else {
            return Ok(None);
        };
        if length == u8::MAX {
            return Err(TestDecodeError);
        }
        let total = usize::from(length) + 1;
        if self.buffered.len() < total {
            return Ok(None);
        }
        let mut complete: Vec<_> = self.buffered.drain(..total).collect();
        complete.remove(0);
        Ok(Some(complete))
    }

    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::try_from(self.buffered.len()).unwrap_or(RetainedBytes::new(u64::MAX))
    }
}

#[derive(Debug)]
struct ExpandingDecoder {
    retained: RetainedBytes,
}

impl FrameDecoder for ExpandingDecoder {
    type Frame = ();
    type Error = TestDecodeError;

    fn feed(&mut self, _: &[u8]) -> Result<(), Self::Error> {
        self.retained = RetainedBytes::new(9);
        Ok(())
    }

    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error> {
        Ok(None)
    }

    fn retained_bytes(&self) -> RetainedBytes {
        self.retained
    }
}

fn driver(limit: u64) -> Result<FrameDriver<TinyDecoder>, FrameDecodeError<TestDecodeError>> {
    FrameDriver::new(TinyDecoder::new(), RetainedBytes::new(limit))
}

#[test]
fn fragmented_prefix_and_body_emit_only_after_adapter_completion() -> Result<(), Box<dyn Error>> {
    let mut driver = driver(16)?;
    driver.feed(&[3, b'a'])?;
    assert_eq!(driver.next_frame()?, None);
    driver.feed(b"bc")?;
    assert_eq!(driver.next_frame()?, Some(b"abc".to_vec()));
    assert_eq!(driver.retained_bytes(), RetainedBytes::ZERO);
    Ok(())
}

#[test]
fn coalesced_input_remains_fifo_without_bornera_interpreting_lengths() -> Result<(), Box<dyn Error>>
{
    let mut driver = driver(16)?;
    driver.feed(&[1, b'a', 2, b'b', b'c'])?;
    assert_eq!(driver.next_frame()?, Some(b"a".to_vec()));
    assert_eq!(driver.next_frame()?, Some(b"bc".to_vec()));
    assert_eq!(driver.next_frame()?, None);
    Ok(())
}

#[test]
fn aggregate_rejection_preserves_the_borrowed_chunk_and_decoder_state() -> Result<(), Box<dyn Error>>
{
    let mut driver = driver(5)?;
    driver.feed(&[4, b'a', b'b'])?;
    let rejected = [b'c', b'd', b'e'];
    assert_eq!(
        driver.feed(&rejected),
        Err(FrameDecodeError::RetainedByteCapacity {
            retained: RetainedBytes::new(3),
            incoming: 3,
            limit: RetainedBytes::new(5),
        })
    );
    assert_eq!(rejected, [b'c', b'd', b'e']);
    assert_eq!(driver.retained_bytes(), RetainedBytes::new(3));
    driver.feed(&rejected[..2])?;
    assert_eq!(driver.next_frame()?, Some(b"abcd".to_vec()));
    Ok(())
}

#[test]
fn adapter_malformation_is_terminal_for_stream_alignment() -> Result<(), Box<dyn Error>> {
    let mut driver = driver(4)?;
    driver.feed(&[u8::MAX])?;
    assert_eq!(
        driver.next_frame(),
        Err(FrameDecodeError::Decoder(TestDecodeError))
    );
    assert_eq!(driver.next_frame(), Err(FrameDecodeError::DecoderFailed));
    assert_eq!(driver.feed(&[]), Err(FrameDecodeError::DecoderFailed));
    assert!(driver.is_failed());
    Ok(())
}

#[test]
fn retained_reporting_violation_is_terminal() -> Result<(), Box<dyn Error>> {
    let mut driver = FrameDriver::new(
        ExpandingDecoder {
            retained: RetainedBytes::ZERO,
        },
        RetainedBytes::new(4),
    )?;
    assert_eq!(
        driver.feed(&[1]),
        Err(FrameDecodeError::RetainedContractViolation {
            retained: RetainedBytes::new(9),
            limit: RetainedBytes::new(4),
        })
    );
    assert!(driver.is_failed());
    Ok(())
}
