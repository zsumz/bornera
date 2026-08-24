//! Exact nonblocking read, decode, classify, and partial-write progression.

use std::io;

use bornera_core::{CloseReason, FrameDecoder, InboundReply};
use calandria::{Interest, Retained};

use crate::{
    ConnectionSlot, EngineError, InboundClassifier, SlotTransport, TransportDiagnostic,
    TransportFailurePhase,
};

impl<D, C> ConnectionSlot<D, C>
where
    D: FrameDecoder,
    D::Frame: Retained,
    C: InboundClassifier<D::Frame>,
{
    pub(crate) fn drive_decoder_once(&mut self) -> Result<(), EngineError> {
        let frame = match self.decoder.next_frame() {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                self.decoder_pending = false;
                if self.decoder.retained_bytes() >= self.limits.decoder_bytes() {
                    self.close_for(CloseReason::InboundRetainedCapacity)?;
                }
                return Ok(());
            }
            Err(error) => {
                self.decoder_pending = false;
                self.close_decode_error(&error)?;
                return Ok(());
            }
        };
        if frame.retained_bytes() > self.limits.reply_bytes() {
            self.close_for(CloseReason::InboundRetainedCapacity)?;
            return Ok(());
        }
        let Ok(key) = self.classifier.reply_key(&frame) else {
            self.close_malformed()?;
            return Ok(());
        };
        let transition = self
            .core
            .apply_reply(InboundReply::new(self.core.epoch(), key, frame))
            .map_err(EngineError::Core)?;
        self.interpret_reply(transition)
    }

    pub(crate) fn drive_read_once<T: SlotTransport + ?Sized>(
        &mut self,
        transport: &mut T,
    ) -> Result<Option<()>, EngineError> {
        if !transport.can_read() {
            return Ok(None);
        }
        let remaining = self
            .limits
            .decoder_bytes()
            .get()
            .saturating_sub(self.decoder.retained_bytes().get());
        let length = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(self.read_buffer.len());
        if length == 0 {
            self.close_for(CloseReason::InboundRetainedCapacity)?;
            return Ok(Some(()));
        }
        match transport.read(&mut self.read_buffer[..length]) {
            Ok(0) => self.close_for(CloseReason::TransportLost)?,
            Ok(read) => {
                if read > length {
                    return Err(EngineError::Invariant(
                        crate::EngineInvariant::TransportReadContract {
                            capacity: length,
                            reported: read,
                        },
                    ));
                }
                let bytes = &self.read_buffer[..read];
                if let Err(error) = self.decoder.feed(bytes) {
                    self.close_decode_error(&error)?;
                } else {
                    self.decoder_pending = true;
                }
            }
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                transport.clear_read();
            }
            Err(source) => {
                self.record_transport_failure(TransportDiagnostic::from_io(
                    TransportFailurePhase::Read,
                    &source,
                ));
                self.close_for(CloseReason::TransportLost)?;
            }
        }
        Ok(Some(()))
    }

    pub(crate) fn drive_write_once<T: SlotTransport + ?Sized>(
        &mut self,
        transport: &mut T,
    ) -> Result<Option<()>, EngineError> {
        if !transport.is_open() {
            return Ok(None);
        }
        let Some(front) = self
            .core
            .front_write(self.limits.io_chunk_bytes())
            .map_err(EngineError::Core)?
        else {
            return Ok(None);
        };
        let epoch = front.epoch;
        let effect = front.effect;
        if front.bytes.is_empty() {
            let transition = self
                .core
                .advance_write(epoch, effect, 0)
                .map_err(EngineError::Core)?;
            self.interpret_unit(transition)?;
            return Ok(Some(()));
        }
        if !transport.can_write() {
            return Ok(None);
        }
        let written = match transport.write(front.bytes) {
            Ok(0) => {
                self.close_for(CloseReason::TransportLost)?;
                return Ok(Some(()));
            }
            Ok(written) => written,
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                transport.clear_write();
                return Ok(Some(()));
            }
            Err(source) => {
                self.record_transport_failure(TransportDiagnostic::from_io(
                    TransportFailurePhase::Write,
                    &source,
                ));
                self.close_for(CloseReason::TransportLost)?;
                return Ok(Some(()));
            }
        };
        let transition = self
            .core
            .advance_write(epoch, effect, written)
            .map_err(EngineError::Core)?;
        self.interpret_unit(transition)?;
        Ok(Some(()))
    }

    pub(crate) fn desired_interest<T: SlotTransport + ?Sized>(&self, transport: &T) -> Interest {
        transport.desired_interest(self.core.queued_write_frames() > 0)
    }

    pub(crate) fn has_runnable_io<T: SlotTransport + ?Sized>(&self, transport: &T) -> bool {
        self.decoder_pending
            || (self.is_connecting() && (transport.is_open() || transport.can_establish()))
            || (self.is_transport_open() && transport.has_transport_work())
            || (self.is_transport_open() && transport.can_read())
            || (self.is_transport_open()
                && transport.is_open()
                && self.core.queued_write_frames() > 0
                && (self.core.front_write_is_empty() || transport.can_write()))
    }
}
