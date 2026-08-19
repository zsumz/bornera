//! Exact nonblocking read, decode, classify, and partial-write progression.

use std::io::{self, Read, Write};

use bornera_core::{CloseReason, FrameDecoder, InboundReply};
use calandria::Retained;

use crate::{ConnectionEngine, EngineError, EngineInvariant, InboundClassifier};

impl<D, C> ConnectionEngine<D, C>
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

    pub(crate) fn drive_read_once(&mut self) -> Result<Option<()>, EngineError> {
        let Some(token) = self.transport else {
            return Ok(None);
        };
        if !self.resource_mut(token)?.1.can_read() {
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
        let result = {
            let (resources, buffer) = (&mut self.resources, &mut self.read_buffer);
            let (_, transport) = resources
                .get_mut(token)
                .map_err(|_| invariant(EngineInvariant::ResourceToken))?;
            transport.read(&mut buffer[..length])
        };
        match result {
            Ok(0) => self.close_for(CloseReason::TransportLost)?,
            Ok(read) => {
                if let Err(error) = self.decoder.feed(&self.read_buffer[..read]) {
                    self.close_decode_error(&error)?;
                } else {
                    self.decoder_pending = true;
                }
            }
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                self.resource_mut(token)?.1.clear_read();
            }
            Err(_) => self.close_for(CloseReason::TransportLost)?,
        }
        Ok(Some(()))
    }

    pub(crate) fn drive_write_once(&mut self) -> Result<Option<()>, EngineError> {
        let Some(token) = self.transport else {
            return Ok(None);
        };
        if !self.resource_mut(token)?.1.is_open() {
            return Ok(None);
        }
        let Some(front) = self.core.front_write(self.limits.io_chunk_bytes()) else {
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
        let result = {
            let (core, resources) = (&self.core, &mut self.resources);
            let Some(front) = core.front_write(self.limits.io_chunk_bytes()) else {
                return Ok(None);
            };
            let (_, transport) = resources
                .get_mut(token)
                .map_err(|_| invariant(EngineInvariant::ResourceToken))?;
            if !transport.can_write() {
                return Ok(None);
            }
            transport.write(front.bytes)
        };
        match result {
            Ok(0) => self.close_for(CloseReason::TransportLost)?,
            Ok(written) => {
                let transition = self
                    .core
                    .advance_write(epoch, effect, written)
                    .map_err(EngineError::Core)?;
                self.interpret_unit(transition)?;
            }
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                self.resource_mut(token)?.1.clear_write();
            }
            Err(_) => self.close_for(CloseReason::TransportLost)?,
        }
        Ok(Some(()))
    }

    pub(crate) fn sync_interest(&mut self) -> Result<usize, EngineError> {
        let Some(token) = self.transport else {
            return Ok(0);
        };
        let has_writes = self.core.queued_write_frames() > 0;
        let desired = self.resource_mut(token)?.1.desired_interest(has_writes);
        let current = self.resource_mut(token)?.1.interest();
        if desired == current {
            return Ok(0);
        }
        let (poller, resources) = (&mut self.poller, &mut self.resources);
        let (_, transport) = resources
            .get_mut(token)
            .map_err(|_| invariant(EngineInvariant::ResourceToken))?;
        poller.reregister(transport, token, desired)?;
        transport.set_interest(desired);
        Ok(1)
    }

    pub(crate) fn has_runnable_io(&self) -> bool {
        if self.decoder_pending {
            return true;
        }
        let Some(token) = self.transport else {
            return false;
        };
        let Ok((_, transport)) = self.resources.get(token) else {
            return false;
        };
        transport.can_finish_connect()
            || transport.can_read()
            || (transport.can_write() && self.core.queued_write_frames() > 0)
    }
}

fn invariant(source: EngineInvariant) -> EngineError {
    EngineError::Invariant(source)
}
