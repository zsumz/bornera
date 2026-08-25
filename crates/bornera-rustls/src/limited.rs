//! Raw I/O wrappers that make rustls respect one Bornera byte budget.

use std::io::{self, Read, Write};

pub(crate) struct LimitedReader<'a, R> {
    inner: &'a mut R,
    remaining: usize,
}

impl<'a, R> LimitedReader<'a, R> {
    pub(crate) const fn new(inner: &'a mut R, remaining: usize) -> Self {
        Self { inner, remaining }
    }
}

impl<R: Read> Read for LimitedReader<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let length = buffer.len().min(self.remaining);
        let read = self.inner.read(&mut buffer[..length])?;
        self.remaining = self.remaining.saturating_sub(read);
        Ok(read)
    }
}

pub(crate) struct LimitedWriter<'a, W> {
    inner: &'a mut W,
    remaining: usize,
}

impl<'a, W> LimitedWriter<'a, W> {
    pub(crate) const fn new(inner: &'a mut W, remaining: usize) -> Self {
        Self { inner, remaining }
    }
}

impl<W: Write> Write for LimitedWriter<'_, W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let length = buffer.len().min(self.remaining);
        let written = self.inner.write(&buffer[..length])?;
        self.remaining = self.remaining.saturating_sub(written);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
