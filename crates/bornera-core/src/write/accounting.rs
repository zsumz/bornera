//! Lossless bridge from public retained bytes to the private aggregate budget.

use bytebudget::{ByteBudget, ByteCount};
use calandria::RetainedBytes;

pub(super) struct RetainedBudget {
    inner: ByteBudget,
}

impl RetainedBudget {
    pub(super) const fn new(limit: RetainedBytes) -> Self {
        Self {
            inner: ByteBudget::new(ByteCount::new(limit.get())),
        }
    }

    pub(super) const fn used(&self) -> RetainedBytes {
        RetainedBytes::new(self.inner.used().get())
    }

    pub(super) const fn can_reserve(&self, amount: RetainedBytes) -> bool {
        self.inner.can_reserve(ByteCount::new(amount.get()))
    }

    pub(super) fn try_reserve(&mut self, amount: RetainedBytes) -> bool {
        self.inner.try_reserve(ByteCount::new(amount.get())).is_ok()
    }

    pub(super) fn release(&mut self, amount: RetainedBytes) -> bool {
        self.inner.release(ByteCount::new(amount.get())).is_ok()
    }

    pub(super) fn clear(&mut self) -> RetainedBytes {
        let used = self.used();
        self.inner = ByteBudget::new(self.inner.limit());
        used
    }
}
