//! Adversarial safe transport capability for selector-free slot tests.

use std::io;

use bornera::{ConnectProgress, SlotTransport, TcpSocketPolicy};
use calandria::Interest;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectBehavior {
    Opened,
    AlreadyOpen,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectReadiness {
    Once,
    Sticky,
    Consumed,
}

#[derive(Debug)]
pub(crate) struct TestTransport {
    open: bool,
    connect_readiness: ConnectReadiness,
    connect: ConnectBehavior,
    policy_failure: bool,
    policy_applications: usize,
    read_overreport: Option<usize>,
    write_overreport: Option<usize>,
}

impl TestTransport {
    pub(crate) const fn benign() -> Self {
        Self::new(ConnectBehavior::Opened, ConnectReadiness::Once)
    }

    pub(crate) const fn malicious_read(extra: usize) -> Self {
        Self {
            read_overreport: Some(extra),
            ..Self::benign()
        }
    }

    pub(crate) const fn malicious_write(extra: usize) -> Self {
        Self {
            write_overreport: Some(extra),
            ..Self::benign()
        }
    }

    pub(crate) const fn sticky_already_open() -> Self {
        Self::new(ConnectBehavior::AlreadyOpen, ConnectReadiness::Sticky)
    }

    pub(crate) const fn connect_failed() -> Self {
        Self::new(ConnectBehavior::Failed, ConnectReadiness::Once)
    }

    pub(crate) const fn policy_failed() -> Self {
        Self {
            policy_failure: true,
            ..Self::benign()
        }
    }

    pub(crate) const fn policy_applications(&self) -> usize {
        self.policy_applications
    }

    const fn new(connect: ConnectBehavior, connect_readiness: ConnectReadiness) -> Self {
        Self {
            open: false,
            connect_readiness,
            connect,
            policy_failure: false,
            policy_applications: 0,
            read_overreport: None,
            write_overreport: None,
        }
    }
}

impl io::Read for TestTransport {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.read_overreport.map_or_else(
            || Err(io::Error::from(io::ErrorKind::WouldBlock)),
            |extra| Ok(buffer.len().saturating_add(extra)),
        )
    }
}

impl io::Write for TestTransport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.write_overreport.map_or_else(
            || Err(io::Error::from(io::ErrorKind::WouldBlock)),
            |extra| Ok(buffer.len().saturating_add(extra)),
        )
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SlotTransport for TestTransport {
    fn finish_connect(&mut self) -> io::Result<ConnectProgress> {
        if self.connect_readiness == ConnectReadiness::Once {
            self.connect_readiness = ConnectReadiness::Consumed;
        }
        match self.connect {
            ConnectBehavior::Opened => {
                self.open = true;
                Ok(ConnectProgress::Opened)
            }
            ConnectBehavior::AlreadyOpen => {
                self.open = true;
                Ok(ConnectProgress::AlreadyOpen)
            }
            ConnectBehavior::Failed => Err(io::Error::from(io::ErrorKind::ConnectionRefused)),
        }
    }

    fn apply_policy(&mut self, _policy: TcpSocketPolicy) -> io::Result<()> {
        self.policy_applications = self.policy_applications.saturating_add(1);
        if self.policy_failure {
            Err(io::Error::from(io::ErrorKind::PermissionDenied))
        } else {
            Ok(())
        }
    }

    fn can_finish_connect(&self) -> bool {
        self.connect_readiness != ConnectReadiness::Consumed
    }

    fn is_open(&self) -> bool {
        self.open
    }

    fn can_read(&self) -> bool {
        self.open && self.read_overreport.is_some()
    }

    fn can_write(&self) -> bool {
        self.open && self.write_overreport.is_some()
    }

    fn desired_interest(&self, has_writes: bool) -> Interest {
        if has_writes {
            Interest::READ_WRITE
        } else {
            Interest::READABLE
        }
    }

    fn clear_read(&mut self) {}

    fn clear_write(&mut self) {}
}
