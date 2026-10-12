//! A client's core and driver together, and the loop's two primitives the
//! synchronous classes build every wait on, as C++ builds them on asio:
//! `run_one`, one event, or 0 when nothing could ever happen, and `poll`,
//! what is ready without waiting, a queue's worth of frames at most. Calls
//! into the core from the class on top go through `call`, which carries out
//! what the core asked for at once, as an asio call would start it.

use std::sync::Arc;
use std::time::Instant;

use crate::core::client::IncomingMode;
use crate::core::Core;
use crate::driver::{Driver, Inbox, Step};
use crate::error::{Error, Result};
use crate::ids::Ids;

/// The core and its driver.
#[derive(Debug)]
pub(crate) struct Engine {
    pub(crate) core: Core,
    pub(crate) driver: Driver,
}

impl Engine {
    /// A client of `peer_type`, nothing connected; `inbox`, for a threaded
    /// class, carries the commands of other threads.
    pub(crate) fn new(peer_type: u32, mode: IncomingMode, inbox: Option<Arc<Inbox>>) -> Result<Engine> {
        let driver = Driver::new(inbox).map_err(|error| Error::Io(error.to_string()))?;
        let core = Core::new(peer_type, Arc::new(Ids::new()), mode, Instant::now());
        Ok(Engine { core, driver })
    }

    /// Runs `body` on the core, at the time now, and carries out what it
    /// asked for.
    pub(crate) fn call<R>(&mut self, body: impl FnOnce(&mut Core) -> R) -> R {
        self.core.now = Instant::now();
        let result = body(&mut self.core);
        self.driver.carry_out_actions(&mut self.core);
        result
    }

    /// One event, waiting for one until `deadline`, which counts as an
    /// event itself; 0 when nothing could ever happen: no socket, no name
    /// being resolved, no timer and no posted work, and no deadline.
    pub(crate) fn run_one(&mut self, deadline: Option<Instant>) -> usize {
        match self.driver.step(&mut self.core, deadline, true) {
            Step::NoWork => 0,
            _ => 1,
        }
    }

    /// Every event ready now, without waiting, until a queue's worth of
    /// frames was handled: a multiplexer that writes faster than the client
    /// reads cannot hold the call here. A connection the multiplexer closed
    /// while no call ran the loop is noticed here and retired.
    pub(crate) fn poll(&mut self) {
        let until = self.core.client.frames_handled + crate::defaults::DEFAULT_INCOMING_QUEUE_MAX_SIZE.max(1) as u64;
        while self.core.client.frames_handled < until {
            if self.driver.step(&mut self.core, None, false) != Step::Ran {
                return;
            }
        }
    }

    /// Whether nothing is left to happen: what ends a threaded class's io
    /// thread after its shutdown.
    pub(crate) fn idle(&mut self) -> bool {
        !self.driver.has_work() && !self.core.has_work()
    }
}
