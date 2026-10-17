//! A lane: one connection for a stream of messages, owned by the caller,
//! shared between the caller's thread and the core's. Mirrors
//! multiplexer/basic_client.h's Lane: empty until its first use, when it
//! takes the connection the library chose; a lane that is not pinned lets
//! go at a failover, the library writing into it the connection that took
//! what the dead one had not written, and adopts the connection a query's
//! reply came through; a pinned lane keeps its first connection for good,
//! and once that is gone every send and query through it fails with
//! `NotConnected`. A lane holds its connection's handle, which carries the
//! connection's live flag, and nothing else: it keeps no connection alive,
//! and the library keeps no registry of lanes. One made before a fork is
//! the parent's: in the child a send or a query through it fails with
//! `UsedAfterFork`, and its getters answer as for a lane nothing goes
//! through, holding no connection, not connected, closed, each before it
//! would take the lock, which a parent thread may hold for good there.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::client::Wrapper;
use crate::error::{Error, Result};

/// What the lock guards: the connection held, and whether one ever was.
#[derive(Debug, Default)]
struct Held {
    connection: Option<Wrapper>,
    holds: bool,
}

#[derive(Debug)]
struct Inner {
    pinned: bool,
    generation: u32,
    /// The instance id of the client whose connection it took, 0 before.
    owner: AtomicU64,
    held: Mutex<Held>,
}

/// A lane; see the module comment. Clones are the same lane.
#[derive(Debug, Clone)]
pub(crate) struct Lane(Arc<Inner>);

impl Lane {
    /// An empty lane, pinned or not.
    pub(crate) fn new(pinned: bool) -> Lane {
        Lane(Arc::new(Inner {
            pinned,
            generation: crate::fork::generation(),
            owner: AtomicU64::new(0),
            held: Mutex::new(Held::default()),
        }))
    }

    /// A lane seeded with `connection`, the one a reply came through: as old
    /// as the connection.
    pub(crate) fn seeded(connection: &Wrapper, pinned: bool) -> Lane {
        Lane(Arc::new(Inner {
            pinned,
            generation: connection.generation,
            owner: AtomicU64::new(connection.owner),
            held: Mutex::new(Held { connection: Some(connection.clone()), holds: true }),
        }))
    }

    /// Fails with `UsedAfterFork` for a lane that is the parent's in this
    /// process; one load, before any lock.
    pub(crate) fn check_not_inherited(&self) -> Result<()> {
        if self.inherited() {
            return Err(Error::UsedAfterFork);
        }
        Ok(())
    }

    /// Whether the lane is the parent's in this process.
    fn inherited(&self) -> bool {
        self.0.generation != crate::fork::generation()
    }

    /// The lock, which nothing panics under.
    fn held(&self) -> std::sync::MutexGuard<'_, Held> {
        self.0.held.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn pinned(&self) -> bool {
        self.0.pinned
    }

    /// The instance id of the client whose connection the lane took, or was
    /// seeded with; None before.
    pub(crate) fn owner(&self) -> Option<u64> {
        match self.0.owner.load(Ordering::Acquire) {
            0 => None,
            owner => Some(owner),
        }
    }

    /// The connection held; None until the first message went through, and
    /// in a forked child.
    pub(crate) fn connection(&self) -> Option<Wrapper> {
        if self.inherited() {
            return None;
        }
        self.held().connection.clone()
    }

    /// Whether a connection was ever written into the lane; not in a forked
    /// child.
    pub(crate) fn holds_connection(&self) -> bool {
        if self.inherited() {
            return false;
        }
        self.held().holds
    }

    /// Whether the connection held is live; not in a forked child.
    pub(crate) fn connected(&self) -> bool {
        if self.inherited() {
            return false;
        }
        let held = self.held();
        held.holds && held.connection.as_ref().is_some_and(Wrapper::live)
    }

    /// A pinned lane whose connection is gone: nothing goes through it any
    /// more. From any thread, and from the moment the connection stops
    /// being live; any lane, in a forked child.
    pub(crate) fn closed(&self) -> bool {
        if self.inherited() {
            return true;
        }
        let held = self.held();
        self.0.pinned && held.holds && !held.connection.as_ref().is_some_and(Wrapper::live)
    }

    /// The library, on the core's thread, writing the connection it used or
    /// a reply came through; a pinned lane takes the first connection only.
    pub(crate) fn adopt(&self, connection: &Wrapper) {
        if self.inherited() {
            return;
        }
        let mut held = self.held();
        if self.0.pinned && held.holds {
            return;
        }
        held.connection = Some(connection.clone());
        held.holds = true;
        self.0.owner.store(connection.owner, Ordering::Release);
    }
}
