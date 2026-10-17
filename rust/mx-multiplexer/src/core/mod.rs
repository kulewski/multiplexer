//! The core: everything about the protocol a client does, with no socket,
//! no thread and no clock of its own, so that one core serves the four
//! classes and runs on a virtual clock in tests. Mirrors the client half of
//! multiplexer/io/connection.h (`connection`), basic_client.cc and its
//! parts with the client's half of connections_manager.h (`client`),
//! outbox.cc and its parts (`outbox`), basic_client_batching.cc
//! (`batching`, with `plain`, a plain send's bytes, and `bodies`, what a
//! batch hands back and keeps), basic_client_confirm.cc (`confirming`,
//! confirmed sends, both sides), the query state machine of
//! threaded_client_query.cc (`query`) and the io thread's other work of
//! threaded_client_io.cc (`threaded`); this file holds what they share.
//!
//! Events go in, each with the time the driver read (`Core::now`):
//! connections up or failed, frames read, writes done, names resolved,
//! timers due, and the calls of the class on top. What the core wants done
//! with the sockets comes out as `Action`s, which the driver carries out
//! after every call into the core, in order. The core's own deferred work,
//! where C++ uses asio's `post`, waits in `posted`, a first-in first-out
//! queue the driver runs one entry at a time, so that the orders the C++
//! tests pin are kept: a flush ends only after the callbacks of the sends
//! it waited for have run, for one.
//!
//! The program's callbacks run inline, as C++ runs them, the core borrowed
//! meanwhile: a `ThreadedClient`'s reach the client again only through its
//! command queue, which the driver runs as posted work, as asio runs a post.

pub(crate) mod timers;

pub(crate) mod batching;
pub(crate) mod bodies;
pub(crate) mod client;
pub(crate) mod confirming;
pub(crate) mod connection;
pub(crate) mod lane;
pub(crate) mod outbox;
pub(crate) mod plain;
pub(crate) mod query;
pub(crate) mod threaded;

#[cfg(test)]
pub(crate) mod harness;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use batching::Batcher;
pub(crate) use client::{ClientState, IncomingMode};
use confirming::Confirmer;
pub(crate) use connection::Conn;
pub(crate) use outbox::Outbox;
use query::{Queries, QueryId};
use threaded::Threaded;
use timers::Timers;

use crate::frame::Head;
use crate::ids::Ids;

/// A connection's number in the core and the driver, never reused: where
/// C++ holds a weak pointer to a connection, Rust holds this and looks the
/// connection up, which fails once it is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ConnId(pub(crate) u64);

/// What a connection was asked to connect to: a host name or an address,
/// and a port. Resolved on every attempt, so that a multiplexer whose
/// address changed is found again at the next reconnect.
pub(crate) type Target = (String, u16);

/// A queued frame's fate: not written yet, written to the socket, or
/// dropped (C++ SendState).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum SendState {
    Queued = 0,
    Sent = 1,
    Lost = 2,
}

/// A message's tracker: what it reads, QUEUED until the message is written
/// or given up on. A connection's queue holds it weakly and whoever follows
/// the message strongly, so that `Arc::strong_count` above one says, as
/// C++ `use_count() > 1` does, that somebody follows it. Atomic only so
/// that the core is `Send`; one thread reads and writes it.
#[derive(Debug)]
pub(crate) struct Tracker(AtomicU8);

impl Tracker {
    /// A new tracker reading `state`.
    pub(crate) fn new(state: SendState) -> Arc<Tracker> {
        Arc::new(Tracker(AtomicU8::new(state as u8)))
    }

    /// What it reads.
    pub(crate) fn get(&self) -> SendState {
        match self.0.load(Ordering::Relaxed) {
            0 => SendState::Queued,
            1 => SendState::Sent,
            _ => SendState::Lost,
        }
    }

    /// Makes it read `state`.
    pub(crate) fn set(&self, state: SendState) {
        self.0.store(state as u8, Ordering::Relaxed);
    }
}

/// Whether somebody besides one queue entry or one waiting entry follows
/// the message `tracker` tracks (C++ `use_count() > 1`).
pub(crate) fn followed(tracker: &Arc<Tracker>) -> bool {
    Arc::strong_count(tracker) > 1
}

/// A frame to write: header and body in one buffer, which every connection
/// it goes to shares, or a big body behind its header kept apart (`head`,
/// `Core::alone_frame`), with what the client knows of it without parsing
/// it back (C++ RawMessage's marks): the id and type a drop is reported by,
/// whether it is pinned to its connection, one of a send to ALL, or a
/// connection's own protocol frame, its place in the order sent, which
/// flush_all() goes by, the deadline its first send gave it, and, for a
/// batch's frame, the ids of the messages inside, by which a drop of it is
/// told; a confirmed batch's, `confirmed`, is told by the confirmed sends,
/// each message once (`Core::confirmed_frame_dropped`). Built whole where it
/// is first placed and never changed, so that a frame a dead connection
/// hands over keeps its place and its deadline.
#[derive(Debug)]
pub(crate) struct OutFrame {
    pub(crate) bytes: Bytes,
    pub(crate) head: Option<Head>,
    pub(crate) id: u64,
    pub(crate) r#type: u32,
    pub(crate) pinned: bool,
    pub(crate) for_all: bool,
    pub(crate) own: bool,
    pub(crate) number: u64,
    pub(crate) deadline: Option<Instant>,
    pub(crate) batch: Option<Arc<[u64]>>,
    pub(crate) confirmed: bool,
}

impl OutFrame {
    /// Its length on the wire: a header kept apart, then its bytes.
    pub(crate) fn wire_length(&self) -> usize {
        self.head.map_or(0, |head| head.len()) + self.bytes.len()
    }
}

/// One entry of a connection's outgoing queue: the frame and its tracker,
/// held weakly.
pub(crate) type QueueEntry = (std::sync::Weak<Tracker>, Arc<OutFrame>);

/// When something given `timeout` stops waiting: never for no timeout or
/// one past MAX_SECONDS, as lib/seconds.h reads every timeout; at once, a
/// deadline already here, for a zero one.
pub(crate) fn deadline_after(now: Instant, timeout: Option<Duration>) -> Option<Instant> {
    match timeout {
        Some(timeout) if timeout <= MAX_TIMEOUT => now.checked_add(timeout),
        _ => None,
    }
}

/// The longest a timeout stands for, about 31.7 years; longer is no
/// deadline at all (lib/seconds.h's MAX_SECONDS).
pub(crate) const MAX_TIMEOUT: Duration = Duration::from_secs(1_000_000_000);

/// What a timer is for (C++ arms an asio timer per job).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Due {
    /// A connection's write silence: a heartbeat goes out.
    SendHeartbit(ConnId),
    /// A connection's read silence, its first phase.
    RequireHeartbitSoon(ConnId),
    /// A connection's read silence, its second phase: the connection goes.
    RequireHeartbitNow(ConnId),
    /// The bound of a connection's reading to its peer's end.
    ReadToTheEnd(ConnId),
    /// A TCP connect's CONNECT_ATTEMPT_SECONDS.
    ConnectDeadline(ConnId),
    /// A lost connection's target is connected to again.
    Reconnect(u64),
    /// The earliest deadline of what waits in the outbox.
    OutboxExpiry,
    /// The drop lines' summary.
    LogSummary,
    /// A query's stage deadline, for the stage numbered so.
    Query(QueryId, u64),
    /// A connect() call's timeout.
    ConnectWait(u64),
    /// A flush_all() call's timeout.
    FlushWait(u64),
    /// The earliest deadline of a flushing send.
    SendsExpire,
    /// The shutdown's write-out ran out of time.
    Drain,
    /// The earliest time a batch open goes by: its delay's end, or, with no
    /// delay, its deadline.
    Batches,
    /// The earliest retry time or deadline among the confirmed batches
    /// waiting for their confirmation.
    Confirmations,
}

/// What the core wants done with a socket, for the driver to carry out in
/// order after the call that asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Action {
    /// Resolve `host` and `port`; the driver answers with `on_resolved`.
    Resolve(ConnId, String, u16),
    /// Open a socket, closed on exec, and connect it to the address; the
    /// driver answers with `on_connected`. A socket the connection had is
    /// closed first.
    Open(ConnId, SocketAddr),
    /// Cancel the connect under way, which then ends with an error.
    CancelConnect(ConnId),
    /// Read the connection's frames as they come: after the connect.
    StartReading(ConnId),
    /// Write a frame, its header first when kept apart from its bytes
    /// (`OutFrame::head`); one at a time, the driver answering with
    /// `on_write_done`.
    Write(ConnId, Option<Head>, Bytes),
    /// No more writes: what the kernel holds goes out, then this end of the
    /// stream; reading goes on.
    ShutdownWrite(ConnId),
    /// No more reading or writing: a write under way ends at once, and its
    /// end is still reported.
    Shutdown(ConnId),
    /// Close the socket; nothing more is reported for it but the end of a
    /// write under way.
    Close(ConnId),
    /// Forget every name being resolved: the client shut down, and no
    /// answer is waited for.
    CancelResolves,
}

/// Deferred work, C++'s `post`: run in order, one entry per step of a
/// synchronous wait.
pub(crate) enum Posted {
    /// A connect that could not even open its socket, reported as a failed
    /// connect is.
    ConnectFailed(ConnId, String),
    /// The pass over the program's callbacks: the drops told, then the
    /// followed sends' ends (outbox's _process_callbacks).
    ProcessCallbacks,
    /// The queries that waited for a connection start again.
    Fill,
    /// A query follows its unwritten attempt to the connection the outbox
    /// handed it to: the query, its stage's number, and whether the attempt
    /// is the request (else the direct request).
    FollowHandover(QueryId, u64, bool),
    /// The shutdown's write-out goes on, or ends.
    WriteOutStep,
    /// A command of the class on top, from another thread or a callback.
    Command(Box<dyn FnOnce(&mut Core) + Send>),
    /// A threaded client's plain send, as data, with its deadline from the
    /// send (`plain`).
    Plain(plain::Plain, Option<Instant>),
    /// What a threaded client owes its producers goes, at the end of the
    /// io thread's turn that took confirmations its program gave
    /// (`confirming`).
    SendConfirmations,
}

impl std::fmt::Debug for Posted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Posted::ConnectFailed(conn, why) => write!(formatter, "ConnectFailed({conn:?}, {why:?})"),
            Posted::ProcessCallbacks => write!(formatter, "ProcessCallbacks"),
            Posted::Fill => write!(formatter, "Fill"),
            Posted::FollowHandover(query, generation, request) => {
                write!(formatter, "FollowHandover({query:?}, {generation}, {request})")
            }
            Posted::WriteOutStep => write!(formatter, "WriteOutStep"),
            Posted::Command(_) => write!(formatter, "Command"),
            Posted::Plain(plain, _) => write!(formatter, "Plain(#{})", plain.id),
            Posted::SendConfirmations => write!(formatter, "SendConfirmations"),
        }
    }
}

/// The core; see the module comment.
pub(crate) struct Core {
    /// The time of the event being handled, set by the driver.
    pub(crate) now: Instant,
    pub(crate) timers: Timers,
    pub(crate) posted: VecDeque<Posted>,
    pub(crate) actions: Vec<Action>,
    pub(crate) ids: Arc<Ids>,
    pub(crate) instance_id: u64,
    pub(crate) peer_type: u32,
    /// Every connection that still exists: live, connecting, reading to
    /// its end, or shut down with a write's end still to come.
    pub(crate) conns: HashMap<ConnId, Conn>,
    next_conn: u64,
    /// The registered connections by the multiplexer's instance id
    /// (ConnectionsManager's connection_by_id_).
    pub(crate) by_peer_id: BTreeMap<u64, ConnId>,
    /// The registered connections in round-robin order
    /// (connections_by_type_[MULTIPLEXER]): the one used goes to the back.
    pub(crate) round_robin: VecDeque<ConnId>,
    pub(crate) client: ClientState,
    pub(crate) outbox: Outbox,
    pub(crate) batcher: Batcher,
    pub(crate) confirmer: Confirmer,
    pub(crate) queries: Queries,
    queries_fill_posted: bool,
    pub(crate) threaded: Threaded,
}

impl std::fmt::Debug for Core {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Core")
            .field("instance_id", &self.instance_id)
            .field("peer_type", &self.peer_type)
            .field("connections", &self.conns.len())
            .finish_non_exhaustive()
    }
}

impl Core {
    /// A core for a peer of `peer_type`, its instance id drawn from `ids`,
    /// at `now`; `mode` says whether what arrives is queued for a receive
    /// or handed out as it comes.
    pub(crate) fn new(peer_type: u32, ids: Arc<Ids>, mode: IncomingMode, now: Instant) -> Core {
        let instance_id = ids.next();
        log::debug!("created a client with id {instance_id}");
        Core {
            now,
            timers: Timers::default(),
            posted: VecDeque::new(),
            actions: Vec::new(),
            ids,
            instance_id,
            peer_type,
            conns: HashMap::new(),
            next_conn: 0,
            by_peer_id: BTreeMap::new(),
            round_robin: VecDeque::new(),
            client: ClientState::new(mode),
            outbox: Outbox::default(),
            batcher: Batcher::default(),
            confirmer: Confirmer::default(),
            queries: Queries::default(),
            queries_fill_posted: false,
            threaded: Threaded::default(),
        }
    }

    /// A new message id.
    pub(crate) fn random64(&self) -> u64 {
        self.ids.next()
    }

    /// A fresh connection number.
    fn new_conn_id(&mut self) -> ConnId {
        self.next_conn += 1;
        ConnId(self.next_conn)
    }

    /// Runs the timer due now with the earliest deadline, if any: one step
    /// of a synchronous wait. True when one ran.
    pub(crate) fn run_one_timer(&mut self) -> bool {
        let Some((_, due)) = self.timers.take_due(self.now) else {
            return false;
        };
        match due {
            Due::SendHeartbit(conn) => self.send_heartbit_now(conn),
            Due::RequireHeartbitSoon(conn) => self.require_heartbit_soon(conn),
            Due::RequireHeartbitNow(conn) => self.require_heartbit_now(conn),
            Due::ReadToTheEnd(conn) => self.read_to_the_end_bound(conn),
            Due::ConnectDeadline(conn) => self.connect_deadline(conn),
            Due::Reconnect(reconnect) => self.reconnect_after_timeout(reconnect),
            Due::OutboxExpiry => self.expire(),
            Due::LogSummary => self.log_summary_tick(),
            Due::Query(query, generation) => self.query_deadline(query, generation),
            Due::ConnectWait(id) => self.connect_wait_expired(id),
            Due::FlushWait(id) => self.flush_wait_expired(id),
            Due::SendsExpire => self.sends_expire(),
            Due::Drain => self.drain_expired(),
            Due::Batches => self.batch_timer_fired(),
            Due::Confirmations => self.confirm_timer_fired(),
        }
        true
    }

    /// Runs the oldest posted work, if any: the virtual clock's step, which
    /// plays the driver's. True when there was some.
    #[cfg(test)]
    pub(crate) fn run_one_posted(&mut self) -> bool {
        let Some(posted) = self.posted.pop_front() else {
            return false;
        };
        self.run_posted(posted);
        true
    }

    /// Runs one piece of posted work, which the driver took in its turn.
    pub(crate) fn run_posted(&mut self, posted: Posted) {
        match posted {
            Posted::ConnectFailed(conn, why) => self.connected(conn, Err(why)),
            Posted::ProcessCallbacks => {
                self.outbox.callbacks_posted = false;
                self.process_callbacks();
            }
            Posted::Fill => self.fill(),
            Posted::FollowHandover(query, generation, request) => self.followed_handover(query, generation, request),
            Posted::WriteOutStep => self.write_out_step(),
            Posted::Command(command) => command(self),
            Posted::Plain(plain, deadline) => self.place_plain(plain, deadline),
            Posted::SendConfirmations => self.send_confirmations_posted(),
        }
    }

    /// Whether the core has work that will come to something with no new
    /// event: posted work or a timer armed. With no socket open and nothing
    /// being resolved either, a synchronous wait has nothing to wait for.
    pub(crate) fn has_work(&mut self) -> bool {
        !self.posted.is_empty() || self.timers.any_armed()
    }
}

/// A connection's live flag, readable from any thread without the core: a
/// lane holding the connection reads it, since a dead connection lives on
/// a while as it reads to the end.
pub(crate) type LivingFlag = Arc<AtomicBool>;
