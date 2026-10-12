//! What a `ThreadedClient` adds to the core on its io thread: the messages
//! that answer no query (the protocol's own handled or dropped, PINGs and
//! searches answered, the rest to on_message or logged and dropped), the
//! connect() and flush_all() calls waiting, the sends a caller waits for
//! or wants told of with their deadlines, and the shutdown's write-out.
//! Mirrors multiplexer/threaded_client_io.cc.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use prost::Message as _;

use super::client::{guarded, DropLine, DropReason, Incoming, Wrapper};
use super::lane::Lane;
use super::outbox::{FlushDone, FlushId, Frame, SendDone};
use super::timers::TimerKey;
use super::{deadline_after, Core, Due, OutFrame};
use crate::constants::types;
use crate::defaults::{DEFAULT_TIMEOUT, MAX_MESSAGE_SIZE, ROOM_GRACE_SECONDS};
use crate::frame;
use crate::log_summary::Kind;
use crate::pending::Queue;
use crate::proto::multiplexer::MultiplexerMessage;

/// What on_message is given.
pub(crate) type OnMessage = Box<dyn FnMut(crate::handles::Incoming) + Send>;

/// The search policy: whether to answer a search for a backend now.
pub(crate) type SearchPolicy = Box<dyn FnMut() -> bool + Send>;

/// A connect() call's end: whether its connection is registered.
pub(crate) type ConnectDone = Box<dyn FnOnce(bool) + Send>;

/// A flush_all() call's end: whether everything was written.
pub(crate) type FlushAllDone = Box<dyn FnOnce(bool) + Send>;

/// A send's end, for its caller: the copies written, 1 at the first or 0,
/// and, for one that wrote nothing, whether no connection was live then or
/// the message was given up on (NotConnected rather than
/// OperationTimedOut).
pub(crate) type SendSettled = Box<dyn FnOnce(u32, bool) + Send>;

/// A connect() waiting for its connection's handshake.
struct ConnectWaiter {
    id: u64,
    connection: Wrapper,
    done: Option<ConnectDone>,
    timer: Option<TimerKey>,
}

/// A flush_all() waiting.
struct FlushWait {
    id: u64,
    flush: Option<FlushId>,
    done: Option<FlushAllDone>,
    timer: Option<TimerKey>,
}

/// A send its caller waits for, or wants told of; a flushing one (`wait`)
/// also ends at its deadline.
struct PendingSend {
    wait: bool,
    settled: Option<SendSettled>,
}

/// The threaded client's state; see the module comment.
#[derive(Default)]
pub(crate) struct Threaded {
    pub(crate) on_message: Option<OnMessage>,
    pub(crate) search_policy: Option<SearchPolicy>,
    /// The client's inbox, `messages()`, once asked for.
    pub(crate) inbox: Option<Arc<Queue>>,
    /// The subscriptions, by type, None for every type; a dropped one goes.
    pub(crate) subscriptions: Vec<(Option<u32>, Weak<Queue>)>,
    connects: Vec<ConnectWaiter>,
    flush_waits: Vec<FlushWait>,
    next_wait: u64,
    sends: HashMap<u64, PendingSend>,
    next_send: u64,
    expiring: BinaryHeap<Reverse<(Instant, u64)>>,
    expiring_compact_at: usize,
    expiry_timer: Option<TimerKey>,
    expiry_armed: Option<Instant>,
    /// shutdown() was called: no new query, connect or routing; a send made
    /// on the io thread meanwhile goes out with the write-out.
    pub(crate) shut_down: bool,
    drain: Option<FlushId>,
    drain_timer: Option<TimerKey>,
    write_out_sent: u64,
    /// The shutdown's end came: the connections are closing.
    pub(crate) torn_down: bool,
}

impl Core {
    /// A connection came up or went: the connect() calls waiting for it
    /// end, and the queries hear of it.
    pub(crate) fn on_connection(&mut self, connection: &Wrapper, up: bool) {
        log::debug!("connection {}; queries watched: {}", if up { "up" } else { "down" }, self.queries.watched_ids());
        let ended: Vec<u64> = self
            .threaded
            .connects
            .iter()
            .filter(|waiter| waiter.connection.is_same_connection(connection))
            .map(|waiter| waiter.id)
            .collect();
        for id in ended {
            self.end_connect(id, up);
        }
        self.queries_on_connection(connection, up);
    }

    /// A message for the threaded client: a query's, or one for nobody
    /// here.
    pub(crate) fn dispatch_incoming(&mut self, incoming: Incoming) {
        if self.query_takes(&incoming) {
            return;
        }
        self.on_unmatched(incoming);
    }

    /// A message that is no reply to a query in flight: REQUEST_RECEIVED
    /// for a query no longer tracked is dropped, a PING, or a search the
    /// policy says yes to, is answered, the rest goes to on_message, or is
    /// logged and dropped without one.
    fn on_unmatched(&mut self, incoming: Incoming) {
        let message = &incoming.message;
        let (id, r#type, sender) = (message.id.unwrap_or(0), message.r#type, message.sender.unwrap_or(0));
        if r#type == types::REQUEST_RECEIVED {
            return;
        }
        if r#type == types::PING || r#type == types::BACKEND_FOR_PACKET_SEARCH {
            if message.references.unwrap_or(0) != 0 {
                return; // an answer to a ping nobody here is waiting for
            }
            if r#type == types::BACKEND_FOR_PACKET_SEARCH {
                let Some(policy) = self.threaded.search_policy.as_mut() else {
                    return; // a client without a search policy is no backend
                };
                let mut answer = false; // a policy that panics answers no
                guarded(|| format!("the search policy, for message #{id}"), || answer = policy());
                if !answer {
                    return; // a backend whose policy declines, saturated for instance
                }
            }
            self.answer_ping(&incoming);
            return;
        }
        // Every subscription of its type and the inbox get it, then
        // on_message; with none of them, it is logged and dropped.
        let public = crate::handles::Incoming::from(incoming);
        let mut delivered = false;
        self.threaded.subscriptions.retain(|(_, queue)| queue.strong_count() > 0);
        for (wanted, queue) in &self.threaded.subscriptions {
            if wanted.is_none_or(|wanted| wanted == r#type) {
                if let Some(queue) = queue.upgrade() {
                    queue.push(public.clone());
                    delivered = true;
                }
            }
        }
        if let Some(inbox) = &self.threaded.inbox {
            inbox.push(public.clone());
            delivered = true;
        }
        if let Some(on_message) = self.threaded.on_message.as_mut() {
            guarded(|| format!("on_message, for message #{id} of type {type} from {sender}"), || on_message(public));
            return;
        }
        if delivered {
            return;
        }
        let kind = Kind { reason: DropLine::NoOnMessage as u32, level: log::Level::Warn, r#type, peer: 0 };
        if self.drop_line_first(kind, || {
            format!("messages of type {type} dropped: this client has no on_message callback")
        }) {
            log::warn!("message #{id} of type {type} dropped: this client has no on_message callback");
        }
    }

    /// The echo of a PING, or the answer to a search, through the
    /// connection it came on: a PING carrying its payload back, or a
    /// BACKEND_ERROR saying the echo would be over MAX_MESSAGE_SIZE.
    fn answer_ping(&mut self, incoming: &Incoming) {
        let message = &incoming.message;
        let payload = message.message.clone().unwrap_or_default();
        let mut pong = MultiplexerMessage {
            id: Some(self.random64()),
            sender: Some(self.instance_id),
            to: message.sender,
            references: message.id,
            r#type: types::PING,
            message: Some(payload.clone()),
            ..Default::default()
        };
        if pong.encoded_len() > MAX_MESSAGE_SIZE {
            let what = if message.r#type == types::PING { "PING" } else { "search" };
            pong.r#type = types::BACKEND_ERROR;
            pong.message = Some(
                format!("the echo of a {what} of {} bytes would be over MAX_MESSAGE_SIZE", payload.len())
                    .into_bytes()
                    .into(),
            );
        }
        let Ok(bytes) = frame::encode(&pong) else { return };
        let frame = Frame { bytes, id: pong.id.unwrap_or(0), r#type: pong.r#type };
        let timeout = Some(DEFAULT_TIMEOUT);
        if self.schedule_on(frame.clone(), false, &incoming.connection, timeout, 0, None).is_none() {
            self.schedule_one(frame, false, None, timeout, 0, None);
        }
    }

    /// A connect() on the io thread: the connection started and its wait
    /// armed, `timeout` from now; `done` hears how it ended: true once the
    /// connection is registered, false as soon as it failed, or at the
    /// timeout.
    pub(crate) fn start_connect(&mut self, host: &str, port: u16, timeout: Option<Duration>, done: ConnectDone) {
        if self.threaded.shut_down {
            done(false);
            return;
        }
        let Ok(conn) = self.async_connect(host, port) else {
            done(false);
            return;
        };
        let Some(state) = self.conns.get(&conn) else {
            done(false); // failed at once
            return;
        };
        if state.shuts_down || state.registered {
            let registered = state.registered && !state.shuts_down;
            done(registered); // failed at once, or connected already
            return;
        }
        self.threaded.next_wait += 1;
        let id = self.threaded.next_wait;
        let timer = deadline_after(self.now, timeout).map(|deadline| self.timers.arm(deadline, Due::ConnectWait(id)));
        let connection = self.wrap(conn);
        self.threaded.connects.push(ConnectWaiter { id, connection, done: Some(done), timer });
    }

    /// A connect() call ends: connected, failed, or out of time.
    pub(crate) fn end_connect(&mut self, id: u64, up: bool) {
        let Some(position) = self.threaded.connects.iter().position(|waiter| waiter.id == id) else { return };
        let mut waiter = self.threaded.connects.remove(position);
        self.timers.cancel(&mut waiter.timer);
        if let Some(done) = waiter.done.take() {
            done(up);
        }
    }

    /// A connect() call's time ran out.
    pub(crate) fn connect_wait_expired(&mut self, id: u64) {
        if let Some(waiter) = self.threaded.connects.iter_mut().find(|waiter| waiter.id == id) {
            waiter.timer = None;
        }
        self.end_connect(id, false);
    }

    /// A flush_all() on the io thread: the outbox's flush, which counts what
    /// waits for room or for a connection, with a timer of its own; `done`
    /// hears how it ended, once. After the teardown, at once with true:
    /// nothing is left to write.
    pub(crate) fn begin_flush_all(&mut self, timeout: Option<Duration>, done: FlushAllDone) {
        if self.threaded.torn_down {
            guarded(|| "a flush_all() callback".to_string(), || done(true));
            return;
        }
        self.threaded.next_wait += 1;
        let id = self.threaded.next_wait;
        self.threaded.flush_waits.push(FlushWait { id, flush: None, done: Some(done), timer: None });
        let flush = self.begin_flush(Some(FlushDone::Wait(id)));
        let Some(wait) = self.threaded.flush_waits.iter_mut().find(|wait| wait.id == id) else {
            self.end_flush(flush);
            return; // nothing to wait for: ended already
        };
        wait.flush = Some(flush);
        if let Some(deadline) = deadline_after(self.now, timeout) {
            wait.timer = Some(self.timers.arm(deadline, Due::FlushWait(id)));
        }
    }

    /// A flush_all() call ends, flushed or out of time: the outbox forgets
    /// its flush, and the caller is told.
    pub(crate) fn end_flush_wait(&mut self, id: u64, flushed: bool) {
        let Some(position) = self.threaded.flush_waits.iter().position(|wait| wait.id == id) else { return };
        let mut wait = self.threaded.flush_waits.remove(position);
        if let Some(flush) = wait.flush {
            self.end_flush(flush);
        }
        self.timers.cancel(&mut wait.timer);
        if let Some(done) = wait.done.take() {
            guarded(|| "a flush_all() callback".to_string(), || done(flushed));
        }
    }

    /// A flush_all() call's time ran out.
    pub(crate) fn flush_wait_expired(&mut self, id: u64) {
        if let Some(wait) = self.threaded.flush_waits.iter_mut().find(|wait| wait.id == id) {
            wait.timer = None;
        }
        self.end_flush_wait(id, false);
    }

    /// A send on the io thread: numbered and handed to the outbox, which
    /// queues it, has it wait for room, or holds it until a connection
    /// comes up; followed to its end when `settled` wants it. A flushing send
    /// (`wait`) also ends at `deadline` with nothing written, its message
    /// waiting ROOM_GRACE_SECONDS longer, as a synchronous flushing send's
    /// does. A pinned lane whose connection is gone takes nothing: the
    /// message is dropped and reported. After the shutdown's end nothing is
    /// placed.
    pub(crate) fn place_send(
        &mut self,
        frame: Frame,
        all: bool,
        lane: Option<Lane>,
        deadline: Option<Instant>,
        wait: bool,
        settled: Option<SendSettled>,
    ) {
        let number = self.next_number();
        let report = OutFrame {
            bytes: frame.bytes.clone(),
            id: frame.id,
            r#type: frame.r#type,
            pinned: false,
            for_all: all,
            own: false,
            number,
            deadline: None,
        };
        if self.threaded.torn_down {
            self.report_drop(&report, DropReason::ShutDown);
            if let Some(settled) = settled {
                guarded(|| "a send's callback".to_string(), || settled(0, true));
            }
            return;
        }
        let mut follow = None;
        let mut send_id = None;
        if let Some(settled) = settled {
            self.threaded.next_send += 1;
            let id = self.threaded.next_send;
            self.threaded.sends.insert(id, PendingSend { wait, settled: Some(settled) });
            if wait {
                self.expire_send_at(id, deadline);
            }
            follow = Some(SendDone::Settle(id));
            send_id = Some(id);
        }
        let left = deadline.map(|deadline| deadline.saturating_duration_since(self.now));
        let timeout = if wait { left.map(|left| left + ROOM_GRACE_SECONDS) } else { left };
        if !self.send(frame, all, lane.as_ref(), timeout, number, None, None, follow) {
            self.report_drop(&report, DropReason::ConnectionLost);
            if let Some(id) = send_id {
                self.settle_send(id, 0, true);
            }
        }
    }

    /// A send's end, once: its caller hears the copies written, and whether
    /// nothing was written for want of a connection.
    pub(crate) fn settle_send(&mut self, id: u64, written: u32, not_connected: bool) {
        let Some(mut pending) = self.threaded.sends.remove(&id) else { return };
        if let Some(settled) = pending.settled.take() {
            guarded(|| "a send's callback".to_string(), || settled(written, not_connected));
        }
    }

    /// A flushing send's deadline into the heap, the timer moved to it when
    /// it is the earliest; entries of sends that ended are cleared out when
    /// the heap has doubled since the last time.
    fn expire_send_at(&mut self, id: u64, deadline: Option<Instant>) {
        let Some(deadline) = deadline else { return };
        let threaded = &mut self.threaded;
        if threaded.expiring.len() >= threaded.expiring_compact_at.max(64) {
            let sends = &threaded.sends;
            let entries = std::mem::take(&mut threaded.expiring).into_vec();
            threaded.expiring = entries.into_iter().filter(|Reverse((_, each))| sends.contains_key(each)).collect();
            threaded.expiring_compact_at = (2 * threaded.expiring.len()).max(64);
        }
        threaded.expiring.push(Reverse((deadline, id)));
        if threaded.expiry_armed.is_none_or(|armed| deadline < armed) {
            self.arm_send_expiry();
        }
    }

    /// The timer set to the earliest deadline of a flushing send not
    /// settled, or stopped when none is left.
    fn arm_send_expiry(&mut self) {
        while let Some(Reverse((_, id))) = self.threaded.expiring.peek().copied() {
            if self.threaded.sends.get(&id).is_some_and(|send| send.wait) {
                break;
            }
            self.threaded.expiring.pop();
        }
        let Some(Reverse((next, _))) = self.threaded.expiring.peek().copied() else {
            self.timers.cancel(&mut self.threaded.expiry_timer);
            self.threaded.expiry_armed = None;
            return;
        };
        if self.threaded.expiry_armed == Some(next) {
            return;
        }
        self.threaded.expiry_armed = Some(next);
        self.timers.rearm(&mut self.threaded.expiry_timer, next, Due::SendsExpire);
    }

    /// The deadline timer: a flushing send that wrote nothing by then ends
    /// with 0, told whether a connection was live, which the synchronous
    /// client asks at its deadline too: NotConnected or OperationTimedOut.
    /// What it sent waits on in the outbox within its own timeout.
    pub(crate) fn sends_expire(&mut self) {
        self.threaded.expiry_timer = None;
        self.threaded.expiry_armed = None;
        let mut none_live = None;
        while let Some(Reverse((deadline, id))) = self.threaded.expiring.peek().copied() {
            if deadline > self.now {
                break;
            }
            self.threaded.expiring.pop();
            if !self.threaded.sends.contains_key(&id) {
                continue;
            }
            let none = *none_live.get_or_insert_with(|| !self.any_live_connection());
            self.settle_send(id, 0, none); // out of time
        }
        self.arm_send_expiry();
    }

    /// Whether some registered connection is live.
    pub(crate) fn any_live_connection(&self) -> bool {
        self.by_peer_id.values().any(|conn| self.conn_living(*conn))
    }

    /// shutdown() on the io thread: every query ends with SHUT_DOWN and the
    /// connect() calls waiting end; what was sent before is written,
    /// `timeout` at most, a zero one dropping it at once; then the
    /// connections close.
    pub(crate) fn begin_shutdown(&mut self, timeout: Option<Duration>) {
        self.threaded.shut_down = true;
        log::debug!("shutting down: {} queries watched", self.queries.watched_ids());
        self.finish_every_query();
        let waiting: Vec<u64> = self.threaded.connects.iter().map(|waiter| waiter.id).collect();
        for id in waiting {
            self.end_connect(id, false);
        }
        if timeout == Some(Duration::ZERO) {
            self.teardown();
            return;
        }
        self.write_out();
        if let Some(deadline) = deadline_after(self.now, timeout) {
            self.timers.rearm(&mut self.threaded.drain_timer, deadline, Due::Drain);
        }
    }

    /// The shutdown's write-out: what was sent before is written, then what
    /// the io thread sent meanwhile, a flush again while anything newer was
    /// sent, until the drain's time ends it; then the teardown. A flush ends
    /// from wherever its last frame was written, so what follows is posted.
    fn write_out(&mut self) {
        self.threaded.write_out_sent = self.outbox.last_number();
        if let Some(previous) = self.threaded.drain.take() {
            self.end_flush(previous);
        }
        let drain = self.begin_flush(Some(FlushDone::Drain));
        self.threaded.drain = Some(drain);
    }

    /// A write-out's flush ended: again while anything newer was sent, else
    /// the teardown.
    pub(crate) fn write_out_step(&mut self) {
        if self.threaded.torn_down {
            return;
        }
        if self.outbox.last_number() != self.threaded.write_out_sent {
            self.write_out();
        } else {
            self.teardown();
        }
    }

    /// The drain's time ran out.
    pub(crate) fn drain_expired(&mut self) {
        self.threaded.drain_timer = None;
        self.teardown();
    }

    /// The end of a shutdown, once: the flush_all() calls still waiting
    /// end, the connections close, and the io thread ends once nothing is
    /// left to do.
    pub(crate) fn teardown(&mut self) {
        if self.threaded.torn_down {
            return;
        }
        self.threaded.torn_down = true;
        self.timers.cancel(&mut self.threaded.drain_timer);
        if let Some(drain) = self.threaded.drain.take() {
            self.end_flush(drain);
        }
        self.threaded.expiring.clear();
        self.timers.cancel(&mut self.threaded.expiry_timer);
        self.threaded.expiry_armed = None;
        let waits: Vec<u64> = self.threaded.flush_waits.iter().map(|wait| wait.id).collect();
        for id in waits {
            self.end_flush_wait(id, false);
        }
        self.shutdown_client();
        // The streams' readers get what they hold, then the end.
        if let Some(inbox) = &self.threaded.inbox {
            inbox.close();
        }
        for (_, queue) in std::mem::take(&mut self.threaded.subscriptions) {
            if let Some(queue) = queue.upgrade() {
                queue.close();
            }
        }
        log::debug!("shut down; the io thread ends when its handlers are done");
    }
}
