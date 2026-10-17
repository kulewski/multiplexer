//! One connection to a multiplexer, its protocol half: the welcome
//! handshake, the outgoing queue with one write in flight, the heartbeats
//! and the silence that ends a connection, and the three ways it ends.
//! Mirrors multiplexer/io/connection.h as a client's connection uses it;
//! the socket is the driver's, which reads frames and writes them when
//! told (`Action`). A client's connections never have a passive peer, so
//! the passive-peer exemption, which only the multiplexer applies, is not
//! here.
//!
//! The lifecycle: `start` once the TCP connect is done, sending the welcome
//! and arming the heartbeats; registered at the multiplexer's welcome; then
//! `shutdown` for any reason, which tells the client first, with the
//! connection already not living, so that what its callbacks send goes
//! elsewhere, then hands back what it had not written, the frame a write
//! still holds set aside for the write's end to settle. A write that failed
//! reads on to the multiplexer's end, HEARTBIT_INTERVAL at most, delivering
//! what comes; `close_gracefully` does the same CLOSE_READ_SECONDS at most,
//! dropping and counting it. Malformed input closes the connection.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use super::timers::TimerKey;
use super::{Action, ConnId, Core, Due, LivingFlag, OutFrame, QueueEntry, SendState, Target, Tracker};
use crate::constants::types;
use crate::defaults::{
    FORCED_FRAMES_PAST_FULL_QUEUE, HEARTBIT_INTERVAL, NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL,
    NO_HEARTBIT_SO_REALLY_DROP_INTERVAL,
};
use crate::frame;
use crate::proto::multiplexer::{MultiplexerMessage, WelcomeMessage};

/// The outgoing queue's limit once the multiplexer's welcome registered
/// the connection; one frame, the client's own welcome, before
/// (ConnectionsManager::DEFAULT_OUT_QUEUE_SIZE: a client's built-in rules
/// name no queue size).
pub(crate) const DEFAULT_OUT_QUEUE_SIZE: usize = 1024;

/// How a connection's reading ended, as the driver saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReadEnd {
    /// The multiplexer's end of the stream.
    Eof,
    /// The socket failed.
    Error(String),
    /// A header of length 0 or over MAX_MESSAGE_SIZE.
    BadHeader,
    /// A body whose checksum is not its header's.
    BadChecksum,
}

/// The state of the outgoing direction (C++ ChannelState).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Channel {
    /// Nothing being written.
    Free,
    /// One write in flight, for the queue's front.
    Busy,
    /// Shut down: nothing is written any more.
    Broken,
}

/// One connection; see the module comment. The fields up to `owner_data`
/// are what C++ keeps in the connection for its manager.
#[derive(Debug)]
pub(crate) struct Conn {
    /// What it was asked to connect to, kept after it is gone; shared
    /// with every handle of the connection, which copies no string.
    pub(crate) target: Arc<Target>,
    /// The address the target resolved to that is in use, or was tried last.
    pub(crate) expected_endpoint: Option<SocketAddr>,
    /// The addresses the target resolved to this time, tried in turn.
    pub(crate) candidates: Vec<SocketAddr>,
    pub(crate) next_candidate: usize,
    /// The TCP connect is under way, and its CONNECT_ATTEMPT_SECONDS.
    pub(crate) connecting: bool,
    pub(crate) connect_timed_out: bool,
    pub(crate) connect_deadline: Option<TimerKey>,
    /// A socket is open for it.
    pub(crate) socket_open: bool,
    /// The client's routing as versions: the one this connection's welcome
    /// carried, and the last one the multiplexer confirmed, by its welcome
    /// or by the PEER_STATUS answering the last PEER_CONTROL sent here,
    /// whose id this keeps.
    pub(crate) routing_in_welcome: u32,
    pub(crate) routing_acknowledged: u32,
    pub(crate) routing_request_id: u64,

    pub(crate) peer_type: u32,
    pub(crate) peer_id: u64,
    living: bool,
    pub(crate) living_flag: LivingFlag,
    pub(crate) shuts_down: bool,
    pub(crate) registered: bool,
    told_gone: bool,
    delivering: bool,
    pub(crate) dropped_while_closing: u64,
    reading: bool,
    send_heartbit_timer: Option<TimerKey>,
    /// When the outgoing queue last emptied: the heartbeat's timer, armed
    /// for an earlier moment, moves on to it when it comes due.
    quiet_since: Option<Instant>,
    require_heartbit_timer: Option<TimerKey>,
    /// When the last frame was read: the silence's first wait, armed for an
    /// earlier one, moves on to it when it comes due.
    heard_at: Option<Instant>,
    /// Whether the silence's wait armed is the second, the drop itself.
    silence_second_wait: bool,
    outgoing: VecDeque<QueueEntry>,
    outgoing_max: usize,
    out: Channel,
    write_in_flight: bool,
    set_aside: VecDeque<QueueEntry>,
    heartbit: Arc<OutFrame>,
}

impl Conn {
    /// A connection for `target`, not started; `heartbit` is its heartbeat
    /// frame, the same every time, written once per connection.
    pub(crate) fn new(target: Target, heartbit: Arc<OutFrame>) -> Conn {
        Conn {
            target: Arc::new(target),
            expected_endpoint: None,
            candidates: Vec::new(),
            next_candidate: 0,
            connecting: false,
            connect_timed_out: false,
            connect_deadline: None,
            socket_open: false,
            routing_in_welcome: 0,
            routing_acknowledged: 0,
            routing_request_id: 0,
            peer_type: 0,
            peer_id: 0,
            living: false,
            living_flag: Arc::new(AtomicBool::new(false)),
            shuts_down: false,
            registered: false,
            told_gone: false,
            delivering: true,
            dropped_while_closing: 0,
            reading: false,
            send_heartbit_timer: None,
            quiet_since: None,
            require_heartbit_timer: None,
            heard_at: None,
            silence_second_wait: false,
            outgoing: VecDeque::new(),
            outgoing_max: 1,
            out: Channel::Free,
            write_in_flight: false,
            set_aside: VecDeque::new(),
            heartbit,
        }
    }

    /// Whether it is live: started, and not shut down or closing.
    pub(crate) fn living(&self) -> bool {
        self.living
    }

    /// Whether its outgoing queue is at its limit.
    pub(crate) fn outgoing_queue_full(&self) -> bool {
        self.outgoing_max <= self.outgoing.len()
    }

    /// The entry queued last but the connection's own frames, which
    /// everything queued before it is written ahead of; None when the queue
    /// holds none other. An own frame, a PEER_CONTROL or a heartbeat, goes
    /// with the connection when it dies, told to nobody: one a flush
    /// followed would be waited for for good, where the frames ahead of it
    /// go to another connection, and the one followed with them.
    pub(crate) fn last_queued(&mut self) -> Option<&mut QueueEntry> {
        self.outgoing.iter_mut().rev().find(|entry| !entry.1.own)
    }

    /// Sets the queue's limit, at the registration.
    pub(crate) fn set_outgoing_queue_max_size(&mut self, size: usize) {
        self.outgoing_max = size;
    }

    /// Whether nothing of it is left to happen: shut down, with no write's
    /// end to come. The core forgets it then.
    pub(crate) fn finished(&self) -> bool {
        self.shuts_down && !self.write_in_flight
    }

    /// Live or not, for this thread and for the others.
    fn set_living(&mut self, living: bool) {
        self.living = living;
        self.living_flag.store(living, Ordering::Release);
    }
}

/// The heartbeat frame of a client with `instance_id`: id 0, which marks it
/// as no real message, and the connection's own.
pub(crate) fn heartbit_frame(instance_id: u64) -> Arc<OutFrame> {
    let message =
        MultiplexerMessage { id: Some(0), r#type: types::HEARTBIT, sender: Some(instance_id), ..Default::default() };
    let bytes = frame::encode(&message).unwrap_or_default();
    Arc::new(OutFrame {
        bytes,
        id: 0,
        r#type: types::HEARTBIT,
        pinned: false,
        for_all: false,
        own: true,
        number: 0,
        deadline: None,
    })
}

/// Whether `message` is one of the protocol's own answers to what this
/// client sent: a delivery error, a backend's acknowledgement or error
/// report, a control frame's status, or a PING answering a PING or a
/// search. Only the client waited for them, so they are no loss when they
/// arrive while it closes.
fn answers_this_peer(message: &MultiplexerMessage) -> bool {
    match message.r#type {
        types::DELIVERY_ERROR
        | types::REQUEST_RECEIVED
        | types::BACKEND_ERROR
        | types::RECORDING_STATUS
        | types::RULES_STATUS
        | types::PEER_STATUS => true,
        types::PING => message.references.unwrap_or(0) != 0,
        _ => false,
    }
}

impl Core {
    /// The connection numbered `conn`, if it still exists.
    pub(crate) fn conn(&self, conn: ConnId) -> Option<&Conn> {
        self.conns.get(&conn)
    }

    /// Whether `conn` exists and is live.
    pub(crate) fn conn_living(&self, conn: ConnId) -> bool {
        self.conns.get(&conn).is_some_and(Conn::living)
    }

    /// Forgets `conn` when nothing of it is left to happen.
    fn forget_if_finished(&mut self, conn: ConnId) {
        if self.conns.get(&conn).is_some_and(Conn::finished) {
            self.conns.remove(&conn);
        }
    }

    /// The TCP connect is done: the connection reads, sends its welcome and
    /// arms the heartbeats (C++ start()). A welcome that cannot be queued
    /// shuts the connection down.
    pub(crate) fn start_connection(&mut self, conn: ConnId) {
        let welcome = self.welcome_frame();
        let Some(state) = self.conns.get_mut(&conn) else { return };
        debug_assert!(!state.living && !state.shuts_down);
        state.set_living(true);
        state.reading = true;
        self.actions.push(Action::StartReading(conn));
        if self.schedule(conn, welcome, true, true).is_some() {
            self.send_heartbit_later(conn);
            self.require_heartbit_later(conn);
        } else {
            self.shutdown_connection(conn);
        }
    }

    /// Queues a frame for writing and starts the write if none is in
    /// flight; the frame's tracker, or None when it was dropped: the
    /// connection is not live, or its queue is full. `force` lets protocol
    /// frames past a full queue, FORCED_FRAMES_PAST_FULL_QUEUE at most, and
    /// `asap` puts them right behind the frame being written.
    pub(crate) fn schedule(
        &mut self,
        conn: ConnId,
        frame: Arc<OutFrame>,
        force: bool,
        asap: bool,
    ) -> Option<Arc<Tracker>> {
        let state = self.conns.get_mut(&conn)?;
        if !state.living {
            return None;
        }
        if state.outgoing_queue_full()
            && (!force || state.outgoing.len() >= state.outgoing_max + FORCED_FRAMES_PAST_FULL_QUEUE)
        {
            log::trace!("outgoing queue full, dropping message");
            return None;
        }
        let tracker = Tracker::new(SendState::Queued);
        let entry = (Arc::downgrade(&tracker), frame);
        if asap && state.out == Channel::Busy {
            state.outgoing.insert(1, entry); // right behind the frame being written
        } else if asap {
            state.outgoing.push_front(entry);
        } else {
            state.outgoing.push_back(entry);
        }
        self.process_send_queue(conn);
        Some(tracker)
    }

    /// Queues an entry the client built itself: a message that waited for
    /// room, or one another connection handed over. False when the
    /// connection is not live or its queue is full.
    pub(crate) fn take_over(&mut self, conn: ConnId, entry: QueueEntry) -> bool {
        let Some(state) = self.conns.get_mut(&conn) else { return false };
        if !state.living || state.outgoing_queue_full() {
            return false;
        }
        state.outgoing.push_back(entry);
        self.process_send_queue(conn);
        true
    }

    /// Starts writing the queue's front when nothing is being written.
    fn process_send_queue(&mut self, conn: ConnId) {
        let Some(state) = self.conns.get_mut(&conn) else { return };
        if state.out != Channel::Free || state.shuts_down {
            return;
        }
        let Some(front) = state.outgoing.front() else { return };
        state.out = Channel::Busy;
        state.write_in_flight = true;
        let bytes = front.1.bytes.clone();
        self.actions.push(Action::Write(conn, bytes));
    }

    /// The driver's word on the write in flight: `written` bytes of it went
    /// to the socket, all of the frame on success, and `failed` says
    /// whether the write ended in an error.
    pub(crate) fn on_write_done(&mut self, conn: ConnId, written: usize, failed: bool) {
        let Some(state) = self.conns.get_mut(&conn) else { return };
        state.write_in_flight = false;
        if failed {
            log::debug!("write error on the connection to {:?} after {written} bytes", state.target);
        }
        if state.out == Channel::Broken {
            self.settle_set_aside(conn, written, failed);
            self.forget_if_finished(conn);
            return;
        }
        let whole = state.outgoing.front().is_some_and(|front| front.1.bytes.len() == written);
        if failed || written == 0 || !whole {
            self.write_failed(conn);
            return;
        }
        state.out = Channel::Free;
        let was_full = state.outgoing_queue_full();
        let Some((tracker, _)) = state.outgoing.pop_front() else { return };
        if let Some(tracker) = tracker.upgrade() {
            tracker.set(SendState::Sent);
            self.tracked_message_done(&tracker, true);
        }
        self.send_heartbit_later(conn);
        self.process_send_queue(conn);
        let has_room = self.conns.get(&conn).is_some_and(|state| !state.outgoing_queue_full());
        if was_full && has_room {
            self.outgoing_queue_has_room(conn);
        }
    }

    /// Arms the heartbeat HEARTBIT_INTERVAL from now when nothing is
    /// queued; with something queued, the write in progress arms it once
    /// the queue is empty. Not on a connection that is no longer living,
    /// whose reading to the end bounds itself with the same timer. A timer
    /// already armed stays, this moment kept for it to move on to when it
    /// comes due (send_heartbit_now): a busy connection costs no timer work
    /// per message, where re-arming would, as asio's expires_after does.
    fn send_heartbit_later(&mut self, conn: ConnId) {
        let now = self.now;
        let Some(state) = self.conns.get_mut(&conn) else { return };
        if !state.living || !state.outgoing.is_empty() {
            return;
        }
        state.quiet_since = Some(now);
        if state.send_heartbit_timer.is_none() {
            state.send_heartbit_timer = Some(self.timers.arm(now + HEARTBIT_INTERVAL, Due::SendHeartbit(conn)));
        }
    }

    /// The heartbeat's timer came due. HEARTBIT_INTERVAL of write silence:
    /// a heartbeat goes out, forced and right behind the frame being
    /// written. A shorter silence, the queue having emptied since the timer
    /// was armed, arms it again for the interval after that.
    pub(crate) fn send_heartbit_now(&mut self, conn: ConnId) {
        let now = self.now;
        let Some(state) = self.conns.get_mut(&conn) else { return };
        state.send_heartbit_timer = None;
        if state.shuts_down || !state.living {
            return;
        }
        let moved_on = state.quiet_since.map(|since| since + HEARTBIT_INTERVAL).filter(|deadline| *deadline > now);
        if let Some(deadline) = moved_on {
            state.send_heartbit_timer = Some(self.timers.arm(deadline, Due::SendHeartbit(conn)));
            return;
        }
        let heartbit = Arc::clone(&state.heartbit);
        if self.schedule(conn, heartbit, true, true).is_none() {
            log::error!("failed to schedule Heartbit message, we may lose the connection");
        }
    }

    /// Arms the drop of a silent connection: NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL
    /// from now, moved on by every frame read. A first wait already armed
    /// stays, this moment kept for it to move on to when it comes due
    /// (require_heartbit_soon), so that a busy connection costs no timer
    /// work per message; a second wait gives way to a first.
    fn require_heartbit_later(&mut self, conn: ConnId) {
        let now = self.now;
        let Some(state) = self.conns.get_mut(&conn) else { return };
        state.heard_at = Some(now);
        if state.require_heartbit_timer.is_some() && !state.silence_second_wait {
            return;
        }
        state.silence_second_wait = false;
        let deadline = now + NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL;
        self.timers.rearm(&mut state.require_heartbit_timer, deadline, Due::RequireHeartbitSoon(conn));
    }

    /// The first wait came due. A frame read since it was armed moves it on
    /// to the prepare interval after that frame. Otherwise the first phase
    /// of the drop: nothing arrived for the prepare interval; one more wait
    /// before really closing, so that a short stall does not cost the
    /// connection. The second wait runs from now, when this one is handled:
    /// a synchronous client, which runs the loop only inside its calls,
    /// handles it at its next call after a long idle, so that idling alone
    /// never costs it the connection, as in C++. C++ checks here that no
    /// frame re-armed the wait in the same pass of the loop
    /// (Connection::_drop_wait_due); here the driver hands the core a
    /// socket's waiting frame before the timers that came due in the same
    /// pass, and that frame moves the wait on.
    pub(crate) fn require_heartbit_soon(&mut self, conn: ConnId) {
        let now = self.now;
        let Some(state) = self.conns.get_mut(&conn) else { return };
        state.require_heartbit_timer = None;
        if state.shuts_down {
            return;
        }
        let moved_on =
            state.heard_at.map(|heard| heard + NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL).filter(|deadline| *deadline > now);
        if let Some(deadline) = moved_on {
            state.require_heartbit_timer = Some(self.timers.arm(deadline, Due::RequireHeartbitSoon(conn)));
            return;
        }
        state.silence_second_wait = true;
        let deadline = now + NO_HEARTBIT_SO_REALLY_DROP_INTERVAL;
        self.timers.rearm(&mut state.require_heartbit_timer, deadline, Due::RequireHeartbitNow(conn));
    }

    /// The second phase: the connection goes.
    pub(crate) fn require_heartbit_now(&mut self, conn: ConnId) {
        let Some(state) = self.conns.get_mut(&conn) else { return };
        state.require_heartbit_timer = None;
        if state.shuts_down {
            return;
        }
        log::warn!(
            "nothing from the multiplexer at {:?} for {} s; closing the connection",
            state.target,
            (NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL + NO_HEARTBIT_SO_REALLY_DROP_INTERVAL).as_secs()
        );
        self.shutdown_connection(conn);
    }

    /// A frame's header arrived ahead of a body that takes a while: the
    /// silence ends here, as C++ re-arms at the header.
    pub(crate) fn on_frame_started(&mut self, conn: ConnId) {
        if self.conns.get(&conn).is_some_and(|state| state.reading && !state.shuts_down) {
            self.require_heartbit_later(conn);
        }
    }

    /// A frame read and verified: parsed, the welcome and heartbeats
    /// handled here, the rest handed to the client once registered; a body
    /// that is not a message, a message before the welcome, a second
    /// welcome or a heartbeat before the welcome closes the connection.
    pub(crate) fn on_frame(&mut self, conn: ConnId, body: Bytes) {
        let Some(state) = self.conns.get(&conn) else { return };
        if !state.reading || state.shuts_down {
            return;
        }
        let delivering = state.delivering;
        self.require_heartbit_later(conn);
        if !delivering {
            // Leaving: read only so that nothing is left unread when the
            // socket closes; what was a message, rather than a heartbeat or
            // an answer only this client waited for, is counted.
            if let Ok(dropped) = frame::decode(body) {
                if dropped.r#type != types::HEARTBIT && !answers_this_peer(&dropped) {
                    if let Some(state) = self.conns.get_mut(&conn) {
                        state.dropped_while_closing += 1;
                    }
                }
            }
            return;
        }
        let message = match frame::decode(body) {
            Ok(message) => message,
            Err(error) => {
                let target = self.conns.get(&conn).map(|state| &state.target);
                log::debug!("received an invalid message from {target:?}: {error}");
                self.shutdown_connection(conn);
                return;
            }
        };
        if self.receive_internal_message(conn, &message) {
            return;
        }
        if !self.conns.get(&conn).is_some_and(|state| state.registered) {
            log::warn!("message of type {} before CONNECTION_WELCOME; dropping the connection", message.r#type);
            self.shutdown_connection(conn);
            return;
        }
        self.handle_message(conn, message);
    }

    /// The two message types a connection handles itself: the welcome,
    /// which registers the multiplexer, exactly once and first, and
    /// heartbeats, which have no content.
    fn receive_internal_message(&mut self, conn: ConnId, message: &MultiplexerMessage) -> bool {
        let registered = self.conns.get(&conn).is_some_and(|state| state.registered);
        match message.r#type {
            types::CONNECTION_WELCOME => {
                if registered {
                    log::warn!("received a repeated CONNECTION_WELCOME; dropping the connection");
                    self.shutdown_connection(conn);
                    return true;
                }
                use prost::Message as _;
                let welcome = match message.message.as_ref().map(|payload| WelcomeMessage::decode(payload.clone())) {
                    Some(Ok(welcome)) => welcome,
                    _ => {
                        log::warn!("received an invalid CONNECTION_WELCOME; dropping the connection");
                        self.shutdown_connection(conn);
                        return true;
                    }
                };
                if let Some(state) = self.conns.get_mut(&conn) {
                    state.peer_id = welcome.id;
                    state.peer_type = welcome.r#type;
                }
                self.register_connection(conn, &welcome);
                if self.conns.get(&conn).is_none_or(|state| state.shuts_down) {
                    return true; // refused
                }
                if let Some(state) = self.conns.get_mut(&conn) {
                    state.registered = true;
                }
                self.after_connection_registration(conn);
                true
            }
            types::HEARTBIT => {
                if !registered {
                    // The welcome must come first; a multiplexer sends its
                    // heartbeats only after it.
                    self.shutdown_connection(conn);
                }
                true
            }
            _ => false,
        }
    }

    /// The driver's word that the connection's reading ended: the
    /// multiplexer's end, an error, or malformed input; the connection shuts
    /// down.
    pub(crate) fn on_read_end(&mut self, conn: ConnId, end: ReadEnd) {
        let Some(state) = self.conns.get_mut(&conn) else { return };
        if !state.reading || state.shuts_down {
            return;
        }
        match &end {
            ReadEnd::Eof => log::debug!("the multiplexer at {:?} shut its end down", state.target),
            ReadEnd::Error(why) => log::error!("read error on the connection to {:?}: {why}", state.target),
            ReadEnd::BadHeader => {
                log::warn!("invalid frame header from the multiplexer at {:?}; dropping the connection", state.target)
            }
            ReadEnd::BadChecksum => log::error!("incoming message verification failed from {:?}", state.target),
        }
        self.shutdown_connection(conn);
    }

    /// Stops all I/O, tells the client, and hands back or drops the
    /// outgoing queue; safe at any point and idempotent. The connection is
    /// marked dead before the client hears of it, so that what the client's
    /// callbacks send goes elsewhere. The client hears last, through
    /// `connection_closed`, that the connection has ended for good.
    pub(crate) fn shutdown_connection(&mut self, conn: ConnId) {
        let Some(state) = self.conns.get_mut(&conn) else { return };
        if state.shuts_down {
            return;
        }
        state.shuts_down = true;
        state.set_living(false);
        self.tell_client_gone(conn);
        let Some(state) = self.conns.get_mut(&conn) else { return };
        state.registered = false;
        state.outgoing_max = 0;
        state.reading = false;
        state.out = Channel::Broken;
        self.timers.cancel(&mut state.send_heartbit_timer);
        self.timers.cancel(&mut state.require_heartbit_timer);
        self.timers.cancel(&mut state.connect_deadline);
        if state.socket_open {
            self.actions.push(Action::Shutdown(conn));
        }
        self.orphan_outgoing_messages(conn);
        self.connection_closed(conn);
        self.forget_if_finished(conn);
    }

    /// Tells the client, once, that the connection is gone: unregistered if
    /// it was registered, then destroyed.
    fn tell_client_gone(&mut self, conn: ConnId) {
        let Some(state) = self.conns.get_mut(&conn) else { return };
        if state.told_gone {
            return;
        }
        state.told_gone = true;
        if state.registered {
            self.unregister_connection(conn);
        }
        self.connection_destroyed(conn);
    }

    /// A write failed: what the multiplexer sent before may still wait in
    /// the socket, so the connection reads on to its end, HEARTBIT_INTERVAL
    /// at most, delivering it. One whose handshake is not done has nothing
    /// to read on for and shuts down.
    fn write_failed(&mut self, conn: ConnId) {
        let Some(state) = self.conns.get(&conn) else { return };
        if state.shuts_down || state.told_gone {
            return;
        }
        if !state.registered {
            self.shutdown_connection(conn);
            return;
        }
        self.read_to_the_end(conn, HEARTBIT_INTERVAL, true);
    }

    /// Stops writing and reads on until the multiplexer's end, `bound` at
    /// most, then shuts down; the client is told the connection is gone and
    /// its queue handed back now. What is read meanwhile goes to the client
    /// when `deliver`, or is dropped.
    fn read_to_the_end(&mut self, conn: ConnId, bound: Duration, deliver: bool) {
        let deadline = self.now + bound;
        let Some(state) = self.conns.get_mut(&conn) else { return };
        state.set_living(false);
        state.out = Channel::Broken;
        state.delivering = deliver;
        self.tell_client_gone(conn);
        self.orphan_outgoing_messages(conn);
        let Some(state) = self.conns.get_mut(&conn) else { return };
        self.actions.push(Action::ShutdownWrite(conn));
        self.timers.cancel(&mut state.require_heartbit_timer);
        self.timers.rearm(&mut state.send_heartbit_timer, deadline, Due::ReadToTheEnd(conn));
    }

    /// The bound of reading to the end ran out: the multiplexer's end never
    /// came.
    pub(crate) fn read_to_the_end_bound(&mut self, conn: ConnId) {
        let Some(state) = self.conns.get_mut(&conn) else { return };
        state.send_heartbit_timer = None;
        if !state.shuts_down {
            self.shutdown_connection(conn);
        }
    }

    /// Ends the connection the polite way, for a client that is leaving:
    /// the client told at once and the queue handed back, nothing more
    /// written, what arrives read and dropped until the multiplexer's end,
    /// `bound` at most. True when it reads on; false when it shut down at
    /// once, before its handshake was done, or was already closing.
    pub(crate) fn close_gracefully(&mut self, conn: ConnId, bound: Duration) -> bool {
        let Some(state) = self.conns.get(&conn) else { return false };
        if state.shuts_down || state.told_gone {
            return false;
        }
        if !state.registered {
            self.shutdown_connection(conn);
            return false;
        }
        self.read_to_the_end(conn, bound, false);
        true
    }

    /// Hands the unsent queue to the client, the frame a write still holds
    /// set aside for that write's end to settle: its bytes may be in the
    /// kernel already, so it is neither reported lost nor handed to another
    /// connection now.
    fn orphan_outgoing_messages(&mut self, conn: ConnId) {
        let Some(state) = self.conns.get_mut(&conn) else { return };
        if state.write_in_flight {
            if let Some(front) = state.outgoing.pop_front() {
                state.set_aside.push_back(front);
            }
        }
        let entries = std::mem::take(&mut state.outgoing);
        self.give_up(conn, entries);
    }

    /// The write of the frame set aside ended, the connection closed
    /// meanwhile: written whole, it reads written; otherwise it goes the
    /// way the rest of the queue went.
    fn settle_set_aside(&mut self, conn: ConnId, written: usize, failed: bool) {
        let Some(state) = self.conns.get_mut(&conn) else { return };
        let Some(entry) = state.set_aside.pop_front() else { return };
        if !failed && written == entry.1.bytes.len() {
            if let Some(tracker) = entry.0.upgrade() {
                tracker.set(SendState::Sent);
                self.tracked_message_done(&tracker, true);
            }
            return;
        }
        self.give_up(conn, VecDeque::from([entry]));
    }

    /// Offers `entries` to the client, which takes what it hands to another
    /// connection, and reports what it left as lost. The connection's own
    /// frames go first, silently.
    fn give_up(&mut self, conn: ConnId, mut entries: VecDeque<QueueEntry>) {
        entries.retain(|entry| !entry.1.own);
        self.handle_orphaned_outgoing_messages(conn, &mut entries);
        for (tracker, _) in entries {
            if let Some(tracker) = tracker.upgrade() {
                tracker.set(SendState::Lost);
                self.tracked_message_done(&tracker, false);
            }
        }
    }
}
