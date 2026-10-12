//! A client's side of the protocol over its connections: the duplicate
//! filter and the incoming queue, the welcome every connection sends, a
//! leaving server's refusals, the shutdown, and the one place every drop is
//! reported. Mirrors multiplexer/basic_client.cc; its parts mirror the C++
//! client's files too: `connect` basic_client_connect.cc's, the targets it
//! was given and their connections, connecting to every address a name
//! resolves to, reconnecting AUTO_RECONNECT_TIME after a connection is
//! lost, dropping a target and the registration at the multiplexer's
//! welcome, the client's half of connections_manager.h; `routing`
//! basic_client_routing.cc's, the routing and key filters with their
//! confirmations and acknowledgement.

mod connect;
mod routing;

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use prost::Message as _;

pub(crate) use connect::target_of;
pub(crate) use routing::{check_key_filters, Acknowledged, KeyFilters};

use super::connection::heartbit_frame;
use super::timers::TimerKey;
use super::{Action, ConnId, Core, Due, LivingFlag, OutFrame, Target};
use crate::constants::types;
use crate::defaults::DEFAULT_INCOMING_QUEUE_MAX_SIZE;
use crate::fork::DescriptorTable;
use crate::frame;
use crate::log_summary::{Kind, LogSummary};
use crate::proto::multiplexer::{KeyFilter, MultiplexerMessage, Routing, WelcomeMessage};
use crate::spanset::SpanSet;

/// Why a client gave up on a message the program sent, as the drop
/// observer and docs/semantics.md name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DropReason {
    /// It waited for room on a full connection past its timeout, or had
    /// none to wait.
    NoRoom,
    /// It waited for a connection to come up past its timeout, or had none
    /// to wait.
    NoConnection,
    /// Its connection ended and nothing else could take it: a pinned
    /// lane's, or a copy for ALL.
    ConnectionLost,
    /// The client shut down before it went.
    ShutDown,
}

/// The kinds of drop lines the library tells apart (C++ BasicClient::DropLine).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub(crate) enum DropLine {
    IncomingQueueFull = 0,
    NoOnMessage = 1,
    SentNoRoom = 3,
    SentNoConnection = 4,
    SentConnectionLost = 5,
    SentShutDown = 6,
}

/// Whether what arrives is queued for a receive (`SyncClient` and the
/// plain server) or handed out as it comes (`ThreadedClient`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IncomingMode {
    Queue,
    Dispatch,
}

/// The handle of a connection callers see (C++ ConnectionWrapper): what it
/// was asked to connect to and the address in use, kept after it is gone,
/// its live flag, and the client it belongs to. Empty, with no connection,
/// for a message of no connection.
#[derive(Debug, Clone)]
pub(crate) struct Wrapper {
    pub(crate) conn: Option<ConnId>,
    pub(crate) target: Target,
    pub(crate) endpoint: Option<SocketAddr>,
    pub(crate) living: Option<LivingFlag>,
    /// The instance id of the client it belongs to.
    pub(crate) owner: u64,
    /// The fork generation it was made in.
    pub(crate) generation: u32,
}

impl Wrapper {
    /// Whether the connection is live, from any thread.
    pub(crate) fn live(&self) -> bool {
        self.living.as_ref().is_some_and(|flag| flag.load(Ordering::Acquire))
    }

    /// The same connection, or the same target once it is gone: what a
    /// "down" from the connection observer carries.
    pub(crate) fn is_same_connection(&self, other: &Wrapper) -> bool {
        self.target == other.target
    }
}

/// One message read and handed out: the message and the connection it
/// came on.
#[derive(Debug, Clone)]
pub(crate) struct Incoming {
    pub(crate) message: MultiplexerMessage,
    pub(crate) connection: Wrapper,
}

/// A callback of the program's, for the drop observer.
pub(crate) type DropObserver = Box<dyn FnMut(u64, DropReason) + Send>;

/// The client's state over its connections; see the module comment.
pub(crate) struct ClientState {
    pub(crate) mode: IncomingMode,
    pub(crate) shuts_down: bool,
    pub(crate) routing: Routing,
    pub(crate) key_filters: KeyFilters,
    key_filters_message: Vec<KeyFilter>,
    routing_version: u32,
    acknowledged_callbacks: Vec<Acknowledged>,
    enqueued: u64,
    dequeued: u64,
    confirmed: bool,
    confirmed_enqueued: u64,
    acknowledged_version: u32,
    acknowledged_wake: bool,
    pub(crate) incoming: VecDeque<Incoming>,
    incoming_max: usize,
    /// A leaving server's: what arrives is refused rather than queued
    /// (`refuse_arrivals`).
    refusing: bool,
    pub(crate) frames_handled: u64,
    pub(crate) drop_lines: LogSummary,
    drop_lines_timer: Option<TimerKey>,
    drop_observer: Option<DropObserver>,
    pub(crate) dropped: Arc<AtomicU64>,
    pub(crate) incoming_dropped: Arc<AtomicU64>,
    /// The targets the client has: those with a connection, live or on its
    /// way, by target in C++'s order.
    pub(crate) by_target: BTreeMap<Target, ConnId>,
    /// The reconnects armed, by number: a timer and its target.
    reconnects: BTreeMap<u64, (TimerKey, Target)>,
    next_reconnect: u64,
    /// The targets whose first connection is over: it came up, or ended
    /// before it did.
    pub(crate) first_connection_over: BTreeSet<Target>,
    reached_by_target: BTreeMap<Target, u64>,
    one_multiplexer_said: HashSet<(Target, Target)>,
    closing: Vec<ConnId>,
    pub(crate) dropped_while_closing: u64,
    pub(crate) descriptors: Arc<DescriptorTable>,
    welcome: Option<Arc<OutFrame>>,
    last_seen: SpanSet,
    heartbit: Option<Arc<OutFrame>>,
}

impl ClientState {
    /// A client's state, nothing connected yet.
    pub(crate) fn new(mode: IncomingMode) -> ClientState {
        ClientState {
            mode,
            shuts_down: false,
            routing: Routing::default(),
            key_filters: KeyFilters::new(),
            key_filters_message: Vec::new(),
            routing_version: 0,
            acknowledged_callbacks: Vec::new(),
            enqueued: 0,
            dequeued: 0,
            confirmed: false,
            confirmed_enqueued: 0,
            acknowledged_version: 0,
            acknowledged_wake: false,
            incoming: VecDeque::new(),
            incoming_max: DEFAULT_INCOMING_QUEUE_MAX_SIZE,
            refusing: false,
            frames_handled: 0,
            drop_lines: LogSummary::default(),
            drop_lines_timer: None,
            drop_observer: None,
            dropped: Arc::new(AtomicU64::new(0)),
            incoming_dropped: Arc::new(AtomicU64::new(0)),
            by_target: BTreeMap::new(),
            reconnects: BTreeMap::new(),
            next_reconnect: 0,
            first_connection_over: BTreeSet::new(),
            reached_by_target: BTreeMap::new(),
            one_multiplexer_said: HashSet::new(),
            closing: Vec::new(),
            dropped_while_closing: 0,
            descriptors: Arc::new(DescriptorTable::default()),
            welcome: None,
            last_seen: SpanSet::new(2048),
            heartbit: None,
        }
    }
}

/// Runs a callback of the program's: one that panics is logged, with what
/// it was called for, and the client goes on, as C++ logs a callback that
/// throws.
pub(crate) fn guarded<F: FnOnce()>(description: impl FnOnce() -> String, callback: F) {
    if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)) {
        let what = panic
            .downcast_ref::<&str>()
            .map(|text| text.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "a panic".to_string());
        log::error!("{} panicked: {what}", description());
    }
}

impl Core {
    /// The heartbeat frame, made once.
    fn heartbit(&mut self) -> Arc<OutFrame> {
        let instance_id = self.instance_id;
        Arc::clone(self.client.heartbit.get_or_insert_with(|| heartbit_frame(instance_id)))
    }

    /// The welcome every new connection sends, carrying the routing and the
    /// key filters; made again after they change.
    pub(crate) fn welcome_frame(&mut self) -> Arc<OutFrame> {
        if let Some(welcome) = &self.client.welcome {
            return Arc::clone(welcome);
        }
        let welcome = WelcomeMessage {
            r#type: self.peer_type,
            id: self.instance_id,
            routing: Some(self.client.routing),
            key_filter: self.client.key_filters_message.clone(),
        };
        let message = MultiplexerMessage {
            id: Some(0),
            sender: Some(self.instance_id),
            r#type: types::CONNECTION_WELCOME,
            message: Some(welcome.encode_to_vec().into()),
            ..Default::default()
        };
        let bytes = frame::encode(&message).unwrap_or_default();
        let frame = Arc::new(OutFrame {
            bytes,
            id: 0,
            r#type: types::CONNECTION_WELCOME,
            pinned: false,
            for_all: false,
            own: true,
            number: 0,
            deadline: None,
        });
        self.client.welcome = Some(Arc::clone(&frame));
        frame
    }

    /// The wrapper of `conn` while it exists, else one of `target` that
    /// reads gone.
    pub(crate) fn wrap_or_gone(&self, conn: ConnId, target: &Target) -> Wrapper {
        if self.conns.contains_key(&conn) {
            return self.wrap(conn);
        }
        Wrapper {
            conn: Some(conn),
            target: target.clone(),
            endpoint: None,
            living: None,
            owner: self.instance_id,
            generation: crate::fork::generation(),
        }
    }

    /// The wrapper of `conn`, from what it stores.
    pub(crate) fn wrap(&self, conn: ConnId) -> Wrapper {
        let state = &self.conns[&conn];
        Wrapper {
            conn: Some(conn),
            target: state.target.clone(),
            endpoint: state.expected_endpoint,
            living: Some(Arc::clone(&state.living_flag)),
            owner: self.instance_id,
            generation: crate::fork::generation(),
        }
    }

    /// Every message the connections read: a PEER_STATUS answers the
    /// client's PEER_CONTROL; one without an id or seen before is dropped;
    /// the rest goes out as the mode says.
    pub(crate) fn handle_message(&mut self, conn: ConnId, message: MultiplexerMessage) {
        self.client.frames_handled += 1;
        if message.r#type == types::PEER_STATUS {
            self.on_peer_status(conn, &message);
            return;
        }
        let id = message.id.unwrap_or(0);
        if id == 0 || !self.client.last_seen.insert(id) {
            return;
        }
        let incoming = Incoming { message, connection: self.wrap(conn) };
        match self.client.mode {
            IncomingMode::Dispatch => self.dispatch_incoming(incoming),
            IncomingMode::Queue => {
                if self.query_takes(&incoming) {
                    return; // a query's, or a late reply to one
                }
                if self.client.refusing {
                    self.refuse(incoming);
                    return;
                }
                if self.client.incoming.len() >= self.client.incoming_max {
                    // Not seen after all: its copy through another
                    // multiplexer, coming once there is room, is taken.
                    self.client.last_seen.forget_last();
                    self.client.incoming_dropped.fetch_add(1, Ordering::Relaxed);
                    let kind = Kind {
                        reason: DropLine::IncomingQueueFull as u32,
                        level: log::Level::Warn,
                        r#type: 0,
                        peer: 0,
                    };
                    if self.drop_line_first(kind, || "incoming_queue_full, dropping".to_string()) {
                        log::warn!("incoming_queue_full, dropping #{id}");
                    }
                    return;
                }
                self.client.incoming.push_back(incoming);
                self.client.enqueued += 1;
            }
        }
    }

    /// Leaving, for the server classes: from now on a request that arrives
    /// is refused at once with the DELIVERY_ERROR a multiplexer sends for a
    /// peer that is gone, so that its sender retries elsewhere; what was
    /// read before stays to be received (Client::refuse_arrivals).
    pub(crate) fn refuse_arrivals(&mut self) {
        self.client.refusing = true;
    }

    /// The messages read and not received yet, refused or dropped by the
    /// same rule (Client::refuse_unread).
    pub(crate) fn refuse_unread(&mut self) {
        while let Some(incoming) = self.next_incoming_message() {
            self.refuse(incoming);
        }
    }

    /// Refuses `incoming` for a server that is leaving: a DELIVERY_ERROR
    /// for a request whose sender wants delivery errors, queued as a reply
    /// is, on the request's connection while it lives; nothing for a reply,
    /// which nobody retries and refusing which could start a loop, nor for
    /// the protocol's own messages.
    pub(crate) fn refuse(&mut self, incoming: Incoming) {
        let message = &incoming.message;
        let (id, r#type) = (message.id.unwrap_or(0), message.r#type);
        if message.references.unwrap_or(0) != 0 {
            log::debug!("reply #{id} of type {type} dropped: leaving");
            return;
        }
        log::debug!("request #{id} of type {type} refused: leaving");
        // A rule reports delivery errors unless told not to, and so does
        // this; a sender that set the message's own flag to false hears
        // nothing.
        if message.report_delivery_error == Some(false) || r#type <= types::MAX_MULTIPLEXER_META_PACKET {
            return;
        }
        let error = crate::proto::multiplexer::DeliveryError {
            packet_id: id,
            failed_type: vec![self.peer_type],
            ..Default::default()
        };
        let refusal_id = self.random64();
        let refusal = MultiplexerMessage {
            id: Some(refusal_id),
            sender: Some(self.instance_id),
            r#type: types::DELIVERY_ERROR,
            to: message.sender,
            references: Some(id),
            workflow: message.workflow.clone(),
            message: Some(error.encode_to_vec().into()),
            ..Default::default()
        };
        let Ok(bytes) = frame::encode(&refusal) else { return };
        let frame = super::outbox::Frame { bytes, id: refusal_id, r#type: types::DELIVERY_ERROR };
        let lane = super::lane::Lane::seeded(&incoming.connection, false);
        self.send(frame, false, Some(&lane), Some(crate::defaults::DEFAULT_TIMEOUT), 0, None, None, None);
    }

    /// The oldest message queued for a receive, taken out.
    pub(crate) fn next_incoming_message(&mut self) -> Option<Incoming> {
        let next = self.client.incoming.pop_front()?;
        self.client.dequeued += 1;
        Some(next)
    }

    /// Counts a drop line of `kind`: true when it is the first of its kind,
    /// which the caller logs now with what it knows; false when it is only
    /// counted for the summary, or the logger drops its level. The summary's
    /// timer runs while anything is counted.
    pub(crate) fn drop_line_first(&mut self, kind: Kind, text: impl FnOnce() -> String) -> bool {
        let Some(first) = self.client.drop_lines.first(kind, text, self.now) else { return false };
        if self.client.drop_lines_timer.is_none() {
            let deadline = self.now + crate::log_summary::INTERVAL;
            self.client.drop_lines_timer = Some(self.timers.arm(deadline, Due::LogSummary));
        }
        first
    }

    /// The drop lines' summary is due: the counts are said, and the timer
    /// runs on while any kind is left.
    pub(crate) fn log_summary_tick(&mut self) {
        self.client.drop_lines_timer = None;
        if self.client.drop_lines.say(false, self.now) {
            let deadline = self.now + crate::log_summary::INTERVAL;
            self.client.drop_lines_timer = Some(self.timers.arm(deadline, Due::LogSummary));
        }
    }

    /// Says every drop line's count now and forgets it, for a client that
    /// stops, whose timer would otherwise keep its loop running.
    pub(crate) fn flush_drop_lines(&mut self) {
        self.timers.cancel(&mut self.client.drop_lines_timer);
        self.client.drop_lines.say(true, self.now);
    }

    /// The drop observer, and the drops counted from any thread.
    pub(crate) fn set_drop_observer(&mut self, observer: Option<DropObserver>) {
        self.client.drop_observer = observer;
    }

    /// The one place every message the program sent and the client gives
    /// up on goes through: counted, logged at the drop lines' rate, a flush
    /// waiting for it told it was not all written, and the drop observer
    /// told its id and why.
    pub(crate) fn report_drop(&mut self, frame: &OutFrame, reason: DropReason) {
        self.client.dropped.fetch_add(1, Ordering::Relaxed);
        let (line, text) = match reason {
            DropReason::NoRoom => {
                (DropLine::SentNoRoom, "message dropped: it waited for room on its connection past its timeout")
            }
            DropReason::NoConnection => {
                (DropLine::SentNoConnection, "message dropped: it waited for a connection past its timeout")
            }
            DropReason::ConnectionLost => {
                (DropLine::SentConnectionLost, "message dropped: its connection ended and nothing else could take it")
            }
            DropReason::ShutDown => (DropLine::SentShutDown, "message dropped: the client shut down before it went"),
        };
        let kind = Kind { reason: line as u32, level: log::Level::Warn, r#type: frame.r#type, peer: 0 };
        if self.drop_line_first(kind, || text.to_string()) {
            log::warn!("{text}; id {}, type {}", frame.id, frame.r#type);
        }
        self.flushes_lost(frame.number);
        if let Some(observer) = self.client.drop_observer.as_mut() {
            let id = frame.id;
            guarded(|| "the drop observer".to_string(), || observer(id, reason));
        }
    }

    /// Closes every connection the polite way, reading what still arrives
    /// until its multiplexer's end or CLOSE_READ_SECONDS; what still waits
    /// is dropped and reported. Idempotent.
    pub(crate) fn shutdown_client(&mut self) {
        if self.client.shuts_down {
            return;
        }
        self.client.shuts_down = true;
        self.actions.push(Action::CancelResolves);
        log::debug!(
            "shutdown: {} target(s), {} registered, {} reconnect(s) pending",
            self.client.by_target.len(),
            self.by_peer_id.len(),
            self.client.reconnects.len()
        );
        // A reconnect armed by a connection lost before would open a
        // connection nobody closes: each is cancelled and taken out.
        for (_, (key, _)) in std::mem::take(&mut self.client.reconnects) {
            self.timers.cancel(&mut Some(key));
        }
        let targets: Vec<ConnId> = self.client.by_target.values().copied().collect();
        for conn in targets {
            self.close_gracefully(conn, crate::defaults::CLOSE_READ_SECONDS);
        }
        self.drop_outbox();
        self.client.acknowledged_callbacks.clear();
        self.flush_drop_lines();
    }

    /// Whether a connection is live or could still come up: one registered,
    /// connecting or resolving, or a reconnect armed.
    pub(crate) fn connection_live_or_coming(&self) -> bool {
        if self.client.shuts_down {
            return false;
        }
        if !self.client.reconnects.is_empty() {
            return true;
        }
        self.client.by_target.values().any(|conn| self.conns.get(conn).is_some_and(|state| !state.shuts_down))
    }

    /// Whether a connection closed the polite way still reads to its end.
    pub(crate) fn closing(&mut self) -> bool {
        let conns = &self.conns;
        self.client.closing.retain(|conn| conns.contains_key(conn));
        !self.client.closing.is_empty()
    }

    /// How many reconnects are armed; for tests.
    #[cfg(test)]
    pub(crate) fn reconnects_pending(&self) -> usize {
        self.client.reconnects.len()
    }

    /// A connection has ended for good: its socket closes, gone from the
    /// table a forked child reads first, and the messages it read after it
    /// began closing, which it could only drop, are added up and logged.
    pub(crate) fn connection_closed(&mut self, conn: ConnId) {
        self.close_socket(conn);
        let Some(state) = self.conns.get(&conn) else { return };
        let dropped = state.dropped_while_closing;
        if dropped == 0 {
            return;
        }
        self.client.dropped_while_closing += dropped;
        log::warn!(
            "{dropped} message(s) from multiplexer {} arrived after the connection began closing, and were dropped: \
             a request among them gets no answer",
            state.peer_id
        );
    }
}
