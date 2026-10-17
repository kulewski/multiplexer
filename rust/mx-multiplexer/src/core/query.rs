//! The query algorithm, one state machine for every class: the request
//! through one connection; on a delivery error, a timeout or the connection
//! lost, a search for a backend of the type on every connection, or for an
//! addressed query a PING addressed to that instance; then the request
//! again to the instance found, by instance id, through the connection its
//! PING came on; and where the query would fail, a wait for a late reply to
//! an attempt a backend may have. The request goes out at most twice and a
//! query never goes back a stage; docs/query.md draws it and lists every
//! way a stage ends. Mirrors the state machine of
//! multiplexer/threaded_client_query.cc; `SyncClient` runs the same machine
//! inside its call, where C++ runs Client::_query.
//!
//! Replies are matched by the ids they reference, so many queries may be
//! in flight at once. A client that hands out what arrives as it comes
//! (`ThreadedClient`) keeps the ids of finished queries a while, twice the
//! longer of the query's timeout and DEFAULT_TIMEOUT, at most 131072 of
//! them, so that a late reply to one is dropped rather than handed out as a
//! message of its own; a client with a receive queue (`SyncClient`) keeps a
//! late reply that arrives between its calls for its receive, as in C++
//! and Python (docs/api_python.md, "ThreadedTestClient").

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use prost::Message as _;

use super::client::{guarded, Incoming, IncomingMode, Wrapper};
use super::lane::Lane;
use super::outbox::Frame;
use super::timers::TimerKey;
use super::{deadline_after, Core, Due, SendState, Tracker};
use crate::constants::types;
use crate::defaults::DEFAULT_TIMEOUT;
use crate::error::{Error, Result};
use crate::frame;
use crate::proto::multiplexer::{BackendForPacketSearch, MultiplexerMessage};

/// How many finished queries' ids are kept at most.
const FINISHED_IDS_KEPT: usize = 1 << 17;

/// How a query ended (C++ ThreadedClient::Outcome). Moved once, into the
/// query's callback: a box for the reply would cost an allocation a query.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Outcome {
    /// The reply.
    Replied(Incoming),
    /// A stage ran out of time.
    TimedOut,
    /// No backend takes the request, the backend found is gone, or the
    /// addressee is gone.
    Failed,
    /// No live connection to send through.
    NotConnected,
    /// The client shut down first.
    ShutDown,
}

impl Outcome {
    /// The reply, or the error the outcome is.
    pub(crate) fn into_result(self) -> Result<Incoming> {
        match self {
            Outcome::Replied(reply) => Ok(reply),
            Outcome::TimedOut => Err(Error::OperationTimedOut),
            Outcome::Failed => Err(Error::OperationFailed),
            Outcome::NotConnected | Outcome::ShutDown => Err(Error::NotConnected),
        }
    }
}

/// A query's end, heard once.
pub(crate) type QueryDone = Box<dyn FnOnce(Outcome) + Send>;

/// A query's on_received: the instance id of each backend that acknowledged
/// one of its attempts.
pub(crate) type Received = Box<dyn FnMut(u64) + Send>;

/// A query's number in the core.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct QueryId(pub(crate) u64);

/// Where a query is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// The request is out, through `sent_via`.
    Request,
    /// The search, or the locating PING, is out.
    Search,
    /// The request again, to the instance found.
    Direct,
    /// No connection was live when a stage's message had to go out: it goes
    /// out when one comes up, the stage's deadline running.
    Waiting,
    /// The query can only wait for a late reply to an attempt a backend may
    /// have (`pending`).
    Late,
}

/// One query in flight.
struct InFlight {
    stage: Stage,
    resume: Stage,
    direct_to: u64,
    sent_via: Option<Wrapper>,
    sent_state: Option<Arc<Tracker>>,
    request_state: Option<Arc<Tracker>>,
    direct_state: Option<Arc<Tracker>>,
    prototype: MultiplexerMessage,
    timeout: Option<Duration>,
    deadline: Option<Instant>,
    lane: Option<Lane>,
    request_id: u64,
    search_id: u64,
    direct_id: u64,
    taken: bool,
    pending: Vec<u64>,
    searched: Vec<Wrapper>,
    search_ids: Vec<u64>,
    generation: u64,
    listed: bool,
    callback: Option<QueryDone>,
    received: Option<Received>,
    timer: Option<TimerKey>,
}

impl InFlight {
    fn addressed(&self) -> bool {
        self.prototype.to.unwrap_or(0) != 0
    }
    fn pinned(&self) -> bool {
        self.lane.as_ref().is_some_and(Lane::pinned)
    }
}

/// The queries' state.
#[derive(Default)]
pub(crate) struct Queries {
    in_flight: BTreeMap<QueryId, InFlight>,
    next_query: u64,
    by_id: HashMap<u64, QueryId>,
    waiting: VecDeque<QueryId>,
    waiting_ended: usize,
    finished_order: VecDeque<(u64, Instant)>,
    finished_ids: HashSet<u64>,
    pub(crate) retries: u64,
}

impl Queries {
    /// How many message ids are watched for an answer; for tests.
    pub(crate) fn watched_ids(&self) -> usize {
        self.by_id.len()
    }

    /// How many queries wait for a connection, or ended there and are not
    /// cleared out yet; for tests.
    pub(crate) fn waiting_queries(&self) -> usize {
        self.waiting.len()
    }

    /// Whether the query waits for a connection.
    pub(crate) fn waiting_for_connection(&self, query: QueryId) -> bool {
        self.in_flight.get(&query).is_some_and(|in_flight| in_flight.stage == Stage::Waiting)
    }

    /// The ids of the query's attempts so far, which a reply references.
    pub(crate) fn attempt_ids(&self, query: QueryId) -> Vec<u64> {
        self.in_flight.get(&query).map_or_else(Vec::new, |in_flight| {
            [in_flight.request_id, in_flight.direct_id].into_iter().filter(|id| *id != 0).collect()
        })
    }

    /// Forgets the ids of every query that has ended.
    pub(crate) fn forget_finished_ids(&mut self) {
        self.finished_ids = HashSet::new();
        self.finished_order = VecDeque::new();
    }
}

/// Measures `request` as a query frames it at its largest, its sender,
/// its id and, for a typed query, its request sent again with the `to` of
/// the backend found and a delivery error asked for: refused over
/// MAX_MESSAGE_SIZE before anything goes out. `request` comes back as it
/// was but for an empty sender, `sender` there as start_query would put it.
pub(crate) fn check_size_sent_again(request: &mut MultiplexerMessage, sender: u64) -> Result<()> {
    if request.sender.unwrap_or(0) == 0 {
        request.sender = Some(sender);
    }
    let (id, to, asked) = (request.id, request.to, request.report_delivery_error);
    request.id = Some(u64::MAX);
    if to.unwrap_or(0) == 0 {
        request.to = Some(u64::MAX);
    }
    request.report_delivery_error = Some(true);
    let size = request.encoded_len();
    request.id = id;
    request.to = to;
    request.report_delivery_error = asked;
    frame::check_key_size(request.key.as_deref())?;
    frame::check_message_size(size)
}

impl Core {
    /// Starts a query: `prototype` is the request, its sender filled in,
    /// each attempt getting an id of its own. `done` hears its end, once.
    pub(crate) fn start_query(
        &mut self,
        mut prototype: MultiplexerMessage,
        timeout: Option<Duration>,
        lane: Option<Lane>,
        done: QueryDone,
        received: Option<Received>,
    ) -> QueryId {
        if prototype.sender.unwrap_or(0) == 0 {
            prototype.sender = Some(self.instance_id);
        }
        if prototype.to.unwrap_or(0) != 0 {
            prototype.report_delivery_error = Some(true); // "not behind this multiplexer" must come back
        }
        self.queries.next_query += 1;
        let query = QueryId(self.queries.next_query);
        let in_flight = InFlight {
            stage: Stage::Request,
            resume: Stage::Request,
            direct_to: 0,
            sent_via: None,
            sent_state: None,
            request_state: None,
            direct_state: None,
            prototype,
            timeout,
            deadline: deadline_after(self.now, timeout),
            lane,
            request_id: 0,
            search_id: 0,
            direct_id: 0,
            taken: false,
            pending: Vec::new(),
            searched: Vec::new(),
            search_ids: Vec::new(),
            generation: 0,
            listed: false,
            callback: Some(done),
            received,
            timer: None,
        };
        self.queries.in_flight.insert(query, in_flight);
        self.start_request(query, false);
        query
    }

    /// The request's frame, for an attempt `id`.
    fn attempt_frame(prototype: &MultiplexerMessage, id: u64) -> Option<Frame> {
        let mut request = prototype.clone();
        request.id = Some(id);
        let bytes = frame::encode(&request).ok()?;
        Some(Frame { bytes, id, r#type: request.r#type })
    }

    /// Hands `frame` to the outbox through the query's lane: on the lane's
    /// connection while it is live, else on any, which the lane adopts.
    /// None when no connection is live; `refused` when a pinned lane's
    /// connection is gone.
    fn schedule_through(
        &mut self,
        frame: Frame,
        lane: Option<&Lane>,
        used: &mut Option<Wrapper>,
        refused: &mut bool,
        timeout: Option<Duration>,
    ) -> Option<Arc<Tracker>> {
        let pinned = lane.is_some_and(Lane::pinned);
        if let Some(lane) = lane {
            if lane.holds_connection() {
                let held = lane.connection();
                if let Some(held) = held {
                    if let Some(tracker) = self.schedule_on(frame.clone(), pinned, &held, timeout, 0, Some(lane)) {
                        *used = Some(held);
                        return Some(tracker);
                    }
                }
                if pinned {
                    *refused = true;
                    return None;
                }
            }
        }
        let tracker = self.schedule_one(frame, pinned, Some(&mut *used), timeout, 0, lane)?;
        if let (Some(lane), Some(used)) = (lane, used.as_ref()) {
            lane.adopt(used);
        }
        Some(tracker)
    }

    /// What the next stage's timer gets: the whole timeout for a typed
    /// query, what is left of the one deadline for an addressed one.
    fn stage_timeout(&self, query: QueryId) -> Option<Duration> {
        let in_flight = &self.queries.in_flight[&query];
        if !in_flight.addressed() {
            return in_flight.timeout;
        }
        in_flight.deadline.map(|deadline| deadline.saturating_duration_since(self.now))
    }

    /// The request's stage: the request goes out, or waits for a
    /// connection within the deadline. Refused from a ThreadedClient's
    /// shutdown() on, where a callback's query would otherwise go out
    /// during the write-out and hold the io thread for its whole stage.
    fn start_request(&mut self, query: QueryId, keep_deadline: bool) {
        if self.client.shuts_down || self.threaded.shut_down {
            self.finish(query, Outcome::ShutDown);
            return;
        }
        let request_id = match self.queries.in_flight[&query].request_id {
            0 => self.random64(),
            id => id, // kept by a request that waits for a connection
        };
        let timeout = self.stage_timeout(query);
        let in_flight = self.queries.in_flight.get_mut(&query).expect("in flight");
        in_flight.request_id = request_id;
        let lane = in_flight.lane.clone();
        let Some(frame) = Self::attempt_frame(&in_flight.prototype, request_id) else {
            self.finish(query, Outcome::Failed);
            return;
        };
        let mut used = None;
        let mut refused = false;
        let tracker = self.schedule_through(frame, lane.as_ref(), &mut used, &mut refused, timeout);
        let Some(tracker) = tracker else {
            if refused {
                self.finish(query, Outcome::NotConnected); // the pinned lane's connection is gone
                return;
            }
            self.wait_for_connection(query, Stage::Request);
            if !keep_deadline {
                self.arm_query(query, self.queries.in_flight[&query].timeout);
            }
            return;
        };
        let in_flight = self.queries.in_flight.get_mut(&query).expect("in flight");
        in_flight.sent_via = used;
        in_flight.sent_state = Some(Arc::clone(&tracker));
        in_flight.request_state = Some(tracker);
        in_flight.stage = Stage::Request;
        let timeout = in_flight.timeout;
        log::trace!("request {request_id}");
        self.queries.by_id.insert(request_id, query);
        if !keep_deadline {
            self.arm_query(query, timeout);
        }
    }

    /// The stage `resume`'s message found no connection live: it goes out
    /// when one comes up, within the stage's deadline.
    fn wait_for_connection(&mut self, query: QueryId, resume: Stage) {
        let in_flight = self.queries.in_flight.get_mut(&query).expect("in flight");
        in_flight.stage = Stage::Waiting;
        in_flight.resume = resume;
        if !in_flight.listed {
            in_flight.listed = true;
            self.queries.waiting.push_back(query);
        }
    }

    /// A connection came up: the waiting queries send the message of the
    /// stage they wait in, in order; one that finds none live waits on.
    pub(crate) fn restart_waiting_queries(&mut self) {
        let pass = std::mem::take(&mut self.queries.waiting);
        self.queries.waiting_ended = 0;
        for query in pass {
            let Some(in_flight) = self.queries.in_flight.get_mut(&query) else { continue };
            in_flight.listed = false;
            if in_flight.stage != Stage::Waiting {
                continue;
            }
            self.queries.retries += 1;
            match in_flight.resume {
                Stage::Search => self.search(query, true),
                Stage::Direct => self.send_direct(query, None, true),
                _ => self.start_request(query, true),
            }
        }
    }

    /// Arms the query's timer `timeout` from now; no deadline arms none.
    fn arm_query(&mut self, query: QueryId, timeout: Option<Duration>) {
        let deadline = deadline_after(self.now, timeout);
        let Some(in_flight) = self.queries.in_flight.get_mut(&query) else { return };
        in_flight.generation += 1;
        let generation = in_flight.generation;
        match deadline {
            Some(deadline) => {
                self.timers.rearm(&mut in_flight.timer, deadline, Due::Query(query, generation));
            }
            None => self.timers.cancel(&mut in_flight.timer),
        }
    }

    /// A stage's deadline: the request's moves on to the search, a backend
    /// perhaps having it; a wait for a connection is NotConnected; any other
    /// stage times out.
    pub(crate) fn query_deadline(&mut self, query: QueryId, generation: u64) {
        let Some(in_flight) = self.queries.in_flight.get_mut(&query) else { return };
        if in_flight.generation != generation {
            return;
        }
        in_flight.timer = None;
        match in_flight.stage {
            Stage::Request => {
                in_flight.taken = true; // no answer in time: a backend may have the request
                self.search(query, false);
            }
            Stage::Waiting => self.finish(query, Outcome::NotConnected),
            _ => self.finish(query, Outcome::TimedOut),
        }
    }

    /// A message for a query in flight, by the id it references: None when
    /// it was one, handled here, a reply moved into the query's outcome;
    /// the message back when it is for nobody here. A late reply to a
    /// finished query is dropped, and None too.
    pub(crate) fn query_takes(&mut self, incoming: Incoming) -> Option<Incoming> {
        let references = incoming.message.references.unwrap_or(0);
        if references == 0 {
            return Some(incoming);
        }
        if let Some(query) = self.queries.by_id.get(&references).copied() {
            if incoming.message.r#type == types::REQUEST_RECEIVED {
                self.acknowledged(query, &incoming.message);
            } else {
                self.advance(query, incoming);
            }
            return None;
        }
        if self.client.mode == IncomingMode::Dispatch && self.queries.finished_ids.contains(&references) {
            log::debug!("late reply #{} to finished query #{references} dropped", incoming.message.id.unwrap_or(0));
            return None;
        }
        Some(incoming)
    }

    /// A backend acknowledged an attempt: the query's on_received hears
    /// which; a search is no attempt.
    fn acknowledged(&mut self, query: QueryId, message: &MultiplexerMessage) {
        let Some(in_flight) = self.queries.in_flight.get_mut(&query) else { return };
        let references = message.references.unwrap_or(0);
        if in_flight.search_ids.contains(&references) {
            return;
        }
        let r#type = in_flight.prototype.r#type;
        if let Some(received) = in_flight.received.as_mut() {
            let sender = message.sender.unwrap_or(0);
            guarded(|| format!("the on_received of a query of type {type}"), || received(sender));
        }
    }

    /// An answer to one of the query's messages: a search's PING or
    /// delivery error, or the reply or a delivery error for an attempt.
    fn advance(&mut self, query: QueryId, incoming: Incoming) {
        let Some(in_flight) = self.queries.in_flight.get(&query) else { return };
        let references = incoming.message.references.unwrap_or(0);
        let to_a_search = in_flight.search_ids.contains(&references);
        let delivery_error = incoming.message.r#type == types::DELIVERY_ERROR;
        if !to_a_search {
            if delivery_error {
                self.refused(query, references);
            } else {
                self.finish(query, Outcome::Replied(incoming));
            }
            return;
        }
        if in_flight.stage != Stage::Search || references != in_flight.search_id {
            return; // a search's answer after the query moved on
        }
        if delivery_error {
            self.answered(query, &incoming.connection);
            if self.queries.in_flight[&query].searched.is_empty() {
                self.wait_late(query, false); // nobody behind any multiplexer
            }
        } else if incoming.message.r#type == types::PING {
            self.direct(query, &incoming);
        }
    }

    /// A delivery error for an attempt: nobody has it, and it is struck off.
    fn refused(&mut self, query: QueryId, id: u64) {
        let Some(in_flight) = self.queries.in_flight.get_mut(&query) else { return };
        if id == in_flight.request_id {
            in_flight.taken = false;
        }
        match in_flight.stage {
            Stage::Request if id == in_flight.request_id => self.search(query, false),
            Stage::Direct if id == in_flight.direct_id => self.wait_late(query, false),
            Stage::Late => {
                in_flight.pending.retain(|each| *each != id);
                if in_flight.pending.is_empty() {
                    self.finish(query, Outcome::Failed);
                }
            }
            _ => {}
        }
    }

    /// The middle stage: a search for a backend of the type on every
    /// connection, carrying the request's key, or for an addressed query a
    /// PING addressed to the instance; through a pinned lane, on its one
    /// connection.
    fn search(&mut self, query: QueryId, keep_deadline: bool) {
        let left = self.stage_timeout(query);
        let in_flight = &self.queries.in_flight[&query];
        if in_flight.addressed() && left == Some(Duration::ZERO) {
            self.finish(query, Outcome::TimedOut);
            return;
        }
        if !keep_deadline {
            self.arm_query(query, left);
        }
        let in_flight = &self.queries.in_flight[&query];
        let id = self.random64();
        let mut message = MultiplexerMessage {
            id: Some(id),
            sender: Some(self.instance_id),
            message: Some(Default::default()),
            ..Default::default()
        };
        if in_flight.addressed() {
            message.r#type = types::PING;
            message.to = in_flight.prototype.to;
            message.report_delivery_error = Some(true);
        } else {
            message.r#type = types::BACKEND_FOR_PACKET_SEARCH;
            let search = BackendForPacketSearch { packet_type: in_flight.prototype.r#type };
            message.message = Some(search.encode_to_vec().into());
            message.key.clone_from(&in_flight.prototype.key); // only a backend that takes the key may be found
        }
        let Ok(bytes) = frame::encode(&message) else {
            self.finish(query, Outcome::Failed);
            return;
        };
        let frame = Frame { bytes, id, r#type: message.r#type };
        let wait = if left.is_some_and(|left| !left.is_zero()) { left } else { in_flight.timeout };
        let mut sent = Vec::new();
        if in_flight.pinned() {
            let lane = in_flight.lane.clone();
            let mut used = None;
            let mut refused = false;
            if self.schedule_through(frame, lane.as_ref(), &mut used, &mut refused, wait).is_some() {
                sent.extend(used);
            } else if refused {
                self.finish(query, Outcome::NotConnected);
                return;
            }
        } else {
            self.schedule_all(frame, Some(&mut sent), wait, 0, None);
        }
        if sent.is_empty() {
            self.wait_for_connection(query, Stage::Search);
            return;
        }
        let in_flight = self.queries.in_flight.get_mut(&query).expect("in flight");
        in_flight.search_id = id;
        in_flight.search_ids.push(id);
        in_flight.searched = sent;
        in_flight.stage = Stage::Search;
        self.queries.by_id.insert(id, query);
    }

    /// The search had its answer from `connection`, a delivery error or the
    /// connection going down: it counts no more. False when the search never
    /// went through it.
    fn answered(&mut self, query: QueryId, connection: &Wrapper) -> bool {
        let Some(in_flight) = self.queries.in_flight.get_mut(&query) else { return false };
        match in_flight.searched.iter().position(|each| each.is_same_connection(connection)) {
            Some(position) => {
                in_flight.searched.remove(position);
                true
            }
            None => false,
        }
    }

    /// The query can only wait for a late reply to an attempt a backend may
    /// have: the request when it may have been taken, the direct request
    /// when its connection went (`direct`), unless the client gave it up.
    /// With neither, nothing can answer any more.
    fn wait_late(&mut self, query: QueryId, direct: bool) {
        let Some(in_flight) = self.queries.in_flight.get_mut(&query) else { return };
        let given_up = |tracker: &Option<Arc<Tracker>>| tracker.as_ref().is_some_and(|t| t.get() == SendState::Lost);
        in_flight.pending.clear();
        if in_flight.taken && !given_up(&in_flight.request_state) {
            in_flight.pending.push(in_flight.request_id);
        }
        if direct && !given_up(&in_flight.direct_state) {
            in_flight.pending.push(in_flight.direct_id);
        }
        if in_flight.pending.is_empty() {
            self.finish(query, Outcome::Failed);
            return;
        }
        in_flight.stage = Stage::Late;
    }

    /// The search found the instance that sent `ping`: the direct request
    /// goes to it, through the connection the PING came on.
    fn direct(&mut self, query: QueryId, ping: &Incoming) {
        let id = self.random64();
        let Some(in_flight) = self.queries.in_flight.get_mut(&query) else { return };
        in_flight.direct_to = ping.message.sender.unwrap_or(0);
        in_flight.direct_id = id; // kept by a direct request that waits for a connection
        self.send_direct(query, Some(ping.connection.clone()), false);
    }

    /// The direct request: through `preferred`, the connection the PING
    /// came on, while it lives, else any, through a pinned lane only the
    /// lane's; the lane adopts it.
    fn send_direct(&mut self, query: QueryId, preferred: Option<Wrapper>, keep_deadline: bool) {
        let left = self.stage_timeout(query);
        let in_flight = &self.queries.in_flight[&query];
        let mut request = in_flight.prototype.clone();
        request.id = Some(in_flight.direct_id);
        request.to = Some(in_flight.direct_to);
        request.report_delivery_error = Some(true); // a backend gone since its PING says so
        let Ok(bytes) = frame::encode(&request) else {
            self.finish(query, Outcome::Failed);
            return;
        };
        let frame = Frame { bytes, id: in_flight.direct_id, r#type: request.r#type };
        let pinned = in_flight.pinned();
        let lane = in_flight.lane.clone();
        if !keep_deadline {
            self.arm_query(query, left);
        }
        let mut used = preferred.clone();
        let mut tracker = None;
        if let Some(preferred) = &preferred {
            tracker = self.schedule_on(frame.clone(), pinned, preferred, left, 0, None);
        }
        if let (Some(_), Some(lane), Some(used)) = (&tracker, &lane, &used) {
            lane.adopt(used);
        }
        if tracker.is_none() {
            let mut refused = false;
            let mut through = None;
            tracker = self.schedule_through(frame, lane.as_ref(), &mut through, &mut refused, left);
            if refused {
                self.finish(query, Outcome::NotConnected);
                return;
            }
            used = through;
        }
        let Some(tracker) = tracker else {
            self.wait_for_connection(query, Stage::Direct);
            return;
        };
        let in_flight = self.queries.in_flight.get_mut(&query).expect("in flight");
        in_flight.sent_via = used;
        in_flight.sent_state = Some(Arc::clone(&tracker));
        in_flight.direct_state = Some(tracker);
        in_flight.stage = Stage::Direct;
        let id = in_flight.direct_id;
        self.queries.by_id.insert(id, query);
    }

    /// A connection came up or went, for the queries: up, those waiting for
    /// one go now, posted; down, a query whose attempt went out through it
    /// moves on, or follows its attempt to where the outbox hands it while
    /// it is unwritten; a pinned lane's search or late wait ends
    /// NotConnected.
    pub(crate) fn queries_on_connection(&mut self, connection: &Wrapper, up: bool) {
        if up {
            self.post_fill();
            return;
        }
        let queries: Vec<QueryId> = self.queries.in_flight.keys().copied().collect();
        for query in queries {
            let Some(in_flight) = self.queries.in_flight.get(&query) else { continue };
            match in_flight.stage {
                Stage::Request | Stage::Direct => {
                    if in_flight.sent_via.as_ref().is_some_and(|via| via.is_same_connection(connection)) {
                        let unwritten =
                            in_flight.sent_state.as_ref().is_some_and(|state| state.get() == SendState::Queued);
                        if !in_flight.pinned() && unwritten {
                            self.follow_handover(query);
                        } else {
                            self.lost(query);
                        }
                    }
                }
                Stage::Search => {
                    if in_flight.pinned() && self.answered(query, connection) {
                        self.finish(query, Outcome::NotConnected);
                    }
                }
                Stage::Late => {
                    let lane_lost = in_flight
                        .lane
                        .as_ref()
                        .and_then(Lane::connection)
                        .is_some_and(|held| held.is_same_connection(connection));
                    if in_flight.pinned() && lane_lost {
                        self.finish(query, Outcome::NotConnected);
                    }
                }
                Stage::Waiting => {}
            }
        }
    }

    /// One pass over the waiting queries on its way, however many
    /// connections came up.
    fn post_fill(&mut self) {
        if self.queries_fill_posted {
            return;
        }
        self.queries_fill_posted = true;
        self.posted.push_back(super::Posted::Fill);
    }

    /// The pass: the queries that waited for a connection start again.
    pub(crate) fn fill(&mut self) {
        self.queries_fill_posted = false;
        if !self.client.shuts_down && !self.threaded.shut_down && !self.queries.waiting.is_empty() {
            self.restart_waiting_queries();
        }
    }

    /// The connection the attempt was queued on is going while the attempt
    /// waits there unwritten: the outbox hands it on; once that is done the
    /// query follows it to the connection that took it, posted.
    fn follow_handover(&mut self, query: QueryId) {
        let Some(in_flight) = self.queries.in_flight.get(&query) else { return };
        let (generation, stage) = (in_flight.generation, in_flight.stage);
        self.posted.push_back(super::Posted::FollowHandover(query, generation, stage == Stage::Request));
    }

    /// The posted half of `follow_handover`.
    pub(crate) fn followed_handover(&mut self, query: QueryId, generation: u64, request: bool) {
        let Some(in_flight) = self.queries.in_flight.get(&query) else { return };
        let stage = if request { Stage::Request } else { Stage::Direct };
        if in_flight.generation != generation || in_flight.stage != stage {
            return; // over, or moved on meanwhile
        }
        let Some(state) = in_flight.sent_state.clone() else { return };
        if state.get() == SendState::Lost {
            if request {
                self.search(query, false); // nobody took it: on to the search, no late reply
            } else {
                self.wait_late(query, false); // the direct request went nowhere
            }
            return;
        }
        let first = in_flight.sent_via.clone();
        let where_to = self.followed_connection(&state, first);
        if !where_to.as_ref().is_some_and(|wrapper| self.live(wrapper)) {
            self.lost(query);
            return;
        }
        if let Some(in_flight) = self.queries.in_flight.get_mut(&query) {
            in_flight.sent_via = where_to;
        }
    }

    /// The connection a request or a direct request went through is gone:
    /// from the request the query moves on to the search at once, a backend
    /// perhaps having it; from the direct request it waits for a late
    /// reply; through a pinned lane there is nothing else to try.
    fn lost(&mut self, query: QueryId) {
        let Some(in_flight) = self.queries.in_flight.get_mut(&query) else { return };
        if in_flight.pinned() {
            self.finish(query, Outcome::NotConnected);
            return;
        }
        let request = in_flight.stage == Stage::Request;
        log::warn!(
            "connection lost under query {}{}",
            in_flight.request_id,
            if !request {
                "; waiting for a late reply"
            } else if in_flight.addressed() {
                "; locating the addressee"
            } else {
                "; searching for a backend"
            }
        );
        if request {
            in_flight.taken = true;
            self.search(query, false);
        } else {
            self.wait_late(query, true);
        }
    }

    /// The query ends: its ids kept a while for the late replies, its lane
    /// adopting the connection a reply came through, its callback told,
    /// once.
    pub(crate) fn finish(&mut self, query: QueryId, outcome: Outcome) {
        let Some(mut in_flight) = self.queries.in_flight.remove(&query) else { return };
        self.timers.cancel(&mut in_flight.timer);
        let kept = match in_flight.timeout {
            Some(timeout) if timeout > DEFAULT_TIMEOUT && timeout <= super::MAX_TIMEOUT => 2 * timeout,
            _ => 2 * DEFAULT_TIMEOUT,
        };
        let until = self.now + kept;
        let mut ids = vec![in_flight.request_id];
        if in_flight.direct_id != 0 {
            ids.push(in_flight.direct_id);
        }
        ids.extend(in_flight.search_ids.iter().copied());
        for id in ids {
            self.queries.by_id.remove(&id);
            if self.client.mode == IncomingMode::Dispatch {
                self.remember_finished(id, until);
            }
        }
        if let (Outcome::Replied(reply), Some(lane)) = (&outcome, &in_flight.lane) {
            lane.adopt(&reply.connection); // what follows the query goes where the answer came from
        }
        let r#type = in_flight.prototype.r#type;
        if let Some(callback) = in_flight.callback.take() {
            guarded(|| format!("the callback of a query of type {type}"), || callback(outcome));
        }
        if in_flight.listed {
            self.queries.waiting_ended += 1;
            self.clear_out_waiting();
        }
    }

    /// A finished query's `id`, kept `until` a late reply to it can no
    /// longer be expected; those whose time is up go first, and the oldest
    /// whatever their time when there are too many.
    fn remember_finished(&mut self, id: u64, until: Instant) {
        let now = self.now;
        let queries = &mut self.queries;
        while let Some(&(oldest, kept)) = queries.finished_order.front() {
            if kept > now && queries.finished_order.len() < FINISHED_IDS_KEPT {
                break;
            }
            queries.finished_ids.remove(&oldest);
            queries.finished_order.pop_front();
        }
        if id == 0 || !queries.finished_ids.insert(id) {
            return;
        }
        queries.finished_order.push_back((id, until));
    }

    /// The waiting queries that ended are cleared out once they are as many
    /// as those still waiting and 64 more, or none waits any more.
    fn clear_out_waiting(&mut self) {
        let queries = &mut self.queries;
        let live = queries.waiting.len() - queries.waiting_ended;
        if live != 0 && queries.waiting_ended < live + 64 {
            return;
        }
        let in_flight = &queries.in_flight;
        queries.waiting.retain(|query| in_flight.contains_key(query));
        queries.waiting_ended = 0;
    }

    /// Ends every query in flight with SHUT_DOWN, in the order they began.
    pub(crate) fn finish_every_query(&mut self) {
        let queries: Vec<QueryId> = self.queries.in_flight.keys().copied().collect();
        for query in queries {
            self.finish(query, Outcome::ShutDown);
        }
        self.queries.waiting.clear();
        self.queries.waiting_ended = 0;
    }
}
