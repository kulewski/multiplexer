//! `SyncClient`: a client with no thread of its own. Every call runs the
//! client's loop inside it until the call's condition holds, and between
//! calls nothing runs, so a peer built on it is passive and its type needs
//! `is_passive` in the rules file. Mirrors multiplexer/client.h and the
//! waits of basic_client.h, each built on `Engine::run_one` and
//! `Engine::poll` as C++ builds them on asio; the query is the one state
//! machine every class shares (core/query.rs). docs/api_rust.md describes
//! it from the caller's side.
//!
//! What a call that waits does with what it reads: a receive hands out the
//! oldest message; a query takes its own replies, and the other messages it
//! reads meanwhile go to `on_unexpected`, or are logged and dropped, as the
//! C++ and Python clients drop them. Callbacks (a send's `on_end`, the drop
//! observer, `on_received`, an acknowledgement's) run inside whichever call
//! runs the loop when their time comes.
//!
//! A `SyncClient` belongs to one thread at a time: it is `Send`, not
//! `Sync`. One this process inherited across a fork fails every call with
//! `UsedAfterFork`; dropped there, it closes the child's copies of its
//! sockets and leaks the rest, as the C++ client does.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::constants::types;
use crate::core::client::{target_of, DropReason, IncomingMode};
use crate::core::outbox::{Frame, SendDone};
use crate::core::query::{check_size_sent_again, Outcome, QueryId};
use crate::core::{deadline_after, SendState, Tracker};
use crate::defaults::{CLOSE_FLUSH_SECONDS, CLOSE_READ_SECONDS, DEFAULT_TIMEOUT, ROOM_GRACE_SECONDS};
use crate::endpoint::{format_endpoint, parse_endpoint};
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::handles::{Connection, Incoming, Lane};
use crate::message::{KeyFilters, Message, Routing};
use crate::proto::multiplexer as proto;

/// A callback for the messages a query reads that are not its reply.
type Unexpected = Box<dyn FnMut(Incoming) + Send>;

/// Builds a `SyncClient`; see `SyncClient::builder`.
#[must_use = "a builder does nothing until build() or connect()"]
pub struct SyncClientBuilder {
    peer_type: u32,
    on_drop: Option<Box<dyn FnMut(u64, DropReason) + Send>>,
    key_filters: Option<KeyFilters>,
    routing: Option<Routing>,
    on_unexpected: Option<Unexpected>,
}

impl SyncClientBuilder {
    /// Every message the program sent that the client gives up on, each
    /// copy of one sent to ALL, is told to `observer` with its id and why,
    /// once the handler it happened in is done, inside whichever call runs
    /// the loop then; `dropped()` counts them at once.
    pub fn on_drop(mut self, observer: impl FnMut(u64, DropReason) + Send + 'static) -> Self {
        self.on_drop = Some(Box::new(observer));
        self
    }

    /// The key filters, in effect from the first message: carried by every
    /// welcome.
    pub fn key_filters(mut self, filters: KeyFilters) -> Self {
        self.key_filters = Some(filters);
        self
    }

    /// The routing, in effect from the first message: carried by every
    /// welcome.
    pub fn routing(mut self, routing: Routing) -> Self {
        self.routing = Some(routing);
        self
    }

    /// What a query does with a message it reads that is not its reply,
    /// which is otherwise logged and dropped.
    pub fn on_unexpected(mut self, handler: impl FnMut(Incoming) + Send + 'static) -> Self {
        self.on_unexpected = Some(Box::new(handler));
        self
    }

    /// The client, connected to nothing yet.
    pub fn build(self) -> Result<SyncClient> {
        let mut engine = Engine::new(self.peer_type, IncomingMode::Queue, None)?;
        if let Some(observer) = self.on_drop {
            engine.core.set_drop_observer(Some(observer));
        }
        if let Some(filters) = self.key_filters {
            engine.call(|core| core.set_key_filters(filters, None))?;
        }
        if let Some(routing) = self.routing {
            engine.call(|core| core.set_routing(routing.into(), None));
        }
        Ok(SyncClient {
            engine: Some(engine),
            generation: crate::fork::generation(),
            on_unexpected: self.on_unexpected,
        })
    }

    /// The client, connected to every address at once, `host:port` or
    /// `[address]:port`, each handshake waited for against one
    /// DEFAULT_TIMEOUT; those that did not come up are tried again as the
    /// loop runs. `InvalidArgument` for an address that does not parse, before
    /// anything connects.
    pub fn connect<A: AsRef<str>>(self, addresses: impl IntoIterator<Item = A>) -> Result<SyncClient> {
        let mut client = self.build()?;
        client.connect_all(addresses, Some(DEFAULT_TIMEOUT))?;
        Ok(client)
    }
}

/// How a wait for a message ended. Taken apart as soon as it is returned,
/// as `Result<Incoming>` is: a box would cost an allocation a message.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Waited {
    /// A message, handed out.
    Message(Incoming),
    /// Its deadline, or an acknowledgement for a wait that ends at one.
    Nothing,
    /// A call from another thread asked for the client.
    Asked,
}

/// A client with no thread of its own; see the module comment.
pub struct SyncClient {
    /// Present until a forked child drops the client, which leaks it.
    engine: Option<Engine>,
    generation: u32,
    on_unexpected: Option<Unexpected>,
}

impl std::fmt::Debug for SyncClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("SyncClient").field("engine", &self.engine).finish_non_exhaustive()
    }
}

/// The time a call's `timeout` ends, from now.
fn deadline(timeout: Option<Duration>) -> Option<Instant> {
    deadline_after(Instant::now(), timeout)
}

/// Whether `deadline` has come.
fn passed(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|deadline| Instant::now() >= deadline)
}

/// How a flushing send's wait has ended, looked at between its steps: Ok
/// once a copy was written; at the deadline `OperationTimedOut`, or
/// `NotConnected` when `none_live` says no connection could write it;
/// before the deadline, `NotConnected` once every copy was given up on;
/// None while it goes on. The deadline comes before the copies, so that a
/// step that woke late, the message's expiry run in it, ends as the
/// timeout it is rather than as a drop.
fn flush_ended(copies: &[Arc<Tracker>], deadline_passed: bool, none_live: impl FnOnce() -> bool) -> Option<Result<()>> {
    if copies.iter().any(|copy| copy.get() == SendState::Sent) {
        return Some(Ok(()));
    }
    if deadline_passed {
        return Some(Err(if none_live() { Error::NotConnected } else { Error::OperationTimedOut }));
    }
    if !copies.iter().any(|copy| copy.get() == SendState::Queued) {
        return Some(Err(Error::NotConnected)); // every copy given up on
    }
    None
}

impl SyncClient {
    /// A builder of a client of `peer_type`, a type whose rules entry has
    /// `is_passive`.
    pub fn builder(peer_type: u32) -> SyncClientBuilder {
        SyncClientBuilder { peer_type, on_drop: None, key_filters: None, routing: None, on_unexpected: None }
    }

    /// The engine.
    fn engine(&mut self) -> &mut Engine {
        self.engine.as_mut().expect("present until the client is dropped")
    }

    /// The engine, to read.
    fn engine_ref(&self) -> &Engine {
        self.engine.as_ref().expect("present until the client is dropped")
    }

    /// The fork generation the client was made in, and its sockets: what a
    /// plain server keeps beside its lock, which a forked child may find
    /// held for good.
    pub(crate) fn fork_state(&self) -> (u32, std::sync::Arc<crate::fork::DescriptorTable>) {
        (self.generation, std::sync::Arc::clone(&self.engine_ref().core.client.descriptors))
    }

    /// The counters behind `dropped()` and `incoming_dropped()`: what a
    /// plain server reads without its lock.
    pub(crate) fn drop_counters(&self) -> (Arc<std::sync::atomic::AtomicU64>, Arc<std::sync::atomic::AtomicU64>) {
        let client = &self.engine_ref().core.client;
        (Arc::clone(&client.dropped), Arc::clone(&client.incoming_dropped))
    }

    /// Whether this process inherited the client across a fork.
    pub fn orphaned(&self) -> bool {
        self.generation != crate::fork::generation()
    }

    /// Fails a call on a client inherited across a fork.
    fn check(&self) -> Result<()> {
        if self.orphaned() {
            return Err(Error::UsedAfterFork);
        }
        Ok(())
    }

    /// Fails for a lane of another client, or one inherited across a fork.
    fn check_lane(&self, lane: Option<&Lane>) -> Result<()> {
        let Some(lane) = lane else { return Ok(()) };
        lane.0.check_not_inherited()?;
        if lane.0.owner().is_some_and(|owner| owner != self.engine_ref().core.instance_id) {
            return Err(Error::InvalidArgument(
                "a lane of another client: a connection or lane belongs to its client".to_string(),
            ));
        }
        Ok(())
    }

    /// This client's instance id: the `sender` of what it sends.
    pub fn instance_id(&self) -> u64 {
        self.engine_ref().core.instance_id
    }

    /// This client's peer type.
    pub fn peer_type(&self) -> u32 {
        self.engine_ref().core.peer_type
    }

    /// How many connections are registered.
    pub fn connections_count(&self) -> Result<usize> {
        self.check()?;
        Ok(self.engine_ref().core.connections_count())
    }

    /// How many messages the program sent the client gave up on, each copy
    /// of one sent to ALL; from any thread's view of this client.
    pub fn dropped(&self) -> u64 {
        self.engine_ref().core.client.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many messages that arrived were dropped for a full receive queue.
    pub fn incoming_dropped(&self) -> u64 {
        self.engine_ref().core.client.incoming_dropped.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The messages the connections read after they began closing, which
    /// they could only drop; the protocol's own answers to what this client
    /// sent are not counted.
    pub fn dropped_while_closing(&self) -> u64 {
        self.engine_ref().core.client.dropped_while_closing
    }

    /// A message of `type` with `payload`, its id and sender filled in.
    pub fn new_message(&self, r#type: u32, payload: impl Into<Bytes>) -> Message {
        let core = &self.engine_ref().core;
        Message { id: core.random64(), sender: core.instance_id, r#type, payload: payload.into(), ..Default::default() }
    }

    /// Connects to one more multiplexer, `host:port` or `[address]:port`,
    /// and waits for its handshake, `timeout` at most; returns the
    /// connection, live or not: the library goes on trying. A multiplexer
    /// the client has a connection to, live or on its way, keeps it.
    pub fn connect(&mut self, address: &str, timeout: Option<Duration>) -> Result<Connection> {
        self.check()?;
        let (host, port) = parse_endpoint(address, None)?;
        log::info!("connecting to {}", format_endpoint(&host, port));
        let conn = self.engine().call(|core| core.async_connect(&host, port))?;
        let until = deadline(timeout);
        loop {
            let up = match self.engine_ref().core.conn(conn) {
                None => true,
                Some(state) => state.shuts_down || state.registered,
            };
            if up || passed(until) || self.engine().run_one(until) == 0 {
                break;
            }
        }
        let target = target_of(&host, port);
        Ok(Connection(self.engine_ref().core.wrap_or_gone(conn, &target)))
    }

    /// Connects to every address at once and waits for their handshakes
    /// against one `timeout`; how many are registered when it returns. The
    /// others the library goes on trying. Every address is parsed first.
    pub fn connect_all<A: AsRef<str>>(
        &mut self,
        addresses: impl IntoIterator<Item = A>,
        timeout: Option<Duration>,
    ) -> Result<usize> {
        self.check()?;
        let parsed: Vec<(String, u16)> =
            addresses.into_iter().map(|address| parse_endpoint(address.as_ref(), None)).collect::<Result<_>>()?;
        let mut conns = Vec::new();
        for (host, port) in &parsed {
            log::info!("connecting to {}", format_endpoint(host, *port));
            conns.push(self.engine().call(|core| core.async_connect(host, *port))?);
        }
        let until = deadline(timeout);
        loop {
            let core = &self.engine_ref().core;
            let pending =
                conns.iter().any(|conn| core.conn(*conn).is_some_and(|state| !state.shuts_down && !state.registered));
            if !pending || passed(until) || self.engine().run_one(until) == 0 {
                break;
            }
        }
        let core = &self.engine_ref().core;
        Ok(conns
            .iter()
            .filter(|conn| core.conn(**conn).is_some_and(|state| state.registered && !state.shuts_down))
            .count())
    }

    /// Drops a multiplexer given to `connect()`: no reconnect to it, and a
    /// live connection to it closed the polite way, what it had not written
    /// going to another connection or held. Whether the client had it.
    /// `NotConnected` after `shutdown()`.
    pub fn disconnect(&mut self, address: &str) -> Result<bool> {
        self.check()?;
        let (host, port) = parse_endpoint(address, None)?;
        self.engine().call(|core| core.disconnect(&host, port))
    }

    /// A query: a request of `type` with `payload`, which `wait()` sends,
    /// returning the reply.
    pub fn query(&mut self, r#type: u32, payload: impl Into<Bytes>) -> SyncQuery<'_> {
        let request = Message { r#type, payload: payload.into(), ..Default::default() };
        self.query_message(request)
    }

    /// A query of a whole message, the request itself, typed by its own
    /// `type` and addressed by its own `to`; each attempt gets an id of its
    /// own, an empty sender is filled in.
    pub fn query_message(&mut self, request: Message) -> SyncQuery<'_> {
        SyncQuery { client: self, request, timeout: Some(DEFAULT_TIMEOUT), lane: None, received: None }
    }

    /// An event of `type` with `payload`, which `queue()` or `wait()` sends.
    pub fn send(&mut self, r#type: u32, payload: impl Into<Bytes>) -> SyncSend<'_> {
        let message = Message { r#type, payload: payload.into(), ..Default::default() };
        self.send_message(message)
    }

    /// A whole message, sent as it is, its empty id and sender filled in.
    pub fn send_message(&mut self, message: Message) -> SyncSend<'_> {
        SyncSend { client: self, message, all: false, lane: None, timeout: Some(DEFAULT_TIMEOUT), on_end: None }
    }

    /// The next message that arrived, waiting `timeout` at most, None for
    /// no deadline. `OperationTimedOut` when none came, `NotConnected` at
    /// once for a wait with no deadline when nothing could arrive. Runs the
    /// callbacks of the changes acknowledged meanwhile.
    pub fn receive(&mut self, timeout: Option<Duration>) -> Result<Incoming> {
        self.wait_to_receive(timeout, false)
    }

    /// `receive()` that also ends, with `OperationTimedOut`, as soon as a
    /// change of the routing or the key filters is acknowledged, or the last
    /// message held for a connection goes (`held_for_a_connection()`): what
    /// a loop of receives acts on at once.
    pub fn receive_until_acknowledged(&mut self, timeout: Option<Duration>) -> Result<Incoming> {
        self.wait_to_receive(timeout, true)
    }

    /// The receive's wait (BasicClient::wait_to_receive).
    fn wait_to_receive(&mut self, timeout: Option<Duration>, end_on_acknowledgement: bool) -> Result<Incoming> {
        match self.wait_for_message(deadline(timeout), end_on_acknowledgement, &|| false)? {
            Waited::Message(incoming) => Ok(incoming),
            Waited::Nothing | Waited::Asked => Err(Error::OperationTimedOut),
        }
    }

    /// A plain server loop's wait: `wait_to_receive()` until `until`, None
    /// for no deadline, ending also, with `Waited::Asked`, as soon as
    /// `asked()` is true, which the loop's handle makes so by waking the
    /// wait (`waker()`) when a call from another thread wants the client.
    /// A deadline or an acknowledgement that ended it is `Waited::Nothing`.
    pub(crate) fn wait_for_server(
        &mut self,
        until: Option<Instant>,
        end_on_acknowledgement: bool,
        asked: &dyn Fn() -> bool,
    ) -> Result<Waited> {
        self.wait_for_message(until, end_on_acknowledgement, asked)
    }

    /// What wakes this client's wait from another thread.
    pub(crate) fn waker(&self) -> Arc<mio::Waker> {
        self.engine_ref().driver.waker()
    }

    /// The wait under both: see `wait_to_receive()` and `wait_for_server()`.
    fn wait_for_message(
        &mut self,
        until: Option<Instant>,
        end_on_acknowledgement: bool,
        asked: &dyn Fn() -> bool,
    ) -> Result<Waited> {
        self.check()?;
        loop {
            self.engine().call(|core| core.run_acknowledged_callbacks());
            if let Some(incoming) = self.engine().core.next_incoming_message() {
                return Ok(Waited::Message(incoming.into()));
            }
            // A confirmation that came while messages routed before it were
            // still queued left the change to the receive that took the last
            // of them: told of now, its callbacks run above, so that a serve
            // loop's wait ends at it as at any other.
            self.engine().core.acknowledged_by_receive();
            if end_on_acknowledgement && self.engine().core.take_acknowledged_wake() {
                return Ok(Waited::Nothing);
            }
            if passed(until) {
                break;
            }
            if asked() {
                return Ok(Waited::Asked);
            }
            if self.engine().run_one(until) == 0 {
                return Err(Error::NotConnected);
            }
        }
        self.engine().poll(); // a last look, without waiting
        self.engine().call(|core| core.run_acknowledged_callbacks());
        match self.engine().core.next_incoming_message() {
            Some(incoming) => Ok(Waited::Message(incoming.into())),
            None => Ok(Waited::Nothing),
        }
    }

    /// Runs the loop until everything sent before the call is written or
    /// given up on, its sends' callbacks run, or `timeout` passes: true when
    /// every one was written. False at once when something waits and no
    /// connection is live or coming.
    pub fn flush_all(&mut self, timeout: Option<Duration>) -> Result<bool> {
        self.check()?;
        Ok(self.flush_all_unchecked(timeout))
    }

    /// `flush_all()` with no check, for the shutdown.
    fn flush_all_unchecked(&mut self, timeout: Option<Duration>) -> bool {
        let flush = self.engine().call(|core| core.begin_flush(None));
        let until = deadline(timeout);
        loop {
            let core = &self.engine_ref().core;
            if core.flushed(flush) || passed(until) || !core.connection_live_or_coming() {
                break;
            }
            if self.engine().run_one(until) == 0 {
                break;
            }
        }
        let core = &mut self.engine().core;
        let written = core.all_written(flush);
        core.end_flush(flush);
        written
    }

    /// Which of a multiplexer's routing paths reach this peer: told to every
    /// multiplexer, carried by every welcome from now on.
    pub fn set_routing(&mut self, routing: Routing) -> Result<()> {
        self.check()?;
        self.engine().call(|core| core.set_routing(routing.into(), None));
        Ok(())
    }

    /// `set_routing()`, and `on_acknowledged` called once this change, or a
    /// later one, is acknowledged: inside a receive, the one after the last
    /// message routed before was received, or the one waiting when it came.
    pub fn set_routing_then(
        &mut self,
        routing: Routing,
        on_acknowledged: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        self.check()?;
        self.engine().call(|core| core.set_routing(routing.into(), Some(Box::new(on_acknowledged))));
        Ok(())
    }

    /// Which keys of each message type reach this peer by the rules,
    /// replacing the filters set before: told and carried as the routing is.
    /// `InvalidArgument` for a protocol message type or a pattern over
    /// MAX_KEY_PATTERN_SIZE.
    pub fn set_key_filters(&mut self, filters: KeyFilters) -> Result<()> {
        self.check()?;
        self.engine().call(|core| core.set_key_filters(filters, None))
    }

    /// `set_key_filters()`, and `on_acknowledged` as for `set_routing_then()`.
    pub fn set_key_filters_then(
        &mut self,
        filters: KeyFilters,
        on_acknowledged: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        self.check()?;
        self.engine().call(|core| core.set_key_filters(filters, Some(Box::new(on_acknowledged))))
    }

    /// Whether every multiplexer has the current routing and key filters in
    /// effect, with everything they routed before received.
    pub fn routing_acknowledged(&self) -> Result<bool> {
        self.check()?;
        Ok(self.engine_ref().core.routing_acknowledged())
    }

    /// How many messages wait for a connection to come up: sent while none
    /// was live, or left by one that died with none to take them, each
    /// going out in order once one is registered, or given up on at its own
    /// deadline. What the server classes' drains wait for, the close
    /// dropping them.
    pub fn held_for_a_connection(&self) -> Result<usize> {
        self.check()?;
        Ok(self.engine_ref().core.held_for_a_connection())
    }

    /// Writes what was sent before, running the loop as `flush_all()` does,
    /// `timeout` at most, None for no deadline, nothing more for zero; then
    /// closes every connection the polite way, returning once every
    /// multiplexer closed its side too, a second at most. What is still
    /// unwritten is dropped and reported. Idempotent; dropping the client
    /// does it with CLOSE_FLUSH_SECONDS.
    pub fn shutdown(&mut self, timeout: Option<Duration>) {
        if self.orphaned() {
            if let Some(engine) = &self.engine {
                engine.core.client.descriptors.close_all();
            }
            return;
        }
        if self.engine_ref().core.client.shuts_down {
            return;
        }
        if timeout != Some(Duration::ZERO) {
            // What the loop sends meanwhile is written too: a flush again
            // while anything newer was sent.
            let until = deadline(timeout);
            loop {
                let sent = self.engine_ref().core.outbox.last_number();
                let left = until.map(|until| until.saturating_duration_since(Instant::now()));
                if left == Some(Duration::ZERO) {
                    break;
                }
                self.flush_all_unchecked(left);
                if self.engine_ref().core.outbox.last_number() == sent {
                    break;
                }
            }
        }
        self.engine().call(|core| core.shutdown_client());
        let until = Some(Instant::now() + CLOSE_READ_SECONDS + Duration::from_millis(500));
        while self.engine().core.closing() && !passed(until) {
            self.engine().run_one(until);
        }
        self.engine().poll();
        self.engine().call(|core| core.end_follows());
    }

    /// Leaving, for the server classes: what arrives from now on is
    /// refused, a request with the DELIVERY_ERROR a multiplexer sends for a
    /// peer that is gone, a reply dropped; what was read before stays.
    pub(crate) fn refuse_arrivals(&mut self) {
        self.engine().call(|core| core.refuse_arrivals());
    }

    /// What was read and not received yet, refused or dropped by the same
    /// rule.
    pub(crate) fn refuse_unread(&mut self) {
        self.engine().call(|core| core.refuse_unread());
    }

    /// Whether messages were read and not received yet.
    pub(crate) fn has_incoming(&self) -> bool {
        !self.engine_ref().core.client.incoming.is_empty()
    }

    /// The messages a query read that are not its reply: to `on_unexpected`,
    /// or logged and dropped; a REQUEST_RECEIVED nobody here waits for goes
    /// silently.
    fn drop_unexpected(&mut self, query: QueryId) {
        while let Some(incoming) = self.engine().core.next_incoming_message() {
            if incoming.message.r#type == types::REQUEST_RECEIVED {
                continue;
            }
            if let Some(handler) = self.on_unexpected.as_mut() {
                let incoming = Incoming::from(incoming);
                crate::core::client::guarded(|| "on_unexpected".to_string(), || handler(incoming));
                continue;
            }
            let message = &incoming.message;
            log::warn!(
                "message (id={}, type={}, sender={}, references={}) while waiting for reply for {:?}",
                message.id.unwrap_or(0),
                message.r#type,
                message.sender.unwrap_or(0),
                message.references.unwrap_or(0),
                self.engine_ref().core.queries.attempt_ids(query)
            );
        }
    }

    /// A message as a frame to send: its empty id and sender filled in, its
    /// size and key checked.
    fn frame_of(&self, message: Message) -> Result<Frame> {
        let core = &self.engine_ref().core;
        crate::message::frame_of(message, || core.random64(), core.instance_id)
    }
}

impl Drop for SyncClient {
    /// `shutdown()` with CLOSE_FLUSH_SECONDS; in a process that inherited
    /// the client across a fork, the child's copies of its sockets closed
    /// and the rest leaked, a thread of the parent's having held it.
    fn drop(&mut self) {
        if self.orphaned() {
            if let Some(engine) = self.engine.take() {
                engine.core.client.descriptors.close_all();
                std::mem::forget(engine);
            }
            return;
        }
        self.shutdown(Some(CLOSE_FLUSH_SECONDS));
    }
}

/// A query being built; `wait()` sends it. Every option has a default: a
/// DEFAULT_TIMEOUT per stage, no lane.
#[must_use = "a query is sent by wait()"]
pub struct SyncQuery<'client> {
    client: &'client mut SyncClient,
    request: Message,
    timeout: Option<Duration>,
    lane: Option<Lane>,
    received: Option<Box<dyn FnMut(u64) + Send>>,
}

impl SyncQuery<'_> {
    /// Each stage's time, one deadline for an addressed query's three.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// No deadline for any stage.
    pub fn no_deadline(mut self) -> Self {
        self.timeout = None;
        self
    }

    /// Addressed to one instance: only it ever gets the request.
    pub fn to(mut self, instance: u64) -> Self {
        self.request.to = Some(instance);
        self
    }

    /// The request's key; its search for a backend carries it too.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.request.key = Some(key.into());
        self
    }

    /// Through `lane`'s connection, which then takes the connection the
    /// reply came through.
    pub fn lane(mut self, lane: &Lane) -> Self {
        self.lane = Some(lane.clone());
        self
    }

    /// Through `connection` while it is live, another when it is gone.
    pub fn connection(mut self, connection: &Connection) -> Self {
        self.lane = Some(Lane::through(connection, false));
        self
    }

    /// `received` hears the instance id of each backend that acknowledges
    /// an attempt with REQUEST_RECEIVED.
    pub fn on_received(mut self, received: impl FnMut(u64) + Send + 'static) -> Self {
        self.received = Some(Box::new(received));
        self
    }

    /// Sends the request and waits for the reply, by the stages
    /// docs/query.md draws. `OperationFailed` when no backend can be found,
    /// `OperationTimedOut` when a stage runs out of time, `NotConnected`
    /// when no connection is live, `Backend` for a BACKEND_ERROR reply,
    /// `TooLarge` before anything is sent.
    pub fn wait(self) -> Result<Incoming> {
        let SyncQuery { client, request, timeout, lane, received } = self;
        client.check()?;
        client.check_lane(lane.as_ref())?;
        let mut prototype = proto::MultiplexerMessage::from(request);
        prototype.id = None;
        check_size_sent_again(&mut prototype, client.instance_id())?;
        client.engine().poll(); // retire what the multiplexers closed while the client was idle
        let outcome: Arc<Mutex<Option<Outcome>>> = Arc::default();
        let ended = Arc::clone(&outcome);
        let done =
            Box::new(move |outcome| *ended.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(outcome));
        let query =
            client.engine().call(|core| core.start_query(prototype, timeout, lane.map(|lane| lane.0), done, received));
        loop {
            if let Some(outcome) = outcome.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).take() {
                let reply = outcome.into_result().map(Incoming::from)?;
                if reply.message.r#type == types::BACKEND_ERROR {
                    return Err(Error::Backend(Box::new(reply)));
                }
                return Ok(reply);
            }
            client.drop_unexpected(query);
            let core = &client.engine_ref().core;
            // Nothing could write a stage's message: NotConnected now, where a
            // wait with nothing coming would last the whole stage.
            let nothing_coming = core.queries.waiting_for_connection(query) && !core.connection_live_or_coming();
            if nothing_coming || client.engine().run_one(None) == 0 {
                client.engine().call(|core| core.finish(query, Outcome::NotConnected));
            }
        }
    }
}

/// A send being built; `queue()` or `wait()` sends it. Every option has a
/// default: one connection, round robin, DEFAULT_TIMEOUT to wait for room
/// or for a connection.
#[must_use = "a send goes by queue() or wait()"]
pub struct SyncSend<'client> {
    client: &'client mut SyncClient,
    message: Message,
    all: bool,
    lane: Option<Lane>,
    timeout: Option<Duration>,
    on_end: Option<Box<dyn FnOnce(bool) + Send>>,
}

impl SyncSend<'_> {
    /// Addressed to one instance.
    pub fn to(mut self, instance: u64) -> Self {
        self.message.to = Some(instance);
        self
    }

    /// The message's key.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.message.key = Some(key.into());
        self
    }

    /// The id of the message this one answers.
    pub fn references(mut self, id: u64) -> Self {
        self.message.references = id;
        self
    }

    /// The work flow, a trace or request id, text, which a reply copies
    /// from its request.
    pub fn workflow(mut self, workflow: impl Into<String>) -> Self {
        self.message.workflow = Some(workflow.into());
        self
    }

    /// Through every connection, and a multiplexer whose first connection
    /// is still in its handshake at its welcome; the receivers drop the
    /// copies.
    pub fn all(mut self) -> Self {
        self.all = true;
        self
    }

    /// Through `lane`'s connection.
    pub fn lane(mut self, lane: &Lane) -> Self {
        self.lane = Some(lane.clone());
        self
    }

    /// Through `connection` while it is live, another when it is gone.
    pub fn connection(mut self, connection: &Connection) -> Self {
        self.lane = Some(Lane::through(connection, false));
        self
    }

    /// How long the message may wait for room or for a connection; for a
    /// flushing send, how long the call waits.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// No deadline.
    pub fn no_deadline(mut self) -> Self {
        self.timeout = None;
        self
    }

    /// `on_end` hears how the message ended, once, inside a later call that
    /// runs the loop: true once it was written, the first copy for ALL,
    /// false once it was given up on or the client shut down first.
    pub fn on_end(mut self, on_end: impl FnOnce(bool) + Send + 'static) -> Self {
        self.on_end = Some(Box::new(on_end));
        self
    }

    /// Sends the message and returns its id at once: it is queued on a
    /// connection, waits for room, or, with none live, is held until one
    /// comes up, the timeout at most, and is written as the loop runs.
    /// `NotConnected` for a pinned lane whose connection is gone, or after
    /// `shutdown()`; `TooLarge` before anything is queued.
    pub fn queue(self) -> Result<u64> {
        let SyncSend { client, message, all, lane, timeout, on_end } = self;
        client.check()?;
        client.check_lane(lane.as_ref())?;
        if lane.as_ref().is_some_and(Lane::closed) {
            return Err(Error::NotConnected);
        }
        let frame = client.frame_of(message)?;
        let id = frame.id;
        client.engine().poll();
        let done = on_end.map(|on_end| SendDone::Callback(Box::new(move |written| on_end(written > 0))));
        let lane = lane.map(|lane| lane.0);
        let taken = client.engine().call(|core| core.send(frame, all, lane.as_ref(), timeout, 0, None, None, done));
        if !taken {
            return Err(Error::NotConnected);
        }
        Ok(id)
    }

    /// Sends the message and waits until it reached a socket, the first
    /// copy for ALL, the timeout at most; returns its id. With no connection
    /// live the message is held until one comes up, and a connection that
    /// dies with it unwritten hands it on. `NotConnected` when nothing wrote
    /// it with no connection live, or through a pinned lane whose connection
    /// is gone; `OperationTimedOut` otherwise.
    pub fn wait(self) -> Result<u64> {
        let SyncSend { client, message, all, lane, timeout, on_end } = self;
        client.check()?;
        client.check_lane(lane.as_ref())?;
        if lane.as_ref().is_some_and(Lane::closed) {
            return Err(Error::NotConnected);
        }
        let frame = client.frame_of(message)?;
        let id = frame.id;
        let until = deadline(timeout);
        client.engine().poll();
        // The message waits a moment past the call's deadline, so that the
        // call times out first rather than see its message dropped; past
        // MAX_TIMEOUT that is no deadline, Duration::MAX included.
        let room = timeout.map(|timeout| timeout.saturating_add(ROOM_GRACE_SECONDS));
        let done = on_end.map(|on_end| SendDone::Callback(Box::new(move |written| on_end(written > 0))));
        let core_lane = lane.as_ref().map(|lane| lane.0.clone());
        let mut copies = Vec::new();
        let taken = client
            .engine()
            .call(|core| core.send(frame, all, core_lane.as_ref(), room, 0, Some(&mut copies), None, done));
        if !taken {
            return Err(Error::NotConnected);
        }
        loop {
            let none_live =
                || client.engine_ref().core.connections_count() == 0 || lane.as_ref().is_some_and(Lane::closed);
            if let Some(ended) = flush_ended(&copies, passed(until), none_live) {
                return ended.map(|()| id);
            }
            if !client.engine_ref().core.connection_live_or_coming() {
                // Nothing could write it, however long the call waited: given
                // up on now, as it would be at the call's deadline.
                client.engine().call(|core| {
                    for copy in &copies {
                        core.drop_held(copy);
                    }
                });
                return Err(Error::NotConnected);
            }
            client.engine().run_one(until);
        }
    }
}

#[cfg(test)]
mod tests {
    //! How a flushing send's wait ends, by what its copies read.

    use super::flush_ended;
    use crate::core::{SendState, Tracker};
    use crate::error::Error;

    #[test]
    fn a_copy_given_up_on_past_the_deadline_with_a_connection_live_is_a_timeout() {
        // A step that woke late ran the message's expiry, which gave the
        // copy up for want of room before the wait looked: the call timed
        // out, a connection being live.
        let lost = [Tracker::new(SendState::Lost)];
        assert_eq!(Some(Err(Error::OperationTimedOut)), flush_ended(&lost, true, || false));
        assert_eq!(Some(Err(Error::NotConnected)), flush_ended(&lost, true, || true), "none live");
        assert_eq!(Some(Err(Error::NotConnected)), flush_ended(&lost, false, || false), "given up on before it");
        assert_eq!(None, flush_ended(&[Tracker::new(SendState::Queued)], false, || false));
        assert_eq!(Some(Ok(())), flush_ended(&[Tracker::new(SendState::Sent)], true, || true));
    }
}
