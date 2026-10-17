//! `BaseMultiplexerServer`: a backend whose loop and handler share one
//! thread, the fastest of the two server classes. Mirrors
//! multiplexer/backend/base_multiplexer_server.cc. `serve_forever()` blocks
//! in its client's receive, so heartbeats and reconnects happen while it
//! waits and the peer type need not be passive, and handles one message at
//! a time: a handler that takes longer than the multiplexer's drop
//! interval (90 s) gets the backend dropped, which
//! `BaseThreadedMultiplexerServer` avoids.
//!
//! The protocol's own messages it answers itself: a client's search for a
//! backend, when the handler's `should_answer_search()` says yes, and a
//! PING, each with a PING carrying its payload back; a DELIVERY_ERROR for
//! one of its own messages it keeps to itself, logged. After every poll,
//! message or not, and at once when a change of the routing or the key
//! filters is acknowledged, `periodic_task()` runs.
//!
//! The client lives behind a mutex, which the loop holds while it waits
//! for a message, and a reply or a handle's call while it is queued: a
//! handler, on the loop's thread, reaches the client between two waits. A
//! call from another thread, a handle's or a kept request's reply, that
//! finds the loop waiting asks for a turn: it wakes the wait, which lets go
//! of the client and takes it again only once every call that asked had
//! it, so that such a call waits for no poll and loses no race to a loop
//! that takes the mutex again at once. The client's callbacks, a change's
//! acknowledgement and the drop observer, run on the loop's thread after
//! its wait, not inside it, where the loop holds the client: they may call
//! the handle and answer kept requests.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant};

use super::{as_reply, handle, nothing_more_arrives, Controlled, Handler, Replier, Request, ServeError, ServerHandle};
use crate::constants::types;
use crate::core::client::{guarded, DropObserver, DropReason};
use crate::defaults::{CLOSE_FLUSH_SECONDS, DEFAULT_TIMEOUT};
use crate::endpoint::parse_endpoint;
use crate::error::{Error, Result};
use crate::fork::DescriptorTable;
use crate::handles::Incoming;
use crate::message::{KeyFilters, Message, Routing};
use crate::sync_client::{SyncClient, Waited};

/// The lock of `mutex`, which nothing panics under.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// What the client's callbacks leave to the loop, to run after its wait.
enum Deferred {
    /// A change's callback, its acknowledgement come.
    Acknowledged(Box<dyn FnOnce() + Send>),
    /// A message the server gave up on, for its drop observer.
    Dropped(u64, DropReason),
}

/// The callbacks waiting for the loop, with a flag it reads after every
/// wait without the lock.
#[derive(Default)]
struct Deferrals {
    items: Mutex<Vec<Deferred>>,
    waiting: AtomicBool,
}

impl Deferrals {
    fn push(&self, item: Deferred) {
        lock(&self.items).push(item);
        self.waiting.store(true, Ordering::Release);
    }

    fn take(&self) -> Vec<Deferred> {
        if !self.waiting.load(Ordering::Acquire) || !self.waiting.swap(false, Ordering::AcqRel) {
            return Vec::new();
        }
        std::mem::take(&mut *lock(&self.items))
    }
}

/// What the server and its handles share.
pub(crate) struct Shared {
    client: Mutex<SyncClient>,
    /// The fork generation the client was made in, and its sockets, read
    /// without the client's lock: in a forked child a thread of the
    /// parent's may hold that lock for good.
    generation: u32,
    descriptors: Arc<DescriptorTable>,
    /// What wakes the loop's wait, and how many calls from other threads
    /// asked for a turn with the client; the loop waits on `turned` until
    /// each had it.
    waker: Arc<mio::Waker>,
    asking: AtomicUsize,
    turns: Mutex<()>,
    turned: Condvar,
    deferred: Arc<Deferrals>,
    on_drop: Mutex<Option<DropObserver>>,
    pub(crate) working: AtomicBool,
    drain_asked: AtomicBool,
    pub(crate) draining: AtomicBool,
    draining_since: Mutex<Option<Instant>>,
    drain_routing: Routing,
    pub(crate) instance_id: u64,
    /// The client's counts of what it sent and gave up on, and of what
    /// arrived and its full receive queue dropped, read without its lock.
    dropped: Arc<AtomicU64>,
    pub(crate) incoming_dropped: Arc<AtomicU64>,
}

/// A call's turn with the client: what `Shared::client()` gives, holding
/// the client until it is dropped.
pub(crate) struct Turn<'a> {
    guard: Option<MutexGuard<'a, SyncClient>>,
    shared: &'a Shared,
    asked: bool,
}

impl Deref for Turn<'_> {
    type Target = SyncClient;

    fn deref(&self) -> &SyncClient {
        self.guard.as_deref().expect("held until the turn ends")
    }
}

impl DerefMut for Turn<'_> {
    fn deref_mut(&mut self) -> &mut SyncClient {
        self.guard.as_deref_mut().expect("held until the turn ends")
    }
}

impl Drop for Turn<'_> {
    /// Lets go of the client first; the last call that asked for a turn
    /// then gives the loop its own back.
    fn drop(&mut self) {
        self.guard.take();
        if self.asked {
            let _turns = lock(&self.shared.turns);
            if self.shared.asking.fetch_sub(1, Ordering::SeqCst) == 1 {
                self.shared.turned.notify_all();
            }
        }
    }
}

impl Shared {
    /// The client, for the loop's own calls.
    pub(crate) fn lock(&self) -> MutexGuard<'_, SyncClient> {
        lock(&self.client)
    }

    /// Whether this process inherited the server across a fork: one load,
    /// before any lock.
    pub(crate) fn orphaned(&self) -> bool {
        self.generation != crate::fork::generation()
    }

    /// The client, for a call from a handle or a kept request, from any
    /// thread: `UsedAfterFork` in a forked child, before the lock. Free, it
    /// is taken at once, as a handler on the loop's thread always finds it;
    /// held by the loop's wait, the call asks for a turn and wakes the
    /// wait, which lets go of it.
    pub(crate) fn client(&self) -> Result<Turn<'_>> {
        if self.orphaned() {
            return Err(Error::UsedAfterFork);
        }
        match self.client.try_lock() {
            Ok(guard) => return Ok(Turn { guard: Some(guard), shared: self, asked: false }),
            Err(TryLockError::Poisoned(poisoned)) => {
                return Ok(Turn { guard: Some(poisoned.into_inner()), shared: self, asked: false })
            }
            Err(TryLockError::WouldBlock) => {}
        }
        self.asking.fetch_add(1, Ordering::SeqCst);
        let _ = self.waker.wake();
        Ok(Turn { guard: Some(self.lock()), shared: self, asked: true })
    }

    /// Whether the loop's wait should end before its time: a call asked for
    /// the client, or `stop()` was called.
    fn asked(&self) -> bool {
        self.asking.load(Ordering::Relaxed) != 0 || !self.working.load(Ordering::Relaxed)
    }

    /// The loop, before it takes the client again: every call that asked
    /// for a turn has it first.
    fn yield_to_callers(&self) {
        if self.asking.load(Ordering::SeqCst) == 0 {
            return;
        }
        let mut turns = lock(&self.turns);
        while self.asking.load(Ordering::SeqCst) != 0 {
            turns = self.turned.wait(turns).unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// `serve_forever()` asked to return, its wait woken.
    pub(crate) fn stop(&self) {
        self.working.store(false, Ordering::SeqCst);
        let _ = self.waker.wake();
    }

    /// `on_acknowledged`, to run on the loop's thread after its wait.
    pub(crate) fn deferred(&self, on_acknowledged: impl FnOnce() + Send + 'static) -> impl FnOnce() + Send + 'static {
        let deferred = Arc::clone(&self.deferred);
        move || deferred.push(Deferred::Acknowledged(Box::new(on_acknowledged)))
    }

    /// Starts the drain, once: the drain routing told to every
    /// multiplexer, and only then the drain published, from that time, so
    /// that the loop never takes the routing in effect before for the
    /// drain's. Nothing in a forked child.
    pub(crate) fn start_draining(&self) {
        if self.orphaned() || self.drain_asked.swap(true, Ordering::SeqCst) {
            return;
        }
        let Ok(mut client) = self.client() else { return };
        let _ = client.set_routing(self.drain_routing);
        *lock(&self.draining_since) = Some(Instant::now());
        self.draining.store(true, Ordering::SeqCst);
    }
}

/// Builds a `BaseMultiplexerServer`; see `BaseMultiplexerServer::builder`.
#[must_use = "a builder does nothing until build()"]
pub struct BaseMultiplexerServerBuilder {
    peer_type: u32,
    addresses: Vec<String>,
    drain_routing: Routing,
    key_filters: Option<KeyFilters>,
    on_drop: Option<Box<dyn FnMut(u64, DropReason) + Send>>,
}

impl BaseMultiplexerServerBuilder {
    /// What `start_draining()` tells the multiplexers: by default nothing
    /// new by the rules; `all` on keeps events coming, `last_resort` keeps
    /// a lone backend serving through its drain.
    pub fn drain_routing(mut self, routing: Routing) -> Self {
        self.drain_routing = routing;
        self
    }

    /// The key filters, in effect from the first request.
    pub fn key_filters(mut self, filters: KeyFilters) -> Self {
        self.key_filters = Some(filters);
        self
    }

    /// Every message the server sent and gave up on is told to `observer`,
    /// on the loop's thread after its wait, where it may use the handle, or
    /// in `close()`.
    pub fn on_drop(mut self, observer: impl FnMut(u64, DropReason) + Send + 'static) -> Self {
        self.on_drop = Some(Box::new(observer));
        self
    }

    /// The server, serving `handler`, connected to nothing until
    /// `connect()` or `serve_forever()`. `InvalidArgument` for an address
    /// that does not parse.
    pub fn build<H: Handler>(self, handler: H) -> Result<BaseMultiplexerServer<H>> {
        for address in &self.addresses {
            parse_endpoint(address, None)?;
        }
        let mut builder = SyncClient::builder(self.peer_type);
        if let Some(filters) = self.key_filters {
            builder = builder.key_filters(filters);
        }
        // The client only notes a drop, wherever it is told of it; the loop
        // tells the program's observer, after its wait.
        let deferred = Arc::new(Deferrals::default());
        if self.on_drop.is_some() {
            let noted = Arc::clone(&deferred);
            builder = builder.on_drop(move |id, reason| noted.push(Deferred::Dropped(id, reason)));
        }
        let client = builder.build()?;
        let instance_id = client.instance_id();
        let (generation, descriptors) = client.fork_state();
        let (dropped, incoming_dropped) = client.drop_counters();
        let waker = client.waker();
        let shared = Arc::new(Shared {
            client: Mutex::new(client),
            generation,
            descriptors,
            waker,
            asking: AtomicUsize::new(0),
            turns: Mutex::new(()),
            turned: Condvar::new(),
            deferred,
            on_drop: Mutex::new(self.on_drop),
            working: AtomicBool::new(true),
            drain_asked: AtomicBool::new(false),
            draining: AtomicBool::new(false),
            draining_since: Mutex::new(None),
            drain_routing: self.drain_routing,
            instance_id,
            dropped,
            incoming_dropped,
        });
        Ok(BaseMultiplexerServer {
            handler,
            shared,
            addresses: self.addresses,
            connected: false,
            drain: None,
            closed: false,
            dropped_while_closing: 0,
        })
    }
}

/// A backend on one thread; see the module comment.
pub struct BaseMultiplexerServer<H: Handler> {
    handler: H,
    shared: Arc<Shared>,
    addresses: Vec<String>,
    connected: bool,
    drain: Option<Duration>,
    closed: bool,
    dropped_while_closing: u64,
}

impl<H: Handler> std::fmt::Debug for BaseMultiplexerServer<H> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BaseMultiplexerServer")
            .field("instance_id", &self.shared.instance_id)
            .finish_non_exhaustive()
    }
}

impl BaseMultiplexerServer<Box<dyn Handler>> {
    /// A builder of a backend of `peer_type` for the multiplexers at
    /// `addresses`, `host:port` or `[address]:port` each.
    pub fn builder<A: AsRef<str>>(
        peer_type: u32,
        addresses: impl IntoIterator<Item = A>,
    ) -> BaseMultiplexerServerBuilder {
        BaseMultiplexerServerBuilder {
            peer_type,
            addresses: addresses.into_iter().map(|address| address.as_ref().to_string()).collect(),
            drain_routing: super::direct_only_routing(),
            key_filters: None,
            on_drop: None,
        }
    }
}

impl<H: Handler> BaseMultiplexerServer<H> {
    /// A handle on the server, for `stop()` and `start_draining()` from any
    /// thread.
    pub fn handle(&self) -> ServerHandle {
        ServerHandle(Controlled::Plain(Arc::clone(&self.shared)))
    }

    /// The handler.
    pub fn handler(&self) -> &H {
        &self.handler
    }

    /// The server's instance id.
    pub fn instance_id(&self) -> u64 {
        self.shared.instance_id
    }

    /// How many messages the server sent and its client gave up on,
    /// replies included, each copy of one sent to ALL, as the clients'
    /// `dropped()` counts; from any thread's view, kept past `close()`.
    pub fn dropped(&self) -> u64 {
        self.shared.dropped.load(Ordering::Relaxed)
    }

    /// How many messages that arrived were dropped for a full receive
    /// queue, as `SyncClient::incoming_dropped()` counts: the requests the
    /// server lost to a queue that held too many; kept past `close()`.
    pub fn incoming_dropped(&self) -> u64 {
        self.shared.incoming_dropped.load(Ordering::Relaxed)
    }

    /// The requests that reached the server while it closed, and went
    /// unanswered; kept past `close()`.
    pub fn dropped_while_closing(&self) -> u64 {
        if self.closed || self.shared.orphaned() {
            return self.dropped_while_closing;
        }
        self.shared.lock().dropped_while_closing()
    }

    /// Connects to every address, once, waiting for every handshake against
    /// one DEFAULT_TIMEOUT: how many came up. For a program that prints a
    /// line others wait for before it serves; `serve_forever()` otherwise
    /// starts the connections itself, waiting for none.
    pub fn connect(&mut self) -> Result<usize> {
        if self.connected {
            return Ok(0);
        }
        self.connected = true;
        self.shared.client()?.connect_all(&self.addresses, Some(DEFAULT_TIMEOUT))
    }

    /// The loop: a connection to every address started unless `connect()`
    /// did, none waited for; then, until `stop()` or the drain is over, a
    /// message waited for, `poll` at most, and handled, and
    /// `periodic_task()`; then what was read is handled, what arrives
    /// refused, and the connections closed. `drain` caps a drain, None for
    /// no cap: while a drain with a cap runs, a wait lasts no longer than
    /// what is left of it, so that the cap ends the drain on time whatever
    /// the poll. Returns the error of a handler whose `on_handler_error()`
    /// said to end, or the client's.
    pub fn serve_forever(&mut self, poll: Duration, drain: Option<Duration>) -> std::result::Result<(), ServeError> {
        if self.shared.orphaned() {
            return Err(ServeError::Client(Error::UsedAfterFork));
        }
        self.drain = drain;
        // A panic out of the handler's callbacks closes the server first,
        // as C++ and Python close it, and then goes on up.
        let served = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.serve(poll)));
        self.close(Some(CLOSE_FLUSH_SECONDS));
        served.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }

    /// One step: a message waited for, `timeout` at most, None for no
    /// deadline, or until `stop()`, and handled. `OperationTimedOut` when
    /// none came.
    pub fn loop_iter(&mut self, timeout: Option<Duration>) -> std::result::Result<(), ServeError> {
        if self.shared.orphaned() {
            return Err(ServeError::Client(Error::UsedAfterFork));
        }
        let received = self.wait(crate::core::deadline_after(Instant::now(), timeout), false);
        self.run_deferred();
        let Some(incoming) = received? else {
            return Err(ServeError::Client(Error::OperationTimedOut));
        };
        match self.handle_received(incoming) {
            Some(error) => Err(ServeError::Handler(error)),
            None => Ok(()),
        }
    }

    /// The loop's wait for a message, until `until`, None for no deadline,
    /// or an acknowledgement when `end_on_acknowledgement`, or `stop()`:
    /// the client held meanwhile, and let go of whenever a call from
    /// another thread asks for it, the wait going on once each such call
    /// had its turn. None for no message.
    fn wait(&mut self, until: Option<Instant>, end_on_acknowledgement: bool) -> Result<Option<Incoming>> {
        let shared = &*self.shared;
        loop {
            shared.yield_to_callers();
            if !shared.working.load(Ordering::SeqCst) {
                return Ok(None);
            }
            match shared.lock().wait_for_server(until, end_on_acknowledgement, &|| shared.asked())? {
                Waited::Message(incoming) => return Ok(Some(incoming)),
                Waited::Nothing => return Ok(None),
                Waited::Asked => {}
            }
        }
    }

    /// What the client's callbacks left to the loop: each acknowledged
    /// change's callback and each drop for the observer, on this thread,
    /// without the client's lock.
    fn run_deferred(&mut self) {
        for item in self.shared.deferred.take() {
            match item {
                Deferred::Acknowledged(callback) => {
                    guarded(|| "the callback of an acknowledged change".to_string(), callback);
                }
                Deferred::Dropped(id, reason) => {
                    if let Some(observer) = lock(&self.shared.on_drop).as_mut() {
                        guarded(|| "the drop observer".to_string(), || observer(id, reason));
                    }
                }
            }
        }
    }

    /// `serve_forever()` but the close.
    fn serve(&mut self, poll: Duration) -> std::result::Result<(), ServeError> {
        if !self.connected {
            self.connected = true;
            self.shared.lock().connect_all(&self.addresses, Some(Duration::ZERO))?;
        }
        let handle = self.handle();
        while self.shared.working.load(Ordering::SeqCst) {
            if self.shared.draining.load(Ordering::SeqCst) && self.drained() {
                break;
            }
            // An acknowledgement ends the wait too, so that a drain is over,
            // and periodic_task() hears of a change, as soon as every
            // multiplexer has it; its callback runs first. So does the last
            // message held for a connection going.
            let received = self.wait(self.next_wait(poll), true);
            self.run_deferred();
            match received {
                Ok(Some(incoming)) => {
                    if let Some(error) = self.handle_received(incoming) {
                        return Err(ServeError::Handler(error));
                    }
                }
                Ok(None) => {}
                Err(error) => return Err(ServeError::Client(error)),
            }
            self.handler.periodic_task(&handle);
        }
        // The drain is over: what arrives from now on is refused, so that
        // its sender retries elsewhere, and what was read already is
        // handled first, a number fixed here however fast more comes.
        if self.shared.draining.load(Ordering::SeqCst) && self.shared.working.load(Ordering::SeqCst) {
            self.shared.lock().refuse_arrivals();
            while self.shared.working.load(Ordering::SeqCst) && self.shared.lock().has_incoming() {
                let received = self.shared.lock().receive(Some(Duration::ZERO));
                self.run_deferred();
                let Ok(incoming) = received else { break };
                if let Some(error) = self.handle_received(incoming) {
                    return Err(ServeError::Handler(error));
                }
            }
        }
        Ok(())
    }

    /// Whether the drain is over: the handler's word on the library's
    /// answer, which is the drain's cap passed, or, with every path off and
    /// no last resort, every multiplexer having confirmed it and nothing
    /// waiting for a connection: with no connection live the confirmation
    /// holds at once, so what is held for the next connection is waited for
    /// too.
    fn drained(&self) -> bool {
        let since = *self.shared.draining_since.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let past_cap = match (since, self.drain) {
            (Some(since), Some(cap)) => since.elapsed() >= cap,
            _ => false,
        };
        let by_default = past_cap
            || (nothing_more_arrives(&self.shared.drain_routing) && {
                let mut client = self.shared.lock();
                // What waits in a batch goes first, a flush that waits for
                // nothing, to be held with the rest; its loop runs first, so
                // that a connection that died since is gone before the
                // confirmation is asked, the handler's sends into a batch
                // having run none.
                let _ = client.flush_all(Some(Duration::ZERO));
                client.routing_acknowledged().unwrap_or(true) && client.held_for_a_connection().unwrap_or(0) == 0
            });
        self.handler.drained(by_default)
    }

    /// When the loop's next wait ends at the latest: `poll` from now, None
    /// for no deadline, and while a drain with a cap runs, no later than
    /// the cap.
    fn next_wait(&self, poll: Duration) -> Option<Instant> {
        let until = crate::core::deadline_after(Instant::now(), Some(poll));
        if !self.shared.draining.load(Ordering::SeqCst) {
            return until; // no drain: the poll alone, and no lock taken per message
        }
        let since = *self.shared.draining_since.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let cap = match (since, self.drain) {
            (Some(since), Some(cap)) => since.checked_add(cap),
            _ => None,
        };
        match (until, cap) {
            (Some(until), Some(cap)) => Some(until.min(cap)),
            (until, None) => until,
            (None, cap) => cap,
        }
    }

    /// One message: the protocol's own answered here, the rest to the
    /// handler. The handler's failure when the server ends for it.
    fn handle_received(&mut self, incoming: Incoming) -> Option<super::HandlerError> {
        if incoming.message.r#type <= types::MAX_MULTIPLEXER_META_PACKET {
            self.handle_internal(incoming);
            return None;
        }
        let request = Request::new(incoming, Replier::Sync(Arc::clone(&self.shared)));
        handle(&self.handler, request, "BaseMultiplexerServer")
    }

    /// The protocol's messages the server answers itself: a search, when
    /// the handler says yes, and a PING without references, each with a
    /// PING carrying its payload back; a DELIVERY_ERROR for a message of
    /// its own it keeps to itself.
    fn handle_internal(&mut self, incoming: Incoming) {
        let message = &incoming.message;
        match message.r#type {
            types::BACKEND_FOR_PACKET_SEARCH => {
                if self.handler.should_answer_search() {
                    self.echo(&incoming, "search");
                }
            }
            types::PING => {
                if message.references == 0 {
                    self.echo(&incoming, "PING");
                }
            }
            types::DELIVERY_ERROR => {
                log::debug!("message #{} went nowhere: a DELIVERY_ERROR, kept here", message.references);
            }
            other => log::error!("received unknown meta-packet type={other}"),
        }
    }

    /// The echo of a PING or a search, a reply carrying its payload back;
    /// BACKEND_ERROR saying so when it would be over MAX_MESSAGE_SIZE.
    fn echo(&self, incoming: &Incoming, what: &str) {
        let replier = Replier::Sync(Arc::clone(&self.shared));
        let sent = match replier.send(echo_of(&incoming.message), &incoming.connection) {
            Err(Error::TooLarge { .. }) => replier.send(too_large(&incoming.message, what), &incoming.connection),
            other => other,
        };
        if let Err(error) = sent {
            log::warn!("could not answer a {what}: {error}");
        }
    }

    /// Closes every connection as the client's `shutdown(timeout)` does,
    /// what arrives meanwhile and what was read and not handled refused
    /// first, so that its senders retry elsewhere at once. Idempotent;
    /// dropping the server does it. In a forked child it only closes the
    /// child's copies of the sockets, with close(2) alone, and takes no
    /// lock.
    pub fn close(&mut self, timeout: Option<Duration>) {
        if self.closed {
            return;
        }
        self.closed = true;
        if self.shared.orphaned() {
            self.shared.descriptors.close_all();
            return;
        }
        let mut client = self.shared.lock();
        if !client.orphaned() {
            client.refuse_arrivals();
            client.refuse_unread();
        }
        client.shutdown(timeout);
        self.dropped_while_closing = client.dropped_while_closing();
        drop(client);
        self.run_deferred(); // the drops of the shutdown, told here
    }
}

impl<H: Handler> Drop for BaseMultiplexerServer<H> {
    fn drop(&mut self) {
        self.close(Some(CLOSE_FLUSH_SECONDS));
    }
}

/// The echo of `request`, a PING or a search, built as a reply: its
/// payload carried back, and its workflow.
fn echo_of(request: &Message) -> Message {
    as_reply(request, Message { r#type: types::PING, payload: request.payload.clone(), ..Default::default() })
}

/// BACKEND_ERROR saying the echo of `request` would be over
/// MAX_MESSAGE_SIZE: a reply with no workflow, as C++ sends it, since the
/// request's may be what made the echo too large.
fn too_large(request: &Message, what: &str) -> Message {
    let text = format!("the echo of a {what} of {} bytes would be over MAX_MESSAGE_SIZE", request.payload.len());
    Message {
        r#type: types::BACKEND_ERROR,
        to: Some(request.sender),
        references: request.id,
        payload: text.into_bytes().into(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    //! The report of an echo too large, measured as the wire has it.

    use prost::Message as _;

    use super::too_large;
    use crate::constants::types;
    use crate::defaults::MAX_MESSAGE_SIZE;
    use crate::message::Message;
    use crate::proto::multiplexer::MultiplexerMessage;

    #[test]
    fn the_report_of_an_echo_too_large_carries_no_workflow_and_fits() {
        // A PING whose workflow is what makes its echo too large: the
        // report says so without the workflow, so that it reaches the
        // requester.
        let ping = Message {
            id: u64::MAX,
            sender: u64::MAX,
            r#type: types::PING,
            workflow: Some("w".repeat(MAX_MESSAGE_SIZE)),
            ..Default::default()
        };
        let mut report = too_large(&ping, "PING");
        (report.id, report.sender) = (u64::MAX, u64::MAX); // the longest the send can give it
        assert!(report.workflow.as_ref().is_none_or(|workflow| workflow.is_empty()));
        assert!(MultiplexerMessage::from(report).encoded_len() <= MAX_MESSAGE_SIZE);
    }
}
