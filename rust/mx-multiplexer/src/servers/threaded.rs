//! `BaseThreadedMultiplexerServer`: a backend whose handlers run on worker
//! threads behind a `ThreadedClient`'s io thread, which heartbeats,
//! reconnects, answers PINGs and the search clients use to find a backend,
//! and hands every other message to a bounded queue the workers take from.
//! Mirrors multiplexer/backend/base_threaded_multiplexer_server.cc. With one
//! worker handling is serial, in arrival order, and the peer stays
//! registered under a request of any length.
//!
//! The queue takes what a worker will take and `queue_size` more that wait
//! for one; past that a request is dropped, as a full queue on the
//! multiplexer drops, and logged at the drop lines' rate. A request that
//! arrives while the server leaves is refused with the DELIVERY_ERROR a
//! multiplexer sends for a peer that is gone, so that its requester retries
//! elsewhere at once; a reply is dropped. `serve_forever()` only polls,
//! waking at a stop, a drain, an acknowledgement and a drain's last request:
//! a change's `on_acknowledged` runs there once the requests the queue took
//! before it are done.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::thread::{JoinHandle, ThreadId};
use std::time::{Duration, Instant};

use super::{
    handle, nothing_more_arrives, Controlled, Handler, HandlerError, Replier, Request, ServeError, ServerHandle,
};
use crate::constants::types;
use crate::core::client::{check_key_filters, DropLine, DropReason};
use crate::defaults::{CLOSE_FLUSH_SECONDS, DEFAULT_TIMEOUT};
use crate::endpoint::parse_endpoint;
use crate::error::{Error, Result};
use crate::handles::Incoming;
use crate::log_summary::Kind;
use crate::message::{DeliveryError, KeyFilters, Routing};
use crate::threaded_client::ThreadedClient;

/// Locks `mutex`, which nothing panics under.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A callback of an acknowledged change.
type Acknowledged = Box<dyn FnOnce() + Send>;

/// What a `BaseThreadedMultiplexerServer` is built with.
#[derive(Debug, Clone)]
pub struct ThreadedServerOptions {
    /// Handler threads, 1 at least.
    pub workers: usize,
    /// Requests waiting for a worker; beyond it they are dropped.
    pub queue_size: usize,
    /// Leave a client's search unanswered while every worker is busy and
    /// requests wait.
    pub decline_searches_when_full: bool,
    /// How long `connect()` waits for the handshakes, None for no deadline.
    pub connect_timeout: Option<Duration>,
    /// What `start_draining()` tells the multiplexers.
    pub drain_routing: Routing,
}

impl Default for ThreadedServerOptions {
    fn default() -> Self {
        ThreadedServerOptions {
            workers: 1,
            queue_size: 1024,
            decline_searches_when_full: false,
            connect_timeout: Some(DEFAULT_TIMEOUT),
            drain_routing: super::direct_only_routing(),
        }
    }
}

/// What a server's handle does with a threaded server, whatever its
/// handler's type.
pub(crate) trait Control: Send + Sync {
    fn stop(&self);
    fn working(&self) -> bool;
    fn start_draining(&self);
    fn draining(&self) -> bool;
    fn instance_id(&self) -> u64;
    fn client(&self) -> &ThreadedClient;
    fn set_key_filters(&self, filters: KeyFilters, on_acknowledged: Option<Acknowledged>) -> Result<()>;
    fn pending(&self) -> usize;
    fn dropped(&self) -> usize;
}

/// A change acknowledged, waiting for the requests the queue took before
/// it, `remaining` of them still queued or held by a worker.
struct Waiting {
    before: u64,
    remaining: usize,
    callback: Option<Acknowledged>,
}

/// What the workers share, under one lock.
struct State {
    queue: VecDeque<(u64, Request)>,
    busy: usize,
    /// Workers waiting for a request: a request is notified only when one
    /// waits, since a notify costs a syscall whether anyone waits or not.
    idle: usize,
    accepted: u64,
    acknowledged: Vec<Waiting>,
    accepting: bool,
    threads: Vec<JoinHandle<()>>,
    worker_ids: Vec<ThreadId>,
    failure: Option<HandlerError>,
}

/// The server's state, which the io thread, the workers, the serving
/// thread and the handles share.
struct Shared<H: Handler> {
    handler: H,
    client: ThreadedClient,
    options: ThreadedServerOptions,
    addresses: Vec<String>,
    peer_type: u32,
    state: Mutex<State>,
    work: Condvar,
    /// The serving loop was woken: set with every notify and taken by its
    /// next wait, so that a wake while it was elsewhere is not lost.
    woken: Mutex<bool>,
    wake: Condvar,
    working: AtomicBool,
    draining: AtomicBool,
    draining_since: Mutex<Option<Instant>>,
    drain: Mutex<Option<Duration>>,
    connected: AtomicBool,
    closed: AtomicBool,
    close_lock: Mutex<()>,
    dropped: AtomicUsize,
    /// A flush `drained()` armed while something waited for a connection:
    /// its end, what was held written once a connection is back or given up
    /// on, wakes the serving loop. One at a time.
    held_flush_armed: AtomicBool,
    /// The server itself, weakly, for the client's callbacks.
    weak: OnceLock<Weak<Shared<H>>>,
}

impl<H: Handler> Shared<H> {
    /// Ends the serving loop's wait, now or at its next.
    fn wake_loop(&self) {
        *lock(&self.woken) = true;
        self.wake.notify_all();
    }

    /// Whether to answer a client's search: never while closing, and not
    /// while saturated when the options say so; then the handler's word.
    fn answers_search(&self) -> bool {
        if self.closed.load(Ordering::SeqCst) {
            return false;
        }
        if self.options.decline_searches_when_full {
            let state = lock(&self.state);
            if state.busy >= self.options.workers && !state.queue.is_empty() {
                return false;
            }
        }
        self.handler.should_answer_search()
    }

    /// The io thread's: `incoming` queued for a worker, dropped when the
    /// queue is full, refused when leaving.
    fn on_message(&self, incoming: Incoming) {
        let request = Request::new(incoming, Replier::Threaded(self.client.clone()));
        let accepting = {
            let mut state = lock(&self.state);
            // What a worker will take, an idle one's included, and
            // queue_size more that wait for one.
            if state.accepting
                && state.queue.len() + state.busy < self.options.workers.saturating_add(self.options.queue_size)
            {
                let number = state.accepted;
                state.accepted += 1;
                state.queue.push_back((number, request));
                let waiting = state.idle > 0;
                drop(state);
                if waiting {
                    self.work.notify_one();
                }
                return;
            }
            state.accepting
        };
        self.dropped.fetch_add(1, Ordering::Relaxed);
        if accepting {
            self.say_full(&request);
            request.no_response(); // said here, not by the request
            return;
        }
        self.refuse(&request);
    }

    /// A request dropped for a full queue, counted with the client's own
    /// drop lines, as C++ counts it: the first of a run logged, the rest
    /// said in a line a second by the client's timer, on the io thread,
    /// where this runs too, at its next turn.
    fn say_full(&self, request: &Request) {
        let (id, r#type) = (request.message().id, request.message().r#type);
        self.client.run_on_io_thread(move |core| {
            let kind = Kind { reason: DropLine::RequestsQueueFull as u32, level: log::Level::Warn, r#type: 0, peer: 0 };
            if core.drop_line_first(kind, || "requests dropped: queue full".to_string()) {
                log::warn!("request #{id} of type {type} dropped: queue full");
            }
        });
    }

    /// A request this server will not serve, as it leaves: refused with the
    /// DELIVERY_ERROR a multiplexer sends for a peer that is gone, or
    /// dropped when it answers another message.
    fn refuse(&self, request: &Request) {
        request.no_response(); // said here, not by the request
        let message = request.message();
        if message.references != 0 {
            log::debug!("reply #{} of type {} dropped: leaving", message.id, message.r#type);
            return;
        }
        log::debug!("request #{} of type {} refused: leaving", message.id, message.r#type);
        if message.report_delivery_error != Some(false) && message.r#type > types::MAX_MULTIPLEXER_META_PACKET {
            let error =
                DeliveryError { packet_id: message.id, failed_type: vec![self.peer_type], ..Default::default() };
            if let Err(error) = request.reply(types::DELIVERY_ERROR, error.encode()) {
                log::debug!("the refusal of #{} did not go: {error}", message.id);
            }
        }
    }

    /// A worker: the next request through the handler, until the queue is
    /// shut and empty. A failure the handler's `on_handler_error()` ends the
    /// server for is kept for `serve_forever()`, which then returns it.
    fn work(self: &Arc<Self>) {
        loop {
            let (number, request) = {
                let mut state = lock(&self.state);
                loop {
                    if let Some(next) = state.queue.pop_front() {
                        state.busy += 1;
                        break next;
                    }
                    if !state.accepting {
                        return;
                    }
                    state.idle += 1;
                    state = self.work.wait(state).unwrap_or_else(|poisoned| poisoned.into_inner());
                    state.idle -= 1;
                }
            };
            let failure = handle(&self.handler, request, "BaseThreadedMultiplexerServer");
            let (wake, failed) = {
                let mut state = lock(&self.state);
                state.busy -= 1;
                let mut wake = false;
                for waiting in &mut state.acknowledged {
                    if waiting.remaining > 0 && number < waiting.before {
                        waiting.remaining -= 1;
                        wake |= waiting.remaining == 0; // its callback is due
                    }
                }
                // A drain waits for the last request too: the loop looks now.
                wake |= self.draining.load(Ordering::SeqCst) && state.queue.is_empty() && state.busy == 0;
                let failed = failure.is_some();
                if let Some(failure) = failure {
                    state.failure.get_or_insert(failure);
                }
                (wake, failed)
            };
            if failed {
                self.working.store(false, Ordering::SeqCst);
            }
            if wake || failed {
                self.wake_loop();
            }
        }
    }

    /// The io thread's: a change every multiplexer acknowledged; its
    /// callback waits for the requests the queue took before it.
    fn acknowledged(&self, callback: Option<Acknowledged>) {
        {
            let mut state = lock(&self.state);
            let (before, remaining) = (state.accepted, state.queue.len() + state.busy);
            state.acknowledged.push(Waiting { before, remaining, callback });
        }
        self.wake_loop();
    }

    /// The serving thread's: the callbacks whose requests are done, in
    /// order, a later change counting the requests of an earlier one too.
    fn run_acknowledged(&self) {
        let due: Vec<Acknowledged> = {
            let mut state = lock(&self.state);
            let done = state.acknowledged.iter().take_while(|waiting| waiting.remaining == 0).count();
            state.acknowledged.drain(..done).filter_map(|waiting| waiting.callback).collect()
        };
        for callback in due {
            crate::core::client::guarded(|| "a callback of an acknowledged change".to_string(), callback);
        }
    }

    /// Whether the drain is over: the handler's word on the library's
    /// answer, which is the drain's cap passed, a closed server, or, with
    /// every path off and no last resort, nothing queued or being handled,
    /// every multiplexer having confirmed the drain, and nothing waiting
    /// for a connection (`nothing_held()`).
    fn drained(&self) -> bool {
        if !self.draining.load(Ordering::SeqCst) {
            return false;
        }
        let since = *lock(&self.draining_since);
        let past_cap = match (since, *lock(&self.drain)) {
            (Some(since), Some(cap)) => since.elapsed() >= cap,
            _ => false,
        };
        let by_default = self.closed.load(Ordering::SeqCst)
            || past_cap
            || (nothing_more_arrives(&self.options.drain_routing)
                && self.pending() == 0
                && self.client.routing_acknowledged().unwrap_or(true)
                && self.nothing_held());
        self.handler.drained(by_default)
    }

    /// Whether nothing the server sent waits for a connection to come up.
    /// With no connection live the confirmation holds at once, so what is
    /// held for the next connection, a reply made while every multiplexer
    /// was down, is waited for too, where the close would drop it: a flush
    /// wakes the loop once it is written, a connection back, or given up on
    /// at its own deadline.
    fn nothing_held(&self) -> bool {
        // What waits in a batch goes first, a flush that waits for nothing,
        // to be held with the rest.
        let _ = self.client.flush_all(Some(Duration::ZERO)).wait();
        if self.client.held_for_a_connection().unwrap_or(0) == 0 {
            return true;
        }
        if !self.held_flush_armed.swap(true, Ordering::SeqCst) {
            let weak = self.this();
            let armed = self.client.flush_all_then(None, move |_| {
                if let Some(shared) = weak.upgrade() {
                    shared.held_flush_armed.store(false, Ordering::SeqCst);
                    shared.wake_loop();
                }
            });
            if !armed {
                self.held_flush_armed.store(false, Ordering::SeqCst); // shut down under us
            }
        }
        false
    }

    /// What the serving loop's next wait may last: `poll`, and while a
    /// drain with a cap runs, no longer than what is left of the cap.
    fn next_wait(&self, poll: Duration) -> Duration {
        if !self.draining.load(Ordering::SeqCst) {
            return poll;
        }
        match (*lock(&self.draining_since), *lock(&self.drain)) {
            (Some(since), Some(cap)) => poll.min(cap.saturating_sub(since.elapsed())),
            _ => poll,
        }
    }

    /// Takes nothing more, starts the drain, lets the workers finish what is
    /// queued and joins them, refuses what no worker took, and shuts the
    /// client down, what was sent before written first, `timeout` at most.
    fn close(self: &Arc<Self>, timeout: Option<Duration>) -> Result<()> {
        let current = std::thread::current().id();
        if lock(&self.state).worker_ids.contains(&current) {
            return Err(Error::InvalidArgument(
                "close() called from a worker thread, which it would join; call stop() instead".to_string(),
            ));
        }
        let _closing = lock(&self.close_lock); // a second close() returns once the first is done
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let threads = {
            let mut state = lock(&self.state);
            state.accepting = false;
            std::mem::take(&mut state.threads)
        };
        self.work.notify_all();
        self.start_draining();
        self.working.store(false, Ordering::SeqCst);
        self.wake_loop();
        for thread in threads {
            let _ = thread.join();
        }
        let left: Vec<(u64, Request)> = lock(&self.state).queue.drain(..).collect();
        for (_, request) in &left {
            self.refuse(request);
        }
        drop(left);
        let _ = self.client.shutdown(timeout).wait();
        Ok(())
    }

    /// Starts the workers, once, and connects to every address at once
    /// against one connect_timeout.
    fn connect(self: &Arc<Self>) -> Result<()> {
        if self.connected.swap(true, Ordering::SeqCst) || self.closed.load(Ordering::SeqCst) {
            return Ok(());
        }
        {
            let mut state = lock(&self.state);
            if state.accepting && state.threads.is_empty() {
                for index in 0..self.options.workers {
                    let worker = Arc::clone(self);
                    let thread = std::thread::Builder::new()
                        .name(format!("mx-worker-{index}"))
                        .spawn(move || worker.work())
                        .map_err(|error| Error::Io(error.to_string()))?;
                    state.worker_ids.push(thread.thread().id());
                    state.threads.push(thread);
                }
            }
        }
        match self.client.connect_all(&self.addresses, self.options.connect_timeout).wait() {
            Err(Error::NotConnected) if self.closed.load(Ordering::SeqCst) => Ok(()), // closed under us
            other => other.map(|_| ()),
        }
    }
}

impl<H: Handler> Control for Shared<H> {
    fn stop(&self) {
        self.working.store(false, Ordering::SeqCst);
        if !self.client.orphaned() {
            self.wake_loop();
        }
    }

    fn working(&self) -> bool {
        self.working.load(Ordering::SeqCst)
    }

    fn start_draining(&self) {
        {
            let mut since = lock(&self.draining_since);
            if since.is_none() {
                *since = Some(Instant::now()); // the first call's: a later one does not move it
            }
        }
        if !self.draining.swap(true, Ordering::SeqCst) {
            // The loop looks again when every multiplexer has it, rather
            // than at its next poll.
            let wake = self.wake_handle();
            let _ = self.client.set_routing_then(self.options.drain_routing, wake);
            self.wake_loop();
        }
    }

    fn draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    fn instance_id(&self) -> u64 {
        self.client.instance_id()
    }

    fn client(&self) -> &ThreadedClient {
        &self.client
    }

    fn set_key_filters(&self, filters: KeyFilters, on_acknowledged: Option<Acknowledged>) -> Result<()> {
        check_key_filters(&filters)?;
        let acknowledged = self.acknowledged_handle();
        self.client.set_key_filters_then(filters, move || acknowledged(on_acknowledged))
    }

    fn pending(&self) -> usize {
        if self.client.orphaned() {
            return 0; // a forked child: the parent's workers, and a lock they may hold for good
        }
        let state = lock(&self.state);
        state.queue.len() + state.busy
    }

    fn dropped(&self) -> usize {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl<H: Handler> Shared<H> {
    /// What wakes the serving loop from the io thread, holding the server
    /// weakly: a callback the client keeps must not keep the server.
    fn wake_handle(&self) -> impl Fn() + Send + 'static {
        let weak = self.this();
        move || {
            if let Some(shared) = weak.upgrade() {
                shared.wake_loop();
            }
        }
    }

    /// What tells the server of an acknowledgement from the io thread,
    /// holding it weakly.
    fn acknowledged_handle(&self) -> impl FnOnce(Option<Acknowledged>) + Send + 'static {
        let weak = self.this();
        move |callback| {
            if let Some(shared) = weak.upgrade() {
                shared.acknowledged(callback);
            }
        }
    }

    /// The server, weakly, as the client's callbacks hold it.
    fn this(&self) -> Weak<Shared<H>> {
        // Every Shared is made in an Arc by the builder, which records it.
        self.weak.get().cloned().unwrap_or_default()
    }
}

/// Builds a `BaseThreadedMultiplexerServer`; see
/// `BaseThreadedMultiplexerServer::builder`.
#[must_use = "a builder does nothing until build()"]
pub struct BaseThreadedMultiplexerServerBuilder {
    peer_type: u32,
    addresses: Vec<String>,
    options: ThreadedServerOptions,
    key_filters: Option<KeyFilters>,
    on_drop: Option<Box<dyn FnMut(u64, DropReason) + Send>>,
}

impl BaseThreadedMultiplexerServerBuilder {
    /// Handler threads, 1 at least.
    pub fn workers(mut self, workers: usize) -> Self {
        self.options.workers = workers;
        self
    }

    /// Requests waiting for a worker; beyond it they are dropped.
    /// `usize::MAX` is no limit.
    pub fn queue_size(mut self, queue_size: usize) -> Self {
        self.options.queue_size = queue_size;
        self
    }

    /// Leave a client's search unanswered while every worker is busy and
    /// requests wait.
    pub fn decline_searches_when_full(mut self, decline: bool) -> Self {
        self.options.decline_searches_when_full = decline;
        self
    }

    /// How long `connect()` waits for the handshakes, None for no deadline.
    pub fn connect_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.options.connect_timeout = timeout;
        self
    }

    /// What `start_draining()` tells the multiplexers.
    pub fn drain_routing(mut self, routing: Routing) -> Self {
        self.options.drain_routing = routing;
        self
    }

    /// Every option at once.
    pub fn options(mut self, options: ThreadedServerOptions) -> Self {
        self.options = options;
        self
    }

    /// The key filters, in effect from the first request.
    pub fn key_filters(mut self, filters: KeyFilters) -> Self {
        self.key_filters = Some(filters);
        self
    }

    /// Every message the server sent and gave up on is told to `observer`,
    /// on the io thread.
    pub fn on_drop(mut self, observer: impl FnMut(u64, DropReason) + Send + 'static) -> Self {
        self.on_drop = Some(Box::new(observer));
        self
    }

    /// The server, serving `handler`: its client and io thread made, no
    /// worker started and nothing connected until `connect()` or
    /// `serve_forever()`, so that no multiplexer knows the backend until it
    /// can serve. `InvalidArgument` for no worker or an address that does
    /// not parse.
    pub fn build<H: Handler>(self, handler: H) -> Result<BaseThreadedMultiplexerServer<H>> {
        if self.options.workers < 1 {
            return Err(Error::InvalidArgument("workers must be at least 1".to_string()));
        }
        for address in &self.addresses {
            parse_endpoint(address, None)?;
        }
        // The client's callbacks reach the server through this, set once it
        // is made: they hold it weakly, so that the client keeps no server.
        let cell: Arc<OnceLock<Weak<Shared<H>>>> = Arc::default();
        let (on_message, on_search) = (Arc::clone(&cell), Arc::clone(&cell));
        let mut builder = ThreadedClient::builder(self.peer_type)
            .on_message(move |incoming| {
                if let Some(shared) = on_message.get().and_then(Weak::upgrade) {
                    shared.on_message(incoming);
                }
            })
            .search_policy(move || {
                on_search.get().and_then(Weak::upgrade).is_some_and(|shared| shared.answers_search())
            });
        if let Some(filters) = self.key_filters {
            builder = builder.key_filters(filters);
        }
        if let Some(observer) = self.on_drop {
            builder = builder.on_drop(observer);
        }
        let client = builder.build()?;
        let shared = Arc::new(Shared {
            handler,
            client,
            options: self.options,
            addresses: self.addresses,
            peer_type: self.peer_type,
            state: Mutex::new(State {
                queue: VecDeque::new(),
                busy: 0,
                idle: 0,
                accepted: 0,
                acknowledged: Vec::new(),
                accepting: true,
                threads: Vec::new(),
                worker_ids: Vec::new(),
                failure: None,
            }),
            work: Condvar::new(),
            woken: Mutex::new(false),
            wake: Condvar::new(),
            working: AtomicBool::new(true),
            draining: AtomicBool::new(false),
            draining_since: Mutex::new(None),
            drain: Mutex::new(None),
            connected: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            close_lock: Mutex::new(()),
            dropped: AtomicUsize::new(0),
            held_flush_armed: AtomicBool::new(false),
            weak: OnceLock::new(),
        });
        let _ = shared.weak.set(Arc::downgrade(&shared));
        let _ = cell.set(Arc::downgrade(&shared));
        Ok(BaseThreadedMultiplexerServer { shared })
    }
}

/// A backend with handlers on workers; see the module comment.
pub struct BaseThreadedMultiplexerServer<H: Handler> {
    shared: Arc<Shared<H>>,
}

impl<H: Handler> std::fmt::Debug for BaseThreadedMultiplexerServer<H> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BaseThreadedMultiplexerServer")
            .field("instance_id", &self.shared.client.instance_id())
            .finish_non_exhaustive()
    }
}

impl BaseThreadedMultiplexerServer<Box<dyn Handler>> {
    /// A builder of a backend of `peer_type` for the multiplexers at
    /// `addresses`, `host:port` or `[address]:port` each.
    pub fn builder<A: AsRef<str>>(
        peer_type: u32,
        addresses: impl IntoIterator<Item = A>,
    ) -> BaseThreadedMultiplexerServerBuilder {
        BaseThreadedMultiplexerServerBuilder {
            peer_type,
            addresses: addresses.into_iter().map(|address| address.as_ref().to_string()).collect(),
            options: ThreadedServerOptions::default(),
            key_filters: None,
            on_drop: None,
        }
    }
}

impl<H: Handler> BaseThreadedMultiplexerServer<H> {
    /// A handle on the server, for `stop()` and `start_draining()` from any
    /// thread.
    pub fn handle(&self) -> ServerHandle {
        let control: Arc<dyn Control> = Arc::clone(&self.shared) as Arc<dyn Control>;
        ServerHandle(Controlled::Threaded(control))
    }

    /// The handler.
    pub fn handler(&self) -> &H {
        &self.shared.handler
    }

    /// The server's client, for messages that are not replies.
    pub fn client(&self) -> &ThreadedClient {
        &self.shared.client
    }

    /// Starts the workers and connects to every address at once, waiting
    /// for them all against one connect_timeout, once; `serve_forever()`
    /// calls it first.
    pub fn connect(&self) -> Result<()> {
        if self.shared.client.orphaned() {
            return Err(Error::UsedAfterFork);
        }
        self.shared.connect()
    }

    /// `connect()`; then, until `stop()`, a drain's end or a handler's
    /// failure that ends the server, `periodic_task()` every `poll`, or
    /// sooner when woken; then `close()`. `drain` caps a drain, None for no
    /// cap: while a drain with a cap runs, a wait lasts no longer than what
    /// is left of it, so that the cap ends the drain on time whatever the
    /// poll. Returns the failure of a handler whose `on_handler_error()` said
    /// to end, or the client's. The calling thread only polls; the
    /// handlers run on the workers.
    pub fn serve_forever(&self, poll: Duration, drain: Option<Duration>) -> std::result::Result<(), ServeError> {
        if self.shared.client.orphaned() {
            return Err(ServeError::Client(Error::UsedAfterFork));
        }
        *lock(&self.shared.drain) = drain;
        // A panic out of periodic_task() or drained() closes the server
        // first, as C++ and Python close it, and then goes on up.
        let served = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.serve(poll)));
        let closed = self.shared.close(Some(CLOSE_FLUSH_SECONDS));
        let served = served.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        served?;
        closed?;
        if let Some(failure) = lock(&self.shared.state).failure.take() {
            return Err(ServeError::Handler(failure));
        }
        Ok(())
    }

    /// `serve_forever()` but the close.
    fn serve(&self, poll: Duration) -> std::result::Result<(), ServeError> {
        self.shared.connect()?;
        let handle = self.handle();
        loop {
            {
                let mut woken = lock(&self.shared.woken);
                if !*woken {
                    let wait = self.shared.next_wait(poll);
                    woken =
                        self.shared.wake.wait_timeout(woken, wait).unwrap_or_else(|poisoned| poisoned.into_inner()).0;
                }
                *woken = false;
            }
            self.shared.run_acknowledged();
            let failed = lock(&self.shared.state).failure.is_some();
            if !self.shared.working.load(Ordering::SeqCst) || self.shared.drained() || failed {
                return Ok(());
            }
            self.shared.handler.periodic_task(&handle);
        }
    }

    /// Takes no more messages, tells every multiplexer the drain routing,
    /// answers no search, lets the workers finish what is queued, stops
    /// them, refuses what no worker took, and shuts the client down, what
    /// was sent before written first, `timeout` at most. Idempotent: a
    /// second call returns once the first is done. `InvalidArgument` from a
    /// worker, which it would join: a handler that wants the server gone
    /// calls `stop()`.
    pub fn close(&self, timeout: Option<Duration>) -> Result<()> {
        if self.shared.client.orphaned() {
            drop(self.shared.client.shutdown(None)); // the child's copies of the sockets, see Drop
            return Err(Error::UsedAfterFork);
        }
        self.shared.close(timeout)
    }
}

impl<H: Handler> Drop for BaseThreadedMultiplexerServer<H> {
    /// `close()` with CLOSE_FLUSH_SECONDS; in a forked child, the child's
    /// copies of the sockets closed and nothing else: the workers are the
    /// parent's, and their clones of the client, never let go of there,
    /// would keep it from tearing itself down as an orphan.
    fn drop(&mut self) {
        if self.shared.client.orphaned() {
            drop(self.shared.client.shutdown(None));
            return;
        }
        let _ = self.shared.close(Some(CLOSE_FLUSH_SECONDS));
    }
}
