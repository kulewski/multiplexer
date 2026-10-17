//! `ThreadedClient`: a client with an io thread of its own, which runs the
//! loop all the time, so heartbeats flow and reconnects happen whatever the
//! program does, and which any thread may call. Its waiting calls end in
//! `.wait()`, from any thread but the io thread, in `.await`, on any
//! executor, which makes it Rust's `AsyncClient`, or in a callback that
//! runs on the io thread. Mirrors multiplexer/threaded_client.cc: the
//! engine lives on the io thread, which owns it, and every call from
//! another thread reaches it through the io thread's inbox (inbox.rs), as
//! C++'s does through its own (threaded_client_inbox.h). docs/api_rust.md
//! describes it from the caller's side.
//!
//! What arrives on its own, not a reply to a query: the protocol's own is
//! handled or dropped; a PING is answered, and a search for a backend when
//! the search policy says yes; the rest goes to every subscription of its
//! type, to the inbox once `messages()` was asked for, and to `on_message`,
//! or, with none of them, is logged and dropped. Callbacks run on the io
//! thread and must be quick; one that panics is logged and the client goes
//! on.
//!
//! The handles are `Clone + Send + Sync`. Dropping the last one shuts the
//! client down, as the C++ destructor does: what was sent before is
//! written, CLOSE_FLUSH_SECONDS at most, then the connections close the
//! polite way, and the drop waits for it, but on the io thread itself, from
//! a callback, where it only starts it. A client one of whose callbacks
//! holds a handle lives until `shutdown()`. `shutdown()` and the posting of
//! commands keep C++'s protocol: a call counts itself in `posting` before
//! it reads `stopped`, and `shutdown()` sets `stopped` before it waits for
//! `posting` to fall to 0, so a call that found the client up is run before
//! the shutdown, and no caller waits on an io thread that has ended.
//!
//! A send nobody follows, `queue()` without `on_end`, a lane or `all()`, of
//! a plain event, once the client batches, goes to the io thread as data,
//! not as a command (`core::plain`): serialized on the caller's thread into
//! a body the io thread handed back, taken from the inbox under the lock
//! it is pushed with and kept by the thread till its next such send
//! (`SPARE_BODY`), its deadline counted from the send. So a stream of them
//! allocates nothing on either thread, and nothing one thread allocated is
//! freed on the other, which with glibc's malloc made each thread wait on
//! the other's arena lock.

use std::cell::Cell;
use std::future::IntoFuture;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::thread::{JoinHandle, ThreadId};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::constants::types;
use crate::core::batching::{check_batching, plain_event, Batching};
use crate::core::bodies::BODY_KEPT_MAX;
use crate::core::client::{check_key_filters, DropReason, IncomingMode};
use crate::core::plain::Plain;
use crate::core::query::{check_size_sent_again, Outcome};
use crate::core::{deadline_after, Core, Posted};
use crate::defaults::{CLOSE_FLUSH_SECONDS, DEFAULT_INCOMING_QUEUE_MAX_SIZE, DEFAULT_TIMEOUT};
use crate::endpoint::{format_endpoint, parse_endpoint};
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::fork::DescriptorTable;
use crate::handles::{Incoming, Lane};
use crate::ids::Ids;
use crate::inbox::Inbox;
use crate::message::{frame_of, framed, plain_of, stamped, KeyFilters, Message, Routing};
use crate::pending::{pending, ready, Completer, Messages, Pending, Queue};
use crate::proto::multiplexer as proto;

/// Locks `mutex`, which nothing panics under.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

thread_local! {
    /// This thread's next body for a plain send: one an io thread handed
    /// back, or empty, which the send's serialization then allocates. Any
    /// client's: a body is only bytes. Read and kept with `try_with`: a send
    /// made from another thread-local's destructor, once this one is gone,
    /// makes a body of its own.
    static SPARE_BODY: Cell<Vec<u8>> = const { Cell::new(Vec::new()) };
}

/// `body` kept as this thread's spare, while the thread's locals live, when
/// it is no bigger than the batching keeps one (`BODY_KEPT_MAX`): a body a
/// refused send leaves may be a whole message's.
fn keep_spare(body: Vec<u8>) {
    if body.capacity() <= BODY_KEPT_MAX {
        let _ = SPARE_BODY.try_with(|spare| spare.set(body));
    }
}

/// A query's outcome as a caller gets it: the reply, or why not; a
/// BACKEND_ERROR reply is `Error::Backend`.
pub(crate) fn query_result(outcome: Outcome) -> Result<Incoming> {
    let reply = outcome.into_result().map(Incoming::from)?;
    if reply.message.r#type == types::BACKEND_ERROR {
        return Err(Error::Backend(Box::new(reply)));
    }
    Ok(reply)
}

/// Builds a `ThreadedClient`; see `ThreadedClient::builder`.
#[must_use = "a builder does nothing until build() or connect()"]
pub struct ThreadedClientBuilder {
    peer_type: u32,
    on_message: Option<Box<dyn FnMut(Incoming) + Send>>,
    on_drop: Option<Box<dyn FnMut(u64, DropReason) + Send>>,
    search_policy: Option<Box<dyn FnMut() -> bool + Send>>,
    key_filters: Option<KeyFilters>,
    routing: Option<Routing>,
}

impl ThreadedClientBuilder {
    /// `on_message` gets, on the io thread, every message that arrives on
    /// its own and is none of the protocol's: events, requests, addressed
    /// messages, the delivery errors of messages that were not queries.
    pub fn on_message(mut self, on_message: impl FnMut(Incoming) + Send + 'static) -> Self {
        self.on_message = Some(Box::new(on_message));
        self
    }

    /// Every message the program sent that the client gives up on, each
    /// copy of one sent to ALL, is told to `observer` with its id and why,
    /// on the io thread.
    pub fn on_drop(mut self, observer: impl FnMut(u64, DropReason) + Send + 'static) -> Self {
        self.on_drop = Some(Box::new(observer));
        self
    }

    /// A backend built on the client answers the search clients use to
    /// find a backend of its type when `answer()`, on the io thread, says
    /// yes; without a policy, no search is answered.
    pub fn search_policy(mut self, answer: impl FnMut() -> bool + Send + 'static) -> Self {
        self.search_policy = Some(Box::new(answer));
        self
    }

    /// The key filters, in effect from the first message: carried by every
    /// welcome.
    pub fn key_filters(mut self, filters: KeyFilters) -> Self {
        self.key_filters = Some(filters);
        self
    }

    /// The routing, in effect from the first message.
    pub fn routing(mut self, routing: Routing) -> Self {
        self.routing = Some(routing);
        self
    }

    /// The client and its io thread, connected to nothing yet.
    pub fn build(self) -> Result<ThreadedClient> {
        if let Some(filters) = &self.key_filters {
            check_key_filters(filters)?;
        }
        let inbox = Arc::new(Inbox::default());
        let mut engine = Engine::new(self.peer_type, IncomingMode::Dispatch, Some(Arc::clone(&inbox)))?;
        engine.core.threaded.on_message = self.on_message;
        engine.core.threaded.search_policy = self.search_policy;
        if let Some(observer) = self.on_drop {
            engine.core.set_drop_observer(Some(observer));
        }
        if let Some(filters) = self.key_filters {
            engine.call(|core| core.set_key_filters(filters, None))?;
        }
        if let Some(routing) = self.routing {
            engine.call(|core| core.set_routing(routing.into(), None));
        }
        let core = &engine.core;
        let shared = Arc::new(Shared {
            inbox,
            waker: engine.driver.waker(),
            stopped: AtomicBool::new(false),
            posting: AtomicUsize::new(0),
            lifecycle: Mutex::new(()),
            posted: Condvar::new(),
            torn_down: AtomicBool::new(false),
            io_thread: OnceLock::new(),
            thread: Mutex::new(None),
            ended: Mutex::new((false, Vec::new())),
            messages: Mutex::new(None),
            instance_id: core.instance_id,
            peer_type: core.peer_type,
            ids: Arc::clone(&core.ids),
            generation: crate::fork::generation(),
            dropped: Arc::clone(&core.client.dropped),
            descriptors: Arc::clone(&core.client.descriptors),
            batching: AtomicBool::new(false),
        });
        let io_shared = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("mx-io".to_string())
            .spawn(move || io_thread(engine, io_shared))
            .map_err(|error| Error::Io(error.to_string()))?;
        let _ = shared.io_thread.set(thread.thread().id());
        *lock(&shared.thread) = Some(thread);
        Ok(ThreadedClient { handle: Arc::new(Handle { shared }) })
    }

    /// The client, connected to every address at once, `host:port` or
    /// `[address]:port`, each handshake waited for against one
    /// DEFAULT_TIMEOUT; those that did not come up the io thread goes on
    /// trying. `InvalidArgument` for an address that does not parse, before
    /// anything connects.
    pub fn connect<A: AsRef<str>>(self, addresses: impl IntoIterator<Item = A>) -> Result<ThreadedClient> {
        let client = self.build()?;
        client.connect_all(addresses, Some(DEFAULT_TIMEOUT)).wait()?;
        Ok(client)
    }
}

/// What the io thread and every handle share; on cache lines of its own,
/// since every send reads `stopped` and counts itself in `posting`.
#[repr(align(64))]
struct Shared {
    inbox: Arc<Inbox>,
    waker: Arc<mio::Waker>,
    stopped: AtomicBool,
    posting: AtomicUsize,
    lifecycle: Mutex<()>,
    posted: Condvar,
    torn_down: AtomicBool,
    io_thread: OnceLock<ThreadId>,
    thread: Mutex<Option<JoinHandle<()>>>,
    /// Whether the io thread has ended, and who waits for it to.
    ended: Mutex<(bool, Vec<Completer<()>>)>,
    /// The inbox `messages()` reads, once asked for.
    messages: Mutex<Option<Arc<Queue>>>,
    instance_id: u64,
    peer_type: u32,
    ids: Arc<Ids>,
    generation: u32,
    dropped: Arc<AtomicU64>,
    descriptors: Arc<DescriptorTable>,
    /// Whether `set_batching()` was ever called: the callers' threads then
    /// mark the frames they make for it (`frame_of`); a hint, a frame made
    /// unmarked going alone.
    batching: AtomicBool,
}

/// Ends a post's count, telling a shutdown that waits for the last one.
struct Posting<'shared>(&'shared Shared);

impl Drop for Posting<'_> {
    fn drop(&mut self) {
        if self.0.posting.fetch_sub(1, Ordering::SeqCst) == 1 && self.0.stopped.load(Ordering::SeqCst) {
            let _lock = lock(&self.0.lifecycle);
            self.0.posted.notify_all();
        }
    }
}

impl Shared {
    /// Whether this process inherited the client across a fork.
    fn orphaned(&self) -> bool {
        self.generation != crate::fork::generation()
    }

    /// Fails a call on a client inherited across a fork, before any lock.
    fn check(&self) -> Result<()> {
        if self.orphaned() {
            return Err(Error::UsedAfterFork);
        }
        Ok(())
    }

    /// Whether this is the client's io thread.
    fn on_io_thread(&self) -> bool {
        self.io_thread.get() == Some(&std::thread::current().id())
    }

    /// The io thread, for a `Pending` whose `wait()` there would wait for
    /// good.
    fn io_thread_id(&self) -> Option<ThreadId> {
        self.io_thread.get().copied()
    }

    /// Posts `command` to the io thread; false when the client was shut
    /// down first. From the io thread itself, a callback's, it goes in
    /// until the shutdown's end, so that what a callback sends while the
    /// shutdown writes out goes out with it.
    fn post(&self, command: impl FnOnce(&mut Core) + Send + 'static) -> bool {
        if self.on_io_thread() {
            if self.torn_down.load(Ordering::Acquire) {
                return false;
            }
            self.inbox.push(Posted::Command(Box::new(command)));
            return true;
        }
        if self.stopped.load(Ordering::SeqCst) {
            return false;
        }
        self.posting.fetch_add(1, Ordering::SeqCst);
        let _posting = Posting(self);
        if self.stopped.load(Ordering::SeqCst) {
            return false;
        }
        if self.inbox.push(Posted::Command(Box::new(command))) {
            let _ = self.waker.wake();
        }
        true
    }

    /// Posts a plain send, with its deadline, as `post` posts a command;
    /// the body handed back for this thread's next one, or, when the client
    /// was shut down first, the send itself back.
    fn post_plain(&self, plain: Plain, deadline: Option<Instant>) -> std::result::Result<Vec<u8>, Plain> {
        if self.on_io_thread() {
            if self.torn_down.load(Ordering::Acquire) {
                return Err(plain);
            }
            return Ok(self.inbox.push_plain(plain, deadline).0);
        }
        if self.stopped.load(Ordering::SeqCst) {
            return Err(plain);
        }
        self.posting.fetch_add(1, Ordering::SeqCst);
        let _posting = Posting(self);
        if self.stopped.load(Ordering::SeqCst) {
            return Err(plain);
        }
        let (spare, wake) = self.inbox.push_plain(plain, deadline);
        if wake {
            let _ = self.waker.wake();
        }
        Ok(spare)
    }

    /// A plain send of the stamped `encoded`, serialized into this thread's
    /// spare body, here, and posted as data (see the module comment): its
    /// id, `TooLarge` before anything is posted, `NotConnected` after the
    /// shutdown.
    fn queue_plain(&self, encoded: proto::MultiplexerMessage, timeout: Option<Duration>) -> Result<u64> {
        let id = encoded.id.unwrap_or(0);
        let spare = SPARE_BODY.try_with(Cell::take).unwrap_or_default();
        let plain = match plain_of(&encoded, spare) {
            Ok(plain) => plain,
            Err((error, body)) => {
                keep_spare(body);
                return Err(error);
            }
        };
        drop(encoded); // here, on the thread that made its payload and key
        let deadline = deadline_after(Instant::now(), timeout);
        match self.post_plain(plain, deadline) {
            Ok(spare) => {
                keep_spare(spare);
                Ok(id)
            }
            Err(plain) => {
                keep_spare(plain.body);
                Err(Error::NotConnected)
            }
        }
    }

    /// Runs `body` on the io thread and returns what it gave: for the short
    /// calls only. `OnIoThread` on the io thread, `NotConnected` after the
    /// shutdown.
    fn call<R: Send + 'static>(&self, body: impl FnOnce(&mut Core) -> R + Send + 'static) -> Result<R> {
        self.check()?;
        if self.on_io_thread() {
            return Err(Error::OnIoThread);
        }
        let (result, completer) = pending(None);
        if !self.post(move |core| completer.complete(Ok(body(core)))) {
            return Err(Error::NotConnected);
        }
        result.wait()
    }

    /// A result that comes when the io thread has ended.
    fn ended(&self) -> Pending<()> {
        let mut ended = lock(&self.ended);
        if ended.0 {
            return ready(Ok(()));
        }
        let (result, completer) = pending(self.io_thread_id());
        ended.1.push(completer);
        result
    }

    /// Ends every client call and the io thread: see `ThreadedClient::shutdown`.
    fn shutdown(&self, timeout: Option<Duration>) -> Pending<()> {
        if self.orphaned() {
            self.descriptors.close_all();
            return ready(Ok(()));
        }
        if self.stopped.swap(true, Ordering::SeqCst) {
            return self.ended(); // already done, or being done by another thread
        }
        {
            let mut guard = lock(&self.lifecycle);
            while self.posting.load(Ordering::SeqCst) != 0 {
                guard = self.posted.wait(guard).unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
        self.inbox.push(Posted::Command(Box::new(move |core: &mut Core| core.begin_shutdown(timeout))));
        let _ = self.waker.wake();
        self.ended()
    }
}

/// The io thread: the loop, run until the shutdown's end and nothing is
/// left to happen; then the sends still followed hear their end, the
/// engine goes, the program's callbacks with it, and whoever waits for the
/// end hears of it.
fn io_thread(mut engine: Engine, shared: Arc<Shared>) {
    loop {
        if engine.core.threaded.torn_down {
            shared.torn_down.store(true, Ordering::Release);
            if engine.idle() {
                break;
            }
        }
        // A bug in one handler does not take the client down: logged, and
        // the loop goes on, as C++ does.
        let stepped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            engine.driver.step(&mut engine.core, None, true);
        }));
        if let Err(panic) = stepped {
            let what = panic
                .downcast_ref::<&str>()
                .map(|text| text.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            log::error!("a panic escaped an io handler: {what}");
        }
    }
    log::debug!("io thread done");
    engine.call(|core| core.end_follows());
    drop(engine);
    let waiting = {
        let mut ended = lock(&shared.ended);
        ended.0 = true;
        std::mem::take(&mut ended.1)
    };
    for completer in waiting {
        completer.complete(Ok(()));
    }
}

/// The handle the program's clones share: its end shuts the client down.
struct Handle {
    shared: Arc<Shared>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        let shared = &self.shared;
        if shared.orphaned() {
            // A forked child: the io thread does not exist, and a lock it
            // held is held for good; the child's copies of the sockets are
            // closed, and everything else is left alone, leaked: the io
            // thread's handle above all, whose drop would detach a pthread_t
            // glibc may have handed to a thread of the child's by then.
            shared.descriptors.close_all();
            std::mem::forget(Arc::clone(shared));
            return;
        }
        let ended = shared.shutdown(Some(CLOSE_FLUSH_SECONDS));
        if shared.on_io_thread() {
            return; // the io thread cannot wait for itself: it ends on its own
        }
        let _ = ended.wait();
        if let Some(thread) = lock(&shared.thread).take() {
            let _ = thread.join();
        }
    }
}

/// A client with an io thread of its own; see the module comment.
#[derive(Clone)]
pub struct ThreadedClient {
    handle: Arc<Handle>,
}

impl std::fmt::Debug for ThreadedClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ThreadedClient").field("instance_id", &self.instance_id()).finish_non_exhaustive()
    }
}

impl ThreadedClient {
    /// A builder of a client of `peer_type`.
    pub fn builder(peer_type: u32) -> ThreadedClientBuilder {
        ThreadedClientBuilder {
            peer_type,
            on_message: None,
            on_drop: None,
            search_policy: None,
            key_filters: None,
            routing: None,
        }
    }

    /// What every handle shares.
    fn shared(&self) -> &Shared {
        &self.handle.shared
    }

    /// Runs `command` on the io thread at its next turn, from any thread,
    /// the io thread's own included: what a threaded server counts its
    /// drops with in the client's drop lines. False after the shutdown.
    pub(crate) fn run_on_io_thread(&self, command: impl FnOnce(&mut Core) + Send + 'static) -> bool {
        self.shared().post(command)
    }

    /// This client's instance id: the `sender` of what it sends.
    pub fn instance_id(&self) -> u64 {
        self.shared().instance_id
    }

    /// This client's peer type.
    pub fn peer_type(&self) -> u32 {
        self.shared().peer_type
    }

    /// Whether this process inherited the client across a fork.
    pub fn orphaned(&self) -> bool {
        self.shared().orphaned()
    }

    /// How many messages the program sent the client gave up on, each copy
    /// of one sent to ALL; from any thread.
    pub fn dropped(&self) -> u64 {
        self.shared().dropped.load(Ordering::Relaxed)
    }

    /// A message of `type` with `payload`, its id and sender filled in.
    pub fn new_message(&self, r#type: u32, payload: impl Into<Bytes>) -> Message {
        let shared = self.shared();
        Message {
            id: shared.ids.next(),
            sender: shared.instance_id,
            r#type,
            payload: payload.into(),
            ..Default::default()
        }
    }

    /// Connects to one more multiplexer, `host:port` or `[address]:port`:
    /// true once its handshake is done, false as soon as it failed or at
    /// `timeout`, the io thread going on trying. A multiplexer the client
    /// has a connection to, live or on its way, keeps it.
    pub fn connect(&self, address: &str, timeout: Option<Duration>) -> Pending<bool> {
        let shared = self.shared();
        if let Err(error) = shared.check() {
            return ready(Err(error));
        }
        if shared.on_io_thread() {
            return ready(Err(Error::OnIoThread));
        }
        let (host, port) = match parse_endpoint(address, None) {
            Ok(parsed) => parsed,
            Err(error) => return ready(Err(error)),
        };
        log::info!("connecting to {}", format_endpoint(&host, port));
        let (result, completer) = pending(shared.io_thread_id());
        if !shared
            .post(move |core| core.start_connect(&host, port, timeout, Box::new(move |up| completer.complete(Ok(up)))))
        {
            return ready(Err(Error::NotConnected));
        }
        result
    }

    /// Connects to every address at once, each handshake waited for
    /// against one `timeout`: how many are registered when it ends. Every
    /// address is parsed first.
    pub fn connect_all<A: AsRef<str>>(
        &self,
        addresses: impl IntoIterator<Item = A>,
        timeout: Option<Duration>,
    ) -> Pending<usize> {
        let shared = self.shared();
        if let Err(error) = shared.check() {
            return ready(Err(error));
        }
        if shared.on_io_thread() {
            return ready(Err(Error::OnIoThread));
        }
        let parsed: Result<Vec<(String, u16)>> =
            addresses.into_iter().map(|address| parse_endpoint(address.as_ref(), None)).collect();
        let parsed = match parsed {
            Ok(parsed) if parsed.is_empty() => return ready(Ok(0)),
            Ok(parsed) => parsed,
            Err(error) => return ready(Err(error)),
        };
        let (result, completer) = pending(shared.io_thread_id());
        let posted = shared.post(move |core| {
            // Every connect starts before any is waited for; the last to end
            // tells how many came up.
            let tally = Arc::new(Mutex::new((parsed.len(), 0usize, Some(completer))));
            for (host, port) in &parsed {
                log::info!("connecting to {}", format_endpoint(host, *port));
                let tally = Arc::clone(&tally);
                core.start_connect(
                    host,
                    *port,
                    timeout,
                    Box::new(move |up| {
                        let mut tally = lock(&tally);
                        tally.0 -= 1;
                        tally.1 += usize::from(up);
                        if tally.0 == 0 {
                            if let Some(completer) = tally.2.take() {
                                completer.complete(Ok(tally.1));
                            }
                        }
                    }),
                );
            }
        });
        if !posted {
            return ready(Err(Error::NotConnected));
        }
        result
    }

    /// Drops a multiplexer given to `connect()`, as `SyncClient::disconnect()`
    /// does; whether the client had it. `NotConnected` after `shutdown()`.
    pub fn disconnect(&self, address: &str) -> Result<bool> {
        let (host, port) = parse_endpoint(address, None)?;
        self.shared().call(move |core| core.disconnect(&host, port))?
    }

    /// How many connections are registered.
    pub fn connections_count(&self) -> Result<usize> {
        self.shared().call(|core| core.connections_count())
    }

    /// A query: a request of `type` with `payload`, which `wait()`, `.await`
    /// or `then()` sends.
    pub fn query(&self, r#type: u32, payload: impl Into<Bytes>) -> ThreadedQuery<'_> {
        self.query_message(Message { r#type, payload: payload.into(), ..Default::default() })
    }

    /// A query of a whole message, the request itself, typed by its own
    /// `type` and addressed by its own `to`; each attempt gets an id of its
    /// own, an empty sender is filled in.
    pub fn query_message(&self, request: Message) -> ThreadedQuery<'_> {
        ThreadedQuery { client: self, request, timeout: Some(DEFAULT_TIMEOUT), lane: None, received: None }
    }

    /// An event of `type` with `payload`, which `queue()`, `wait()` or
    /// `written()` sends.
    pub fn send(&self, r#type: u32, payload: impl Into<Bytes>) -> ThreadedSend<'_> {
        self.send_message(Message { r#type, payload: payload.into(), ..Default::default() })
    }

    /// A whole message, sent as it is, its empty id and sender filled in.
    pub fn send_message(&self, message: Message) -> ThreadedSend<'_> {
        ThreadedSend { client: self, message, all: false, lane: None, timeout: Some(DEFAULT_TIMEOUT), on_end: None }
    }

    /// Waits until everything sent before the call has been written or
    /// given up on, the callbacks of those sends run, or `timeout`: true
    /// when every one was written. True at once after `shutdown()`: what is
    /// left to write is the shutdown's, whose result comes once it is over.
    pub fn flush_all(&self, timeout: Option<Duration>) -> Pending<bool> {
        let shared = self.shared();
        if let Err(error) = shared.check() {
            return ready(Err(error));
        }
        let (result, completer) = pending(shared.io_thread_id());
        if !shared
            .post(move |core| core.begin_flush_all(timeout, Box::new(move |flushed| completer.complete(Ok(flushed)))))
        {
            return ready(Ok(true));
        }
        result
    }

    /// Which of a multiplexer's routing paths reach this peer: told to every
    /// multiplexer, carried by every welcome from now on. Nothing after
    /// `shutdown()`.
    pub fn set_routing(&self, routing: Routing) -> Result<()> {
        self.shared().check()?;
        self.shared().post(move |core| {
            if !core.threaded.shut_down {
                core.set_routing(routing.into(), None);
            }
        });
        Ok(())
    }

    /// `set_routing()`, and `on_acknowledged` called on the io thread once
    /// this change, or a later one, is acknowledged, after everything routed
    /// before went to `on_message`, the streams or its query.
    pub fn set_routing_then(&self, routing: Routing, on_acknowledged: impl FnOnce() + Send + 'static) -> Result<()> {
        self.shared().check()?;
        self.shared().post(move |core| {
            if !core.threaded.shut_down {
                core.set_routing(routing.into(), Some(Box::new(on_acknowledged)));
            }
        });
        Ok(())
    }

    /// Which keys of each message type reach this peer by the rules,
    /// replacing those set before. `InvalidArgument`, here, for a protocol
    /// message type or a pattern over MAX_KEY_PATTERN_SIZE.
    pub fn set_key_filters(&self, filters: KeyFilters) -> Result<()> {
        self.shared().check()?;
        check_key_filters(&filters)?;
        self.shared().post(move |core| {
            if !core.threaded.shut_down {
                let _ = core.set_key_filters(filters, None);
            }
        });
        Ok(())
    }

    /// Batching (docs/batching.md): the events of `r#type`, each key
    /// apart, or, given `key`, of that key, ahead of what is set for its
    /// type, that `queue()` sends through no lane and not to ALL, go several
    /// in one message, which the receiving library splits back into one
    /// message each. A batch goes once it holds `max_records` messages,
    /// once one more would take it past `max_bytes` on the wire, or
    /// `max_delay` after its first message; with no delay, once a
    /// connection has nothing left to write, so that a message sent while
    /// one is free goes at once, and those sent while every one is busy go
    /// together. A flush, a send that does not batch, another change of
    /// what is batched and the shutdown first let every batch go.
    /// `InvalidArgument`, here, for a protocol message type, a key over
    /// MAX_KEY_SIZE, no message in a batch or more than MAX_BATCH_RECORDS,
    /// no byte or more than MAX_MESSAGE_SIZE, or a delay over MAX_SECONDS;
    /// in effect for every send made after the call.
    pub fn set_batching(&self, r#type: u32, key: Option<&str>, batching: Batching) -> Result<()> {
        self.shared().check()?;
        check_batching(r#type, key, &batching)?;
        self.shared().batching.store(true, Ordering::Relaxed); // frames are marked for it from now on
        let key = key.map(str::to_string);
        self.shared().post(move |core| {
            if !core.threaded.shut_down {
                let _ = core.set_batching(r#type, key.as_deref(), batching);
            }
        });
        Ok(())
    }

    /// No batching from now on for `r#type`, its keys' settings included,
    /// or, given `key`, for that key, which then batches as its type says;
    /// every batch goes first.
    pub fn clear_batching(&self, r#type: u32, key: Option<&str>) -> Result<()> {
        self.shared().check()?;
        let key = key.map(str::to_string);
        self.shared().post(move |core| {
            if !core.threaded.shut_down {
                core.clear_batching(r#type, key.as_deref());
            }
        });
        Ok(())
    }

    /// `set_key_filters()`, and `on_acknowledged` as for `set_routing_then()`.
    pub fn set_key_filters_then(
        &self,
        filters: KeyFilters,
        on_acknowledged: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        self.shared().check()?;
        check_key_filters(&filters)?;
        self.shared().post(move |core| {
            if !core.threaded.shut_down {
                let _ = core.set_key_filters(filters, Some(Box::new(on_acknowledged)));
            }
        });
        Ok(())
    }

    /// Whether every multiplexer has the current routing and key filters in
    /// effect, with everything they routed before handed out.
    pub fn routing_acknowledged(&self) -> Result<bool> {
        self.shared().call(|core| core.routing_acknowledged())
    }

    /// How many messages wait for a connection to come up: sent while none
    /// was live, or left by one that died with none to take them, each
    /// going out in order once one is registered, or given up on at its own
    /// deadline. What the server classes' drains wait for, the close
    /// dropping them.
    pub fn held_for_a_connection(&self) -> Result<usize> {
        self.shared().call(|core| core.held_for_a_connection())
    }

    /// `flush_all()` with `done(flushed)` on the io thread at its end
    /// instead of a wait: what a draining threaded server wakes its loop
    /// with. False after `shutdown()` or in a forked child, where `done`
    /// never runs.
    pub(crate) fn flush_all_then(&self, timeout: Option<Duration>, done: impl FnOnce(bool) + Send + 'static) -> bool {
        let shared = self.shared();
        shared.check().is_ok() && shared.post(move |core| core.begin_flush_all(timeout, Box::new(done)))
    }

    /// The client's inbox: every message that arrives on its own, in one
    /// stream of DEFAULT_INCOMING_QUEUE_MAX_SIZE that drops its oldest, with
    /// a warning, when nobody reads it. Readers share it. After `shutdown()`
    /// a stream that ends at once, and in a forked child, before any lock,
    /// never the parent's inbox, which no io thread there fills or ends.
    pub fn messages(&self) -> Messages {
        let shared = self.shared();
        if shared.orphaned() {
            let queue = Queue::new(1, "the inbox of a client inherited across a fork".to_string());
            queue.close();
            return Messages::new(queue);
        }
        let mut inbox = lock(&shared.messages);
        if let Some(queue) = inbox.as_ref() {
            return Messages::new(Arc::clone(queue));
        }
        let queue = Queue::new(DEFAULT_INCOMING_QUEUE_MAX_SIZE, "the client's inbox, messages(),".to_string());
        let registered = Arc::clone(&queue);
        if shared.check().is_err() || !shared.post(move |core| core.register_inbox(registered)) {
            queue.close();
        }
        *inbox = Some(Arc::clone(&queue));
        Messages::new(queue)
    }

    /// The messages of `type` that arrive on its own, a stream of its own,
    /// bounded as the inbox is; dropping it ends the subscription.
    pub fn subscribe(&self, r#type: u32) -> Messages {
        let queue = Queue::new(DEFAULT_INCOMING_QUEUE_MAX_SIZE, format!("the subscription to type {type}"));
        let registered = Arc::downgrade(&queue);
        let shared = self.shared();
        if shared.check().is_err() || !shared.post(move |core| core.register_subscription(r#type, registered)) {
            queue.close();
        }
        Messages::new(queue)
    }

    /// Ends every query in flight with `NotConnected`, writes what was sent
    /// before the call, and what the io thread sends meanwhile, `timeout`
    /// at most, None for no deadline, nothing more for zero; then closes
    /// the connections the polite way and ends the io thread. What is still
    /// unwritten is dropped and reported. Any other call fails with
    /// `NotConnected` from now on. Idempotent. The result comes once the io
    /// thread has ended.
    pub fn shutdown(&self, timeout: Option<Duration>) -> Pending<()> {
        self.shared().shutdown(timeout)
    }

    /// How many message ids the client watches for an answer, every
    /// attempt's and every search's of the queries in flight; for tests.
    #[doc(hidden)]
    pub fn watched_ids(&self) -> Result<usize> {
        self.shared().call(|core| core.queries.watched_ids())
    }

    /// How many times a message or a query was tried again after it could
    /// not go at once; for tests.
    #[doc(hidden)]
    pub fn retries(&self) -> Result<u64> {
        self.shared().call(|core| core.outbox.retries() + core.queries.retries)
    }

    /// How many queries wait for a connection; for tests.
    #[doc(hidden)]
    pub fn waiting_queries(&self) -> Result<usize> {
        self.shared().call(|core| core.queries.waiting_queries())
    }

    /// How many entries the outbox keeps; for tests.
    #[doc(hidden)]
    pub fn waiting_messages(&self) -> Result<usize> {
        self.shared().call(|core| core.outbox.entries())
    }

    /// Forgets the ids of every query that has ended; for tests that
    /// measure the heap. From another thread it returns once they are
    /// forgotten, as C++'s does; from a callback, which runs inside the io
    /// thread's work, they are forgotten as soon as the callback returns.
    #[doc(hidden)]
    pub fn forget_finished_ids(&self) {
        let shared = self.shared();
        if shared.on_io_thread() {
            shared.post(|core| core.queries.forget_finished_ids());
            return;
        }
        let _ = shared.call(|core| core.queries.forget_finished_ids());
    }

    /// Fails for a lane of another client, or one inherited across a fork.
    fn check_lane(&self, lane: Option<&Lane>) -> Result<()> {
        let Some(lane) = lane else { return Ok(()) };
        lane.0.check_not_inherited()?;
        if lane.0.owner().is_some_and(|owner| owner != self.instance_id()) {
            return Err(Error::InvalidArgument(
                "a lane of another client: a connection or lane belongs to its client".to_string(),
            ));
        }
        Ok(())
    }
}

/// A query being built; `wait()`, `.await` or `then()` sends it. Every
/// option has a default: DEFAULT_TIMEOUT per stage, no lane.
#[must_use = "a query is sent by wait(), .await or then()"]
pub struct ThreadedQuery<'client> {
    client: &'client ThreadedClient,
    request: Message,
    timeout: Option<Duration>,
    lane: Option<Lane>,
    received: Option<Box<dyn FnMut(u64) + Send>>,
}

impl ThreadedQuery<'_> {
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
    pub fn connection(mut self, connection: &crate::handles::Connection) -> Self {
        self.lane = Some(Lane::through(connection, false));
        self
    }

    /// `received` hears, on the io thread, the instance id of each backend
    /// that acknowledges an attempt with REQUEST_RECEIVED.
    pub fn on_received(mut self, received: impl FnMut(u64) + Send + 'static) -> Self {
        self.received = Some(Box::new(received));
        self
    }

    /// Starts the query, `done` hearing its end on the io thread; after
    /// `shutdown()`, at once, here, with `NotConnected`. A request too large
    /// is refused before anything goes out.
    fn start(self, done: Box<dyn FnOnce(Result<Incoming>) + Send>) -> Result<()> {
        let ThreadedQuery { client, request, timeout, lane, received } = self;
        let shared = client.shared();
        shared.check()?;
        client.check_lane(lane.as_ref())?;
        let mut prototype = proto::MultiplexerMessage::from(request);
        prototype.id = None;
        check_size_sent_again(&mut prototype, shared.instance_id)?;
        let (done, kept) = {
            // The callback goes to the io thread, or, if the command never
            // runs, is called here: one of the two.
            let slot = Arc::new(Mutex::new(Some(done)));
            (Arc::clone(&slot), slot)
        };
        let posted = shared.post(move |core| {
            core.start_query(
                prototype,
                timeout,
                lane.map(|lane| lane.0),
                Box::new(move |outcome| {
                    if let Some(done) = lock(&done).take() {
                        done(query_result(outcome));
                    }
                }),
                received,
            );
        });
        if !posted {
            if let Some(done) = lock(&kept).take() {
                done(Err(Error::NotConnected));
            }
        }
        Ok(())
    }

    /// Sends the query; `done` hears its end on the io thread, where it must
    /// be quick, or at once, here, after `shutdown()`: one made from a
    /// callback while the shutdown writes out hears `NotConnected` on the io
    /// thread. `TooLarge` and the other refusals at the call come back here,
    /// and `done` is not called.
    pub fn then(self, done: impl FnOnce(Result<Incoming>) + Send + 'static) -> Result<()> {
        self.start(Box::new(done))
    }

    /// Sends the query and waits for its end, by the stages docs/query.md
    /// draws. `OnIoThread` on the client's io thread, from a callback.
    pub fn wait(self) -> Result<Incoming> {
        if self.client.shared().on_io_thread() {
            return Err(Error::OnIoThread);
        }
        self.into_future().wait()
    }
}

impl IntoFuture for ThreadedQuery<'_> {
    type Output = Result<Incoming>;
    type IntoFuture = Pending<Incoming>;

    /// Sends the query; the result is awaited on any executor. Dropping
    /// the future cancels nothing: the query runs its stages on, its reply
    /// dropped; `.timeout(d)` bounds it.
    fn into_future(self) -> Pending<Incoming> {
        let (result, completer) = pending(self.client.shared().io_thread_id());
        match self.start(Box::new(move |outcome| completer.complete(outcome))) {
            Ok(()) => result,
            Err(error) => ready(Err(error)),
        }
    }
}

/// A send being built; `queue()`, `wait()` or `written()` sends it. Every
/// option has a default: one connection, round robin, DEFAULT_TIMEOUT to
/// wait for room or for a connection.
#[must_use = "a send goes by queue(), wait() or written()"]
pub struct ThreadedSend<'client> {
    client: &'client ThreadedClient,
    message: Message,
    all: bool,
    lane: Option<Lane>,
    timeout: Option<Duration>,
    on_end: Option<Box<dyn FnOnce(bool) + Send>>,
}

impl ThreadedSend<'_> {
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
    /// is still in its handshake at its welcome.
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
    pub fn connection(mut self, connection: &crate::handles::Connection) -> Self {
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

    /// `on_end` hears how the message ended, once, on the io thread: true
    /// once it was written, the first copy for ALL, false once it was given
    /// up on or the client shut down first.
    pub fn on_end(mut self, on_end: impl FnOnce(bool) + Send + 'static) -> Self {
        self.on_end = Some(Box::new(on_end));
        self
    }

    /// The checks every form makes, and the frame, the message moved into
    /// it rather than copied.
    fn frame(&mut self) -> Result<crate::core::outbox::Frame> {
        let client = self.client;
        let shared = client.shared();
        shared.check()?;
        client.check_lane(self.lane.as_ref())?;
        if self.lane.as_ref().is_some_and(Lane::closed) {
            return Err(Error::NotConnected); // a pinned lane whose connection is gone
        }
        let batching = shared.batching.load(Ordering::Relaxed);
        frame_of(std::mem::take(&mut self.message), || shared.ids.next(), shared.instance_id, batching)
    }

    /// Sends the message and returns its id at once: the io thread queues
    /// it on a connection, has it wait for room, or holds it until one
    /// comes up, the timeout at most, and drops and reports it after that.
    /// `NotConnected` after `shutdown()` and for a pinned lane whose
    /// connection is gone; `TooLarge` before anything is queued.
    pub fn queue(mut self) -> Result<u64> {
        let shared = self.client.shared();
        let frame =
            if self.on_end.is_none() && self.lane.is_none() && !self.all && shared.batching.load(Ordering::Relaxed) {
                // Nobody follows it: a plain event goes as data (`core::plain`).
                shared.check()?;
                let encoded = stamped(std::mem::take(&mut self.message), || shared.ids.next(), shared.instance_id);
                if plain_event(&encoded) {
                    return shared.queue_plain(encoded, self.timeout);
                }
                framed(encoded, true)?
            } else {
                self.frame()?
            };
        let id = frame.id;
        let ThreadedSend { client, all, lane, timeout, on_end, .. } = self;
        let deadline = deadline_after(Instant::now(), timeout);
        let settled = on_end
            .map(|on_end| -> crate::core::threaded::SendSettled { Box::new(move |written, _| on_end(written > 0)) });
        let posted = client
            .shared()
            .post(move |core| core.place_send(frame, all, lane.map(|lane| lane.0), deadline, false, settled));
        if !posted {
            return Err(Error::NotConnected);
        }
        Ok(id)
    }

    /// Sends the message; the result comes once it reached a socket, the
    /// first copy for ALL, with its id, or at the timeout: `NotConnected`
    /// when nothing wrote it with no connection live, or it was given up
    /// on, `OperationTimedOut` otherwise. Awaited on any executor.
    pub fn written(mut self) -> Pending<u64> {
        let frame = match self.frame() {
            Ok(frame) => frame,
            Err(error) => return ready(Err(error)),
        };
        let id = frame.id;
        let ThreadedSend { client, all, lane, timeout, on_end, .. } = self;
        let shared = client.shared();
        let (result, completer) = pending(shared.io_thread_id());
        let deadline = deadline_after(Instant::now(), timeout);
        let settled: crate::core::threaded::SendSettled = Box::new(move |written, not_connected| {
            if let Some(on_end) = on_end {
                on_end(written > 0);
            }
            completer.complete(match (written > 0, not_connected) {
                (true, _) => Ok(id),
                (false, true) => Err(Error::NotConnected),
                (false, false) => Err(Error::OperationTimedOut),
            });
        });
        if !shared.post(move |core| core.place_send(frame, all, lane.map(|lane| lane.0), deadline, true, Some(settled)))
        {
            return ready(Err(Error::NotConnected));
        }
        result
    }

    /// `written()`, waited for. `OnIoThread` on the client's io thread.
    pub fn wait(self) -> Result<u64> {
        if self.client.shared().on_io_thread() {
            return Err(Error::OnIoThread);
        }
        self.written().wait()
    }
}
