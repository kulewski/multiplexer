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
//! The client lives behind a mutex, held only while the loop waits for a
//! message and while a reply is queued, so that a request answers from the
//! handler without the loop, and a handle on another thread reaches the
//! client between two waits: a request kept and answered from another
//! thread goes out once the loop's wait ends, `poll` at most.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use super::{handle, nothing_more_arrives, Controlled, Handler, Replier, Request, ServeError, ServerHandle};
use crate::constants::types;
use crate::core::client::DropReason;
use crate::defaults::{CLOSE_FLUSH_SECONDS, DEFAULT_TIMEOUT};
use crate::endpoint::parse_endpoint;
use crate::error::{Error, Result};
use crate::handles::Incoming;
use crate::message::{KeyFilters, Message, Routing};
use crate::sync_client::SyncClient;

/// What the server and its handles share.
pub(crate) struct Shared {
    client: Arc<Mutex<SyncClient>>,
    pub(crate) working: AtomicBool,
    pub(crate) draining: AtomicBool,
    draining_since: Mutex<Option<Instant>>,
    drain_routing: Routing,
    pub(crate) instance_id: u64,
}

impl Shared {
    /// The client, for the one call that holds it.
    pub(crate) fn lock(&self) -> MutexGuard<'_, SyncClient> {
        self.client.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Starts the drain, once: the drain routing told to every
    /// multiplexer, from the time of the first call.
    pub(crate) fn start_draining(&self) {
        if self.draining.swap(true, Ordering::SeqCst) {
            return;
        }
        *self.draining_since.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Instant::now());
        let _ = self.lock().set_routing(self.drain_routing);
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
    /// inside the loop.
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
        if let Some(observer) = self.on_drop {
            builder = builder.on_drop(observer);
        }
        let client = builder.build()?;
        let instance_id = client.instance_id();
        let shared = Arc::new(Shared {
            client: Arc::new(Mutex::new(client)),
            working: AtomicBool::new(true),
            draining: AtomicBool::new(false),
            draining_since: Mutex::new(None),
            drain_routing: self.drain_routing,
            instance_id,
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

    /// The requests that reached the server while it closed, and went
    /// unanswered; kept past `close()`.
    pub fn dropped_while_closing(&self) -> u64 {
        if self.closed {
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
        self.shared.lock().connect_all(&self.addresses, Some(DEFAULT_TIMEOUT))
    }

    /// The loop: a connection to every address started unless `connect()`
    /// did, none waited for; then, until `stop()` or the drain is over, a
    /// message waited for, `poll` at most, and handled, and
    /// `periodic_task()`; then what was read is handled, what arrives
    /// refused, and the connections closed. `drain` caps a drain, None for
    /// no cap. Returns the error of a handler whose `on_handler_error()`
    /// said to end, or the client's.
    pub fn serve_forever(&mut self, poll: Duration, drain: Option<Duration>) -> std::result::Result<(), ServeError> {
        if self.shared.lock().orphaned() {
            return Err(ServeError::Client(Error::UsedAfterFork));
        }
        self.drain = drain;
        let served = self.serve(poll);
        self.close(Some(CLOSE_FLUSH_SECONDS));
        served
    }

    /// One step: a message waited for, `timeout` at most, None for no
    /// deadline, and handled. `OperationTimedOut` when none came.
    pub fn loop_iter(&mut self, timeout: Option<Duration>) -> std::result::Result<(), ServeError> {
        let incoming = self.shared.lock().receive(timeout)?;
        match self.handle_received(incoming) {
            Some(error) => Err(ServeError::Handler(error)),
            None => Ok(()),
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
            // multiplexer has it.
            let received = self.shared.lock().receive_until_acknowledged(Some(poll));
            match received {
                Ok(incoming) => {
                    if let Some(error) = self.handle_received(incoming) {
                        return Err(ServeError::Handler(error));
                    }
                }
                Err(Error::OperationTimedOut) => {}
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
                let Ok(incoming) = self.shared.lock().receive(Some(Duration::ZERO)) else { break };
                if let Some(error) = self.handle_received(incoming) {
                    return Err(ServeError::Handler(error));
                }
            }
        }
        Ok(())
    }

    /// Whether the drain is over: the handler's word on the library's
    /// answer, which is the drain's cap passed, or, with every path off and
    /// no last resort, every multiplexer having confirmed it.
    fn drained(&self) -> bool {
        let since = *self.shared.draining_since.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let past_cap = match (since, self.drain) {
            (Some(since), Some(cap)) => since.elapsed() >= cap,
            _ => false,
        };
        let by_default = past_cap
            || (nothing_more_arrives(&self.shared.drain_routing)
                && self.shared.lock().routing_acknowledged().unwrap_or(true));
        self.handler.drained(by_default)
    }

    /// One message: the protocol's own answered here, the rest to the
    /// handler. The handler's failure when the server ends for it.
    fn handle_received(&mut self, incoming: Incoming) -> Option<super::HandlerError> {
        if incoming.message.r#type <= types::MAX_MULTIPLEXER_META_PACKET {
            self.handle_internal(incoming);
            return None;
        }
        let request = Request::new(incoming, Replier::Sync(Arc::clone(&self.shared.client)));
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
        let request = Request::new(incoming.clone(), Replier::Sync(Arc::clone(&self.shared.client)));
        let echo = Message { r#type: types::PING, payload: incoming.message.payload.clone(), ..Default::default() };
        let sent = match request.reply_message(echo) {
            Err(Error::TooLarge { .. }) => request.report_error(format!(
                "the echo of a {what} of {} bytes would be over MAX_MESSAGE_SIZE",
                incoming.message.payload.len()
            )),
            other => other,
        };
        if let Err(error) = sent {
            log::warn!("could not answer a {what}: {error}");
        }
    }

    /// Closes every connection as the client's `shutdown(timeout)` does,
    /// what arrives meanwhile and what was read and not handled refused
    /// first, so that its senders retry elsewhere at once. Idempotent;
    /// dropping the server does it.
    pub fn close(&mut self, timeout: Option<Duration>) {
        if self.closed {
            return;
        }
        self.closed = true;
        let mut client = self.shared.lock();
        if !client.orphaned() {
            client.refuse_arrivals();
            client.refuse_unread();
        }
        client.shutdown(timeout);
        self.dropped_while_closing = client.dropped_while_closing();
    }
}

impl<H: Handler> Drop for BaseMultiplexerServer<H> {
    fn drop(&mut self) {
        self.close(Some(CLOSE_FLUSH_SECONDS));
    }
}
