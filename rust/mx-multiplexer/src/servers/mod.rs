//! The server classes, on which a backend is usually built:
//! `BaseMultiplexerServer`, the loop and the handler on one thread, the
//! fastest, and `BaseThreadedMultiplexerServer`, handlers on workers behind
//! an io thread that keeps heartbeating. Mirrors multiplexer/backend/: one
//! `Handler` serves both, a `Request` is the same in both, and both drain
//! the same way (docs/leaving.md). This file holds what they share.
//!
//! A handler answers with the `Request`: `reply()` fills in `to`,
//! `references`, `workflow` and the connection from the request, sent as
//! every client sends. A handler that fails, an `Err` or a panic, gets its
//! requester a BACKEND_ERROR, unless a reply went out or the message
//! answers another, and then `on_handler_error()` says whether the server
//! goes on. A request a handler drops without a reply or `no_response()`
//! is logged; one it keeps is answered later, from any thread, and logged
//! when it is dropped unanswered.

pub(crate) mod plain;
pub(crate) mod threaded;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;

use crate::constants::types;
use crate::error::{Error, Result};
use crate::handles::{Connection, Incoming};
use crate::message::{Message, Routing};
use crate::threaded_client::ThreadedClient;

/// Why a handler failed: any error, or a panic's message.
pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;

/// What a handler returns.
pub type HandlerResult = std::result::Result<(), HandlerError>;

/// A backend's behaviour, served by either server class.
pub trait Handler: Send + Sync + 'static {
    /// Called with every message that is not the protocol's own: answer
    /// with `request.reply()`, or call `request.no_response()` for an event.
    /// On `BaseThreadedMultiplexerServer` it runs on a worker, `workers` at
    /// once, and gets the DELIVERY_ERRORs of the server's own messages too.
    fn handle_message(&self, request: Request) -> HandlerResult;

    /// Called after every poll of `serve_forever()`, message or not, and at
    /// once when a change of the routing or the key filters is
    /// acknowledged, a drain's included: work on the backend's own schedule,
    /// and noticing a request to leave (`server.start_draining()`).
    fn periodic_task(&self, server: &ServerHandle) {
        let _ = server;
    }

    /// Called when `handle_message()` failed, after the requester was sent
    /// BACKEND_ERROR: true keeps serving, false ends `serve_forever()` with
    /// the error.
    fn on_handler_error(&self, error: &HandlerError) -> bool {
        let _ = error;
        true
    }

    /// Whether to answer a client's search for a backend of this type now.
    /// On the threaded server it runs on the io thread, so quick.
    fn should_answer_search(&self) -> bool {
        true
    }

    /// Whether the drain is over and `serve_forever()` may return;
    /// `by_default` is the library's answer: the drain's time has passed,
    /// or, with every path turned off and no last resort, every multiplexer
    /// confirmed it, nothing is left to handle and nothing the server sent
    /// waits for a connection to come up, a reply made while every
    /// multiplexer was down going out once one is back, where the close
    /// would drop it.
    fn drained(&self, by_default: bool) -> bool {
        by_default
    }
}

/// A boxed handler serves as the handler it boxes; it is also the type the
/// server classes' `builder()` hangs on, so that a call names no handler
/// type before `build()` gets one.
impl Handler for Box<dyn Handler> {
    fn handle_message(&self, request: Request) -> HandlerResult {
        (**self).handle_message(request)
    }
    fn periodic_task(&self, server: &ServerHandle) {
        (**self).periodic_task(server)
    }
    fn on_handler_error(&self, error: &HandlerError) -> bool {
        (**self).on_handler_error(error)
    }
    fn should_answer_search(&self) -> bool {
        (**self).should_answer_search()
    }
    fn drained(&self, by_default: bool) -> bool {
        (**self).drained(by_default)
    }
}

/// How `serve_forever()` ended other than by `stop()` or a drain.
#[derive(Debug)]
pub enum ServeError {
    /// The client failed: used after a fork, a worker thread that could
    /// not start, or a receive's error. A multiplexer that cannot be
    /// reached is none: the library keeps trying while the server serves.
    Client(Error),
    /// A handler failed and `on_handler_error()` returned false.
    Handler(HandlerError),
}

impl std::fmt::Display for ServeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServeError::Client(error) => write!(formatter, "the server's client failed: {error}"),
            ServeError::Handler(error) => write!(formatter, "a handler failed: {error}"),
        }
    }
}

impl std::error::Error for ServeError {}

impl From<Error> for ServeError {
    fn from(error: Error) -> ServeError {
        ServeError::Client(error)
    }
}

/// The routing a draining backend asks for unless told otherwise: nothing
/// new by the rules, only what is addressed to it.
pub fn direct_only_routing() -> Routing {
    Routing { any: false, all: false, last_resort: false }
}

/// Whether a drain with this routing may end as soon as the multiplexers
/// confirmed it: every path off and no last resort.
pub(crate) fn nothing_more_arrives(routing: &Routing) -> bool {
    !routing.any && !routing.all && !routing.last_resort
}

/// Through what a request's replies go: the plain server's client, which
/// its loop lends, or the threaded server's.
#[derive(Clone)]
pub(crate) enum Replier {
    Sync(Arc<plain::Shared>),
    Threaded(ThreadedClient),
}

impl Replier {
    /// Sends `message` through `connection` while it is live, another when
    /// it is gone, as every client sends.
    pub(crate) fn send(&self, message: Message, connection: &Connection) -> Result<()> {
        match self {
            Replier::Sync(shared) => shared.client()?.send_message(message).connection(connection).queue().map(|_| ()),
            Replier::Threaded(client) => client.send_message(message).connection(connection).queue().map(|_| ()),
        }
    }
}

/// A request's message, what answers it, and what the server and the
/// request share of its fate: whether it was answered, and whether a drop
/// unanswered is the request's to log. One per request, held by the
/// request and by the server that handles it, which so keeps what a
/// failure's report needs without copying the message.
pub(crate) struct RequestState {
    incoming: Incoming,
    replier: Replier,
    answered: AtomicBool,
    /// The handler returned and kept the request: it answers later.
    kept: AtomicBool,
}

/// A message being handled, and what answers it; see the module comment.
/// A handler may keep it and answer later, from any thread.
pub struct Request {
    state: Arc<RequestState>,
}

impl std::fmt::Debug for Request {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Request").field("message", &self.state.incoming.message).finish_non_exhaustive()
    }
}

/// `reply` as the reply to `request`: its empty `to`, `references` and
/// `workflow` filled in from the request.
pub(crate) fn as_reply(request: &Message, mut reply: Message) -> Message {
    if reply.to.unwrap_or(0) == 0 {
        reply.to = Some(request.sender);
    }
    if reply.references == 0 {
        reply.references = request.id;
    }
    if reply.workflow.as_ref().is_none_or(String::is_empty) {
        reply.workflow.clone_from(&request.workflow);
    }
    reply
}

impl Request {
    /// A request for `incoming`, answered through `replier`.
    pub(crate) fn new(incoming: Incoming, replier: Replier) -> Request {
        let state = RequestState { incoming, replier, answered: AtomicBool::new(false), kept: AtomicBool::new(false) };
        Request { state: Arc::new(state) }
    }

    /// The message.
    pub fn message(&self) -> &Message {
        &self.state.incoming.message
    }

    /// The message's payload.
    pub fn payload(&self) -> &Bytes {
        &self.state.incoming.message.payload
    }

    /// The connection it came on, which a reply goes back through.
    pub fn connection(&self) -> &Connection {
        &self.state.incoming.connection
    }

    /// Whether a reply went out, or `no_response()` was called.
    pub fn answered(&self) -> bool {
        self.state.answered.load(Ordering::Acquire)
    }

    /// The reply: `payload` as a message of `type`, to the requester,
    /// referencing the request, the way it came.
    pub fn reply(&self, r#type: u32, payload: impl Into<Bytes>) -> Result<()> {
        self.reply_message(Message { r#type, payload: payload.into(), ..Default::default() })
    }

    /// A reply built whole: its empty id and sender set, its empty `to`,
    /// `references` and `workflow` filled in from the request. One reply
    /// per request: `references` means "this is the reply"; a follow-up that
    /// is not the reply goes through the server's handle with `to` set. A
    /// reply that failed did not go out, and the request is not answered.
    pub fn reply_message(&self, message: Message) -> Result<()> {
        let reply = as_reply(&self.state.incoming.message, message);
        self.state.replier.send(reply, &self.state.incoming.connection)?;
        self.state.answered.store(true, Ordering::Release);
        Ok(())
    }

    /// The message needs no reply, as an event does.
    pub fn no_response(&self) {
        self.state.answered.store(true, Ordering::Release);
    }

    /// BACKEND_ERROR carrying `text`: a Rust or Python requester's query
    /// fails with it, a C++ one gets the message itself.
    pub fn report_error(&self, text: impl Into<String>) -> Result<()> {
        self.reply(types::BACKEND_ERROR, text.into().into_bytes())
    }

    /// REQUEST_RECEIVED to the requester at once, for a handler that takes
    /// long: the requester's `on_received` hears it. The request is no more
    /// answered than before.
    pub fn notify_start(&self) -> Result<()> {
        let reply =
            as_reply(&self.state.incoming.message, Message { r#type: types::REQUEST_RECEIVED, ..Default::default() });
        self.state.replier.send(reply, &self.state.incoming.connection)
    }
}

impl Drop for Request {
    /// A request the handler kept, dropped unanswered, is logged.
    fn drop(&mut self) {
        if self.state.kept.load(Ordering::Acquire) && !self.answered() {
            log::warn!(
                "request #{} of type {} dropped without a reply or no_response()",
                self.state.incoming.message.id,
                self.state.incoming.message.r#type
            );
        }
    }
}

/// What a panic says, as a handler's failure.
fn panic_text(panic: Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|text| text.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic".to_string())
}

/// Runs the handler on `request` and does what follows: a failure, an
/// `Err` or a panic, is logged and gets the requester BACKEND_ERROR unless
/// a reply went out or the message answers another; a request dropped
/// unanswered is logged, one kept is left to log its own drop. Returns the
/// failure when `on_handler_error()` says the server ends, or what it
/// panicked with: as an exception out of C++'s on_handler_exception(), it
/// ends the server, where it killed a threaded server's worker with its
/// request counted as being handled for good.
pub(crate) fn handle(handler: &dyn Handler, request: Request, kind: &str) -> Option<HandlerError> {
    let state = Arc::clone(&request.state);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler.handle_message(request)));
    let failure = match outcome {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(panic) => Some(HandlerError::from(panic_text(panic))),
    };
    let message = &state.incoming.message;
    let Some(error) = failure else {
        if Arc::strong_count(&state) > 1 {
            state.kept.store(true, Ordering::Release); // answered later, or logged at its drop
        } else if !state.answered.load(Ordering::Acquire) {
            log::warn!("request #{} of type {} dropped without a reply or no_response()", message.id, message.r#type);
        }
        return None;
    };
    log::error!("{kind}: handle_message failed: {error}");
    // As C++: a report that fails leaves the requester to its timeout; a
    // message that answers another gets none, nobody waiting for an answer
    // to it, and two backends whose handlers fail on what they do not
    // expect would answer each other's reports for good.
    if !state.answered.load(Ordering::Acquire) && message.references == 0 {
        let report = as_reply(
            message,
            Message {
                r#type: types::BACKEND_ERROR,
                payload: error.to_string().into_bytes().into(),
                ..Default::default()
            },
        );
        if let Err(reporting) = state.replier.send(report, &state.incoming.connection) {
            log::error!("{kind}: could not report the failure to the requester: {reporting}");
        }
        state.answered.store(true, Ordering::Release);
    }
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler.on_handler_error(&error))) {
        Ok(true) => None,
        Ok(false) => Some(error),
        Err(panic) => {
            let text = panic_text(panic);
            log::error!("{kind}: on_handler_error panicked: {text}");
            Some(HandlerError::from(format!("on_handler_error panicked: {text}")))
        }
    }
}

/// What a server's handle reaches: the plain server's client and flags, or
/// the threaded server's state.
#[derive(Clone)]
pub(crate) enum Controlled {
    Plain(Arc<plain::Shared>),
    Threaded(Arc<dyn threaded::Control>),
}

/// A handle on a server, `Clone + Send + Sync`: what `periodic_task()` is
/// given, and what a thread of the program's, a signal's watcher say, holds
/// to stop the server or start its drain.
#[derive(Clone)]
pub struct ServerHandle(pub(crate) Controlled);

impl std::fmt::Debug for ServerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ServerHandle").field("instance_id", &self.instance_id()).finish()
    }
}

impl ServerHandle {
    /// Asks `serve_forever()` to return, from any thread: it notices at
    /// once, its wait woken, finishes what it holds and closes the
    /// connections.
    pub fn stop(&self) {
        match &self.0 {
            Controlled::Plain(shared) => shared.stop(),
            Controlled::Threaded(control) => control.stop(),
        }
    }

    /// Whether `stop()` was not called.
    pub fn working(&self) -> bool {
        match &self.0 {
            Controlled::Plain(shared) => shared.working.load(Ordering::SeqCst),
            Controlled::Threaded(control) => control.working(),
        }
    }

    /// Starts the drain: every multiplexer is told the drain routing, so
    /// that nothing new comes by the rules, and the server serves what
    /// still arrives and leaves once drained (docs/leaving.md).
    pub fn start_draining(&self) {
        match &self.0 {
            Controlled::Plain(shared) => shared.start_draining(),
            Controlled::Threaded(control) => control.start_draining(),
        }
    }

    /// Whether the drain started.
    pub fn draining(&self) -> bool {
        match &self.0 {
            Controlled::Plain(shared) => shared.draining.load(Ordering::SeqCst),
            Controlled::Threaded(control) => control.draining(),
        }
    }

    /// The server's instance id.
    pub fn instance_id(&self) -> u64 {
        match &self.0 {
            Controlled::Plain(shared) => shared.instance_id,
            Controlled::Threaded(control) => control.instance_id(),
        }
    }

    /// How many connections are registered. From another thread than the
    /// plain server's own, a call that finds its loop waiting wakes it and
    /// has its turn with the client at once, as every call here does.
    pub fn connections_count(&self) -> Result<usize> {
        match &self.0 {
            Controlled::Plain(shared) => shared.client()?.connections_count(),
            Controlled::Threaded(control) => control.client().connections_count(),
        }
    }

    /// Whether every multiplexer has the current routing and key filters in
    /// effect, a drain's routing included, with everything they routed
    /// before received: what a backend that reports its drain waits for.
    /// Waits as `connections_count()` does.
    pub fn routing_acknowledged(&self) -> Result<bool> {
        match &self.0 {
            Controlled::Plain(shared) => shared.client()?.routing_acknowledged(),
            Controlled::Threaded(control) => control.client().routing_acknowledged(),
        }
    }

    /// How many messages the server's client holds for a connection to
    /// come up, which a drain waits for; see
    /// `SyncClient::held_for_a_connection()`. Waits as
    /// `connections_count()` does.
    pub fn held_for_a_connection(&self) -> Result<usize> {
        match &self.0 {
            Controlled::Plain(shared) => shared.client()?.held_for_a_connection(),
            Controlled::Threaded(control) => control.client().held_for_a_connection(),
        }
    }

    /// Sends a message that is no reply, routed by its type, queued as
    /// every client sends; its id.
    pub fn send(&self, r#type: u32, payload: impl Into<Bytes>) -> Result<u64> {
        self.send_message(Message { r#type, payload: payload.into(), ..Default::default() })
    }

    /// The same for a whole message, sent as it is, its empty id and sender
    /// filled in.
    pub fn send_message(&self, message: Message) -> Result<u64> {
        match &self.0 {
            Controlled::Plain(shared) => shared.client()?.send_message(message).queue(),
            Controlled::Threaded(control) => control.client().send_message(message).queue(),
        }
    }

    /// Batching for the events the server sends with `send()`, as
    /// `SyncClient::set_batching()` and `ThreadedClient::set_batching()`
    /// say: the plain server writes only inside its loop's calls, so give
    /// it a delay. Waits as `connections_count()` does.
    pub fn set_batching(&self, r#type: u32, key: Option<&str>, batching: crate::Batching) -> Result<()> {
        match &self.0 {
            Controlled::Plain(shared) => shared.client()?.set_batching(r#type, key, batching),
            Controlled::Threaded(control) => control.client().set_batching(r#type, key, batching),
        }
    }

    /// No batching from now on for `r#type`, or, given `key`, for that key;
    /// see `SyncClient::clear_batching()`.
    pub fn clear_batching(&self, r#type: u32, key: Option<&str>) -> Result<()> {
        match &self.0 {
            Controlled::Plain(shared) => shared.client()?.clear_batching(r#type, key),
            Controlled::Threaded(control) => control.client().clear_batching(r#type, key),
        }
    }

    /// Which keys of each message type reach this backend by the rules,
    /// replacing those set before, a drain keeping them.
    pub fn set_key_filters(&self, filters: crate::message::KeyFilters) -> Result<()> {
        match &self.0 {
            Controlled::Plain(shared) => shared.client()?.set_key_filters(filters),
            Controlled::Threaded(control) => control.set_key_filters(filters, None),
        }
    }

    /// `set_key_filters()`, and `on_acknowledged` called on the serving
    /// thread once every multiplexer has the change, or a later one, and
    /// every request routed under the filters before has been handled; the
    /// serving loop wakes at it, `periodic_task()` running once more.
    pub fn set_key_filters_then(
        &self,
        filters: crate::message::KeyFilters,
        on_acknowledged: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        match &self.0 {
            Controlled::Plain(shared) => {
                let on_acknowledged = shared.deferred(on_acknowledged);
                shared.client()?.set_key_filters_then(filters, on_acknowledged)
            }
            Controlled::Threaded(control) => control.set_key_filters(filters, Some(Box::new(on_acknowledged))),
        }
    }

    /// Requests waiting for a worker or being handled: always 0 on the plain
    /// server, which handles one at a time on its own thread.
    pub fn pending(&self) -> usize {
        match &self.0 {
            Controlled::Plain(_) => 0,
            Controlled::Threaded(control) => control.pending(),
        }
    }

    /// Requests dropped: on the threaded server for a full queue or
    /// refused while leaving, on the plain server for its client's full
    /// receive queue, `BaseMultiplexerServer::incoming_dropped()`.
    pub fn dropped(&self) -> usize {
        match &self.0 {
            Controlled::Plain(shared) => {
                usize::try_from(shared.incoming_dropped.load(Ordering::Relaxed)).unwrap_or(usize::MAX)
            }
            Controlled::Threaded(control) => control.dropped(),
        }
    }
}
