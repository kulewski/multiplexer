//! The server classes against real multiplexers: a handler's replies, the
//! PING a server answers itself, a failing handler's BACKEND_ERROR and its
//! end of the server, workers serving at once, a drain that ends once every
//! multiplexer confirmed it and gets no new request meanwhile, and the
//! plain server reached from another thread and from its own callbacks: a
//! call waits for no poll, a drain started there waits for its
//! confirmation, and a callback may use the handle and answer kept
//! requests; the plain server's count of what its client gave up on; a
//! panic out of the serving loop closing either server first; a threaded
//! server's burst of queue-full drops said in the client's drop lines; and
//! a drain that waits for what its server holds for a connection, every
//! multiplexer down, and ends on its cap whatever the poll. Every wait on
//! the server is bounded only to detect a failure: the plain server's poll
//! is an hour where a test says so.

mod support;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, Once, OnceLock};
use std::time::{Duration, Instant};

use mx_multiplexer::{
    BaseMultiplexerServer, BaseThreadedMultiplexerServer, Batching, Error, Handler, HandlerResult, Message, Request,
    Routing, ServeError, ServerHandle, SyncClient, ThreadedClient,
};
use support::testing_constants::{peers, types};
use support::{beside, thread_id, wait_in_epoll_wait, Multiplexer};

const POLL: Duration = Duration::from_millis(100);

/// A poll no test waits out: something else has to end the wait.
const HOUR: Duration = Duration::from_secs(3600);

/// What only a failure reaches.
const BOUND: Duration = Duration::from_secs(10);

/// Every line of WARN or above any thread of this process logged, once
/// `capture_lines()` installed the logger: what the library says on its
/// own threads.
static LINES: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct Lines;

impl log::Log for Lines {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Warn
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            LINES.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).push(record.args().to_string());
        }
    }

    fn flush(&self) {}
}

/// Installs the logger that keeps `LINES`, once per process.
fn capture_lines() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = log::set_logger(&Lines);
        log::set_max_level(log::LevelFilter::Warn);
    });
}

/// The lines logged so far that contain `text`.
fn logged(text: &str) -> Vec<String> {
    LINES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .filter(|line| line.contains(text))
        .cloned()
        .collect()
}

/// Waits, BOUND at most, for `done`; whether it came.
fn eventually(done: impl Fn() -> bool) -> bool {
    let until = Instant::now() + BOUND;
    while !done() {
        if Instant::now() >= until {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    true
}

/// `body` on a thread of its own, its result within BOUND or None: a call
/// that should come back at once, and on the code before waited for good.
fn within_bound<T: Send + 'static>(body: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    let (done, result) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(body());
    });
    result.recv_timeout(BOUND).ok()
}

/// Upper-cases what it is asked; fails on "fail", with the server ending
/// when told to.
struct UpperCase {
    end_on_failure: bool,
    handled: AtomicUsize,
}

impl UpperCase {
    fn new(end_on_failure: bool) -> UpperCase {
        UpperCase { end_on_failure, handled: AtomicUsize::new(0) }
    }
}

impl Handler for UpperCase {
    fn handle_message(&self, request: Request) -> HandlerResult {
        self.handled.fetch_add(1, Ordering::SeqCst);
        if request.payload().as_ref() == b"fail" {
            return Err("asked to fail".into());
        }
        request.reply(types::TEST_RESPONSE, request.payload().to_ascii_uppercase())?;
        Ok(())
    }

    fn on_handler_error(&self, _: &mx_multiplexer::HandlerError) -> bool {
        !self.end_on_failure
    }
}

#[test]
fn a_plain_server_answers_queries_and_pings_and_reports_a_failure() {
    let mx = Multiplexer::start();
    let mut server =
        BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()]).build(UpperCase::new(false)).unwrap();
    server.connect().unwrap();
    let (handle, instance) = (server.handle(), server.instance_id());
    let serving =
        beside(move || server.serve_forever(POLL, None).map(|()| server.handler().handled.load(Ordering::SeqCst)));
    let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    let reply = client.query(types::TEST_REQUEST_A, "plain").wait().unwrap();
    assert_eq!(&b"PLAIN"[..], &reply.message.payload[..]);
    let ping = Message { r#type: types::PING, to: Some(instance), payload: "echo".into(), ..Default::default() };
    let echo = client.query_message(ping).wait().unwrap();
    assert_eq!((types::PING, &b"echo"[..]), (echo.message.r#type, &echo.message.payload[..]));
    match client.query(types::TEST_REQUEST_A, "fail").wait() {
        Err(Error::Backend(reply)) => assert_eq!(&b"asked to fail"[..], &reply.message.payload[..]),
        other => panic!("{other:?}"),
    }
    handle.stop();
    assert_eq!(2, serving.join().unwrap().unwrap(), "two requests reached the handler");
}

#[test]
fn a_plain_server_counts_what_its_client_gave_up_on() {
    // Sent with no connection, a message is held; the close gives it up,
    // and the server's count is its client's, kept past the close.
    let mut server =
        BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, ["127.0.0.1:1"]).build(UpperCase::new(false)).unwrap();
    server.handle().send(types::TEST_REQUEST_A, "held").unwrap();
    assert_eq!((0, 0), (server.dropped(), server.incoming_dropped()));
    server.close(Some(Duration::ZERO));
    assert_eq!((1, 0), (server.dropped(), server.incoming_dropped()));
    assert_eq!(0, server.handle().dropped(), "no request dropped");
}

/// Panics in its first periodic_task(), as a bug in the program would.
struct PanicsInPeriodicTask;

impl Handler for PanicsInPeriodicTask {
    fn handle_message(&self, request: Request) -> HandlerResult {
        request.no_response();
        Ok(())
    }

    fn periodic_task(&self, _: &ServerHandle) {
        panic!("periodic_task panicked on purpose");
    }
}

#[test]
fn a_panic_out_of_the_serving_loop_closes_the_server_first() {
    // As C++ and Python close it before the exception goes on: the server
    // is closed, its client shut down, though its caller keeps it.
    let mx = Multiplexer::start();
    let mut plain =
        BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()]).build(PanicsInPeriodicTask).unwrap();
    let served = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| plain.serve_forever(POLL, None)));
    assert!(served.is_err(), "the panic goes on up");
    let connections = plain.handle().connections_count();
    assert!(!matches!(connections, Ok(count) if count > 0), "the plain server closed: {connections:?}");
    let threaded = BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
        .build(PanicsInPeriodicTask)
        .unwrap();
    let served = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| threaded.serve_forever(POLL, None)));
    assert!(served.is_err(), "the panic goes on up");
    let connections = threaded.handle().connections_count();
    assert!(!matches!(connections, Ok(count) if count > 0), "the threaded server closed: {connections:?}");
}

#[test]
fn a_handler_whose_failure_ends_the_server_ends_serve_forever_with_it() {
    let mx = Multiplexer::start();
    let mut server =
        BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()]).build(UpperCase::new(true)).unwrap();
    server.connect().unwrap();
    let serving = beside(move || server.serve_forever(POLL, None));
    let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    assert!(matches!(client.query(types::TEST_REQUEST_A, "fail").wait(), Err(Error::Backend(_))));
    match serving.join().unwrap() {
        Err(ServeError::Handler(error)) => assert_eq!("asked to fail", error.to_string()),
        other => panic!("{other:?}"),
    }
}

/// Holds each request until four are held at once, BOUND at most, then
/// answers it upper-cased; the most it held at once.
#[derive(Default)]
struct Gathers {
    held: Mutex<usize>,
    changed: Condvar,
    most: AtomicUsize,
    handled: AtomicUsize,
}

impl Handler for Gathers {
    fn handle_message(&self, request: Request) -> HandlerResult {
        let mut held = self.held.lock().unwrap();
        *held += 1;
        self.most.fetch_max(*held, Ordering::SeqCst);
        self.changed.notify_all();
        let (mut held, _) =
            self.changed.wait_timeout_while(held, BOUND, |_| self.most.load(Ordering::SeqCst) < 4).unwrap();
        *held -= 1;
        drop(held);
        self.handled.fetch_add(1, Ordering::SeqCst);
        request.reply(types::TEST_RESPONSE, request.payload().to_ascii_uppercase())?;
        Ok(())
    }
}

#[test]
fn a_threaded_server_serves_on_its_workers_at_once() {
    // Eight queries at once to four workers: four handlers hold theirs at
    // the same time, which one worker, or two, never reach.
    let mx = Multiplexer::start();
    let server = Arc::new(
        BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
            .workers(4)
            .build(Gathers::default())
            .unwrap(),
    );
    server.connect().unwrap();
    let serving = {
        let server = Arc::clone(&server);
        beside(move || server.serve_forever(POLL, None))
    };
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    let askers: Vec<_> = (0..8)
        .map(|index| {
            let client = client.clone();
            beside(move || {
                let reply = client.query(types::TEST_REQUEST_A, format!("ask {index}")).wait().unwrap();
                assert_eq!(format!("ASK {index}").as_bytes(), &reply.message.payload[..]);
            })
        })
        .collect();
    for asker in askers {
        asker.join().unwrap();
    }
    server.handle().stop();
    serving.join().unwrap().unwrap();
    assert_eq!(8, server.handler().handled.load(Ordering::SeqCst));
    assert_eq!(4, server.handler().most.load(Ordering::SeqCst), "four handlers at once");
}

#[test]
fn a_drain_ends_once_every_multiplexer_confirmed_it_and_new_queries_go_elsewhere() {
    let mx = Multiplexer::start();
    for threaded in [false, true] {
        let draining = UpperCase::new(false);
        let other = BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
            .build(UpperCase::new(false))
            .unwrap();
        other.connect().unwrap();
        let other_handle = other.handle();
        let other_serving = beside(move || other.serve_forever(POLL, None));
        let serving = if threaded {
            let server =
                BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()]).build(draining).unwrap();
            server.connect().unwrap();
            let handle = server.handle();
            handle.start_draining();
            beside(move || server.serve_forever(POLL, None))
        } else {
            let mut server =
                BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()]).build(draining).unwrap();
            server.connect().unwrap();
            server.handle().start_draining();
            beside(move || server.serve_forever(POLL, None))
        };
        serving.join().unwrap().unwrap(); // over once the multiplexer confirmed the drain
        let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
        let reply = client.query(types::TEST_REQUEST_A, "elsewhere").wait().unwrap();
        assert_eq!(&b"ELSEWHERE"[..], &reply.message.payload[..]);
        other_handle.stop();
        other_serving.join().unwrap().unwrap();
    }
}

/// Counts its periodic tasks, keeps the requests it is told to, answering
/// the rest, and says it is drained only once let go.
#[derive(Default)]
struct Keeps {
    turns: Mutex<usize>,
    turned: Condvar,
    kept: Mutex<Vec<Request>>,
    handled: AtomicUsize,
    let_go: AtomicBool,
}

impl Keeps {
    /// The periodic tasks the loop ran so far.
    fn turns(&self) -> usize {
        *self.turns.lock().unwrap()
    }

    /// Waits until the loop ran `count` periodic tasks in all; whether it
    /// did within BOUND.
    fn turns_reach(&self, count: usize) -> bool {
        let turns = self.turns.lock().unwrap();
        let (turns, waited) = self.turned.wait_timeout_while(turns, BOUND, |turns| *turns < count).unwrap();
        drop(turns);
        !waited.timed_out()
    }
}

impl Handler for Keeps {
    fn handle_message(&self, request: Request) -> HandlerResult {
        self.handled.fetch_add(1, Ordering::SeqCst);
        if request.payload().as_ref() == b"keep" {
            self.kept.lock().unwrap().push(request);
            return Ok(());
        }
        request.reply(types::TEST_RESPONSE, request.payload().to_ascii_uppercase())?;
        Ok(())
    }

    fn periodic_task(&self, _: &ServerHandle) {
        *self.turns.lock().unwrap() += 1;
        self.turned.notify_all();
    }

    fn drained(&self, by_default: bool) -> bool {
        by_default && self.let_go.load(Ordering::SeqCst)
    }
}

/// A `Keeps` the test holds too.
struct Shared(Arc<Keeps>);

impl Handler for Shared {
    fn handle_message(&self, request: Request) -> HandlerResult {
        self.0.handle_message(request)
    }

    fn periodic_task(&self, server: &ServerHandle) {
        self.0.periodic_task(server)
    }

    fn drained(&self, by_default: bool) -> bool {
        self.0.drained(by_default)
    }
}

#[test]
fn a_call_from_another_thread_waits_for_no_poll_of_the_plain_server() {
    // The loop holds the client through its wait, an hour here, and took
    // it again at once after each: a call from another thread waited for
    // the wait to end, and then lost the race for the lock, poll after
    // poll. A kept request answered from another thread went out as late,
    // and stop() was noticed at the next poll.
    let mx = Multiplexer::start();
    let keeps = Arc::new(Keeps::default());
    let mut server = BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
        .build(Shared(Arc::clone(&keeps)))
        .unwrap();
    server.connect().unwrap();
    let handle = server.handle();
    let (named, serving_thread) = mpsc::channel();
    let serving = beside(move || {
        named.send(thread_id()).unwrap();
        server.serve_forever(HOUR, None)
    });
    let serving_thread = serving_thread.recv().unwrap();
    wait_in_epoll_wait(serving_thread); // in its wait, the client held
    let counting = handle.clone();
    assert_eq!(Some(Ok(1)), within_bound(move || counting.connections_count()), "a call waited for the poll");
    let before = keeps.turns(); // a call's turn is no poll: none ran
    let asking = mx.address().to_string();
    let query = beside(move || {
        let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([asking]).unwrap();
        client.query(types::TEST_REQUEST_A, "keep").timeout(BOUND).wait()
    });
    let request = loop {
        if let Some(request) = keeps.kept.lock().unwrap().pop() {
            break request;
        }
        std::thread::yield_now();
    };
    assert!(keeps.turns_reach(before + 1)); // the loop handled it, ran its task, and waits again
    wait_in_epoll_wait(serving_thread);
    let answered = within_bound(move || request.reply(types::TEST_RESPONSE, "from another thread"));
    assert!(matches!(answered, Some(Ok(()))), "a kept request's reply waited for the poll");
    let reply = query.join().unwrap().unwrap();
    assert_eq!(&b"from another thread"[..], &reply.message.payload[..]);
    let stopping = handle.clone();
    assert!(within_bound(move || stopping.stop()).is_some());
    assert!(
        within_bound(move || serving.join().unwrap()).is_some_and(|served| served.is_ok()),
        "stop() waited for the poll"
    );
}

#[test]
fn a_plain_servers_callbacks_may_use_its_handle_and_answer_kept_requests() {
    // An acknowledged change's callback and the drop observer ran inside
    // the loop's wait, where the loop held the client: a callback that
    // sent through the handle, or answered a kept request, waited for the
    // client its own thread held, for good.
    let mx = Multiplexer::start();
    let keeps = Arc::new(Keeps::default());
    let mut server = BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
        .build(Shared(Arc::clone(&keeps)))
        .unwrap();
    server.connect().unwrap();
    let handle = server.handle();
    let serving = beside(move || server.serve_forever(POLL, None));
    let asking = mx.address().to_string();
    let query = beside(move || {
        let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([asking]).unwrap();
        client.query(types::TEST_REQUEST_A, "keep").timeout(BOUND).wait()
    });
    while keeps.kept.lock().unwrap().is_empty() {
        std::thread::yield_now();
    }
    let (kept, notes) = (Arc::clone(&keeps), handle.clone());
    let filters = std::collections::BTreeMap::from([(types::TEST_REQUEST_A, vec![])]);
    handle
        .set_key_filters_then(filters, move || {
            let request = kept.kept.lock().unwrap().pop().expect("the kept request");
            request.reply(types::TEST_RESPONSE, "from the callback").unwrap();
            notes.send(types::TEST_UNROUTED, "acknowledged").unwrap();
        })
        .unwrap();
    let reply = query.join().unwrap();
    assert_eq!(&b"from the callback"[..], &reply.unwrap().message.payload[..], "the callback waited for good");
    handle.stop();
    assert!(within_bound(move || serving.join().unwrap()).is_some_and(|served| served.is_ok()));
}

#[test]
fn a_plain_servers_drop_observer_may_use_its_handle() {
    // A message held for a connection that never comes is given up on at
    // the close, and its observer, which calls the handle, ran inside the
    // client's shutdown, with the client held.
    let slot: Arc<OnceLock<ServerHandle>> = Arc::new(OnceLock::new());
    let told = Arc::new(AtomicUsize::new(0));
    let (observer_slot, observer_told) = (Arc::clone(&slot), Arc::clone(&told));
    let mut server = BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, ["127.0.0.1:9"])
        .on_drop(move |_, _| {
            let _ = observer_slot.get().expect("the handle").connections_count();
            observer_told.fetch_add(1, Ordering::SeqCst);
        })
        .build(Keeps::default())
        .unwrap();
    let handle = server.handle();
    slot.set(handle.clone()).ok();
    handle.send(types::TEST_EVENT, "held").unwrap(); // nothing connected: held
    let closing = within_bound(move || server.close(Some(Duration::ZERO)));
    assert!(closing.is_some(), "the close waited for good");
    assert_eq!(1, told.load(Ordering::SeqCst));
}

#[test]
fn a_drain_started_from_another_thread_waits_for_its_confirmation() {
    // The drain was published before its routing was set: the loop, its
    // wait over, took the routing in effect before for the drain's,
    // acknowledged, and left before any multiplexer heard of the drain.
    // The multiplexer frozen, no confirmation can come: the loop serves on.
    let mx = Multiplexer::start();
    let keeps = Arc::new(Keeps::default());
    keeps.let_go.store(true, Ordering::SeqCst);
    let mut server = BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
        .build(Shared(Arc::clone(&keeps)))
        .unwrap();
    server.connect().unwrap();
    let handle = server.handle();
    let (ended, ending) = mpsc::channel();
    let serving = beside(move || {
        let served = server.serve_forever(POLL, None);
        ended.send(()).unwrap();
        served
    });
    mx.freeze();
    let draining = handle.clone();
    assert!(within_bound(move || draining.start_draining()).is_some());
    assert!(keeps.turns_reach(keeps.turns() + 5), "the loop goes on serving");
    assert!(ending.try_recv().is_err(), "a drain over before any multiplexer heard of it");
    mx.thaw();
    assert!(ending.recv_timeout(BOUND).is_ok(), "the drain over at the confirmation");
    serving.join().unwrap().unwrap();
}

#[test]
fn a_draining_server_gets_no_new_request() {
    // Once every multiplexer confirmed the drain, a new query goes to the
    // other backend, the draining one still connected and serving: the
    // test of the drain checked the queries only after the draining server
    // had left, which they would avoid whatever the routing said.
    let mx = Multiplexer::start();
    let other = BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
        .build(UpperCase::new(false))
        .unwrap();
    other.connect().unwrap();
    let other_handle = other.handle();
    let other_serving = beside(move || other.serve_forever(POLL, None));
    for threaded in [false, true] {
        let keeps = Arc::new(Keeps::default());
        let (handle, serving) = if threaded {
            let server = BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
                .build(Shared(Arc::clone(&keeps)))
                .unwrap();
            server.connect().unwrap();
            let handle = server.handle();
            (handle, beside(move || server.serve_forever(POLL, None)))
        } else {
            let mut server = BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
                .build(Shared(Arc::clone(&keeps)))
                .unwrap();
            server.connect().unwrap();
            let handle = server.handle();
            (handle, beside(move || server.serve_forever(POLL, None)))
        };
        handle.start_draining();
        let acknowledged = handle.clone();
        while !within_bound({
            let acknowledged = acknowledged.clone();
            move || acknowledged.routing_acknowledged().unwrap()
        })
        .unwrap_or(false)
        {
            std::thread::yield_now();
        }
        let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
        for index in 0..10 {
            let reply = client.query(types::TEST_REQUEST_A, format!("new {index}")).wait().unwrap();
            assert_eq!(format!("NEW {index}").as_bytes(), &reply.message.payload[..]);
        }
        assert_eq!(0, keeps.handled.load(Ordering::SeqCst), "threaded: {threaded}: none to the draining server");
        keeps.let_go.store(true, Ordering::SeqCst);
        handle.stop();
        serving.join().unwrap().unwrap();
    }
    other_handle.stop();
    other_serving.join().unwrap().unwrap();
}

/// Fails every request, and panics in on_handler_error().
struct PanicsOnFailure;

impl Handler for PanicsOnFailure {
    fn handle_message(&self, _: Request) -> HandlerResult {
        Err("asked to fail".into())
    }

    fn on_handler_error(&self, _: &mx_multiplexer::HandlerError) -> bool {
        panic!("on purpose")
    }
}

#[test]
fn a_panic_in_on_handler_error_ends_the_threaded_server() {
    // The panic killed the worker past its request: counted as being
    // handled for good, it kept a drain from ending, and serve_forever()
    // never heard of it. It ends the server now, as an exception out of
    // on_handler_exception() does in C++ and Python.
    let mx = Multiplexer::start();
    let server = Arc::new(
        BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()]).build(PanicsOnFailure).unwrap(),
    );
    server.connect().unwrap();
    let serving = {
        let server = Arc::clone(&server);
        beside(move || server.serve_forever(POLL, None))
    };
    let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    assert!(matches!(client.query(types::TEST_REQUEST_A, "fail").wait(), Err(Error::Backend(_))));
    match within_bound(move || serving.join().unwrap()) {
        Some(Err(ServeError::Handler(error))) => assert!(error.to_string().contains("on purpose"), "{error}"),
        other => panic!("serve_forever(): {other:?}"),
    }
    assert_eq!(0, server.handle().pending(), "the request done with");
}

/// Holds the first request until released, BOUND at most, and answers
/// every one upper-cased.
#[derive(Default)]
struct HoldsFirst {
    started: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
}

impl HoldsFirst {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

impl Handler for HoldsFirst {
    fn handle_message(&self, request: Request) -> HandlerResult {
        if !self.started.swap(true, Ordering::SeqCst) {
            let released = self.released.lock().unwrap();
            drop(self.changed.wait_timeout_while(released, BOUND, |released| !*released).unwrap());
        }
        request.reply(types::TEST_RESPONSE, request.payload().to_ascii_uppercase())?;
        Ok(())
    }
}

#[test]
fn a_threaded_servers_queue_full_drops_are_said_once_the_burst_is_over() {
    // One worker and no queue: the worker holds the first request, and the
    // four behind it are dropped. The first drop is logged at once, the
    // other three in the client's drop lines' summary a second later, with
    // no later drop needed to bring it.
    capture_lines();
    let mx = Multiplexer::start();
    let server = Arc::new(
        BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
            .workers(1)
            .queue_size(0)
            .build(HoldsFirst::default())
            .unwrap(),
    );
    server.connect().unwrap();
    let serving = {
        let server = Arc::clone(&server);
        beside(move || server.serve_forever(POLL, None))
    };
    let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    for _ in 0..5 {
        client.send(types::TEST_REQUEST_A, "burst").wait().unwrap();
    }
    assert!(eventually(|| server.handle().dropped() == 4), "dropped: {}", server.handle().dropped());
    let summary = "requests dropped: queue full [3 more in the last";
    assert!(eventually(|| !logged(summary).is_empty()), "{:?}", logged("queue full"));
    let first: Vec<String> =
        logged("dropped: queue full").into_iter().filter(|line| line.starts_with("request #")).collect();
    assert_eq!(1, first.len(), "the first drop, logged at once: {first:?}");
    server.handler().release();
    server.handle().stop();
    serving.join().unwrap().unwrap();
}

#[test]
fn a_threaded_server_with_no_queue_limit_takes_every_request() {
    // queue_size(usize::MAX), no limit, overflowed the capacity it is
    // added to, which wrapped to nothing: every request was dropped.
    let mx = Multiplexer::start();
    let server = Arc::new(
        BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
            .queue_size(usize::MAX)
            .build(UpperCase::new(false))
            .unwrap(),
    );
    server.connect().unwrap();
    let serving = {
        let server = Arc::clone(&server);
        beside(move || server.serve_forever(POLL, None))
    };
    let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    let reply = client.query(types::TEST_REQUEST_A, "taken").wait().unwrap();
    assert_eq!(&b"TAKEN"[..], &reply.message.payload[..]);
    assert_eq!(0, server.handle().dropped());
    server.handle().stop();
    serving.join().unwrap().unwrap();
}

/// Holds every request until let go, then answers it.
#[derive(Default)]
struct Holds {
    /// A request is held; the test let it go.
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl Holds {
    /// Waits, BOUND at most, until a request is held; whether one is.
    fn holding(&self) -> bool {
        let state = self.state.lock().unwrap();
        self.changed.wait_timeout_while(state, BOUND, |(holding, _)| !*holding).unwrap().0 .0
    }

    /// Lets the held request go.
    fn let_go(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}

/// A `Holds` the test holds too.
struct HoldsShared(Arc<Holds>);

impl Handler for HoldsShared {
    fn handle_message(&self, request: Request) -> HandlerResult {
        {
            let mut state = self.0.state.lock().unwrap();
            state.0 = true;
            self.0.changed.notify_all();
            drop(self.0.changed.wait_timeout_while(state, BOUND, |(_, let_go)| !*let_go).unwrap());
        }
        request.reply(types::TEST_RESPONSE, "held")?;
        Ok(())
    }
}

#[test]
fn a_drain_waits_for_what_it_holds_for_a_connection() {
    // A server drains while its only multiplexer is down, a request in its
    // handler: the reply is held for the next connection, and the drain
    // waits for it, the multiplexer back, where the confirmation, with no
    // connection, held at once and the close dropped the reply. Counted:
    // what the server's client gave up on.
    for threaded in [false, true] {
        let mut mx = Multiplexer::start();
        let holds = Arc::new(Holds::default());
        let (handle, serving) = if threaded {
            let server = BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
                .build(HoldsShared(Arc::clone(&holds)))
                .unwrap();
            server.connect().unwrap();
            let handle = server.handle();
            (handle, beside(move || server.serve_forever(HOUR, Some(HOUR)).map(|()| server.client().dropped())))
        } else {
            let mut server = BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
                .build(HoldsShared(Arc::clone(&holds)))
                .unwrap();
            server.connect().unwrap();
            let handle = server.handle();
            (handle, beside(move || server.serve_forever(HOUR, Some(HOUR)).map(|()| server.dropped())))
        };
        let mut asking = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
        asking.send(types::TEST_REQUEST_A, "hold").queue().unwrap();
        assert!(asking.flush_all(Some(BOUND)).unwrap());
        assert!(holds.holding(), "threaded: {threaded}: the request reached the handler, which holds it");
        mx.stop(); // every connection to it ends
        if threaded {
            // Its io thread sees the end at once; the plain server's client,
            // run by its loop alone, at the reply's send.
            assert!(eventually(|| handle.connections_count().is_ok_and(|count| count == 0)));
        }
        handle.start_draining();
        holds.let_go();
        // The reply held; a server that did not wait for it has left by
        // now, and what it dropped is checked below.
        assert!(
            eventually(|| serving.is_finished() || !matches!(handle.held_for_a_connection(), Ok(0))),
            "threaded: {threaded}: the reply held"
        );
        mx.start_again(); // back on the same address
        let served = within_bound(move || serving.join().unwrap());
        assert!(served.is_some(), "threaded: {threaded}: the drain over once the reply went");
        assert_eq!(0, served.unwrap().unwrap(), "threaded: {threaded}: the reply written once a connection was back");
    }
}

/// Answers nothing; for a server that only sends.
struct Quiet;

impl Handler for Quiet {
    fn handle_message(&self, request: Request) -> HandlerResult {
        request.no_response();
        Ok(())
    }
}

#[test]
fn a_drain_waits_for_what_waits_in_a_batch() {
    // A server drains while its only multiplexer is down, an event it sent
    // waiting in a batch, its delay an hour: the drain lets the batch go,
    // to be held for the next connection, and waits for it, the
    // multiplexer back, where the drain ended at once, nothing being held,
    // and the close dropped the batch. Counted: what the server's client
    // gave up on.
    for threaded in [false, true] {
        let mut mx = Multiplexer::start();
        let (handle, serving) = if threaded {
            let server =
                BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()]).build(Quiet).unwrap();
            server.connect().unwrap();
            let handle = server.handle();
            (handle, beside(move || server.serve_forever(HOUR, Some(HOUR)).map(|()| server.client().dropped())))
        } else {
            let mut server =
                BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()]).build(Quiet).unwrap();
            server.connect().unwrap();
            let handle = server.handle();
            (handle, beside(move || server.serve_forever(HOUR, Some(HOUR)).map(|()| server.dropped())))
        };
        let batching = Batching { max_delay: HOUR, ..Batching::default() };
        handle.set_batching(types::TEST_EVENT, None, batching).unwrap();
        mx.stop(); // every connection to it ends
        if threaded {
            assert!(eventually(|| handle.connections_count().is_ok_and(|count| count == 0)));
        }
        handle.send(types::TEST_EVENT, "batched").unwrap();
        handle.start_draining();
        assert!(
            eventually(|| serving.is_finished() || !matches!(handle.held_for_a_connection(), Ok(0))),
            "threaded: {threaded}: the batch let go, and held"
        );
        mx.start_again(); // back on the same address
        let served = within_bound(move || serving.join().unwrap());
        assert!(served.is_some(), "threaded: {threaded}: the drain over once the batch went");
        assert_eq!(0, served.unwrap().unwrap(), "threaded: {threaded}: the batch written once a connection was back");
    }
}

#[test]
fn a_drain_ends_on_its_cap_whatever_the_poll() {
    // A drain that keeps a path open ends on its cap alone: with a poll of
    // an hour the loop's wait ends at the cap, where nothing woke the loop
    // after the confirmation and the drain went on for good.
    let mx = Multiplexer::start();
    let events = Routing { any: false, all: true, last_resort: false };
    let cap = Some(Duration::from_secs(1));
    for threaded in [false, true] {
        let (handle, serving) = if threaded {
            let server = BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
                .drain_routing(events)
                .build(UpperCase::new(false))
                .unwrap();
            server.connect().unwrap();
            let handle = server.handle();
            (handle, beside(move || server.serve_forever(HOUR, cap)))
        } else {
            let mut server = BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
                .drain_routing(events)
                .build(UpperCase::new(false))
                .unwrap();
            server.connect().unwrap();
            let handle = server.handle();
            (handle, beside(move || server.serve_forever(HOUR, cap)))
        };
        handle.start_draining();
        assert!(
            within_bound(move || serving.join().unwrap()).is_some_and(|served| served.is_ok()),
            "threaded: {threaded}: the drain over at its cap"
        );
    }
}
