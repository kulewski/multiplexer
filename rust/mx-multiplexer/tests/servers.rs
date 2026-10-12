//! The server classes against real multiplexers: a handler's replies, the
//! PING a server answers itself, a failing handler's BACKEND_ERROR and its
//! end of the server, workers serving at once, and a drain that ends once
//! every multiplexer confirmed it.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mx_multiplexer::{
    BaseMultiplexerServer, BaseThreadedMultiplexerServer, Error, Handler, HandlerResult, Message, Request, ServeError,
    SyncClient, ThreadedClient,
};
use support::testing_constants::{peers, types};
use support::{beside, Multiplexer};

const POLL: Duration = Duration::from_millis(100);

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

#[test]
fn a_threaded_server_serves_on_its_workers_at_once() {
    let mx = Multiplexer::start();
    let server = Arc::new(
        BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
            .workers(4)
            .build(UpperCase::new(false))
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
