//! `SyncClient` against real multiplexers: connecting, a query answered by
//! a backend, events to every backend of a type, what a query reads
//! meanwhile, receiving, a wait ended by an acknowledgement, and the
//! addresses it refuses. Each test starts the multiplexers it needs
//! (tests/support).

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mx_multiplexer::{Error, Routing, SyncClient};
use support::testing_constants::{peers, types};
use support::{beside, Multiplexer};

const TEN: Option<Duration> = Some(Duration::from_secs(10));

/// A wait no test waits out: something else has to end it.
const HOUR: Option<Duration> = Some(Duration::from_secs(3600));

/// A backend on a `SyncClient` of its own that answers `count` requests
/// with their payload upper-cased, then returns.
fn upper_case_backend(address: String, count: usize) -> std::thread::JoinHandle<SyncClient> {
    let mut backend = SyncClient::builder(peers::TEST_BACKEND_A).connect([address]).expect("a backend");
    beside(move || {
        for _ in 0..count {
            let request = backend.receive(TEN).expect("a request");
            let answer = request.message.payload.to_ascii_uppercase();
            backend
                .send(types::TEST_RESPONSE, answer)
                .to(request.message.sender)
                .references(request.message.id)
                .connection(&request.connection)
                .wait()
                .expect("the reply written");
        }
        backend
    })
}

#[test]
fn a_query_is_answered_by_a_backend() {
    let mx = Multiplexer::start();
    let backend = upper_case_backend(mx.address().to_string(), 1);
    let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    assert_eq!(1, client.connections_count().unwrap());
    let reply = client.query(types::TEST_REQUEST_A, "hello").wait().unwrap();
    assert_eq!(&b"HELLO"[..], &reply.message.payload[..]);
    assert_eq!(types::TEST_RESPONSE, reply.message.r#type);
    assert!(reply.connection.is_live());
    backend.join().unwrap();
}

#[test]
fn an_event_reaches_every_backend_of_its_type() {
    let mx = Multiplexer::start();
    let mut backends: Vec<SyncClient> =
        (0..2).map(|_| SyncClient::builder(peers::TEST_EVENT_BACKEND).connect([mx.address()]).unwrap()).collect();
    let mut client = SyncClient::builder(peers::TEST_EVENT_CLIENT).connect([mx.address()]).unwrap();
    let id = client.send(types::TEST_EVENT, "seen").wait().unwrap();
    for backend in &mut backends {
        let event = backend.receive(TEN).unwrap();
        assert_eq!((id, types::TEST_EVENT), (event.message.id, event.message.r#type));
        assert_eq!(client.instance_id(), event.message.sender);
    }
}

#[test]
fn a_query_with_no_backend_fails_at_once() {
    let mx = Multiplexer::start();
    let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    assert_eq!(Err(Error::OperationFailed), client.query(types::TEST_REQUEST_B, "anyone?").wait().map(|_| ()));
}

#[test]
fn a_receive_times_out_and_with_nothing_connected_and_no_deadline_says_not_connected() {
    let mx = Multiplexer::start();
    let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    assert_eq!(Err(Error::OperationTimedOut), client.receive(Some(Duration::from_millis(50))).map(|_| ()));
    let mut alone = SyncClient::builder(peers::TEST_CLIENT).build().unwrap();
    assert_eq!(Err(Error::NotConnected), alone.receive(None).map(|_| ()));
    assert_eq!(Err(Error::OperationTimedOut), alone.receive(Some(Duration::from_millis(50))).map(|_| ()));
}

#[test]
fn a_wait_ends_at_an_acknowledgement_the_last_receive_made() {
    // Events routed to the receiver, then a change of its routing whose
    // confirmation it reads with them before receiving any, as
    // multiplexer/acknowledged_test.py arranges it: what the receiver sends
    // after the change reaches the sender after the multiplexer took the
    // change, and its next send reads its sockets first. The receive that
    // takes the last event acknowledges the change; the next receive that
    // ends at an acknowledgement runs the callback and ends at once rather
    // than wait out its hour.
    const EVENTS: usize = 50;
    let mx = Multiplexer::start();
    let mut receiver = SyncClient::builder(peers::TEST_EVENT_BACKEND).connect([mx.address()]).unwrap();
    let mut sender = SyncClient::builder(peers::TEST_EVENT_CLIENT).connect([mx.address()]).unwrap();
    let sender_id = sender.instance_id();
    for index in 0..EVENTS {
        sender.send(types::TEST_EVENT, index.to_string()).queue().unwrap();
    }
    // Routed once the sender's marker, addressed to itself, comes back:
    // frames from one connection are routed in their order.
    let marker = sender.send(types::TEST_EVENT, "marker").to(sender_id).wait().unwrap();
    while sender.receive(TEN).unwrap().message.id != marker {}
    let heard = Arc::new(AtomicUsize::new(0));
    let hearing = Arc::clone(&heard);
    let routing = Routing { all: false, ..Routing::default() };
    receiver
        .set_routing_then(routing, move || {
            hearing.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    receiver.send(types::TEST_EVENT, "after").to(sender_id).wait().unwrap();
    while sender.receive(TEN).unwrap().message.payload.as_ref() != b"after" {}
    receiver.send(types::TEST_EVENT, "poll").to(sender_id).wait().unwrap();
    for index in 0..EVENTS {
        assert_eq!(index.to_string().as_bytes(), &receiver.receive(TEN).unwrap().message.payload[..]);
    }
    assert!(receiver.routing_acknowledged().unwrap());
    assert_eq!(0, heard.load(Ordering::SeqCst), "its callback waits for the next receive");
    assert_eq!(Err(Error::OperationTimedOut), receiver.receive_until_acknowledged(HOUR).map(|_| ()));
    assert_eq!(1, heard.load(Ordering::SeqCst), "run before the wait ended");
}

#[test]
fn an_address_that_does_not_parse_is_refused_before_anything_connects() {
    let refusal = SyncClient::builder(peers::TEST_CLIENT).connect(["127.0.0.1:1980", "::1:1980"]).map(|_| ());
    assert_eq!(
        Err(Error::InvalidArgument("'::1:1980': an IPv6 address goes in brackets, as in [::1]:1980".to_string())),
        refusal
    );
}

#[test]
fn a_query_survives_its_multiplexers_restart() {
    let mut mx = Multiplexer::start();
    let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    mx.restart();
    let backend = upper_case_backend(mx.address().to_string(), 1);
    // The next call notices the dead connection, holds the request until the
    // reconnect, about 3 s later, and sends it then.
    let reply = client.query(types::TEST_REQUEST_A, "again").wait().unwrap();
    assert_eq!(&b"AGAIN"[..], &reply.message.payload[..]);
    backend.join().unwrap();
}
