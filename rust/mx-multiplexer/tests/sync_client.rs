//! `SyncClient` against real multiplexers: connecting, a query answered by
//! a backend, events to every backend of a type, what a query reads
//! meanwhile, receiving, a wait ended by an acknowledgement, the addresses
//! it refuses, a connect that ended while it idled, one busy connection
//! beside another, and batches: split by a receiver of any class, and a
//! DELIVERY_ERROR for one nobody took told for each message inside. Each
//! test starts the multiplexers it needs (tests/support).

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mx_multiplexer::{
    Batching, DeliveryError, Error, Message, Routing, SyncClient, ThreadedClient, AUTO_RECONNECT_TIME,
    CONNECT_ATTEMPT_SECONDS, MAX_MESSAGE_SIZE,
};
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

#[test]
fn a_connect_that_ended_while_the_client_idled_is_kept() {
    // Started and left: the connect ends while nothing runs the loop, and
    // its deadline passes too. The next call saw the deadline's timer first,
    // which cancelled the connect that had ended, and the connection came
    // up only at the reconnect, AUTO_RECONNECT_TIME later, if a call was
    // waiting then; the end of the connect comes first now.
    let mx = Multiplexer::start();
    let mut client = SyncClient::builder(peers::TEST_CLIENT).build().unwrap();
    client.connect_all([mx.address()], Some(Duration::ZERO)).unwrap();
    std::thread::sleep(CONNECT_ATTEMPT_SECONDS + Duration::from_secs(1));
    let until = Instant::now() + AUTO_RECONNECT_TIME - Duration::from_secs(1);
    while client.connections_count().unwrap() == 0 && Instant::now() < until {
        let _ = client.receive(Some(Duration::from_millis(10))); // runs the loop
    }
    assert_eq!(1, client.connections_count().unwrap(), "up before a reconnect could have brought it");
}

#[test]
fn a_busy_connection_keeps_no_other_unread() {
    // The receiver has a backlog from A in its socket and reads it one
    // message at a time while B brings a marker. Readiness was looked at
    // only when nothing was ready, so B was read once A's backlog was gone.
    const BACKLOG: usize = 5000;
    let (a, b) = (Multiplexer::start(), Multiplexer::start());
    let mut receiver = SyncClient::builder(peers::TEST_EVENT_BACKEND).connect([a.address(), b.address()]).unwrap();
    let mut from_a = SyncClient::builder(peers::TEST_EVENT_CLIENT).connect([a.address()]).unwrap();
    let mut from_b = SyncClient::builder(peers::TEST_EVENT_CLIENT).connect([b.address()]).unwrap();
    for index in 0..BACKLOG {
        from_a.send(types::TEST_EVENT, index.to_string()).queue().unwrap();
    }
    routed(&mut from_a); // the backlog is the receiver's to read now
    for _ in 0..10 {
        receiver.receive(TEN).unwrap(); // A is the connection with frames now
    }
    from_b.send(types::TEST_EVENT, "marker").queue().unwrap();
    routed(&mut from_b);
    let mut before = 0;
    while receiver.receive(TEN).unwrap().message.payload.as_ref() != b"marker" {
        before += 1;
    }
    assert!(before < BACKLOG / 2, "{before} of A's messages came before B's marker");
}

/// Returns once `sender`'s multiplexer has routed everything it sent: a
/// marker it addresses to itself comes back after it, frames from one
/// connection being routed in their order.
fn routed(sender: &mut SyncClient) {
    let own = sender.instance_id();
    let marker = sender.send(types::TEST_EVENT, "routed").to(own).wait().unwrap();
    while sender.receive(TEN).unwrap().message.id != marker {}
}

#[test]
fn a_send_waited_for_with_no_deadline_as_duration_max_is_written() {
    // Duration::MAX, the usual "forever", is no deadline everywhere else;
    // the flushing send added its grace to it and panicked on the overflow.
    let mx = Multiplexer::start();
    let mut client = SyncClient::builder(peers::TEST_EVENT_CLIENT).connect([mx.address()]).unwrap();
    let own = client.instance_id();
    let id = client.send(types::TEST_EVENT, "forever").to(own).timeout(Duration::MAX).wait().unwrap();
    assert_eq!(id, client.receive(TEN).unwrap().message.id);
}

#[test]
fn a_query_too_large_once_its_sender_is_filled_in_is_refused_at_the_call() {
    // The size check measured the request before its sender was filled in:
    // up to 11 bytes the frames carry went uncounted, and such a request
    // was sent, to fail as something else.
    let mut client = SyncClient::builder(peers::TEST_CLIENT).build().unwrap();
    let request = |payload_size: usize, sender: u64| Message {
        r#type: types::TEST_REQUEST_A,
        payload: vec![0; payload_size].into(),
        sender,
        ..Default::default()
    };
    let own = client.instance_id();
    let Err(Error::TooLarge { size, .. }) = client.query_message(request(MAX_MESSAGE_SIZE, own)).wait() else {
        panic!("a query over the limit is refused")
    };
    let envelope = size - MAX_MESSAGE_SIZE;
    match client.query_message(request(MAX_MESSAGE_SIZE - envelope + 1, 0)).wait() {
        Err(Error::TooLarge { size, .. }) => assert_eq!(MAX_MESSAGE_SIZE + 1, size),
        other => panic!("refused as too large, not {other:?}"),
    }
}

#[test]
fn a_delivery_error_for_a_batch_is_told_for_each_message() {
    // Nobody takes TEST_EVENT: the multiplexer sends the batch back whole in
    // its DELIVERY_ERROR, and the program hears one DELIVERY_ERROR for each
    // message, naming it, with its workflow and the message as the original,
    // as without batching, on a SyncClient and on a ThreadedClient.
    let mx = Multiplexer::start();
    let batching = Batching { max_records: 3, max_delay: Duration::from_secs(3600), ..Batching::default() };
    let mut client = SyncClient::builder(peers::TEST_EVENT_CLIENT).connect([mx.address()]).unwrap();
    client.set_batching(types::TEST_EVENT, None, batching).unwrap();
    let sent: Vec<u64> = (0..3)
        .map(|number| client.send(types::TEST_EVENT, format!("{number}")).workflow(format!("flow {number}")).queue())
        .collect::<Result<_, _>>()
        .unwrap();
    let told: Vec<Message> = sent.iter().map(|_| client.receive(TEN).unwrap().message).collect();
    let threaded = ThreadedClient::builder(peers::TEST_EVENT_CLIENT).connect([mx.address()]).unwrap();
    let errors = threaded.messages();
    threaded.set_batching(types::TEST_EVENT, None, batching).unwrap();
    let threaded_sent: Vec<u64> = (0..3)
        .map(|number| threaded.send(types::TEST_EVENT, format!("{number}")).workflow(format!("flow {number}")).queue())
        .collect::<Result<_, _>>()
        .unwrap();
    let threaded_told: Vec<Message> =
        threaded_sent.iter().map(|_| errors.recv(TEN).expect("a delivery error").message).collect();
    for (name, ids, told) in [("SyncClient", sent, told), ("ThreadedClient", threaded_sent, threaded_told)] {
        for (number, (id, error)) in ids.iter().zip(&told).enumerate() {
            assert_eq!(types::DELIVERY_ERROR, error.r#type, "{name}");
            assert_eq!(*id, error.references, "{name}");
            assert_eq!(Some(format!("flow {number}")), error.workflow, "{name}");
            let report = DeliveryError::decode(&error.payload).expect("a DeliveryError");
            assert_eq!(*id, report.packet_id, "{name}");
            assert_eq!(vec![peers::TEST_EVENT_BACKEND], report.failed_type, "{name}");
            let original = report.original_message.expect("the message");
            assert_eq!(*id, original.id, "{name}");
            assert_eq!(format!("{number}").as_bytes(), &original.payload[..], "{name}");
        }
    }
    threaded.shutdown(None).wait().unwrap();
}
