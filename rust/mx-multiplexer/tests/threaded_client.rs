//! `ThreadedClient` against real multiplexers: queries waited for, awaited
//! with no runtime, and called back; what arrives on its own through
//! on_message, the inbox and a subscription; a reply sent from a callback;
//! a blocking call refused on the io thread; and the shutdown.

mod support;

use std::sync::{mpsc, Arc, OnceLock};
use std::time::Duration;

use mx_multiplexer::{Error, SyncClient, ThreadedClient};
use support::testing_constants::{peers, types};
use support::{beside, block_on, Multiplexer};

const TEN: Option<Duration> = Some(Duration::from_secs(10));

/// A backend on a `SyncClient` of its own that answers `count` requests
/// with their payload upper-cased.
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
fn a_query_is_answered_waited_for_awaited_and_called_back() {
    let mx = Multiplexer::start();
    let backend = upper_case_backend(mx.address().to_string(), 3);
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    assert_eq!(1, client.connections_count().unwrap());
    let reply = client.query(types::TEST_REQUEST_A, "waited").wait().unwrap();
    assert_eq!(&b"WAITED"[..], &reply.message.payload[..]);
    let reply = block_on(async { client.query(types::TEST_REQUEST_A, "awaited").await }).unwrap();
    assert_eq!(&b"AWAITED"[..], &reply.message.payload[..]);
    let (sender, receiver) = mpsc::channel();
    client.query(types::TEST_REQUEST_A, "called back").then(move |reply| sender.send(reply).unwrap()).unwrap();
    let reply = receiver.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
    assert_eq!(&b"CALLED BACK"[..], &reply.message.payload[..]);
    assert_eq!(0, client.watched_ids().unwrap(), "every query's ids let go");
    backend.join().unwrap();
}

#[test]
fn what_arrives_on_its_own_goes_to_on_message_the_inbox_and_a_subscription() {
    let mx = Multiplexer::start();
    let (sender, receiver) = mpsc::channel();
    let backend = ThreadedClient::builder(peers::TEST_EVENT_BACKEND)
        .on_message(move |incoming| sender.send(incoming.message.id).unwrap())
        .connect([mx.address()])
        .unwrap();
    let inbox = backend.messages();
    let subscription = backend.subscribe(types::TEST_EVENT);
    let other = backend.subscribe(types::TEST_EVENT_ANY);
    // The streams are registered on the io thread: a round trip there first.
    backend.connections_count().unwrap();
    let mut client = SyncClient::builder(peers::TEST_EVENT_CLIENT).connect([mx.address()]).unwrap();
    let id = client.send(types::TEST_EVENT, "seen").wait().unwrap();
    assert_eq!(id, receiver.recv_timeout(Duration::from_secs(10)).unwrap());
    assert_eq!(Some(id), inbox.recv(TEN).map(|incoming| incoming.message.id));
    assert_eq!(Some(id), subscription.recv(TEN).map(|incoming| incoming.message.id));
    assert!(other.try_recv().is_none(), "a subscription of another type");
    backend.shutdown(TEN).wait().unwrap();
    assert_eq!(None, inbox.recv(TEN), "the stream ends with the client");
}

#[test]
fn a_backend_answers_from_its_callback_and_a_blocking_call_there_is_refused() {
    let mx = Multiplexer::start();
    let (sender, receiver) = mpsc::channel();
    let sender = std::sync::Mutex::new(Some(sender));
    // The callback answers through its own client, handed to it once
    // built: a client one of whose callbacks holds a handle lives until
    // shutdown().
    let itself: Arc<OnceLock<ThreadedClient>> = Arc::default();
    let in_callback = Arc::clone(&itself);
    let backend = ThreadedClient::builder(peers::TEST_BACKEND_A)
        .on_message(move |request| {
            let client = in_callback.get().expect("set once built");
            let refused = client.query(types::TEST_REQUEST_B, "nested").wait().map(|_| ());
            if let Some(sender) = sender.lock().unwrap().take() {
                sender.send(refused).unwrap();
            }
            let answer = request.message.payload.to_ascii_uppercase();
            client
                .send(types::TEST_RESPONSE, answer)
                .to(request.message.sender)
                .references(request.message.id)
                .connection(&request.connection)
                .queue()
                .unwrap();
        })
        .connect([mx.address()])
        .unwrap();
    itself.set(backend.clone()).unwrap();
    let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    let reply = client.query(types::TEST_REQUEST_A, "from a callback").wait().unwrap();
    assert_eq!(&b"FROM A CALLBACK"[..], &reply.message.payload[..]);
    assert_eq!(Ok(Err(Error::OnIoThread)), receiver.recv_timeout(Duration::from_secs(10)));
    backend.shutdown(TEN).wait().unwrap();
}

#[test]
fn after_shutdown_every_call_says_not_connected_and_flush_all_has_nothing_left() {
    let mx = Multiplexer::start();
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    assert_eq!(Ok(true), client.flush_all(TEN).wait());
    client.shutdown(TEN).wait().unwrap();
    assert_eq!(Err(Error::NotConnected), client.query(types::TEST_REQUEST_A, "late").wait().map(|_| ()));
    assert_eq!(Err(Error::NotConnected), client.send(types::TEST_EVENT, "late").queue());
    assert_eq!(Err(Error::NotConnected), client.connections_count());
    assert_eq!(Ok(true), client.flush_all(TEN).wait());
    client.shutdown(TEN).wait().unwrap();
}

#[test]
fn a_query_with_no_backend_fails_and_with_no_connection_waits_out_its_stage() {
    let mx = Multiplexer::start();
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    assert_eq!(Err(Error::OperationFailed), client.query(types::TEST_REQUEST_B, "anyone?").wait().map(|_| ()));
    let alone = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).build().unwrap();
    let outcome = alone.query(types::TEST_REQUEST_B, "nobody").timeout(Duration::from_millis(100)).wait();
    assert_eq!(Err(Error::NotConnected), outcome.map(|_| ()));
}
