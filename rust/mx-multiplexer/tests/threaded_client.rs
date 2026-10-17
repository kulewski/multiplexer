//! `ThreadedClient` against real multiplexers: queries waited for, awaited
//! with no runtime, and called back; what arrives on its own through
//! on_message, the inbox and a subscription; a reply sent from a callback;
//! a blocking call refused on the io thread; the shutdown; a message that
//! arrives while other threads keep the io thread busy; and plain sends,
//! which go to the io thread as data, batched as their bytes beside sends
//! followed, and written out by the shutdown.

mod support;

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use mx_multiplexer::{Delivery, Error, Message, Messages, SyncClient, ThreadedClient, MAX_MESSAGE_SIZE};
use support::testing_constants::{peers, types};
use support::{arrived_by_key, beside, block_on, Multiplexer};

const TEN: Option<Duration> = Some(Duration::from_secs(10));

/// A wait no test waits out: something else has to end it.
const HOUR: Duration = Duration::from_secs(3600);

/// What only a failure reaches.
const BOUND: Duration = Duration::from_secs(10);

/// Whether `stream` has ended, asked once, with no wait: a `recv` with a
/// timeout cannot tell the end from silence.
fn ended(stream: &Messages) -> bool {
    let mut next = std::pin::pin!(stream.next());
    matches!(next.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(None))
}

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
    assert!(ended(&inbox), "the inbox ends with the client");
    assert!(ended(&subscription) && ended(&other), "and so does every subscription");
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

#[test]
fn a_message_that_arrives_while_other_threads_send_reaches_on_message() {
    // Two threads post sends as fast as they can. What the io thread ran
    // took in every command posted meanwhile, so while posts kept coming no
    // pass ran: no socket was read and no timer ran until the flow stopped.
    const SENDS: usize = 200_000;
    let mx = Multiplexer::start();
    let arrived = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&arrived);
    let client = ThreadedClient::builder(peers::TEST_EVENT_BACKEND)
        .on_message(move |_| seen.store(true, Ordering::SeqCst))
        .connect([mx.address()])
        .unwrap();
    let senders: Vec<_> = (0..2)
        .map(|_| {
            let (client, arrived) = (client.clone(), Arc::clone(&arrived));
            beside(move || {
                let mut sent = 0;
                while !arrived.load(Ordering::SeqCst) && sent < SENDS {
                    let _ = client.send(types::TEST_UNROUTED, "x").queue();
                    sent += 1;
                }
                sent
            })
        })
        .collect();
    let mut sender = SyncClient::builder(peers::TEST_EVENT_CLIENT).connect([mx.address()]).unwrap();
    sender.send(types::TEST_EVENT, "arrives").wait().unwrap();
    let sent: Vec<usize> = senders.into_iter().map(|thread| thread.join().unwrap()).collect();
    assert!(arrived.load(Ordering::SeqCst), "never reached on_message");
    assert!(sent.iter().all(|count| *count < SENDS), "it waited for the sends to end: {sent:?}");
}

#[test]
fn a_query_too_large_once_its_sender_is_filled_in_is_refused_at_the_call() {
    // As for SyncClient: the size check left out the sender, and then()
    // started a query whose frames could not be written.
    let client = ThreadedClient::builder(peers::TEST_CLIENT).build().unwrap();
    let request = |payload_size: usize, sender: u64| Message {
        r#type: types::TEST_REQUEST_A,
        payload: vec![0; payload_size].into(),
        sender,
        ..Default::default()
    };
    let own = client.instance_id();
    let Err(Error::TooLarge { size, .. }) = client.query_message(request(MAX_MESSAGE_SIZE, own)).then(|_| {}) else {
        panic!("a query over the limit is refused")
    };
    let envelope = size - MAX_MESSAGE_SIZE;
    match client.query_message(request(MAX_MESSAGE_SIZE - envelope + 1, 0)).then(|_| {}) {
        Err(Error::TooLarge { size, .. }) => assert_eq!(MAX_MESSAGE_SIZE + 1, size),
        other => panic!("refused as too large, not {other:?}"),
    }
}

#[test]
fn a_query_a_callback_starts_once_the_shutdown_began_is_refused() {
    // It went out during the write-out, and once the connections closed it
    // searched with none left and held the io thread, and the shutdown,
    // for its whole stage.
    let mx = Multiplexer::start();
    let mut backend = SyncClient::builder(peers::TEST_BACKEND_A).connect([mx.address()]).unwrap();
    let client = ThreadedClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    let (ended, follow_up) = mpsc::channel();
    let handle = client.clone();
    client
        .query(types::TEST_REQUEST_A, "first")
        .then(move |_| {
            // The shutdown ends the first one: this runs inside it.
            let started = handle.query(types::TEST_REQUEST_A, "follow-up").timeout(HOUR).then(move |result| {
                let _ = ended.send(result.map(|_| ()));
            });
            assert!(started.is_ok(), "the io thread takes the command until the teardown");
        })
        .unwrap();
    backend.receive(TEN).expect("the first request, unanswered");
    let shutting = beside(move || client.shutdown(TEN).wait());
    match follow_up.recv_timeout(BOUND) {
        Ok(Err(Error::NotConnected)) => {}
        other => panic!("the follow-up refused, not {other:?}"),
    }
    shutting.join().unwrap().unwrap();
}

#[test]
fn a_stream_a_callback_asks_for_inside_the_shutdown_ends_with_the_client() {
    // Asked for right before the teardown, the stream was registered after
    // it had ended the others, and nothing ended it: its readers waited
    // for good.
    let mx = Multiplexer::start();
    let mut backend = SyncClient::builder(peers::TEST_BACKEND_A).connect([mx.address()]).unwrap();
    let client = ThreadedClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    let (asked, streams) = mpsc::channel();
    let handle = client.clone();
    client
        .query(types::TEST_REQUEST_A, "unanswered")
        .then(move |_| {
            // The shutdown ends the query: this runs inside it.
            let _ = asked.send((handle.messages(), handle.subscribe(types::TEST_EVENT)));
        })
        .unwrap();
    backend.receive(TEN).expect("the request");
    client.shutdown(Some(Duration::ZERO)).wait().unwrap();
    let (inbox, subscription) = streams.recv_timeout(BOUND).unwrap();
    assert!(ended(&inbox), "the inbox ends with the client");
    assert!(ended(&subscription), "the subscription ends with the client");
}

#[test]
fn a_send_a_callback_makes_once_the_write_out_step_is_queued_is_written() {
    // The io thread's own commands joined its queue behind the write-out's
    // step queued before them: the teardown came first and dropped the
    // send.
    let mx = Multiplexer::start();
    let mut backend = SyncClient::builder(peers::TEST_BACKEND_A).connect([mx.address()]).unwrap();
    let to = backend.instance_id();
    let client = ThreadedClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    let (ended, written) = mpsc::channel();
    let handle = client.clone();
    client
        .query(types::TEST_REQUEST_A, "unanswered")
        .then(move |_| {
            // Inside the shutdown, which finds nothing to write and queues
            // its step: the query started here is refused a step later, and
            // its callback sends.
            let sender = handle.clone();
            let _ = handle.query(types::TEST_REQUEST_A, "refused").then(move |_| {
                let _ = sender
                    .send(types::TEST_EVENT, "after")
                    .to(to)
                    .on_end(move |written| {
                        let _ = ended.send(written);
                    })
                    .queue();
            });
        })
        .unwrap();
    backend.receive(TEN).expect("the first request");
    client.shutdown(TEN).wait().unwrap();
    assert_eq!(Ok(true), written.recv_timeout(BOUND), "the send written");
    assert_eq!(&b"after"[..], &backend.receive(TEN).unwrap().message.payload[..]);
}

#[test]
fn plain_sends_arrive_in_their_batches_in_order_beside_sends_followed() {
    // A queue() nobody follows goes to the io thread as data and to the
    // batching as its bytes, one with on_end as a frame, both in the same
    // batches: every message arrives once, with its id, payload and key, in
    // the order sent within its key, and each followed send hears it was
    // written. From two threads, each its own keys.
    let mx = Multiplexer::start();
    let mut receiver = SyncClient::builder(peers::TEST_EVENT_BACKEND).connect([mx.address()]).unwrap();
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    let batching = Delivery { batch: true, max_records: 7, max_delay: HOUR, timeout: None, ..Delivery::default() };
    client.set_delivery(types::TEST_EVENT, None, batching).unwrap();
    let written = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let senders: Vec<_> = ["even", "odd"]
        .into_iter()
        .map(|parity| {
            let (client, written) = (client.clone(), Arc::clone(&written));
            beside(move || {
                let mut sent = std::collections::BTreeMap::new();
                for number in 0..100 {
                    let key = format!("{parity} {}", number % 3);
                    let send = client.send(types::TEST_EVENT, number.to_string()).key(key.clone());
                    let id = if number % 10 == 0 {
                        let written = Arc::clone(&written);
                        send.on_end(move |was| {
                            written.fetch_add(usize::from(was), Ordering::SeqCst);
                        })
                        .queue()
                    } else {
                        send.queue()
                    }
                    .unwrap();
                    sent.entry(Some(key)).or_insert_with(Vec::new).push((id, number.to_string().into_bytes()));
                }
                sent
            })
        })
        .collect();
    let mut sent = std::collections::BTreeMap::new();
    for sender in senders {
        sent.extend(sender.join().unwrap());
    }
    assert!(client.flush_all(TEN).wait().unwrap());
    assert_eq!(20, written.load(Ordering::SeqCst), "each followed send heard written");
    assert_eq!(sent, arrived_by_key(&mut receiver, 200));
}

#[test]
fn big_plain_sends_that_go_alone_arrive_whole_in_order() {
    // Each too big for its batch: the frame its own body, its header kept
    // apart, written with it in one call, a few megabytes each, so that the
    // socket takes it in several writes.
    let mx = Multiplexer::start();
    let mut receiver = SyncClient::builder(peers::TEST_EVENT_BACKEND).connect([mx.address()]).unwrap();
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    let batching =
        Delivery { batch: true, max_bytes: 64 * 1024, max_delay: HOUR, timeout: None, ..Delivery::default() };
    client.set_delivery(types::TEST_EVENT, None, batching).unwrap();
    let payloads: Vec<Vec<u8>> = (0..4u8)
        .map(|number| (0..3 * 1024 * 1024).map(|index: usize| (index as u8).wrapping_add(number)).collect())
        .collect();
    let sent: Vec<u64> =
        payloads.iter().map(|payload| client.send(types::TEST_EVENT, payload.clone()).queue().unwrap()).collect();
    for (id, payload) in sent.iter().zip(&payloads) {
        let arrived = receiver.receive(TEN).unwrap().message;
        assert_eq!(*id, arrived.id);
        assert!(arrived.payload[..] == payload[..], "message {id} arrived whole");
    }
}

#[test]
fn the_shutdown_writes_out_the_plain_sends_made_before_it() {
    let mx = Multiplexer::start();
    let mut receiver = SyncClient::builder(peers::TEST_EVENT_BACKEND).connect([mx.address()]).unwrap();
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    let batching = Delivery { batch: true, max_delay: HOUR, timeout: None, ..Delivery::default() };
    client.set_delivery(types::TEST_EVENT, None, batching).unwrap();
    let sent: Vec<u64> =
        (0..10).map(|number| client.send(types::TEST_EVENT, number.to_string()).queue().unwrap()).collect();
    client.shutdown(None).wait().unwrap();
    let arrived: Vec<u64> = sent.iter().map(|_| receiver.receive(TEN).unwrap().message.id).collect();
    assert_eq!(sent, arrived, "the batch let go first, whole");
    assert!(matches!(client.send(types::TEST_EVENT, "late").queue(), Err(Error::NotConnected)));
}

/// A thread-local whose destructor sends what it holds, as a per-thread log
/// buffer flushing at the thread's end would.
struct FlushedAtExit {
    client: Option<ThreadedClient>,
    sent: Option<mpsc::Sender<Result<u64, Error>>>,
}

impl Drop for FlushedAtExit {
    fn drop(&mut self) {
        if let (Some(client), Some(sent)) = (&self.client, &self.sent) {
            let _ = sent.send(client.send(types::TEST_EVENT, "flushed at the thread's end").queue());
        }
    }
}

thread_local! {
    static FLUSHED_AT_EXIT: std::cell::RefCell<FlushedAtExit> =
        const { std::cell::RefCell::new(FlushedAtExit { client: None, sent: None }) };
}

#[test]
fn a_plain_send_from_a_thread_locals_destructor_goes_out() {
    let mx = Multiplexer::start();
    let mut receiver = SyncClient::builder(peers::TEST_EVENT_BACKEND).connect([mx.address()]).unwrap();
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    client.set_delivery(types::TEST_EVENT, None, Delivery { batch: true, ..Delivery::default() }).unwrap();
    let (sent, results) = mpsc::channel();
    let theirs = client.clone();
    std::thread::spawn(move || {
        // This thread-local first, then a plain send: the send's spare body
        // is made after it, so it goes before it at the thread's end.
        FLUSHED_AT_EXIT.with(|flushed| {
            let mut flushed = flushed.borrow_mut();
            flushed.client = Some(theirs.clone());
            flushed.sent = Some(sent);
        });
        theirs.send(types::TEST_EVENT, "a plain send").queue().unwrap();
    })
    .join()
    .unwrap();
    let id = results.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
    assert!(client.flush_all(TEN).wait().unwrap());
    let ids: Vec<u64> = (0..2).map(|_| receiver.receive(TEN).unwrap().message.id).collect();
    assert_eq!(id, ids[1], "the one sent at the thread's end, after the other");
}
