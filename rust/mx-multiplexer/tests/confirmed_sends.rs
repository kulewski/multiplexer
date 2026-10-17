//! Confirmed sends (`Delivery::confirm`, docs/delivery.md) against real
//! multiplexers, the producer's library and the consumer's both this
//! crate's, the cases of multiplexer/confirmed_sends_test.cc: a producer of
//! each class numbers its messages, its sends ending at their confirmation
//! and its flush waiting for it; a message that would go alone goes as a
//! batch of one, and a type confirmed and not batched one message at a
//! time; a refused message goes again with its id and number, a failed one
//! is told, numbers not waited for are passed by; a batch nobody took goes
//! again once somebody takes it, what an ended connection wrote goes again
//! through another, and what nothing confirmed for retry_after goes again;
//! a message is given up on at its batch's deadline. A consumer of each
//! class hands every message of a confirmed batch out with its number: a
//! SyncClient's program confirms with processed(), sent once its receive
//! waits; a ThreadedClient once on_message returned, Failed when it
//! panicked, or, made not to, its program from any thread; a stream's
//! reader confirms what it reads; the server classes as their handlers
//! return, or their requests go. What the wire carries frame by frame is
//! the core's tests' (src/core/tests/confirming.rs); here both libraries
//! play their parts. Counted, not timed: every wait is for a message or an
//! end, bounded only to detect a failure, but for the deadline's and
//! retry_after's own, which wait on them.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use mx_multiplexer::{
    BaseMultiplexerServer, BaseThreadedMultiplexerServer, Confirmed, Delivery, DropReason, Error, Handler,
    HandlerResult, Incoming, Message, Request, SyncClient, ThreadedClient,
};
use support::testing_constants::{peers, types};
use support::{beside, Multiplexer};

/// What only a failure reaches.
const BOUND: Duration = Duration::from_secs(10);

/// A delay or a retry_after no test waits out.
const HOUR: Duration = Duration::from_secs(3600);

/// The type every test sends: to one TEST_EVENT_BACKEND.
const EVENT: u32 = types::TEST_EVENT_ANY;

/// Confirmed batching of `max_records` messages, closed by count or a
/// flush, its retry_after `retry_after`, its batches given up on never.
fn confirmed(max_records: usize, retry_after: Duration) -> Delivery {
    Delivery {
        batch: true,
        max_records,
        max_delay: HOUR,
        confirm: true,
        retry_after,
        timeout: None,
        ..Delivery::default()
    }
}

/// How each followed send ended, by id, as on_end heard it.
type Ends = mpsc::Receiver<(u64, bool)>;

/// Sends event `number` of `producer`, followed to its end into `ends`:
/// its id.
fn send(producer: &ThreadedClient, number: usize, ends: &mpsc::Sender<(u64, bool)>) -> u64 {
    let message = producer.new_message(EVENT, format!("event {number}"));
    let (id, ends) = (message.id, ends.clone());
    producer.send_message(message).on_end(move |written| ends.send((id, written)).unwrap()).queue().unwrap();
    id
}

/// The next `count` ends, BOUND each at most, sorted by id.
fn ends_of(ends: &Ends, count: usize) -> Vec<(u64, bool)> {
    let mut heard: Vec<(u64, bool)> = (0..count).map(|_| ends.recv_timeout(BOUND).expect("an end")).collect();
    heard.sort_unstable();
    heard
}

/// Each of `ids` ended `written`, sorted.
fn all(ids: &[u64], written: bool) -> Vec<(u64, bool)> {
    let mut each: Vec<(u64, bool)> = ids.iter().map(|id| (*id, written)).collect();
    each.sort_unstable();
    each
}

/// The drop observer's word, kept.
type Drops = Arc<Mutex<Vec<(u64, DropReason)>>>;

/// A ThreadedClient producer, connected, whose drops `drops` keeps.
fn producer(address: &str, drops: &Drops) -> ThreadedClient {
    let kept = Arc::clone(drops);
    ThreadedClient::builder(peers::TEST_EVENT_CLIENT)
        .on_drop(move |id, reason| kept.lock().unwrap().push((id, reason)))
        .connect([address])
        .unwrap()
}

/// A SyncClient consumer, connected.
fn consumer(address: &str) -> SyncClient {
    SyncClient::builder(peers::TEST_EVENT_BACKEND).connect([address]).unwrap()
}

/// The next message `consumer` receives, BOUND at most.
fn next(consumer: &mut SyncClient) -> Message {
    consumer.receive(Some(BOUND)).expect("a message").message
}

/// A receive with nothing to wait for: what the consumer owes goes.
fn send_what_is_owed(consumer: &mut SyncClient) {
    assert!(matches!(consumer.receive(Some(Duration::ZERO)), Err(Error::OperationTimedOut)));
}

// The producer's side.

#[test]
fn a_batch_numbers_its_messages_and_their_sends_end_at_the_confirmation() {
    let mx = Multiplexer::start();
    let mut consumer = consumer(mx.address());
    let drops = Drops::default();
    let producer = producer(mx.address(), &drops);
    producer.set_delivery(EVENT, None, confirmed(4, HOUR)).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..4).map(|number| send(&producer, number, &sender)).collect();
    let received: Vec<Message> = (0..4).map(|_| next(&mut consumer)).collect();
    assert_eq!(sent, received.iter().map(|message| message.id).collect::<Vec<_>>());
    assert_eq!(vec![Some(1), Some(2), Some(3), Some(4)], received.iter().map(|m| m.confirmation).collect::<Vec<_>>());
    assert_eq!(4, producer.unconfirmed().unwrap(), "written, kept until confirmed");
    assert!(!producer.flush_all(Some(Duration::ZERO)).wait().unwrap(), "a flush waits for the confirmation");
    assert!(ends.try_recv().is_err(), "no send ends at its write");
    for message in &received {
        consumer.processed(message);
    }
    assert_eq!(4, consumer.confirmations_owed());
    send_what_is_owed(&mut consumer);
    assert_eq!(0, consumer.confirmations_owed());
    assert!(producer.flush_all(Some(BOUND)).wait().unwrap());
    assert_eq!(all(&sent, true), ends_of(&ends, 4));
    assert_eq!((0, 0, 0), (producer.unconfirmed().unwrap(), producer.sent_again().unwrap(), producer.dropped()));
    assert!(drops.lock().unwrap().is_empty());
}

#[test]
fn a_message_that_would_go_alone_goes_as_a_batch_of_one() {
    // With no delay and the connection free, a message goes at once, as
    // without batching, but in an envelope of its own that numbers it.
    let mx = Multiplexer::start();
    let mut consumer = consumer(mx.address());
    let producer = producer(mx.address(), &Drops::default());
    producer.set_delivery(EVENT, None, Delivery { max_delay: Duration::ZERO, ..confirmed(100, HOUR) }).unwrap();
    let (sender, ends) = mpsc::channel();
    let id = send(&producer, 0, &sender);
    let message = next(&mut consumer);
    assert_eq!((id, Some(1)), (message.id, message.confirmation));
    consumer.processed(&message);
    send_what_is_owed(&mut consumer);
    assert!(producer.flush_all(Some(BOUND)).wait().unwrap());
    assert_eq!(vec![(id, true)], ends_of(&ends, 1));
    assert_eq!(0, producer.unconfirmed().unwrap());
}

#[test]
fn a_refused_message_goes_again_at_once_with_its_id_and_number() {
    let mx = Multiplexer::start();
    let mut consumer = consumer(mx.address());
    let producer = producer(mx.address(), &Drops::default());
    producer.set_delivery(EVENT, None, confirmed(4, HOUR)).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..4).map(|number| send(&producer, number, &sender)).collect();
    let received: Vec<Message> = (0..4).map(|_| next(&mut consumer)).collect();
    for (index, message) in received.iter().enumerate() {
        consumer.confirm(message, if index == 2 { Confirmed::Refused } else { Confirmed::Processed });
    }
    let again = next(&mut consumer); // its wait sent the CONFIRMATION
    assert_eq!((sent[2], Some(3)), (again.id, again.confirmation), "its id and number");
    assert_eq!(received[2].payload, again.payload);
    assert_eq!((1, 1), (producer.sent_again().unwrap(), producer.unconfirmed().unwrap()));
    consumer.processed(&again);
    send_what_is_owed(&mut consumer);
    assert!(producer.flush_all(Some(BOUND)).wait().unwrap());
    assert_eq!(all(&sent, true), ends_of(&ends, 4));
}

#[test]
fn a_failed_message_is_given_up_on_and_told() {
    // A SyncClient producer, whose flush lets the batch go and waits for
    // its confirmation, the consumer beside it.
    let mx = Multiplexer::start();
    let mut consumer = consumer(mx.address());
    let drops = Drops::default();
    let kept = Arc::clone(&drops);
    let mut producer = SyncClient::builder(peers::TEST_EVENT_CLIENT)
        .on_drop(move |id, reason| kept.lock().unwrap().push((id, reason)))
        .connect([mx.address()])
        .unwrap();
    producer.set_delivery(EVENT, None, confirmed(10, HOUR)).unwrap();
    let (sender, ends) = mpsc::channel();
    let mut sent = Vec::new();
    for number in 0..4 {
        let message = producer.new_message(EVENT, format!("event {number}"));
        let (id, sender) = (message.id, sender.clone());
        producer.send_message(message).on_end(move |written| sender.send((id, written)).unwrap()).queue().unwrap();
        sent.push(id);
    }
    let far_side = beside(move || {
        for index in 0..4 {
            let message = next(&mut consumer);
            consumer.confirm(&message, if index == 3 { Confirmed::Failed } else { Confirmed::Processed });
        }
        send_what_is_owed(&mut consumer);
    });
    assert!(!producer.flush_all(Some(BOUND)).unwrap(), "one was given up on");
    far_side.join().unwrap();
    let mut expected = all(&sent[..3], true);
    expected.push((sent[3], false));
    expected.sort_unstable();
    assert_eq!(expected, ends_of(&ends, 4));
    assert_eq!(vec![(sent[3], DropReason::Failed)], *drops.lock().unwrap());
    assert_eq!((0, 0), (producer.unconfirmed(), producer.sent_again()));
}

#[test]
fn a_number_not_waited_for_is_passed_by() {
    // A consumer that confirms numbers never given, and one twice, every
    // way: only what the producer waits for counts.
    let mx = Multiplexer::start();
    let mut consumer = consumer(mx.address());
    let drops = Drops::default();
    let producer = producer(mx.address(), &drops);
    producer.set_delivery(EVENT, None, confirmed(2, HOUR)).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..2).map(|number| send(&producer, number, &sender)).collect();
    let received: Vec<Message> = (0..2).map(|_| next(&mut consumer)).collect();
    let never = |number| Message { sender: producer.instance_id(), confirmation: Some(number), ..Default::default() };
    for number in 100..105 {
        consumer.processed(&never(number));
    }
    consumer.confirm(&never(50), Confirmed::Failed);
    consumer.processed(&received[0]);
    consumer.confirm(&received[0], Confirmed::Refused);
    consumer.confirm(&received[0], Confirmed::Failed);
    send_what_is_owed(&mut consumer);
    assert_eq!(vec![(sent[0], true)], ends_of(&ends, 1));
    consumer.processed(&received[1]);
    send_what_is_owed(&mut consumer);
    assert!(producer.flush_all(Some(BOUND)).wait().unwrap());
    assert_eq!(vec![(sent[1], true)], ends_of(&ends, 1));
    assert_eq!((0, 0), (producer.sent_again().unwrap(), producer.dropped()));
    assert!(drops.lock().unwrap().is_empty());
}

#[test]
fn a_batch_nobody_took_goes_again_once_somebody_takes_it() {
    // Nobody to take it: the multiplexer's DELIVERY_ERROR, which the
    // program never sees, sends it again after a back-off, until a
    // consumer comes.
    let mx = Multiplexer::start();
    let handed = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&handed);
    let producer = ThreadedClient::builder(peers::TEST_EVENT_CLIENT)
        .on_message(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
        })
        .connect([mx.address()])
        .unwrap();
    producer.set_delivery(EVENT, None, confirmed(3, HOUR)).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..3).map(|number| send(&producer, number, &sender)).collect();
    let until = Instant::now() + BOUND;
    while producer.sent_again().unwrap() < 3 {
        assert!(Instant::now() < until, "sent again after the DELIVERY_ERROR");
        std::thread::sleep(Duration::from_millis(10)); // a failure detector's bound: the back-off sends
    }
    let mut consumer = consumer(mx.address());
    let received: Vec<Message> = (0..3).map(|_| next(&mut consumer)).collect();
    assert_eq!(sent, received.iter().map(|message| message.id).collect::<Vec<_>>());
    assert_eq!(vec![Some(1), Some(2), Some(3)], received.iter().map(|m| m.confirmation).collect::<Vec<_>>());
    for message in &received {
        consumer.processed(message);
    }
    send_what_is_owed(&mut consumer);
    assert!(producer.flush_all(Some(BOUND)).wait().unwrap());
    assert_eq!(all(&sent, true), ends_of(&ends, 3));
    assert_eq!(0, handed.load(Ordering::SeqCst), "no DELIVERY_ERROR reaches on_message");
}

#[test]
fn what_the_ended_connection_wrote_goes_again_through_another() {
    // A multiplexer that dies takes the batch it was written to with it:
    // the batch goes again at once, through the connection left.
    let mut first = Multiplexer::start();
    let second = Multiplexer::start();
    let mut lost = consumer(first.address());
    let producer = producer(first.address(), &Drops::default());
    producer.set_delivery(EVENT, None, confirmed(3, HOUR)).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..3).map(|number| send(&producer, number, &sender)).collect();
    for _ in 0..3 {
        next(&mut lost); // read, never confirmed
    }
    let mut consumer = consumer(second.address());
    assert!(producer.connect(second.address(), Some(BOUND)).wait().unwrap());
    first.kill();
    let again: Vec<Message> = (0..3).map(|_| next(&mut consumer)).collect();
    assert_eq!(sent, again.iter().map(|message| message.id).collect::<Vec<_>>());
    assert_eq!(vec![Some(1), Some(2), Some(3)], again.iter().map(|m| m.confirmation).collect::<Vec<_>>());
    assert_eq!(3, producer.sent_again().unwrap());
    for message in &again {
        consumer.processed(message);
    }
    send_what_is_owed(&mut consumer);
    assert!(producer.flush_all(Some(BOUND)).wait().unwrap());
    assert_eq!(all(&sent, true), ends_of(&ends, 3));
    drop(lost);
}

#[test]
fn what_nothing_confirmed_for_retry_after_goes_again() {
    // Nothing confirmed for retry_after: what is unresolved goes again,
    // handed out again with its id and number, each send ending once.
    let mx = Multiplexer::start();
    let mut consumer = consumer(mx.address());
    let producer = producer(mx.address(), &Drops::default());
    producer.set_delivery(EVENT, None, confirmed(3, Duration::from_millis(500))).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..3).map(|number| send(&producer, number, &sender)).collect();
    let first: Vec<Message> = (0..3).map(|_| next(&mut consumer)).collect();
    let again: Vec<Message> = (0..3).map(|_| next(&mut consumer)).collect(); // the silence's resend
    assert_eq!(sent, again.iter().map(|message| message.id).collect::<Vec<_>>());
    assert_eq!(
        first.iter().map(|m| m.confirmation).collect::<Vec<_>>(),
        again.iter().map(|m| m.confirmation).collect::<Vec<_>>()
    );
    assert!(producer.sent_again().unwrap() >= 3);
    for message in &again {
        consumer.processed(message);
    }
    // What the consumer owes, sent by a flush, not by a receive that must
    // find nothing: a test thread late by another retry_after finds a third
    // copy there, as sent_again() allows.
    assert!(consumer.flush_all(Some(BOUND)).unwrap());
    assert!(producer.flush_all(Some(BOUND)).wait().unwrap());
    assert_eq!(all(&sent, true), ends_of(&ends, 3), "once each");
    assert!(ends.try_recv().is_err());
}

#[test]
fn a_message_is_given_up_on_at_its_batchs_deadline() {
    // Nobody confirms: each message is given up on at its batch's deadline,
    // the batch's timeout from its first message, and the flush waiting for
    // it says so; the sends' own timeouts, none, are not used.
    let mx = Multiplexer::start();
    let _consumer = consumer(mx.address()); // reads nothing, confirms nothing
    let drops = Drops::default();
    let kept = Arc::clone(&drops);
    let mut producer = SyncClient::builder(peers::TEST_EVENT_CLIENT)
        .on_drop(move |id, reason| kept.lock().unwrap().push((id, reason)))
        .connect([mx.address()])
        .unwrap();
    let delivery = Delivery {
        max_delay: Duration::from_millis(100), // goes full, by its count, before that
        timeout: Some(Duration::from_millis(300)),
        ..confirmed(3, HOUR)
    };
    producer.set_delivery(EVENT, None, delivery).unwrap();
    let (sender, ends) = mpsc::channel();
    let mut sent = Vec::new();
    for number in 0..3 {
        let message = producer.new_message(EVENT, format!("event {number}"));
        let (id, sender) = (message.id, sender.clone());
        producer
            .send_message(message)
            .no_deadline()
            .on_end(move |written| sender.send((id, written)).unwrap())
            .queue()
            .unwrap();
        sent.push(id);
    }
    assert!(!producer.flush_all(Some(BOUND)).unwrap(), "given up on");
    assert_eq!(all(&sent, false), ends_of(&ends, 3));
    let mut told = drops.lock().unwrap().clone();
    told.sort_unstable_by_key(|(id, _)| *id);
    let mut expected: Vec<(u64, DropReason)> = sent.iter().map(|id| (*id, DropReason::NotConfirmed)).collect();
    expected.sort_unstable_by_key(|(id, _)| *id);
    assert_eq!(expected, told);
    assert_eq!(0, producer.unconfirmed());
}

#[test]
fn a_sync_clients_flush_waits_for_the_confirmation() {
    // The SyncClient producer hears the confirmation as its call runs the
    // loop.
    let mx = Multiplexer::start();
    let mut consumer = consumer(mx.address());
    let mut producer = SyncClient::builder(peers::TEST_EVENT_CLIENT).connect([mx.address()]).unwrap();
    producer.set_delivery(EVENT, None, confirmed(5, HOUR)).unwrap();
    for number in 0..5 {
        producer.send(EVENT, format!("event {number}")).queue().unwrap();
    }
    let far_side = beside(move || {
        for _ in 0..5 {
            let message = next(&mut consumer);
            consumer.processed(&message);
        }
        send_what_is_owed(&mut consumer);
    });
    assert!(producer.flush_all(Some(BOUND)).unwrap());
    far_side.join().unwrap();
    assert_eq!(0, producer.unconfirmed());
}

#[test]
fn set_delivery_checks_retry_after_with_confirm() {
    let client = ThreadedClient::builder(peers::TEST_EVENT_CLIENT).build().unwrap();
    for retry_after in [Duration::ZERO, Duration::from_secs(2_000_000_000)] {
        let refused = client.set_delivery(EVENT, None, confirmed(10, retry_after));
        assert!(matches!(refused, Err(Error::InvalidArgument(_))), "{retry_after:?}: {refused:?}");
    }
    let unconfirmed = Delivery { confirm: false, ..confirmed(10, Duration::ZERO) };
    client.set_delivery(EVENT, None, unconfirmed).expect("read only with confirm");
}

#[test]
fn without_batch_each_message_goes_alone_and_is_confirmed() {
    // A type confirmed and not batched: each message goes at once, in an
    // envelope of its own, which numbers it, and its send ends once its
    // consumer confirmed it.
    let mx = Multiplexer::start();
    let mut consumer = consumer(mx.address());
    let producer = producer(mx.address(), &Drops::default());
    producer.set_delivery(EVENT, None, Delivery { confirm: true, ..Delivery::default() }).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..3).map(|number| send(&producer, number, &sender)).collect();
    for (index, id) in sent.iter().enumerate() {
        let message = next(&mut consumer);
        assert_eq!((*id, Some(index as u64 + 1)), (message.id, message.confirmation), "in order, each its number");
        consumer.processed(&message);
    }
    send_what_is_owed(&mut consumer);
    assert!(producer.flush_all(Some(BOUND)).wait().unwrap());
    assert_eq!(all(&sent, true), ends_of(&ends, 3));
    assert_eq!((0, 0), (producer.unconfirmed().unwrap(), producer.sent_again().unwrap()));
}

// The consumer's side.

#[test]
fn a_sync_client_consumer_confirms_what_it_processed_in_one_go() {
    // Each message carries its number; processed() gathers them, sent at
    // once when a receive waits.
    let mx = Multiplexer::start();
    let mut consumer = consumer(mx.address());
    let producer = producer(mx.address(), &Drops::default());
    producer.set_delivery(EVENT, None, confirmed(50, HOUR)).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..50).map(|number| send(&producer, number, &sender)).collect();
    for (index, id) in sent.iter().enumerate() {
        let message = next(&mut consumer);
        assert_eq!((*id, Some(index as u64 + 1)), (message.id, message.confirmation));
        consumer.processed(&message);
    }
    assert_eq!(50, consumer.confirmations_owed(), "fewer than CONFIRMATIONS_GATHERED_MAX, kept while in hand");
    send_what_is_owed(&mut consumer);
    assert_eq!(0, consumer.confirmations_owed());
    assert_eq!(all(&sent, true), ends_of(&ends, 50));
}

#[test]
fn only_a_confirmed_batchs_message_carries_a_number() {
    // Whatever its sender put there.
    let mx = Multiplexer::start();
    let mut consumer = consumer(mx.address());
    let mut producer = SyncClient::builder(peers::TEST_EVENT_CLIENT).connect([mx.address()]).unwrap();
    let alone = Message { r#type: EVENT, confirmation: Some(5), ..producer.new_message(EVENT, "alone") };
    let id = producer.send_message(alone).wait().unwrap();
    let message = next(&mut consumer);
    assert_eq!((id, None), (message.id, message.confirmation));
    consumer.processed(&message);
    assert_eq!(0, consumer.confirmations_owed());
}

#[test]
fn a_threaded_client_confirms_what_on_message_returned_from() {
    // Processed once on_message returned, Failed where it panicked.
    let mx = Multiplexer::start();
    let _consumer = ThreadedClient::builder(peers::TEST_EVENT_BACKEND)
        .on_message(|incoming| {
            if incoming.message.payload.as_ref() == b"event 4" {
                panic!("this one fails, as asked");
            }
        })
        .connect([mx.address()])
        .unwrap();
    let drops = Drops::default();
    let producer = producer(mx.address(), &drops);
    producer.set_delivery(EVENT, None, confirmed(10, HOUR)).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..10).map(|number| send(&producer, number, &sender)).collect();
    let mut expected = all(&sent, true);
    expected.retain(|(id, _)| *id != sent[4]);
    expected.push((sent[4], false));
    expected.sort_unstable();
    assert_eq!(expected, ends_of(&ends, 10));
    assert_eq!(vec![(sent[4], DropReason::Failed)], *drops.lock().unwrap());
}

#[test]
fn a_threaded_clients_program_may_confirm_from_any_thread() {
    // Made not to confirm at on_message's return, the client confirms what
    // its program says, from the program's thread; a refused message comes
    // back.
    let mx = Multiplexer::start();
    let (handed, taken) = mpsc::channel::<Message>();
    let consumer = ThreadedClient::builder(peers::TEST_EVENT_BACKEND)
        .confirm_after_on_message(false)
        .on_message(move |incoming| handed.send(incoming.message).unwrap())
        .connect([mx.address()])
        .unwrap();
    let drops = Drops::default();
    let producer = producer(mx.address(), &drops);
    producer.set_delivery(EVENT, None, confirmed(4, HOUR)).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..4).map(|number| send(&producer, number, &sender)).collect();
    let received: Vec<Message> = (0..4).map(|_| taken.recv_timeout(BOUND).expect("a message")).collect();
    assert_eq!(0, consumer.confirmations_owed().unwrap(), "nothing confirmed by on_message's return");
    consumer.processed(&received[0]);
    consumer.confirm(&received[1], Confirmed::Refused);
    consumer.processed(&received[2]);
    consumer.confirm(&received[3], Confirmed::Failed);
    let again = taken.recv_timeout(BOUND).expect("the refused one, again");
    assert_eq!((sent[1], Some(2)), (again.id, again.confirmation));
    consumer.processed(&again);
    let mut expected = all(&sent[..3], true);
    expected.push((sent[3], false));
    expected.sort_unstable();
    assert_eq!(expected, ends_of(&ends, 4));
    assert_eq!(vec![(sent[3], DropReason::Failed)], *drops.lock().unwrap());
    assert_eq!(1, producer.sent_again().unwrap());
}

#[test]
fn a_streams_reader_confirms_what_it_read() {
    // No on_message: a message the client's inbox takes is its reader's to
    // confirm, nothing confirmed for it meanwhile.
    let mx = Multiplexer::start();
    let consumer = ThreadedClient::builder(peers::TEST_EVENT_BACKEND).build().unwrap();
    let inbox = consumer.messages();
    assert_eq!(1, consumer.connect_all([mx.address()], Some(BOUND)).wait().unwrap());
    let producer = producer(mx.address(), &Drops::default());
    producer.set_delivery(EVENT, None, confirmed(2, HOUR)).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..2).map(|number| send(&producer, number, &sender)).collect();
    let read: Vec<Incoming> = (0..2).map(|_| inbox.recv(Some(BOUND)).expect("a message")).collect();
    assert_eq!(0, consumer.confirmations_owed().unwrap());
    assert!(ends.try_recv().is_err());
    for incoming in &read {
        consumer.processed(&incoming.message);
    }
    assert_eq!(all(&sent, true), ends_of(&ends, 2));
}

/// Takes no reply; fails on "event 1", and keeps the request of "event 2"
/// until told to let it go.
#[derive(Default)]
struct Events {
    kept: Mutex<Vec<Request>>,
}

impl Handler for Events {
    fn handle_message(&self, request: Request) -> HandlerResult {
        request.no_response();
        match request.payload().as_ref() {
            b"event 1" => Err("this one fails, as asked".into()),
            b"event 2" => {
                self.kept.lock().unwrap().push(request);
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[test]
fn base_multiplexer_server_confirms_what_its_handler_returned_from() {
    // As the handler returns, a request it kept included; Failed where it
    // failed.
    let mx = Multiplexer::start();
    let mut server =
        BaseMultiplexerServer::builder(peers::TEST_EVENT_BACKEND, [mx.address()]).build(Events::default()).unwrap();
    server.connect().unwrap();
    let handle = server.handle();
    let serving = beside(move || server.serve_forever(Duration::from_millis(50), None));
    let drops = Drops::default();
    let producer = producer(mx.address(), &drops);
    producer.set_delivery(EVENT, None, confirmed(3, HOUR)).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..3).map(|number| send(&producer, number, &sender)).collect();
    let mut expected = vec![(sent[0], true), (sent[1], false), (sent[2], true)];
    expected.sort_unstable();
    assert_eq!(expected, ends_of(&ends, 3));
    assert_eq!(vec![(sent[1], DropReason::Failed)], *drops.lock().unwrap());
    handle.stop();
    serving.join().unwrap().unwrap();
}

#[test]
fn base_threaded_multiplexer_server_confirms_when_the_request_goes() {
    // At the handler's return, Failed where it failed; a request the
    // handler kept once it is let go of.
    let mx = Multiplexer::start();
    let server = Arc::new(
        BaseThreadedMultiplexerServer::builder(peers::TEST_EVENT_BACKEND, [mx.address()])
            .workers(2)
            .build(Events::default())
            .unwrap(),
    );
    server.connect().unwrap();
    let serving = {
        let server = Arc::clone(&server);
        beside(move || server.serve_forever(Duration::from_millis(50), None))
    };
    let drops = Drops::default();
    let producer = producer(mx.address(), &drops);
    producer.set_delivery(EVENT, None, confirmed(4, HOUR)).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..4).map(|number| send(&producer, number, &sender)).collect();
    let mut expected = vec![(sent[0], true), (sent[1], false), (sent[3], true)];
    expected.sort_unstable();
    assert_eq!(expected, ends_of(&ends, 3), "every one but the request kept");
    assert!(ends.try_recv().is_err(), "the request kept: not yet");
    let kept = std::mem::take(&mut *server.handler().kept.lock().unwrap());
    assert_eq!(1, kept.len());
    drop(kept); // let go of: confirmed
    assert_eq!(vec![(sent[2], true)], ends_of(&ends, 1));
    assert_eq!(vec![(sent[1], DropReason::Failed)], *drops.lock().unwrap());
    server.handle().stop();
    serving.join().unwrap().unwrap();
}

// Both sides.

#[test]
fn the_libraries_agree() {
    // A ThreadedClient producer and a SyncClient consumer: every send ends
    // confirmed, none sent again.
    let mx = Multiplexer::start();
    let mut consumer = consumer(mx.address());
    let producer = producer(mx.address(), &Drops::default());
    producer.set_delivery(EVENT, None, confirmed(25, HOUR)).unwrap();
    let (sender, ends) = mpsc::channel();
    let sent: Vec<u64> = (0..100).map(|number| send(&producer, number, &sender)).collect();
    for number in 0..100 {
        let message = next(&mut consumer);
        assert_eq!(format!("event {number}").as_bytes(), message.payload.as_ref());
        consumer.processed(&message);
    }
    send_what_is_owed(&mut consumer);
    assert!(producer.flush_all(Some(BOUND)).wait().unwrap());
    assert_eq!(all(&sent, true), ends_of(&ends, 100));
    assert_eq!((0, 0), (producer.unconfirmed().unwrap(), producer.sent_again().unwrap()));
}
