//! Confirmed sends on the virtual clock (`confirming`), the cases of
//! multiplexer/confirmed_sends_test.cc and more, the harness playing the
//! multiplexer and the other library frame by frame. The producer's side: a
//! confirmed batch numbers its messages, a batch of one too, and their
//! sends end at the confirmation, not at the write, a flush waiting for it;
//! a type confirmed and not batched goes a message at a time; a refused
//! message goes again at once, its id and number kept; a failed one is
//! given up on and told; numbers not waited for are passed by; a batch
//! nobody took goes again after a back-off; what an ended connection wrote
//! goes again through another; what nothing confirmed for retry_after goes
//! again; what is left of a batch is given up on at its deadline, its
//! timeout from its first message, which a batch sent again keeps, a frame
//! given up on and the shutdown telling each message once. The consumer's
//! side: a confirmed batch's messages are handed out with their numbers and
//! confirmed in one CONFIRMATION, on the connection they came on; one whose
//! handlers run as messages arrive confirms what one read brought together,
//! and at once when that read's connection closes; a message sent again is
//! handed out again; only a confirmed batch's message carries a number;
//! on_message's return confirms, Failed when it panicked; a leaving server
//! refuses in a CONFIRMATION. And two cores, one producing and one
//! consuming, agree.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;

use super::batching::{event, inside, EVENT, HOUR};
use crate::constants::types;
use crate::core::batching::{Batch, Delivery, Record};
use crate::core::client::{DropReason, IncomingMode};
use crate::core::confirming::Confirmed;
use crate::core::harness::{multiplexer_id, Harness};
use crate::core::outbox::SendDone;
use crate::core::{ConnId, SendState, Tracker, MAX_TIMEOUT};
use crate::defaults::CONFIRMATIONS_GATHERED_MAX;
use crate::error::Error;
use crate::proto::multiplexer::{Confirmation, DeliveryError, MessageBatch, MultiplexerMessage};

/// The instance id of the peer on the other side: the consumer of the
/// harness's batches, or the producer of those it receives.
const PEER: u64 = 77;

/// A timeout no test reaches.
const LONG: Option<Duration> = Some(Duration::from_secs(3600));

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

/// What a send's end heard, in order.
type Ends = Arc<Mutex<Vec<u32>>>;

/// Sends an event as a send nobody waits for, which batches, with `timeout`
/// and its end heard in `ends`, its tracker kept in `trackers` when given.
fn send(h: &mut Harness, id: u64, timeout: Option<Duration>, ends: &Ends, trackers: Option<&mut Vec<Arc<Tracker>>>) {
    let frame = event(h, id, None, format!("event {id}").as_bytes());
    let heard = Arc::clone(ends);
    let done = SendDone::Callback(Box::new(move |written| heard.lock().unwrap().push(written)));
    assert!(h.core.send(frame, false, None, timeout, 0, trackers, None, Some(done), true));
    h.settle();
}

/// Sends an event nobody follows, no end to hear and no tracker, as a
/// plain send of a program that does not wait for its messages.
fn send_unfollowed(h: &mut Harness, id: u64) {
    let frame = event(h, id, None, format!("event {id}").as_bytes());
    assert!(h.core.send(frame, false, None, LONG, 0, None, None, None, true));
    h.settle();
}

/// The numbers (first, count) pairs stand for.
fn numbers_of(pairs: &[u64]) -> Vec<u64> {
    pairs.chunks(2).flat_map(|pair| pair[0]..pair[0] + pair[1]).collect()
}

/// A confirmed batch as the far side reads it off the wire: its envelope's
/// id, its messages and their numbers.
#[derive(Debug)]
struct Wrote {
    id: u64,
    messages: Vec<MultiplexerMessage>,
    numbers: Vec<u64>,
}

impl Wrote {
    fn ids(&self) -> Vec<u64> {
        self.messages.iter().map(|message| message.id.unwrap_or(0)).collect()
    }
}

/// The batches of type EVENT written on `conn` since the last call; a
/// message of EVENT written alone fails the test.
fn batches(h: &mut Harness, conn: ConnId) -> Vec<Wrote> {
    h.written(conn)
        .into_iter()
        .filter(|written| written.r#type == EVENT)
        .map(|envelope| {
            assert!(envelope.batch.is_some(), "a confirmed message goes in an envelope: {envelope:?}");
            let numbers = numbers_of(&envelope.confirm);
            Wrote { id: envelope.id.unwrap_or(0), numbers, messages: inside(envelope) }
        })
        .collect()
}

/// The CONFIRMATIONs written on `conn` since the last call, each to whom
/// and what it says.
fn confirmations(h: &mut Harness, conn: ConnId) -> Vec<(u64, Confirmation)> {
    h.written(conn)
        .into_iter()
        .filter(|written| written.r#type == types::CONFIRMATION)
        .map(|written| {
            assert_eq!(Some(false), written.report_delivery_error, "lost, its messages go again in time");
            let confirmation = Confirmation::decode(written.message.clone().unwrap_or_default()).unwrap();
            (written.to.unwrap_or(0), confirmation)
        })
        .collect()
}

/// A CONFIRMATION from the consumer to the harness's client, each list
/// (first, count) pairs.
fn confirm(h: &mut Harness, conn: ConnId, processed: &[u64], refused: &[u64], failed: &[u64]) {
    let confirmation =
        Confirmation { processed: processed.to_vec(), refused: refused.to_vec(), failed: failed.to_vec() };
    let id = h.core.random64();
    let message = MultiplexerMessage {
        id: Some(id),
        sender: Some(PEER),
        to: Some(h.core.instance_id),
        r#type: types::CONFIRMATION,
        message: Some(confirmation.encode_to_vec().into()),
        ..Default::default()
    };
    h.deliver(conn, &message);
}

/// The DELIVERY_ERROR the multiplexer of target 1 sends for the envelope
/// `envelope`, which nobody took.
fn not_taken(h: &mut Harness, conn: ConnId, envelope: u64) {
    let report = DeliveryError { packet_id: envelope, failed_type: vec![205], ..Default::default() };
    let id = h.core.random64();
    let message = MultiplexerMessage {
        id: Some(id),
        sender: Some(multiplexer_id(1)),
        to: Some(h.core.instance_id),
        report_delivery_error: Some(false),
        r#type: types::DELIVERY_ERROR,
        references: Some(envelope),
        message: Some(report.encode_to_vec().into()),
        ..Default::default()
    };
    h.deliver(conn, &message);
}

#[test]
fn a_batch_numbers_its_messages_and_their_sends_end_at_the_confirmation() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, confirmed(4, HOUR)).expect("set");
    let ends = Ends::default();
    let mut trackers = Vec::new();
    for id in 1..=4 {
        send(&mut h, id, LONG, &ends, Some(&mut trackers));
    }
    let wrote = batches(&mut h, conn);
    assert_eq!(1, wrote.len());
    assert_eq!(vec![1, 2, 3, 4], wrote[0].ids());
    assert_eq!(vec![1, 2, 3, 4], wrote[0].numbers);
    assert_eq!(4, h.core.unconfirmed(), "written, kept until confirmed");
    let flush = h.core.begin_flush(None);
    h.settle();
    assert!(!h.core.flushed(flush), "a flush waits for the confirmation");
    assert!(ends.lock().unwrap().is_empty(), "no send ends at its write");
    assert!(trackers.iter().all(|tracker| tracker.get() == SendState::Queued));
    confirm(&mut h, conn, &[1, 4], &[], &[]);
    assert_eq!(vec![1, 1, 1, 1], *ends.lock().unwrap());
    assert!(trackers.iter().all(|tracker| tracker.get() == SendState::Sent));
    assert!(h.core.flushed(flush) && h.core.all_written(flush));
    assert_eq!((0, 0), (h.core.unconfirmed(), h.core.sent_again()));
    assert!(drops.lock().unwrap().is_empty());
    assert_eq!(None, h.core.confirmer_armed(), "nothing waits: no timer");
}

#[test]
fn a_message_that_would_go_alone_goes_as_a_batch_of_one() {
    // With no delay and the connection free, a message goes at once, as
    // without batching, and so does one too big for a batch: each in an
    // envelope of its own that numbers it.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    let delivery = Delivery { max_delay: Duration::ZERO, max_bytes: 100, ..confirmed(100, HOUR) };
    h.core.set_delivery(EVENT, None, delivery).expect("set");
    let ends = Ends::default();
    send(&mut h, 1, LONG, &ends, None);
    let big = event(&h, 2, None, &[7; 200]);
    assert!(h.core.send(big, false, None, LONG, 0, None, None, None, true));
    h.settle();
    let wrote = batches(&mut h, conn);
    assert_eq!(vec![vec![1], vec![2]], wrote.iter().map(Wrote::ids).collect::<Vec<_>>());
    assert_eq!(vec![vec![1], vec![2]], wrote.iter().map(|batch| batch.numbers.clone()).collect::<Vec<_>>());
    confirm(&mut h, conn, &[1, 2], &[], &[]);
    assert_eq!(vec![1], *ends.lock().unwrap());
    assert_eq!(0, h.core.unconfirmed());
}

#[test]
fn without_batch_each_message_goes_alone_and_is_confirmed() {
    // A type confirmed and not batched: each message goes at once, in an
    // envelope of its own that numbers it, and is kept until confirmed; one
    // of a key whose batch waits lets it go first, so that nothing
    // overtakes a message sent before it.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, Delivery { confirm: true, ..Delivery::default() }).expect("set");
    let by_count = Delivery { confirm: false, timeout: None, ..confirmed(100, HOUR) };
    h.core.set_delivery(EVENT, Some("batched"), by_count).expect("set");
    let ends = Ends::default();
    for id in 1..=3 {
        send(&mut h, id, LONG, &ends, None);
    }
    assert_eq!((0, 3), (h.core.batched_messages(), h.core.unconfirmed()), "none waits in a batch");
    let wrote = batches(&mut h, conn);
    assert_eq!(vec![vec![1], vec![2], vec![3]], wrote.iter().map(Wrote::ids).collect::<Vec<_>>());
    assert_eq!(vec![vec![1], vec![2], vec![3]], wrote.iter().map(|batch| batch.numbers.clone()).collect::<Vec<_>>());
    let flush = h.core.begin_flush(None);
    h.settle();
    for number in 1..=3 {
        assert!(!h.core.flushed(flush), "a flush waits for the confirmations");
        confirm(&mut h, conn, &[number, 1], &[], &[]);
    }
    assert!(h.core.flushed(flush) && h.core.all_written(flush));
    assert_eq!(vec![1, 1, 1], *ends.lock().unwrap());
    assert_eq!((0, 0), (h.core.unconfirmed(), h.core.sent_again()));
    let batched = event(&h, 4, Some("batched"), b"event 4");
    assert!(h.core.send(batched, false, None, LONG, 0, None, None, None, true));
    send(&mut h, 5, LONG, &ends, None);
    let written: Vec<(u64, bool)> = h
        .written(conn)
        .into_iter()
        .filter(|message| message.r#type == EVENT)
        .map(|message| match message.batch.is_some() {
            true => (inside(message)[0].id.unwrap_or(0), true),
            false => (message.id.unwrap_or(0), false),
        })
        .collect();
    assert_eq!(vec![(4, false), (5, true)], written, "the batch first, as itself, then the envelope");
}

#[test]
fn a_batch_cut_for_its_bytes_numbers_each_part_in_order() {
    // The numbers are measured with the rest: a batch over max_bytes as it
    // goes on the wire is cut, and those cut keep their numbers, going
    // next. Built whole here: the batching lets a batch go before its
    // messages' bytes pass max_bytes, so that only an envelope's own bytes
    // take one over.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    let records: Vec<Record> = (1..=6u64)
        .map(|id| {
            let body = super::batching::message(&h, id, None, &[7; 20]).encode_to_vec();
            Record { body, id, done: None, tracker: None, number: 0 }
        })
        .collect();
    let bytes = records.iter().map(|record| record.body.len()).sum();
    let delivery = Delivery { max_bytes: 120, ..confirmed(100, HOUR) };
    h.core.send_batch(Batch::at_once(EVENT, None, delivery, records, bytes, Duration::ZERO, None), None);
    h.settle();
    let wrote = batches(&mut h, conn);
    assert!(wrote.len() > 1, "cut: {wrote:?}");
    let ids: Vec<u64> = wrote.iter().flat_map(Wrote::ids).collect();
    let numbers: Vec<u64> = wrote.iter().flat_map(|batch| batch.numbers.clone()).collect();
    assert_eq!((1..=6).collect::<Vec<u64>>(), ids);
    assert_eq!((1..=6).collect::<Vec<u64>>(), numbers, "each message its number, in order");
    for batch in &wrote {
        assert_eq!(batch.messages.len(), batch.numbers.len());
    }
}

#[test]
fn a_refused_message_goes_again_at_once_with_its_id_and_number() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, confirmed(4, HOUR)).expect("set");
    let ends = Ends::default();
    for id in 1..=4 {
        send(&mut h, id, LONG, &ends, None);
    }
    let first = batches(&mut h, conn).pop().expect("a batch");
    confirm(&mut h, conn, &[1, 2, 4, 1], &[3, 1], &[]);
    let again = batches(&mut h, conn);
    assert_eq!(1, again.len(), "at once");
    assert_ne!(first.id, again[0].id, "a batch of its own");
    assert_eq!(vec![3], again[0].ids());
    assert_eq!(first.messages[2].message, again[0].messages[0].message, "its bytes, read back out of the frame");
    assert_eq!(vec![3], again[0].numbers);
    assert_eq!((1, 1), (h.core.sent_again(), h.core.unconfirmed()));
    assert_eq!(vec![1, 1, 1], *ends.lock().unwrap(), "processed, in the order resolved");
    confirm(&mut h, conn, &[3, 1], &[], &[]);
    assert_eq!(vec![1, 1, 1, 1], *ends.lock().unwrap());
    assert_eq!(0, h.core.unconfirmed());
}

#[test]
fn messages_without_ends_are_resolved_a_run_at_a_time() {
    // Ranges across words of the flags, a range confirmed twice and a
    // refusal in the middle: every message counted once.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, confirmed(200, HOUR)).expect("set");
    for id in 1..=200 {
        send_unfollowed(&mut h, id);
    }
    let wrote = batches(&mut h, conn);
    assert_eq!(1, wrote.len());
    assert_eq!((1..=200).collect::<Vec<u64>>(), wrote[0].numbers);
    let flush = h.core.begin_flush(None);
    h.settle();
    confirm(&mut h, conn, &[1, 70, 71, 29, 50, 40], &[100, 1], &[]); // 1-99, 50-89 again, 100 refused
    let again = batches(&mut h, conn);
    assert_eq!(1, again.len(), "the refused one, at once");
    assert_eq!(vec![100], again[0].ids());
    assert_eq!(vec![100], again[0].numbers);
    assert_eq!((101, 1), (h.core.unconfirmed(), h.core.sent_again()));
    assert!(!h.core.flushed(flush), "a flush waits for them all");
    confirm(&mut h, conn, &[101, 100, 100, 1, 1, 200], &[], &[]); // the rest, then 100, then all again
    assert_eq!(0, h.core.unconfirmed());
    assert!(h.core.flushed(flush) && h.core.all_written(flush));
}

#[test]
fn a_failed_message_is_given_up_on_and_told() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, confirmed(10, HOUR)).expect("set");
    let ends = Ends::default();
    for id in 1..=4 {
        send(&mut h, id, LONG, &ends, None);
    }
    let flush = h.core.begin_flush(None); // lets the batch go, and waits for it
    h.settle();
    assert_eq!(vec![1, 2, 3, 4], batches(&mut h, conn)[0].ids());
    confirm(&mut h, conn, &[1, 3], &[], &[4, 1]);
    assert_eq!(vec![1, 1, 1, 0], *ends.lock().unwrap());
    assert_eq!(vec![(4, DropReason::Failed)], *drops.lock().unwrap());
    assert_eq!(1, h.core.client.dropped.load(std::sync::atomic::Ordering::Relaxed));
    assert!(h.core.flushed(flush) && !h.core.all_written(flush), "one was given up on");
    assert_eq!((0, 0), (h.core.unconfirmed(), h.core.sent_again()));
}

#[test]
fn a_number_not_waited_for_is_passed_by() {
    // A pair past the last number there is, one empty, one that overflows,
    // an odd number left over, a payload that does not decode, and numbers
    // confirmed twice change nothing.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, confirmed(2, HOUR)).expect("set");
    let ends = Ends::default();
    for id in 1..=2 {
        send(&mut h, id, LONG, &ends, None);
    }
    batches(&mut h, conn);
    confirm(&mut h, conn, &[1, 1, 100, 5, u64::MAX, 2, 9], &[7, 0], &[50, 1]);
    let garbage = MultiplexerMessage {
        id: Some(h.core.random64()),
        sender: Some(PEER),
        r#type: types::CONFIRMATION,
        message: Some(Bytes::from_static(b"\xff\xff\xff not a confirmation")),
        ..Default::default()
    };
    h.deliver(conn, &garbage);
    confirm(&mut h, conn, &[1, 1], &[1, 1], &[1, 1]);
    assert_eq!(vec![1], *ends.lock().unwrap());
    assert!(batches(&mut h, conn).is_empty(), "a refusal of a number confirmed is passed by");
    confirm(&mut h, conn, &[2, 1], &[], &[]);
    assert_eq!(vec![1, 1], *ends.lock().unwrap());
    assert_eq!(0, h.core.sent_again());
    assert!(drops.lock().unwrap().is_empty());
}

#[test]
fn a_batch_nobody_took_goes_again_after_a_back_off_until_somebody_takes_it() {
    // The multiplexer's DELIVERY_ERROR for the envelope, which the program
    // never sees, sends the batch again after 0.1 s, the back-off doubling
    // with each to 1 s; a confirmation ends it.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, confirmed(3, HOUR)).expect("set");
    let ends = Ends::default();
    for id in 1..=3 {
        send(&mut h, id, LONG, &ends, None);
    }
    let mut envelope = batches(&mut h, conn).pop().expect("a batch").id;
    for backoff in [100, 200, 400, 800, 1000, 1000] {
        not_taken(&mut h, conn, envelope);
        h.advance(Duration::from_millis(backoff - 1));
        assert!(batches(&mut h, conn).is_empty(), "not before {backoff} ms");
        h.advance(Duration::from_millis(1));
        let again = batches(&mut h, conn).pop().expect("sent again");
        assert_eq!((vec![1, 2, 3], vec![1, 2, 3]), (again.ids(), again.numbers.clone()), "after {backoff} ms");
        envelope = again.id;
    }
    assert_eq!(18, h.core.sent_again());
    assert!(h.core.client.incoming.is_empty(), "no DELIVERY_ERROR reaches the program");
    confirm(&mut h, conn, &[1, 1], &[], &[]);
    // Somebody takes its messages: the back-off starts over.
    not_taken(&mut h, conn, envelope);
    h.advance(Duration::from_millis(100));
    assert_eq!(vec![2, 3], batches(&mut h, conn).pop().expect("sent again").numbers);
    confirm(&mut h, conn, &[2, 2], &[], &[]);
    assert_eq!(vec![1, 1, 1], *ends.lock().unwrap());
    assert_eq!(0, h.core.unconfirmed());
}

#[test]
fn what_the_ended_connection_wrote_goes_again_through_another() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let one = h.up(1);
    let two = h.up(2);
    h.written(one);
    h.written(two);
    h.core.set_delivery(EVENT, None, confirmed(3, HOUR)).expect("set");
    let ends = Ends::default();
    for id in 1..=3 {
        send(&mut h, id, LONG, &ends, None);
    }
    let (written, other) = if batches(&mut h, one).is_empty() { (two, one) } else { (one, two) };
    h.written(written);
    h.end(written, crate::core::connection::ReadEnd::Eof);
    let again = batches(&mut h, other);
    assert_eq!(1, again.len(), "at once, through the connection left");
    assert_eq!((vec![1, 2, 3], vec![1, 2, 3]), (again[0].ids(), again[0].numbers.clone()));
    assert_eq!(3, h.core.sent_again());
    confirm(&mut h, other, &[1, 3], &[], &[]);
    assert_eq!(vec![1, 1, 1], *ends.lock().unwrap());
}

#[test]
fn what_its_connection_had_not_written_moves_with_its_queue_and_is_not_sent_again() {
    // Placed on a connection that ends before writing it, the frame goes to
    // another with the rest of the queue: the same frame, nothing sent again.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let one = h.up(1);
    let two = h.up(2);
    h.written(one);
    h.written(two);
    h.hold_writes = true;
    h.core.set_delivery(EVENT, None, confirmed(2, HOUR)).expect("set");
    let ends = Ends::default();
    for id in 1..=2 {
        send(&mut h, id, LONG, &ends, None);
    }
    let holding = if h.sockets[&one].in_flight.is_some() { one } else { two };
    let other = if holding == one { two } else { one };
    h.end(holding, crate::core::connection::ReadEnd::Eof);
    h.hold_writes = false;
    h.complete_writes();
    let moved = batches(&mut h, other);
    assert_eq!(vec![1, 2], moved.iter().flat_map(Wrote::ids).collect::<Vec<_>>());
    assert_eq!(0, h.core.sent_again());
    confirm(&mut h, other, &[1, 2], &[], &[]);
    assert_eq!(vec![1, 1], *ends.lock().unwrap());
}

#[test]
fn what_nothing_confirmed_for_retry_after_goes_again() {
    // Nothing confirmed for retry_after since the write: what is unresolved
    // goes again; a confirmation of some starts the wait over for the rest;
    // each send ends once.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, confirmed(3, Duration::from_secs(2))).expect("set");
    let ends = Ends::default();
    for id in 1..=3 {
        send(&mut h, id, LONG, &ends, None);
    }
    batches(&mut h, conn);
    h.advance(Duration::from_millis(1_999));
    assert!(batches(&mut h, conn).is_empty());
    h.advance(Duration::from_millis(1));
    let again = batches(&mut h, conn).pop().expect("the silence's resend");
    assert_eq!((vec![1, 2, 3], vec![1, 2, 3]), (again.ids(), again.numbers.clone()));
    h.advance(Duration::from_secs(1));
    confirm(&mut h, conn, &[1, 1], &[], &[]);
    h.advance(Duration::from_millis(1_999));
    assert!(batches(&mut h, conn).is_empty(), "two seconds from the confirmation, not from the write");
    h.advance(Duration::from_millis(1));
    assert_eq!(vec![2, 3], batches(&mut h, conn).pop().expect("the rest").numbers);
    confirm(&mut h, conn, &[1, 3], &[], &[]); // 1 a second time: passed by
    assert_eq!(vec![1, 1, 1], *ends.lock().unwrap(), "once each");
    assert_eq!(5, h.core.sent_again());
    assert_eq!(None, h.core.confirmer_armed());
}

#[test]
fn a_message_is_given_up_on_at_its_batchs_deadline() {
    // What is left of a batch is given up on at its deadline, its timeout
    // from its first message, one sent later in it included, though the
    // batch waited for its delay: the flush waiting for them says so. The
    // sends' own timeouts, none, an hour and zero, are not used.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let conn = h.up(1);
    h.written(conn);
    let delivery =
        Delivery { max_delay: Duration::from_secs(1), timeout: Some(Duration::from_secs(3)), ..confirmed(100, HOUR) };
    h.core.set_delivery(EVENT, None, delivery).expect("set");
    let ends = Ends::default();
    send(&mut h, 1, None, &ends, None);
    h.advance(Duration::from_millis(500));
    send(&mut h, 2, LONG, &ends, None);
    send(&mut h, 3, Some(Duration::ZERO), &ends, None);
    h.advance(Duration::from_millis(500)); // the delay: the batch goes
    assert_eq!(vec![1, 2, 3], batches(&mut h, conn)[0].ids());
    let flush = h.core.begin_flush(None);
    confirm(&mut h, conn, &[3, 1], &[], &[]);
    h.advance(Duration::from_millis(1_999));
    assert!(drops.lock().unwrap().is_empty());
    h.advance(Duration::from_millis(1));
    assert_eq!(vec![(1, DropReason::NotConfirmed), (2, DropReason::NotConfirmed)], *drops.lock().unwrap());
    assert_eq!(vec![1, 0, 0], *ends.lock().unwrap(), "the third confirmed, the rest given up on together");
    assert!(h.core.flushed(flush) && !h.core.all_written(flush), "two were given up on");
    assert_eq!((0, None), (h.core.unconfirmed(), h.core.confirmer_armed()));
}

#[test]
fn a_batch_sent_again_keeps_its_deadline() {
    // A refused message goes again at once, in a batch of its own with the
    // deadline its first batch had, not one of its own from now; refused
    // at that deadline, before the timer gives it up, it goes no more.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let conn = h.up(1);
    h.written(conn);
    let delivery =
        Delivery { max_delay: Duration::from_secs(1), timeout: Some(Duration::from_secs(3)), ..confirmed(3, HOUR) };
    h.core.set_delivery(EVENT, None, delivery).expect("set");
    let ends = Ends::default();
    for id in 1..=3 {
        send(&mut h, id, LONG, &ends, None); // the third fills the batch: it goes
    }
    assert_eq!(1, batches(&mut h, conn).len());
    h.advance(Duration::from_secs(1));
    confirm(&mut h, conn, &[1, 1], &[2, 2], &[]); // 2 and 3 refused: again at once
    let again = batches(&mut h, conn);
    assert_eq!((vec![2, 3], vec![2, 3]), (again[0].ids(), again[0].numbers.clone()));
    confirm(&mut h, conn, &[2, 1], &[], &[]);
    h.core.now += Duration::from_secs(2); // its deadline, its timer not run yet
    confirm(&mut h, conn, &[], &[3, 1], &[]);
    assert!(batches(&mut h, conn).is_empty(), "past the deadline: given up on, not sent again");
    assert_eq!(vec![(3, DropReason::NotConfirmed)], *drops.lock().unwrap());
    assert_eq!(vec![1, 1, 0], *ends.lock().unwrap());
    assert_eq!((0, 2), (h.core.unconfirmed(), h.core.sent_again()));
    assert_eq!(None, h.core.confirmer_armed(), "nothing waits: no timer");
}

#[test]
fn a_batch_sent_again_is_given_up_on_at_the_deadline_it_kept() {
    // Sent again a second after its batch went, and confirmed by nobody:
    // given up on at the first batch's deadline, three seconds from its
    // first message, not three from the send again.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let conn = h.up(1);
    h.written(conn);
    let delivery =
        Delivery { max_delay: Duration::from_secs(1), timeout: Some(Duration::from_secs(3)), ..confirmed(2, HOUR) };
    h.core.set_delivery(EVENT, None, delivery).expect("set");
    let ends = Ends::default();
    for id in 1..=2 {
        send(&mut h, id, LONG, &ends, None);
    }
    batches(&mut h, conn);
    h.advance(Duration::from_secs(1));
    confirm(&mut h, conn, &[], &[1, 2], &[]); // refused: again at once
    assert_eq!(vec![1, 2], batches(&mut h, conn)[0].ids());
    h.advance(Duration::from_millis(1_999));
    assert!(drops.lock().unwrap().is_empty());
    h.advance(Duration::from_millis(1));
    assert_eq!(vec![(1, DropReason::NotConfirmed), (2, DropReason::NotConfirmed)], *drops.lock().unwrap());
    assert_eq!(vec![0, 0], *ends.lock().unwrap());
}

#[test]
fn a_frame_given_up_on_tells_each_of_its_messages_once() {
    // No connection: the batch is held for one until its deadline, when
    // the frame and the batch's messages are given up on at once, by two
    // timers: whichever runs first, each message is told once, for its
    // reason, the others with it; a message of a key with no deadline held
    // beside it goes at the shutdown, told once too.
    for unconfirmed_first in [false, true] {
        let mut h = Harness::new(200, IncomingMode::Queue);
        let drops = h.observe_drops();
        if unconfirmed_first {
            // An unconfirmed message held with an earlier deadline arms the
            // outbox's timer first, so that the confirmed batch's timer
            // comes due ahead of the frame's.
            h.send_with(900, Some(Duration::from_secs(1)), false, None);
        }
        let timed =
            Delivery { max_delay: Duration::from_secs(1), timeout: Some(Duration::from_secs(2)), ..confirmed(2, HOUR) };
        h.core.set_delivery(EVENT, None, timed).expect("set");
        h.core.set_delivery(EVENT, Some("forever"), confirmed(1, HOUR)).expect("set");
        let ends = Ends::default();
        send(&mut h, 1, LONG, &ends, None);
        send(&mut h, 2, LONG, &ends, None); // the batch full, held for a connection
        let forever = event(&h, 3, Some("forever"), b"event 3");
        let heard = Arc::clone(&ends);
        let done = SendDone::Callback(Box::new(move |written| heard.lock().unwrap().push(written)));
        assert!(h.core.send(forever, false, None, LONG, 0, None, None, Some(done), true)); // a batch of one
        h.settle();
        h.advance(Duration::from_secs(2));
        let told: Vec<(u64, DropReason)> = drops.lock().unwrap().iter().filter(|(id, _)| *id != 900).copied().collect();
        let first = if unconfirmed_first { DropReason::NotConfirmed } else { DropReason::NoConnection };
        assert_eq!(vec![(1, first), (2, first)], told, "{unconfirmed_first}: each once");
        assert_eq!(vec![0, 0], *ends.lock().unwrap());
        h.advance(Duration::from_secs(10));
        assert_eq!(2, drops.lock().unwrap().iter().filter(|(id, _)| *id != 900).count(), "nothing more");
        h.core.shutdown_client();
        h.settle();
        let told: Vec<(u64, DropReason)> = drops.lock().unwrap().iter().filter(|(id, _)| *id != 900).copied().collect();
        assert_eq!((3, DropReason::ShutDown), told[2], "{unconfirmed_first}");
        assert_eq!(3, told.len());
        assert_eq!(vec![0, 0, 0], *ends.lock().unwrap());
        assert_eq!(0, h.core.unconfirmed());
    }
}

#[test]
fn a_shutdown_gives_up_on_what_waits_for_its_confirmation_once() {
    // A batch written and one still queued, the connection busy: each
    // message told once, ShutDown.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, confirmed(2, HOUR)).expect("set");
    let ends = Ends::default();
    for id in 1..=2 {
        send(&mut h, id, LONG, &ends, None);
    }
    h.hold_writes = true;
    for id in 3..=4 {
        send(&mut h, id, LONG, &ends, None);
    }
    h.core.shutdown_client();
    h.settle();
    let mut told = drops.lock().unwrap().clone();
    told.sort_unstable_by_key(|(id, _)| *id);
    assert_eq!(
        vec![
            (1, DropReason::ShutDown),
            (2, DropReason::ShutDown),
            (3, DropReason::ShutDown),
            (4, DropReason::ShutDown)
        ],
        told
    );
    assert_eq!(vec![0, 0, 0, 0], *ends.lock().unwrap());
    assert_eq!(4, h.core.client.dropped.load(std::sync::atomic::Ordering::Relaxed));
    assert_eq!((0, None), (h.core.unconfirmed(), h.core.confirmer_armed()));
}

#[test]
fn a_flush_waits_only_for_the_confirmed_sends_numbered_before_it() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, confirmed(2, HOUR)).expect("set");
    let ends = Ends::default();
    for id in 1..=2 {
        send(&mut h, id, LONG, &ends, None);
    }
    let flush = h.core.begin_flush(None);
    for id in 3..=4 {
        send(&mut h, id, LONG, &ends, None);
    }
    confirm(&mut h, conn, &[1, 2], &[], &[]);
    assert!(h.core.flushed(flush) && h.core.all_written(flush), "the later ones are not its");
    let later = h.core.begin_flush(None);
    h.settle();
    assert!(!h.core.flushed(later));
    confirm(&mut h, conn, &[3, 2], &[], &[]);
    assert!(h.core.flushed(later));
}

#[test]
fn retry_after_is_checked_with_confirm_alone() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    for retry_after in [Duration::ZERO, MAX_TIMEOUT + Duration::from_secs(1)] {
        let result = h.core.set_delivery(EVENT, None, confirmed(10, retry_after));
        assert!(matches!(result, Err(Error::InvalidArgument(_))), "{retry_after:?}: {result:?}");
    }
    let unconfirmed = Delivery { confirm: false, ..confirmed(10, Duration::ZERO) };
    h.core.set_delivery(EVENT, None, unconfirmed).expect("read only with confirm");
}

// The consumer's side.

/// A batch's envelope from PEER, of type EVENT, its messages `ids` and,
/// when `numbers` has any, its `confirm` pairs.
fn envelope(id: u64, ids: &[u64], numbers: &[u64]) -> MultiplexerMessage {
    let bodies = ids
        .iter()
        .map(|id| {
            let message = MultiplexerMessage {
                id: Some(*id),
                sender: Some(PEER),
                r#type: EVENT,
                message: Some(Bytes::from(format!("event {id}"))),
                ..Default::default()
            };
            message.encode_to_vec().into()
        })
        .collect();
    MultiplexerMessage {
        id: Some(id),
        sender: Some(PEER),
        r#type: EVENT,
        batch: Some(MessageBatch { message: bodies }.encode_to_vec().into()),
        confirm: numbers.to_vec(),
        ..Default::default()
    }
}

/// The numbers of the messages queued for a receive, taken out.
fn received(h: &mut Harness) -> Vec<(u64, Option<u64>)> {
    let mut taken = Vec::new();
    while let Some(incoming) = h.core.next_incoming_message() {
        taken.push((incoming.message.id.unwrap_or(0), incoming.message.confirmation));
    }
    taken
}

#[test]
fn a_confirmed_batchs_messages_carry_their_numbers_and_are_confirmed_in_one_message() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.deliver(conn, &envelope(900, &[11, 12, 13, 14, 15], &[40, 2, 100, 3]));
    let taken = received(&mut h);
    assert_eq!(vec![(11, Some(40)), (12, Some(41)), (13, Some(100)), (14, Some(101)), (15, Some(102))], taken);
    for (_, number) in &taken {
        h.core.confirm(PEER, number.unwrap(), Confirmed::Processed);
    }
    h.core.confirm(PEER, 0, Confirmed::Processed); // no number: nothing
    assert_eq!(5, h.core.confirmations_owed(), "kept while in hand");
    assert!(confirmations(&mut h, conn).is_empty());
    h.core.send_confirmations();
    h.settle();
    let sent = confirmations(&mut h, conn);
    assert_eq!(1, sent.len(), "every one, in one message");
    assert_eq!(PEER, sent[0].0);
    assert_eq!(vec![40, 2, 100, 3], sent[0].1.processed, "as pairs");
    assert!(sent[0].1.refused.is_empty() && sent[0].1.failed.is_empty());
    assert_eq!(0, h.core.confirmations_owed());
}

#[test]
fn a_consumer_confirms_on_the_connection_the_batch_came_on_while_it_lives() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let one = h.up(1);
    let two = h.up(2);
    h.written(one);
    h.written(two);
    for (round, numbers) in [(0, [1, 1]), (1, [2, 1])] {
        h.deliver(two, &envelope(900 + round, &[11 + round], &numbers));
        let taken = received(&mut h);
        h.core.confirm(PEER, taken[0].1.unwrap(), Confirmed::Processed);
        if round == 1 {
            h.end(two, crate::core::connection::ReadEnd::Eof); // gone: another takes it
        }
        h.core.send_confirmations();
        h.settle();
        let (on_two, on_one) = (confirmations(&mut h, two), confirmations(&mut h, one));
        match round {
            0 => assert_eq!((1, 0), (on_two.len(), on_one.len()), "the way the batch came"),
            _ => assert_eq!((0, 1), (on_two.len(), on_one.len()), "another live one"),
        }
    }
}

#[test]
fn confirmations_go_once_gathered_max_are_owed_one_producer() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    let ids: Vec<u64> = (1..=300).collect();
    h.deliver(conn, &envelope(900, &ids, &[1, 300]));
    for (_, number) in received(&mut h) {
        h.core.confirm(PEER, number.unwrap(), Confirmed::Processed);
    }
    h.settle();
    let sent = confirmations(&mut h, conn);
    assert_eq!(1, sent.len());
    assert_eq!(vec![1, CONFIRMATIONS_GATHERED_MAX as u64], sent[0].1.processed, "at the most, the program still busy");
    assert_eq!(300 - CONFIRMATIONS_GATHERED_MAX, h.core.confirmations_owed());
}

#[test]
fn a_message_sent_again_is_handed_out_again() {
    // The same messages, ids and numbers, as a resend after a lost
    // confirmation: at least once, so not dropped as seen before, where an
    // unconfirmed batch's copy is.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.deliver(conn, &envelope(900, &[11, 12], &[1, 2]));
    h.deliver(conn, &envelope(901, &[11, 12], &[1, 2]));
    assert_eq!(vec![(11, Some(1)), (12, Some(2)), (11, Some(1)), (12, Some(2))], received(&mut h));
    h.deliver(conn, &envelope(902, &[21], &[]));
    h.deliver(conn, &envelope(903, &[21], &[]));
    assert_eq!(vec![(21, None)], received(&mut h), "unconfirmed, the copy dropped");
}

#[test]
fn only_a_confirmed_batchs_message_carries_a_number() {
    // Whatever its sender put there: alone, or in a batch with no numbers.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    let alone = MultiplexerMessage {
        id: Some(77),
        sender: Some(PEER),
        r#type: EVENT,
        confirmation: Some(5),
        ..Default::default()
    };
    h.deliver(conn, &alone);
    let mut batch = envelope(900, &[], &[]);
    let inner = MultiplexerMessage {
        id: Some(78),
        sender: Some(PEER),
        r#type: EVENT,
        confirmation: Some(6),
        ..Default::default()
    };
    batch.batch = Some(MessageBatch { message: vec![inner.encode_to_vec().into()] }.encode_to_vec().into());
    h.deliver(conn, &batch);
    assert_eq!(vec![(77, None), (78, None)], received(&mut h));
}

#[test]
fn what_does_not_belong_in_a_confirmed_batch_is_confirmed_failed() {
    // Its number read by its place, as nothing will ever process it.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    let mut batch = envelope(900, &[11, 12, 13], &[1, 3]);
    let mut bodies = MessageBatch::decode(batch.batch.take().unwrap()).unwrap().message;
    bodies[1] = Bytes::from_static(b"\xff\xff not a message");
    batch.batch = Some(MessageBatch { message: bodies }.encode_to_vec().into());
    h.deliver(conn, &batch);
    assert_eq!(vec![(11, Some(1)), (13, Some(3))], received(&mut h));
    h.core.send_confirmations();
    h.settle();
    assert_eq!(vec![2, 1], confirmations(&mut h, conn)[0].1.failed);
}

#[test]
fn a_full_queue_drops_a_confirmed_batch_unconfirmed() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    for id in 1..=crate::defaults::DEFAULT_INCOMING_QUEUE_MAX_SIZE as u64 {
        let event = MultiplexerMessage { id: Some(id), sender: Some(PEER), r#type: EVENT, ..Default::default() };
        h.deliver(conn, &event);
    }
    h.deliver(conn, &envelope(90_000, &[50_001, 50_002], &[1, 2]));
    assert_eq!(0, h.core.confirmations_owed(), "its producer sends it again in time");
    h.core.send_confirmations();
    h.settle();
    assert!(confirmations(&mut h, conn).is_empty());
}

#[test]
fn a_leaving_server_refuses_a_confirmed_batch_in_a_confirmation() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.refuse_arrivals();
    h.deliver(conn, &envelope(900, &[11, 12], &[5, 2]));
    let written = h.written(conn);
    assert!(written.iter().all(|message| message.r#type != types::DELIVERY_ERROR), "no DELIVERY_ERROR");
    let refusals: Vec<&MultiplexerMessage> =
        written.iter().filter(|message| message.r#type == types::CONFIRMATION).collect();
    assert_eq!(1, refusals.len(), "once the read that brought the batch is handed over");
    let confirmation = Confirmation::decode(refusals[0].message.clone().unwrap()).unwrap();
    assert_eq!(vec![5, 2], confirmation.refused);
}

#[test]
fn a_leaving_server_refuses_what_it_read_and_did_not_receive_in_a_confirmation() {
    // refuse_unread at the close: the CONFIRMATION goes with the shutdown.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.deliver(conn, &envelope(900, &[11, 12, 13], &[5, 3]));
    h.core.next_incoming_message();
    h.core.refuse_unread();
    h.core.shutdown_client();
    h.settle();
    let written = h.written(conn);
    assert!(written.iter().all(|message| message.r#type != types::DELIVERY_ERROR), "no DELIVERY_ERROR");
    let refusals: Vec<&MultiplexerMessage> =
        written.iter().filter(|message| message.r#type == types::CONFIRMATION).collect();
    assert_eq!(1, refusals.len());
    let confirmation = Confirmation::decode(refusals[0].message.clone().unwrap()).unwrap();
    assert_eq!(vec![6, 2], confirmation.refused, "the two not received");
}

#[test]
fn producers_met_once_each_are_forgotten_past_a_few_thousand() {
    // A producer met keeps its entry, the connection its batches came on;
    // past 4096, those owed nothing go at the next send.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    for producer in 1..=4096 {
        h.core.confirmed_batch_from(conn, producer);
    }
    h.core.confirm(1, 1, Confirmed::Processed);
    h.core.send_confirmations();
    assert_eq!(4096, h.core.producers_remembered(), "kept up to the limit");
    h.core.confirmed_batch_from(conn, 4097);
    h.core.confirm(4097, 1, Confirmed::Processed);
    h.core.send_confirmations();
    assert_eq!(0, h.core.producers_remembered(), "past it, those owed nothing go");
}

#[test]
fn a_threaded_client_confirms_what_on_message_returned_from() {
    // Processed once on_message returned, Failed where it panicked, in one
    // CONFIRMATION once the read that brought the batch was handed over.
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let kept = Arc::clone(&seen);
    h.core.threaded.on_message = Some(Box::new(move |incoming| {
        kept.lock().unwrap().push(incoming.message.confirmation);
        if incoming.message.id == 14 {
            std::panic::panic_any("this one fails, as asked");
        }
    }));
    let conn = h.up(1);
    h.written(conn);
    h.deliver(conn, &envelope(900, &[11, 12, 13, 14, 15], &[1, 5]));
    assert_eq!(vec![Some(1), Some(2), Some(3), Some(4), Some(5)], *seen.lock().unwrap());
    let sent = confirmations(&mut h, conn);
    assert_eq!(1, sent.len());
    assert_eq!((vec![1, 3, 5, 1], vec![4, 1]), (sent[0].1.processed.clone(), sent[0].1.failed.clone()));
    assert_eq!(0, h.core.confirmations_owed());
}

#[test]
fn what_one_read_brought_is_confirmed_together() {
    // A threaded client, whose on_message runs as messages arrive, confirms
    // what one read brought in one CONFIRMATION: twenty confirmed batches
    // of one message each, from a producer that does not batch, where each
    // batch cost a CONFIRMATION of its own. Nothing goes before the read is
    // handed over.
    let mut h = Harness::new(201, IncomingMode::Dispatch);
    let handled = Arc::new(Mutex::new(0));
    let counted = Arc::clone(&handled);
    h.core.threaded.on_message = Some(Box::new(move |_| *counted.lock().unwrap() += 1));
    let conn = h.up(1);
    h.written(conn);
    let read: Vec<MultiplexerMessage> =
        (1..=20).map(|number| envelope(900 + number, &[100 + number], &[number, 1])).collect();
    for batch in &read[..19] {
        h.deliver_part(conn, batch);
    }
    assert!(confirmations(&mut h, conn).is_empty(), "the read not handed over yet");
    assert_eq!((19, 1), (h.core.confirmations_owed(), h.core.client.read_drains_wanted));
    h.deliver_read(conn, &read[19..]);
    let sent = confirmations(&mut h, conn);
    assert_eq!(1, sent.len(), "one for what one read brought");
    assert_eq!(vec![1, 20], sent[0].1.processed);
    assert_eq!(20, *handled.lock().unwrap());
    assert_eq!((0, 0), (h.core.confirmations_owed(), h.core.client.read_drains_wanted));
}

#[test]
fn what_is_owed_goes_on_another_connection_when_the_reads_connection_closes() {
    // The connection that brought a confirmed batch closes before its read
    // is handed over: what the handlers confirmed goes at once, on the
    // connection left, rather than wait for a read that will not come.
    let mut h = Harness::new(201, IncomingMode::Dispatch);
    h.core.threaded.on_message = Some(Box::new(|_| {}));
    let (one, two) = (h.up(1), h.up(2));
    h.written(one);
    h.written(two);
    h.deliver_part(two, &envelope(900, &[11, 12], &[1, 2]));
    assert_eq!((2, 1), (h.core.confirmations_owed(), h.core.client.read_drains_wanted));
    h.end(two, crate::core::connection::ReadEnd::Eof);
    let sent = confirmations(&mut h, one);
    assert_eq!(1, sent.len(), "at once, on the other connection");
    assert_eq!(vec![1, 2], sent[0].1.processed);
    assert_eq!((0, 0), (h.core.confirmations_owed(), h.core.client.read_drains_wanted), "nothing waits for it");
}

#[test]
fn a_threaded_client_that_confirms_itself_leaves_on_messages_return_alone() {
    // The server classes' way: nothing at on_message's return. A message
    // on_message confirmed itself, as a callback on the io thread does
    // through the inbox, is not confirmed again at its return, and goes
    // with the rest of the batch.
    for confirms_itself in [true, false] {
        let mut h = Harness::new(200, IncomingMode::Dispatch);
        h.core.threaded.confirms_itself = confirms_itself;
        let inbox = Arc::new(crate::inbox::Inbox::default());
        h.core.threaded.io_inbox = Some(Arc::clone(&inbox));
        let in_hand = Arc::clone(&h.core.threaded.in_hand);
        h.core.threaded.on_message = Some(Box::new(move |incoming| {
            if incoming.message.id == 12 {
                let (sender, number) = (incoming.message.sender, incoming.message.confirmation.unwrap());
                in_hand.note(sender, number);
                inbox.push_confirmation(crate::inbox::HandedConfirmation {
                    sender,
                    number,
                    outcome: Confirmed::Refused,
                });
            }
        }));
        let conn = h.up(1);
        h.written(conn);
        h.deliver(conn, &envelope(900, &[11, 12, 13], &[1, 3]));
        let sent = confirmations(&mut h, conn);
        assert_eq!(1, sent.len(), "{confirms_itself}: one message");
        assert_eq!(vec![2, 1], sent[0].1.refused, "{confirms_itself}");
        let processed = if confirms_itself { vec![] } else { vec![1, 1, 3, 1] };
        assert_eq!(processed, sent[0].1.processed, "{confirms_itself}");
    }
}

#[test]
fn a_threaded_client_with_nothing_to_take_a_confirmed_message_confirms_it_failed() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let conn = h.up(1);
    h.written(conn);
    h.deliver(conn, &envelope(900, &[11], &[7, 1]));
    assert_eq!(vec![7, 1], confirmations(&mut h, conn)[0].1.failed);
}

#[test]
fn confirmations_handed_over_go_at_the_end_of_the_turn_in_one_message() {
    // What a threaded client's other threads confirmed, taken from its
    // inbox: one send posted however many come, nothing once torn down.
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let conn = h.up(1);
    h.written(conn);
    let handed = |number| crate::inbox::HandedConfirmation { sender: PEER, number, outcome: Confirmed::Processed };
    h.core.confirm_handed(&mut vec![handed(1), handed(2)]);
    h.core.confirm_handed(&mut vec![handed(3)]);
    assert_eq!(1, h.core.posted.len(), "one send posted");
    h.settle();
    let sent = confirmations(&mut h, conn);
    assert_eq!(1, sent.len());
    assert_eq!(vec![1, 3], sent[0].1.processed);
    h.core.threaded.torn_down = true;
    h.core.confirm_handed(&mut vec![handed(4)]);
    assert_eq!(0, h.core.confirmations_owed());
}

#[test]
fn a_flush_and_the_shutdown_send_what_is_owed() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.confirm(PEER, 1, Confirmed::Processed);
    h.core.begin_flush(None);
    h.settle();
    assert_eq!(1, confirmations(&mut h, conn).len(), "a flush");
    h.core.confirm(PEER, 2, Confirmed::Failed);
    h.core.shutdown_client();
    h.settle();
    assert_eq!(vec![2, 1], confirmations(&mut h, conn)[0].1.failed, "the shutdown, before the connections close");
}

#[test]
fn a_producing_core_and_a_consuming_core_agree() {
    // Each core's frames delivered to the other by hand: a hundred events in
    // batches of 25, each confirmed by the consumer as received, in one
    // CONFIRMATION a receive's wait sends.
    let mut producer = Harness::new(200, IncomingMode::Queue);
    let mut consumer = Harness::new(201, IncomingMode::Queue);
    let (out, into) = (producer.up(1), consumer.up(1));
    producer.written(out);
    consumer.written(into);
    producer.core.set_delivery(EVENT, None, confirmed(25, HOUR)).expect("set");
    let ends = Ends::default();
    for id in 1..=100 {
        send(&mut producer, id, LONG, &ends, None);
    }
    for frame in producer.written(out) {
        consumer.deliver(into, &frame);
    }
    for (id, number) in received(&mut consumer) {
        assert_eq!(Some(id), number, "the n-th message the n-th number");
        consumer.core.confirm(producer.core.instance_id, number.unwrap(), Confirmed::Processed);
    }
    consumer.core.send_confirmations();
    consumer.settle();
    let sent: Vec<MultiplexerMessage> = consumer.written(into);
    assert_eq!(1, sent.len());
    producer.deliver(out, &sent[0]);
    assert_eq!(vec![1; 100], *ends.lock().unwrap());
    assert_eq!((0, 0), (producer.core.unconfirmed(), producer.core.sent_again()));
}
