//! A client's delivery, its batching (`Delivery::batch`): the events sent
//! nobody waits for, of a batched type, go several in one message, full
//! batches as they fill and the rest at a flush or at their delay; the keys
//! of a type batch apart, and one key's setting goes ahead of its type's,
//! one that neither batches nor confirms sending its messages alone, by
//! its own timeout; a send that does not batch lets the batches go first; a
//! batch's message stays within max_bytes; with no delay, a message goes
//! alone while a connection has nothing to write and those sent while it is
//! busy go together once it is not, or at their batch's deadline; a batch's
//! timeout counts from its first message, the sends' own not used; each
//! message's tracker and `done` hear its batch's end, and a batch given up
//! on is told for each message inside, a flush that let it go hearing it
//! lost, a shutdown letting it go first; the settings are checked; and a
//! batch that arrives is split into its messages, what does not belong in
//! it dropped, a full queue dropping it whole. Mirrors
//! multiplexer/batching_test.cc.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;

use crate::core::batching::{BatchMark, Delivery};
use crate::core::client::{DropReason, IncomingMode};
use crate::core::harness::{decode, Harness};
use crate::core::outbox::{Frame, SendDone};
use crate::core::{ConnId, SendState, Tracker, MAX_TIMEOUT};
use crate::defaults::{DEFAULT_INCOMING_QUEUE_MAX_SIZE, MAX_BATCH_RECORDS, MAX_KEY_SIZE, MAX_MESSAGE_SIZE};
use crate::error::Error;
use crate::frame;
use crate::proto::multiplexer::{DeliveryError, MessageBatch, MultiplexerMessage};

pub(super) const EVENT: u32 = 300;
pub(super) const TEN: Option<Duration> = Some(Duration::from_secs(10));
/// A delay no test lasts: its batches close by count, a flush or a write.
pub(super) const HOUR: Duration = Duration::from_secs(3600);

/// Batching by `max_records`, its delay an hour, its batches given up on
/// never.
pub(super) fn by_count(max_records: usize) -> Delivery {
    Delivery { batch: true, max_records, max_delay: HOUR, timeout: None, ..Delivery::default() }
}

/// Batching with no delay, a batch going as soon as a connection has
/// nothing left to write, its timeout the default's.
pub(super) fn no_delay() -> Delivery {
    Delivery { batch: true, ..Delivery::default() }
}

/// An event of type EVENT from the client of `h`, with `key` when given.
pub(super) fn message(h: &Harness, id: u64, key: Option<&str>, payload: &[u8]) -> MultiplexerMessage {
    MultiplexerMessage {
        id: Some(id),
        sender: Some(h.core.instance_id),
        r#type: EVENT,
        message: Some(Bytes::copy_from_slice(payload)),
        key: key.map(str::to_string),
        ..Default::default()
    }
}

/// Its frame, made and marked as `message::frame_of` makes a plain event's.
pub(super) fn event(h: &Harness, id: u64, key: Option<&str>, payload: &[u8]) -> Frame {
    Frame {
        bytes: frame::encode_unsealed(&message(h, id, key, payload)).expect("a frame"),
        head: None,
        id,
        r#type: EVENT,
        batch: BatchMark::Event(key.map(str::to_string)),
    }
}

/// Sends an event as a send nobody waits for, which may batch.
pub(super) fn queue(h: &mut Harness, id: u64, key: Option<&str>, payload: &[u8]) {
    let frame = event(h, id, key, payload);
    assert!(h.core.send(frame, false, None, TEN, 0, None, None, None, true));
    h.settle();
}

/// A frame written on the wire, of type EVENT: a batch's, its key and the
/// ids of the messages inside it, in order, or one message's, alone.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Wrote {
    Batch(Option<String>, Vec<u64>),
    Alone(Option<String>, u64),
}

/// What was written on `conn` since the last call, of type EVENT.
pub(super) fn wrote(h: &mut Harness, conn: ConnId) -> Vec<Wrote> {
    h.written(conn)
        .into_iter()
        .filter(|written| written.r#type == EVENT)
        .map(|written| match written.batch.is_some() {
            true => {
                let key = written.key.clone();
                Wrote::Batch(key, inside(written).iter().map(|each| each.id.unwrap_or(0)).collect())
            }
            false => Wrote::Alone(written.key.clone(), written.id.unwrap_or(0)),
        })
        .collect()
}

/// The messages inside a batch's message.
pub(super) fn inside(envelope: MultiplexerMessage) -> Vec<MultiplexerMessage> {
    let batch = MessageBatch::decode(envelope.batch.expect("a batch")).expect("a MessageBatch");
    batch.message.into_iter().map(|body| MultiplexerMessage::decode(body).expect("a message")).collect()
}

/// A batch's message from instance 77, of type EVENT unless `r#type`
/// says, holding `bodies`.
fn envelope(id: u64, r#type: u32, key: Option<&str>, bodies: Vec<Bytes>) -> MultiplexerMessage {
    MultiplexerMessage {
        id: Some(id),
        sender: Some(77),
        r#type,
        key: key.map(str::to_string),
        batch: Some(MessageBatch { message: bodies }.encode_to_vec().into()),
        ..Default::default()
    }
}

/// An event's encoded form as a batch carries it.
fn body(message: &MultiplexerMessage) -> Bytes {
    message.encode_to_vec().into()
}

/// An event from instance 77, as another client batched it.
fn theirs(id: u64, key: Option<&str>) -> MultiplexerMessage {
    MultiplexerMessage {
        id: Some(id),
        sender: Some(77),
        r#type: EVENT,
        message: Some(Bytes::from(format!("payload {id}"))),
        key: key.map(str::to_string),
        ..Default::default()
    }
}

#[test]
fn queued_events_go_in_batches_and_a_flush_sends_the_rest() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, by_count(3)).expect("set");
    for id in 1..=7 {
        queue(&mut h, id, None, b"event");
    }
    assert_eq!(vec![Wrote::Batch(None, vec![1, 2, 3]), Wrote::Batch(None, vec![4, 5, 6])], wrote(&mut h, conn));
    assert_eq!(1, h.core.batched_messages());
    h.core.begin_flush(None);
    h.settle();
    assert_eq!(vec![Wrote::Alone(None, 7)], wrote(&mut h, conn), "a batch of one goes as the message itself");
    assert_eq!(0, h.core.batched_messages());
}

#[test]
fn a_batch_goes_at_its_first_messages_delay() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    let delivery = Delivery { batch: true, max_delay: Duration::from_secs(2), ..Delivery::default() };
    h.core.set_delivery(EVENT, None, delivery).expect("set");
    queue(&mut h, 1, None, b"event");
    h.advance(Duration::from_secs(1));
    queue(&mut h, 2, None, b"event");
    h.advance(Duration::from_millis(999));
    assert!(wrote(&mut h, conn).is_empty(), "not before the first message's delay is over");
    h.advance(Duration::from_millis(1));
    assert_eq!(vec![Wrote::Batch(None, vec![1, 2])], wrote(&mut h, conn));
    assert_eq!(None, h.core.batcher_armed(), "no batch timer left");
}

#[test]
fn a_batch_that_went_before_its_delay_leaves_no_timer() {
    // A synchronous client's wait takes an armed timer for work: one left
    // armed for a batch already gone kept a receive with nothing connected
    // waiting for the whole delay.
    let mut h = Harness::new(200, IncomingMode::Queue);
    h.core.set_delivery(EVENT, None, by_count(2)).expect("set");
    queue(&mut h, 1, None, b"event");
    assert!(h.core.batcher_armed().is_some(), "armed for the delay");
    queue(&mut h, 2, None, b"event"); // full: gone, and held for a connection
    assert_eq!(None, h.core.batcher_armed(), "stopped with the last batch that had a delay");
    queue(&mut h, 3, None, b"event");
    h.core.begin_flush(None);
    h.settle();
    assert_eq!(None, h.core.batcher_armed(), "and at a flush");
}

#[test]
fn the_keys_of_a_type_batch_apart_and_go_in_the_order_they_began() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, by_count(100)).expect("set");
    let sent = [(1, Some("a")), (2, Some("b")), (3, None), (4, Some("a")), (5, Some("")), (6, None), (7, Some(""))];
    for (id, key) in sent {
        queue(&mut h, id, key, b"event");
    }
    queue(&mut h, 8, Some("b"), b"event");
    h.core.begin_flush(None);
    h.settle();
    assert_eq!(
        vec![
            Wrote::Batch(Some("a".to_string()), vec![1, 4]),
            Wrote::Batch(Some("b".to_string()), vec![2, 8]),
            Wrote::Batch(None, vec![3, 6]),
            Wrote::Batch(Some(String::new()), vec![5, 7]), // an empty key is a key: it routes otherwise than none
        ],
        wrote(&mut h, conn)
    );
}

#[test]
fn a_keys_own_setting_goes_ahead_of_its_types_and_other_keys_go_alone() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, by_count(10)).expect("set");
    h.core.set_delivery(EVENT, Some("a"), by_count(2)).expect("set");
    for (id, key) in [(1, "a"), (2, "b"), (3, "a"), (4, "a"), (5, "b")] {
        queue(&mut h, id, Some(key), b"event");
    }
    assert_eq!(vec![Wrote::Batch(Some("a".to_string()), vec![1, 3])], wrote(&mut h, conn), "two by its own setting");
    h.core.begin_flush(None);
    h.settle();
    assert_eq!(
        vec![Wrote::Batch(Some("b".to_string()), vec![2, 5]), Wrote::Alone(Some("a".to_string()), 4)],
        wrote(&mut h, conn)
    );
    // The type's setting gone, its keys' with it; the key's own set again.
    h.core.clear_delivery(EVENT, None);
    h.core.set_delivery(EVENT, Some("a"), by_count(2)).expect("set");
    queue(&mut h, 6, Some("a"), b"event");
    queue(&mut h, 7, Some("b"), b"event");
    queue(&mut h, 8, None, b"event");
    assert_eq!(
        vec![Wrote::Alone(Some("a".to_string()), 6), Wrote::Alone(Some("b".to_string()), 7), Wrote::Alone(None, 8)],
        wrote(&mut h, conn),
        "a key not batched lets the batch go first, and goes alone"
    );
}

/// A synchronous client's queue() runs the loop first unless the message
/// only joins a batch with a delay: the one that would fill its batch, one
/// with no delay, one too big, one not batched, one to ALL or through a
/// lane, and any once a batch's delay is over, do (C++
/// AQueuedMessageThatOnlyJoinsABatchRunsNoLoop).
#[test]
fn a_send_that_only_joins_a_batch_needs_no_loop() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    let now = h.core.now;
    let needs = |h: &Harness, id: u64, all: bool, lane: bool, at| {
        h.core.send_needs_loop(&event(h, id, None, b"event"), all, lane, at)
    };
    assert!(needs(&h, 1, false, false, now), "nothing batched: as every send");
    h.core.set_delivery(EVENT, None, by_count(3)).expect("set");
    assert!(!needs(&h, 1, false, false, now), "it only opens a batch");
    queue(&mut h, 1, None, b"event");
    assert!(!needs(&h, 2, false, false, now), "it only joins one");
    assert!(needs(&h, 2, false, false, now + HOUR), "the batch's delay over");
    assert!(needs(&h, 2, true, false, now), "to ALL it goes alone");
    assert!(needs(&h, 2, false, true, now), "through a lane too");
    queue(&mut h, 2, None, b"event");
    assert!(needs(&h, 3, false, false, now), "it fills the batch, which goes");
    let big = event(&h, 4, None, &[b'b'; 100]);
    h.core.set_delivery(EVENT, None, Delivery { max_bytes: 50, ..by_count(100) }).expect("set");
    assert!(h.core.send_needs_loop(&big, false, false, now), "too big: it goes alone");
    h.core.set_delivery(EVENT, None, Delivery { max_delay: Duration::ZERO, ..by_count(100) }).expect("set");
    assert!(needs(&h, 5, false, false, now), "no delay: whether a connection is free decides");
    h.core.set_delivery(EVENT, None, Delivery { confirm: true, ..Delivery::default() }).expect("set");
    assert!(needs(&h, 6, false, false, now), "not batched: it goes alone");
}

/// An event's frame, made unsealed, is sealed only if it goes alone: by a
/// send that does not batch, or as a batch of one; never in a batch, where
/// only its body is read. The harness checks every written frame's
/// checksum.
#[test]
fn an_event_in_a_batch_gets_no_checksum_of_its_own() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, by_count(100)).expect("set");
    let sealed_before = frame::SEALED.with(|sealed| sealed.get());
    for id in 1..=3 {
        queue(&mut h, id, None, b"event");
    }
    let waited_for = event(&h, 4, None, b"event");
    assert!(h.core.send(waited_for, false, None, TEN, 0, None, None, None, false));
    queue(&mut h, 5, None, b"event");
    h.core.begin_flush(None);
    h.settle();
    assert_eq!(
        vec![Wrote::Batch(None, vec![1, 2, 3]), Wrote::Alone(None, 4), Wrote::Alone(None, 5)],
        wrote(&mut h, conn)
    );
    let sealed = frame::SEALED.with(|sealed| sealed.get()) - sealed_before;
    assert_eq!(2, sealed, "the two that went alone, and none of the batch's three");
}

#[test]
fn a_send_that_does_not_batch_lets_the_batches_go_first() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, by_count(100)).expect("set");
    queue(&mut h, 1, None, b"event");
    queue(&mut h, 2, None, b"event");
    let waited_for = event(&h, 3, None, b"event");
    assert!(h.core.send(waited_for, false, None, TEN, 0, None, None, None, false));
    queue(&mut h, 4, None, b"event");
    queue(&mut h, 5, None, b"event");
    let to_all = event(&h, 6, None, b"event");
    assert!(h.core.send(to_all, true, None, TEN, 0, None, None, None, true));
    h.settle();
    assert_eq!(
        vec![
            Wrote::Batch(None, vec![1, 2]),
            Wrote::Alone(None, 3),
            Wrote::Batch(None, vec![4, 5]),
            Wrote::Alone(None, 6)
        ],
        wrote(&mut h, conn)
    );
}

#[test]
fn a_batchs_message_stays_within_max_bytes_on_the_wire() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    // The messages' own sizes fill the batch to the brim: its envelope,
    // measured, is over, and the last messages go next.
    let payload = [b'x'; 140];
    let one = message(&h, 1, None, &payload).encoded_len();
    let max_bytes = 4 * one + 2;
    let delivery = Delivery { max_bytes, ..by_count(crate::defaults::DEFAULT_BATCH_MAX_RECORDS) };
    h.core.set_delivery(EVENT, None, delivery).expect("set");
    for id in 1..=20 {
        queue(&mut h, id, None, &payload);
    }
    let big = [b'y'; 1000];
    queue(&mut h, 21, None, &big); // over max_bytes alone: after the batch, by itself
    h.core.begin_flush(None);
    h.settle();
    let written = h.written(conn);
    let mut ids = Vec::new();
    let mut batches = 0;
    for frame in &written {
        if frame.r#type != EVENT {
            continue;
        }
        if frame.batch.is_some() {
            assert!(frame.encoded_len() <= max_bytes, "{} bytes, over {max_bytes}", frame.encoded_len());
            batches += 1;
            ids.extend(inside(frame.clone()).iter().map(|each| each.id.unwrap_or(0)));
        } else {
            ids.push(frame.id.unwrap_or(0));
        }
    }
    assert_eq!((1..=21).collect::<Vec<u64>>(), ids, "every message, in order");
    assert!(batches >= 5, "{batches} batches: four messages' worth is over max_bytes, measured");
}

#[test]
fn with_no_delay_messages_sent_while_the_connection_is_busy_go_together() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.hold_writes = true;
    h.core.set_delivery(EVENT, None, no_delay()).expect("set");
    queue(&mut h, 1, None, b"event");
    assert_eq!(0, h.core.batched_messages(), "a connection with nothing to write takes it at once");
    for id in 2..=4 {
        queue(&mut h, id, None, b"event");
    }
    assert_eq!(3, h.core.batched_messages(), "gathered while the first is written");
    h.complete_write(conn);
    assert_eq!(vec![Wrote::Alone(None, 1)], wrote(&mut h, conn));
    assert_eq!(0, h.core.batched_messages(), "the connection's write left nothing to write: the batch went");
    h.complete_write(conn);
    assert_eq!(vec![Wrote::Batch(None, vec![2, 3, 4])], wrote(&mut h, conn));
}

#[test]
fn with_no_delay_a_batch_goes_when_the_busy_connection_dies() {
    // Nothing would ever be written on the busy connection again: its death
    // hands its queue over, and the batch goes after it, held for the next
    // connection with nothing live.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.hold_writes = true;
    h.core.set_delivery(EVENT, None, no_delay()).expect("set");
    for id in 1..=3 {
        queue(&mut h, id, None, b"event");
    }
    assert_eq!(2, h.core.batched_messages());
    h.fail_write(conn);
    assert_eq!(0, h.core.batched_messages(), "the batch went");
    assert_eq!(2, h.core.held_for_a_connection(), "the first and the batch, held for a connection");
    h.hold_writes = false;
    let again = h.up(2);
    assert_eq!(vec![Wrote::Alone(None, 1), Wrote::Batch(None, vec![2, 3])], wrote(&mut h, again));
}

#[test]
fn with_no_delay_a_batch_goes_when_a_connection_comes_up() {
    // The only connection stays busy: the one that comes up has nothing to
    // write, and takes the batch at its registration.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let busy = h.up(1);
    h.written(busy);
    h.hold_writes = true;
    h.core.set_delivery(EVENT, None, no_delay()).expect("set");
    for id in 1..=3 {
        queue(&mut h, id, None, b"event");
    }
    assert_eq!(2, h.core.batched_messages());
    let conn = h.connect(2);
    h.accept(conn);
    h.complete_write(conn); // its welcome
    h.welcome(conn, 2);
    assert_eq!(0, h.core.batched_messages(), "the batch went at the registration");
    h.complete_writes();
    assert_eq!(vec![Wrote::Alone(None, 1)], wrote(&mut h, busy));
    assert_eq!(vec![Wrote::Batch(None, vec![2, 3])], wrote(&mut h, conn));
}

#[test]
fn each_messages_tracker_and_done_hear_its_batchs_end() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.hold_writes = true;
    h.core.set_delivery(EVENT, None, by_count(3)).expect("set");
    let heard = Arc::new(Mutex::new(Vec::new()));
    let mut trackers: Vec<Arc<Tracker>> = Vec::new();
    for id in 1..=3 {
        let hearing = Arc::clone(&heard);
        let done = SendDone::Callback(Box::new(move |written| hearing.lock().unwrap().push((id, written))));
        let frame = event(&h, id, None, b"event");
        assert!(h.core.send(frame, false, None, TEN, 0, Some(&mut trackers), None, Some(done), true));
        h.settle();
    }
    assert_eq!(3, trackers.len());
    assert!(trackers.iter().all(|tracker| tracker.get() == SendState::Queued), "until the batch is written");
    assert!(heard.lock().unwrap().is_empty());
    h.complete_write(conn);
    assert!(trackers.iter().all(|tracker| tracker.get() == SendState::Sent));
    assert_eq!(vec![(1, 1), (2, 1), (3, 1)], *heard.lock().unwrap(), "each, in the order sent");
}

#[test]
fn a_batch_given_up_on_is_told_for_each_message_inside() {
    // Nothing connected: the full batch is held for a connection, one
    // message that holds three, until its deadline, its timeout from its
    // first message; none comes, and then it is given up on, told and
    // counted for each message, each tracker and `done` hearing it.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let delivery = Delivery { max_delay: Duration::from_secs(5), timeout: TEN, ..by_count(3) };
    h.core.set_delivery(EVENT, None, delivery).expect("set");
    let heard = Arc::new(Mutex::new(Vec::new()));
    let mut trackers: Vec<Arc<Tracker>> = Vec::new();
    for id in 1..=3 {
        let hearing = Arc::clone(&heard);
        let done = SendDone::Callback(Box::new(move |written| hearing.lock().unwrap().push(written)));
        let frame = event(&h, id, None, b"event");
        assert!(h.core.send(frame, false, None, None, 0, Some(&mut trackers), None, Some(done), true));
        h.settle();
        if id < 3 {
            h.advance(Duration::from_secs(1));
        }
    }
    assert_eq!(1, h.core.held_for_a_connection(), "one batch, held");
    h.advance(Duration::from_millis(7_999));
    assert!(drops.lock().unwrap().is_empty());
    h.advance(Duration::from_millis(1));
    assert_eq!(
        vec![(1, DropReason::NoConnection), (2, DropReason::NoConnection), (3, DropReason::NoConnection)],
        *drops.lock().unwrap(),
        "ten seconds from its first message, not from when it went nor from the sends'"
    );
    assert_eq!(3, h.core.client.dropped.load(Ordering::Relaxed), "counted for each");
    assert_eq!(vec![0, 0, 0], *heard.lock().unwrap());
    assert!(trackers.iter().all(|tracker| tracker.get() == SendState::Lost));
}

#[test]
fn a_sends_own_timeout_is_not_used() {
    // The time is the batch's: a message sent with no time to wait joins
    // the batch as any other does, where it went alone before.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, by_count(100)).expect("set");
    queue(&mut h, 1, None, b"event");
    let now = event(&h, 2, None, b"event");
    assert!(h.core.send(now, false, None, Some(Duration::ZERO), 0, None, None, None, true));
    queue(&mut h, 3, None, b"event");
    assert_eq!(3, h.core.batched_messages());
    h.core.begin_flush(None);
    h.settle();
    assert_eq!(vec![Wrote::Batch(None, vec![1, 2, 3])], wrote(&mut h, conn));
}

#[test]
fn a_key_set_to_go_as_it_is_goes_alone_while_its_type_is_batched() {
    // A key's setting that neither batches nor confirms sends its messages
    // as any send, the type's batch let go first, its other keys batched.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, by_count(100)).expect("set");
    h.core.set_delivery(EVENT, Some("urgent"), Delivery::default()).expect("set");
    queue(&mut h, 1, Some("other"), b"event");
    queue(&mut h, 2, Some("urgent"), b"event");
    queue(&mut h, 3, Some("other"), b"event");
    assert_eq!(1, h.core.batched_messages(), "the last, in its batch");
    h.core.begin_flush(None);
    h.settle();
    assert_eq!(
        vec![
            Wrote::Alone(Some("other".to_string()), 1),
            Wrote::Alone(Some("urgent".to_string()), 2),
            Wrote::Alone(Some("other".to_string()), 3)
        ],
        wrote(&mut h, conn)
    );
}

#[test]
fn a_delivery_that_neither_batches_nor_confirms_sends_by_its_own_timeout() {
    // Nothing connected: the message is held for a connection, given up
    // on at the setting's timeout from its send, not at the send's own.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let delivery = Delivery { timeout: Some(Duration::from_secs(2)), ..Delivery::default() };
    h.core.set_delivery(EVENT, None, delivery).expect("set");
    queue(&mut h, 1, None, b"event"); // its own timeout ten seconds
    assert_eq!((0, 1), (h.core.batched_messages(), h.core.held_for_a_connection()), "alone, held");
    h.advance(Duration::from_millis(1_999));
    assert!(drops.lock().unwrap().is_empty());
    h.advance(Duration::from_millis(1));
    assert_eq!(vec![(1, DropReason::NoConnection)], *drops.lock().unwrap());
}

#[test]
fn a_batchs_timeout_counts_from_its_first_message() {
    // A batch's time is its timeout from its first message, its delay
    // included: one that goes past it, nothing connected, is given up on at
    // once, every message of it told, the sends' own timeouts not used. Its
    // client ran no loop meanwhile, as a synchronous one between calls.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let delivery =
        Delivery { max_delay: Duration::from_millis(1), timeout: Some(Duration::from_millis(2)), ..by_count(100) };
    h.core.set_delivery(EVENT, None, delivery).expect("set");
    queue(&mut h, 1, None, b"event"); // its own timeout ten seconds
    queue(&mut h, 2, None, b"event");
    h.core.now += Duration::from_millis(10);
    h.core.begin_flush(None); // lets the batch go: with nothing connected, given up on
    h.settle();
    assert_eq!(vec![(1, DropReason::NoConnection), (2, DropReason::NoConnection)], *drops.lock().unwrap());
    assert_eq!(2, h.core.client.dropped.load(Ordering::Relaxed));
    assert_eq!(0, h.core.held_for_a_connection(), "not held for its sends' ten seconds");
}

#[test]
fn with_no_delay_a_batch_goes_at_its_deadline() {
    // A batch with no delay waits for room: its timeout runs from its
    // first message, and it goes when it is up, at the latest, as a message
    // that waited for room on a full connection would; the queue still
    // full, it is given up on then, where it waited for a connection that
    // never had nothing left to write. The sends' own timeouts, none, are
    // not used: by them the batch would wait for good.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let conn = h.up(1);
    h.written(conn);
    h.hold_writes = true;
    for id in 1..=crate::core::connection::DEFAULT_OUT_QUEUE_SIZE as u64 {
        assert!(h.send(id)); // before any delivery: the queue full
    }
    let delivery = Delivery { timeout: Some(Duration::from_secs(2)), ..no_delay() };
    h.core.set_delivery(EVENT, None, delivery).expect("set");
    for id in 5001..=5002 {
        let frame = event(&h, id, None, b"event");
        assert!(h.core.send(frame, false, None, None, 0, None, None, None, true));
        h.settle();
    }
    assert_eq!(2, h.core.batched_messages());
    h.advance(Duration::from_millis(1_999));
    assert!(drops.lock().unwrap().is_empty());
    h.advance(Duration::from_millis(1));
    assert_eq!(0, h.core.batched_messages(), "gone at the deadline");
    assert_eq!(vec![(5001, DropReason::NoRoom), (5002, DropReason::NoRoom)], *drops.lock().unwrap());
}

#[test]
fn a_batch_that_goes_lets_the_batches_waiting_for_a_connection_go_first() {
    // Key "a" batches with no delay, key "b" by three. The connection is
    // busy, so "a"'s batch waits for it to have nothing left to write; "b"'s
    // fills meanwhile and goes on its own, and "a"'s, older, goes before
    // it, where it waited behind "b"'s and every batch after.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.hold_writes = true;
    h.core.set_delivery(EVENT, Some("a"), no_delay()).expect("set");
    h.core.set_delivery(EVENT, Some("b"), by_count(3)).expect("set");
    assert!(h.send(1)); // no key, not batched: the connection busy
    queue(&mut h, 2, Some("a"), b"event");
    for id in 3..=5 {
        queue(&mut h, id, Some("b"), b"event");
    }
    h.complete_writes();
    assert_eq!(
        vec![
            Wrote::Alone(None, 1),
            Wrote::Alone(Some("a".to_string()), 2),
            Wrote::Batch(Some("b".to_string()), vec![3, 4, 5])
        ],
        wrote(&mut h, conn)
    );
}

#[test]
fn a_batch_a_connection_let_go_goes_on_that_connection() {
    // Two connections, both busy: the batch waits. The second finishes its
    // write, and the batch goes on it, where the round robin had put it on
    // the first, behind the write still in flight there.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let one = h.up(1);
    let two = h.up(2);
    h.written(one);
    h.written(two);
    h.hold_writes = true;
    assert!(h.send(1));
    assert!(h.send(2));
    h.core.set_delivery(EVENT, None, no_delay()).expect("set");
    for id in 3..=4 {
        queue(&mut h, id, None, b"event");
    }
    assert_eq!(2, h.core.batched_messages());
    // The connection that took the second message, the round robin's last
    // and so at the back of its turns, finishes first.
    let writing = |h: &Harness, conn: ConnId| h.sockets[&conn].in_flight.as_ref().map(|bytes| decode(bytes).id);
    let last = if writing(&h, one) == Some(Some(2)) { one } else { two };
    h.complete_write(last);
    assert_eq!(0, h.core.batched_messages());
    h.complete_write(last);
    assert_eq!(vec![Wrote::Alone(None, 2), Wrote::Batch(None, vec![3, 4])], wrote(&mut h, last));
}

#[test]
fn a_shutdown_lets_the_batches_go_first() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    h.core.set_delivery(EVENT, None, by_count(100)).expect("set");
    for id in 1..=2 {
        queue(&mut h, id, None, b"event");
    }
    h.core.shutdown_client();
    h.settle();
    assert_eq!(vec![(1, DropReason::ShutDown), (2, DropReason::ShutDown)], *drops.lock().unwrap());
    assert_eq!(None, h.core.batcher_armed(), "the timer stopped");
}

#[test]
fn the_settings_are_checked() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let batched = no_delay();
    let second = Duration::from_secs(1);
    let refused = [
        (50, None, batched),
        (EVENT, Some("k".repeat(MAX_KEY_SIZE + 1)), batched),
        (EVENT, None, Delivery { max_records: 0, ..batched }),
        (EVENT, None, Delivery { max_records: MAX_BATCH_RECORDS + 1, ..batched }),
        (EVENT, None, Delivery { max_bytes: 0, ..batched }),
        (EVENT, None, Delivery { max_bytes: MAX_MESSAGE_SIZE + 1, ..batched }),
        (EVENT, None, Delivery { max_delay: MAX_TIMEOUT + second, ..batched }),
        // A timeout no longer than the delay would give a batch up as it
        // goes, and zero a message alone at once.
        (EVENT, None, Delivery { max_delay: second, timeout: Some(Duration::ZERO), ..batched }),
        (EVENT, None, Delivery { max_delay: second, timeout: Some(second / 2), ..batched }),
        (EVENT, None, Delivery { max_delay: second, timeout: Some(second), ..batched }),
        (EVENT, None, Delivery { timeout: Some(Duration::ZERO), ..Delivery::default() }),
        (EVENT, None, Delivery { confirm: true, timeout: Some(Duration::ZERO), ..Delivery::default() }),
    ];
    for (r#type, key, delivery) in refused {
        let result = h.core.set_delivery(r#type, key.as_deref(), delivery);
        assert!(matches!(result, Err(Error::InvalidArgument(_))), "{type} {key:?} {delivery:?}: {result:?}");
    }
    let most = Delivery {
        batch: true,
        max_records: MAX_BATCH_RECORDS,
        max_bytes: MAX_MESSAGE_SIZE,
        max_delay: MAX_TIMEOUT,
        confirm: true,
        retry_after: MAX_TIMEOUT,
        timeout: Some(Duration::MAX),
    };
    h.core.set_delivery(EVENT, Some(&"k".repeat(MAX_KEY_SIZE)), most).expect("the limits themselves");
    for timeout in [Some(second + second / 2), None, Some(Duration::MAX)] {
        let longer = Delivery { max_delay: second, timeout, ..batched };
        h.core.set_delivery(EVENT, None, longer).unwrap_or_else(|error| panic!("{timeout:?}: {error}"));
    }
    h.core.set_delivery(EVENT, None, Delivery::default()).expect("one by one, by the default timeout");
    h.core.clear_delivery(EVENT, None);
    assert!(!h.core.batcher.configured());
}

#[test]
fn a_batch_that_arrives_is_split_into_its_messages_as_if_each_came_alone() {
    for mode in [IncomingMode::Queue, IncomingMode::Dispatch] {
        let mut h = Harness::new(200, mode);
        let handed = Arc::new(Mutex::new(Vec::new()));
        let kept = Arc::clone(&handed);
        h.core.threaded.on_message = Some(Box::new(move |incoming| kept.lock().unwrap().push(incoming.message)));
        let conn = h.up(1);
        let sent: Vec<MultiplexerMessage> = (11..=13).map(|id| theirs(id, Some("k"))).collect();
        let batch = envelope(900, EVENT, Some("k"), sent.iter().map(body).collect());
        let handled = h.core.client.frames_handled;
        h.deliver(conn, &batch);
        assert_eq!(handled + 3, h.core.client.frames_handled, "a poll counts each message");
        h.deliver(conn, &batch); // its copy through another multiplexer
        let received: Vec<MultiplexerMessage> = match mode {
            IncomingMode::Queue => h.core.client.incoming.drain(..).map(|incoming| incoming.message).collect(),
            IncomingMode::Dispatch => handed.lock().unwrap().iter().map(|message| message.clone().into()).collect(),
        };
        assert_eq!(sent, received, "{mode:?}: each as it was sent, once");
    }
}

#[test]
fn what_does_not_belong_in_a_batch_that_arrives_is_dropped() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    let good = theirs(21, None);
    let addressed = MultiplexerMessage { to: Some(5), ..theirs(22, None) };
    let other_type = MultiplexerMessage { r#type: EVENT + 1, ..theirs(23, None) };
    let other_key = theirs(24, Some("k"));
    let no_id = MultiplexerMessage { id: None, ..theirs(25, None) };
    let reply = MultiplexerMessage { references: Some(9), ..theirs(26, None) };
    let batch_inside = MultiplexerMessage { batch: Some(Bytes::new()), ..theirs(27, None) };
    let last = theirs(28, None);
    let mut bodies: Vec<Bytes> =
        [&good, &addressed, &other_type, &other_key, &no_id, &reply, &batch_inside].into_iter().map(body).collect();
    bodies.push(Bytes::from_static(b"\xff\xff\xff not a message"));
    bodies.push(body(&last));
    h.deliver(conn, &envelope(900, EVENT, None, bodies));
    assert_eq!(vec![21, 28], h.queued_ids());
    // A batch of a protocol type, or whose batch does not decode, is
    // dropped whole.
    h.deliver(conn, &envelope(901, 50, None, vec![body(&theirs(31, None))]));
    let garbage = MultiplexerMessage { batch: Some(Bytes::from_static(b"\xff\xff\xff")), ..theirs(902, None) };
    h.deliver(conn, &garbage);
    assert_eq!(vec![21, 28], h.queued_ids());
    // One message more than MAX_BATCH_RECORDS: dropped whole.
    let over: Vec<Bytes> = (0..=MAX_BATCH_RECORDS as u64).map(|id| body(&theirs(40_000 + id, None))).collect();
    h.deliver(conn, &envelope(903, EVENT, None, over));
    assert_eq!(vec![21, 28], h.queued_ids());
    // A message seen before is dropped, inside a batch as alone; an enum's
    // value the protocol does not name is no value, as C++ reads it.
    let unknown = MultiplexerMessage { compression: Some(5), logging_method: Some(7), ..theirs(32, None) };
    h.deliver(conn, &envelope(904, EVENT, None, vec![body(&theirs(28, None)), body(&unknown), body(&unknown)]));
    assert_eq!(vec![21, 28, 32], h.queued_ids());
}

#[test]
fn a_delivery_error_for_a_batch_is_told_for_each_message() {
    // The multiplexer sends a batch nobody took back whole in its
    // DELIVERY_ERROR: the program hears one DELIVERY_ERROR for each message
    // inside, naming it, with its workflow and the message as the original,
    // as the multiplexer would have sent it for the message alone. One whose
    // original was left out goes on as it is.
    for mode in [IncomingMode::Queue, IncomingMode::Dispatch] {
        let mut h = Harness::new(200, mode);
        let handed = Arc::new(Mutex::new(Vec::new()));
        let kept = Arc::clone(&handed);
        h.core.threaded.on_message = Some(Box::new(move |incoming| kept.lock().unwrap().push(incoming.message)));
        let conn = h.up(1);
        let mine = h.core.instance_id;
        let sent: Vec<MultiplexerMessage> = (11..=13)
            .map(|id| MultiplexerMessage {
                sender: Some(mine),
                workflow: Some(format!("flow {id}")),
                ..theirs(id, None)
            })
            .collect();
        let batch =
            MultiplexerMessage { sender: Some(mine), ..envelope(900, EVENT, None, sent.iter().map(body).collect()) };
        let report = DeliveryError {
            packet_id: 900,
            failed_type: vec![201],
            original_message: Some(batch),
            ..Default::default()
        };
        let error = |id: u64, references: u64, report: &DeliveryError| MultiplexerMessage {
            id: Some(id),
            sender: Some(crate::core::harness::multiplexer_id(1)),
            to: Some(mine),
            report_delivery_error: Some(false),
            r#type: crate::constants::types::DELIVERY_ERROR,
            references: Some(references),
            message: Some(report.encode_to_vec().into()),
            ..Default::default()
        };
        h.deliver(conn, &error(7000, 900, &report));
        let omitted = DeliveryError { packet_id: 901, original_message_omitted: Some(true), ..Default::default() };
        h.deliver(conn, &error(7001, 901, &omitted));
        let received: Vec<MultiplexerMessage> = match mode {
            IncomingMode::Queue => h.core.client.incoming.drain(..).map(|incoming| incoming.message).collect(),
            IncomingMode::Dispatch => handed.lock().unwrap().iter().map(|message| message.clone().into()).collect(),
        };
        assert_eq!(4, received.len(), "{mode:?}");
        for (told, original) in received.iter().zip(&sent) {
            assert_eq!(crate::constants::types::DELIVERY_ERROR, told.r#type);
            assert_eq!(original.id, told.references, "{mode:?}");
            assert_eq!(original.workflow, told.workflow);
            let each = DeliveryError::decode(told.message.clone().unwrap()).unwrap();
            assert_eq!(original.id, Some(each.packet_id));
            assert_eq!(vec![201], each.failed_type);
            assert_eq!(Some(original), each.original_message.as_ref());
        }
        assert_eq!(Some(7001), received[3].id, "{mode:?}: left out, as it is");
    }
}

#[test]
fn a_leaving_server_with_a_full_queue_refuses_each_message_of_a_batch() {
    // As it refuses a message that came alone, whatever its queue holds:
    // where a full queue dropped the batch, no sender told.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    for id in 1..=DEFAULT_INCOMING_QUEUE_MAX_SIZE as u64 {
        h.deliver(conn, &theirs(id, None));
    }
    h.core.refuse_arrivals();
    h.written(conn);
    let bodies: Vec<Bytes> = (5001..=5003).map(|id| body(&theirs(id, None))).collect();
    h.deliver(conn, &envelope(90_000, EVENT, None, bodies));
    let refused: Vec<u64> = h
        .written(conn)
        .iter()
        .filter(|written| written.r#type == crate::constants::types::DELIVERY_ERROR)
        .map(|written| written.references.unwrap_or(0))
        .collect();
    assert_eq!(vec![5001, 5002, 5003], refused);
    assert_eq!(0, h.core.client.incoming_dropped.load(Ordering::Relaxed));
}

#[test]
fn a_full_queue_drops_a_batch_whole_and_takes_its_copy_once_there_is_room() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    for id in 1..=DEFAULT_INCOMING_QUEUE_MAX_SIZE as u64 {
        h.deliver(conn, &theirs(id, None));
    }
    let bodies: Vec<Bytes> = (5001..=5003).map(|id| body(&theirs(id, None))).collect();
    let batch = envelope(90_000, EVENT, None, bodies); // an id none of the others has
    h.deliver(conn, &batch);
    assert_eq!(DEFAULT_INCOMING_QUEUE_MAX_SIZE, h.queued_ids().len(), "dropped whole");
    assert_eq!(3, h.core.client.incoming_dropped.load(Ordering::Relaxed), "counted for each message");
    h.core.next_incoming_message();
    h.deliver(conn, &batch); // its copy through another multiplexer, taken once there is room
    let queued = h.queued_ids();
    assert_eq!(DEFAULT_INCOMING_QUEUE_MAX_SIZE + 2, queued.len(), "past the limit by one batch at most");
    assert_eq!(vec![5001, 5002, 5003], queued[queued.len() - 3..]);
}
