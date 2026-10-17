//! Plain sends as bytes (core/plain.rs): their bytes go in their batches as
//! they came, beside frames' bodies, by type and key, in the order sent;
//! one goes alone, as a frame made of its bytes, when its type has no
//! delivery set or one that does not batch, it is too big, a connection is
//! free with no delay, or it is a batch of one, a big one's own buffer then
//! the frame's body, its header kept apart; a batch hands its bodies back,
//! keeps its envelope and is serialized into a frame buffer an earlier
//! batch frame left; a threaded client's plain send counts its deadline
//! from its send where no delivery's takes its place, and is reported
//! dropped after the shutdown's end. What C++'s BasicClient::send_plain
//! does, which its batching and send allocation tests cover.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;

use super::batching::{by_count, event, inside, message, queue, wrote, Wrote, EVENT, TEN};
use crate::core::batching::Delivery;
use crate::core::bodies::BODY_KEPT_MAX;
use crate::core::client::{DropReason, IncomingMode};
use crate::core::deadline_after;
use crate::core::harness::Harness;
use crate::core::outbox::SendDone;
use crate::core::plain::Plain;
use crate::message::plain_of;

/// An event of type EVENT from the client of `h`, with `key` when given,
/// as a plain send's bytes, serialized into a body the batching kept.
fn plain(h: &mut Harness, id: u64, key: Option<&str>, payload: &[u8]) -> Plain {
    let body = h.core.batcher.bodies.take();
    plain_of(&message(h, id, key, payload), body).expect("a plain send")
}

/// Sends it as a synchronous client's queue() does, 10 s to wait at most.
fn send(h: &mut Harness, id: u64, key: Option<&str>, payload: &[u8]) {
    let plain = plain(h, id, key, payload);
    assert!(h.core.send_plain(plain, TEN, 0));
    h.settle();
}

#[test]
fn plain_sends_batch_with_frames_by_type_and_key_in_the_order_sent() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, None, by_count(100)).expect("set");
    send(&mut h, 1, Some("a"), b"one");
    queue(&mut h, 2, Some("a"), b"two"); // a frame, which a followed send makes
    send(&mut h, 3, None, b"three");
    send(&mut h, 4, Some(""), b"four");
    send(&mut h, 5, Some("a"), b"five");
    send(&mut h, 6, None, b"six");
    h.core.begin_flush(None);
    h.settle();
    let written = h.written(conn);
    let batches: Vec<_> = written.into_iter().filter(|written| written.r#type == EVENT).collect();
    assert_eq!(3, batches.len(), "{batches:?}");
    let ours = |id: u64, key: Option<&str>, payload: &[u8]| message(&h, id, key, payload);
    assert_eq!(Some("a".to_string()), batches[0].key);
    assert_eq!(
        vec![ours(1, Some("a"), b"one"), ours(2, Some("a"), b"two"), ours(5, Some("a"), b"five")],
        inside(batches[0].clone()),
        "each message as it was sent, its key inside it"
    );
    assert_eq!(None, batches[1].key);
    assert_eq!(vec![ours(3, None, b"three"), ours(6, None, b"six")], inside(batches[1].clone()));
    assert_eq!(ours(4, Some(""), b"four"), batches[2], "a batch of one: the message itself, an empty key a key");
}

#[test]
fn a_plain_send_goes_alone_as_a_frame_of_its_bytes() {
    // The harness checks each frame's length and checksum as the
    // multiplexer does.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.core.set_delivery(EVENT, Some("batched"), by_count(100)).expect("set");
    h.core.set_delivery(EVENT, Some("as it is"), Delivery::default()).expect("set");
    send(&mut h, 1, None, b"its type not batched, its key none");
    send(&mut h, 2, Some("as it is"), b"its key's delivery neither batches nor confirms");
    h.core.set_delivery(EVENT, None, Delivery { max_bytes: 40, ..by_count(100) }).expect("set");
    send(&mut h, 3, None, &[b'b'; 100]);
    h.core.set_delivery(EVENT, None, Delivery { max_delay: Duration::ZERO, ..by_count(100) }).expect("set");
    send(&mut h, 4, None, b"no delay, the connection free");
    assert_eq!(
        vec![
            Wrote::Alone(None, 1),
            Wrote::Alone(Some("as it is".to_string()), 2),
            Wrote::Alone(None, 3),
            Wrote::Alone(None, 4)
        ],
        wrote(&mut h, conn)
    );
    assert_eq!(1, h.core.batcher.bodies.len(), "the one body, back each time its bytes were copied into a frame");
}

#[test]
fn a_big_body_that_goes_alone_is_written_from_its_own_buffer_behind_its_header() {
    // A body bigger than the bodies kept, which none would take back, is
    // the frame's bytes, its header kept apart, as C++ swaps a body in; a
    // smaller one is copied behind its header and goes back. The harness
    // checks each frame's length and checksum as the multiplexer does.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.hold_writes = true;
    h.core.set_delivery(EVENT, Some("batched"), by_count(100)).expect("set");
    let big = plain(&mut h, 1, None, &vec![b'b'; BODY_KEPT_MAX]);
    let buffer = big.body.as_ptr();
    assert!(h.core.send_plain(big, TEN, 0));
    h.settle();
    let socket = &h.sockets[&conn];
    assert_eq!(Some(buffer), socket.in_flight.as_ref().map(|bytes| bytes.as_ptr()), "its own buffer, no copy");
    assert!(socket.in_flight_head.is_some(), "its header apart");
    h.complete_write(conn);
    let small = plain(&mut h, 2, None, b"small");
    let buffer = small.body.as_ptr();
    assert!(h.core.send_plain(small, TEN, 0));
    h.settle();
    let socket = &h.sockets[&conn];
    assert_ne!(Some(buffer), socket.in_flight.as_ref().map(|bytes| bytes.as_ptr()), "a copy");
    assert!(socket.in_flight_head.is_none(), "header and body in one buffer");
    assert_eq!(vec![buffer], h.core.batcher.bodies.pointers(), "its body back, for the next plain send");
    h.complete_write(conn);
    assert_eq!(vec![Wrote::Alone(None, 1), Wrote::Alone(None, 2)], wrote(&mut h, conn));
    assert_eq!(1, h.sockets[&conn].apart);
}

#[test]
fn a_big_batch_of_one_is_written_from_its_own_buffer_behind_its_header() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.hold_writes = true;
    h.core.set_delivery(EVENT, None, by_count(100)).expect("set");
    let big = plain(&mut h, 1, None, &vec![b'b'; BODY_KEPT_MAX]);
    let buffer = big.body.as_ptr();
    assert!(h.core.send_plain(big, TEN, 0));
    h.settle();
    assert!(h.sockets[&conn].in_flight.is_none(), "in its batch");
    h.core.begin_flush(None);
    h.settle();
    let socket = &h.sockets[&conn];
    assert_eq!(Some(buffer), socket.in_flight.as_ref().map(|bytes| bytes.as_ptr()), "its own buffer, no copy");
    assert!(socket.in_flight_head.is_some(), "its header apart");
    h.complete_write(conn);
    assert_eq!(vec![Wrote::Alone(None, 1)], wrote(&mut h, conn), "a batch of one: the message itself");
}

#[test]
fn a_batch_hands_its_bodies_back_keeps_its_envelope_and_recycles_its_frames_buffer() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.hold_writes = true;
    h.core.set_delivery(EVENT, None, by_count(3)).expect("set");
    for id in 1..=3 {
        send(&mut h, id, None, b"a longer event"); // the third fills the batch: it goes
    }
    let first_frame = h.sockets[&conn].in_flight.as_ref().expect("the batch, being written").as_ptr();
    let bodies = h.core.batcher.bodies.pointers();
    assert_eq!(3, bodies.len(), "the three bodies, back once the frame was made");
    let room = h.core.envelope_room();
    assert!(room >= 3);
    assert_eq!(0, h.core.frame_buffers_kept(), "its buffer, held by the frame being written");
    h.complete_write(conn);
    assert_eq!(vec![Wrote::Batch(None, vec![1, 2, 3])], wrote(&mut h, conn));
    assert_eq!(1, h.core.frame_buffers_kept(), "back once written and let go");
    // Shorter: the next frame fits in the buffer, whatever its own id's size.
    let mut used = Vec::new();
    for id in 4..=6 {
        let plain = plain(&mut h, id, None, b"event");
        used.push(plain.body.as_ptr());
        assert!(h.core.send_plain(plain, TEN, 0));
        h.settle();
    }
    assert_eq!(bodies, used, "the next sends serialized into the bodies handed back");
    let second_frame = h.sockets[&conn].in_flight.as_ref().expect("the next batch").as_ptr();
    assert_eq!(first_frame, second_frame, "serialized into the buffer the first frame left");
    assert_eq!(room, h.core.envelope_room(), "the one envelope, kept");
    assert_eq!(3, h.core.batcher.bodies.len());
    h.complete_write(conn);
    assert_eq!(vec![Wrote::Batch(None, vec![4, 5, 6])], wrote(&mut h, conn));
}

#[test]
fn a_batch_given_up_on_is_told_for_each_plain_send_and_followed_frame_inside() {
    // Nothing connected: the full batch is held, then given up on at its
    // deadline, told for each, the followed one's `done` hearing.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let delivery = Delivery { max_delay: Duration::from_secs(5), timeout: TEN, ..by_count(3) };
    h.core.set_delivery(EVENT, None, delivery).expect("set");
    send(&mut h, 1, None, b"plain");
    let heard = Arc::new(Mutex::new(Vec::new()));
    let hearing = Arc::clone(&heard);
    let done = SendDone::Callback(Box::new(move |written| hearing.lock().unwrap().push(written)));
    let frame = event(&h, 2, None, b"followed");
    assert!(h.core.send(frame, false, None, TEN, 0, None, None, Some(done), true));
    send(&mut h, 3, None, b"plain");
    assert_eq!(1, h.core.held_for_a_connection(), "one batch, held");
    h.advance(Duration::from_secs(10));
    assert_eq!(
        vec![(1, DropReason::NoConnection), (2, DropReason::NoConnection), (3, DropReason::NoConnection)],
        *drops.lock().unwrap()
    );
    assert_eq!(vec![0], *heard.lock().unwrap());
}

#[test]
fn a_threaded_clients_plain_send_counts_its_deadline_from_its_send() {
    // One of a type with no delivery set whose time ran out while it waited
    // for the io thread goes alone with no time to wait for a connection,
    // and none is live, so it is given up on at once; one with time left is
    // held. One of a batched key joins its batch whatever its own time, the
    // batch's own counting from its first message.
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let drops = h.observe_drops();
    h.core.set_delivery(EVENT, Some("batched"), by_count(100)).expect("set");
    let sent_at = h.core.now;
    h.advance(Duration::from_secs(10));
    let late = plain(&mut h, 1, None, b"late");
    h.core.place_plain(late, deadline_after(sent_at, TEN));
    h.settle();
    assert_eq!(vec![(1, DropReason::NoConnection)], *drops.lock().unwrap());
    let sent_at = h.core.now;
    h.advance(Duration::from_secs(9));
    let early = plain(&mut h, 2, None, b"early");
    h.core.place_plain(early, deadline_after(sent_at, TEN));
    h.settle();
    assert_eq!(1, h.core.held_for_a_connection(), "a second left: held for a connection");
    let batched = plain(&mut h, 3, Some("batched"), b"late, batched");
    h.core.place_plain(batched, deadline_after(h.start, TEN)); // its ten seconds over long ago
    h.settle();
    assert_eq!(1, h.core.batched_messages(), "its own time over: in its batch all the same");
    assert_eq!(1, drops.lock().unwrap().len());
}

#[test]
fn a_plain_send_after_the_shutdowns_end_is_reported_dropped() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let drops = h.observe_drops();
    h.core.threaded.torn_down = true;
    let late = plain(&mut h, 7, None, b"late");
    h.core.place_plain(late, None);
    h.settle();
    assert_eq!(vec![(7, DropReason::ShutDown)], *drops.lock().unwrap());
    assert_eq!(1, h.core.batcher.bodies.len(), "its body kept for the next");
    // And one the client takes no more, shutting down.
    h.core.client.shuts_down = true;
    let refused = plain(&mut h, 8, None, b"refused");
    assert!(!h.core.send_plain(refused, TEN, 0));
}

#[test]
fn a_plain_sends_key_is_its_bytes_after_the_message() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let keyed = plain(&mut h, 1, Some("shard.7"), b"payload");
    assert_eq!(Some("shard.7"), keyed.key());
    assert_eq!(keyed.length + "shard.7".len(), keyed.body.len(), "the message, then its key's bytes");
    let none = plain(&mut h, 2, None, b"payload");
    assert_eq!(None, none.key());
    let empty = plain(&mut h, 3, Some(""), b"payload");
    assert_eq!(Some(""), empty.key(), "an empty key is a key");
    let body = keyed.into_body();
    let decoded = <crate::proto::multiplexer::MultiplexerMessage as prost::Message>::decode(Bytes::from(body))
        .expect("the message alone");
    assert_eq!(Some("shard.7".to_string()), decoded.key, "the message keeps its own copy");
}
