//! The one way every client sends: round robin over the connections with
//! room, a backlog per full connection, the held messages with no
//! connection, deadlines and the drops they report, the hand-over of what a
//! dead connection had not written, pinned lanes, sends to ALL, flushes and
//! send callbacks. Mirrors outbox.cc with its parts, and the C++ and
//! Python tests outage, drop_reports, send_callbacks, threaded_sends and
//! the lanes' tests.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::core::client::{DropReason, IncomingMode};
use crate::core::connection::{ReadEnd, DEFAULT_OUT_QUEUE_SIZE};
use crate::core::harness::Harness;
use crate::core::lane::Lane;
use crate::core::outbox::{FlushDone, SendDone};
use crate::core::SendState;
use crate::proto::multiplexer::Routing;
use crate::test_log::capture;

const TEN: Option<Duration> = Some(Duration::from_secs(10));

/// Fills `conn`'s queue: one write held in flight and the rest queued, ids
/// from `first` on; the next message waits for room.
fn fill(h: &mut Harness, first: u64) {
    h.hold_writes = true;
    for id in first..first + DEFAULT_OUT_QUEUE_SIZE as u64 {
        assert!(h.send(id));
    }
}

#[test]
fn messages_go_round_robin_over_the_connections_with_room() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let one = h.up(1);
    let two = h.up(2);
    h.written(one);
    h.written(two);
    for id in 1..=4 {
        assert!(h.send(id));
    }
    let (first, second) = (h.written_ids(one), h.written_ids(two));
    assert_eq!(2, first.len());
    assert_eq!(2, second.len());
}

#[test]
fn a_message_given_no_connection_passes_over_a_full_one_and_then_goes_where_fewer_wait() {
    // The round robin had room on every connection in every test: one that
    // gave every other message to a full connection, a hung multiplexer's,
    // passed them all.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let one = h.up(1);
    let two = h.up(2);
    h.written(one);
    h.written(two);
    h.hold_writes = true;
    let lane = Lane::new(false);
    let mut ids = 1..;
    // The lane's connection full, two more waiting for its room.
    let through_lane: Vec<u64> = ids.by_ref().take(DEFAULT_OUT_QUEUE_SIZE + 2).collect();
    for id in &through_lane {
        assert!(h.send_with(*id, TEN, false, Some(&lane)).0);
    }
    let full = lane.connection().unwrap().conn.unwrap();
    let other = if full == one { two } else { one };
    // The other filled, then two more waiting there, where fewer wait.
    let given_none: Vec<u64> = ids.take(DEFAULT_OUT_QUEUE_SIZE + 2).collect();
    for id in &given_none {
        assert!(h.send(*id));
    }
    h.complete_writes();
    assert_eq!(through_lane, h.written_ids(full));
    assert_eq!(given_none, h.written_ids(other));
}

#[test]
fn a_full_connection_keeps_what_waits_for_room_in_order() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    fill(&mut h, 1);
    let over = DEFAULT_OUT_QUEUE_SIZE as u64 + 1;
    assert!(h.send(over));
    assert!(h.send(over + 1));
    h.complete_writes();
    let written = h.written_ids(conn);
    assert_eq!((1..=over + 1).collect::<Vec<u64>>(), written, "in the order sent");
    assert_eq!(2, h.core.outbox.retries(), "two moved in from the backlog");
}

#[test]
fn a_message_waits_for_room_until_its_deadline_and_is_reported_no_room() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    let drops = h.observe_drops();
    fill(&mut h, 1);
    let (taken, trackers) = h.send_with(5000, Some(Duration::from_secs(2)), false, None);
    assert!(taken);
    h.advance(Duration::from_millis(1999));
    assert!(drops.lock().unwrap().is_empty());
    let (_, lines) = capture(|| h.advance(Duration::from_millis(1)));
    assert_eq!(vec![(5000, DropReason::NoRoom)], *drops.lock().unwrap());
    assert_eq!(SendState::Lost, trackers[0].get());
    assert!(lines
        .iter()
        .any(|(_, line)| line
            == "message dropped: it waited for room on its connection past its timeout; id 5000, type 300"));
    h.complete_writes();
    assert!(!h.written_ids(conn).contains(&5000), "never written late");
}

#[test]
fn a_drop_is_told_in_the_pass_after_its_handler_ahead_of_the_sends_callback() {
    // Given up on at once, with no time and nothing connected: counted in
    // the send, told to the observer in the posted pass that runs the
    // program's callbacks, and the send's own `done` after it.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let heard = Arc::new(Mutex::new(Vec::new()));
    let hearing = Arc::clone(&heard);
    let done = SendDone::Callback(Box::new(move |written| hearing.lock().unwrap().push(written)));
    let frame = h.event(5, 300, b"event");
    assert!(h.core.send(frame, false, None, Some(Duration::ZERO), 0, None, None, Some(done), false));
    assert_eq!(1, h.core.client.dropped.load(std::sync::atomic::Ordering::Relaxed), "counted at once");
    assert!(drops.lock().unwrap().is_empty(), "not told inside the send");
    assert!(h.core.run_one_posted());
    assert_eq!(vec![(5, DropReason::NoConnection)], *drops.lock().unwrap());
    assert_eq!(vec![0], *heard.lock().unwrap(), "in the same pass, after the drop");
}

#[test]
fn a_flush_begun_after_a_drop_ends_after_it_was_told() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let frame = h.event(5, 300, b"event");
    assert!(h.core.send(frame, false, None, Some(Duration::ZERO), 0, None, None, None, false));
    h.core.begin_flush(None);
    assert_eq!(vec![(5, DropReason::NoConnection)], *drops.lock().unwrap(), "told as the flush begins");
}

#[test]
fn with_no_connection_a_message_is_held_and_dropped_at_its_deadline() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let (taken, trackers) = h.send_with(5, Some(Duration::from_secs(10)), false, None);
    assert!(taken);
    assert_eq!(SendState::Queued, trackers[0].get());
    h.advance(Duration::from_secs(10));
    assert_eq!(vec![(5, DropReason::NoConnection)], *drops.lock().unwrap());
    assert_eq!(SendState::Lost, trackers[0].get());
    assert_eq!(1, h.core.client.dropped.load(std::sync::atomic::Ordering::Relaxed));
}

#[test]
fn a_held_message_goes_out_in_order_once_a_connection_comes_up() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    for id in [5, 6, 7] {
        assert!(h.send(id));
    }
    let conn = h.up(1);
    let written = h.written_ids(conn);
    assert_eq!(vec![0, 5, 6, 7], written, "the welcome, then what was held");
}

#[test]
fn a_message_with_no_time_to_wait_goes_only_where_a_connection_takes_it_now() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    let (_, trackers) = h.send_with(5, Some(Duration::ZERO), false, None);
    assert_eq!(SendState::Lost, trackers[0].get());
    let conn = h.up(1);
    h.written(conn);
    fill(&mut h, 100);
    h.send_with(6, Some(Duration::ZERO), false, None);
    assert_eq!(vec![(5, DropReason::NoConnection), (6, DropReason::NoRoom)], *drops.lock().unwrap());
}

#[test]
fn a_dead_connection_hands_what_it_had_not_written_to_another_and_its_lane_follows() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let one = h.up(1);
    let two = h.up(2);
    h.written(one);
    h.written(two);
    h.hold_writes = true;
    let lane = Lane::new(false);
    for id in 1..=4 {
        assert!(h.send_with(id, TEN, false, Some(&lane)).0);
    }
    let first = lane.connection().unwrap().conn.unwrap();
    let other = if first == one { two } else { one };
    h.end(first, ReadEnd::Eof);
    assert!(h.send_with(5, TEN, false, Some(&lane)).0);
    h.complete_writes();
    assert!(h.written_ids(first).is_empty());
    // The frame a write still held goes after the rest, as in C++.
    assert_eq!(vec![2, 3, 4, 1, 5], h.written_ids(other));
    assert_eq!(Some(other), lane.connection().unwrap().conn, "the lane followed its messages");
}

#[test]
fn a_pinned_lanes_messages_are_lost_with_its_connection_and_it_takes_no_more() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let one = h.up(1);
    let _two = h.up(2);
    h.written(one);
    let drops = h.observe_drops();
    h.hold_writes = true;
    let lane = Lane::new(true);
    let mut trackers = Vec::new();
    for id in [1, 2] {
        let (taken, mut copies) = h.send_with(id, TEN, false, Some(&lane));
        assert!(taken);
        trackers.append(&mut copies);
    }
    let pinned = lane.connection().unwrap().conn.unwrap();
    h.end(pinned, ReadEnd::Eof);
    let mut dropped: Vec<u64> = drops.lock().unwrap().iter().map(|(id, _)| *id).collect();
    dropped.sort();
    assert_eq!(vec![1, 2], dropped);
    assert!(drops.lock().unwrap().iter().all(|(_, reason)| *reason == DropReason::ConnectionLost));
    assert!(trackers.iter().all(|tracker| tracker.get() == SendState::Lost));
    assert!(lane.closed());
    assert!(!h.send_with(3, TEN, false, Some(&lane)).0, "nothing goes through a closed pinned lane");
}

#[test]
fn a_send_to_all_reaches_every_connection_and_one_in_its_first_handshake_at_its_welcome() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let one = h.up(1);
    let two = h.connect(2);
    h.accept(two);
    h.written(one);
    h.written(two);
    let (taken, trackers) = h.send_with(5, TEN, true, None);
    assert!(taken);
    assert_eq!(2, trackers.len(), "a copy each");
    assert_eq!(vec![5], h.written_ids(one));
    // Behind the client's own welcome, which the multiplexer reads first.
    h.welcome(two, 2);
    assert_eq!(vec![5], h.written_ids(two));
}

#[test]
fn a_send_to_all_with_no_connection_is_held_once_whole() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    assert!(h.send_with(5, TEN, true, None).0);
    let one = h.connect(1);
    let two = h.connect(2);
    h.accept(one);
    h.accept(two);
    h.welcome(one, 1);
    assert_eq!(vec![0, 5], h.written_ids(one));
    h.welcome(two, 2);
    assert_eq!(vec![0, 5], h.written_ids(two), "its copy made when the first came up");
}

#[test]
fn a_copy_for_all_whose_connection_dies_is_dropped_the_others_having_theirs() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let one = h.up(1);
    let two = h.up(2);
    h.written(one);
    h.written(two);
    let drops = h.observe_drops();
    h.hold_writes = true;
    assert!(h.send_with(5, TEN, true, None).0);
    h.end(one, ReadEnd::Eof);
    assert_eq!(vec![(5, DropReason::ConnectionLost)], *drops.lock().unwrap());
    h.complete_writes();
    assert_eq!(vec![5], h.written_ids(two), "written once there, never handed over");
}

/// A flush whose end is kept, in `ends`, after the sends' ends.
fn flush_into(h: &mut Harness, ends: &Arc<Mutex<Vec<String>>>) {
    let ends = Arc::clone(ends);
    h.core
        .begin_flush(Some(FlushDone::Callback(Box::new(move |all| ends.lock().unwrap().push(format!("flush {all}"))))));
    h.settle();
}

#[test]
fn a_flush_waits_for_what_was_sent_before_it_and_after_the_sends_callbacks() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.hold_writes = true;
    let ends = Arc::new(Mutex::new(Vec::new()));
    for id in [1, 2] {
        let seen = Arc::clone(&ends);
        let done = SendDone::Callback(Box::new(move |written| seen.lock().unwrap().push(format!("{id} {written}"))));
        let frame = h.event(id, 300, b"event");
        assert!(h.core.send(frame, false, None, TEN, 0, None, None, Some(done), false));
        h.settle();
    }
    flush_into(&mut h, &ends);
    assert!(h.send(3), "sent after the flush began: not waited for");
    h.complete_write(conn);
    assert_eq!(vec!["1 1"], *ends.lock().unwrap());
    h.complete_write(conn);
    assert_eq!(vec!["1 1", "2 1", "flush true"], *ends.lock().unwrap(), "message 3 still in flight");
}

#[test]
fn a_flush_ends_not_all_written_when_one_was_given_up_on() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let (taken, _) = h.send_with(5, Some(Duration::from_secs(1)), false, None);
    assert!(taken);
    let ends = Arc::new(Mutex::new(Vec::new()));
    flush_into(&mut h, &ends);
    assert!(ends.lock().unwrap().is_empty());
    h.advance(Duration::from_secs(1));
    assert_eq!(vec!["flush false"], *ends.lock().unwrap());
}

#[test]
fn a_flush_ends_when_its_connection_goes_with_the_clients_own_frame_last() {
    // The flush followed the frame queued last on each connection, the
    // client's own PEER_CONTROL on the stalled one, behind a message whose
    // write hangs. That write fails, the message goes to the other
    // connection and is written there, and the PEER_CONTROL goes with its
    // connection, told to nobody: the flush waited for it for good. It
    // follows the last frame the program sent now.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let stalled = h.up(1);
    let other = h.up(2);
    h.written(stalled);
    h.written(other);
    h.hold_writes = true;
    assert!(h.send(1));
    assert!(h.send(2)); // one on each connection, round robin, each write held
    h.core.set_routing(Routing { any: Some(false), all: Some(false), last_resort: Some(false) }, None);
    h.settle();
    h.complete_write(other);
    h.complete_write(other); // the other's message and PEER_CONTROL written
    let ends = Arc::new(Mutex::new(Vec::new()));
    flush_into(&mut h, &ends);
    assert!(ends.lock().unwrap().is_empty(), "the message on the stalled connection is still to be written");
    h.fail_write(stalled);
    h.complete_writes();
    assert_eq!(vec!["flush true"], *ends.lock().unwrap());
}

#[test]
fn a_send_callback_hears_zero_once_its_message_is_given_up_on() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let ends = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&ends);
    let done = SendDone::Callback(Box::new(move |written| seen.lock().unwrap().push(written)));
    let frame = h.event(5, 300, b"event");
    assert!(h.core.send(frame, true, None, Some(Duration::from_secs(1)), 0, None, None, Some(done), false));
    h.settle();
    h.advance(Duration::from_secs(5));
    assert_eq!(vec![0], *ends.lock().unwrap());
}

#[test]
fn a_shutdown_drops_and_reports_what_still_waits() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let drops = h.observe_drops();
    for id in [5, 6] {
        assert!(h.send(id));
    }
    h.core.shutdown_client();
    h.settle();
    assert_eq!(vec![(5, DropReason::ShutDown), (6, DropReason::ShutDown)], *drops.lock().unwrap());
    assert!(!h.send(7), "nothing is taken after a shutdown");
}
