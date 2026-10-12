//! A client's connections: the handshake, the heartbeats and the silence
//! that ends a connection, reconnecting and resolving again, the connect's
//! deadline, a polite close, a failed write, the duplicate filter and the
//! incoming queue. Mirrors what multiplexer/io/connection.h,
//! basic_client.cc and basic_client_connect.cc keep, and the C++ tests of
//! connect_again, connect_by_name, connect_deadline, disconnect and
//! duplicate_target.

use std::net::SocketAddr;
use std::time::Duration;

use prost::Message as _;

use crate::constants::types;
use crate::core::client::IncomingMode;
use crate::core::connection::ReadEnd;
use crate::core::harness::Harness;
use crate::defaults::{CLOSE_READ_SECONDS, HEARTBIT_INTERVAL};
use crate::proto::multiplexer::{KeyFilter, WelcomeMessage};
use crate::test_log::capture;

/// One millisecond, the virtual clock's step around a deadline.
const MS: Duration = Duration::from_millis(1);

/// An address of the test's.
fn address(text: &str) -> SocketAddr {
    text.parse().expect("an address")
}

/// Whether a line of `level` contains `text`.
fn logged(lines: &[(log::Level, String)], level: log::Level, text: &str) -> bool {
    lines.iter().any(|(each, line)| *each == level && line.contains(text))
}

#[test]
fn the_welcome_goes_first_and_carries_the_routing_and_the_key_filters() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let filters = std::collections::BTreeMap::from([(300, vec!["user-.*".to_string()])]);
    h.core.set_key_filters(filters, None).unwrap();
    let conn = h.connect(1);
    assert_eq!(Some(address("10.0.0.1:1980")), h.sockets[&conn].address);
    assert!(h.written(conn).is_empty(), "nothing before the TCP connect is done");
    h.accept(conn);
    let written = h.written(conn);
    assert_eq!(vec![types::CONNECTION_WELCOME], written.iter().map(|message| message.r#type).collect::<Vec<_>>());
    let welcome = WelcomeMessage::decode(written[0].message.clone().unwrap()).unwrap();
    assert_eq!((200, h.core.instance_id), (welcome.r#type, welcome.id));
    assert!(welcome.routing.is_some());
    assert_eq!(vec![KeyFilter { r#type: 300, pattern: vec!["user-.*".to_string()] }], welcome.key_filter);
}

#[test]
fn what_is_sent_before_the_multiplexers_welcome_waits_for_it() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.connect(1);
    h.accept(conn);
    h.written(conn);
    assert!(h.send(5));
    assert!(h.written(conn).is_empty(), "held until the multiplexer's welcome registers the connection");
    h.welcome(conn, 1);
    assert_eq!(vec![300], h.written_types(conn));
    assert_eq!(1, h.core.connections_count());
}

#[test]
fn a_message_a_heartbeat_or_a_second_welcome_out_of_turn_closes_the_connection() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let first = h.connect(1);
    h.accept(first);
    let message = h.message(5, 300, b"early");
    h.deliver(first, &message);
    assert!(!h.core.conn_living(first) && h.sockets[&first].closed, "a message before the welcome");

    let second = h.connect(2);
    h.accept(second);
    h.heartbit(second);
    assert!(!h.core.conn_living(second) && h.sockets[&second].closed, "a heartbeat before the welcome");

    let third = h.up(3);
    h.welcome(third, 3);
    assert!(!h.core.conn_living(third) && h.sockets[&third].closed, "a second welcome");
    assert_eq!(3, h.core.reconnects_pending(), "each target is tried again");
}

#[test]
fn only_a_multiplexer_is_taken_and_never_the_client_itself() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.connect(1);
    h.accept(conn);
    let (_, lines) = capture(|| h.welcome_from(conn, 5, 4242));
    assert!(!h.core.conn_living(conn));
    assert!(logged(&lines, log::Level::Warn, "invalid peer type 5 announced by 4242"), "{lines:?}");

    let conn = h.connect(2);
    h.accept(conn);
    let own = h.core.instance_id;
    let (_, lines) = capture(|| h.welcome_from(conn, crate::constants::peers::MULTIPLEXER, own));
    assert!(!h.core.conn_living(conn));
    assert!(logged(&lines, log::Level::Warn, "connected to self; refused"), "{lines:?}");
}

#[test]
fn a_heartbeat_goes_out_after_three_seconds_without_a_write() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.advance(HEARTBIT_INTERVAL - MS);
    assert!(h.written(conn).is_empty());
    h.advance(MS);
    assert_eq!(vec![types::HEARTBIT], h.written_types(conn));
    // A write pushes the next one back: written at 5 s, a heartbeat at 8 s.
    h.advance(Duration::from_secs(2));
    assert!(h.send(5));
    assert_eq!(vec![300], h.written_types(conn));
    h.advance(HEARTBIT_INTERVAL - MS);
    assert!(h.written(conn).is_empty());
    h.advance(MS);
    assert_eq!(vec![types::HEARTBIT], h.written_types(conn));
    assert_eq!(Duration::from_secs(8), h.elapsed());
}

#[test]
fn a_silent_multiplexer_is_dropped_after_ninety_seconds_and_a_frame_holds_it_off() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.advance(Duration::from_secs(29));
    let message = h.message(5, 300, b"alive");
    h.deliver(conn, &message);
    h.advance(Duration::from_secs(90) - MS);
    assert!(h.core.conn_living(conn), "30 s and then 60 s from the last frame read");
    let (_, lines) = capture(|| h.advance(MS));
    assert!(!h.core.conn_living(conn) && h.sockets[&conn].closed);
    assert!(logged(&lines, log::Level::Warn, "for 90 s; closing the connection"), "{lines:?}");
    assert_eq!(Duration::from_secs(119), h.elapsed());
}

#[test]
fn a_busy_connection_arms_no_timer_per_message() {
    // Every frame read and every write ended would move the silence's wait
    // and the heartbeat on; they keep their time instead, the timers moving
    // on when they come due, so that the heap holds what it held.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    let entries = h.core.timers.entries();
    for id in 1..=10_000 {
        let message = h.message(id, 300, b"in");
        h.deliver(conn, &message);
        h.core.next_incoming_message();
        assert!(h.send(id));
        h.written(conn);
    }
    assert_eq!(entries, h.core.timers.entries());
    h.advance(HEARTBIT_INTERVAL);
    assert_eq!(vec![types::HEARTBIT], h.written_types(conn), "the heartbeat moved on to the last write");
}

#[test]
fn a_passive_client_idle_past_both_drop_intervals_keeps_its_connection() {
    // The loop runs only inside its calls: the first wait of the drop is
    // handled at the next call, and the second runs from then.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.jump(Duration::from_secs(100));
    assert!(h.core.conn_living(conn));
    assert_eq!(vec![types::HEARTBIT], h.written_types(conn), "its heartbeat is late, and goes now");
    h.heartbit(conn);
    h.advance(Duration::from_secs(60));
    assert!(h.core.conn_living(conn), "the multiplexer's answer re-armed the wait");
}

#[test]
fn a_lost_connection_is_connected_again_after_three_seconds() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.end(conn, ReadEnd::Eof);
    assert!(!h.core.conn_living(conn) && h.sockets[&conn].closed);
    assert_eq!(1, h.core.reconnects_pending());
    h.advance(Duration::from_secs(3) - MS);
    assert_eq!(1, h.sockets.len());
    h.advance(MS);
    assert_eq!(2, h.sockets.len(), "a new connection");
    assert_eq!(0, h.core.reconnects_pending());
    assert!(h.sockets.values().all(|socket| socket.address == Some(address("10.0.0.1:1980"))));
}

#[test]
fn a_name_is_resolved_at_every_attempt_and_its_addresses_tried_in_turn() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.core.async_connect("mx.example", 1980).unwrap();
    h.settle();
    assert_eq!(vec![(conn, "mx.example".to_string(), 1980)], std::mem::take(&mut h.resolves));
    let (first, second) = (address("10.0.0.7:1980"), address("[fd00::7]:1980"));
    h.core.on_resolved(conn, Ok(vec![first, second]));
    h.settle();
    assert_eq!(Some(first), h.sockets[&conn].address);
    h.refuse(conn);
    assert_eq!(Some(second), h.sockets[&conn].address);
    h.refuse(conn);
    assert!(!h.core.conn_living(conn));
    assert_eq!(1, h.core.reconnects_pending(), "every address failed: the attempt ends");
    h.advance(Duration::from_secs(3));
    let again = std::mem::take(&mut h.resolves);
    assert_eq!(1, again.len());
    assert_eq!(("mx.example".to_string(), 1980), (again[0].1.clone(), again[0].2), "resolved again");
}

#[test]
fn a_name_that_does_not_resolve_is_tried_again_as_a_refused_port_is() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.core.async_connect("mx.example", 1980).unwrap();
    h.settle();
    let (_, lines) = capture(|| {
        h.core.on_resolved(conn, Err("no such host".to_string()));
        h.settle();
    });
    assert!(logged(&lines, log::Level::Warn, "mx.example:1980 does not resolve: no such host; trying again later"));
    assert_eq!(1, h.core.reconnects_pending());
}

#[test]
fn a_connect_unanswered_for_five_seconds_goes_on_to_the_next_address() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.core.async_connect("mx.example", 1980).unwrap();
    h.settle();
    let (first, second) = (address("10.0.0.7:1980"), address("10.0.0.8:1980"));
    h.core.on_resolved(conn, Ok(vec![first, second]));
    h.settle();
    h.advance(Duration::from_secs(5) - MS);
    assert_eq!(Some(first), h.sockets[&conn].address);
    h.advance(MS);
    assert_eq!(Some(second), h.sockets[&conn].address, "the first given up at CONNECT_ATTEMPT_SECONDS");
    // A connect done in time is not cancelled: the deadline goes with it.
    h.accept(conn);
    h.advance(Duration::from_secs(10));
    assert!(h.core.conn_living(conn));
}

#[test]
fn disconnect_closes_the_connection_politely_and_connects_no_more() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    assert!(!h.core.disconnect("10.0.0.1", 1980).unwrap(), "not a target of the client");
    let conn = h.up(1);
    h.written(conn);
    assert!(h.core.disconnect("10.0.0.1", 1980).unwrap());
    h.settle();
    assert!(h.sockets[&conn].write_shut && !h.sockets[&conn].closed, "this end first, reading on");
    assert!(!h.core.conn_living(conn));
    // What arrives meanwhile is read and dropped, a message counted.
    let message = h.message(5, 300, b"too late");
    h.deliver(conn, &message);
    h.heartbit(conn);
    let (_, lines) = capture(|| h.end(conn, ReadEnd::Eof));
    assert!(h.sockets[&conn].closed);
    assert!(h.queued_ids().is_empty());
    assert_eq!(1, h.core.client.dropped_while_closing);
    assert!(logged(
        &lines,
        log::Level::Warn,
        "1 message(s) from multiplexer 1001 arrived after the connection began closing"
    ));
    h.advance(Duration::from_secs(10));
    assert_eq!(1, h.sockets.len(), "no reconnect");
    assert!(h.written(conn).is_empty(), "nothing written after the close began");
}

#[test]
fn a_polite_close_reads_on_for_close_read_seconds_at_most() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.core.disconnect("10.0.0.1", 1980).unwrap();
    h.settle();
    h.advance(CLOSE_READ_SECONDS - MS);
    assert!(!h.sockets[&conn].closed);
    h.advance(MS);
    assert!(h.sockets[&conn].closed, "the multiplexer's end never came");
}

#[test]
fn a_failed_write_reads_on_to_the_end_and_delivers_what_comes() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let conn = h.up(1);
    h.written(conn);
    h.hold_writes = true;
    assert!(h.send(5));
    let before = h.message(6, 301, b"before");
    h.deliver(conn, &before);
    h.fail_write(conn);
    assert!(!h.core.conn_living(conn) && h.sockets[&conn].write_shut && !h.sockets[&conn].closed);
    let after = h.message(7, 301, b"after");
    h.deliver(conn, &after);
    assert_eq!(vec![6, 7], h.queued_ids(), "what the multiplexer sent before its end is not lost");
    h.advance(HEARTBIT_INTERVAL - MS);
    assert!(!h.sockets[&conn].closed);
    h.advance(MS);
    assert!(h.sockets[&conn].closed);
}

#[test]
fn a_message_without_an_id_or_seen_before_is_dropped() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let one = h.up(1);
    let two = h.up(2);
    for (conn, message) in
        [(one, h.message(0, 300, b"no id")), (one, h.message(9, 300, b"first")), (two, h.message(9, 300, b"copy"))]
    {
        h.deliver(conn, &message);
    }
    assert_eq!(vec![9], h.queued_ids());
}

#[test]
fn a_full_incoming_queue_drops_and_takes_the_copy_through_another_multiplexer() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let one = h.up(1);
    let two = h.up(2);
    for id in 1..=crate::defaults::DEFAULT_INCOMING_QUEUE_MAX_SIZE as u64 {
        let message = h.message(id, 300, b"");
        h.deliver(one, &message);
    }
    let over = h.message(5000, 300, b"over");
    let (_, lines) = capture(|| h.deliver(one, &over));
    assert_eq!(crate::defaults::DEFAULT_INCOMING_QUEUE_MAX_SIZE, h.queued_ids().len());
    assert_eq!(1, h.core.client.incoming_dropped.load(std::sync::atomic::Ordering::Relaxed));
    assert!(logged(&lines, log::Level::Warn, "incoming_queue_full, dropping #5000"), "{lines:?}");
    h.core.next_incoming_message();
    let copy = h.message(5000, 300, b"copy");
    h.deliver(two, &copy);
    assert_eq!(Some(&5000), h.queued_ids().last(), "the copy is new: the dropped one was not kept as seen");
}

#[test]
fn a_multiplexer_reached_through_two_targets_is_said_once() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let one = h.up(1);
    let two = h.connect(2);
    h.accept(two);
    let (_, lines) = capture(|| h.welcome(two, 1));
    assert!(
        logged(
            &lines,
            log::Level::Warn,
            "multiplexer 1001 is reached through two targets, 10.0.0.1:1980 and 10.0.0.2:1980"
        ),
        "{lines:?}"
    );
    assert!(h.core.conn_living(one) && !h.core.conn_living(two), "the live connection keeps its id");
    h.advance(Duration::from_secs(3));
    let again = *h.sockets.keys().max().unwrap();
    h.accept(again);
    let (_, lines) = capture(|| h.welcome(again, 1));
    assert!(!logged(&lines, log::Level::Warn, "is reached through two targets"), "said once");
}
