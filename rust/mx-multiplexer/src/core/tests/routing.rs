//! The routing and the key filters a client asks for: carried by every
//! welcome, told to every registered connection with PEER_CONTROL,
//! confirmed by the PEER_STATUS that answers the last one, and acknowledged
//! once every connection confirmed and what was read before was handed
//! out. Mirrors basic_client_routing.cc's set_routing and the C++ and
//! Python acknowledged tests.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use prost::Message as _;

use crate::constants::types;
use crate::core::client::{Acknowledged, IncomingMode};
use crate::core::harness::{multiplexer_id, Harness};
use crate::error::Error;
use crate::proto::multiplexer::{MultiplexerMessage, PeerStatus, Routing};
use crate::test_log::capture;

/// The routing of a backend draining: nothing by a rule but as a last resort.
fn draining() -> Routing {
    Routing { any: Some(false), all: Some(false), last_resort: Some(true) }
}

/// A callback that counts its calls, and the count.
fn counted() -> (Acknowledged, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&count);
    (
        Box::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        }),
        count,
    )
}

#[test]
fn a_change_with_no_connection_is_acknowledged_at_once() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let (callback, count) = counted();
    h.core.set_routing(draining(), Some(callback));
    assert!(h.core.routing_acknowledged());
    h.core.run_acknowledged_callbacks();
    assert_eq!(1, count.load(Ordering::SeqCst));
}

#[test]
fn a_change_goes_to_every_connection_and_is_acknowledged_by_their_status() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let one = h.up(1);
    let two = h.up(2);
    h.written(one);
    h.written(two);
    let (callback, count) = counted();
    h.core.set_routing(draining(), Some(callback));
    h.settle();
    let controls: Vec<MultiplexerMessage> = [one, two].iter().flat_map(|conn| h.written(*conn)).collect();
    assert_eq!(vec![types::PEER_CONTROL; 2], controls.iter().map(|control| control.r#type).collect::<Vec<_>>());
    assert!(controls.iter().all(|control| Harness::routing_of(control) == draining()));
    assert!(!h.core.routing_acknowledged());
    h.deliver(one, &Harness::peer_status(&controls[0], multiplexer_id(1)));
    assert!(!h.core.routing_acknowledged() && count.load(Ordering::SeqCst) == 0);
    assert!(!h.core.take_acknowledged_wake());
    h.deliver(two, &Harness::peer_status(&controls[1], multiplexer_id(2)));
    assert!(h.core.routing_acknowledged());
    assert_eq!(1, count.load(Ordering::SeqCst), "run at once by a client that hands out what arrives");
    assert!(h.core.take_acknowledged_wake() && !h.core.take_acknowledged_wake(), "a serve loop's wake, taken once");
}

#[test]
fn a_status_answering_an_older_control_confirms_nothing() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let conn = h.up(1);
    h.written(conn);
    let (first, first_count) = counted();
    let (second, second_count) = counted();
    h.core.set_routing(draining(), Some(first));
    h.core.set_routing(Routing::default(), Some(second));
    h.settle();
    let controls = h.written(conn);
    assert_eq!(2, controls.len());
    h.deliver(conn, &Harness::peer_status(&controls[0], multiplexer_id(1)));
    assert!(!h.core.routing_acknowledged(), "the answer to the first of two changes");
    h.deliver(conn, &Harness::peer_status(&controls[1], multiplexer_id(1)));
    assert!(h.core.routing_acknowledged());
    assert_eq!((1, 1), (first_count.load(Ordering::SeqCst), second_count.load(Ordering::SeqCst)));
}

#[test]
fn a_refused_change_is_logged_and_stays_unacknowledged() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let conn = h.up(1);
    h.written(conn);
    let filters = std::collections::BTreeMap::from([(300, vec!["(".to_string()])]);
    h.core.set_key_filters(filters, None).unwrap();
    h.settle();
    let control = h.written(conn).remove(0);
    let status = PeerStatus {
        multiplexer_id: Some(multiplexer_id(1)),
        error: Some("a key pattern that does not compile".to_string()),
        ..Default::default()
    };
    let refusal = MultiplexerMessage {
        id: Some(99),
        sender: Some(multiplexer_id(1)),
        references: control.id,
        r#type: types::PEER_STATUS,
        message: Some(Bytes::from(status.encode_to_vec())),
        ..Default::default()
    };
    let (_, lines) = capture(|| h.deliver(conn, &refusal));
    assert!(lines.iter().any(|(level, line)| *level == log::Level::Error
        && line == "PEER_STATUS from 1001: a key pattern that does not compile"));
    assert!(!h.core.routing_acknowledged());
}

#[test]
fn a_change_during_the_handshake_goes_out_once_the_welcome_came() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let conn = h.connect(1);
    h.accept(conn);
    h.written(conn);
    h.core.set_routing(draining(), None);
    h.settle();
    assert!(h.written(conn).is_empty(), "the connection is not registered yet");
    assert!(!h.core.routing_acknowledged(), "a handshake in flight counts as not confirmed");
    h.welcome(conn, 1);
    let controls = h.written(conn);
    assert_eq!(vec![types::PEER_CONTROL], controls.iter().map(|control| control.r#type).collect::<Vec<_>>());
    assert!(!h.core.routing_acknowledged());
    h.deliver(conn, &Harness::peer_status(&controls[0], multiplexer_id(1)));
    assert!(h.core.routing_acknowledged());
}

/// Two messages read, then a change whose confirmation comes behind them
/// while they are still queued: the count of its callback's runs.
fn confirmed_behind_two_messages(h: &mut Harness) -> Arc<AtomicUsize> {
    let conn = h.up(1);
    h.written(conn);
    for id in [5, 6] {
        let message = h.message(id, 300, b"routed before");
        h.deliver(conn, &message);
    }
    let (callback, count) = counted();
    h.core.set_routing(draining(), Some(callback));
    h.settle();
    let control = h.written(conn).remove(0);
    h.deliver(conn, &Harness::peer_status(&control, multiplexer_id(1)));
    count
}

#[test]
fn a_receive_hands_out_what_was_read_before_the_acknowledgement_first() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let count = confirmed_behind_two_messages(&mut h);
    h.core.run_acknowledged_callbacks();
    assert!(!h.core.routing_acknowledged() && count.load(Ordering::SeqCst) == 0, "two messages read before");
    h.core.next_incoming_message();
    assert!(!h.core.routing_acknowledged());
    h.core.next_incoming_message();
    assert!(h.core.routing_acknowledged());
    h.core.run_acknowledged_callbacks();
    assert_eq!(1, count.load(Ordering::SeqCst));
}

#[test]
fn an_acknowledgement_a_receive_made_ends_the_next_wait() {
    // The receive that took the last message queued before the confirmation
    // acknowledged the change; the next one, before it waits, tells of it as
    // a confirmation does: the wake a serve loop's wait ends at.
    let mut h = Harness::new(200, IncomingMode::Queue);
    let count = confirmed_behind_two_messages(&mut h);
    assert!(!h.core.take_acknowledged_wake(), "nothing to tell while they are queued");
    h.core.next_incoming_message();
    h.core.next_incoming_message();
    h.core.run_acknowledged_callbacks();
    h.core.acknowledged_by_receive();
    assert_eq!(1, count.load(Ordering::SeqCst));
    assert!(h.core.take_acknowledged_wake(), "told of before the wait");
    h.core.acknowledged_by_receive();
    assert!(!h.core.take_acknowledged_wake(), "once");
}

#[test]
fn key_filters_the_multiplexer_would_refuse_are_refused_at_the_call() {
    let mut h = Harness::new(200, IncomingMode::Queue);
    let mut set = |filters: Vec<(u32, Vec<String>)>| h.core.set_key_filters(filters.into_iter().collect(), None);
    let refusal = |result: crate::error::Result<()>| match result {
        Err(Error::InvalidArgument(why)) => why,
        other => panic!("{other:?}"),
    };
    assert_eq!("a key filter for message type 99, a protocol type", refusal(set(vec![(99, vec![])])));
    assert_eq!(
        "a key pattern of 1025 bytes, over the limit of 1024 (MAX_KEY_PATTERN_SIZE)",
        refusal(set(vec![(100, vec!["x".repeat(1025)])]))
    );
    assert!(set(vec![(100, vec!["x".repeat(1024)])]).is_ok());
}
