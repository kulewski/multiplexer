//! The query algorithm, case for case from docs/query.md's "Every way a
//! stage ends" and its table of what comes back, on the virtual clock:
//! every answer is the test's, sent at the moment it says, and the requests
//! the client wrote are counted. Mirrors multiplexer/query_stages_test.cc
//! and the stage cases of threaded_client_test.cc.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;

use crate::constants::types;
use crate::core::client::IncomingMode;
use crate::core::connection::ReadEnd;
use crate::core::harness::{multiplexer_id, Harness};
use crate::core::lane::Lane;
use crate::core::query::Outcome;
use crate::core::ConnId;
use crate::proto::multiplexer::{BackendForPacketSearch, DeliveryError, MultiplexerMessage};

/// The type of the test's requests, and of their replies.
const REQUEST: u32 = 310;
const REPLY: u32 = 311;
/// The backend that answers.
const BACKEND: u64 = 88;
/// A stage's timeout.
const STAGE: Duration = Duration::from_secs(10);
const MS: Duration = Duration::from_millis(1);

/// A query started, and what it ended with, said in words, and what its
/// on_received heard.
struct Asked {
    outcome: Arc<Mutex<Option<String>>>,
    received: Arc<Mutex<Vec<u64>>>,
}

impl Asked {
    /// How it ended, or None while it runs.
    fn outcome(&self) -> Option<String> {
        self.outcome.lock().unwrap().clone()
    }
}

/// Starts a query: a request of type REQUEST, addressed `to` when given,
/// with `key`, through `lane`, each stage given STAGE.
fn ask(h: &mut Harness, to: Option<u64>, key: Option<&str>, lane: Option<Lane>) -> Asked {
    let prototype = MultiplexerMessage {
        r#type: REQUEST,
        message: Some(Bytes::from_static(b"ask")),
        to,
        key: key.map(str::to_string),
        workflow: Some(Bytes::from_static(b"flow")),
        ..Default::default()
    };
    let outcome = Arc::new(Mutex::new(None));
    let received = Arc::new(Mutex::new(Vec::new()));
    let (ended, heard) = (Arc::clone(&outcome), Arc::clone(&received));
    h.core.start_query(
        prototype,
        Some(STAGE),
        lane,
        Box::new(move |outcome| {
            let said = match outcome {
                Outcome::Replied(reply) => format!("replied {}", reply.message.id.unwrap_or(0)),
                Outcome::TimedOut => "timed out".to_string(),
                Outcome::Failed => "failed".to_string(),
                Outcome::NotConnected => "not connected".to_string(),
                Outcome::ShutDown => "shut down".to_string(),
            };
            *ended.lock().unwrap() = Some(said);
        }),
        Some(Box::new(move |backend| heard.lock().unwrap().push(backend))),
    );
    h.settle();
    Asked { outcome, received }
}

/// What the client wrote on `conn` since the last look, but heartbeats.
fn wrote(h: &mut Harness, conn: ConnId) -> Vec<MultiplexerMessage> {
    h.written(conn).into_iter().filter(|message| message.r#type != types::HEARTBIT).collect()
}

/// A message to the client: `id`, `type`, from `sender`, answering
/// `references`.
fn answer(h: &Harness, id: u64, r#type: u32, sender: u64, references: u64) -> MultiplexerMessage {
    MultiplexerMessage {
        id: Some(id),
        sender: Some(sender),
        to: Some(h.core.instance_id),
        references: Some(references),
        r#type,
        message: Some(Bytes::from_static(b"answer")),
        ..Default::default()
    }
}

/// A DELIVERY_ERROR from multiplexer `n` for message `id`.
fn nobody(h: &Harness, answer_id: u64, n: u64, id: u64) -> MultiplexerMessage {
    let error = DeliveryError { packet_id: id, ..Default::default() };
    let mut message = answer(h, answer_id, types::DELIVERY_ERROR, multiplexer_id(n), id);
    message.message = Some(error.encode_to_vec().into());
    message
}

/// The one message of type `r#type` among `messages`.
fn one_of(messages: &[MultiplexerMessage], r#type: u32) -> MultiplexerMessage {
    let found: Vec<&MultiplexerMessage> = messages.iter().filter(|message| message.r#type == r#type).collect();
    assert_eq!(1, found.len(), "one of type {type} among {messages:?}");
    found[0].clone()
}

/// The request and the connection it went through, of the two.
fn sent_through(h: &mut Harness, one: ConnId, two: ConnId) -> (MultiplexerMessage, ConnId, ConnId) {
    let (first, second) = (wrote(h, one), wrote(h, two));
    if first.iter().any(|message| message.r#type == REQUEST) {
        (one_of(&first, REQUEST), one, two)
    } else {
        (one_of(&second, REQUEST), two, one)
    }
}

/// Two connections up, nothing written on them yet.
fn two_up(h: &mut Harness) -> (ConnId, ConnId) {
    let one = h.up(1);
    let two = h.up(2);
    h.written(one);
    h.written(two);
    (one, two)
}

#[test]
fn stage_one_a_reply_answers_the_query() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let (one, two) = two_up(&mut h);
    let asked = ask(&mut h, None, None, None);
    let (request, via, _) = sent_through(&mut h, one, two);
    assert_eq!((None, Some(Bytes::from_static(b"flow"))), (request.to, request.workflow.clone()));
    let reply = answer(&h, 500, REPLY, BACKEND, request.id.unwrap());
    h.deliver(via, &reply);
    assert_eq!(Some("replied 500".to_string()), asked.outcome());
    assert_eq!(0, h.core.queries.watched_ids());
}

#[test]
fn stage_one_a_delivery_error_searches_and_nobody_anywhere_fails_at_once() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let (one, two) = two_up(&mut h);
    let asked = ask(&mut h, None, None, None);
    let (request, via, _) = sent_through(&mut h, one, two);
    let refused = nobody(&h, 500, 1, request.id.unwrap());
    h.deliver(via, &refused);
    let searches = [
        one_of(&wrote(&mut h, one), types::BACKEND_FOR_PACKET_SEARCH),
        one_of(&wrote(&mut h, two), types::BACKEND_FOR_PACKET_SEARCH),
    ];
    assert_eq!(searches[0].id, searches[1].id, "one search, under one id, on every connection");
    let search = BackendForPacketSearch::decode(searches[0].message.clone().unwrap()).unwrap();
    assert_eq!(REQUEST, search.packet_type);
    let search_id = searches[0].id.unwrap();
    let first = nobody(&h, 501, 1, search_id);
    h.deliver(one, &first);
    assert_eq!(None, asked.outcome(), "one multiplexer said nobody");
    let second = nobody(&h, 502, 2, search_id);
    h.deliver(two, &second);
    assert_eq!(Some("failed".to_string()), asked.outcome(), "nobody, and nobody took the request");
}

#[test]
fn stage_one_running_out_of_time_searches_and_waits_for_a_late_reply() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let conn = h.up(1);
    h.written(conn);
    let asked = ask(&mut h, None, None, None);
    let request = one_of(&wrote(&mut h, conn), REQUEST);
    h.advance(STAGE - MS);
    assert!(wrote(&mut h, conn).is_empty());
    h.advance(MS);
    let search = one_of(&wrote(&mut h, conn), types::BACKEND_FOR_PACKET_SEARCH);
    let said = nobody(&h, 500, 1, search.id.unwrap());
    h.deliver(conn, &said);
    assert_eq!(None, asked.outcome(), "the request may be with a backend");
    let late = answer(&h, 501, REPLY, BACKEND, request.id.unwrap());
    h.deliver(conn, &late);
    assert_eq!(Some("replied 501".to_string()), asked.outcome());
}

#[test]
fn a_late_wait_with_no_reply_times_out_at_the_stages_end() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let conn = h.up(1);
    h.written(conn);
    let asked = ask(&mut h, None, None, None);
    h.advance(STAGE);
    let search = one_of(&wrote(&mut h, conn), types::BACKEND_FOR_PACKET_SEARCH);
    let said = nobody(&h, 500, 1, search.id.unwrap());
    h.deliver(conn, &said);
    h.advance(STAGE - MS);
    assert_eq!(None, asked.outcome());
    h.advance(MS);
    assert_eq!(Some("timed out".to_string()), asked.outcome());
}

#[test]
fn stage_one_a_lost_connection_searches_at_once_and_the_backend_found_gets_the_request_again() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let (one, two) = two_up(&mut h);
    let asked = ask(&mut h, None, None, None);
    let (request, via, other) = sent_through(&mut h, one, two);
    h.end(via, ReadEnd::Eof);
    let search = one_of(&wrote(&mut h, other), types::BACKEND_FOR_PACKET_SEARCH);
    let found = answer(&h, 500, types::PING, BACKEND, search.id.unwrap());
    h.deliver(other, &found);
    let again = one_of(&wrote(&mut h, other), REQUEST);
    assert_ne!(request.id, again.id, "a fresh id");
    assert_eq!((Some(BACKEND), Some(true)), (again.to, again.report_delivery_error));
    assert_eq!((request.message.clone(), request.workflow.clone()), (again.message.clone(), again.workflow.clone()));
    let reply = answer(&h, 501, REPLY, BACKEND, again.id.unwrap());
    h.deliver(other, &reply);
    assert_eq!(Some("replied 501".to_string()), asked.outcome());
}

#[test]
fn a_request_unwritten_when_its_connection_dies_is_followed_where_the_outbox_hands_it() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let (one, two) = two_up(&mut h);
    h.hold_writes = true;
    let lane = Lane::new(false);
    assert!(h.send_with(5, Some(STAGE), false, Some(&lane)).0, "in flight ahead of the request");
    let asked = ask(&mut h, None, None, Some(lane.clone()));
    let first = lane.connection().unwrap().conn.unwrap();
    let other = if first == one { two } else { one };
    h.end(first, ReadEnd::Eof);
    h.complete_writes();
    let written = wrote(&mut h, other);
    let request = one_of(&written, REQUEST);
    assert!(written.iter().all(|message| message.r#type != types::BACKEND_FOR_PACKET_SEARCH), "no search");
    let reply = answer(&h, 500, REPLY, BACKEND, request.id.unwrap());
    h.deliver(other, &reply);
    assert_eq!(Some("replied 500".to_string()), asked.outcome());
}

#[test]
fn with_no_connection_a_stage_waits_for_one_and_sends_once() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let asked = ask(&mut h, None, None, None);
    h.advance(Duration::from_secs(5));
    let conn = h.up(1);
    let requests: Vec<MultiplexerMessage> =
        wrote(&mut h, conn).into_iter().filter(|message| message.r#type == REQUEST).collect();
    assert_eq!(1, requests.len());
    let reply = answer(&h, 500, REPLY, BACKEND, requests[0].id.unwrap());
    h.deliver(conn, &reply);
    assert_eq!(Some("replied 500".to_string()), asked.outcome());

    let unanswered = ask(&mut h, None, None, None);
    h.end(conn, ReadEnd::Eof);
    let late = ask(&mut h, None, None, None);
    h.advance(STAGE);
    assert_eq!(Some("not connected".to_string()), late.outcome(), "no connection came within the stage");
    assert!(unanswered.outcome().is_some());
}

#[test]
fn stage_two_a_lost_searched_connection_is_no_answer() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let (one, two) = two_up(&mut h);
    let asked = ask(&mut h, None, None, None);
    let (request, via, other) = sent_through(&mut h, one, two);
    let refused = nobody(&h, 500, 1, request.id.unwrap());
    h.deliver(via, &refused);
    let search = one_of(&wrote(&mut h, via), types::BACKEND_FOR_PACKET_SEARCH);
    let said = nobody(&h, 501, 1, search.id.unwrap());
    h.deliver(via, &said);
    h.end(other, ReadEnd::Eof);
    assert_eq!(None, asked.outcome(), "its copy may have been routed: a PING may still come back");
    h.advance(STAGE);
    assert_eq!(Some("timed out".to_string()), asked.outcome());
}

#[test]
fn stage_three_a_delivery_error_fails_at_once_when_nobody_took_the_request() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let conn = h.up(1);
    h.written(conn);
    let asked = ask(&mut h, None, None, None);
    let request = one_of(&wrote(&mut h, conn), REQUEST);
    let refused = nobody(&h, 500, 1, request.id.unwrap());
    h.deliver(conn, &refused);
    let search = one_of(&wrote(&mut h, conn), types::BACKEND_FOR_PACKET_SEARCH);
    let found = answer(&h, 501, types::PING, BACKEND, search.id.unwrap());
    h.deliver(conn, &found);
    let again = one_of(&wrote(&mut h, conn), REQUEST);
    let gone = nobody(&h, 502, 1, again.id.unwrap());
    h.deliver(conn, &gone);
    assert_eq!(Some("failed".to_string()), asked.outcome());
}

#[test]
fn stage_three_a_delivery_error_waits_for_the_request_a_backend_may_have() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let conn = h.up(1);
    h.written(conn);
    let asked = ask(&mut h, None, None, None);
    let request = one_of(&wrote(&mut h, conn), REQUEST);
    h.advance(STAGE);
    let search = one_of(&wrote(&mut h, conn), types::BACKEND_FOR_PACKET_SEARCH);
    let found = answer(&h, 500, types::PING, BACKEND, search.id.unwrap());
    h.deliver(conn, &found);
    let again = one_of(&wrote(&mut h, conn), REQUEST);
    let gone = nobody(&h, 501, 1, again.id.unwrap());
    h.deliver(conn, &gone);
    assert_eq!(None, asked.outcome());
    let late = answer(&h, 502, REPLY, BACKEND, request.id.unwrap());
    h.deliver(conn, &late);
    assert_eq!(Some("replied 502".to_string()), asked.outcome());
}

#[test]
fn stage_three_a_lost_connection_waits_for_a_reply_to_either_attempt() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let (one, two) = two_up(&mut h);
    let asked = ask(&mut h, None, None, None);
    let (request, via, other) = sent_through(&mut h, one, two);
    let refused = nobody(&h, 500, 1, request.id.unwrap());
    h.deliver(via, &refused);
    let search = one_of(&wrote(&mut h, via), types::BACKEND_FOR_PACKET_SEARCH);
    wrote(&mut h, other);
    let found = answer(&h, 501, types::PING, BACKEND, search.id.unwrap());
    h.deliver(via, &found);
    let again = one_of(&wrote(&mut h, via), REQUEST);
    h.end(via, ReadEnd::Eof);
    assert!(wrote(&mut h, other).iter().all(|message| message.r#type != REQUEST), "never sent a third time");
    let reply = answer(&h, 502, REPLY, BACKEND, again.id.unwrap());
    h.deliver(other, &reply);
    assert_eq!(Some("replied 502".to_string()), asked.outcome());
}

#[test]
fn an_addressed_query_locates_its_instance_with_a_ping_and_shares_one_deadline() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let conn = h.up(1);
    h.written(conn);
    let asked = ask(&mut h, Some(BACKEND), Some("k"), None);
    let request = one_of(&wrote(&mut h, conn), REQUEST);
    assert_eq!((Some(BACKEND), Some(true)), (request.to, request.report_delivery_error));
    let refused = nobody(&h, 500, 1, request.id.unwrap());
    h.deliver(conn, &refused);
    let locate = one_of(&wrote(&mut h, conn), types::PING);
    assert_eq!((Some(BACKEND), Some(true), None), (locate.to, locate.report_delivery_error, locate.key.clone()));
    let found = answer(&h, 501, types::PING, BACKEND, locate.id.unwrap());
    h.deliver(conn, &found);
    let again = one_of(&wrote(&mut h, conn), REQUEST);
    assert_eq!(Some(BACKEND), again.to);
    let reply = answer(&h, 502, REPLY, BACKEND, again.id.unwrap());
    h.deliver(conn, &reply);
    assert_eq!(Some("replied 502".to_string()), asked.outcome());

    let silent = ask(&mut h, Some(BACKEND), None, None);
    wrote(&mut h, conn);
    h.advance(STAGE);
    assert_eq!(Some("timed out".to_string()), silent.outcome(), "one deadline: no locate after it");
    assert!(wrote(&mut h, conn).iter().all(|message| message.r#type != types::PING));
}

#[test]
fn the_search_carries_the_requests_key() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let conn = h.up(1);
    h.written(conn);
    let _asked = ask(&mut h, None, Some("tenant-7"), None);
    let request = one_of(&wrote(&mut h, conn), REQUEST);
    assert_eq!(Some("tenant-7".to_string()), request.key);
    let refused = nobody(&h, 500, 1, request.id.unwrap());
    h.deliver(conn, &refused);
    let search = one_of(&wrote(&mut h, conn), types::BACKEND_FOR_PACKET_SEARCH);
    assert_eq!(Some("tenant-7".to_string()), search.key, "only a backend that takes the key may be found");
}

#[test]
fn on_received_hears_each_backend_that_acknowledges_an_attempt() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let conn = h.up(1);
    h.written(conn);
    let asked = ask(&mut h, None, None, None);
    let request = one_of(&wrote(&mut h, conn), REQUEST);
    let acknowledged = answer(&h, 500, types::REQUEST_RECEIVED, BACKEND, request.id.unwrap());
    h.deliver(conn, &acknowledged);
    assert_eq!(vec![BACKEND], *asked.received.lock().unwrap());
    assert_eq!(None, asked.outcome(), "nothing else changes");
}

#[test]
fn a_late_reply_to_a_finished_query_is_dropped_by_a_threaded_client_and_kept_by_a_synchronous_one() {
    for (mode, kept) in [(IncomingMode::Dispatch, 0), (IncomingMode::Queue, 1)] {
        let mut h = Harness::new(200, mode);
        let conn = h.up(1);
        h.written(conn);
        let asked = ask(&mut h, None, None, None);
        let request = one_of(&wrote(&mut h, conn), REQUEST);
        let reply = answer(&h, 500, REPLY, BACKEND, request.id.unwrap());
        h.deliver(conn, &reply);
        assert_eq!(Some("replied 500".to_string()), asked.outcome());
        let late = answer(&h, 501, REPLY, BACKEND + 1, request.id.unwrap());
        h.deliver(conn, &late);
        assert_eq!(kept, h.queued_ids().len(), "{mode:?}");
    }
}

#[test]
fn a_pinned_query_ends_not_connected_when_its_lanes_connection_goes() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    let (one, two) = two_up(&mut h);
    let lane = Lane::new(true);
    let asked = ask(&mut h, None, None, Some(lane.clone()));
    let pinned = lane.connection().unwrap().conn.unwrap();
    let _ = (one, two);
    h.end(pinned, ReadEnd::Eof);
    assert_eq!(Some("not connected".to_string()), asked.outcome());
}

#[test]
fn a_shutdown_ends_every_query_in_flight() {
    let mut h = Harness::new(200, IncomingMode::Dispatch);
    h.up(1);
    let first = ask(&mut h, None, None, None);
    let second = ask(&mut h, None, None, None);
    h.core.finish_every_query();
    assert_eq!(Some("shut down".to_string()), first.outcome());
    assert_eq!(Some("shut down".to_string()), second.outcome());
}
