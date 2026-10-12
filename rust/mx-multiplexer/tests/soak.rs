//! The heap over thousands of queries, measured exactly, as
//! multiplexer/soak_test.cc measures the C++ library's: a backend on a
//! thread of its own and a client in this process, against a real
//! multiplexer, the C heap counted by glibc (mallinfo2) before and after.
//! A leak per message shows here in about a second, and so does a
//! structure that grows with the rate of messages rather than with what is
//! in flight, a timer heap that keeps cancelled timers until their
//! deadlines say, which a soak of minutes would take for a plateau. The
//! tests run one at a time: the heap is the process's.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use mx_multiplexer::{
    BaseMultiplexerServer, BaseThreadedMultiplexerServer, Handler, HandlerResult, Lane, Message, Request, SyncClient,
    ThreadedClient,
};
use support::testing_constants::{peers, types};
use support::{beside, Multiplexer};

/// How much the heap may grow over a measured run, for what settles late;
/// a leak per message would be megabytes.
const SLACK: usize = 64 * 1024;

/// How often a server's loop looks up from its wait.
const POLL: Duration = Duration::from_millis(50);

/// One test at a time: the heap is the process's.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// The C heap in use, as lib/memory.cc counts it: glibc's chunks in use
/// plus its mmapped ones.
fn heap_in_use_bytes() -> usize {
    // SAFETY: mallinfo2 reads the allocator's statistics and nothing else.
    let info = unsafe { libc::mallinfo2() };
    info.uordblks + info.hblkhd
}

/// Upper-cases what it is asked.
struct UpperCase;

impl Handler for UpperCase {
    fn handle_message(&self, request: Request) -> HandlerResult {
        request.reply(types::TEST_RESPONSE, request.payload().to_ascii_uppercase())?;
        Ok(())
    }
}

/// `count` queries one after another, each answered.
fn run_queries(client: &ThreadedClient, count: usize) {
    for _ in 0..count {
        let reply = client.query(types::TEST_REQUEST_A, "hello").wait().unwrap();
        assert_eq!(&b"HELLO"[..], &reply.message.payload[..]);
    }
}

/// The same through lanes and by address, five queries and a send a
/// round: typed on one lane, addressed on it, addressed on a lane made and
/// dropped each round, a send waited for through that lane, and addressed
/// through the connection the reply came on.
fn run_lane_queries(client: &ThreadedClient, backend: u64, rounds: usize) {
    let lane = Lane::new();
    let addressed =
        Message { r#type: types::TEST_REQUEST_A, to: Some(backend), payload: "hello".into(), ..Default::default() };
    for round in 0..rounds {
        client.query(types::TEST_REQUEST_A, "hello").lane(&lane).wait().unwrap();
        let reply = client.query_message(addressed.clone()).lane(&lane).wait().unwrap();
        assert_eq!(&b"HELLO"[..], &reply.message.payload[..]);
        let dropped = if round % 3 == 0 { Lane::pinned() } else { Lane::new() };
        let reply = client.query_message(addressed.clone()).lane(&dropped).wait().unwrap();
        client.send(types::TEST_REQUEST_A, "event").lane(&dropped).wait().unwrap();
        client.query_message(addressed.clone()).connection(&reply.connection).wait().unwrap();
    }
}

#[test]
fn thousands_of_queries_do_not_grow_the_heap() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let mx = Multiplexer::start();
    let mut server = BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()]).build(UpperCase).unwrap();
    server.connect().unwrap();
    let (handle, backend) = (server.handle(), server.instance_id());
    let serving = beside(move || server.serve_forever(POLL, None));
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();

    run_queries(&client, 2000); // warm-up: buffers, caches, the window of ids seen
    run_lane_queries(&client, backend, 200);
    // The ids of ended queries are kept a while, by design and bounded:
    // left out of both measurements.
    client.forget_finished_ids();
    let before = heap_in_use_bytes();
    run_queries(&client, 8000);
    run_lane_queries(&client, backend, 1000); // five thousand more, through lanes and by address
    client.forget_finished_ids();
    let after = heap_in_use_bytes();
    // The client, the backend and this test share the heap.
    assert!(after <= before + SLACK, "the heap grew from {before} to {after} bytes over 13000 queries");

    handle.stop();
    serving.join().unwrap().unwrap();
}

#[test]
fn thousands_of_queries_of_a_sync_client_do_not_grow_the_heap() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let mx = Multiplexer::start();
    let mut server = BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()]).build(UpperCase).unwrap();
    server.connect().unwrap();
    let handle = server.handle();
    let serving = beside(move || server.serve_forever(POLL, None));
    let mut client = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    let mut query = |count: usize| {
        for _ in 0..count {
            let reply = client.query(types::TEST_REQUEST_A, "hello").wait().unwrap();
            assert_eq!(&b"HELLO"[..], &reply.message.payload[..]);
        }
    };

    query(2000); // warm-up
    let before = heap_in_use_bytes();
    query(10000);
    let after = heap_in_use_bytes();
    assert!(after <= before + SLACK, "the heap grew from {before} to {after} bytes over 10000 queries");

    handle.stop();
    serving.join().unwrap().unwrap();
}

#[test]
fn thousands_of_requests_through_a_threaded_backend_do_not_grow_the_heap() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let mx = Multiplexer::start();
    let server = Arc::new(
        BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()])
            .workers(2)
            .build(UpperCase)
            .unwrap(),
    );
    server.connect().unwrap(); // registered before the first query, however late the thread below runs
    let serving = {
        let server = Arc::clone(&server);
        beside(move || server.serve_forever(POLL, None))
    };
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();

    run_queries(&client, 2000); // warm-up
    client.forget_finished_ids(); // kept a while by design and bounded: left out, as above
    let before = heap_in_use_bytes();
    run_queries(&client, 8000);
    let at_once: Vec<_> = (0..4)
        .map(|_| {
            let client = client.clone();
            beside(move || run_queries(&client, 1000)) // and from four threads at once, for the two workers
        })
        .collect();
    for thread in at_once {
        thread.join().unwrap();
    }
    client.forget_finished_ids();
    let after = heap_in_use_bytes();
    assert!(after <= before + SLACK, "the heap grew from {before} to {after} bytes over 12000 requests");

    server.handle().stop();
    serving.join().unwrap().unwrap();
}
