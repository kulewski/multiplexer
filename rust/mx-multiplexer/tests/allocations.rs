//! What a stream of sends allocates, counted on each thread by a global
//! allocator of this test binary's own, which marks every block with the
//! thread that allocated it: once warmed up, a batching `ThreadedClient`'s
//! sends of plain events allocate nothing for each send, on the sending
//! thread or on the io thread, only each batch's own, and nothing one of
//! the two allocated is freed on the other; a batching `SyncClient`'s sends
//! allocate nothing for each send; a big plain event that goes alone is
//! not copied on the io thread. Counted, not timed: every batch closes
//! by its count, its delay an hour, and a flush after each round of sends
//! bounds what is in flight. The tests run one at a time, the counters
//! being the process's.

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use mx_multiplexer::{Batching, Bytes, SyncClient, ThreadedClient};
use support::testing_constants::{peers, types};
use support::{beside, Multiplexer};

const TEN: Option<Duration> = Some(Duration::from_secs(10));

/// Sends a round: a batch's worth, then a flush.
const ROUND: usize = 1000;

/// Rounds that warm the client up: the bodies, the envelope, the frame
/// buffers and every queue reach their size.
const WARM_UP: usize = 5;

/// Rounds counted.
const ROUNDS: usize = 20;

/// The threads counted apart: the test's own, which sends; the client's io
/// thread; every other.
const SENDER: usize = 0;
const IO: usize = 1;
const OTHER: usize = 2;

/// What is counted for each thread: blocks allocated, blocks freed, of
/// those freed the ones another thread allocated, and bytes allocated.
const ALLOCATED: usize = 0;
const FREED: usize = 1;
const FOREIGN: usize = 2;
const BYTES: usize = 3;
const COUNTED: usize = 4;

static COUNTS: [AtomicU64; 3 * COUNTED] = [const { AtomicU64::new(0) }; 3 * COUNTED];

/// One test at a time: the counts are the process's.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

thread_local! {
    /// Which of the threads counted apart this one is.
    static ROLE: Cell<usize> = const { Cell::new(OTHER) };
    /// A byte whose address names this thread in the blocks it allocates.
    static MARK: u8 = const { 0 };
}

/// `amount` more of `what` for this thread.
fn count(what: usize, amount: u64) {
    let role = ROLE.with(Cell::get);
    COUNTS[role * COUNTED + what].fetch_add(amount, Ordering::Relaxed);
}

/// What names this thread in the blocks it allocates.
fn thread_mark() -> usize {
    MARK.with(|mark| mark as *const u8 as usize)
}

/// The counts so far, by thread and what.
fn counts() -> [u64; 3 * COUNTED] {
    std::array::from_fn(|index| COUNTS[index].load(Ordering::Relaxed))
}

/// The system's allocator, each block preceded by the mark of the thread
/// that allocated it, every allocation and free counted.
struct Counting;

/// The layout of a block with its mark in front, and where the block
/// starts in it: as far in as the block's alignment, 16 at least.
fn outer(layout: Layout) -> (Layout, usize) {
    let offset = layout.align().max(16);
    match Layout::from_size_align(layout.size() + offset, offset) {
        Ok(outer) => (outer, offset),
        Err(_) => std::process::abort(), // a size no program asks for
    }
}

// SAFETY: every block is one of the system allocator's, offset past its
// mark by the block's own alignment or more, so aligned as asked; dealloc
// undoes the offset of alloc with the same layout, and realloc, the
// trait's own, goes through both.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let (outer, offset) = outer(layout);
        // SAFETY: `outer` has a size of 16 at least.
        let base = unsafe { System.alloc(outer) };
        if base.is_null() {
            return base;
        }
        count(ALLOCATED, 1);
        count(BYTES, layout.size() as u64);
        // SAFETY: the block starts `offset` bytes in, within what was
        // allocated, and its mark takes the 8 bytes before it, aligned.
        unsafe {
            let block = base.add(offset);
            block.sub(8).cast::<usize>().write(thread_mark());
            block
        }
    }

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        let (outer, offset) = outer(layout);
        count(FREED, 1);
        // SAFETY: `block` came from `alloc` with this layout: its mark is
        // the 8 bytes before it, the system's block `offset` before it.
        unsafe {
            if block.sub(8).cast::<usize>().read() != thread_mark() {
                count(FOREIGN, 1);
            }
            System.dealloc(block.sub(offset), outer);
        }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Batches of a round each, closed by their count.
fn batching() -> Batching {
    Batching { max_records: ROUND, max_delay: Duration::from_secs(3600), ..Batching::default() }
}

/// A receiver of `count` events, on a thread of its own, so that nothing
/// comes back to the sender as a delivery error.
fn receiver(address: &str, count: usize) -> JoinHandle<()> {
    let mut receiver = SyncClient::builder(peers::TEST_EVENT_BACKEND).connect([address]).expect("a receiver");
    beside(move || {
        for _ in 0..count {
            receiver.receive(TEN).expect("an event");
        }
    })
}

/// What a thread did per send between `before` and `after`, `sends` sends.
fn per_send(before: &[u64; 3 * COUNTED], after: &[u64; 3 * COUNTED], role: usize, what: usize, sends: usize) -> f64 {
    (after[role * COUNTED + what] - before[role * COUNTED + what]) as f64 / sends as f64
}

/// Names the thread `client`'s callbacks run on, its io thread, IO in the
/// counts from here on.
fn name_io_thread(client: &ThreadedClient) {
    let (named, naming) = mpsc::channel();
    client
        .send(types::TEST_EVENT, "names the io thread")
        .on_end(move |_| {
            ROLE.with(|role| role.set(IO));
            named.send(()).unwrap();
        })
        .queue()
        .unwrap();
    naming.recv_timeout(Duration::from_secs(10)).expect("the callback ran");
}

#[test]
fn a_threaded_clients_plain_sends_allocate_nothing_each_and_free_nothing_across_threads() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let mx = Multiplexer::start();
    let received = receiver(mx.address(), 1 + (WARM_UP + ROUNDS) * ROUND);
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    name_io_thread(&client);
    client.set_batching(types::TEST_EVENT, None, batching()).unwrap();
    ROLE.with(|role| role.set(SENDER));
    let payload = Bytes::from_static(&[b'r'; 100]);
    let round = || {
        for _ in 0..ROUND {
            client.send(types::TEST_EVENT, payload.clone()).queue().unwrap();
        }
        assert!(client.flush_all(TEN).wait().unwrap());
    };
    for _ in 0..WARM_UP {
        round();
    }
    let before = counts();
    for _ in 0..ROUNDS {
        round();
    }
    let after = counts();
    ROLE.with(|role| role.set(OTHER));
    let report = |role, what| per_send(&before, &after, role, what, ROUNDS * ROUND);
    let sender = (report(SENDER, ALLOCATED), report(SENDER, FOREIGN));
    let io = (report(IO, ALLOCATED), report(IO, FOREIGN));
    // A batch's own allocations, a few dozen, are a few hundredths a send;
    // one a send, a frame's or a command's, would be 1.
    assert!(sender.0 < 0.01, "the sender allocated {} a send", sender.0);
    assert!(io.0 < 0.1, "the io thread allocated {} a send", io.0);
    assert!(sender.1 < 0.01, "the sender freed {} a send of the io thread's", sender.1);
    assert!(io.1 < 0.01, "the io thread freed {} a send of the sender's", io.1);
    client.shutdown(None).wait().unwrap();
    received.join().unwrap();
}

#[test]
fn a_sync_clients_plain_sends_allocate_nothing_each() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let mx = Multiplexer::start();
    let received = receiver(mx.address(), (WARM_UP + ROUNDS) * ROUND);
    let mut client = SyncClient::builder(peers::TEST_EVENT_CLIENT).connect([mx.address()]).unwrap();
    client.set_batching(types::TEST_EVENT, None, batching()).unwrap();
    ROLE.with(|role| role.set(SENDER));
    let payload = Bytes::from_static(&[b'r'; 100]);
    let mut round = || {
        for _ in 0..ROUND {
            client.send(types::TEST_EVENT, payload.clone()).queue().unwrap();
        }
        assert!(client.flush_all(TEN).unwrap());
    };
    for _ in 0..WARM_UP {
        round();
    }
    let before = counts();
    for _ in 0..ROUNDS {
        round();
    }
    let after = counts();
    ROLE.with(|role| role.set(OTHER));
    let allocated = per_send(&before, &after, SENDER, ALLOCATED, ROUNDS * ROUND);
    assert!(allocated < 0.1, "the client allocated {allocated} a send");
    drop(client);
    received.join().unwrap();
}

#[test]
fn a_big_plain_send_that_goes_alone_is_not_copied_on_the_io_thread() {
    // A body bigger than the batching keeps that goes alone is the frame's
    // own buffer, its header kept apart: the io thread allocates nothing of
    // its size, where a frame made of a copy would be one.
    const BIG: usize = 1024 * 1024;
    const SENDS: usize = 20;
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let mx = Multiplexer::start();
    let received = receiver(mx.address(), 1 + SENDS);
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    name_io_thread(&client);
    // Batched up to 64 KiB: each of these is too big for a batch.
    client.set_batching(types::TEST_EVENT, None, Batching { max_bytes: 64 * 1024, ..batching() }).unwrap();
    ROLE.with(|role| role.set(SENDER));
    let payload = Bytes::from(vec![b'r'; BIG]);
    let before = counts();
    for _ in 0..SENDS {
        client.send(types::TEST_EVENT, payload.clone()).queue().unwrap();
        assert!(client.flush_all(TEN).wait().unwrap());
    }
    let after = counts();
    ROLE.with(|role| role.set(OTHER));
    let bytes = per_send(&before, &after, IO, BYTES, SENDS);
    assert!(bytes < (BIG / 4) as f64, "the io thread allocated {bytes} bytes a send of {BIG}");
    client.shutdown(None).wait().unwrap();
    received.join().unwrap();
}
