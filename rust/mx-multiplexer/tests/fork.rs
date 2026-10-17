//! Clients and servers inherited across fork() are orphans in the child,
//! with real forks, as the C++ and Python fork tests check theirs: every
//! call fails with `UsedAfterFork` before any lock, while a parent thread
//! holds one too; a lane and the inbox answer without the parent's state;
//! an orphan's teardown closes the child's copies of its sockets once,
//! leaving alone what the child opened since, and lets go of the parent's
//! threads with no call on them; the parent's clients keep working; and
//! children forked while another thread makes the process's first client
//! make their own. A child reports by its exit code, under alarm(): 0 when
//! every check held, the number of the first that did not, 99 for a panic,
//! and -1 for a child a signal ended, the alarm's when a call waited for
//! good. The forks take turns, so that no other test of this binary is
//! inside a call of the library at a fork.

mod support;

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use mx_multiplexer::{
    BaseMultiplexerServer, BaseThreadedMultiplexerServer, Error, Handler, HandlerResult, Lane, Request, SyncClient,
    ThreadedClient,
};
use support::testing_constants::{peers, types};
use support::{beside, thread_id, wait_in_epoll_wait, Multiplexer};

/// The seconds a child has before its alarm ends it.
const ALARM: u32 = 10;
const TEN: Option<Duration> = Some(Duration::from_secs(10));

/// One fork at a time.
static FORKS: Mutex<()> = Mutex::new(());

/// The turn of the calling test.
fn turn() -> std::sync::MutexGuard<'static, ()> {
    FORKS.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Forks: in the child, `check` runs under alarm(), a panic of it caught,
/// and the child ends with its result, never returning into the test
/// harness; in the parent, the child's exit code, -1 when a signal ended it.
fn in_child(check: impl FnOnce() -> i32) -> i32 {
    // SAFETY: fork(2); the child runs `check` and ends with _exit(2).
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork: {}", std::io::Error::last_os_error());
    if pid == 0 {
        // SAFETY: alarm(2) arms this process's timer and nothing else.
        unsafe { libc::alarm(ALARM) };
        let code = std::panic::catch_unwind(std::panic::AssertUnwindSafe(check)).unwrap_or(99);
        // SAFETY: _exit(2) ends the child at once, with no destructor or
        // atexit handler of the parent's run.
        unsafe { libc::_exit(code) }
    }
    exit_code_of(pid)
}

/// The exit code of the child `pid`, waited for; -1 when a signal ended it.
fn exit_code_of(pid: libc::pid_t) -> i32 {
    let mut status = 0;
    // SAFETY: waitpid(2) writes the status of `pid`, a child of this
    // process not reaped yet, into `status`.
    let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
    if waited != pid || !libc::WIFEXITED(status) {
        return -1;
    }
    libc::WEXITSTATUS(status)
}

/// The number of the first of `checks` that does not hold, counted from
/// `first`; 0 when every one holds.
fn first_failed(first: i32, checks: &[bool]) -> i32 {
    checks.iter().position(|held| !held).map_or(0, |index| first + index as i32)
}

/// How many of this process's descriptors are sockets.
fn sockets() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("/proc/self/fd")
        .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
        .filter(|target| target.to_string_lossy().starts_with("socket:"))
        .count()
}

#[test]
fn inherited_clients_fail_their_calls_and_the_parent_keeps_working() {
    let _turn = turn();
    let mx = Multiplexer::start();
    let mut sync = SyncClient::builder(peers::TEST_EVENT_BACKEND).connect([mx.address()]).unwrap();
    let threaded = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    let code = in_child(|| {
        let checks = [
            sync.orphaned(),
            matches!(sync.send(types::TEST_EVENT, "x").queue(), Err(Error::UsedAfterFork)),
            matches!(sync.receive(Some(Duration::ZERO)), Err(Error::UsedAfterFork)),
            matches!(sync.connections_count(), Err(Error::UsedAfterFork)),
            threaded.orphaned(),
            matches!(threaded.send(types::TEST_EVENT, "x").queue(), Err(Error::UsedAfterFork)),
            matches!(threaded.query(types::TEST_REQUEST_A, "x").wait(), Err(Error::UsedAfterFork)),
            matches!(threaded.connections_count(), Err(Error::UsedAfterFork)),
        ];
        // The orphans' teardown: the child's copies of the sockets closed.
        sync.shutdown(None);
        drop(threaded.shutdown(None));
        first_failed(1, &checks)
    });
    assert_eq!(0, code);
    let id = sync.instance_id();
    sync.send(types::TEST_EVENT, "back").to(id).wait().unwrap();
    assert_eq!(&b"back"[..], &sync.receive(TEN).unwrap().message.payload[..], "the parent's connection goes on");
    assert_eq!(1, threaded.connections_count().unwrap());
}

/// Answers every request.
struct Answers;

impl Handler for Answers {
    fn handle_message(&self, request: Request) -> HandlerResult {
        request.reply(types::TEST_RESPONSE, "answer")?;
        Ok(())
    }
}

#[test]
fn a_plain_servers_handle_fails_while_its_serving_thread_holds_the_client() {
    // The serving thread waits for a message holding the client's lock: a
    // call through the handle in a child forked meanwhile waited for good.
    let _turn = turn();
    let mx = Multiplexer::start();
    let mut server = BaseMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()]).build(Answers).unwrap();
    server.connect().unwrap();
    let handle = server.handle();
    let serving_thread = Arc::new(AtomicI32::new(0));
    let named = Arc::clone(&serving_thread);
    let serving = beside(move || {
        named.store(thread_id(), Ordering::SeqCst);
        server.serve_forever(Duration::from_secs(1), None)
    });
    while serving_thread.load(Ordering::SeqCst) == 0 {
        std::thread::yield_now();
    }
    wait_in_epoll_wait(serving_thread.load(Ordering::SeqCst));
    let code = in_child(|| {
        let checks = [
            matches!(handle.connections_count(), Err(Error::UsedAfterFork)),
            matches!(handle.routing_acknowledged(), Err(Error::UsedAfterFork)),
            matches!(handle.send(types::TEST_EVENT, "x"), Err(Error::UsedAfterFork)),
            matches!(handle.set_key_filters(Default::default()), Err(Error::UsedAfterFork)),
        ];
        handle.start_draining(); // nothing, in a child
        first_failed(1, &checks)
    });
    handle.stop();
    serving.join().unwrap().unwrap();
    assert_eq!(0, code, "-1: a call waited for a lock a parent thread held");
}

#[test]
fn an_inherited_client_closes_the_childs_copies_once() {
    // Each orphan teardown call closed the recorded numbers again, the
    // child's own files among them by then.
    let _turn = turn();
    let mx = Multiplexer::start();
    let mut sync = SyncClient::builder(peers::TEST_CLIENT).connect([mx.address()]).unwrap();
    let threaded = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    let code = in_child(move || {
        sync.shutdown(None);
        drop(threaded.shutdown(None));
        let files: Vec<std::fs::File> = (0..64).map(|_| std::fs::File::open("/dev/null").expect("/dev/null")).collect();
        sync.shutdown(None);
        drop(threaded.shutdown(None));
        drop(sync);
        drop(threaded);
        first_failed(1, &[files.iter().all(|file| file.metadata().is_ok())])
    });
    assert_eq!(0, code, "1: a file of the child's closed under it");
}

#[test]
fn a_fresh_client_outlives_the_inherited_ones_teardown() {
    // The inherited client's handle, dropped, detached the parent's io
    // thread, whose pthread_t glibc gives the child's next thread: the
    // fresh client's io thread, which its own drop then failed to join.
    let _turn = turn();
    let mx = Multiplexer::start();
    let old = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    old.shutdown(None).wait().unwrap();
    let address = mx.address().to_string();
    let code = in_child(move || {
        let fresh = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([address]).unwrap();
        drop(old);
        let connected = fresh.connections_count().is_ok_and(|count| count == 1);
        drop(fresh);
        first_failed(1, &[connected])
    });
    assert_eq!(0, code, "99: the fresh client's io thread could not be joined");
}

/// Holds every request until the test lets it go.
struct Holds {
    holding: Mutex<mpsc::Sender<()>>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl Handler for Holds {
    fn handle_message(&self, request: Request) -> HandlerResult {
        let _ = self.holding.lock().unwrap().send(());
        let _ = self.release.lock().unwrap().recv_timeout(Duration::from_secs(10));
        request.reply(types::TEST_RESPONSE, "answer")?;
        Ok(())
    }
}

#[test]
fn an_inherited_threaded_server_fails_its_calls_and_closes_the_childs_copies() {
    // Its workers and their clones of the client are the parent's: the
    // child's copies of its sockets stayed open for the child's life, and
    // pending() read the parent's count, under a lock a worker may hold.
    let _turn = turn();
    let mx = Multiplexer::start();
    let (holding, held) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let handler = Holds { holding: Mutex::new(holding), release: Mutex::new(released) };
    let server = BaseThreadedMultiplexerServer::builder(peers::TEST_BACKEND_A, [mx.address()]).build(handler).unwrap();
    server.connect().unwrap();
    let handle = server.handle();
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    let (answered, answer) = mpsc::channel();
    client.query(types::TEST_REQUEST_A, "hold").then(move |reply| answered.send(reply.is_ok()).unwrap()).unwrap();
    held.recv_timeout(Duration::from_secs(10)).expect("a worker holds the request");
    assert_eq!(1, handle.pending());
    let code = in_child(|| {
        let before = sockets();
        let checks = [handle.pending() == 0, matches!(server.close(None), Err(Error::UsedAfterFork))];
        let after = sockets();
        first_failed(1, &[checks[0], checks[1], after < before])
    });
    release.send(()).unwrap();
    assert!(answer.recv_timeout(Duration::from_secs(10)).unwrap(), "the parent's worker answered");
    assert_eq!(0, code, "3: the child's copies of the server's sockets left open");
}

#[test]
fn an_inherited_inbox_ends_and_an_inherited_lane_holds_nothing() {
    // The parent's inbox, which no io thread fills or ends in the child,
    // was handed out there, and a lane's getters read the parent's state
    // under a lock the parent's io thread takes.
    let _turn = turn();
    let mx = Multiplexer::start();
    let client = ThreadedClient::builder(peers::TEST_ACTIVE_CLIENT).connect([mx.address()]).unwrap();
    let inbox = client.messages();
    let lane = Lane::new();
    client.send(types::TEST_EVENT, "through the lane").lane(&lane).wait().unwrap();
    assert!(lane.connected());
    let code = in_child(|| {
        first_failed(
            1,
            &[client.messages().recv(None).is_none(), lane.connection().is_none(), !lane.connected(), lane.closed()],
        )
    });
    assert_eq!(0, code, "-1: the child read the parent's inbox, which never ends there");
    drop(inbox);
}

/// The environment variable that makes the burst's own process run it.
const BURST: &str = "MX_FORK_BURST";

#[test]
fn children_forked_while_a_thread_makes_the_first_client_make_their_own() {
    // A first client registers the fork handler, inside a std Once that a
    // child forked meanwhile copied half run: its own first client waited
    // on that Once for good. Only a process that made no client yet can
    // show it, so the test runs again in a process of its own.
    if std::env::var_os(BURST).is_some() {
        burst();
        return;
    }
    let _turn = turn();
    let status = std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args(["children_forked_while_a_thread_makes_the_first_client_make_their_own", "--exact", "--nocapture"])
        .env(BURST, "1")
        .status()
        .expect("the test binary, again");
    assert!(status.success(), "children hung at their first client: {status}");
}

/// Forks a burst of children while another thread makes the process's
/// first lane, each child making one of its own; none may hang.
fn burst() {
    const CHILDREN: usize = 60;
    let go = Arc::new(AtomicBool::new(false));
    let started = Arc::clone(&go);
    let first = std::thread::spawn(move || {
        while !started.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        Lane::new()
    });
    let mut children = Vec::new();
    for index in 0..CHILDREN {
        if index == 5 {
            go.store(true, Ordering::Release);
        }
        // SAFETY: fork(2); the child makes its lane and ends with _exit(2).
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork: {}", std::io::Error::last_os_error());
        if pid == 0 {
            // SAFETY: alarm(2) arms this process's timer and nothing else.
            unsafe { libc::alarm(3) };
            let _lane = Lane::new();
            // SAFETY: _exit(2) ends the child at once.
            unsafe { libc::_exit(0) }
        }
        children.push(pid);
    }
    let _ = first.join();
    let hung = children.into_iter().filter(|pid| exit_code_of(*pid) != 0).count();
    assert_eq!(0, hung, "children that never made their first lane");
}
