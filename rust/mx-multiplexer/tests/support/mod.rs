//! What the crate's tests against real multiplexers share: a multiplexer
//! started for a test, `mxcontrol run_multiplexer` on a port of its own with
//! the tests' rules file, stopped, frozen, thawed and restarted on the same
//! address, and killed when the test ends; and the constants of that rules
//! file. The binary is `$MXCONTROL`, or the repository's bazel-bin after a
//! `bazel build //mxcontrol`; the rules file is `$MX_TEST_RULES`, or the
//! repository's tests/testing.rules. Bazel sets both from runfiles.
//!
//! As the Python harness has it: each multiplexer watches a pipe only this
//! process writes to (`MX_TEST_PARENT_FD`, lib/program.h) and ends itself
//! once the test process is gone, however it died; a stop that does not
//! end it with 0, and a multiplexer that ended on its own before the test
//! killed it or ended, fail the test, its log printed.

#![allow(dead_code)]

use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// The tests' rules file's types: the system rules and the test types.
#[allow(clippy::all)]
pub mod testing_constants;

/// The repository's root, two levels above the crate.
fn repository() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The mxcontrol binary.
pub fn mxcontrol() -> PathBuf {
    let path = std::env::var_os("MXCONTROL")
        .map(PathBuf::from)
        .unwrap_or_else(|| repository().join("bazel-bin/mxcontrol/mxcontrol"));
    assert!(path.exists(), "no mxcontrol at {}: bazel build //mxcontrol, or set MXCONTROL", path.display());
    path
}

/// The tests' rules file.
pub fn rules() -> PathBuf {
    std::env::var_os("MX_TEST_RULES").map(PathBuf::from).unwrap_or_else(|| repository().join("tests/testing.rules"))
}

/// The read end of a pipe whose write end this process holds, close on
/// exec, for its whole life: handed to a multiplexer as MX_TEST_PARENT_FD,
/// it reaches its end when this process is gone, however it died.
fn parent_pipe() -> libc::c_int {
    static READ_END: OnceLock<libc::c_int> = OnceLock::new();
    *READ_END.get_or_init(|| {
        let mut ends: [libc::c_int; 2] = [0; 2];
        // SAFETY: pipe2(2) writes two descriptors into `ends`, which
        // outlives the call.
        let made = unsafe { libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC) };
        assert_eq!(0, made, "pipe2: {}", std::io::Error::last_os_error());
        ends[0] // the write end, ends[1], is never closed: the process's to hold
    })
}

/// The last `count` lines of `text`.
fn tail(text: &str, count: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(count)..].join("\n")
}

/// A directory of this test process's own, for port files and logs.
fn scratch() -> PathBuf {
    static COUNT: AtomicU32 = AtomicU32::new(0);
    let base = std::env::var_os("TEST_TMPDIR").map(PathBuf::from).unwrap_or_else(std::env::temp_dir);
    let directory = base.join(format!("mx-rust-test-{}-{}", std::process::id(), COUNT.fetch_add(1, Ordering::Relaxed)));
    std::fs::create_dir_all(&directory).expect("a scratch directory");
    directory
}

/// A multiplexer process; see the module comment.
pub struct Multiplexer {
    child: Option<Child>,
    address: String,
    directory: PathBuf,
}

impl Multiplexer {
    /// A multiplexer on 127.0.0.1, on a port the system picks.
    pub fn start() -> Multiplexer {
        Multiplexer::start_on("127.0.0.1:0")
    }

    /// A multiplexer listening on `address`, port 0 for one the system
    /// picks; returns once it wrote its port file.
    pub fn start_on(address: &str) -> Multiplexer {
        let mut multiplexer = Multiplexer { child: None, address: address.to_string(), directory: scratch() };
        multiplexer.run();
        multiplexer
    }

    /// Starts the process on `self.address` and waits for its port file,
    /// 15 s at most: a bound that only detects a failure.
    fn run(&mut self) {
        let port_file = self.directory.join("port");
        let _ = std::fs::remove_file(&port_file);
        let log = std::fs::File::create(self.directory.join("log")).expect("a log file");
        let watched = parent_pipe();
        let mut command = Command::new(mxcontrol());
        command
            .args(["run_multiplexer", "--address", &self.address, "--rules"])
            .arg(rules())
            .arg("--port-file")
            .arg(&port_file)
            .env("MX_TEST_PARENT_FD", watched.to_string())
            .stdout(Stdio::null())
            .stderr(log);
        // SAFETY: between fork and exec the child calls fcntl(2) alone,
        // which is async-signal-safe: the pipe's read end, close on exec
        // here, stays open in the multiplexer.
        unsafe {
            command.pre_exec(move || {
                if libc::fcntl(watched, libc::F_SETFD, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command.spawn().expect("mxcontrol runs");
        self.child = Some(child);
        let until = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(text) = std::fs::read_to_string(&port_file) {
                if !text.trim().is_empty() {
                    self.address = text.trim().to_string();
                    return;
                }
            }
            assert!(Instant::now() < until, "the multiplexer reported no port: {}", self.log());
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Where it listens, `host:port`.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Its process id.
    pub fn pid(&self) -> u32 {
        self.child.as_ref().expect("running").id()
    }

    /// What it logged.
    pub fn log(&self) -> String {
        std::fs::read_to_string(self.directory.join("log")).unwrap_or_default()
    }

    /// Sends `signal` to the process.
    fn signal(&self, signal: libc::c_int) {
        send_signal(self.pid(), signal);
    }

    /// Stops it with SIGTERM, as an operator would, and waits for it: a
    /// multiplexer that does not end with 0 fails the test.
    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            send_signal(child.id(), libc::SIGCONT);
            send_signal(child.id(), libc::SIGTERM);
            let status = child.wait().expect("the multiplexer waited for");
            assert!(status.success(), "the multiplexer's stop ended it with {status}:\n{}", tail(&self.log(), 40));
        }
    }

    /// Kills it with SIGKILL: its connections reset. One that had ended on
    /// its own fails the test.
    pub fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            if let Ok(Some(status)) = child.try_wait() {
                panic!("the multiplexer ended on its own, {status}:\n{}", tail(&self.log(), 40));
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Starts it again on the address it had.
    pub fn restart(&mut self) {
        self.stop();
        self.run();
    }

    /// Starts it again on the address it had, after `stop()`: what was
    /// connected to it reconnects on its own.
    pub fn start_again(&mut self) {
        assert!(self.child.is_none(), "stopped first");
        self.run();
    }

    /// Freezes it with SIGSTOP: it reads and writes nothing, its sockets
    /// open.
    pub fn freeze(&self) {
        self.signal(libc::SIGSTOP);
    }

    /// Thaws it.
    pub fn thaw(&self) {
        self.signal(libc::SIGCONT);
    }
}

/// Sends `signal` to `pid`, a child of this process not reaped yet, with
/// kill(2) itself: a kill program is not on every system, Debian's images
/// having no procps, and a stop that could not signal waited for good.
fn send_signal(pid: u32, signal: libc::c_int) {
    let pid = libc::pid_t::try_from(pid).expect("a process id fits pid_t");
    // SAFETY: kill(2) reads and writes no memory of this process, and `pid`
    // is a child not reaped yet, so it names no other process.
    let result = unsafe { libc::kill(pid, signal) };
    assert_eq!(0, result, "kill({pid}, {signal}): {}", std::io::Error::last_os_error());
}

impl Drop for Multiplexer {
    /// Killed at the test's end; one that ended on its own meanwhile fails
    /// the test. A test that failed keeps the log, and prints its end.
    fn drop(&mut self) {
        let ended = self.child.as_mut().and_then(|child| child.try_wait().ok().flatten());
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if std::thread::panicking() {
            eprintln!(
                "the multiplexer at {} logged, in {}:\n{}",
                self.address,
                self.directory.display(),
                tail(&self.log(), 40)
            );
            return;
        }
        let log = self.log();
        let _ = std::fs::remove_dir_all(&self.directory);
        if let Some(status) = ended {
            panic!("the multiplexer ended on its own during the test, {status}:\n{}", tail(&log, 40));
        }
    }
}

/// The next `count` messages `receiver` gets, by key, each with its id and
/// payload, in the order they arrived: what a test of batches compares,
/// order being kept within a key.
pub fn arrived_by_key(
    receiver: &mut mx_multiplexer::SyncClient,
    count: usize,
) -> std::collections::BTreeMap<Option<String>, Vec<(u64, Vec<u8>)>> {
    let mut arrived: std::collections::BTreeMap<Option<String>, Vec<(u64, Vec<u8>)>> = Default::default();
    for _ in 0..count {
        let message = receiver.receive(Some(Duration::from_secs(10))).expect("a message").message;
        arrived.entry(message.key).or_default().push((message.id, message.payload.to_vec()));
    }
    arrived
}

/// Runs `body` on a thread of its own, joined at the end of the test: a
/// peer the test plays beside the client under test.
pub fn beside<T: Send + 'static>(body: impl FnOnce() -> T + Send + 'static) -> std::thread::JoinHandle<T> {
    std::thread::spawn(body)
}

/// Runs `future` to its end on this thread, parked between its wakes: the
/// whole of an executor, enough to show the client needs no runtime.
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};
    struct Unpark(std::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        std::thread::park();
    }
}

/// The calling thread's id, as /proc/self/task names it.
pub fn thread_id() -> i32 {
    // SAFETY: gettid(2) takes no argument and only returns the id.
    let id = unsafe { libc::syscall(libc::SYS_gettid) };
    i32::try_from(id).expect("a thread id fits i32")
}

/// Waits until thread `id` of this process is blocked in epoll_wait,
/// which /proc/self/task/ID/syscall shows as its first number.
pub fn wait_in_epoll_wait(id: i32) {
    let waits = [
        #[cfg(target_arch = "x86_64")]
        libc::SYS_epoll_wait,
        libc::SYS_epoll_pwait,
        libc::SYS_epoll_pwait2,
    ];
    let until = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < until {
        let line = std::fs::read_to_string(format!("/proc/self/task/{id}/syscall")).unwrap_or_default();
        let number = line.split_whitespace().next().and_then(|first| first.parse::<libc::c_long>().ok());
        if number.is_some_and(|number| waits.contains(&number)) {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("thread {id} never waited in epoll_wait");
}
