//! What the crate's tests against real multiplexers share: a multiplexer
//! started for a test, `mxcontrol run_multiplexer` on a port of its own with
//! the tests' rules file, stopped, frozen, thawed and restarted on the same
//! address, and killed when the test ends; and the constants of that rules
//! file. The binary is `$MXCONTROL`, or the repository's bazel-bin after a
//! `bazel build //mxcontrol`; the rules file is `$MX_TEST_RULES`, or the
//! repository's tests/testing.rules. Bazel sets both from runfiles.

#![allow(dead_code)]

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
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
        let child = Command::new(mxcontrol())
            .args(["run_multiplexer", "--address", &self.address, "--rules"])
            .arg(rules())
            .arg("--port-file")
            .arg(&port_file)
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("mxcontrol runs");
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
    fn signal(&self, signal: &str) {
        let status = Command::new("kill").args([signal, &self.pid().to_string()]).status().expect("kill runs");
        assert!(status.success());
    }

    /// Stops it with SIGTERM, as an operator would, and waits for it.
    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = Command::new("kill").args(["-CONT", &child.id().to_string()]).status();
            let _ = Command::new("kill").args(["-TERM", &child.id().to_string()]).status();
            let _ = child.wait();
        }
    }

    /// Kills it with SIGKILL: its connections reset.
    pub fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Starts it again on the address it had.
    pub fn restart(&mut self) {
        self.stop();
        self.run();
    }

    /// Freezes it with SIGSTOP: it reads and writes nothing, its sockets
    /// open.
    pub fn freeze(&self) {
        self.signal("-STOP");
    }

    /// Thaws it.
    pub fn thaw(&self) {
        self.signal("-CONT");
    }
}

impl Drop for Multiplexer {
    fn drop(&mut self) {
        self.kill();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
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
