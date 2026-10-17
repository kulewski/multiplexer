//! The echo example end to end, as examples/echo's echo_test.py runs its
//! binaries: a multiplexer started for each test with echo.rules, on a port
//! the system picks; the backend, read "ready" before anything asks; the
//! client asking. Then the backend asked to leave with SIGTERM: alone, the
//! last resort of its type, it answers through its drain, asked once the
//! multiplexer has logged the drain routing in effect, by a `SyncClient`
//! the test connected before the signal, and leaves after it with 0. The
//! multiplexer is `$MXCONTROL`, or `mxcontrol` on the PATH.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use mx_multiplexer::SyncClient;

mod multiplexer_constants; // generated from echo.rules
use multiplexer_constants::{peers, types};

/// A process the test started, killed when the test ends, however it ends.
struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A directory of this test's own, for the port file and the
/// multiplexer's log, removed when the test ends.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Scratch {
        let directory = std::env::temp_dir().join(format!("echo-rust-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("a scratch directory");
        Scratch(directory)
    }

    /// Whether the multiplexer has logged `text`.
    fn logged(&self, text: &str) -> bool {
        std::fs::read_to_string(self.0.join("log")).is_ok_and(|log| log.contains(text))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A multiplexer with echo.rules on a port the system picks, its address
/// once it wrote it, 15 s at most, a bound that only detects a failure,
/// and the directory it logs in.
fn multiplexer(name: &str) -> (Running, String, Scratch) {
    let scratch = Scratch::new(name);
    let port_file = scratch.0.join("port");
    let _ = std::fs::remove_file(&port_file);
    let log = std::fs::File::create(scratch.0.join("log")).expect("a log file");
    let mxcontrol = std::env::var_os("MXCONTROL").unwrap_or_else(|| "mxcontrol".into());
    let child = Command::new(mxcontrol)
        .args(["run_multiplexer", "--address", "127.0.0.1:0", "--rules"])
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("echo.rules"))
        .arg("--port-file")
        .arg(&port_file)
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("mxcontrol runs: set MXCONTROL, as test.sh does");
    let running = Running(child);
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(address) = std::fs::read_to_string(&port_file) {
            if !address.trim().is_empty() {
                return (running, address.trim().to_string(), scratch);
            }
        }
        assert!(Instant::now() < until, "the multiplexer wrote no port");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The backend, once it printed "ready".
fn backend(address: &str) -> Running {
    let mut child = Command::new(env!("CARGO_BIN_EXE_backend"))
        .arg(address)
        .stdout(Stdio::piped())
        .spawn()
        .expect("the backend runs");
    let mut line = String::new();
    BufReader::new(child.stdout.take().expect("its stdout")).read_line(&mut line).expect("a line");
    assert_eq!("ready\n", line);
    Running(child)
}

/// What the client printed, asking `text`, and whether it succeeded.
fn ask(address: &str, text: &str) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_client")).args([address, text]).output().expect("the client runs");
    (output.status.success(), String::from_utf8_lossy(&output.stdout).into_owned())
}

#[test]
fn the_client_gets_the_text_upper_cased_from_the_backend() {
    let (_mx, address, _scratch) = multiplexer("query");
    let _backend = backend(&address);
    assert_eq!((true, "HELLO MULTIPLEXER\n".to_string()), ask(&address, "hello multiplexer"));
}

#[test]
fn the_last_backend_answers_through_its_drain_and_leaves_after_it() {
    let (_mx, address, scratch) = multiplexer("drain");
    let mut backend = backend(&address);
    // Connected before the signal, so that what the drain's five seconds
    // must hold is one query, not a client process starting.
    let mut client = SyncClient::builder(peers::ECHO_CLIENT).connect([&address]).expect("a client");
    assert_eq!(1, client.connections_count().expect("its connections"), "connected before the signal");
    // SAFETY: kill(2) takes a process id and a signal and touches no memory.
    let sent = unsafe { libc::kill(backend.0.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(0, sent, "SIGTERM sent");
    // Asked once the multiplexer has the drain routing, which it logs:
    // before it, the request is routed as if no drain had begun.
    let until = Instant::now() + Duration::from_secs(15);
    while !scratch.logged("(ECHO_BACKEND) takes any=no all=no last_resort=yes") {
        assert!(Instant::now() < until, "the multiplexer never had the drain routing");
        std::thread::sleep(Duration::from_millis(20));
    }
    let reply = client.query(types::ECHO_REQUEST, "still here").timeout(Duration::from_secs(10)).wait();
    assert_eq!("STILL HERE", String::from_utf8_lossy(&reply.expect("an answer during the drain").message.payload));
    let until = Instant::now() + Duration::from_secs(30);
    let code = loop {
        if let Some(status) = backend.0.try_wait().expect("the backend's status") {
            break status.code();
        }
        assert!(Instant::now() < until, "the backend did not leave after its drain");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(Some(0), code);
}
