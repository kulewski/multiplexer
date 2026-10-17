//! Echo backend in Rust: answers every ECHO_REQUEST with the upper-cased
//! payload, the backend of examples/echo on the crate mx-multiplexer.
//!
//! Usage: backend [host:port]     (default 127.0.0.1:1980)
//!
//! SIGTERM or SIGINT asks the backend to leave. The library installs no
//! signal handler: the signals are blocked in every thread, and a thread of
//! the program's waits for them with sigwait and starts the drain through
//! the server's handle, which any thread may call.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use mx_multiplexer::{direct_only_routing, BaseMultiplexerServer, Handler, HandlerResult, Request, ServerHandle};

mod multiplexer_constants; // generated from echo.rules
use multiplexer_constants::{peers, types};

/// Upper-cases what it is asked, and says "ready" once a multiplexer
/// welcomed the backend.
#[derive(Default)]
struct Echo {
    ready_said: AtomicBool,
}

impl Echo {
    /// "ready" on stdout, once, when `connections` are up: the echo test
    /// reads the line and queries at once, so it must mean reachable. True
    /// once said.
    fn say_ready_once_connected(&self, connections: usize) -> bool {
        if connections > 0 && !self.ready_said.swap(true, Ordering::SeqCst) {
            println!("ready");
        }
        self.ready_said.load(Ordering::SeqCst)
    }
}

impl Handler for Echo {
    fn handle_message(&self, request: Request) -> HandlerResult {
        request.reply(types::ECHO_RESPONSE, request.payload().to_ascii_uppercase())?;
        Ok(())
    }

    /// After every poll: "ready" once a multiplexer welcomed the backend,
    /// when it had none at the start.
    fn periodic_task(&self, server: &ServerHandle) {
        if !self.ready_said.load(Ordering::SeqCst) {
            self.say_ready_once_connected(server.connections_count().unwrap_or(0));
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let signals = block_leave_signals(); // before any thread starts, so that each has them blocked
    let address = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:1980".to_string());
    // The drain as the last resort of the type: alone, the backend keeps
    // serving through it; beside another, it gets nothing new.
    let mut routing = direct_only_routing();
    routing.last_resort = true;
    let mut server =
        BaseMultiplexerServer::builder(peers::ECHO_BACKEND, [address]).drain_routing(routing).build(Echo::default())?;
    // connect() waits for the handshakes, so that "ready" can come at once;
    // with no multiplexer reachable, the loop goes on trying and says it
    // once one welcomes the backend.
    let connected = server.connect()?;
    if !server.handler().say_ready_once_connected(connected) {
        eprintln!("no multiplexer reachable yet; ready once one is");
    }
    let handle = server.handle();
    std::thread::spawn(move || {
        wait_for(&signals);
        handle.start_draining();
    });
    // Drain for five seconds once asked, as the last resort: while another
    // backend is there the multiplexers route nothing new here, and alone
    // this one keeps serving to the end, so a replacement that registers
    // within those five seconds takes over without a failed request.
    server.serve_forever(Duration::from_millis(500), Some(Duration::from_secs(5)))?;
    Ok(())
}

/// Blocks SIGTERM and SIGINT in this thread, and so in every thread it
/// starts from now on, for `wait_for` to take them; returns the set.
fn block_leave_signals() -> libc::sigset_t {
    // SAFETY: the set is made empty by sigemptyset before it is used.
    unsafe {
        let mut signals: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut signals);
        libc::sigaddset(&mut signals, libc::SIGTERM);
        libc::sigaddset(&mut signals, libc::SIGINT);
        libc::pthread_sigmask(libc::SIG_BLOCK, &signals, std::ptr::null_mut());
        signals
    }
}

/// Waits until one of `signals` comes.
fn wait_for(signals: &libc::sigset_t) {
    let mut signal = 0;
    // SAFETY: sigwait reads the set and writes the signal's number.
    unsafe {
        libc::sigwait(signals, &mut signal);
    }
}
