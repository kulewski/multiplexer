//! Echo backend in Rust: answers every ECHO_REQUEST with the upper-cased
//! payload, the backend of examples/echo on the crate mx-multiplexer.
//!
//! Usage: backend [host:port]     (default 127.0.0.1:1980)
//!
//! SIGTERM or SIGINT asks the backend to leave. The library installs no
//! signal handler: the signals are blocked in every thread, and a thread of
//! the program's waits for them with sigwait and starts the drain through
//! the server's handle, which any thread may call.

use std::time::Duration;

use mx_multiplexer::{direct_only_routing, BaseMultiplexerServer, Handler, HandlerResult, Request};

mod multiplexer_constants; // generated from echo.rules
use multiplexer_constants::{peers, types};

/// Upper-cases what it is asked.
struct Echo;

impl Handler for Echo {
    fn handle_message(&self, request: Request) -> HandlerResult {
        request.reply(types::ECHO_RESPONSE, request.payload().to_ascii_uppercase())?;
        Ok(())
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
        BaseMultiplexerServer::builder(peers::ECHO_BACKEND, [address]).drain_routing(routing).build(Echo)?;
    // The echo test reads "ready" and queries at once, so the line must mean
    // reachable: connect() first; serve_forever() would otherwise.
    server.connect()?;
    println!("ready");
    let handle = server.handle();
    std::thread::spawn(move || {
        wait_for(&signals);
        handle.start_draining();
    });
    // Drain for five seconds once asked, as the last resort: while another
    // backend is there the multiplexers route nothing new here, and alone
    // this one keeps serving to the end. A rolling restart costs nobody a
    // request.
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
