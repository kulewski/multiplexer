//! Echo client in Rust: sends one ECHO_REQUEST and prints the answer, the
//! client of examples/echo on the crate mx-multiplexer.
//!
//! Usage: client [host:port] [text]     (default 127.0.0.1:1980 "hello multiplexer")

use std::process::ExitCode;
use std::time::Duration;

use mx_multiplexer::{Error, SyncClient};

mod multiplexer_constants; // generated from echo.rules
use multiplexer_constants::{peers, types};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let address = args.next().unwrap_or_else(|| "127.0.0.1:1980".to_string());
    let text = args.next().unwrap_or_else(|| "hello multiplexer".to_string());
    // host:port, or [address]:port for an IPv6 one; one that does not parse
    // is refused here, before anything connects.
    let mut client = match SyncClient::builder(peers::ECHO_CLIENT).connect([address]) {
        Ok(client) => client,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    // wait() sends the request through one connection and returns the
    // reply, or why there is none: OperationFailed when no backend of the
    // type is connected anywhere, OperationTimedOut when nothing answered.
    match client.query(types::ECHO_REQUEST, text).timeout(Duration::from_secs(10)).wait() {
        Ok(reply) => {
            println!("{}", String::from_utf8_lossy(&reply.message.payload));
            ExitCode::SUCCESS
        }
        Err(Error::OperationFailed) => {
            eprintln!("no ECHO_BACKEND is connected");
            ExitCode::FAILURE
        }
        Err(Error::OperationTimedOut) => {
            eprintln!("no answer within the timeout");
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
