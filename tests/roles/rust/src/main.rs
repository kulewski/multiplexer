//! mxtestroles <role> [options]: the Rust test roles, the counterparts of
//! tests/roles/cc and tests/roles/py on the crate mx-multiplexer, as
//! subcommands of one binary, one module per role (backend.rs, client.rs,
//! event_backend.rs, event_client.rs; common.rs, event.rs and args.rs are
//! what they share). The harness runs it for a role a scenario gives to
//! "rust" (multiplexer/testing, Role.executable); tests/README.md is the
//! reference for the options and the events, the same in every language.
//!
//! Exit codes: the role's own, 0 by default, 3 for --crash-after, 4 for a
//! handler failure let out; 2 for a command line it cannot take; 1 for a
//! failure it does not report as an event.

mod args;
mod backend;
mod client;
mod common;
mod event;
mod event_backend;
mod event_client;

use common::Failure;

/// What `mxtestroles` without a role says.
const USAGE: &str = "usage: mxtestroles backend|client|event_backend|event_client [options] (see tests/README.md)";

fn main() {
    common::end_with_the_harness();
    common::install_logger();
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some((role, rest)) = argv.split_first() else {
        eprintln!("{USAGE}");
        std::process::exit(2);
    };
    let outcome = match role.as_str() {
        "backend" => backend::run(rest),
        "client" => client::run(rest),
        "event_backend" => event_backend::run(rest),
        "event_client" => event_client::run(rest),
        _ => Err(Failure::Usage(format!("unknown role {role}\n{USAGE}"))),
    };
    let code = match outcome {
        Ok(code) => code,
        Err(Failure::Usage(text)) => {
            eprintln!("mxtestroles {role}: {text}");
            2
        }
        Err(Failure::Fatal(text)) => {
            eprintln!("mxtestroles {role}: {text}");
            1
        }
    };
    std::process::exit(code);
}
