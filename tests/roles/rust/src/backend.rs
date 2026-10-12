//! The `backend` role: a backend that serves request types, with knobs for
//! the failure scenarios, on `BaseMultiplexerServer` or, with --threaded,
//! `BaseThreadedMultiplexerServer`, one handler serving both. --serves maps
//! each request type to its reply type; --behaviour says what a request
//! gets: upper (the default), echo, drop (no reply), raise (the handler
//! fails), sleep:MS (wait, then echo); --crash-after N exits with status 3
//! after N requests, mid-run. The role leaves at SIGTERM or when its
//! --drain-file appears, draining first with --drain-seconds or
//! --drain-min-handled. Events: connected, request, unexpected, crash,
//! draining, acked, handler_exception, stopped (tests/README.md).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use mx_multiplexer::{
    BaseMultiplexerServer, BaseThreadedMultiplexerServer, Handler, HandlerError, HandlerResult, Request, ServeError,
    ServerHandle,
};

use crate::args::Args;
use crate::common::{self, Common, Failure, Outcome, STOP};
use crate::event::{emit, Event};

/// How the role is asked to leave and what it does then.
struct LeaveOptions {
    drain_seconds: f64,
    drain_file: String,
    drain_min_handled: i64,
    exit_on_exception: bool,
}

/// What a request gets, as --behaviour says.
struct Behaviour {
    serves: BTreeMap<u32, u32>,
    behaviour: String,
    crash_after: i64,
    memory_every: i64,
}

impl Behaviour {
    /// Reports the request, the `handled`th, then answers it, drops it,
    /// fails, or ends the process, as configured.
    fn handle(&self, request: &Request, handled: i64) -> HandlerResult {
        if self.memory_every > 0 && handled % self.memory_every == 0 {
            emit(&common::memory_event(handled));
        }
        let message = request.message();
        let mut reported = Event::new("request");
        reported.set("type", message.r#type).set("id", message.id).set("sender", message.sender);
        reported.set("size", message.payload.len());
        if let Some(key) = &message.key {
            reported.set("key", key.as_str());
        }
        emit(&reported);
        let Some(&reply_type) = self.serves.get(&message.r#type) else {
            emit(&Event::new("unexpected").with("type", message.r#type).with("id", message.id));
            request.no_response();
            return Ok(());
        };
        match self.behaviour.as_str() {
            "drop" => request.no_response(),
            "raise" => return Err("handler failed on purpose".into()),
            behaviour => {
                if let Some(milliseconds) = behaviour.strip_prefix("sleep:") {
                    std::thread::sleep(Duration::from_millis(milliseconds.parse().unwrap_or(0)));
                }
                let payload = match behaviour {
                    "upper" => message.payload.to_ascii_uppercase(),
                    _ => message.payload.to_vec(),
                };
                request.reply(reply_type, payload)?;
            }
        }
        if self.crash_after > 0 && handled >= self.crash_after {
            emit(&Event::new("crash").with("handled", handled));
            std::process::exit(3);
        }
        Ok(())
    }
}

/// How far the drain went, for its events.
#[derive(Default)]
struct Leaving {
    started: Option<Instant>,
    acked: bool,
}

/// The handler both servers serve.
struct Backend {
    behaviour: Behaviour,
    leave: LeaveOptions,
    handled: AtomicI64,
    leaving: Mutex<Leaving>,
}

impl Handler for Backend {
    /// Counts the request and does what --behaviour says.
    fn handle_message(&self, request: Request) -> HandlerResult {
        let handled = self.handled.fetch_add(1, Ordering::SeqCst) + 1;
        self.behaviour.handle(&request, handled)
    }

    /// Notices a request to leave, SIGTERM or the drain file: stops at
    /// once, or starts the drain when one was configured; reports once
    /// that every multiplexer has the drain routing.
    fn periodic_task(&self, server: &ServerHandle) {
        let mut leaving = self.leaving.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if server.draining() {
            if !leaving.acked && server.routing_acknowledged().unwrap_or(false) {
                leaving.acked = true;
                let ms = leaving.started.map_or(0.0, |started| started.elapsed().as_secs_f64() * 1000.0);
                emit(&Event::new("acked").with("ms", ms));
            }
            return;
        }
        let asked = STOP.load(Ordering::SeqCst)
            || (!self.leave.drain_file.is_empty() && Path::new(&self.leave.drain_file).exists());
        if !asked {
            return;
        }
        if self.leave.drain_seconds <= 0.0 && self.leave.drain_min_handled <= 0 {
            server.stop();
            return;
        }
        leaving.started = Some(Instant::now());
        server.start_draining(); // the multiplexers route it nothing new; it serves what arrives
        emit(&Event::new("draining").with("drain_seconds", self.leave.drain_seconds));
    }

    /// Keeps serving after a failure, unless --exit-on-exception.
    fn on_handler_error(&self, _error: &HandlerError) -> bool {
        !self.leave.exit_on_exception
    }

    /// The drain is over and at least --drain-min-handled requests were
    /// served.
    fn drained(&self, by_default: bool) -> bool {
        by_default && self.handled.load(Ordering::SeqCst) >= self.leave.drain_min_handled
    }
}

/// Runs the role with the arguments after its name.
pub fn run(argv: &[String]) -> Outcome {
    let args = Args::parse(
        argv,
        &[
            "serves",
            "behaviour",
            "crash-after",
            "memory-every",
            "drain-seconds",
            "drain-file",
            "drain-min-handled",
            "drain-routing",
            "key-filter",
        ],
        &["exit-on-exception", "threaded"],
    )
    .map_err(Failure::Usage)?;
    let common = Common::from(&args)?;
    let usage = Failure::Usage;
    let backend = Backend {
        behaviour: Behaviour {
            serves: common::kv_ints(args.all("serves"))?,
            behaviour: args.get("behaviour", "upper".to_string()).map_err(usage)?,
            crash_after: args.get("crash-after", 0).map_err(usage)?,
            memory_every: args.get("memory-every", 0).map_err(usage)?,
        },
        leave: LeaveOptions {
            drain_seconds: args.get("drain-seconds", 0.0).map_err(usage)?,
            drain_file: args.text("drain-file"),
            drain_min_handled: args.get("drain-min-handled", 0).map_err(usage)?,
            exit_on_exception: args.switch("exit-on-exception"),
        },
        handled: AtomicI64::new(0),
        leaving: Mutex::default(),
    };
    let drain_routing = common::parse_drain_routing(&args.text("drain-routing"))?;
    let key_filters = common::parse_key_filters(args.all("key-filter"))?;
    // A negative cap is no cap; zero is none, the drain over as it starts
    // unless --drain-min-handled holds it, as in C++.
    let drain = (backend.leave.drain_seconds >= 0.0).then(|| common::seconds(backend.leave.drain_seconds));
    let poll = Duration::from_millis(250);
    common::install_signal_handlers();
    if args.switch("threaded") {
        let mut builder =
            BaseThreadedMultiplexerServer::builder(common.peer_type, &common.mx).drain_routing(drain_routing);
        if !key_filters.is_empty() {
            builder = builder.key_filters(key_filters);
        }
        let server = builder.build(backend)?;
        server.connect()?;
        emit(&common.connected_event(server.handle().instance_id(), server.client().connections_count()?));
        let served = server.serve_forever(poll, drain);
        return Ok(report_end(served, server.handler()));
    }
    let mut builder = BaseMultiplexerServer::builder(common.peer_type, &common.mx).drain_routing(drain_routing);
    if !key_filters.is_empty() {
        builder = builder.key_filters(key_filters);
    }
    let mut server = builder.build(backend)?;
    server.connect()?;
    emit(&common.connected_event(server.instance_id(), server.handle().connections_count()?));
    let served = server.serve_forever(poll, drain);
    Ok(report_end(served, server.handler()))
}

/// The end of serve_forever() as an event: stopped (0), or
/// handler_exception for a failure it let out (4).
fn report_end(served: Result<(), ServeError>, backend: &Backend) -> i32 {
    let handled = backend.handled.load(Ordering::SeqCst);
    match served {
        Ok(()) => {
            emit(&Event::new("stopped").with("handled", handled));
            0
        }
        Err(error) => {
            let kind = match error {
                ServeError::Handler(error) => error.to_string(),
                ServeError::Client(error) => error.to_string(),
            };
            emit(&Event::new("handler_exception").with("kind", kind).with("handled", handled));
            4
        }
    }
}
