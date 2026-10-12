//! The `event_backend` role: a backend that receives events in a loop of
//! its own, `BaseMultiplexerServer::loop_iter()`, reports each one and
//! never answers, until SIGTERM, --until N messages or --for S seconds.
//! With --drain-file it starts draining when the file appears, the
//! --drain-routing flags kept on, and keeps looping. Events: connected,
//! received, draining, acked, done (tests/README.md).

use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use mx_multiplexer::{BaseMultiplexerServer, Error, Handler, HandlerResult, Request, ServeError};

use crate::args::Args;
use crate::common::{self, Common, Failure, Outcome, STOP};
use crate::event::{emit, Event};

/// Receives events, reports each one and never answers.
#[derive(Default)]
struct EventBackend {
    received: AtomicI64,
}

impl Handler for EventBackend {
    /// Reports one received event.
    fn handle_message(&self, request: Request) -> HandlerResult {
        self.received.fetch_add(1, Ordering::SeqCst);
        let message = request.message();
        let mut received = Event::new("received");
        received.set("type", message.r#type).set("id", message.id).set("sender", message.sender);
        received.set("to", message.to.unwrap_or(0));
        common::set_payload(&mut received, &message.payload);
        if let Some(key) = &message.key {
            received.set("key", key.as_str());
        }
        emit(&received);
        request.no_response();
        Ok(())
    }
}

/// Runs the role with the arguments after its name.
pub fn run(argv: &[String]) -> Outcome {
    let args = Args::parse(argv, &["until", "for", "drain-file", "drain-routing", "key-filter"], &[])
        .map_err(Failure::Usage)?;
    let common = Common::from(&args)?;
    let until: i64 = args.get("until", 0).map_err(Failure::Usage)?;
    let duration: f64 = args.get("for", 0.0).map_err(Failure::Usage)?;
    let drain_file = args.text("drain-file");
    let mut builder = BaseMultiplexerServer::builder(common.peer_type, &common.mx)
        .drain_routing(common::parse_drain_routing(&args.text("drain-routing"))?);
    let key_filters = common::parse_key_filters(args.all("key-filter"))?;
    if !key_filters.is_empty() {
        builder = builder.key_filters(key_filters);
    }
    common::install_signal_handlers();
    let mut server = builder.build(EventBackend::default())?;
    server.connect()?; // the loop below is its own; connect() is what serve_forever() would do first
    let handle = server.handle();
    emit(&common.connected_event(server.instance_id(), handle.connections_count()?));
    let deadline = Instant::now() + common::seconds(duration);
    let mut acked = false;
    while !STOP.load(Ordering::SeqCst) {
        if until > 0 && server.handler().received.load(Ordering::SeqCst) >= until {
            break;
        }
        if duration > 0.0 && Instant::now() >= deadline {
            break;
        }
        if !drain_file.is_empty() && !handle.draining() && Path::new(&drain_file).exists() {
            handle.start_draining();
            emit(&Event::new("draining").with("drain_seconds", 0.0));
        }
        if handle.draining() && !acked && handle.routing_acknowledged()? {
            acked = true;
            emit(&Event::new("acked").with("ms", 0.0));
        }
        match server.loop_iter(Some(Duration::from_millis(250))) {
            Ok(()) | Err(ServeError::Client(Error::OperationTimedOut)) => {}
            Err(error) => return Err(Failure::Fatal(error.to_string())),
        }
    }
    emit(&Event::new("done").with("received", server.handler().received.load(Ordering::SeqCst)));
    server.close(Some(mx_multiplexer::CLOSE_FLUSH_SECONDS));
    Ok(0)
}
