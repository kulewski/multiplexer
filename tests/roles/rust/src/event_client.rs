//! The `event_client` role: a passive client, a `SyncClient`, that sends
//! events and never waits for answers. --send TYPE:payload (repeatable) in
//! order; --to ID addresses one peer directly; --all sends through every
//! connection; --no-flush returns before the write; --key keys every
//! message; --interval pauses between sends; --linger keeps the process
//! alive after the last one. --batch-records N batches every type sent
//! (`set_delivery()`'s batch), N messages a batch, --batch-delay S their
//! max_delay: each send then returns at once, `queue()` putting the
//! message in its type's batch, and `flush_all()` after the last lets the
//! last batch go. With --confirm every type sent is confirmed sends, in
//! those batches or one by one, --retry-after S their retry_after: each
//! send returns at once too, and the flush then waits for every
//! confirmation, `flushed` saying whether every message was confirmed.
//! --batch-timeout S is the delivery's timeout, a batch's, negative for
//! none. Events: connected, sent, error, flushed, done (tests/README.md).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mx_multiplexer::{Delivery, Result, DEFAULT_TIMEOUT};

use crate::args::Args;
use crate::common::{self, Common, Failure, Outcome};
use crate::event::{emit, Event};

/// How long a flushing send waits for its write, as the C++ role's.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// Runs the role with the arguments after its name.
pub fn run(argv: &[String]) -> Outcome {
    let valued =
        ["send", "to", "interval", "linger", "key", "batch-records", "batch-delay", "retry-after", "batch-timeout"];
    let args = Args::parse(argv, &valued, &["all", "no-flush", "confirm"]).map_err(Failure::Usage)?;
    let common = Common::from(&args)?;
    let to: u64 = args.get("to", 0).map_err(Failure::Usage)?;
    let interval = common::seconds(args.get("interval", 0.0).map_err(Failure::Usage)?);
    let linger = common::seconds(args.get("linger", 0.0).map_err(Failure::Usage)?);
    let key = args.text("key");
    let (all, no_flush) = (args.switch("all"), args.switch("no-flush"));
    let batch_records: usize = args.get("batch-records", 0).map_err(Failure::Usage)?;
    let batch_delay = common::seconds(args.get("batch-delay", 0.0).map_err(Failure::Usage)?);
    let confirm = args.switch("confirm");
    // A negative timeout is none, as in C++ and Python.
    let batch_timeout: f64 = args.get("batch-timeout", DEFAULT_TIMEOUT.as_secs_f64()).map_err(Failure::Usage)?;
    let delivered = batch_records > 0 || confirm;
    let mut client = common.connect(Default::default())?;
    emit(&common.connected_event(client.instance_id(), client.connections_count()?));
    let sends = common::typed_payloads(args.all("send"))?;
    if delivered {
        let mut delivery = Delivery {
            batch: batch_records > 0,
            confirm,
            retry_after: common::seconds(args.get("retry-after", 5.0).map_err(Failure::Usage)?),
            timeout: (batch_timeout >= 0.0).then(|| common::seconds(batch_timeout)),
            ..Delivery::default()
        };
        if delivery.batch {
            delivery.max_records = batch_records;
            delivery.max_delay = batch_delay;
        }
        let types: BTreeSet<u32> = sends.iter().map(|(message_type, _)| *message_type).collect();
        for message_type in types {
            client.set_delivery(message_type, None, delivery)?;
        }
    }
    let mut sent_count = 0;
    for (message_type, payload) in sends {
        let mut message = client.new_message(message_type, payload.into_bytes());
        if to != 0 {
            message.to = Some(to);
        }
        if !key.is_empty() {
            message.key = Some(key.clone());
        }
        let mut sent = Event::new("sent").with("type", message_type).with("id", message.id);
        let error = Event::new("error").with("type", message_type).with("id", message.id);
        let outcome: Result<()> = if all {
            match client.send_message(message).all().queue() {
                Ok(_) => client.connections_count().map(|connections| {
                    sent.set("connections", connections);
                }),
                Err(error) => Err(error),
            }
        } else if delivered {
            // Returns at once, the message in its type's batch, which goes
            // once full, or on its way alone; the flush after the last lets
            // the last go.
            client.send_message(message).queue().map(|_| ())
        } else if no_flush {
            // Queued only: where the message stands right after, as its
            // callback has heard, nothing yet while it waits.
            let heard = Arc::new(Mutex::new(None));
            let hearing = Arc::clone(&heard);
            let send = client.send_message(message).on_end(move |written| {
                *hearing.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(written);
            });
            send.queue().map(|_| {
                let heard = *heard.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                sent.set("in_queue", heard.is_none()).set("is_sent", heard == Some(true));
                sent.set("is_lost", heard == Some(false));
            })
        } else {
            // Written, through whatever connection is alive by then.
            client.send_message(message).timeout(FLUSH_TIMEOUT).wait().map(|_| {
                sent.set("in_queue", false).set("is_sent", true).set("is_lost", false);
            })
        };
        if let Err(failure) = outcome {
            emit(&error.with("kind", common::kind_of(&failure)));
            continue;
        }
        sent_count += 1;
        emit(&sent);
        std::thread::sleep(interval);
    }
    if delivered {
        emit(&Event::new("flushed").with("is_sent", client.flush_all(Some(FLUSH_TIMEOUT))?));
    }
    std::thread::sleep(linger);
    emit(&Event::new("done").with("sent", sent_count).with("connections", client.connections_count()?));
    Ok(0)
}
