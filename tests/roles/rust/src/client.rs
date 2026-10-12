//! The `client` role: requests and their answers, in four modes, all
//! producing the same connected, response, error and done events:
//! - by default a `SyncClient` per worker (--parallel N), one query at a
//!   time;
//! - --threaded, a `ThreadedClient` per worker, one query at a time;
//! - --threaded --async N, up to N queries in flight through `then()`;
//! - --workers N, N threads sharing one `ThreadedClient`, each with its own
//!   reply queue, the pattern of examples/echo/workers.py.
//!
//! --query TYPE:payload (repeatable) is the list each worker sends --count
//! times; {worker}, {round} and {i} in a payload are filled in. --timeout,
//! --payload-size, --sleep-before and --sleep-between shape the run, --key
//! keys every request. See tests/README.md.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use mx_multiplexer::{Incoming, Result, ThreadedClient};

use crate::args::Args;
use crate::common::{self, Common, Failure, Outcome};
use crate::event::{emit, Event};

/// The role's options.
struct Options {
    common: Common,
    queries: Vec<(u32, String)>,
    count: i64,
    parallel: i64,
    timeout: Duration,
    payload_size: usize,
    sleep_before: Duration,
    sleep_between: Duration,
    threaded: bool,
    in_flight: i64,
    workers: i64,
    memory_every: i64,
    key: String,
    /// Queries answered so far, across workers, for --memory-every.
    answered: AtomicI64,
}

/// One query of a worker's list, its payload filled in.
#[derive(Debug, Clone)]
struct Query {
    round: i64,
    index: i64,
    r#type: u32,
    payload: Vec<u8>,
}

/// A query's end, as a callback hands it to its worker: the query, when it
/// was sent, and how it ended.
type Ended = (Query, Instant, Result<Incoming>);

impl Options {
    /// The worker's whole list: --count rounds of --query, payloads filled
    /// in.
    fn queries_for(&self, worker: i64) -> Vec<Query> {
        let mut list = Vec::new();
        for round in 0..self.count {
            for (index, (message_type, template)) in self.queries.iter().enumerate() {
                let template = if self.payload_size > 0 { "x".repeat(self.payload_size) } else { template.clone() };
                let payload = template
                    .replace("{worker}", &worker.to_string())
                    .replace("{round}", &round.to_string())
                    .replace("{i}", &index.to_string());
                list.push(Query { round, index: index as i64, r#type: *message_type, payload: payload.into_bytes() });
            }
        }
        list
    }

    /// One more query answered: a memory event every --memory-every, a
    /// `ThreadedClient` forgetting its finished queries' ids first, which
    /// it keeps a while by design, bounded (docs/semantics.md).
    fn count_answered(&self, client: Option<&ThreadedClient>) {
        if self.memory_every <= 0 {
            return;
        }
        let answered = self.answered.fetch_add(1, Ordering::SeqCst) + 1;
        if answered % self.memory_every == 0 {
            if let Some(client) = client {
                client.forget_finished_ids();
            }
            emit(&common::memory_event(answered));
        }
    }

    /// Sends `query` on a `ThreadedClient`; `done` hears its end on the io
    /// thread, or at once, here, for a client already shut down. A refusal
    /// at the call comes back here, and `done` is not called.
    fn query_then(
        &self,
        client: &ThreadedClient,
        query: &Query,
        done: impl FnOnce(Result<Incoming>) + Send + 'static,
    ) -> Result<()> {
        let mut built = client.query(query.r#type, query.payload.clone()).timeout(self.timeout);
        if !self.key.is_empty() {
            built = built.key(self.key.as_str());
        }
        built.then(done)
    }
}

/// Locks `mutex`, which nothing panics under.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The fields every response or error event carries.
fn query_event(name: &str, worker: i64, query: &Query) -> Event {
    Event::new(name)
        .with("worker", worker)
        .with("round", query.round)
        .with("index", query.index)
        .with("query_type", query.r#type)
}

/// Reports a query's end, `started` when it was sent: a response for a
/// reply, an error, named, for a failure. True for a response.
fn report(worker: i64, query: &Query, started: Instant, result: &Result<Incoming>) -> bool {
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    match result {
        Ok(reply) => {
            let message = &reply.message;
            let mut response = query_event("response", worker, query);
            response.set("type", message.r#type).set("sender", message.sender).set("references", message.references);
            response.set("ms", ms);
            common::set_payload(&mut response, &message.payload);
            emit(&response);
            true
        }
        Err(error) => {
            emit(&query_event("error", worker, query).with("kind", common::kind_of(error)).with("ms", ms));
            false
        }
    }
}

/// How many queries of a worker got a reply, and how many failed.
#[derive(Debug, Default)]
struct Tally {
    responses: i64,
    errors: i64,
}

impl Tally {
    /// Counts one end, a response or not.
    fn add(&mut self, replied: bool) {
        if replied {
            self.responses += 1;
        } else {
            self.errors += 1;
        }
    }

    /// The event that closes a worker's run.
    fn report_done(&self, worker: i64, connections: usize, instance_id: u64) {
        emit(
            &Event::new("done")
                .with("worker", worker)
                .with("responses", self.responses)
                .with("errors", self.errors)
                .with("connections", connections)
                .with("instance_id", instance_id),
        );
    }
}

/// One worker of --parallel on a `SyncClient` of its own, one query at a
/// time.
fn run_worker(options: &Options, worker: i64) -> std::result::Result<(), Failure> {
    let mut client = options.common.connect(Default::default())?;
    let connected = options.common.connected_event(client.instance_id(), client.connections_count()?);
    emit(&connected.with("worker", worker).with("threaded", false));
    std::thread::sleep(options.sleep_before);
    let mut tally = Tally::default();
    for (position, query) in options.queries_for(worker).iter().enumerate() {
        if position > 0 {
            std::thread::sleep(options.sleep_between);
        }
        let started = Instant::now();
        let mut built = client.query(query.r#type, query.payload.clone()).timeout(options.timeout);
        if !options.key.is_empty() {
            built = built.key(options.key.as_str());
        }
        let result = built.wait();
        tally.add(report(worker, query, started, &result));
        options.count_answered(None);
    }
    tally.report_done(worker, client.connections_count()?, client.instance_id());
    Ok(())
}

/// A `ThreadedClient` connected to every --mx.
fn connect_threaded(options: &Options) -> std::result::Result<ThreadedClient, Failure> {
    Ok(ThreadedClient::builder(options.common.peer_type).connect(&options.common.mx)?)
}

/// One worker of --parallel on a `ThreadedClient` of its own: one query at
/// a time with `wait()`, or up to --async N in flight through callbacks,
/// which report on the io thread.
fn run_worker_threaded(options: &Arc<Options>, worker: i64) -> std::result::Result<(), Failure> {
    let client = connect_threaded(options)?;
    let connected = options.common.connected_event(client.instance_id(), client.connections_count()?);
    emit(&connected.with("worker", worker).with("threaded", true));
    std::thread::sleep(options.sleep_before);
    // The tally and the queries in flight, which the callbacks share.
    let shared = Arc::new((Mutex::new((Tally::default(), 0i64)), Condvar::new()));
    for (position, query) in options.queries_for(worker).into_iter().enumerate() {
        if position > 0 {
            std::thread::sleep(options.sleep_between);
        }
        let started = Instant::now();
        if options.in_flight <= 1 {
            let mut built = client.query(query.r#type, query.payload.clone()).timeout(options.timeout);
            if !options.key.is_empty() {
                built = built.key(options.key.as_str());
            }
            let result = built.wait();
            lock(&shared.0).0.add(report(worker, &query, started, &result));
            options.count_answered(Some(&client));
            continue;
        }
        {
            let mut state = lock(&shared.0);
            while state.1 >= options.in_flight {
                state = shared.1.wait(state).unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            state.1 += 1;
        }
        let (reported, done_shared, done_options, done_client) =
            (query.clone(), Arc::clone(&shared), Arc::clone(options), client.clone());
        let done = move |result: Result<Incoming>| {
            // On the io thread: report, then let the worker issue the next.
            let replied = report(worker, &reported, started, &result);
            done_options.count_answered(Some(&done_client));
            let mut state = lock(&done_shared.0);
            state.0.add(replied);
            state.1 -= 1;
            done_shared.1.notify_all();
        };
        if let Err(error) = options.query_then(&client, &query, done) {
            let replied = report(worker, &query, started, &Err(error));
            let mut state = lock(&shared.0);
            state.0.add(replied);
            state.1 -= 1;
        }
    }
    let tally = {
        let mut state = lock(&shared.0);
        while state.1 > 0 {
            state = shared.1.wait(state).unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        std::mem::take(&mut state.0)
    };
    tally.report_done(worker, client.connections_count()?, client.instance_id());
    Ok(())
}

/// --workers N: N threads share one `ThreadedClient`. Each issues its
/// queries through callbacks, does its own work meanwhile (--sleep-between),
/// and takes the replies from its own queue, which the io thread fills.
fn run_workers(options: &Arc<Options>) -> std::result::Result<(), Failure> {
    let client = connect_threaded(options)?;
    let connected = options.common.connected_event(client.instance_id(), client.connections_count()?);
    emit(&connected.with("threaded", true).with("workers", options.workers));
    let client = &client;
    let outcomes: Vec<std::result::Result<(), Failure>> = std::thread::scope(|scope| {
        let threads: Vec<_> = (0..options.workers)
            .map(|worker| scope.spawn(move || run_shared_worker(options, client, worker)))
            .collect();
        threads.into_iter().map(|thread| thread.join().unwrap_or_else(|_| Err(panicked()))).collect()
    });
    outcomes.into_iter().collect()
}

/// One thread of --workers: issue, work, take what arrived, repeat; then
/// wait for the rest.
fn run_shared_worker(options: &Arc<Options>, client: &ThreadedClient, worker: i64) -> std::result::Result<(), Failure> {
    let (sender, inbox) = mpsc::channel::<Ended>();
    let mut tally = Tally::default();
    let mut pending = 0usize;
    let take = |tally: &mut Tally, (query, started, result): Ended| {
        tally.add(report(worker, &query, started, &result));
        options.count_answered(Some(client));
    };
    for query in options.queries_for(worker) {
        let started = Instant::now();
        let (sent, ended) = (sender.clone(), query.clone());
        let done = move |result: Result<Incoming>| {
            let _ = sent.send((ended, started, result));
        };
        match options.query_then(client, &query, done) {
            Ok(()) => pending += 1,
            Err(error) => take(&mut tally, (query, started, Err(error))),
        }
        std::thread::sleep(options.sleep_between); // the worker's own work
        while let Ok(ended) = inbox.try_recv() {
            pending -= 1;
            take(&mut tally, ended);
        }
    }
    while pending > 0 {
        let Ok(ended) = inbox.recv() else { break };
        pending -= 1;
        take(&mut tally, ended);
    }
    tally.report_done(worker, client.connections_count()?, client.instance_id());
    Ok(())
}

/// The failure of a worker thread that panicked.
fn panicked() -> Failure {
    Failure::Fatal("a worker panicked".to_string())
}

/// Runs the role with the arguments after its name.
pub fn run(argv: &[String]) -> Outcome {
    let args = Args::parse(
        argv,
        &[
            "query",
            "count",
            "parallel",
            "timeout",
            "payload-size",
            "sleep-before",
            "sleep-between",
            "async",
            "workers",
            "memory-every",
            "key",
        ],
        &["threaded"],
    )
    .map_err(Failure::Usage)?;
    let usage = Failure::Usage;
    let options = Arc::new(Options {
        common: Common::from(&args)?,
        queries: common::typed_payloads(args.all("query"))?,
        count: args.get("count", 1).map_err(usage)?,
        parallel: args.get("parallel", 1).map_err(usage)?,
        timeout: common::seconds(args.get("timeout", 10.0).map_err(usage)?),
        payload_size: args.get("payload-size", 0).map_err(usage)?,
        sleep_before: common::seconds(args.get("sleep-before", 0.0).map_err(usage)?),
        sleep_between: common::seconds(args.get("sleep-between", 0.0).map_err(usage)?),
        threaded: args.switch("threaded"),
        in_flight: args.get("async", 0).map_err(usage)?,
        workers: args.get("workers", 0).map_err(usage)?,
        memory_every: args.get("memory-every", 0).map_err(usage)?,
        key: args.text("key"),
        answered: AtomicI64::new(0),
    });
    if options.workers > 0 {
        run_workers(&options)?;
        return Ok(0);
    }
    let outcomes: Vec<std::result::Result<(), Failure>> = std::thread::scope(|scope| {
        let threads: Vec<_> = (0..options.parallel)
            .map(|worker| {
                let options = &options;
                scope.spawn(move || match options.threaded {
                    true => run_worker_threaded(options, worker),
                    false => run_worker(options, worker),
                })
            })
            .collect();
        threads.into_iter().map(|thread| thread.join().unwrap_or_else(|_| Err(panicked()))).collect()
    });
    outcomes.into_iter().collect::<std::result::Result<(), Failure>>()?;
    Ok(0)
}
