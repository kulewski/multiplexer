//! What every Rust test role shares, as tests/roles/cc/common.h has it for
//! the C++ roles: the options all of them take (--mx, --type, --name), the
//! parsers of the option formats, the SIGTERM flag, the end of the process
//! with the harness's, the memory event, the payload's report, the names of
//! the errors, and the library's log on stderr, which the harness keeps.
//!
//! The libc calls here, signal handlers, the parent's pipe and mallinfo2,
//! are the roles' only unsafe code.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mx_multiplexer::{Error, KeyFilters, Routing, SyncClient};

use crate::args::Args;
use crate::event::Event;

/// How a role ends other than with an exit code of its own: a command
/// line it cannot take (2), or a failure it reports on stderr alone (1).
#[derive(Debug)]
pub enum Failure {
    Usage(String),
    Fatal(String),
}

impl From<Error> for Failure {
    fn from(error: Error) -> Failure {
        Failure::Fatal(error.to_string())
    }
}

/// What a role's `run` returns: its exit code, or how it failed.
pub type Outcome = Result<i32, Failure>;

/// --mx (repeatable), --type and --name.
#[derive(Debug, Clone)]
pub struct Common {
    pub mx: Vec<String>,
    pub peer_type: u32,
    pub name: String,
}

impl Common {
    /// The common options of `args`; --type is required.
    pub fn from(args: &Args) -> Result<Common, Failure> {
        if args.all("type").is_empty() {
            return Err(Failure::Usage("--type is required".to_string()));
        }
        Ok(Common {
            mx: args.all("mx").to_vec(),
            peer_type: args.get("type", 0).map_err(Failure::Usage)?,
            name: args.text("name"),
        })
    }

    /// A `SyncClient` connected to every --mx, `key_filters` set before
    /// the first connection, so that every welcome carries them.
    pub fn connect(&self, key_filters: KeyFilters) -> Result<SyncClient, Failure> {
        let mut builder = SyncClient::builder(self.peer_type);
        if !key_filters.is_empty() {
            builder = builder.key_filters(key_filters);
        }
        Ok(builder.connect(&self.mx)?)
    }

    /// The "connected" event: the instance id, the connections and the name.
    pub fn connected_event(&self, instance_id: u64, connections: usize) -> Event {
        Event::new("connected")
            .with("instance_id", instance_id)
            .with("connections", connections)
            .with("name", self.name.as_str())
    }
}

/// "201=203" pairs, as --serves takes them.
pub fn kv_ints(items: &[String]) -> Result<BTreeMap<u32, u32>, Failure> {
    let mut pairs = BTreeMap::new();
    for item in items {
        let (key, value) =
            item.split_once('=').ok_or_else(|| Failure::Usage(format!("expected KEY=VALUE, not {item}")))?;
        pairs.insert(number(key)?, number(value)?);
    }
    Ok(pairs)
}

/// "201:payload" pairs, as --query and --send take them.
pub fn typed_payloads(items: &[String]) -> Result<Vec<(u32, String)>, Failure> {
    items
        .iter()
        .map(|item| {
            let (message_type, payload) =
                item.split_once(':').ok_or_else(|| Failure::Usage(format!("expected TYPE:payload, not {item}")))?;
            Ok((number(message_type)?, payload.to_string()))
        })
        .collect()
}

/// --key-filter's TYPE:PATTERN values: per message type its patterns, each
/// split at its first colon, so that a pattern may have more.
pub fn parse_key_filters(items: &[String]) -> Result<KeyFilters, Failure> {
    let mut filters = KeyFilters::new();
    for item in items {
        let (message_type, pattern) =
            item.split_once(':').ok_or_else(|| Failure::Usage(format!("a key filter is TYPE:PATTERN, not {item}")))?;
        filters.entry(number(message_type)?).or_default().push(pattern.to_string());
    }
    Ok(filters)
}

/// --drain-routing's comma-separated flag names as a Routing, every flag
/// off unless named.
pub fn parse_drain_routing(flags: &str) -> Result<Routing, Failure> {
    let mut routing = Routing { any: false, all: false, last_resort: false };
    for flag in flags.split(',').filter(|flag| !flag.is_empty()) {
        match flag {
            "any" => routing.any = true,
            "all" => routing.all = true,
            "last_resort" => routing.last_resort = true,
            _ => return Err(Failure::Usage(format!("unknown routing flag: {flag}"))),
        }
    }
    Ok(routing)
}

/// A number of an option's value.
fn number(text: &str) -> Result<u32, Failure> {
    text.parse().map_err(|_| Failure::Usage(format!("not a number: {text:?}")))
}

/// `value` seconds as a Duration, a negative or NaN one as zero.
pub fn seconds(value: f64) -> Duration {
    Duration::try_from_secs_f64(value.max(0.0)).unwrap_or(Duration::MAX)
}

/// Sets `size` and, for a payload of 256 bytes at most that is UTF-8,
/// `payload`: a line with bytes that are not would not parse back.
pub fn set_payload(event: &mut Event, payload: &[u8]) {
    event.set("size", payload.len());
    if payload.len() <= 256 {
        if let Ok(text) = std::str::from_utf8(payload) {
            event.set("payload", text);
        }
    }
}

/// The name an error is reported by, the Python exception's, which the
/// C++ roles report too.
pub fn kind_of(error: &Error) -> &'static str {
    match error {
        Error::Backend(_) => "BackendError",
        Error::NotConnected => "NotConnected",
        Error::OperationTimedOut => "OperationTimedOut",
        Error::OperationFailed => "OperationFailed",
        Error::TooLarge { .. } | Error::InvalidArgument(_) => "ValueError",
        Error::OnIoThread => "RuntimeError",
        Error::UsedAfterFork => "UsedAfterFork",
        _ => "Error",
    }
}

/// SIGTERM and SIGINT set this; the roles that loop check it between
/// messages.
pub static STOP: AtomicBool = AtomicBool::new(false);

/// The handler of SIGTERM and SIGINT.
extern "C" fn on_signal(_signal: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

/// Has SIGTERM and SIGINT set STOP instead of ending the process.
pub fn install_signal_handlers() {
    let handler = on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
    // SAFETY: the handler stores to an atomic and nothing else, which is
    // async-signal-safe.
    unsafe {
        libc::signal(libc::SIGTERM, handler);
        libc::signal(libc::SIGINT, handler);
    }
}

/// Under the test harness, which hands every process it starts the read end
/// of a pipe only it writes to, named by MX_TEST_PARENT_FD: the role ends
/// as SIGTERM ends it once the pipe reaches its end, which is when the test
/// process is gone, however it died, as lib/program.h has the C++ processes
/// do. The variable is cleared and the descriptor kept from children.
/// Called first thing, before any thread exists.
pub fn end_with_the_harness() {
    let Some(named) = std::env::var_os("MX_TEST_PARENT_FD") else { return };
    std::env::remove_var("MX_TEST_PARENT_FD");
    let Some(descriptor) = named.to_str().and_then(|text| text.parse::<libc::c_int>().ok()) else { return };
    // SAFETY: fcntl on a descriptor number touches no memory of ours.
    if descriptor < 0 || unsafe { libc::fcntl(descriptor, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
        return; // not open: nothing to watch
    }
    let watch = move || {
        let mut byte = 0u8;
        loop {
            // SAFETY: reads one byte at most into `byte`, which outlives the call.
            let got = unsafe { libc::read(descriptor, std::ptr::addr_of_mut!(byte).cast(), 1) };
            if got == 0 {
                break; // the test process is gone
            }
            if got < 0 && std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
                return;
            }
        }
        // SAFETY: a signal to this process, as kill(1) would send it.
        unsafe {
            libc::kill(libc::getpid(), libc::SIGTERM);
        }
    };
    let _ = std::thread::Builder::new().name("end-with-the-harness".to_string()).spawn(watch);
}

/// The C heap in use, as lib/memory.cc measures it: glibc's chunks in use
/// plus its mmapped ones. Rust's allocator is the system's, glibc's.
pub fn heap_in_use_bytes() -> u64 {
    // SAFETY: mallinfo2 reads the allocator's statistics and nothing else.
    let info = unsafe { libc::mallinfo2() };
    (info.uordblks + info.hblkhd) as u64
}

/// A "memory" event: the C heap in use after `after` messages.
pub fn memory_event(after: i64) -> Event {
    Event::new("memory").with("after", after).with("heap_bytes", heap_in_use_bytes())
}

/// The library's log, at info and above, on stderr: the harness keeps a
/// role's stderr beside its events.
struct StderrLog;

impl log::Log for StderrLog {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Info
    }

    fn log(&self, record: &log::Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let _ = writeln!(
            std::io::stderr().lock(),
            "{}.{:03} {} {}] {}",
            now.as_secs(),
            now.subsec_millis(),
            record.level(),
            record.target(),
            record.args()
        );
    }

    fn flush(&self) {}
}

/// Sends the library's log to stderr.
pub fn install_logger() {
    static LOG: StderrLog = StderrLog;
    if log::set_logger(&LOG).is_ok() {
        log::set_max_level(log::LevelFilter::Info);
    }
}

#[cfg(test)]
mod tests {
    //! The option formats, as the C++ and Python roles read them.

    use super::{kv_ints, parse_drain_routing, parse_key_filters, set_payload, typed_payloads};
    use crate::event::Event;

    /// `items` as owned strings.
    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| item.to_string()).collect()
    }

    #[test]
    fn the_option_formats_split_where_the_other_roles_split_them() {
        assert_eq!(vec![(201, 203)], kv_ints(&strings(&["201=203"])).unwrap().into_iter().collect::<Vec<_>>());
        assert_eq!(vec![(201, "a:b".to_string())], typed_payloads(&strings(&["201:a:b"])).unwrap());
        let filters = parse_key_filters(&strings(&["201:x:y", "201:z"])).unwrap();
        assert_eq!(Some(&strings(&["x:y", "z"])), filters.get(&201));
        let routing = parse_drain_routing("all,last_resort").unwrap();
        assert!(!routing.any && routing.all && routing.last_resort);
        assert!(parse_drain_routing("most").is_err());
    }

    #[test]
    fn a_payload_is_reported_when_short_and_utf8_its_size_always() {
        let mut short = Event::new("received");
        set_payload(&mut short, "h\u{e9}".as_bytes());
        assert_eq!("event: \"received\" payload: \"h\\303\\251\" size: 3", short.line());
        let mut binary = Event::new("received");
        set_payload(&mut binary, &[0xff, 0x00]);
        assert_eq!("event: \"received\" size: 2", binary.line());
        let mut long = Event::new("received");
        set_payload(&mut long, &[b'x'; 257]);
        assert_eq!("event: \"received\" size: 257", long.line());
    }
}
