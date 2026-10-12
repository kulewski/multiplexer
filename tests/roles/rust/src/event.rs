//! The Event of multiplexer/events.proto, which every role reports, printed
//! as one line of protocol buffer text format, as the C++ and Python roles
//! print theirs: neither prost nor any crate this repository takes has a
//! text format, and the message is flat, so the roles print it themselves.
//!
//! Fields come out in field-number order, strings with the C escapes and
//! every byte outside printable ASCII in octal, as C++'s printer writes
//! them, so that the line is ASCII whatever a payload holds; Python's
//! parser reads it back. A double comes out in plain decimals, which
//! tests/bench.sh's `ms:` pattern needs. A field events.proto lacks is a
//! mistake in the role, which panics.

use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::Mutex;

/// The fields of events.proto in field-number order: the number of a field
/// is its index plus one.
const FIELDS: [&str; 35] = [
    "event",
    "worker",
    "round",
    "index",
    "query_type",
    "type",
    "id",
    "sender",
    "to",
    "references",
    "ms",
    "payload",
    "size",
    "sha256",
    "kind",
    "instance_id",
    "connections",
    "name",
    "threaded",
    "workers",
    "responses",
    "errors",
    "sent",
    "received",
    "handled",
    "in_queue",
    "is_sent",
    "is_lost",
    "line",
    "after",
    "heap_bytes",
    "py_bytes",
    "objects",
    "drain_seconds",
    "key",
];

/// A field's value, as the text format writes it.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Text(Vec<u8>),
    Signed(i64),
    Unsigned(u64),
    Double(f64),
    Bool(bool),
}

impl From<&str> for Value {
    fn from(text: &str) -> Value {
        Value::Text(text.as_bytes().to_vec())
    }
}

impl From<String> for Value {
    fn from(text: String) -> Value {
        Value::Text(text.into_bytes())
    }
}

impl From<i32> for Value {
    fn from(number: i32) -> Value {
        Value::Signed(number.into())
    }
}

impl From<i64> for Value {
    fn from(number: i64) -> Value {
        Value::Signed(number)
    }
}

impl From<u32> for Value {
    fn from(number: u32) -> Value {
        Value::Unsigned(number.into())
    }
}

impl From<u64> for Value {
    fn from(number: u64) -> Value {
        Value::Unsigned(number)
    }
}

impl From<usize> for Value {
    fn from(number: usize) -> Value {
        Value::Unsigned(number as u64)
    }
}

impl From<f64> for Value {
    fn from(number: f64) -> Value {
        Value::Double(number)
    }
}

impl From<bool> for Value {
    fn from(flag: bool) -> Value {
        Value::Bool(flag)
    }
}

/// One Event: its name and the fields it sets.
#[derive(Debug, Clone)]
pub struct Event {
    fields: [Option<Value>; FIELDS.len()],
}

impl Event {
    /// An event of `name`, its other fields unset.
    pub fn new(name: &str) -> Event {
        let mut event = Event { fields: std::array::from_fn(|_| None) };
        event.set("event", name);
        event
    }

    /// Sets `field` to `value`; panics for a field events.proto lacks.
    pub fn set(&mut self, field: &str, value: impl Into<Value>) -> &mut Event {
        let Some(index) = FIELDS.iter().position(|name| *name == field) else {
            panic!("events.proto has no field {field}");
        };
        self.fields[index] = Some(value.into());
        self
    }

    /// `set()`, by value, for an event built in one expression.
    pub fn with(mut self, field: &str, value: impl Into<Value>) -> Event {
        self.set(field, value);
        self
    }

    /// The event as one line of text format, without the newline.
    pub fn line(&self) -> String {
        let mut line = String::new();
        for (name, value) in FIELDS.iter().zip(&self.fields) {
            let Some(value) = value else { continue };
            if !line.is_empty() {
                line.push(' ');
            }
            let _ = write!(line, "{name}: ");
            match value {
                Value::Text(bytes) => escape_into(&mut line, bytes),
                Value::Signed(number) => {
                    let _ = write!(line, "{number}");
                }
                Value::Unsigned(number) => {
                    let _ = write!(line, "{number}");
                }
                Value::Double(number) => write_double(&mut line, *number),
                Value::Bool(flag) => {
                    let _ = write!(line, "{flag}");
                }
            }
        }
        line
    }
}

/// Writes `bytes` as a quoted text-format string: the C escapes, and every
/// byte outside printable ASCII as three octal digits.
fn escape_into(line: &mut String, bytes: &[u8]) {
    line.push('"');
    for byte in bytes {
        match byte {
            b'\n' => line.push_str("\\n"),
            b'\r' => line.push_str("\\r"),
            b'\t' => line.push_str("\\t"),
            b'"' => line.push_str("\\\""),
            b'\'' => line.push_str("\\'"),
            b'\\' => line.push_str("\\\\"),
            0x20..=0x7e => line.push(char::from(*byte)),
            _ => {
                let _ = write!(line, "\\{byte:03o}");
            }
        }
    }
    line.push('"');
}

/// Writes a double as the text format reads it: plain decimals, which is
/// what Rust's `Display` gives, and the words for the values without.
fn write_double(line: &mut String, number: f64) {
    if number.is_nan() {
        line.push_str("nan");
    } else if number.is_infinite() {
        line.push_str(if number > 0.0 { "inf" } else { "-inf" });
    } else {
        let _ = write!(line, "{number}");
    }
}

/// Lines are written whole, one at a time: roles report from several
/// threads, a threaded backend's workers and a threaded client's io thread.
static STDOUT: Mutex<()> = Mutex::new(());

/// Prints `event` as one line on stdout, flushed at once, for the harness.
pub fn emit(event: &Event) {
    let line = event.line();
    let _guard = STDOUT.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(stdout, "{line}");
    let _ = stdout.flush();
}

#[cfg(test)]
mod tests {
    //! The lines the harness parses: field order, escapes, numbers.

    use super::Event;

    #[test]
    fn fields_come_out_in_field_number_order_whatever_the_order_they_were_set_in() {
        let event = Event::new("response").with("ms", 0.25).with("worker", 2).with("type", 203u32);
        assert_eq!("event: \"response\" worker: 2 type: 203 ms: 0.25", event.line());
    }

    #[test]
    fn a_string_is_escaped_as_cs_printer_escapes_it() {
        let event = Event::new("received").with("payload", "a\"b\\c\n\u{e9}\u{1}");
        assert_eq!("event: \"received\" payload: \"a\\\"b\\\\c\\n\\303\\251\\001\"", event.line());
    }

    #[test]
    fn numbers_keep_their_range_and_doubles_their_decimals() {
        let event = Event::new("memory")
            .with("id", u64::MAX)
            .with("after", -1i64)
            .with("ms", 0.00001)
            .with("drain_seconds", 3.0)
            .with("threaded", true);
        assert_eq!(
            "event: \"memory\" id: 18446744073709551615 ms: 0.00001 threaded: true after: -1 drain_seconds: 3",
            event.line()
        );
    }

    #[test]
    #[should_panic(expected = "events.proto has no field")]
    fn a_field_events_proto_lacks_is_a_mistake() {
        Event::new("connected").with("connection", 1);
    }
}
