//! A logger for the crate's own tests: every level enabled, and the lines
//! logged on a thread captured while that thread asks for them, so that a
//! test checks what the library logs, as the C++ tests check log lines.
//! Per thread, since tests run side by side; the core's code under test runs
//! on the test's own thread.

use std::cell::RefCell;
use std::sync::Once;

/// The logger; it keeps nothing itself.
struct TestLogger;

thread_local! {
    /// The lines captured on this thread, while capturing.
    static LINES: RefCell<Option<Vec<(log::Level, String)>>> = const { RefCell::new(None) };
}

impl log::Log for TestLogger {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        LINES.with(|lines| {
            if let Some(lines) = lines.borrow_mut().as_mut() {
                lines.push((record.level(), record.args().to_string()));
            }
        });
    }

    fn flush(&self) {}
}

/// Installs the logger, once per process.
pub(crate) fn install() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = log::set_logger(&TestLogger);
        log::set_max_level(log::LevelFilter::Trace);
    });
}

/// Runs `body` and returns what it gave and the lines it logged on this
/// thread.
pub(crate) fn capture<R>(body: impl FnOnce() -> R) -> (R, Vec<(log::Level, String)>) {
    install();
    LINES.with(|lines| *lines.borrow_mut() = Some(Vec::new()));
    let result = body();
    let lines = LINES.with(|lines| lines.borrow_mut().take()).unwrap_or_default();
    (result, lines)
}
