//! Log lines about messages that went nowhere, at most about two per kind
//! and second however many messages there are. Mirrors
//! multiplexer/log_summary.cc: the first line of a kind is logged at once
//! by the caller, with what it knows about that message; the rest are
//! counted, and about once a second one line per kind says how many more
//! there were: "<the kind's text> [N more in the last 1.0 s]". A kind that
//! went quiet for a whole second is forgotten, so its next line is logged
//! at once again. The timer is the core's (`Due::LogSummary`), running only
//! while something is counted, so an idle client never wakes for it.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// How often the counts are said.
pub(crate) const INTERVAL: Duration = Duration::from_secs(1);

/// At most this many kinds are counted at once; lines of further kinds go
/// into one count of their own.
pub(crate) const MAX_KINDS: usize = 256;

/// What tells two kinds of line apart: what the line says (a number of the
/// caller's own), its level, and the message type and instance id it
/// names, 0 where it names none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Kind {
    pub(crate) reason: u32,
    pub(crate) level: log::Level,
    pub(crate) r#type: u32,
    pub(crate) peer: u64,
}

/// A kind's text and how many more lines of it were counted since `since`.
#[derive(Debug)]
struct Count {
    text: String,
    more: u64,
    since: Instant,
}

/// The counts; see the module comment.
#[derive(Debug, Default)]
pub(crate) struct LogSummary {
    counts: BTreeMap<Kind, Count>,
    other: u64,
    other_level: Option<log::Level>,
    other_since: Option<Instant>,
}

/// " [N more in the last 1.0 s]", or with `what` in place of "more".
fn more_text(count: u64, what: &str, elapsed: Duration) -> String {
    format!(" [{count} {what} in the last {:.1} s]", elapsed.as_secs_f64())
}

impl LogSummary {
    /// Counts one line of `kind` at `now`: Some(true) for the first of its
    /// kind, whose text `make_text` gives, kept for the summary, the caller
    /// logging its own line about this message now; Some(false) when it is
    /// counted for the summary; None, counting nothing, when the logger
    /// drops `kind.level`.
    pub(crate) fn first(&mut self, kind: Kind, make_text: impl FnOnce() -> String, now: Instant) -> Option<bool> {
        if !log::log_enabled!(kind.level) {
            return None;
        }
        if let Some(count) = self.counts.get_mut(&kind) {
            count.more += 1;
            return Some(false);
        }
        if self.counts.len() >= MAX_KINDS {
            if self.other == 0 {
                self.other_since = Some(now);
            }
            self.other += 1;
            self.other_level = Some(self.other_level.map_or(kind.level, |level| level.min(kind.level)));
            return Some(false);
        }
        self.counts.insert(kind, Count { text: make_text(), more: 0, since: now });
        Some(true)
    }

    /// Logs every count above zero and resets it; with `forget`, drops
    /// every kind, else only those that had nothing to say. True when a
    /// kind or a count is left, for the timer to run on.
    pub(crate) fn say(&mut self, forget: bool, now: Instant) -> bool {
        self.counts.retain(|kind, count| {
            if count.more > 0 {
                log::log!(kind.level, "{}{}", count.text, more_text(count.more, "more", now - count.since));
                count.more = 0;
                count.since = now;
                return true;
            }
            forget
        });
        if self.other > 0 {
            let level = self.other_level.unwrap_or(log::Level::Warn);
            let since = self.other_since.unwrap_or(now);
            log::log!(level, "{}", more_text(self.other, "more lines of other kinds", now - since));
            self.other = 0;
            self.other_level = None;
        }
        if forget {
            self.counts.clear();
        }
        !self.counts.is_empty()
    }
}

#[cfg(test)]
mod tests {
    //! The counting and the lines, by multiplexer/log_summary_test.cc's
    //! cases.

    use super::{Kind, LogSummary, MAX_KINDS};
    use crate::test_log::capture;
    use std::time::{Duration, Instant};

    fn kind(reason: u32) -> Kind {
        Kind { reason, level: log::Level::Error, r#type: 0, peer: 0 }
    }

    #[test]
    fn the_first_of_a_kind_is_logged_and_the_rest_counted() {
        let now = Instant::now();
        let mut summary = LogSummary::default();
        let (_, lines) = capture(|| {
            assert_eq!(Some(true), summary.first(kind(1), || "one".into(), now));
            assert_eq!(Some(false), summary.first(kind(1), || "one".into(), now));
            assert_eq!(Some(false), summary.first(kind(1), || "one".into(), now));
            assert_eq!(Some(true), summary.first(kind(2), || "two".into(), now));
            assert!(summary.say(false, now + Duration::from_secs(1)), "kind 1 had more, and stays");
            // A whole interval without one: forgotten, logged at once next time.
            assert!(!summary.say(false, now + Duration::from_secs(2)));
            assert_eq!(Some(true), summary.first(kind(1), || "one".into(), now));
        });
        assert_eq!(vec![(log::Level::Error, "one [2 more in the last 1.0 s]".to_string())], lines);
    }

    #[test]
    fn kinds_past_the_limit_share_one_count() {
        let now = Instant::now();
        let mut summary = LogSummary::default();
        let (_, lines) = capture(|| {
            for reason in 0..MAX_KINDS as u32 {
                assert_eq!(Some(true), summary.first(kind(reason), || "a kind".into(), now));
            }
            assert_eq!(Some(false), summary.first(kind(MAX_KINDS as u32), || "more".into(), now));
            assert!(!summary.say(true, now + Duration::from_millis(1500)));
            assert_eq!(Some(true), summary.first(kind(MAX_KINDS as u32), || "more".into(), now));
        });
        assert_eq!(vec![(log::Level::Error, " [1 more lines of other kinds in the last 1.5 s]".to_string())], lines);
    }
}
