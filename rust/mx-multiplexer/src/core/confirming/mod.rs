//! Confirmed sends, both sides (docs/delivery.md "Confirmed sends"; the
//! rules at `Delivery`): a producer keeps every message of a confirmed
//! batch until a consumer's library confirms it, and sends it again or
//! gives up on it; a consumer's library gathers what it owes each producer
//! into one CONFIRMATION. Mirrors multiplexer/basic_client_confirm.cc and
//! confirming.h, whose comments this follows.
//!
//! The producer. A confirmed batch goes as an envelope whose `confirm`
//! numbers its messages (`numbered`, one counter per client), a batch of
//! one too, and waits once its frame is made (`wait_for_confirmation`): the
//! frame kept, which holds the messages' bytes, with their ids, their
//! numbers, as a first one and a count for a batch's first frame, the ends
//! of the messages someone follows, few or none, and a bit per message,
//! resolved or not; found by each run of consecutive numbers and by the
//! envelope's id. A CONFIRMATION resolves numbers (`on_confirmation`):
//! processed ends the send with 1, a run at a time where nobody follows its
//! messages, failed gives it up, refused sends it again at once. What is
//! unresolved goes again (`send_again`), keeping its ids and numbers, read
//! back out of the frame into a batch of its own that goes at once with the
//! same deadline: when nobody took the batch (a DELIVERY_ERROR, after a
//! back-off), when the connection it was written on closed, and when
//! nothing confirmed any of it for retry_after; what is left of a batch is
//! given up on, NotConfirmed, at its deadline, and every message left,
//! ShutDown, at shutdown. One timer runs to the earliest of every batch's
//! retry time and deadline, stopped once none waits. The ends run in the
//! callbacks pass, as a send's do, and a flush waits while a number at or
//! below its mark is unresolved. A confirmation costs a look-up per run of
//! numbers it names, not per message: a batch is taken out of its map once
//! per run (`with_batch`), so that resolving its messages may call into the
//! rest of the core.
//!
//! What goes again, and when, is `again`'s: the resends, the back-off, the
//! connection that closed, and the timer. The consumer's side is `owed`'s:
//! a message of a confirmed batch is handed out with its number
//! (`split_batch` in `client`), and what the program confirms is gathered
//! into one CONFIRMATION per producer.
//!
//! A confirmed batch's frame given up on tells only its messages still
//! unresolved, each once (`confirmed_frame_dropped`), whichever of the
//! frame's drop, the deadline and the shutdown comes first; batches sent
//! again together, and those given up on at shutdown, go in the order
//! their messages were numbered; and the timer stops once no batch waits,
//! so that a synchronous wait with nothing to wait for does not wait it
//! out. C++ does the same.

mod again;
mod owed;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use prost::Message as _;

use super::batching::{Batch, Delivery, Record};
use super::client::DropReason;
use super::lane::Lane;
use super::outbox::{Frame, SendDone};
use super::timers::TimerKey;
use super::{deadline_after, ConnId, Core, SendState, Tracker};
use crate::proto::multiplexer::{Confirmation, MultiplexerMessage};
use owed::Owed;

/// What became of a message taken from a confirmed batch, as its
/// consumer's library tells its producer (`SyncClient::confirm`,
/// `ThreadedClient::confirm`; docs/delivery.md, "Confirmed sends").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Confirmed {
    /// The program is done with it.
    Processed,
    /// Not processed, the consumer leaving or closing: its producer sends
    /// it again at once.
    Refused,
    /// It cannot be processed, its handler failed on it: its producer gives
    /// up on it.
    Failed,
}

/// A batch written whose messages wait for their confirmation (C++
/// Confirming): its group and delivery; its frame, which holds their bytes
/// for a send again, and their ids, as the C++ frame's marks hold them;
/// their numbers, `first_number` and on, one each, as a batch's first
/// frame numbers them, when `numbers` is empty, else each message's, as a
/// batch sent again keeps those its messages had; the ends of the few
/// messages someone follows; per message whether it is resolved, confirmed
/// or given up on; where its runs begin; the connection it was placed on,
/// when placed at once, and its frame's tracker, which reads SENT once the
/// frame is written, ahead of the end that says so (`confirming_written`);
/// when what is unresolved goes again, retry_after after the write or the
/// last confirmation, or a back-off after a DELIVERY_ERROR; its batch's
/// deadline, when what is unresolved is given up on; and its entry among
/// those due. Found by its envelope's id. Nothing in it is per message but
/// an id and a bit, and a number for a batch sent again.
pub(crate) struct Confirming {
    r#type: u32,
    key: Option<String>,
    delivery: Delivery,
    frame: Bytes,
    ids: Box<[u64]>,
    first_number: u64,
    numbers: Vec<u64>,
    /// The messages whose end someone hears, an `on_end` or a tracker, by
    /// their place in the frame, in order: none, mostly, and then a
    /// confirmation marks a whole run of them at once (`on_confirmation`).
    followed: Vec<Followed>,
    resolved: Flags,
    unresolved: usize,
    run_keys: Vec<u64>,
    placed_on: Option<ConnId>,
    frame_state: Option<Arc<Tracker>>,
    written: bool,
    retry_at: Option<Instant>,
    backoff: Duration,
    deadline: Option<Instant>,
    due: Option<Instant>,
}

/// A message of a batch waiting whose end someone hears: its place in the
/// frame, its `done` and its tracker, each taken once it ends.
struct Followed {
    index: usize,
    done: Option<SendDone>,
    tracker: Option<Arc<Tracker>>,
}

impl Confirming {
    /// How many messages its frame holds.
    fn count(&self) -> usize {
        self.ids.len()
    }

    /// The number of message `index`.
    fn number(&self, index: usize) -> u64 {
        if self.numbers.is_empty() {
            self.first_number + index as u64
        } else {
            self.numbers[index]
        }
    }

    /// The id of message `index`.
    fn id_of(&self, index: usize) -> u64 {
        self.ids[index]
    }

    /// The end of message `index`, its `done` and its tracker, taken, when
    /// someone follows it.
    fn take_end(&mut self, index: usize) -> (Option<SendDone>, Option<Arc<Tracker>>) {
        match self.followed.binary_search_by_key(&index, |each| each.index) {
            Ok(found) => {
                let followed = &mut self.followed[found];
                (followed.done.take(), followed.tracker.take())
            }
            Err(_) => (None, None),
        }
    }
}

/// One flag a message, whether it is resolved, 64 to a word: a
/// confirmation of a whole run marks words, not messages.
pub(crate) struct Flags {
    words: Vec<u64>,
}

impl Flags {
    /// `count` flags, none set.
    fn new(count: usize) -> Self {
        Flags { words: vec![0; count.div_ceil(64)] }
    }

    /// Flag `index` set.
    fn set(&mut self, index: usize) {
        self.words[index / 64] |= 1 << (index % 64);
    }

    /// The flags of `begin..end` set; how many of them were not.
    fn set_range(&mut self, mut begin: usize, end: usize) -> usize {
        let mut newly = 0;
        while begin < end {
            let bit = begin % 64;
            let span = (64 - bit).min(end - begin);
            let mask = if span == 64 { u64::MAX } else { ((1u64 << span) - 1) << bit };
            let word = &mut self.words[begin / 64];
            newly += (mask & !*word).count_ones() as usize;
            *word |= mask;
            begin += span;
        }
        newly
    }
}

impl std::ops::Index<usize> for Flags {
    type Output = bool;

    /// Whether flag `index` is set.
    fn index(&self, index: usize) -> &bool {
        if self.words[index / 64] >> (index % 64) & 1 == 1 {
            &true
        } else {
            &false
        }
    }
}

/// A run of consecutive numbers in a batch waiting, by its first number:
/// the batch's envelope id, where in it the run begins, its length, and how
/// many of it are unresolved, the run forgotten once none is.
struct Run {
    batch: u64,
    index: usize,
    count: usize,
    unresolved: usize,
}

/// What a list of a CONFIRMATION names that this client waits for: per run
/// it meets, the batch, the run's key and the messages' places in the
/// batch.
struct Segment {
    batch: u64,
    key: u64,
    begin: usize,
    end: usize,
}

/// Both sides' state (C++ BasicClient::Confirmer). The producer's: the last
/// number given; the runs by first number; the batches waiting by their
/// envelope's id, and by when they are due, one timer running to the
/// first, armed for `armed`, not while it fires; how many messages are
/// unresolved and how many went again; the ends resolved, for the callbacks
/// pass. The consumer's (`owed`): what is owed each producer and every
/// producer's count together; whether a send of them is posted.
#[derive(Default)]
pub(crate) struct Confirmer {
    pub(crate) last_number: u64,
    runs: BTreeMap<u64, Run>,
    by_id: HashMap<u64, Confirming>,
    by_due: BTreeSet<(Instant, u64)>,
    timer: Option<TimerKey>,
    armed: Option<Instant>,
    firing: bool,
    unconfirmed: usize,
    sent_again: u64,
    pub(crate) ends: Vec<(SendDone, u32)>,
    owed: HashMap<u64, Owed>,
    owed_count: usize,
    posted: bool,
}

/// `number` appended to `pairs`, (first, count) pairs, the last one grown
/// when the number continues it. Wrapping: a consumer's numbers come off the
/// wire.
pub(super) fn add_number(pairs: &mut Vec<u64>, number: u64) {
    let size = pairs.len();
    if size >= 2 && pairs[size - 2].wrapping_add(pairs[size - 1]) == number {
        pairs[size - 1] += 1;
        return;
    }
    pairs.push(number);
    pairs.push(1);
}

/// The earlier of two times, None standing for never.
fn earliest(first: Option<Instant>, second: Option<Instant>) -> Option<Instant> {
    match (first, second) {
        (Some(first), Some(second)) => Some(first.min(second)),
        (first, second) => first.or(second),
    }
}

impl Core {
    /// `records` numbered, those with no number yet given the next, and
    /// their numbers into an envelope's `confirm`, `pairs`.
    pub(super) fn numbered(&mut self, records: &mut [Record], pairs: &mut Vec<u64>) {
        pairs.clear();
        for record in records {
            if record.number == 0 {
                self.confirmer.last_number += 1;
                record.number = self.confirmer.last_number;
            }
            add_number(pairs, record.number);
        }
    }

    /// The records `begin..end` of `batch`, whose envelope is `frame`, kept
    /// until each is resolved: their ids and numbers, the ends someone
    /// follows, the frame holding their bytes, the records themselves left
    /// to the batch. The frame sent through `lane` when given, waiting for
    /// room and for a connection until the batch's deadline, its write
    /// heard (`confirming_written`), and the connection it was placed on
    /// kept when it went at once.
    pub(super) fn wait_for_confirmation(
        &mut self,
        frame: Frame,
        batch: &mut Batch,
        begin: usize,
        end: usize,
        lane: Option<&Lane>,
    ) {
        let id = frame.id;
        let records = &mut batch.records[begin..end];
        let count = records.len();
        let first_number = records[0].number;
        let mut consecutive = true;
        let mut followed = Vec::new();
        for (index, record) in records.iter_mut().enumerate() {
            consecutive = consecutive && record.number == first_number + index as u64;
            if record.done.is_some() || record.tracker.is_some() {
                followed.push(Followed { index, done: record.done.take(), tracker: record.tracker.take() });
            }
        }
        // Sent again, among others resolved before: each its own.
        let numbers = if consecutive { Vec::new() } else { records.iter().map(|record| record.number).collect() };
        let ids = records.iter().map(|record| record.id).collect();
        let confirming = Confirming {
            r#type: frame.r#type,
            key: batch.key.clone(),
            delivery: batch.delivery,
            frame: frame.bytes.clone(),
            ids,
            first_number,
            numbers,
            followed,
            resolved: Flags::new(count),
            unresolved: count,
            run_keys: Vec::new(),
            placed_on: None,
            frame_state: None,
            written: false,
            retry_at: None,
            backoff: batch.backoff,
            deadline: batch.deadline,
            due: None,
        };
        self.confirmer.unconfirmed += count;
        self.confirmer.by_id.insert(id, confirming);
        self.rebuild_runs(id);
        let mut trackers = Vec::with_capacity(1);
        let mut used = None;
        self.send_at(
            frame,
            false,
            lane,
            batch.deadline,
            0,
            Some(&mut trackers),
            Some(&mut used),
            Some(SendDone::Confirmed(id)),
        );
        if let Some(confirming) = self.confirmer.by_id.get_mut(&id) {
            confirming.frame_state = trackers.into_iter().next();
            confirming.placed_on = used.and_then(|wrapper| wrapper.conn);
        }
        self.confirming_due(id);
    }

    /// The records `begin..end` of `batch`, which no frame could hold, past
    /// MAX_MESSAGE_SIZE in an envelope, given up on, NotConfirmed: none is
    /// sent unnumbered.
    pub(super) fn cannot_confirm(&mut self, batch: &mut Batch, begin: usize, end: usize) {
        let r#type = batch.r#type;
        log::error!(
            "a confirmed batch's message could not be made, its {} message(s) are given up on; type {type}",
            end - begin
        );
        for record in &mut batch.records[begin..end] {
            let (done, tracker) = (record.done.take(), record.tracker.take());
            self.confirmed_end(record.id, record.number, r#type, tracker, done, false, DropReason::NotConfirmed, true);
        }
    }

    /// Runs `work` on the batch of envelope `batch`, taken out of the map
    /// meanwhile so that it may call into the core; None when no batch has
    /// that id.
    fn with_batch<R>(&mut self, batch: u64, work: impl FnOnce(&mut Core, &mut Confirming) -> R) -> Option<R> {
        let mut confirming = self.confirmer.by_id.remove(&batch)?;
        let result = work(self, &mut confirming);
        self.confirmer.by_id.insert(batch, confirming);
        Some(result)
    }

    /// The runs of consecutive numbers among the batch's unresolved
    /// messages, registered anew: each unresolved number in exactly one
    /// run, so that a message sent again, which keeps its number, is found
    /// in its new batch.
    fn rebuild_runs(&mut self, batch: u64) {
        let Confirmer { runs, by_id, .. } = &mut self.confirmer;
        let Some(confirming) = by_id.get_mut(&batch) else { return };
        for key in confirming.run_keys.drain(..) {
            if runs.get(&key).is_some_and(|run| run.batch == batch) {
                runs.remove(&key);
            }
        }
        if confirming.unresolved == 0 {
            return; // none left: its runs erased
        }
        let count = confirming.count();
        if confirming.unresolved == count && confirming.numbers.is_empty() {
            // Nothing resolved and the numbers consecutive, as a batch's are
            // when it is made: one run, found at once.
            let first = confirming.first_number;
            runs.insert(first, Run { batch, index: 0, count, unresolved: count });
            confirming.run_keys.push(first);
            return;
        }
        let mut index = 0;
        while index < count {
            if confirming.resolved[index] {
                index += 1;
                continue;
            }
            let first = confirming.number(index);
            let mut length = 1;
            while index + length < count
                && !confirming.resolved[index + length]
                && confirming.number(index + length) == first + length as u64
            {
                length += 1;
            }
            runs.insert(first, Run { batch, index, count: length, unresolved: length });
            confirming.run_keys.push(first);
            index += length;
        }
    }

    /// `count` more messages of the batch's run that holds `number`
    /// resolved; the run forgotten once none of it is left.
    fn resolved_in_run(&mut self, batch: u64, number: u64, count: usize) {
        let runs = &mut self.confirmer.runs;
        let Some((&key, run)) = runs.range_mut(..=number).next_back() else { return };
        if run.batch != batch || number >= key + run.count as u64 {
            return;
        }
        run.unresolved -= count.min(run.unresolved);
        if run.unresolved == 0 {
            runs.remove(&key);
        }
    }

    /// The frame of batch `batch` written: what is unresolved goes again
    /// retry_after from now, unless a DELIVERY_ERROR set an earlier time.
    /// Given up on instead, its drop told and ended every message still
    /// unresolved then (`confirmed_frame_dropped`): what is left ends lost,
    /// untold again. Nothing for a batch resolved, or sent again,
    /// meanwhile.
    pub(crate) fn confirming_written(&mut self, batch: u64, written: u32) {
        if !self.confirmer.by_id.contains_key(&batch) {
            return;
        }
        if written == 0 {
            // Its drop told every message still unresolved then.
            self.with_batch(batch, |core, confirming| {
                for index in 0..confirming.count() {
                    core.resolve_in(confirming, index, false, DropReason::NoConnection, false);
                }
            });
            self.forget_resolved(batch);
            return;
        }
        let now = self.now;
        if let Some(confirming) = self.confirmer.by_id.get_mut(&batch) {
            confirming.written = true;
            let retry = deadline_after(now, Some(confirming.delivery.retry_after));
            confirming.retry_at = earliest(confirming.retry_at, retry);
        }
        self.confirming_due(batch);
    }

    /// The frame of the confirmed batch of envelope `batch` was given up
    /// on, for `reason`: its messages still unresolved end lost, each told
    /// for that reason, once; the frame's end, which follows, finds them
    /// resolved. Nothing for a batch resolved meanwhile, its messages told
    /// as they were given up on.
    pub(crate) fn confirmed_frame_dropped(&mut self, batch: u64, reason: DropReason) {
        self.with_batch(batch, |core, confirming| {
            for index in 0..confirming.count() {
                core.resolve_in(confirming, index, false, reason, true);
            }
        });
        self.forget_resolved(batch);
    }

    /// Message `index` of `confirming` resolved, unless it was: processed,
    /// its send ends with 1; else given up on for `reason`, told as a drop
    /// unless `report` is false (`confirmed_end`). Its run is left to the
    /// caller (`resolved_in_run`).
    fn resolve_in(
        &mut self,
        confirming: &mut Confirming,
        index: usize,
        processed: bool,
        reason: DropReason,
        report: bool,
    ) -> bool {
        if confirming.resolved[index] {
            return false;
        }
        confirming.resolved.set(index);
        confirming.unresolved -= 1;
        self.confirmer.unconfirmed -= 1;
        let (done, tracker) = confirming.take_end(index);
        let id = if !processed && report { confirming.id_of(index) } else { 0 };
        self.confirmed_end(id, confirming.number(index), confirming.r#type, tracker, done, processed, reason, report);
        true
    }

    /// A confirmed send's end, the message `id` numbered `number`, of
    /// `r#type`: processed, its tracker, when given, reads SENT and its
    /// `done`, when given, hears 1; else it is given up on for `reason`,
    /// told and counted as a drop unless `report` is false (its frame's
    /// drop told it), a flush that waits for it ending with not all written,
    /// its tracker reading LOST and its `done` hearing 0. The ends run in the
    /// callbacks pass, after the drop observer heard of the drops.
    #[allow(clippy::too_many_arguments)]
    fn confirmed_end(
        &mut self,
        id: u64,
        number: u64,
        r#type: u32,
        tracker: Option<Arc<Tracker>>,
        done: Option<SendDone>,
        processed: bool,
        reason: DropReason,
        report: bool,
    ) {
        if !processed {
            self.flushes_lost_confirmed(number);
            if report {
                self.report_record_drop(id, r#type, reason);
            }
        }
        if let Some(tracker) = tracker {
            if tracker.get() == SendState::Queued {
                tracker.set(if processed { SendState::Sent } else { SendState::Lost });
            }
        }
        if let Some(done) = done {
            self.confirmer.ends.push((done, u32::from(processed)));
        }
        if !self.confirmer.ends.is_empty() || self.flushing() {
            self.post_callbacks(); // the ends, and a flush that waited for this
        }
    }

    /// The batch of envelope `batch`, once nothing of it is unresolved,
    /// forgotten, with its runs and its place among those due; the timer
    /// stopped with the last batch waiting.
    fn forget_resolved(&mut self, batch: u64) {
        if self.confirmer.by_id.get(&batch).is_none_or(|confirming| confirming.unresolved != 0) {
            return;
        }
        let Some(confirming) = self.confirmer.by_id.remove(&batch) else { return };
        let Confirmer { runs, by_due, .. } = &mut self.confirmer;
        if let Some(due) = confirming.due {
            by_due.remove(&(due, batch));
        }
        for key in &confirming.run_keys {
            if runs.get(key).is_some_and(|run| run.batch == batch) {
                runs.remove(key);
            }
        }
        if by_due.is_empty() {
            self.stop_confirm_timer();
        }
    }

    /// The segments of this client's runs that a CONFIRMATION's list names,
    /// found before any is resolved: resolving erases a run once none of it
    /// is left. A pair that does not read, empty or past the last number
    /// there is, names nothing; so does an odd number left over.
    fn segments(&self, pairs: &[u64]) -> Vec<Segment> {
        let runs = &self.confirmer.runs;
        let mut found = Vec::new();
        for &[first, count] in pairs.as_chunks::<2>().0 {
            let Some(past) = first.checked_add(count).filter(|past| *past > first) else { continue };
            let start = runs.range(..=first).next_back().map_or(first, |(key, _)| *key);
            for (&key, run) in runs.range(start..past) {
                let from = first.max(key);
                let to = past.min(key + run.count as u64);
                if from < to {
                    found.push(Segment {
                        batch: run.batch,
                        key,
                        begin: run.index + (from - key) as usize,
                        end: run.index + (to - key) as usize,
                    });
                }
            }
        }
        found
    }

    /// A CONFIRMATION: processed numbers end their sends with 1, failed ones
    /// are given up on, refused ones go again at once; a batch that heard of
    /// one of its messages waits retry_after again before it sends what is
    /// left, its back-off over. A number this client does not wait for,
    /// resolved before, is passed by, as is a payload that does not read:
    /// what it names goes again in time.
    pub(crate) fn on_confirmation(&mut self, message: &MultiplexerMessage) {
        let Some(payload) = message.message.clone() else { return };
        let Ok(confirmation) = Confirmation::decode(payload) else { return };
        let mut heard: Vec<u64> = Vec::new();
        let lists = [
            (&confirmation.processed, true, DropReason::NotConfirmed),
            (&confirmation.failed, false, DropReason::Failed),
        ];
        for (pairs, processed, reason) in lists {
            for segment in self.segments(pairs) {
                let resolved = if let Some(resolved) = self.resolved_at_once(&segment, processed) {
                    resolved
                } else {
                    self.with_batch(segment.batch, |core, confirming| {
                        let mut resolved = 0;
                        for index in segment.begin..segment.end {
                            resolved += usize::from(core.resolve_in(confirming, index, processed, reason, true));
                        }
                        resolved
                    })
                    .unwrap_or(0)
                };
                if resolved > 0 {
                    self.resolved_in_run(segment.batch, segment.key, resolved);
                    if heard.last() != Some(&segment.batch) {
                        heard.push(segment.batch);
                    }
                }
            }
        }
        let mut refused: Vec<(u64, Vec<usize>)> = Vec::new();
        for segment in self.segments(&confirmation.refused) {
            match refused.last_mut() {
                Some((batch, indexes)) if *batch == segment.batch => indexes.extend(segment.begin..segment.end),
                _ => refused.push((segment.batch, (segment.begin..segment.end).collect())),
            }
        }
        for (batch, indexes) in refused {
            self.send_again(batch, &indexes);
        }
        heard.sort_unstable();
        heard.dedup();
        let now = self.now;
        for batch in heard {
            let Some(confirming) = self.confirmer.by_id.get_mut(&batch) else { continue };
            if confirming.unresolved == 0 {
                self.forget_resolved(batch);
                continue;
            }
            confirming.backoff = Duration::ZERO; // somebody takes its messages
            if confirming.written {
                confirming.retry_at = deadline_after(now, Some(confirming.delivery.retry_after));
            }
            self.confirming_due(batch);
        }
    }

    /// The processed messages of `segment` resolved a word of flags at a
    /// time when its batch has nothing to tell them, no message of it
    /// followed by an `on_end` or a tracker: how many were not resolved
    /// before; None for a batch that has, or a failure, each message of
    /// which is resolved on its own.
    fn resolved_at_once(&mut self, segment: &Segment, processed: bool) -> Option<usize> {
        if !processed {
            return None;
        }
        let Confirmer { by_id, unconfirmed, .. } = &mut self.confirmer;
        let confirming = by_id.get_mut(&segment.batch)?;
        if !confirming.followed.is_empty() {
            return None;
        }
        let resolved = confirming.resolved.set_range(segment.begin, segment.end);
        confirming.unresolved -= resolved;
        *unconfirmed -= resolved;
        if resolved > 0 && self.flushing() {
            self.post_callbacks(); // a flush that waited for these
        }
        Some(resolved)
    }

    /// Every message still waiting for its confirmation given up on, for
    /// `reason`, in the order sent: the client shuts down. A frame of
    /// theirs dropped later finds them resolved and tells nothing more.
    pub(crate) fn give_up_confirmations(&mut self, reason: DropReason) {
        let mut waiting: Vec<Confirming> = std::mem::take(&mut self.confirmer.by_id).into_values().collect();
        waiting.sort_by_key(|confirming| confirming.number(0));
        for mut confirming in waiting {
            for index in 0..confirming.count() {
                self.resolve_in(&mut confirming, index, false, reason, true);
            }
        }
        self.confirmer.runs.clear();
        self.confirmer.by_due.clear();
        self.stop_confirm_timer();
    }

    /// The first number still waiting for its confirmation, if any: a
    /// flush waits while it is at or below its mark.
    pub(crate) fn first_unconfirmed(&self) -> Option<u64> {
        self.confirmer.runs.first_key_value().map(|(first, _)| *first)
    }

    /// The ends of the confirmed sends resolved, in the order resolved.
    pub(crate) fn run_confirmed_ends(&mut self) {
        if self.confirmer.ends.is_empty() {
            return;
        }
        for (done, written) in std::mem::take(&mut self.confirmer.ends) {
            self.end_send(done, written);
        }
    }

    /// When the timer is armed to fire, if it is; for tests.
    #[cfg(test)]
    pub(crate) fn confirmer_armed(&self) -> Option<Instant> {
        self.confirmer.timer.and(self.confirmer.armed)
    }

    /// How many messages this client sent wait for their confirmation; for
    /// tests.
    pub(crate) fn unconfirmed(&self) -> usize {
        self.confirmer.unconfirmed
    }

    /// How many messages this client sent again, unconfirmed; for tests.
    pub(crate) fn sent_again(&self) -> u64 {
        self.confirmer.sent_again
    }
}
