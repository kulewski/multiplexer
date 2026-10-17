//! A client's batching: the events the program sends without waiting for
//! them, of the types and keys it named (`Core::set_batching`), go
//! together, several in one message (MultiplexerMessage.batch), which the
//! receiving library splits back into one message each (`split_batch` in
//! `client`). Mirrors multiplexer/basic_client_batching.cc, whose comment
//! this follows.
//!
//! A message for a batch joins the open batch of its group, its type and
//! key, or opens one. A batch is a list of the messages' bodies, each
//! serialized once, where it was sent: a plain send's bytes as they came
//! (`plain`), a frame's body copied into a body kept (`bodies`). It goes
//! (`close_batch`) when it is full, by messages or by bytes, when its
//! max_delay is over, one timer running to the earliest, or, with
//! max_delay zero, when a connection has nothing left to write, on it:
//! while one has, a message goes at once and alone, as without batching,
//! and only while every connection is busy do messages gather, each batch
//! going as soon as one is not (`Conn`'s notify_when_idle), or at its first
//! message's deadline, the time it waited being a wait for room. A batch
//! that goes on its own account, full, at its delay or at its deadline,
//! takes those waiting for a connection with nothing left to write with
//! it. Anything sent that does not join a batch, a flush, a shutdown and a
//! change of what is batched first let every batch go, in the order they
//! began, so that a message overtakes no batched one sent before it; a
//! batch may go before an older one of another group. Open batches are
//! indexed by the order they began and by when they are due, so that each
//! step is a look-up however many keys are open.
//!
//! Going, a batch becomes one frame (`send_batch`, in `bodies`): an
//! envelope with the group's type and key, a fresh id and this client as
//! its sender, and the messages' bodies in a MessageBatch inside it
//! (`BodiesEnvelope`, the one envelope the batcher keeps, the bodies moved
//! in and encoded once, straight into a frame buffer an earlier batch frame
//! left), measured as it goes on the wire and cut where it would be over
//! max_bytes, as many at once as cover the excess, what is cut going next
//! in a batch of its own; the bodies then go back for the next plain
//! sends. A batch of one goes as the message itself, a frame made of its
//! body. The frame then goes as any message does (`send_at`): round robin,
//! waiting for room and for a connection until the earliest deadline among
//! its messages, counted from their sends in a batch with no delay and from
//! now in one with a delay. Its end is each message's: their trackers and
//! `done`s hear it from the frame's (`SendDone::Batch`), and a drop of the
//! frame is told for each message (`report_drop`, `OutFrame::batch`).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::bodies::{Bodies, BodiesEnvelope, FramePool};
use super::outbox::{Frame, SendDone};
use super::plain::Plain;
use super::timers::TimerKey;
use super::{deadline_after, ConnId, Core, Due, SendState, Tracker, MAX_TIMEOUT};
use crate::constants::types;
use crate::defaults::{
    DEFAULT_BATCH_MAX_BYTES, DEFAULT_BATCH_MAX_RECORDS, MAX_BATCH_RECORDS, MAX_KEY_SIZE, MAX_MESSAGE_SIZE,
};
use crate::error::{Error, Result};
use crate::frame::HEADER_LENGTH;
use crate::proto::multiplexer::{compression, logging_method, MultiplexerMessage};

/// How the events of a type, or of one key of it, are batched
/// (`SyncClient::set_batching`): a batch goes once it holds `max_records`
/// messages, once one more would take its message past `max_bytes` as it
/// goes on the wire, or `max_delay` after its first message; with no
/// delay, once a connection has nothing left to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Batching {
    /// The most messages in one batch, 1 to MAX_BATCH_RECORDS;
    /// DEFAULT_BATCH_MAX_RECORDS by default.
    pub max_records: usize,
    /// The most bytes a batch's message has on the wire, 1 to
    /// MAX_MESSAGE_SIZE; DEFAULT_BATCH_MAX_BYTES by default.
    pub max_bytes: usize,
    /// How long a batch's first message waits for more, at most
    /// MAX_SECONDS; zero, the default, for a batch that goes as soon as a
    /// connection has nothing left to write.
    pub max_delay: Duration,
}

impl Default for Batching {
    fn default() -> Self {
        Batching {
            max_records: DEFAULT_BATCH_MAX_RECORDS,
            max_bytes: DEFAULT_BATCH_MAX_BYTES,
            max_delay: Duration::ZERO,
        }
    }
}

/// What batching knows of a frame, read where its message became the frame
/// (`message::frame_of`): its message is a plain event, which may go in a
/// batch, with its key or none, which routes otherwise than an empty one,
/// the frame made unsealed, without its checksum, which `Core::out_frame`
/// writes if it goes alone (`frame::seal`); it is a batch's frame, with the
/// ids of the messages inside, in order, by which a drop of it is told, one
/// each; or neither.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) enum BatchMark {
    #[default]
    None,
    Event(Option<String>),
    Batch(Arc<[u64]>),
}

/// Whether `message` may go in a batch: an event the program sends that
/// nobody waits for, with an id, a sender, a type, a payload and at most a
/// key, a workflow and a timestamp, nothing that routes it otherwise or
/// asks the multiplexer for a report, and no batch itself (C++
/// RawMessage::plain_event). What a receiver checks of each message in a
/// batch too. An enum's value the protocol does not name is no value, as
/// C++ reads it: its field's default.
pub(crate) fn plain_event(message: &MultiplexerMessage) -> bool {
    let logging = message
        .logging_method
        .filter(|value| logging_method::Values::try_from(*value).is_ok())
        .unwrap_or(logging_method::Values::Both as i32);
    let compressed = message
        .compression
        .filter(|value| compression::Values::try_from(*value).is_ok())
        .unwrap_or(compression::Values::NoCompression as i32);
    message.to.unwrap_or(0) == 0
        && message.references.unwrap_or(0) == 0
        && !message.report_delivery_error.unwrap_or(false)
        && !message.include_original_packet_in_report.unwrap_or(false)
        && message.override_rrules.is_empty()
        && logging == logging_method::Values::Both as i32
        && compressed == compression::Values::NoCompression as i32
        && message.batch.is_none()
}

/// `InvalidArgument` for batching a protocol message type (99 and below), a
/// key over MAX_KEY_SIZE, no message in a batch or more than
/// MAX_BATCH_RECORDS, no byte or more than MAX_MESSAGE_SIZE, or a delay
/// over MAX_SECONDS; checked where the program gives them, on its own
/// thread.
pub(crate) fn check_batching(r#type: u32, key: Option<&str>, batching: &Batching) -> Result<()> {
    if r#type <= types::MAX_MULTIPLEXER_META_PACKET {
        return Err(Error::InvalidArgument(format!("batching for message type {type}, a protocol type")));
    }
    if let Some(key) = key.filter(|key| key.len() > MAX_KEY_SIZE) {
        return Err(Error::InvalidArgument(format!(
            "batching for a key of {} bytes, over the limit of {MAX_KEY_SIZE} (MAX_KEY_SIZE)",
            key.len()
        )));
    }
    if batching.max_records < 1 || batching.max_records > MAX_BATCH_RECORDS {
        return Err(Error::InvalidArgument(format!(
            "batching with max_records {}: 1 to MAX_BATCH_RECORDS ({MAX_BATCH_RECORDS})",
            batching.max_records
        )));
    }
    if batching.max_bytes < 1 || batching.max_bytes > MAX_MESSAGE_SIZE {
        return Err(Error::InvalidArgument(format!(
            "batching with max_bytes {}: 1 to MAX_MESSAGE_SIZE ({MAX_MESSAGE_SIZE})",
            batching.max_bytes
        )));
    }
    if batching.max_delay > MAX_TIMEOUT {
        return Err(Error::InvalidArgument(format!(
            "batching with max_delay {:?}: at most MAX_SECONDS ({} s)",
            batching.max_delay,
            MAX_TIMEOUT.as_secs()
        )));
    }
    Ok(())
}

/// What joins a batch: a frame a send made, or a plain send's bytes.
pub(crate) enum Joining {
    Frame(Frame),
    Plain(Plain),
}

impl Joining {
    /// The message's type.
    fn r#type(&self) -> u32 {
        match self {
            Joining::Frame(frame) => frame.r#type,
            Joining::Plain(plain) => plain.r#type,
        }
    }

    /// The key it batches by, if it has one.
    fn key(&self) -> Option<&str> {
        match self {
            Joining::Frame(Frame { batch: BatchMark::Event(key), .. }) => key.as_deref(),
            Joining::Frame(_) => None,
            Joining::Plain(plain) => plain.key(),
        }
    }

    /// The bytes of the serialized message, without a frame's header.
    fn bytes(&self) -> usize {
        match self {
            Joining::Frame(frame) => frame.bytes.len() - HEADER_LENGTH,
            Joining::Plain(plain) => plain.length,
        }
    }
}

/// One message waiting in a batch, its body and id, with what its send
/// asked for: its timeout, and, in a batch that waits for a connection with
/// nothing left to write, its deadline from its send (see
/// `Batch::waits_for_idle`).
pub(super) struct Record {
    pub(super) body: Vec<u8>,
    pub(super) id: u64,
    pub(super) timeout: Option<Duration>,
    pub(super) deadline: Option<Instant>,
    pub(super) done: Option<SendDone>,
    pub(super) tracker: Option<Arc<Tracker>>,
}

/// A batch open: its group's messages, in the order sent, and their bytes
/// without the frames' headers. With no delay a batch waits for a
/// connection with nothing left to write, a wait for room, which its
/// messages' timeouts bound from their sends: `due` is the earliest of
/// their deadlines, and the batch goes by it at the latest. With a delay
/// `due` is when the delay is over, and a message's time in the batch is
/// the batching's, its timeout counting from when the batch goes.
pub(super) struct Batch {
    pub(super) r#type: u32,
    pub(super) key: Option<String>,
    pub(super) batching: Batching,
    pub(super) records: Vec<Record>,
    bytes: usize,
    pub(super) waits_for_idle: bool,
    due: Option<Instant>,
}

/// What is batched, and the batches open (C++ BasicClient::Batcher).
#[derive(Default)]
pub(crate) struct Batcher {
    by_type: HashMap<u32, Batching>,
    by_key: HashMap<u32, HashMap<String, Batching>>,
    /// The batches open, by the order they began, found by their group
    /// through `unkeyed` and `keyed`; of those, the ones that wait for a
    /// connection with nothing left to write, and those with a time to go
    /// by in the order of it, one timer running to the first: each step a
    /// look-up, whatever their number.
    open: BTreeMap<u64, Batch>,
    unkeyed: HashMap<u32, u64>,
    keyed: HashMap<u32, HashMap<String, u64>>,
    began: u64,
    waiting_for_idle: BTreeSet<u64>,
    by_due: BTreeSet<(Instant, u64)>,
    timer: Option<TimerKey>,
    armed: Option<Instant>,
    /// The envelope every batch is built in, kept with its list of bodies.
    pub(super) envelope: BodiesEnvelope,
    /// The bodies handed back: what a synchronous client serializes its next
    /// plain sends into, or, on a threaded client's io thread, what its
    /// inbox hands back to the senders at its next take (`inbox::Inbox`).
    pub(crate) bodies: Bodies,
    /// The buffers of batch frames written, for the next batch frames.
    pub(super) frames: Arc<FramePool>,
}

impl Batcher {
    /// The open batch of a group.
    fn find(&self, r#type: u32, key: Option<&str>) -> Option<u64> {
        match key {
            None => self.unkeyed.get(&r#type).copied(),
            Some(key) => self.keyed.get(&r#type).and_then(|keys| keys.get(key)).copied(),
        }
    }

    /// Whether anything is batched.
    pub(crate) fn configured(&self) -> bool {
        !self.by_type.is_empty() || !self.by_key.is_empty()
    }
}

impl Core {
    /// For every key of `r#type`, each key's messages in batches of their
    /// own, or, given `key`, for that key, ahead of what is set for its
    /// type; every batch goes first. `InvalidArgument` as
    /// `check_batching()` says.
    pub(crate) fn set_batching(&mut self, r#type: u32, key: Option<&str>, batching: Batching) -> Result<()> {
        check_batching(r#type, key, &batching)?;
        self.close_batches();
        match key {
            None => {
                self.batcher.by_type.insert(r#type, batching);
            }
            Some(key) => {
                self.batcher.by_key.entry(r#type).or_default().insert(key.to_string(), batching);
            }
        }
        Ok(())
    }

    /// What `set_batching()` set for `r#type`, its keys' included, or,
    /// given `key`, for that key: no batching for them from now on; every
    /// batch goes first.
    pub(crate) fn clear_batching(&mut self, r#type: u32, key: Option<&str>) {
        self.close_batches();
        let batcher = &mut self.batcher;
        match key {
            None => {
                batcher.by_type.remove(&r#type);
                batcher.by_key.remove(&r#type);
            }
            Some(key) => {
                if let Some(keys) = batcher.by_key.get_mut(&r#type) {
                    keys.remove(key);
                    if keys.is_empty() {
                        batcher.by_key.remove(&r#type);
                    }
                }
            }
        }
    }

    /// Whether any message waits in a batch: a batch opens with its first.
    pub(crate) fn batches_open(&self) -> bool {
        !self.batcher.open.is_empty()
    }

    /// How many messages wait in batches; for tests.
    #[cfg(test)]
    pub(crate) fn batched_messages(&self) -> usize {
        self.batcher.open.values().map(|batch| batch.records.len()).sum()
    }

    /// How many bodies the kept envelope has room for; for tests.
    #[cfg(test)]
    pub(crate) fn envelope_room(&self) -> usize {
        self.batcher.envelope.batch.as_ref().map_or(0, |inside| inside.message.capacity())
    }

    /// How many batch frame buffers are kept for the next; for tests.
    #[cfg(test)]
    pub(crate) fn frame_buffers_kept(&self) -> usize {
        self.batcher.frames.len()
    }

    /// When the batch timer is armed to fire, if it is; for tests.
    #[cfg(test)]
    pub(crate) fn batcher_armed(&self) -> Option<Instant> {
        self.batcher.timer.and(self.batcher.armed)
    }

    /// What batching applies to `frame`, or None: the setting for its key,
    /// else for its type, when it is a plain event.
    pub(super) fn batching_for(&self, frame: &Frame) -> Option<Batching> {
        let BatchMark::Event(key) = &frame.batch else { return None };
        self.batching_of(frame.r#type, key.as_deref())
    }

    /// What batching applies to a plain event of `r#type` and `key`, or
    /// None: the setting for its key, else for its type.
    pub(crate) fn batching_of(&self, r#type: u32, key: Option<&str>) -> Option<Batching> {
        let batcher = &self.batcher;
        if batcher.by_type.is_empty() && batcher.by_key.is_empty() {
            return None; // nothing batched: no look-up per message
        }
        if let Some(key) = key {
            if let Some(batching) = batcher.by_key.get(&r#type).and_then(|keys| keys.get(key)) {
                return Some(*batching);
            }
        }
        batcher.by_type.get(&r#type).copied()
    }

    /// Whether a synchronous client's send of `frame` must run the loop
    /// first, as `SyncClient::queue` does before choosing a connection, so
    /// that one the multiplexer closed meanwhile is retired first: always,
    /// but for a message that only joins a batch with a delay, which chooses
    /// none, while that batch has room for it and no batch's delay is over
    /// by `now`, which only the loop's timer lets go. Such a call runs no
    /// loop, a system call spared for each message (C++
    /// BasicClient::_batch_needs_loop).
    pub(crate) fn send_needs_loop(
        &self,
        frame: &Frame,
        all: bool,
        lane: bool,
        timeout: Option<Duration>,
        now: Instant,
    ) -> bool {
        let BatchMark::Event(key) = &frame.batch else { return true };
        let bytes = frame.bytes.len() - HEADER_LENGTH;
        self.needs_loop_for(frame.r#type, key.as_deref(), bytes, all, lane, timeout, now)
    }

    /// `send_needs_loop` for a plain event of `r#type` and `key` whose
    /// serialized message has `bytes`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn needs_loop_for(
        &self,
        r#type: u32,
        key: Option<&str>,
        bytes: usize,
        all: bool,
        lane: bool,
        timeout: Option<Duration>,
        now: Instant,
    ) -> bool {
        if all || lane || timeout == Some(Duration::ZERO) {
            return true;
        }
        let Some(batching) = self.batching_of(r#type, key) else { return true };
        if batching.max_delay.is_zero() || self.batcher.by_due.first().is_some_and(|(due, _)| *due <= now) {
            return true;
        }
        let (records, held) = self
            .batcher
            .find(r#type, key)
            .map_or((0, 0), |began| (self.batcher.open[&began].records.len(), self.batcher.open[&began].bytes));
        records + 1 >= batching.max_records || held + bytes >= batching.max_bytes
    }

    /// A message into the open batch of its group, or one it opens; with no
    /// room there, that batch goes first. A message too big for a batch,
    /// or, with no delay, sent while a connection is free to write it or
    /// none is live, goes alone, at once, after every batch: a frame as it
    /// is, a plain send as a frame made of its bytes. Its tracker, when the
    /// caller keeps them, reads QUEUED until its batch ends.
    pub(crate) fn batch(
        &mut self,
        joining: Joining,
        batching: Batching,
        timeout: Option<Duration>,
        trackers: Option<&mut Vec<Arc<Tracker>>>,
        done: Option<SendDone>,
    ) -> bool {
        let bytes = joining.bytes();
        let r#type = joining.r#type();
        let key = joining.key();
        let mut open = self.batcher.find(r#type, key);
        if let Some(began) = open {
            let batch = &self.batcher.open[&began];
            if batch.records.len() >= batching.max_records || batch.bytes + bytes > batching.max_bytes {
                self.close_on_its_own(began);
                open = None;
            }
        }
        let began = match open {
            Some(began) => began,
            None => {
                if bytes >= batching.max_bytes || (batching.max_delay.is_zero() && self.idle_or_none()) {
                    self.close_batches();
                    let deadline = deadline_after(self.now, timeout);
                    let frame = match joining {
                        Joining::Frame(frame) => frame,
                        Joining::Plain(plain) => self.frame_of_plain(plain),
                    };
                    return self.send_at(frame, false, None, deadline, 0, trackers, None, done);
                }
                self.open_batch(r#type, key.map(str::to_string), batching)
            }
        };
        let (body, id) = match joining {
            Joining::Plain(plain) => {
                let id = plain.id;
                (plain.into_body(), id)
            }
            Joining::Frame(frame) => {
                // A frame's body, copied into a body kept: its own frame is
                // never written.
                let mut body = self.batcher.bodies.take();
                body.extend_from_slice(&frame.bytes[HEADER_LENGTH..]);
                (body, frame.id)
            }
        };
        let tracker = trackers.map(|trackers| {
            let tracker = Tracker::new(SendState::Queued);
            trackers.push(Arc::clone(&tracker));
            tracker
        });
        // A wait for room, in a batch with no delay: its deadline runs from now.
        let deadline = if self.batcher.open[&began].waits_for_idle { deadline_after(self.now, timeout) } else { None };
        if let Some(deadline) = deadline {
            if self.batcher.open[&began].due.is_none_or(|due| deadline < due) {
                self.due_at(began, deadline);
            }
        }
        let batch = self.batcher.open.get_mut(&began).expect("the batch is open");
        batch.bytes += bytes;
        batch.records.push(Record { body, id, timeout, deadline, done, tracker });
        if batch.records.len() >= batching.max_records || batch.bytes >= batching.max_bytes {
            self.close_on_its_own(began);
        }
        true
    }

    /// A new batch for a group, its delay's end armed, or waiting for a
    /// connection with nothing left to write.
    fn open_batch(&mut self, r#type: u32, key: Option<String>, batching: Batching) -> u64 {
        let batcher = &mut self.batcher;
        batcher.began += 1;
        let began = batcher.began;
        match &key {
            None => {
                batcher.unkeyed.insert(r#type, began);
            }
            Some(key) => {
                batcher.keyed.entry(r#type).or_default().insert(key.clone(), began);
            }
        }
        let waits_for_idle = batching.max_delay.is_zero();
        batcher
            .open
            .insert(began, Batch { r#type, key, batching, records: Vec::new(), bytes: 0, waits_for_idle, due: None });
        if waits_for_idle {
            self.batcher.waiting_for_idle.insert(began);
            self.wait_for_idle();
        } else {
            self.due_at(began, self.now + batching.max_delay);
        }
        began
    }

    /// The batch that began `began` goes at `due` at the latest, the timer
    /// moved to it when it is the earliest.
    fn due_at(&mut self, began: u64, due: Instant) {
        let batcher = &mut self.batcher;
        let Some(batch) = batcher.open.get_mut(&began) else { return };
        if let Some(before) = batch.due.replace(due) {
            batcher.by_due.remove(&(before, began));
        }
        batcher.by_due.insert((due, began));
        if batcher.armed.is_none_or(|armed| due < armed) {
            self.arm_batch_timer();
        }
    }

    /// Every batch goes now, in the order they began: what a flush, a send
    /// that does not batch and a shutdown do first.
    pub(crate) fn close_batches(&mut self) {
        while let Some((&began, _)) = self.batcher.open.first_key_value() {
            self.close_batch(began, None);
        }
    }

    /// A batch goes on its own account, full, at its delay or at its
    /// deadline, and with it every batch that waits for a connection with
    /// nothing left to write, which would only be written after it, in the
    /// order they began: while batches keep going, a group with fewer
    /// messages waits no longer than until the next one goes.
    fn close_on_its_own(&mut self, began: u64) {
        let mut going: Vec<u64> = self.batcher.waiting_for_idle.iter().copied().collect();
        if !self.batcher.waiting_for_idle.contains(&began) {
            going.push(began);
            going.sort_unstable();
        }
        for each in going {
            self.close_batch(each, None);
        }
    }

    /// The batch that began `began` goes: out of the open ones, the timer
    /// stopped with the last batch that has a time to go by, so that
    /// nothing waits for it; then sent, on `idle` when given, a connection
    /// that has nothing left to write.
    fn close_batch(&mut self, began: u64, idle: Option<ConnId>) {
        let batcher = &mut self.batcher;
        let Some(batch) = batcher.open.remove(&began) else { return };
        match &batch.key {
            None => {
                batcher.unkeyed.remove(&batch.r#type);
            }
            Some(key) => {
                if let Some(keys) = batcher.keyed.get_mut(&batch.r#type) {
                    keys.remove(key);
                    if keys.is_empty() {
                        batcher.keyed.remove(&batch.r#type);
                    }
                }
            }
        }
        if batch.waits_for_idle {
            batcher.waiting_for_idle.remove(&began);
        }
        if let Some(due) = batch.due {
            batcher.by_due.remove(&(due, began));
            if batcher.by_due.is_empty() {
                self.timers.cancel(&mut self.batcher.timer);
                self.batcher.armed = None;
            }
        }
        self.send_batch(batch, idle);
    }

    /// A connection with nothing queued, written or waiting for room, or
    /// none live at all, which a message would then be held for.
    fn idle_or_none(&self) -> bool {
        let mut live = false;
        for conn in self.by_peer_id.values() {
            let Some(state) = self.conns.get(conn) else { continue };
            if !state.living() {
                continue;
            }
            live = true;
            if state.outgoing_queue_empty() && !self.outbox.waits_for(*conn) {
                return true;
            }
        }
        !live
    }

    /// Every live connection says when it has nothing left to write, for
    /// the batches that wait for that.
    fn wait_for_idle(&mut self) {
        let registered: Vec<_> = self.by_peer_id.values().copied().collect();
        for conn in registered {
            if let Some(state) = self.conns.get_mut(&conn) {
                if state.living() {
                    state.notify_when_idle();
                }
            }
        }
    }

    /// A connection that was asked to says it has nothing left to write:
    /// the batches waiting for that go, on it.
    pub(crate) fn outgoing_queue_idle(&mut self, conn: ConnId) {
        self.close_idle_batches(Some(conn));
    }

    /// The batches that wait for a connection with nothing left to write
    /// go, in the order they began, on `idle` when given: when one has,
    /// when one came up, and when none is live any more.
    pub(crate) fn close_idle_batches(&mut self, idle: Option<ConnId>) {
        let waiting: Vec<u64> = self.batcher.waiting_for_idle.iter().copied().collect();
        for began in waiting {
            self.close_batch(began, idle);
        }
    }

    /// What a dead connection handed over was placed: a batch that waited
    /// for a busy connection to finish waits no more once none is busy or
    /// none is live.
    pub(crate) fn close_idle_batches_if_idle(&mut self) {
        if !self.batcher.waiting_for_idle.is_empty() && self.idle_or_none() {
            self.close_idle_batches(None);
        }
    }

    /// The timer runs to the earliest time to go by among the batches open.
    fn arm_batch_timer(&mut self) {
        let Some(&(earliest, _)) = self.batcher.by_due.first() else { return };
        if self.batcher.armed.is_some_and(|armed| armed <= earliest) {
            return;
        }
        self.batcher.armed = Some(earliest);
        self.timers.rearm(&mut self.batcher.timer, earliest, Due::Batches);
    }

    /// The batches whose time is up go, first come first, each on its own
    /// account; the timer runs on to the next.
    pub(crate) fn batch_timer_fired(&mut self) {
        self.batcher.timer = None;
        self.batcher.armed = None;
        let now = self.now;
        let due: Vec<u64> =
            self.batcher.by_due.iter().take_while(|(due, _)| *due <= now).map(|(_, began)| *began).collect();
        for began in due {
            self.close_on_its_own(began);
        }
        self.arm_batch_timer();
    }

    /// The client shuts down: every batch goes first, and the timer stops.
    pub(crate) fn stop_batching(&mut self) {
        self.close_batches();
        self.timers.cancel(&mut self.batcher.timer);
        self.batcher.armed = None;
    }
}
