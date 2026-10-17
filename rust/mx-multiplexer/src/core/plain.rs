//! A plain send as bytes: a send nobody follows (no `on_end`, no wait),
//! through no lane and not to ALL, of a plain event (`plain_event`), made
//! while a delivery is set, reaches the batching as the bytes it was
//! serialized into on the sender's thread, with its id, type and key, and
//! with no frame or boxed command of its own (`Plain`). The batching puts
//! the bytes in their batch as they are (`Core::send_plain`); a frame is
//! made of them only when the message goes alone: its type with no
//! delivery, or one that does not batch, a batch of one, too big, no delay
//! and a connection free, or cut from a batch, a big one's own buffer then
//! the frame's body, its header kept apart (`Core::alone_frame`). Once a
//! batch's frame is made, its bodies are handed back for the next plain
//! sends (`bodies`). Mirrors C++ BasicClient::send_plain, a delivery's
//! timeout used over the send's as there.
//!
//! A `ThreadedClient` posts a plain send to its io thread as data, the
//! deadline counted from the send, which a delivery's own takes the place
//! of (`Posted::Plain`, `Core::place_plain`); a `SyncClient` hands it over
//! in its own call. Either way the sender serializes into a body the
//! batching handed back, so that in a steady stream nothing is allocated
//! for a message on either side, and nothing one thread allocated is freed
//! on another.

use std::time::{Duration, Instant};

use super::batching::Joining;
use super::client::DropReason;
use super::outbox::Frame;
use super::{deadline_after, Core, OutFrame};

/// A plain send as bytes: the message serialized, then its key's bytes,
/// which the batching groups it by, the message's own copy staying inside
/// it; the message's length, its id and type, and whether it has a key,
/// an empty one being a key.
#[derive(Debug)]
pub(crate) struct Plain {
    pub(crate) body: Vec<u8>,
    pub(crate) length: usize,
    pub(crate) keyed: bool,
    pub(crate) id: u64,
    pub(crate) r#type: u32,
}

impl Plain {
    /// Its key, when it has one: the bytes after the message, a `String`'s
    /// once, so UTF-8.
    pub(crate) fn key(&self) -> Option<&str> {
        if !self.keyed {
            return None;
        }
        std::str::from_utf8(&self.body[self.length..]).ok()
    }

    /// The body as a batch holds it: the serialized message alone.
    pub(crate) fn into_body(mut self) -> Vec<u8> {
        self.body.truncate(self.length);
        self.body
    }
}

impl Core {
    /// How every client sends a plain send: by the delivery set for its
    /// type or key, into the open batch of its type and key, or one it
    /// opens, or kept until confirmed, when the delivery batches or
    /// confirms, `timeout` not used; otherwise alone, as a frame made of its
    /// bytes, after every batch, by a delivery's timeout, else `timeout`,
    /// numbered `number`, 0 for a fresh one. False, its body kept, when the
    /// client is shutting down. Its body goes back to the bodies kept once
    /// a frame holds it.
    pub(crate) fn send_plain(&mut self, plain: Plain, mut timeout: Option<Duration>, number: u64) -> bool {
        if self.client.shuts_down {
            self.batcher.bodies.give(plain.body);
            return false;
        }
        if let Some(delivery) = self.delivery_of(plain.r#type, plain.key()) {
            if delivery.batch || delivery.confirm {
                return self.batch(Joining::Plain(plain), delivery, None, None);
            }
            timeout = delivery.timeout; // a delivery that sends it as it is, by its own timeout
        }
        self.close_batches();
        let deadline = deadline_after(self.now, timeout);
        let frame = self.frame_of_plain(plain);
        self.send_at(frame, false, None, deadline, number, None, None, None)
    }

    /// Whether a synchronous client's plain send must run the loop first,
    /// as `send_needs_loop` says for a frame.
    pub(crate) fn plain_needs_loop(&self, plain: &Plain, now: Instant) -> bool {
        self.needs_loop_for(plain.r#type, plain.key(), plain.length, false, false, now)
    }

    /// The frame a plain send goes alone as, made of its bytes
    /// (`alone_frame`).
    pub(crate) fn frame_of_plain(&mut self, plain: Plain) -> Frame {
        let (id, r#type) = (plain.id, plain.r#type);
        let (head, bytes) = self.alone_frame(plain.into_body());
        Frame { bytes, head, id, r#type, batch: super::batching::BatchMark::None }
    }

    /// A threaded client's plain send on its io thread: numbered and handed
    /// to the batching (`send_plain`), what it has left of the time until
    /// `deadline`, counted from the send, reckoned at the time the driver
    /// read for the commands it runs, its timeout unless a delivery's takes
    /// its place. After the shutdown's end nothing is placed: the message is
    /// reported dropped, as `place_send` does.
    pub(crate) fn place_plain(&mut self, plain: Plain, deadline: Option<Instant>) {
        let number = self.next_number();
        // What a drop report reads (`place_send`'s).
        let report = OutFrame {
            bytes: bytes::Bytes::new(),
            head: None,
            id: plain.id,
            r#type: plain.r#type,
            pinned: false,
            for_all: false,
            own: false,
            number,
            deadline: None,
            batch: None,
            confirmed: false,
        };
        if self.threaded.torn_down {
            self.report_drop(&report, DropReason::ShutDown);
            self.batcher.bodies.give(plain.body);
            return;
        }
        let timeout = deadline.map(|deadline| deadline.saturating_duration_since(self.now));
        if !self.send_plain(plain, timeout, number) {
            self.report_drop(&report, DropReason::ConnectionLost);
        }
    }
}
