//! A plain send as bytes: a send nobody follows (no `on_end`, no wait),
//! through no lane and not to ALL, of a plain event (`plain_event`), made
//! while the client batches, reaches the batching as the bytes it was
//! serialized into on the sender's thread, with its id, type and key, and
//! with no frame or boxed command of its own (`Plain`). The batching puts
//! the bytes in their batch as they are (`Core::send_plain`); a frame is
//! made of them only when the message goes alone: its type not batched, a
//! batch of one, too big, no delay and a connection free, no time to wait,
//! or cut from a batch, a big one's own buffer then the frame's body, its
//! header kept apart (`Core::alone_frame`). Once a batch's frame is made,
//! its bodies are handed back for the next plain sends (`bodies`). Mirrors
//! C++ BasicClient::send_plain.
//!
//! A `ThreadedClient` posts a plain send to its io thread as data, the
//! deadline counted from the send (`Posted::Plain`, `Core::place_plain`);
//! a `SyncClient` hands it over in its own call. Either way the sender
//! serializes into a body the batching handed back, so that in a steady
//! stream nothing is allocated for a message on either side, and nothing
//! one thread allocated is freed on another.

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
    /// How every client sends a plain send: into the open batch of its type
    /// and key, or one it opens, when its type or key is batched and it has
    /// time to wait; otherwise alone, as a frame made of its bytes, after
    /// every batch, numbered `number`, 0 for a fresh one. False, its body
    /// kept, when the client is shutting down. Its body goes back to the
    /// bodies kept once a frame holds it.
    pub(crate) fn send_plain(&mut self, plain: Plain, timeout: Option<Duration>, number: u64) -> bool {
        if self.client.shuts_down {
            self.batcher.bodies.give(plain.body);
            return false;
        }
        // One with no time to wait goes alone: in a batch it would leave the
        // batch no time either.
        if timeout != Some(Duration::ZERO) {
            if let Some(batching) = self.batching_of(plain.r#type, plain.key()) {
                return self.batch(Joining::Plain(plain), batching, timeout, None, None);
            }
        }
        self.close_batches();
        let deadline = deadline_after(self.now, timeout);
        let frame = self.frame_of_plain(plain);
        self.send_at(frame, false, None, deadline, number, None, None, None)
    }

    /// Whether a synchronous client's plain send must run the loop first,
    /// as `send_needs_loop` says for a frame.
    pub(crate) fn plain_needs_loop(&self, plain: &Plain, timeout: Option<Duration>, now: Instant) -> bool {
        self.needs_loop_for(plain.r#type, plain.key(), plain.length, false, false, timeout, now)
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
    /// read for the commands it runs. After the shutdown's end nothing is
    /// placed: the message is reported dropped, as `place_send` does.
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
