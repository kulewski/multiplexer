//! A batch as it goes, and what a client's batching keeps from one batch to
//! the next, so that a batch costs no allocation for each of its messages.
//! A batch goes as frames (`Core::send_batch`): as many of its messages as
//! fit in its max_bytes in one envelope, then the next, a message alone as
//! itself. What is kept: the bodies, the byte vectors plain sends were
//! serialized into, handed back once their batch's frame is made, for the
//! senders' next plain sends (`Bodies`); the envelope every batch is built
//! in, kept with its list of bodies (`BodiesEnvelope`); and the buffers of
//! the batch frames written, which the next batch frames are serialized
//! into (`FramePool`). Mirrors the C++ library's SpentBodies, the kept
//! BatchEnvelope of its batching and its BatchFramePool, with C++'s caps:
//! bodies of 64 KiB at most are kept, 16 MiB of them, and 4 frame buffers
//! of 4 MiB at most; what is over is freed.
//!
//! A frame's buffer goes back to its pool when the last holder of the frame
//! lets go of it, wherever that is, hence the pool's lock: a batch frame is
//! a `Bytes` whose owner (`PooledFrame`) gives the buffer back as it drops.

use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use prost::Message as _;

use super::batching::{Batch, BatchMark, Record};
use super::lane::Lane;
use super::outbox::{Frame, SendDone};
use super::{deadline_after, ConnId, Core, SendState, Tracker};
use crate::frame::{self, frame_of_body, Head};

/// The largest body kept for another send: bigger ones are freed.
pub(crate) const BODY_KEPT_MAX: usize = 64 * 1024;

/// The bodies' capacity a client keeps at most, every body counted.
pub(crate) const BODIES_KEPT_BYTES: usize = 16 * 1024 * 1024;

/// The batch frames' buffers kept at most.
pub(crate) const FRAME_BUFFERS_KEPT: usize = 4;

/// The largest batch frame buffer kept: bigger ones are freed.
pub(crate) const FRAME_BUFFER_KEPT_MAX: usize = 4 * 1024 * 1024;

/// Locks `mutex`, which nothing panics under.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Bodies kept for the next plain sends, emptied, their capacity counted
/// against BODIES_KEPT_BYTES.
#[derive(Debug, Default)]
pub(crate) struct Bodies {
    kept: Vec<Vec<u8>>,
    bytes: usize,
}

impl Bodies {
    /// Keeps `body`, emptied, when it is at most BODY_KEPT_MAX and the
    /// bodies kept leave it room; frees it otherwise.
    pub(crate) fn give(&mut self, mut body: Vec<u8>) {
        let capacity = body.capacity();
        if capacity == 0 || capacity > BODY_KEPT_MAX || self.bytes + capacity > BODIES_KEPT_BYTES {
            return;
        }
        body.clear();
        self.bytes += capacity;
        self.kept.push(body);
    }

    /// A body kept, empty, or a new one, which has nothing allocated yet.
    pub(crate) fn take(&mut self) -> Vec<u8> {
        match self.kept.pop() {
            Some(body) => {
                self.bytes -= body.capacity();
                body
            }
            None => Vec::new(),
        }
    }

    /// Every body `other` kept, kept here as `give` keeps them.
    pub(crate) fn take_all(&mut self, other: &mut Bodies) {
        for body in other.kept.drain(..) {
            self.give(body);
        }
        other.bytes = 0;
    }

    /// How many bodies are kept.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.kept.len()
    }

    /// Whether no body is kept.
    pub(crate) fn is_empty(&self) -> bool {
        self.kept.is_empty()
    }

    /// Where the bodies kept are, the last given first; for tests.
    #[cfg(test)]
    pub(crate) fn pointers(&self) -> Vec<*const u8> {
        self.kept.iter().rev().map(|body| body.as_ptr()).collect()
    }

    /// The capacity of the bodies kept, in bytes.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

/// The generated `BatchEnvelope` (multiplexer/Multiplexer.proto) as this
/// client writes it from bodies: the same fields, numbers and types, but
/// the batch's messages as the vectors they were serialized into
/// (`bytes = "vec"`), which the generated type's `bytes::Bytes` would take
/// only with an allocation of its own for each. prost's derive writes it,
/// as it writes the generated types; a test checks that it writes what the
/// generated envelope writes.
#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct BodiesEnvelope {
    #[prost(uint64, optional, tag = "1")]
    pub(crate) id: Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    pub(crate) sender: Option<u64>,
    #[prost(uint32, required, tag = "4")]
    pub(crate) r#type: u32,
    #[prost(string, optional, tag = "9")]
    pub(crate) key: Option<String>,
    #[prost(message, optional, tag = "10")]
    pub(crate) batch: Option<BodiesBatch>,
}

/// The generated `MessageBatch` with its messages as vectors; see
/// `BodiesEnvelope`.
#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct BodiesBatch {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub(crate) message: Vec<Vec<u8>>,
}

/// The buffers of batch frames written, for the next batch frames (C++
/// FramePool): FRAME_BUFFERS_KEPT at most, of FRAME_BUFFER_KEPT_MAX each at
/// most.
#[derive(Debug, Default)]
pub(crate) struct FramePool(Mutex<Vec<Vec<u8>>>);

impl FramePool {
    /// A buffer for a frame: one kept, emptied, or a new one.
    pub(crate) fn take(&self) -> Vec<u8> {
        lock(&self.0).pop().unwrap_or_default()
    }

    /// `buffer`, a whole frame, as the `Bytes` every holder of the frame
    /// shares; the last to let go gives the buffer back here.
    pub(crate) fn frame(self: &Arc<Self>, buffer: Vec<u8>) -> Bytes {
        Bytes::from_owner(PooledFrame { buffer, pool: Arc::clone(self) })
    }

    /// Keeps `buffer`, emptied, when the pool has room for it and it is at
    /// most FRAME_BUFFER_KEPT_MAX; frees it otherwise, outside the lock.
    fn give(&self, mut buffer: Vec<u8>) {
        if buffer.capacity() == 0 || buffer.capacity() > FRAME_BUFFER_KEPT_MAX {
            return;
        }
        let mut kept = lock(&self.0);
        if kept.len() < FRAME_BUFFERS_KEPT {
            buffer.clear();
            kept.push(buffer);
        }
    }

    /// How many buffers are kept.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        lock(&self.0).len()
    }
}

/// A batch frame's buffer, back to its pool when the frame's last holder
/// lets go of it.
struct PooledFrame {
    buffer: Vec<u8>,
    pool: Arc<FramePool>,
}

impl AsRef<[u8]> for PooledFrame {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}

impl Drop for PooledFrame {
    fn drop(&mut self) {
        self.pool.give(std::mem::take(&mut self.buffer));
    }
}

impl Core {
    /// A batch as frames: an envelope of its type and key holding as many
    /// of its messages as fit in max_bytes, measured as it goes on the
    /// wire, then the next, a message alone going as itself. Each frame
    /// waits for room and for a connection until the earliest of its
    /// messages' deadlines: from their sends in a batch that waited for a
    /// connection with nothing left to write, from now in one that waited
    /// for its delay. On `idle`, when given and live, rather than where the
    /// round robin points, which may be a connection still writing.
    pub(super) fn send_batch(&mut self, batch: Batch, idle: Option<ConnId>) {
        let Batch { r#type, mut key, batching, mut records, waits_for_idle, .. } = batch;
        let lane = idle.filter(|conn| self.conn_living(*conn)).map(|conn| Lane::seeded(&self.wrap(conn), false));
        let now = self.now;
        while !records.is_empty() {
            let mut end = records.len();
            let mut envelope_frame = None;
            if end > 1 {
                let id = self.random64();
                let (fit, bytes) = self.batch_frame(id, r#type, &mut key, &mut records, batching.max_bytes);
                end = fit;
                if let Some(bytes) = bytes {
                    let ids: Arc<[u64]> = records[..end].iter().map(|record| record.id).collect();
                    envelope_frame = Some(Frame { bytes, head: None, id, r#type, batch: BatchMark::Batch(ids) });
                }
            }
            let taken: Vec<Record> = match envelope_frame {
                Some(_) => records.drain(..end).collect(),
                None => records.drain(..1).collect(), // a batch of one goes as the message itself
            };
            let deadline = taken
                .iter()
                .filter_map(|record| if waits_for_idle { record.deadline } else { deadline_after(now, record.timeout) })
                .min();
            let (frame, done) = match envelope_frame {
                Some(frame) => {
                    let ends: Vec<(Option<SendDone>, Option<Arc<Tracker>>)> =
                        taken.into_iter().map(|record| (record.done, record.tracker)).collect();
                    let followed = ends.iter().any(|(done, tracker)| done.is_some() || tracker.is_some());
                    (frame, followed.then_some(SendDone::Batch(ends)))
                }
                None => {
                    let record = taken.into_iter().next().expect("one message");
                    let done = match record.tracker {
                        None => record.done,
                        Some(tracker) => Some(SendDone::Batch(vec![(record.done, Some(tracker))])),
                    };
                    let (head, bytes) = self.alone_frame(record.body);
                    (Frame { bytes, head, id: record.id, r#type, batch: BatchMark::None }, done)
                }
            };
            self.send_at(frame, false, lane.as_ref(), deadline, 0, None, None, done);
        }
    }

    /// The frame of a batch of `records`, in an envelope of id `id`,
    /// `r#type` and `key` holding the first of them that fit in `max_bytes`
    /// on the wire: how many, and the frame, made in a buffer of the frame
    /// pool, or None when it would hold fewer than two or could not be
    /// made. The kept envelope takes the records' bodies without a copy;
    /// those the frame holds go back to the bodies kept, the others to
    /// their records.
    fn batch_frame(
        &mut self,
        id: u64,
        r#type: u32,
        key: &mut Option<String>,
        records: &mut [Record],
        max_bytes: usize,
    ) -> (usize, Option<bytes::Bytes>) {
        let mut envelope = std::mem::take(&mut self.batcher.envelope);
        envelope.id = Some(id);
        envelope.sender = Some(self.instance_id);
        envelope.r#type = r#type;
        envelope.key = key.take();
        let inside = envelope.batch.get_or_insert_with(BodiesBatch::default);
        inside.message.extend(records.iter_mut().map(|record| std::mem::take(&mut record.body)));
        // Where the message would be over max_bytes, measured as it goes on
        // the wire, its last messages are cut, to go next: at once as many
        // as their own sizes cover the excess with, which each adds at least
        // to the message, and the rest measured again, nothing encoded until
        // the frame is made.
        let mut end = records.len();
        let mut size = envelope.encoded_len();
        while end > 1 && size > max_bytes {
            let excess = size - max_bytes;
            let mut cut = 0;
            let inside = envelope.batch.as_mut().expect("the envelope's batch");
            while end > 1 && cut < excess {
                let body = inside.message.pop().expect("a body for each record");
                cut += body.len();
                records[end - 1].body = body;
                end -= 1;
            }
            size = envelope.encoded_len();
        }
        let frame = if end > 1 {
            frame::encode_into(&envelope, envelope.key.as_deref(), self.batcher.frames.take())
                .ok()
                .map(|buffer| self.batcher.frames.frame(buffer))
        } else {
            None
        };
        let inside = envelope.batch.as_mut().expect("the envelope's batch");
        match frame {
            Some(_) => {
                for body in inside.message.drain(..) {
                    self.batcher.bodies.give(body);
                }
            }
            None => {
                // Each goes alone, as itself: the bodies back to their records.
                for (record, body) in records.iter_mut().zip(inside.message.drain(..)) {
                    record.body = body;
                }
            }
        }
        *key = envelope.key.take();
        self.batcher.envelope = envelope;
        (end, frame)
    }

    /// The frame `body`, a serialized message, goes alone as, sealed: a body
    /// over BODY_KEPT_MAX, which the bodies kept would not take back, is the
    /// frame's own bytes, behind its header kept apart, written with it in
    /// one call, so that it is not copied, as C++ swaps a body into its
    /// frame; a smaller one is copied behind its header and goes back to the
    /// bodies kept, for the next plain sends.
    pub(crate) fn alone_frame(&mut self, body: Vec<u8>) -> (Option<Head>, Bytes) {
        if body.capacity() > BODY_KEPT_MAX {
            return (Some(frame::head_of(&body)), Bytes::from(body));
        }
        let bytes = frame_of_body(&body);
        self.batcher.bodies.give(body);
        (None, bytes)
    }

    /// A batched send's end, each of its messages': its tracker reads SENT
    /// or LOST, and its `done` hears `written`, in the order they were sent.
    pub(crate) fn end_batch(&mut self, ends: Vec<(Option<SendDone>, Option<Arc<Tracker>>)>, written: u32) {
        for (done, tracker) in ends {
            if let Some(tracker) = tracker {
                if tracker.get() == SendState::Queued {
                    tracker.set(if written > 0 { SendState::Sent } else { SendState::Lost });
                }
            }
            if let Some(done) = done {
                self.end_send(done, written);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! The caps on what is kept, the frame buffers' way back, and the
    //! envelope written as the generated one is.

    use super::{Bodies, BodiesBatch, BodiesEnvelope, FramePool, BODIES_KEPT_BYTES, BODY_KEPT_MAX};
    use super::{FRAME_BUFFERS_KEPT, FRAME_BUFFER_KEPT_MAX};
    use crate::proto::multiplexer::{BatchEnvelope, MessageBatch};
    use bytes::Bytes;
    use prost::Message as _;
    use std::sync::Arc;

    #[test]
    fn bodies_are_kept_up_to_their_caps_and_come_back_empty() {
        let mut bodies = Bodies::default();
        let mut body = Vec::with_capacity(100);
        body.extend_from_slice(b"spent");
        bodies.give(body);
        bodies.give(Vec::with_capacity(BODY_KEPT_MAX + 1)); // too big: freed
        bodies.give(Vec::new()); // nothing to keep
        assert_eq!(1, bodies.len());
        let taken = bodies.take();
        assert!(taken.is_empty() && taken.capacity() >= 100, "emptied, its capacity kept");
        assert_eq!(0, bodies.bytes());
        assert_eq!(0, bodies.take().capacity(), "none left: a new one, nothing allocated");
        let count = BODIES_KEPT_BYTES / BODY_KEPT_MAX;
        for _ in 0..count + 3 {
            bodies.give(Vec::with_capacity(BODY_KEPT_MAX));
        }
        assert_eq!(count, bodies.len(), "16 MiB of them at most");
        let mut other = Bodies::default();
        other.give(Vec::with_capacity(BODY_KEPT_MAX));
        bodies.take_all(&mut other);
        assert!(other.is_empty());
        assert_eq!(count, bodies.len(), "past the cap: freed");
        assert!(bodies.bytes() <= BODIES_KEPT_BYTES);
    }

    #[test]
    fn a_frames_buffer_goes_back_to_its_pool_when_its_last_holder_lets_go() {
        let pool = Arc::new(FramePool::default());
        let mut buffer = pool.take();
        buffer.extend_from_slice(&[7; 1000]);
        let pointer = buffer.as_ptr();
        let frame = pool.frame(buffer);
        let copy = frame.clone();
        drop(frame);
        assert_eq!(0, pool.len(), "held still");
        assert_eq!(&[7; 1000][..], &copy[..]);
        drop(copy);
        assert_eq!(1, pool.len(), "back with the last holder");
        let again = pool.take();
        assert_eq!(pointer, again.as_ptr(), "the same buffer, for the next frame");
        assert!(again.is_empty());
        for _ in 0..FRAME_BUFFERS_KEPT + 2 {
            drop(pool.frame(vec![0; 10]));
        }
        assert_eq!(FRAME_BUFFERS_KEPT, pool.len(), "four at most");
        let pool = Arc::new(FramePool::default());
        drop(pool.frame(vec![0; FRAME_BUFFER_KEPT_MAX + 1]));
        assert_eq!(0, pool.len(), "too big to keep");
    }

    #[test]
    fn a_bodies_envelope_writes_what_the_generated_envelope_writes() {
        let bodies = vec![b"one".to_vec(), Vec::new(), b"three".to_vec()];
        let ours = BodiesEnvelope {
            id: Some(1),
            sender: Some(2),
            r#type: 300,
            key: Some("key".to_string()),
            batch: Some(BodiesBatch { message: bodies.clone() }),
        };
        let generated = BatchEnvelope {
            id: Some(1),
            sender: Some(2),
            r#type: 300,
            key: Some("key".to_string()),
            batch: Some(MessageBatch { message: bodies.into_iter().map(Bytes::from).collect() }),
        };
        assert_eq!(generated.encode_to_vec(), ours.encode_to_vec());
        assert_eq!(generated.encoded_len(), ours.encoded_len());
        // The same fields: a field added to the generated envelope and not
        // here fails this, as message.rs's conversions are checked.
        let fields = |text: String| text.matches(": ").count();
        assert_eq!(
            fields(format!("{:?}", BatchEnvelope::default())),
            fields(format!("{:?}", BodiesEnvelope::default()))
        );
        assert_eq!(fields(format!("{:?}", MessageBatch::default())), fields(format!("{:?}", BodiesBatch::default())));
    }
}
