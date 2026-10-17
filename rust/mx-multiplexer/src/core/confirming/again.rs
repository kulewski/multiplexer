//! Confirmed sends, what goes again and when (`super`, docs/delivery.md
//! "Confirmed sends"): the unresolved messages of a batch go again, each
//! keeping its id, number and end, its bytes read back out of the kept
//! frame, in a batch of their own that goes at once with the deadline their
//! batch had (`send_again`): refused, at once; nobody took their batch, a
//! DELIVERY_ERROR, after a back-off from 0.1 s doubling to 1 s, carried on
//! to the batch sent again and over once a confirmation comes; the
//! connection their batch was written on closed, at once; nothing confirmed
//! any of them for retry_after, then. Past the batch's deadline what would
//! go again is given up on instead. One timer runs to the earliest of every
//! batch's retry time and deadline, what is left of a batch given up on at
//! its deadline, and stops once no batch waits, so that a synchronous wait
//! with nothing to wait for does not wait it out. Mirrors those parts of
//! multiplexer/basic_client_confirm.cc.

use std::time::Duration;

use prost::Message as _;

use super::super::batching::{Batch, Record};
use super::super::client::DropReason;
use super::super::{ConnId, Core, Due, SendState};
use super::earliest;
use crate::frame::{self, HEADER_LENGTH};
use crate::proto::multiplexer::MessageBatch;

/// The back-off of a batch nobody took: its first wait, doubled at every
/// DELIVERY_ERROR up to the second.
const BACKOFF_FIRST: Duration = Duration::from_millis(100);
const BACKOFF_MOST: Duration = Duration::from_secs(1);

impl Core {
    /// The unresolved messages `indexes` of the batch of envelope `batch`
    /// go again, in a batch of their own that goes at once with the same
    /// deadline, each keeping its id, number and end, its bytes read back
    /// out of the frame, and resolved here as moved, without an end; past
    /// the deadline they are given up on instead.
    pub(super) fn send_again(&mut self, batch: u64, indexes: &[usize]) {
        let Some(mut confirming) = self.confirmer.by_id.remove(&batch) else { return };
        let late = confirming.deadline.is_some_and(|deadline| deadline <= self.now);
        let bodies = if late {
            Vec::new()
        } else {
            let read = frame::decode(confirming.frame.slice(HEADER_LENGTH..))
                .ok()
                .and_then(|envelope| envelope.batch)
                .and_then(|inside| MessageBatch::decode(inside).ok())
                .map(|inside| inside.message)
                .filter(|bodies| bodies.len() == confirming.count());
            let Some(bodies) = read else {
                // Its own frame, which reads back; should it not, what would
                // go again is given up on.
                log::error!("a confirmed batch's frame does not read back; its messages are given up on, id {batch}");
                for &index in indexes {
                    let number = confirming.number(index);
                    if self.resolve_in(&mut confirming, index, false, DropReason::NotConfirmed, true) {
                        self.resolved_in_run(batch, number, 1);
                    }
                }
                self.confirmer.by_id.insert(batch, confirming);
                self.forget_resolved(batch);
                return;
            };
            bodies
        };
        let mut records = Vec::with_capacity(if late { 0 } else { indexes.len() });
        let mut bytes = 0;
        for &index in indexes {
            if confirming.resolved[index] {
                continue;
            }
            if late {
                self.resolve_in(&mut confirming, index, false, DropReason::NotConfirmed, true);
                continue;
            }
            confirming.resolved.set(index);
            confirming.unresolved -= 1;
            self.confirmer.unconfirmed -= 1; // counted again in its new batch
            let mut body = self.batcher.bodies.take();
            body.extend_from_slice(&bodies[index]);
            bytes += body.len();
            let (done, tracker) = confirming.take_end(index);
            records.push(Record { body, id: confirming.id_of(index), done, tracker, number: confirming.number(index) });
        }
        let again = (!records.is_empty()).then(|| {
            let deadline = confirming.deadline;
            (confirming.r#type, confirming.key.clone(), confirming.delivery, confirming.backoff, deadline)
        });
        self.confirmer.by_id.insert(batch, confirming);
        self.rebuild_runs(batch);
        self.forget_resolved(batch);
        if let Some((r#type, key, delivery, backoff, deadline)) = again {
            self.confirmer.sent_again += records.len() as u64;
            self.send_batch(Batch::at_once(r#type, key, delivery, records, bytes, backoff, deadline), None);
        }
    }

    /// What is unresolved of the batch of envelope `batch` goes again.
    pub(super) fn send_unresolved_again(&mut self, batch: u64) {
        let Some(confirming) = self.confirmer.by_id.get(&batch) else { return };
        let indexes: Vec<usize> = (0..confirming.count()).filter(|index| !confirming.resolved[*index]).collect();
        self.send_again(batch, &indexes);
    }

    /// The batch's place among those due: the earliest of its retry time,
    /// once written, and its deadline; the timer moved to it when it is the
    /// earliest, stopped once none is due.
    pub(super) fn confirming_due(&mut self, batch: u64) {
        let Some(confirming) = self.confirmer.by_id.get_mut(&batch) else { return };
        let due = earliest(if confirming.written { confirming.retry_at } else { None }, confirming.deadline);
        if due == confirming.due {
            return;
        }
        let before = std::mem::replace(&mut confirming.due, due);
        if let Some(before) = before {
            self.confirmer.by_due.remove(&(before, batch));
        }
        match due {
            Some(due) => {
                self.confirmer.by_due.insert((due, batch));
                if self.confirmer.armed.is_none_or(|armed| due < armed) {
                    self.arm_confirm_timer();
                }
            }
            None if self.confirmer.by_due.is_empty() => self.stop_confirm_timer(),
            None => {}
        }
    }

    /// The timer runs to the earliest time due among the batches waiting;
    /// not while it fires, which arms it once at its end.
    fn arm_confirm_timer(&mut self) {
        if self.confirmer.firing {
            return;
        }
        let Some(&(earliest, _)) = self.confirmer.by_due.first() else {
            self.stop_confirm_timer();
            return;
        };
        if self.confirmer.armed.is_some_and(|armed| armed <= earliest) {
            return;
        }
        self.confirmer.armed = Some(earliest);
        self.timers.rearm(&mut self.confirmer.timer, earliest, Due::Confirmations);
    }

    /// No batch is due: the timer stops, so that nothing waits for it.
    pub(super) fn stop_confirm_timer(&mut self) {
        self.timers.cancel(&mut self.confirmer.timer);
        self.confirmer.armed = None;
    }

    /// The batches whose time came: what is left of one past its deadline
    /// given up on, NotConfirmed, and what is unresolved of one whose retry
    /// time came sent again; the timer runs on to the next, armed once at
    /// the end.
    pub(crate) fn confirm_timer_fired(&mut self) {
        self.confirmer.timer = None;
        self.confirmer.armed = None;
        self.confirmer.firing = true;
        let now = self.now;
        let due: Vec<u64> =
            self.confirmer.by_due.iter().take_while(|(due, _)| *due <= now).map(|(_, batch)| *batch).collect();
        for batch in due {
            let deadline_passed = self
                .confirmer
                .by_id
                .get(&batch)
                .is_some_and(|confirming| confirming.deadline.is_some_and(|deadline| deadline <= now));
            if deadline_passed {
                self.with_batch(batch, |core, confirming| {
                    for index in 0..confirming.count() {
                        core.resolve_in(confirming, index, false, DropReason::NotConfirmed, true);
                    }
                });
            }
            let Some(confirming) = self.confirmer.by_id.get(&batch) else { continue };
            if confirming.unresolved == 0 {
                self.forget_resolved(batch);
            } else if confirming.written && confirming.retry_at.is_some_and(|at| at <= now) {
                self.send_unresolved_again(batch);
            } else {
                self.confirming_due(batch);
            }
        }
        self.confirmer.firing = false;
        self.arm_confirm_timer();
    }

    /// A DELIVERY_ERROR for a confirmed batch, which nobody took: what is
    /// unresolved goes again after a back-off, from 0.1 s, doubling to 1 s.
    /// False for an envelope this client does not wait for.
    pub(crate) fn confirmed_batch_not_taken(&mut self, envelope_id: u64) -> bool {
        let now = self.now;
        let Some(confirming) = self.confirmer.by_id.get_mut(&envelope_id) else { return false };
        confirming.backoff =
            if confirming.backoff.is_zero() { BACKOFF_FIRST } else { (confirming.backoff * 2).min(BACKOFF_MOST) };
        confirming.written = true;
        confirming.retry_at = Some(now + confirming.backoff);
        self.confirming_due(envelope_id);
        true
    }

    /// A connection closed: the batches written on it whose messages are
    /// not all resolved go again at once, in the order their messages were
    /// numbered. One placed on it and not written went to another
    /// connection with the rest of its queue
    /// (`handle_orphaned_outgoing_messages`), its connection unknown from
    /// now on. What a client whose handlers run as messages arrive owes
    /// goes now too, on another connection: the read it waited for
    /// (`read_drained`) will not come.
    pub(crate) fn confirmations_on_closed(&mut self, conn: ConnId) {
        let mut again: Vec<(u64, u64)> = Vec::new();
        for (id, confirming) in &mut self.confirmer.by_id {
            if confirming.placed_on != Some(conn) {
                continue;
            }
            confirming.placed_on = None;
            let sent = confirming.frame_state.as_ref().is_some_and(|tracker| tracker.get() == SendState::Sent);
            if (confirming.written || sent) && confirming.unresolved > 0 {
                again.push((confirming.number(0), *id));
            }
        }
        again.sort_unstable();
        for (_, batch) in again {
            self.send_unresolved_again(batch);
        }
        if self.confirmer.owed_count != 0 && self.hands_out_as_it_comes() && !self.client.shuts_down {
            self.send_confirmations();
        }
    }
}
