//! Confirmed sends, the consumer's side (`super`, docs/delivery.md
//! "Confirmed sends"): what a client owes the producers whose confirmed
//! batches it took, gathered per producer as (first, count) pairs per
//! outcome and sent in one CONFIRMATION, on the connection the producer's
//! messages came on while it lives, else another. Mirrors the consumer's
//! half of multiplexer/basic_client_confirm.cc.
//!
//! A message of a confirmed batch is handed out with its number
//! (`split_batch` in `client`); `confirm()` adds it to what is owed its
//! producer, which goes when this client has nothing more in hand for the
//! program (`send_confirmations`): where handlers run as messages arrive,
//! once the connection handed over every whole frame its last read brought
//! (`read_drained`), so that the batches of one read, of one message each
//! when the producer does not batch, are confirmed together, or at once
//! when the connection closes before that; before a receive waits; at the
//! end of a threaded client's turn that took confirmations its program
//! gave (`confirm_handed`); and at a flush, at CONFIRMATIONS_GATHERED_MAX
//! owed one producer, or at shutdown. A CONFIRMATION lost costs nothing but
//! time: what it named goes again.

use prost::Message as _;

use super::super::batching::BatchMark;
use super::super::lane::Lane;
use super::super::outbox::Frame;
use super::super::{deadline_after, ConnId, Core, Posted};
use super::{add_number, Confirmed};
use crate::constants::types;
use crate::defaults::{CONFIRMATIONS_GATHERED_MAX, DEFAULT_TIMEOUT};
use crate::frame;
use crate::inbox::HandedConfirmation;
use crate::proto::multiplexer::{Confirmation, MultiplexerMessage};

/// Past this many producers remembered, those owed nothing are forgotten
/// (`send_confirmations`), so that producers met once each do not pile up.
const OWED_KEPT: usize = 4096;

/// What a consumer owes one producer: the numbers, by what became of them,
/// as (first, count) pairs in the order confirmed, how many, and the
/// connection the producer's messages came on last.
#[derive(Default)]
pub(super) struct Owed {
    connection: Option<ConnId>,
    processed: Vec<u64>,
    refused: Vec<u64>,
    failed: Vec<u64>,
    count: usize,
}

impl Core {
    /// Tells the producer `sender` what became of its message `number`,
    /// gathered with the rest this client owes it into one CONFIRMATION,
    /// which goes at once once CONFIRMATIONS_GATHERED_MAX are owed it.
    /// Nothing for number 0, a message with no number.
    pub(crate) fn confirm(&mut self, sender: u64, number: u64, outcome: Confirmed) {
        if number == 0 {
            return;
        }
        let owed = self.confirmer.owed.entry(sender).or_default();
        add_number(
            match outcome {
                Confirmed::Processed => &mut owed.processed,
                Confirmed::Refused => &mut owed.refused,
                Confirmed::Failed => &mut owed.failed,
            },
            number,
        );
        owed.count += 1;
        let full = owed.count >= CONFIRMATIONS_GATHERED_MAX;
        self.confirmer.owed_count += 1;
        if full {
            self.send_owed(sender);
        }
    }

    /// Every confirmation owed, sent now. A producer met keeps its entry,
    /// the connection its messages came on, for the next ones; past
    /// OWED_KEPT, those owed nothing go.
    pub(crate) fn send_confirmations(&mut self) {
        if self.confirmer.owed_count == 0 {
            return;
        }
        let producers: Vec<u64> =
            self.confirmer.owed.iter().filter(|(_, owed)| owed.count > 0).map(|(producer, _)| *producer).collect();
        for producer in producers {
            self.send_owed(producer);
        }
        if self.confirmer.owed.len() > OWED_KEPT {
            self.confirmer.owed.retain(|_, owed| owed.count > 0);
        }
    }

    /// How many confirmations this client owes producers; for tests.
    pub(crate) fn confirmations_owed(&self) -> usize {
        self.confirmer.owed_count
    }

    /// How many producers this client remembers; for tests.
    #[cfg(test)]
    pub(crate) fn producers_remembered(&self) -> usize {
        self.confirmer.owed.len()
    }

    /// The messages of a confirmed batch from `producer` came on `conn`:
    /// what is owed it goes back that way while the connection lives.
    pub(crate) fn confirmed_batch_from(&mut self, conn: ConnId, producer: u64) {
        self.confirmer.owed.entry(producer).or_default().connection = Some(conn);
    }

    /// Confirmations a threaded client's program gave, which its inbox
    /// handed over (`HandedConfirmation`), gathered, unless the client was
    /// torn down: what is owed goes at the end of the io thread's turn, one
    /// send posted however many come meanwhile. `handed` is left empty.
    pub(crate) fn confirm_handed(&mut self, handed: &mut Vec<HandedConfirmation>) {
        if handed.is_empty() {
            return;
        }
        if self.threaded.torn_down {
            handed.clear();
            return;
        }
        for confirmation in handed.drain(..) {
            self.confirm(confirmation.sender, confirmation.number, confirmation.outcome);
        }
        if !self.confirmer.posted {
            self.confirmer.posted = true;
            self.posted.push_back(Posted::SendConfirmations);
        }
    }

    /// The send `confirm_handed` posted.
    pub(crate) fn send_confirmations_posted(&mut self) {
        self.confirmer.posted = false;
        if !self.threaded.torn_down {
            self.send_confirmations();
        }
    }

    /// What this client owes `producer`, in one CONFIRMATION, on the
    /// connection its messages came on, else another live one,
    /// DEFAULT_TIMEOUT at most; lost, its messages go again in time. The
    /// lists keep their room for the next.
    fn send_owed(&mut self, producer: u64) {
        let Some(owed) = self.confirmer.owed.get_mut(&producer) else { return };
        let confirmation = Confirmation {
            processed: std::mem::take(&mut owed.processed),
            refused: std::mem::take(&mut owed.refused),
            failed: std::mem::take(&mut owed.failed),
        };
        self.confirmer.owed_count -= owed.count;
        owed.count = 0;
        let connection = owed.connection;
        let payload = confirmation.encode_to_vec();
        let Confirmation { mut processed, mut refused, mut failed } = confirmation;
        processed.clear();
        refused.clear();
        failed.clear();
        if let Some(owed) = self.confirmer.owed.get_mut(&producer) {
            (owed.processed, owed.refused, owed.failed) = (processed, refused, failed);
        }
        let id = self.random64();
        let message = MultiplexerMessage {
            id: Some(id),
            sender: Some(self.instance_id),
            to: Some(producer),
            r#type: types::CONFIRMATION,
            report_delivery_error: Some(false),
            message: Some(payload.into()),
            ..Default::default()
        };
        let Ok(bytes) = frame::encode(&message) else { return };
        let frame = Frame { bytes, head: None, id, r#type: types::CONFIRMATION, batch: BatchMark::None };
        let lane = connection.filter(|conn| self.conn_living(*conn)).map(|conn| Lane::seeded(&self.wrap(conn), false));
        let deadline = deadline_after(self.now, Some(DEFAULT_TIMEOUT));
        self.send_at(frame, false, lane.as_ref(), deadline, 0, None, None, None);
    }
}
