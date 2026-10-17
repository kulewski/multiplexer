//! A client's routing and key filters: told to every connection with
//! PEER_CONTROL and carried in every welcome, confirmed by PEER_STATUS, and
//! acknowledged once every connection confirmed and what was read before
//! has been handed out. Mirrors multiplexer/basic_client_routing.cc. Part of
//! the client; see `super`.

use std::collections::BTreeMap;
use std::sync::Arc;

use prost::Message as _;

use super::{guarded, IncomingMode};
use crate::constants::types;
use crate::core::{ConnId, Core, OutFrame};
use crate::defaults::MAX_KEY_PATTERN_SIZE;
use crate::error::{Error, Result};
use crate::frame;
use crate::proto::multiplexer::{KeyFilter, MultiplexerMessage, PeerControl, PeerStatus, Routing};

/// The program's key filters: per message type, the patterns of which one
/// must match a key whole.
pub(crate) type KeyFilters = BTreeMap<u32, Vec<String>>;

/// A callback of an acknowledged change.
pub(crate) type Acknowledged = Box<dyn FnOnce() + Send>;

/// The Routing flags as text for a log line.
pub(crate) fn routing_text(routing: &Routing) -> String {
    let flag = |on: bool| if on { "yes" } else { "no" };
    format!("any={} all={} last_resort={}", flag(routing.any()), flag(routing.all()), flag(routing.last_resort()))
}

/// Whether two Routings say the same.
fn same_routing(first: &Routing, second: &Routing) -> bool {
    first.any() == second.any() && first.all() == second.all() && first.last_resort() == second.last_resort()
}

/// Key filters as text for a log line: each type's patterns, quoted.
fn key_filters_text(filters: &[KeyFilter]) -> String {
    let mut text = String::new();
    for filter in filters {
        text.push_str(if text.is_empty() { "type " } else { "; type " });
        text.push_str(&format!("{}:", filter.r#type));
        for pattern in &filter.pattern {
            text.push_str(&format!(" \"{pattern}\""));
        }
    }
    if text.is_empty() {
        "every key".to_string()
    } else {
        text
    }
}

/// The checks a library can make of key filters without RE2; the
/// multiplexer checks the rest.
pub(crate) fn check_key_filters(filters: &KeyFilters) -> Result<()> {
    for (r#type, patterns) in filters {
        if *r#type <= types::MAX_MULTIPLEXER_META_PACKET {
            return Err(Error::InvalidArgument(format!("a key filter for message type {type}, a protocol type")));
        }
        for pattern in patterns {
            if pattern.len() > MAX_KEY_PATTERN_SIZE {
                return Err(Error::InvalidArgument(format!(
                    "a key pattern of {} bytes, over the limit of {MAX_KEY_PATTERN_SIZE} (MAX_KEY_PATTERN_SIZE)",
                    pattern.len()
                )));
            }
        }
    }
    Ok(())
}

impl Core {
    /// Which of a multiplexer's routing paths reach this peer: told to
    /// every registered connection now, carried in every welcome from now
    /// on. `acknowledged` runs once this change, or a later one, is
    /// acknowledged.
    pub(crate) fn set_routing(&mut self, routing: Routing, acknowledged: Option<Acknowledged>) {
        self.client.routing = routing;
        log::info!("routing {}", routing_text(&routing));
        if let Some(acknowledged) = acknowledged {
            self.client.acknowledged_callbacks.push(acknowledged);
        }
        self.routing_changed();
    }

    /// Which keys of each message type reach this peer by the rules,
    /// replacing those set before, told and carried as the routing is.
    pub(crate) fn set_key_filters(&mut self, filters: KeyFilters, acknowledged: Option<Acknowledged>) -> Result<()> {
        check_key_filters(&filters)?;
        if let Some(acknowledged) = acknowledged {
            self.client.acknowledged_callbacks.push(acknowledged);
        }
        self.client.key_filters_message =
            filters.iter().map(|(r#type, patterns)| KeyFilter { r#type: *r#type, pattern: patterns.clone() }).collect();
        self.client.key_filters = filters;
        log::info!("key filters {}", key_filters_text(&self.client.key_filters_message));
        self.routing_changed();
        Ok(())
    }

    /// The routing or the key filters changed: a new version, a new
    /// welcome, and PEER_CONTROL on every live connection.
    fn routing_changed(&mut self) {
        self.client.routing_version += 1;
        self.client.confirmed = false;
        self.client.welcome = None;
        let registered: Vec<ConnId> = self.by_peer_id.values().copied().collect();
        for conn in registered {
            if self.conn_living(conn) {
                self.send_routing(conn);
            }
        }
        self.update_confirmation();
    }

    /// PEER_CONTROL with the current routing and key filters on `conn`:
    /// forced past a full queue, since the peer that steps out is often the
    /// saturated one, pinned, so that a connection that dies does not hand
    /// it to another multiplexer, and the connection's own.
    pub(super) fn send_routing(&mut self, conn: ConnId) {
        let control = PeerControl { routing: self.client.routing, key_filter: self.client.key_filters_message.clone() };
        let id = self.random64();
        let message = MultiplexerMessage {
            id: Some(id),
            sender: Some(self.instance_id),
            r#type: types::PEER_CONTROL,
            message: Some(control.encode_to_vec().into()),
            ..Default::default()
        };
        let Ok(bytes) = frame::encode(&message) else { return };
        let frame = Arc::new(OutFrame {
            bytes,
            id,
            r#type: types::PEER_CONTROL,
            pinned: true,
            for_all: false,
            own: true,
            number: 0,
            deadline: None,
            batch: None,
        });
        let Some(state) = self.conns.get_mut(&conn) else { return };
        state.routing_request_id = id;
        let peer = state.peer_id;
        if self.schedule(conn, frame, true, false).is_none() {
            log::warn!("could not queue PEER_CONTROL on the connection to {peer}");
        }
    }

    /// The answer to the last PEER_CONTROL sent on a connection confirms
    /// the current routing and key filters; one to an older one confirms
    /// nothing. A refusal is logged as an error.
    pub(super) fn on_peer_status(&mut self, conn: ConnId, message: &MultiplexerMessage) {
        let peer = self.conns.get(&conn).map_or(0, |state| state.peer_id);
        let status = message.message.as_ref().map(|payload| PeerStatus::decode(payload.clone()));
        let status = match status {
            Some(Ok(status)) if status.error.is_none() => status,
            Some(Ok(status)) => {
                log::error!("PEER_STATUS from {peer}: {}", status.error.unwrap_or_default());
                return;
            }
            _ => {
                log::error!("PEER_STATUS from {peer}: garbled");
                return;
            }
        };
        let version = self.client.routing_version;
        let Some(state) = self.conns.get_mut(&conn) else { return };
        if message.references.unwrap_or(0) == state.routing_request_id
            && same_routing(&status.routing.unwrap_or_default(), &self.client.routing)
            && status.key_filter == self.client.key_filters_message
        {
            state.routing_acknowledged = version;
            self.update_confirmation();
        }
    }

    /// Whether every living connection is registered and has confirmed the
    /// current routing.
    fn all_confirmed(&self) -> bool {
        self.client.by_target.values().all(|conn| match self.conns.get(conn) {
            Some(state) if state.living() => {
                state.registered && state.routing_acknowledged == self.client.routing_version
            }
            _ => true,
        })
    }

    /// After whatever can confirm the routing or take that back: notes
    /// where the incoming queue stood when every connection confirmed, and
    /// tells of an acknowledgement once per change.
    pub(crate) fn update_confirmation(&mut self) {
        if !self.all_confirmed() {
            self.client.confirmed = false;
            return;
        }
        if !self.client.confirmed {
            self.client.confirmed = true;
            self.client.confirmed_enqueued = self.client.enqueued;
        }
        if self.routing_acknowledged()
            && (self.client.acknowledged_version != self.client.routing_version
                || !self.client.acknowledged_callbacks.is_empty())
        {
            self.client.acknowledged_version = self.client.routing_version;
            self.change_acknowledged();
        }
    }

    /// A change that became acknowledged when a receive took the last
    /// message read before every connection confirmed it, rather than at a
    /// confirmation, which update_confirmation tells of: told of the same
    /// way, once. What a receive calls before it waits, nothing being
    /// queued, so that nothing is added per message.
    pub(crate) fn acknowledged_by_receive(&mut self) {
        if self.client.acknowledged_version != self.client.routing_version && self.routing_acknowledged() {
            self.client.acknowledged_version = self.client.routing_version;
            self.change_acknowledged();
        }
    }

    /// A change acknowledged: the serve loop's wake, and the callbacks at
    /// once for a client that hands out what arrives (BasicClient's
    /// _acknowledged()).
    fn change_acknowledged(&mut self) {
        self.client.acknowledged_wake = true;
        if self.client.mode == IncomingMode::Dispatch {
            self.run_acknowledged_callbacks(); // everything before went out already
        }
    }

    /// Whether every connection has the current routing and key filters in
    /// effect, and everything read before the last confirmation has been
    /// handed out.
    pub(crate) fn routing_acknowledged(&self) -> bool {
        self.client.confirmed && self.client.dequeued >= self.client.confirmed_enqueued && self.all_confirmed()
    }

    /// The acknowledged changes' callbacks, run now when the routing is
    /// acknowledged; nothing otherwise.
    pub(crate) fn run_acknowledged_callbacks(&mut self) {
        if self.client.acknowledged_callbacks.is_empty() || self.client.shuts_down || !self.routing_acknowledged() {
            return;
        }
        // A callback's own change waits for its own acknowledgement.
        for callback in std::mem::take(&mut self.client.acknowledged_callbacks) {
            guarded(|| "a callback of an acknowledged change".to_string(), callback);
        }
    }

    /// Whether a change was acknowledged, or the last message held for a
    /// connection went, since the last call, which clears it: what ends a
    /// serve loop's wait early, each a reason for a drain to look again.
    pub(crate) fn take_acknowledged_wake(&mut self) -> bool {
        std::mem::take(&mut self.client.acknowledged_wake)
    }
}
