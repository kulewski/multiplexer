//! A client's connections coming and going: the targets it was given and
//! their connections, connecting to every address a name resolves to,
//! reconnecting AUTO_RECONNECT_TIME after a connection is lost, dropping a
//! target, and the registration at the multiplexer's welcome. Mirrors
//! multiplexer/basic_client_connect.cc and the client's half of
//! connections_manager.h. Part of the client; see `super`.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use super::Wrapper;
use crate::constants::peers;
use crate::core::{Action, Conn, ConnId, Core, Due, Posted, Target};
use crate::defaults::{AUTO_RECONNECT_TIME, CONNECT_ATTEMPT_SECONDS};
use crate::endpoint::format_endpoint;
use crate::error::{Error, Result};
use crate::proto::multiplexer::WelcomeMessage;

/// The target of `host` and `port`: an address in text as the standard
/// library writes it, so that two spellings of one address are one target,
/// a name as given.
pub(crate) fn target_of(host: &str, port: u16) -> Target {
    match host.parse::<IpAddr>() {
        Ok(address) => (address.to_string(), port),
        Err(_) => (host.to_string(), port),
    }
}

impl Core {
    /// Closes the connection's socket, if open.
    pub(super) fn close_socket(&mut self, conn: ConnId) {
        let Some(state) = self.conns.get_mut(&conn) else { return };
        if !state.socket_open {
            return;
        }
        state.socket_open = false;
        self.actions.push(Action::Close(conn));
    }

    /// A welcome from the multiplexer: checks the peer type and a clash on
    /// the instance id, records the connection, and warns once of two
    /// targets that reach one multiplexer. Refusal is a shutdown.
    pub(crate) fn register_connection(&mut self, conn: ConnId, welcome: &WelcomeMessage) {
        let Some(state) = self.conns.get(&conn) else { return };
        let target = (*state.target).clone();
        for (reached, id) in &self.client.reached_by_target {
            if *id != welcome.id || *reached == target {
                continue;
            }
            let said =
                if *reached < target { (reached.clone(), target.clone()) } else { (target.clone(), reached.clone()) };
            if self.client.one_multiplexer_said.insert(said.clone()) {
                log::warn!(
                    "multiplexer {} is reached through two targets, {} and {}: list it once; while both are listed, \
                     their connections replace or refuse each other at every reconnect, and what was queued on one \
                     replaced is lost",
                    welcome.id,
                    format_endpoint(&said.0 .0, said.0 .1),
                    format_endpoint(&said.1 .0, said.1 .1)
                );
            }
        }
        self.client.reached_by_target.insert(target, welcome.id);
        if welcome.r#type != peers::MULTIPLEXER {
            log::warn!("invalid peer type {} announced by {}: not a multiplexer; refused", welcome.r#type, welcome.id);
            self.shutdown_connection(conn);
            return;
        }
        if welcome.id == self.instance_id {
            log::warn!("connected to self; refused");
            self.shutdown_connection(conn);
            return;
        }
        let mut replaced = false;
        if let Some(previous) = self.by_peer_id.get(&welcome.id).copied() {
            if self.conns.contains_key(&previous) && previous != conn {
                // The same host reconnecting replaces its stale connection;
                // another host claiming an id that is still live is refused.
                let newcomer = self.conns[&conn].expected_endpoint.map(|endpoint| endpoint.ip());
                let old = self.conns[&previous].expected_endpoint.map(|endpoint| endpoint.ip());
                let refusal = match (newcomer, old) {
                    (None, _) => Some(": its socket is gone"),
                    (Some(new), Some(old)) if new != old && self.conns[&previous].living() => {
                        Some(" from a different address")
                    }
                    _ => None,
                };
                if let Some(refusal) = refusal {
                    log::warn!("refusing connection that claims live id {}{refusal}", welcome.id);
                    self.shutdown_connection(conn);
                    return;
                }
                self.shutdown_connection(previous);
                replaced = true;
            }
        }
        self.by_peer_id.insert(welcome.id, conn);
        self.round_robin.push_front(conn);
        if let Some(state) = self.conns.get_mut(&conn) {
            state.set_outgoing_queue_max_size(crate::core::connection::DEFAULT_OUT_QUEUE_SIZE);
        }
        log::info!(
            "registered connection id={} type={} (MULTIPLEXER){}",
            welcome.id,
            welcome.r#type,
            if replaced { "; replaces its earlier connection" } else { "" }
        );
    }

    /// Right after the registration: the multiplexer's welcome confirms the
    /// routing ours carried, a change made meanwhile goes out now; what
    /// waited on this connection for room moves in, then what waited for
    /// any connection; the observer hears "up".
    pub(crate) fn after_connection_registration(&mut self, conn: ConnId) {
        let version = self.client.routing_version;
        let Some(state) = self.conns.get_mut(&conn) else { return };
        state.routing_acknowledged = state.routing_in_welcome;
        let outdated = state.routing_in_welcome != version;
        let target = (*state.target).clone();
        if outdated {
            self.send_routing(conn);
        }
        self.update_confirmation();
        self.client.first_connection_over.insert(target);
        self.outgoing_queue_has_room(conn);
        self.place_held();
        let wrapper = self.wrap(conn);
        self.on_connection(&wrapper, true);
    }

    /// Takes a registered connection out of the indexes, at its shutdown.
    pub(crate) fn unregister_connection(&mut self, conn: ConnId) {
        let Some(state) = self.conns.get(&conn) else { return };
        let peer = state.peer_id;
        if self.by_peer_id.get(&peer) != Some(&conn) {
            log::warn!("unregistering a connection that was never registered id={peer}");
            return;
        }
        log::info!("unregistered connection id={peer} type={} (MULTIPLEXER)", state.peer_type);
        self.by_peer_id.remove(&peer);
        self.round_robin.retain(|each| *each != conn);
    }

    /// A connection ended, for any reason: what waited for it is put aside
    /// to follow its queue; a reconnect is armed AUTO_RECONNECT_TIME from
    /// now while the target is the client's, before the observer hears
    /// "down", so that nothing it does can skip it.
    pub(crate) fn connection_destroyed(&mut self, conn: ConnId) {
        let Some(state) = self.conns.get(&conn) else { return };
        if !state.shuts_down {
            self.client.closing.push(conn);
        }
        let target = (*state.target).clone();
        self.displace(conn);
        let still_wanted = self.client.by_target.get(&target) == Some(&conn);
        if still_wanted {
            self.client.by_target.remove(&target);
            self.client.first_connection_over.insert(target.clone());
        }
        if !self.client.shuts_down && still_wanted {
            log::debug!(
                "scheduling reconnecting after {} seconds to {}",
                AUTO_RECONNECT_TIME.as_secs(),
                format_endpoint(&target.0, target.1)
            );
            self.client.next_reconnect += 1;
            let number = self.client.next_reconnect;
            let key = self.timers.arm(self.now + AUTO_RECONNECT_TIME, Due::Reconnect(number));
            self.client.reconnects.insert(number, (key, target.clone()));
        }
        let wrapper = Wrapper {
            conn: None,
            target: Arc::new(target),
            endpoint: self.conns.get(&conn).and_then(|state| state.expected_endpoint),
            living: None,
            owner: self.instance_id,
            generation: crate::fork::generation(),
        };
        self.on_connection(&wrapper, false);
        if !self.client.shuts_down {
            self.update_confirmation();
        }
    }

    /// A reconnect's time came: the target connects again unless it was
    /// dropped, or the client shut down, meanwhile.
    pub(crate) fn reconnect_after_timeout(&mut self, number: u64) {
        let Some((_, target)) = self.client.reconnects.remove(&number) else { return };
        if self.client.shuts_down {
            return;
        }
        if !self.client.by_target.contains_key(&target) {
            let _ = self.async_connect(&target.0, target.1);
        }
    }

    /// The connection `target` has, live or on its way, that nobody shut
    /// down.
    fn live_connection(&self, target: &Target) -> Option<ConnId> {
        let conn = *self.client.by_target.get(target)?;
        self.conns.get(&conn).filter(|state| !state.shuts_down).map(|_| conn)
    }

    /// Starts connecting to `host` and `port`, an address or a name, which
    /// is resolved first, every address it has tried in turn; returns the
    /// connection at once. A target with a connection, live or on its way,
    /// keeps it. NotConnected after the client shut down.
    pub(crate) fn async_connect(&mut self, host: &str, port: u16) -> Result<ConnId> {
        if self.client.shuts_down {
            return Err(Error::NotConnected);
        }
        let target = target_of(host, port);
        if let Some(live) = self.live_connection(&target) {
            return Ok(live);
        }
        if let Some(old) = self.client.by_target.get(&target).copied() {
            self.shutdown_connection(old);
        }
        let conn = self.new_conn_id();
        let heartbit = self.heartbit();
        self.conns.insert(conn, Conn::new(target.clone(), heartbit));
        self.client.by_target.insert(target.clone(), conn);
        match target.0.parse::<IpAddr>() {
            Ok(address) => {
                self.conns.get_mut(&conn).expect("just made").candidates = vec![SocketAddr::new(address, port)];
                self.try_next_candidate(conn);
            }
            Err(_) => self.resolve_and_start(conn),
        }
        Ok(conn)
    }

    /// Drops the target given with this host and port: its reconnect
    /// stops, a connection to it on its way is abandoned and a live one
    /// closed the polite way. True when the client had the target.
    /// NotConnected after the client shut down.
    pub(crate) fn disconnect(&mut self, host: &str, port: u16) -> Result<bool> {
        if self.client.shuts_down {
            return Err(Error::NotConnected);
        }
        let target = target_of(host, port);
        let mut had = false;
        let numbers: Vec<u64> =
            self.client.reconnects.iter().filter(|(_, (_, each))| *each == target).map(|(number, _)| *number).collect();
        for number in numbers {
            if let Some((key, _)) = self.client.reconnects.remove(&number) {
                self.timers.cancel(&mut Some(key));
            }
            had = true;
        }
        let conn = self.client.by_target.remove(&target);
        had |= conn.is_some();
        self.client.first_connection_over.remove(&target);
        self.client.reached_by_target.remove(&target);
        if had {
            log::info!("disconnecting from {}", format_endpoint(&target.0, target.1));
        }
        if let Some(conn) = conn {
            self.close_gracefully(conn, crate::defaults::CLOSE_READ_SECONDS);
        }
        Ok(had)
    }

    /// Resolves the connection's target, on the driver's resolver thread,
    /// then connects.
    fn resolve_and_start(&mut self, conn: ConnId) {
        let target = (*self.conns[&conn].target).clone();
        self.actions.push(Action::Resolve(conn, target.0, target.1));
    }

    /// The addresses a target resolved to, or why it did not: none is an
    /// attempt that failed, retried by the reconnect.
    pub(crate) fn on_resolved(&mut self, conn: ConnId, resolved: std::result::Result<Vec<SocketAddr>, String>) {
        let Some(state) = self.conns.get_mut(&conn) else { return };
        if state.shuts_down {
            return;
        }
        match resolved {
            Ok(candidates) if !candidates.is_empty() => {
                state.candidates = candidates;
                state.next_candidate = 0;
                self.try_next_candidate(conn);
            }
            failed => {
                let why = match failed {
                    Err(why) => format!(": {why}"),
                    Ok(_) => String::new(),
                };
                log::warn!(
                    "{} does not resolve{why}; trying again later",
                    format_endpoint(&state.target.0, state.target.1)
                );
                self.shutdown_connection(conn); // arms the reconnect, which resolves again
            }
        }
    }

    /// Connects to the next address of the connection's target, or ends the
    /// attempt when every one failed. An address that drops the SYNs gets
    /// CONNECT_ATTEMPT_SECONDS.
    fn try_next_candidate(&mut self, conn: ConnId) {
        let deadline = self.now + CONNECT_ATTEMPT_SECONDS;
        let Some(state) = self.conns.get_mut(&conn) else { return };
        if state.next_candidate >= state.candidates.len() {
            self.shutdown_connection(conn);
            return;
        }
        let address = state.candidates[state.next_candidate];
        state.next_candidate += 1;
        state.expected_endpoint = Some(address);
        state.socket_open = true;
        state.connecting = true;
        self.timers.rearm(&mut state.connect_deadline, deadline, Due::ConnectDeadline(conn));
        self.actions.push(Action::Open(conn, address));
    }

    /// A connect's CONNECT_ATTEMPT_SECONDS ran out with it under way: it is
    /// cancelled, and its end is taken for the failure it is.
    pub(crate) fn connect_deadline(&mut self, conn: ConnId) {
        let Some(state) = self.conns.get_mut(&conn) else { return };
        state.connect_deadline = None;
        if state.connecting {
            state.connect_timed_out = true;
            self.actions.push(Action::CancelConnect(conn));
        }
    }

    /// The driver's word on the TCP connect: the handshake starts, or the
    /// next address is tried, or the attempt ends.
    pub(crate) fn connected(&mut self, conn: ConnId, result: std::result::Result<(), String>) {
        let version = self.client.routing_version;
        let Some(state) = self.conns.get_mut(&conn) else { return };
        let timed_out = state.connect_timed_out;
        state.connecting = false;
        state.connect_timed_out = false;
        self.timers.cancel(&mut state.connect_deadline);
        match result {
            Ok(()) => {
                state.routing_in_welcome = version;
                self.start_connection(conn);
            }
            Err(why) if !state.shuts_down => {
                log::debug!(
                    "connect to {:?} failed: {}",
                    state.expected_endpoint,
                    if timed_out { format!("no answer within {} s", CONNECT_ATTEMPT_SECONDS.as_secs()) } else { why }
                );
                self.try_next_candidate(conn);
            }
            Err(_) => {}
        }
    }

    /// A connect that could not even open its socket: reported as a failed
    /// connect, posted.
    pub(crate) fn open_failed(&mut self, conn: ConnId, why: String) {
        self.posted.push_back(Posted::ConnectFailed(conn, why));
    }

    /// How many connections are registered.
    pub(crate) fn connections_count(&self) -> usize {
        self.by_peer_id.values().filter(|conn| self.conns.contains_key(conn)).count()
    }
}
