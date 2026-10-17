//! The core on a virtual clock, for its tests: the harness plays the
//! driver and the multiplexers. It carries out the core's actions on fake
//! sockets, keeps the frames the client wrote, decoded, completes writes
//! as an idle socket would (or holds them, for a test of a full one), and
//! moves time itself, every timer firing at its own deadline. Nothing waits
//! and nothing depends on the machine's speed: a test says what happens
//! and when, and counts what the client did.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use prost::Message as _;

use super::client::IncomingMode;
use super::connection::ReadEnd;
use super::{Action, ConnId, Core};
use crate::constants::{peers, types};
use crate::frame::{self, Head, HEADER_LENGTH};
use crate::ids::Ids;
use crate::proto::multiplexer::{MultiplexerMessage, PeerStatus, Routing, WelcomeMessage};

/// A fake socket: what the client did with it.
#[derive(Debug, Default)]
pub(crate) struct FakeSocket {
    pub(crate) address: Option<SocketAddr>,
    pub(crate) connect_cancelled: bool,
    pub(crate) reading: bool,
    pub(crate) write_shut: bool,
    pub(crate) shut: bool,
    pub(crate) closed: bool,
    /// Frames written whole, decoded, in order.
    pub(crate) written: Vec<MultiplexerMessage>,
    /// The write in flight, held while `hold_writes` says so: its bytes, a
    /// big body alone when its header is kept apart, and that header.
    pub(crate) in_flight: Option<Bytes>,
    pub(crate) in_flight_head: Option<Head>,
    /// The frames written with their header kept apart.
    pub(crate) apart: usize,
}

impl FakeSocket {
    /// The write in flight, taken, as it went on the wire.
    fn take_in_flight(&mut self) -> Option<Bytes> {
        let bytes = self.in_flight.take()?;
        Some(on_the_wire(self.in_flight_head.take(), bytes))
    }
}

/// A frame's bytes as they go on the wire: a header kept apart first.
fn on_the_wire(head: Option<Head>, bytes: Bytes) -> Bytes {
    match head {
        None => bytes,
        Some(head) => [&head[..], &bytes[..]].concat().into(),
    }
}

/// The harness; see the module comment.
pub(crate) struct Harness {
    pub(crate) core: Core,
    pub(crate) start: Instant,
    pub(crate) sockets: HashMap<ConnId, FakeSocket>,
    pub(crate) resolves: Vec<(ConnId, String, u16)>,
    /// Write completions due to the core, in order: (connection, bytes
    /// written, failed).
    write_dones: VecDeque<(ConnId, usize, bool)>,
    /// Writes stay in flight until `complete_write`, as on a socket the
    /// multiplexer does not read.
    pub(crate) hold_writes: bool,
}

/// The multiplexer's instance id a test's welcome carries for target `n`.
pub(crate) fn multiplexer_id(n: u64) -> u64 {
    1000 + n
}

impl Harness {
    /// A client of `peer_type` at the start of the virtual clock.
    pub(crate) fn new(peer_type: u32, mode: IncomingMode) -> Harness {
        crate::test_log::install();
        let start = Instant::now();
        let core = Core::new(peer_type, Arc::new(Ids::seeded(7)), mode, start);
        Harness {
            core,
            start,
            sockets: HashMap::new(),
            resolves: Vec::new(),
            write_dones: VecDeque::new(),
            hold_writes: false,
        }
    }

    /// The virtual time since the start.
    pub(crate) fn elapsed(&self) -> Duration {
        self.core.now - self.start
    }

    /// Carries out what the core asked for.
    pub(crate) fn apply(&mut self) {
        while !self.core.actions.is_empty() {
            for action in std::mem::take(&mut self.core.actions) {
                match action {
                    Action::Resolve(conn, host, port) => self.resolves.push((conn, host, port)),
                    Action::Open(conn, address) => {
                        let socket = self.sockets.entry(conn).or_default();
                        socket.address = Some(address);
                        socket.connect_cancelled = false;
                        socket.closed = false;
                    }
                    Action::CancelConnect(conn) => {
                        self.sockets.entry(conn).or_default().connect_cancelled = true;
                        self.core.connected(conn, Err("operation cancelled".to_string()));
                    }
                    Action::StartReading(conn) => self.sockets.entry(conn).or_default().reading = true,
                    Action::Write(conn, head, bytes) => {
                        let socket = self.sockets.entry(conn).or_default();
                        assert!(socket.in_flight.is_none(), "two writes at once on {conn:?}");
                        socket.apart += usize::from(head.is_some());
                        if self.hold_writes {
                            socket.in_flight = Some(bytes);
                            socket.in_flight_head = head;
                        } else {
                            let whole = on_the_wire(head, bytes);
                            socket.written.push(decode(&whole));
                            self.write_dones.push_back((conn, whole.len(), false));
                        }
                    }
                    Action::ShutdownWrite(conn) => {
                        let socket = self.sockets.entry(conn).or_default();
                        socket.write_shut = true;
                        if socket.take_in_flight().is_some() {
                            self.write_dones.push_back((conn, 0, true));
                        }
                    }
                    Action::Shutdown(conn) => {
                        let socket = self.sockets.entry(conn).or_default();
                        socket.shut = true;
                        socket.reading = false;
                        if socket.take_in_flight().is_some() {
                            self.write_dones.push_back((conn, 0, true));
                        }
                    }
                    Action::Close(conn) => {
                        let socket = self.sockets.entry(conn).or_default();
                        socket.closed = true;
                        if socket.take_in_flight().is_some() {
                            self.write_dones.push_back((conn, 0, true));
                        }
                    }
                    Action::CancelResolves => self.resolves.clear(),
                }
            }
        }
    }

    /// Runs what is due now: posted work, write completions and timers,
    /// until nothing is left. Bounded: a core that keeps posting work at
    /// one instant is a bug the test reports.
    pub(crate) fn settle(&mut self) {
        self.apply();
        for _ in 0..100_000 {
            if self.core.run_one_posted() {
                self.apply();
                continue;
            }
            if let Some((conn, written, failed)) = self.write_dones.pop_front() {
                self.core.on_write_done(conn, written, failed);
                self.apply();
                continue;
            }
            if self.core.timers.next_deadline().is_some_and(|due| due <= self.core.now) && self.core.run_one_timer() {
                self.apply();
                continue;
            }
            return;
        }
        panic!("the core did not settle");
    }

    /// Moves the clock `by` forward, every timer due on the way firing at
    /// its own deadline.
    pub(crate) fn advance(&mut self, by: Duration) {
        let target = self.core.now + by;
        self.settle();
        for _ in 0..100_000 {
            match self.core.timers.next_deadline() {
                Some(due) if due <= target => {
                    self.core.now = self.core.now.max(due);
                    self.settle();
                }
                _ => {
                    self.core.now = target;
                    self.settle();
                    return;
                }
            }
        }
        panic!("timers kept coming due");
    }

    /// Moves the clock `by` forward with nothing running meanwhile, as for
    /// a synchronous client between its calls, then runs what came due, at
    /// the new time.
    pub(crate) fn jump(&mut self, by: Duration) {
        self.core.now += by;
        self.settle();
    }

    /// Starts connecting to target `n`, the address 10.0.0.n:1980.
    pub(crate) fn connect(&mut self, n: u8) -> ConnId {
        let conn = self.core.async_connect(&format!("10.0.0.{n}"), 1980).expect("connect");
        self.settle();
        conn
    }

    /// The TCP connect of `conn` succeeds: the client sends its welcome.
    pub(crate) fn accept(&mut self, conn: ConnId) {
        self.core.connected(conn, Ok(()));
        self.settle();
    }

    /// The TCP connect of `conn` fails.
    pub(crate) fn refuse(&mut self, conn: ConnId) {
        self.core.connected(conn, Err("connection refused".to_string()));
        self.settle();
    }

    /// A frame from the multiplexer on `conn`, all a read brought: the
    /// connection's read drained after it.
    pub(crate) fn deliver(&mut self, conn: ConnId, message: &MultiplexerMessage) {
        self.deliver_read(conn, std::slice::from_ref(message));
    }

    /// The frames from the multiplexer on `conn` that one read brought, in
    /// turn, each a step of its own, as the driver hands them over, the
    /// connection's read drained after the last, in its step.
    pub(crate) fn deliver_read(&mut self, conn: ConnId, messages: &[MultiplexerMessage]) {
        for (index, message) in messages.iter().enumerate() {
            let bytes = frame::encode(message).expect("a frame");
            self.core.on_frame(conn, bytes.slice(HEADER_LENGTH..));
            if index + 1 == messages.len() {
                self.core.on_read_drained(conn);
            }
            self.settle();
        }
    }

    /// A frame from the multiplexer on `conn` that a read brought with more
    /// behind it, whose drain is still to come.
    pub(crate) fn deliver_part(&mut self, conn: ConnId, message: &MultiplexerMessage) {
        let bytes = frame::encode(message).expect("a frame");
        self.core.on_frame(conn, bytes.slice(HEADER_LENGTH..));
        self.settle();
    }

    /// The multiplexer of target `n` welcomes the client on `conn`.
    pub(crate) fn welcome(&mut self, conn: ConnId, n: u64) {
        self.welcome_from(conn, peers::MULTIPLEXER, multiplexer_id(n));
    }

    /// A welcome on `conn` from a peer of `peer_type` with instance id `id`.
    pub(crate) fn welcome_from(&mut self, conn: ConnId, peer_type: u32, id: u64) {
        let welcome = WelcomeMessage { r#type: peer_type, id, ..Default::default() };
        let message = MultiplexerMessage {
            id: Some(0),
            sender: Some(id),
            r#type: types::CONNECTION_WELCOME,
            message: Some(welcome.encode_to_vec().into()),
            ..Default::default()
        };
        self.deliver(conn, &message);
    }

    /// A heartbeat from the multiplexer on `conn`.
    pub(crate) fn heartbit(&mut self, conn: ConnId) {
        let message =
            MultiplexerMessage { id: Some(0), sender: Some(1), r#type: types::HEARTBIT, ..Default::default() };
        self.deliver(conn, &message);
    }

    /// An event's frame from this client: its id, type and payload, made
    /// unsealed, as `message::frame_of` makes a plain event's for a client
    /// with a delivery set.
    pub(crate) fn event(&self, id: u64, r#type: u32, payload: &[u8]) -> super::outbox::Frame {
        let message = MultiplexerMessage {
            id: Some(id),
            sender: Some(self.core.instance_id),
            r#type,
            message: Some(Bytes::copy_from_slice(payload)),
            ..Default::default()
        };
        super::outbox::Frame {
            bytes: frame::encode_unsealed(&message).expect("a frame"),
            head: None,
            id,
            r#type,
            batch: super::batching::BatchMark::Event(None),
        }
    }

    /// Sends an event of id `id` and type 300, ONE way, waiting 10 s at
    /// most: true when the client took it.
    pub(crate) fn send(&mut self, id: u64) -> bool {
        let frame = self.event(id, 300, b"event");
        let taken = self.core.send(frame, false, None, Some(Duration::from_secs(10)), 0, None, None, None, false);
        self.settle();
        taken
    }

    /// Sends an event of id `id` and type 300: ONE way, through `lane` when
    /// given, or to ALL, waiting `timeout` at most; whether the client took
    /// it, and the trackers of its copies.
    pub(crate) fn send_with(
        &mut self,
        id: u64,
        timeout: Option<Duration>,
        all: bool,
        lane: Option<&super::lane::Lane>,
    ) -> (bool, Vec<Arc<super::Tracker>>) {
        let frame = self.event(id, 300, b"event");
        let mut trackers = Vec::new();
        let taken = self.core.send(frame, all, lane, timeout, 0, Some(&mut trackers), None, None, false);
        self.settle();
        (taken, trackers)
    }

    /// Installs a drop observer that keeps what it hears.
    pub(crate) fn observe_drops(&mut self) -> Arc<std::sync::Mutex<Vec<(u64, super::client::DropReason)>>> {
        let drops = Arc::new(std::sync::Mutex::new(Vec::new()));
        let kept = Arc::clone(&drops);
        self.core.set_drop_observer(Some(Box::new(move |id, reason| kept.lock().unwrap().push((id, reason)))));
        drops
    }

    /// The ids of the frames written on `conn` since the last call.
    pub(crate) fn written_ids(&mut self, conn: ConnId) -> Vec<u64> {
        self.written(conn).iter().map(|message| message.id.unwrap_or(0)).collect()
    }

    /// Completes every write in flight, again and again, until none is left.
    pub(crate) fn complete_writes(&mut self) {
        for _ in 0..100_000 {
            let Some(conn) = self.sockets.iter().find(|(_, socket)| socket.in_flight.is_some()).map(|(conn, _)| *conn)
            else {
                return;
            };
            self.complete_write(conn);
        }
        panic!("writes kept coming");
    }

    /// The ids of the messages queued for a receive.
    pub(crate) fn queued_ids(&self) -> Vec<u64> {
        self.core.client.incoming.iter().map(|incoming| incoming.message.id.unwrap_or(0)).collect()
    }

    /// Target `n` connected and registered: its connection.
    pub(crate) fn up(&mut self, n: u8) -> ConnId {
        let conn = self.connect(n);
        self.accept(conn);
        self.welcome(conn, u64::from(n));
        conn
    }

    /// The reading of `conn` ends.
    pub(crate) fn end(&mut self, conn: ConnId, end: ReadEnd) {
        self.core.on_read_end(conn, end);
        self.settle();
    }

    /// The write in flight on `conn` completes whole.
    pub(crate) fn complete_write(&mut self, conn: ConnId) {
        let bytes = self.sockets.get_mut(&conn).and_then(FakeSocket::take_in_flight).expect("a write in flight");
        self.sockets.get_mut(&conn).expect("socket").written.push(decode(&bytes));
        self.core.on_write_done(conn, bytes.len(), false);
        self.settle();
    }

    /// The write in flight on `conn` fails.
    pub(crate) fn fail_write(&mut self, conn: ConnId) {
        self.sockets.get_mut(&conn).and_then(FakeSocket::take_in_flight).expect("a write in flight");
        self.core.on_write_done(conn, 0, true);
        self.settle();
    }

    /// The frames written on `conn` since the last call, taken.
    pub(crate) fn written(&mut self, conn: ConnId) -> Vec<MultiplexerMessage> {
        std::mem::take(&mut self.sockets.entry(conn).or_default().written)
    }

    /// The types of the frames written on `conn` since the last call.
    pub(crate) fn written_types(&mut self, conn: ConnId) -> Vec<u32> {
        self.written(conn).iter().map(|message| message.r#type).collect()
    }

    /// A message from instance 77 to this client.
    pub(crate) fn message(&self, id: u64, r#type: u32, payload: &[u8]) -> MultiplexerMessage {
        MultiplexerMessage {
            id: Some(id),
            sender: Some(77),
            to: Some(self.core.instance_id),
            r#type,
            message: Some(Bytes::copy_from_slice(payload)),
            ..Default::default()
        }
    }

    /// The PEER_STATUS a multiplexer answers `control` with, the routing and
    /// filters it carried applied.
    pub(crate) fn peer_status(control: &MultiplexerMessage, multiplexer: u64) -> MultiplexerMessage {
        let request = crate::proto::multiplexer::PeerControl::decode(control.message.clone().unwrap_or_default())
            .expect("a PeerControl");
        let status = PeerStatus {
            multiplexer_id: Some(multiplexer),
            routing: Some(request.routing),
            error: None,
            key_filter: request.key_filter,
        };
        MultiplexerMessage {
            id: Some(control.id.unwrap_or(0) + 1),
            sender: Some(multiplexer),
            references: control.id,
            r#type: types::PEER_STATUS,
            message: Some(status.encode_to_vec().into()),
            ..Default::default()
        }
    }

    /// The routing a PEER_CONTROL carries.
    pub(crate) fn routing_of(control: &MultiplexerMessage) -> Routing {
        crate::proto::multiplexer::PeerControl::decode(control.message.clone().unwrap_or_default())
            .expect("a PeerControl")
            .routing
    }
}

/// A written frame's message, its header checked as the multiplexer checks
/// it: a frame written unsealed, or with a wrong length, fails the test.
pub(crate) fn decode(bytes: &Bytes) -> MultiplexerMessage {
    let header: [u8; HEADER_LENGTH] = bytes[..HEADER_LENGTH].try_into().expect("a header");
    let header = frame::unpack_header(header).expect("a header the multiplexer takes");
    assert!(
        frame::verify(header, &bytes[HEADER_LENGTH..]),
        "a frame written with a length or a checksum not its body's"
    );
    frame::decode(bytes.slice(HEADER_LENGTH..)).expect("the client writes whole messages")
}
