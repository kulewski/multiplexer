//! The I/O half of a client: its sockets, the waits for its timers, its
//! resolver thread and the wakeups from other threads, on mio, the reactor
//! tokio is built on, where the C++ library has asio. It carries out the
//! core's actions (`core::Action`) and feeds the core what happened, one
//! event per step, as asio runs one handler per `run_one()`: a synchronous
//! class steps it inside its calls until the call's condition holds, and a
//! threaded class's io thread steps it for good.
//!
//! Work reaches the core in passes, as asio runs its queue of handlers:
//! when the queue is empty, or every TURN entries run however full it is,
//! a pass adds the commands of other threads, each reading socket's next
//! frame, then a turn for the timers that came due, and every step runs the
//! queue's first entry, or, for a threaded client's plain sends, the run of
//! them at the queue's front, TURN at most, at the one time the step read;
//! what the core posts, and a write's or a connect's end, joins the back as
//! it happens. A pass first gives the reactor a turn, readiness polled with
//! no wait, when a timer is due or TURN entries ran since the last poll, as
//! asio polls once per cycle of its queue: a connect that ended or a frame
//! that arrived while a synchronous client did nothing is seen before a
//! timer that came due meanwhile, and one socket that always has a frame,
//! or a flow of other threads' commands, keeps no other socket unread, its
//! writes unresumed, or the timers waiting. So a frame already waiting in a
//! socket is handed over before a timer that came due in the same pass,
//! which a synchronous client that wakes after a long idle relies on: the
//! frame re-arms the silence timer before that timer can act, and the end
//! of its connect comes before the connect's deadline. A timer cancelled
//! meanwhile never runs: the turn takes the timers due when it comes. A
//! poll every TURN entries costs a syscall per TURN messages, none per
//! message.
//!
//! The commands of other threads wait in an inbox (`inbox`), which a pass
//! takes whole, handing back the bodies of plain sends the core is done
//! with; the driver tells it when it is about to wait for its sockets, so
//! that a sender wakes it only then.
//!
//! Readiness is edge-triggered, so a socket's flags are kept here and a
//! socket is read and written until it would block. A frame is pulled from
//! a socket one per pass, a read made only when the socket's buffer holds
//! no whole frame, so that a multiplexer that writes faster than the client
//! reads is slowed down by TCP, as in C++, and the sockets and the timers
//! take turns. A write starts at once, inside the step that asked for it,
//! and its end reaches the core in a later step, as asio posts a write's
//! handler. What counts as work, for a synchronous wait that must not wait
//! for nothing, is what asio counts: a socket open, a name being resolved,
//! work in the queue, and the core's own posted work and timers.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{ErrorKind, IoSlice, Write};
use std::net::{Shutdown, SocketAddr, ToSocketAddrs};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token, Waker};

use crate::core::connection::ReadEnd;
use crate::core::{Action, ConnId, Core, Posted};
use crate::frame::{FrameReader, Head, Next, HEADER_LENGTH};
use crate::inbox::Inbox;

/// The waker's token; sockets get the ones after it.
const WAKER: Token = Token(0);

/// The most entries a step runs between two passes, and between two polls
/// of the sockets' readiness; see the module comment.
pub(crate) const TURN: usize = 64;

/// Work ready for the core, in the order it became ready; see the module
/// comment.
enum Ready {
    Posted(Posted),
    Connected(ConnId, std::result::Result<(), String>),
    WriteDone(ConnId, usize, bool),
    Resolved(ConnId, std::result::Result<Vec<SocketAddr>, String>),
    Pulled(ConnId, Pulled),
    /// The timers' turn: the earliest due then runs.
    Timers,
}

/// What a socket gave next.
#[derive(Debug)]
enum Pulled {
    Frame(Bytes),
    Started,
    End(ReadEnd),
}

/// One connection's socket and what the driver knows of it.
#[derive(Debug)]
struct Socket {
    stream: TcpStream,
    token: Token,
    fd: i32,
    connecting: bool,
    reading: bool,
    readable: bool,
    writable: bool,
    read_ended: bool,
    reader: FrameReader,
    /// The frame being written.
    write: Option<Writing>,
}

/// A frame being written: its header when kept apart from its bytes
/// (`Core::alone_frame`), its bytes, and how much of the whole went to the
/// socket.
#[derive(Debug)]
struct Writing {
    head: Option<Head>,
    bytes: Bytes,
    written: usize,
}

impl Writing {
    /// The frame's length on the wire.
    fn length(&self) -> usize {
        self.head.map_or(0, |head| head.len()) + self.bytes.len()
    }

    /// One write of what is left of the frame: while some of a header kept
    /// apart is, the header and the bytes together, in one call.
    fn write_to(&self, stream: &mut impl Write) -> std::io::Result<usize> {
        match &self.head {
            Some(head) if self.written < HEADER_LENGTH => {
                stream.write_vectored(&[IoSlice::new(&head[self.written..]), IoSlice::new(&self.bytes)])
            }
            Some(_) => stream.write(&self.bytes[self.written - HEADER_LENGTH..]),
            None => stream.write(&self.bytes[self.written..]),
        }
    }
}

/// The resolver thread's results, which wake the driver.
type Resolved = Arc<Mutex<Vec<(ConnId, std::result::Result<Vec<SocketAddr>, String>)>>>;

/// The resolver thread, started at the first name to resolve: getaddrinfo
/// blocks, so names are resolved off the driver's thread, as asio does.
struct ResolverThread {
    requests: mpsc::Sender<(ConnId, String, u16)>,
    results: Resolved,
    /// Answers wait in `results`: one load per pass when none do.
    answered: Arc<AtomicBool>,
}

/// How a step ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// Something was handed to the core.
    Ran,
    /// The call's deadline came.
    Deadline,
    /// Nothing is ready, for a step that may not wait.
    Idle,
    /// Another thread woke the wait, and nothing came to run: a caller
    /// with a condition of its own looks at it again.
    Woken,
    /// Nothing can happen any more: no socket, no name being resolved, no
    /// timer and no posted work, and the call has no deadline.
    NoWork,
}

/// The driver; see the module comment.
pub(crate) struct Driver {
    poll: Poll,
    events: Events,
    fired: Vec<(Token, bool, bool)>, // what a wait's events say of each socket, kept from one wait to the next
    waker: Arc<Waker>,
    sockets: HashMap<ConnId, Socket>,
    by_token: HashMap<Token, ConnId>,
    next_token: usize,
    /// The sockets in the order they take turns to give frames.
    turns: VecDeque<ConnId>,
    ready: VecDeque<Ready>,
    resolving: HashSet<ConnId>,
    resolver: Option<ResolverThread>,
    inbox: Option<Arc<Inbox>>,
    /// What the inbox's queue is swapped with, empty between two takes.
    taken: VecDeque<Posted>,
    /// Entries run since the last pass, and since the last poll.
    since_pass: usize,
    since_poll: usize,
    /// The waker fired since a blocking step last looked.
    woken: bool,
}

impl std::fmt::Debug for Driver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Driver").field("sockets", &self.sockets.len()).finish_non_exhaustive()
    }
}

impl Driver {
    /// A driver with no socket yet; `inbox`, for a threaded class, carries
    /// the commands of other threads.
    pub(crate) fn new(inbox: Option<Arc<Inbox>>) -> std::io::Result<Driver> {
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKER)?);
        Ok(Driver {
            poll,
            events: Events::with_capacity(256),
            fired: Vec::new(),
            waker,
            sockets: HashMap::new(),
            by_token: HashMap::new(),
            next_token: 1,
            turns: VecDeque::new(),
            ready: VecDeque::new(),
            resolving: HashSet::new(),
            resolver: None,
            inbox,
            taken: VecDeque::new(),
            since_pass: 0,
            since_poll: 0,
            woken: false,
        })
    }

    /// What wakes the driver from another thread.
    pub(crate) fn waker(&self) -> Arc<Waker> {
        Arc::clone(&self.waker)
    }

    /// Whether the driver has work of its own: a socket open, a name being
    /// resolved, or work in its queue.
    pub(crate) fn has_work(&self) -> bool {
        !self.sockets.is_empty() || !self.resolving.is_empty() || !self.ready.is_empty()
    }

    /// Runs one entry of the queue, making a pass when it is empty, and
    /// waits for something to happen when the pass finds nothing and `block`
    /// says it may, until the core's next timer or `deadline`, the call's
    /// own. See `Step`.
    pub(crate) fn step(&mut self, core: &mut Core, deadline: Option<Instant>, block: bool) -> Step {
        let mut looked = false;
        loop {
            core.now = Instant::now();
            self.ready.extend(core.posted.drain(..).map(Ready::Posted));
            if self.since_pass < TURN {
                if let Some(ready) = self.ready.pop_front() {
                    self.since_pass += 1;
                    self.since_poll += 1;
                    self.run(core, ready);
                    return Step::Ran;
                }
            }
            self.pass(core);
            if !self.ready.is_empty() {
                continue;
            }
            if deadline.is_some_and(|deadline| deadline <= core.now) {
                return Step::Deadline;
            }
            if !block {
                if looked {
                    return Step::Idle;
                }
                looked = true;
                self.wait(Some(Duration::ZERO));
                continue;
            }
            if std::mem::take(&mut self.woken) {
                return Step::Woken;
            }
            if !self.has_work() && !core.has_work() && self.inbox.is_none() {
                let Some(deadline) = deadline else {
                    return Step::NoWork;
                };
                self.wait(Some(deadline.saturating_duration_since(core.now)));
                continue;
            }
            let until = match (core.timers.next_deadline(), deadline) {
                (Some(timer), Some(deadline)) => Some(timer.min(deadline)),
                (timer, deadline) => timer.or(deadline),
            };
            self.sleep(until.map(|until| until.saturating_duration_since(core.now)));
        }
    }

    /// Waits for readiness, `timeout` at most, as `wait` does, telling the
    /// threads that post, which wake it only now (`Inbox`); not at all when
    /// something was posted meanwhile.
    fn sleep(&mut self, timeout: Option<Duration>) {
        if let Some(inbox) = &self.inbox {
            if !inbox.fall_asleep() {
                return;
            }
        }
        self.wait(timeout);
        if let Some(inbox) = &self.inbox {
            inbox.wake_up();
        }
    }

    /// Takes what other threads posted into the queue, in order, handing
    /// back the bodies the core is done with.
    fn take_inbox(&mut self, core: &mut Core) {
        let Some(inbox) = &self.inbox else { return };
        inbox.take(&mut self.taken, &mut core.batcher.bodies);
        self.ready.extend(self.taken.drain(..).map(Ready::Posted));
    }

    /// A pass; see the module comment. Its work joins the back of the
    /// queue, behind what is there.
    fn pass(&mut self, core: &mut Core) {
        self.since_pass = 0;
        let timers_due = core.timers.next_deadline().is_some_and(|due| due <= core.now);
        if timers_due || self.since_poll >= TURN {
            self.wait(Some(Duration::ZERO));
        }
        self.take_inbox(core);
        self.take_resolved();
        self.pull_pass();
        if timers_due {
            self.ready.push_back(Ready::Timers);
        }
    }

    /// Hands one entry of the queue to the core, then carries out what the
    /// core asked for.
    fn run(&mut self, core: &mut Core, ready: Ready) {
        match ready {
            Ready::Posted(Posted::WriteOutStep) if self.commands_queued() => {
                // A command queued behind the shutdown's write-out step,
                // a callback's send among them, runs first: the step
                // looks at what was sent once they did, as the C++ client
                // numbers a callback's send at once.
                self.ready.push_back(Ready::Posted(Posted::WriteOutStep));
            }
            Ready::Posted(Posted::Plain(plain, deadline)) => {
                core.place_plain(plain, deadline);
                self.run_plains(core);
            }
            Ready::Posted(posted) => core.run_posted(posted),
            Ready::Connected(conn, result) => core.connected(conn, result),
            Ready::WriteDone(conn, written, failed) => core.on_write_done(conn, written, failed),
            Ready::Resolved(conn, result) => core.on_resolved(conn, result),
            Ready::Pulled(conn, Pulled::Frame(body)) => core.on_frame(conn, body),
            Ready::Pulled(conn, Pulled::Started) => core.on_frame_started(conn),
            Ready::Pulled(conn, Pulled::End(end)) => core.on_read_end(conn, end),
            Ready::Timers => {
                // The timers due in this pass go before what they post.
                if core.run_one_timer() && core.timers.next_deadline().is_some_and(|due| due <= core.now) {
                    self.ready.push_front(Ready::Timers);
                }
            }
        }
        self.apply(core);
        // What the io thread's own callbacks posted goes into the queue at
        // once, as an asio post would, ahead of what the core posted in
        // the same step: a send a callback makes while the shutdown writes
        // out goes out with it.
        self.take_inbox(core);
        self.ready.extend(core.posted.drain(..).map(Ready::Posted));
    }

    /// The plain sends right behind the one that ran, in the same step, at
    /// the time it read: one clock read for a run of them. Each counts as an
    /// entry, so a run ends at TURN, and the sockets and the timers get
    /// their turn as after any TURN entries.
    fn run_plains(&mut self, core: &mut Core) {
        while self.since_pass < TURN {
            let Some(Ready::Posted(Posted::Plain(..))) = self.ready.front() else { break };
            let Some(Ready::Posted(Posted::Plain(plain, deadline))) = self.ready.pop_front() else { break };
            self.since_pass += 1;
            self.since_poll += 1;
            core.place_plain(plain, deadline);
        }
    }

    /// Whether a command waits in the queue: once the shutdown began, only
    /// the io thread's own callbacks post any.
    fn commands_queued(&self) -> bool {
        self.ready.iter().any(|ready| matches!(ready, Ready::Posted(Posted::Command(_) | Posted::Plain(..))))
    }

    /// Waits for readiness, `timeout` at most, and notes what is ready:
    /// connects ended, writes that may go on, sockets to read.
    fn wait(&mut self, timeout: Option<Duration>) {
        self.since_poll = 0;
        if let Err(error) = self.poll.poll(&mut self.events, timeout) {
            if error.kind() != ErrorKind::Interrupted {
                log::error!("waiting for the sockets failed: {error}");
            }
            return;
        }
        // Copied out, since handling them changes the driver, into a
        // buffer kept from one wait to the next.
        let mut fired = std::mem::take(&mut self.fired);
        fired.clear();
        fired.extend(self.events.iter().map(|event| {
            let failed = event.is_error();
            (
                event.token(),
                event.is_readable() || event.is_read_closed() || failed,
                event.is_writable() || event.is_write_closed() || failed,
            )
        }));
        for &(token, readable, writable) in &fired {
            if token == WAKER {
                self.woken = true;
                continue;
            }
            let Some(conn) = self.by_token.get(&token).copied() else { continue };
            let Some(socket) = self.sockets.get_mut(&conn) else { continue };
            socket.readable |= readable;
            socket.writable |= writable;
            if socket.connecting && (writable || readable) {
                self.check_connect(conn);
            }
            if self.sockets.get(&conn).is_some_and(|socket| socket.write.is_some() && socket.writable) {
                self.write_some(conn);
            }
        }
        self.fired = fired;
    }

    /// Whether the connect under way on `conn` has ended, and how.
    fn check_connect(&mut self, conn: ConnId) {
        let Some(socket) = self.sockets.get_mut(&conn) else { return };
        let failed = match socket.stream.take_error() {
            Ok(Some(error)) | Err(error) => Some(error.to_string()),
            Ok(None) => match socket.stream.peer_addr() {
                Ok(_) => None,
                Err(error) if error.kind() == ErrorKind::NotConnected => return, // not yet
                Err(error) => Some(error.to_string()),
            },
        };
        socket.connecting = false;
        match failed {
            Some(why) => self.ready.push_back(Ready::Connected(conn, Err(why))),
            None => {
                // Every write is one whole frame: Nagle's algorithm has
                // nothing to coalesce and would only delay a small frame.
                let _ = socket.stream.set_nodelay(true);
                self.ready.push_back(Ready::Connected(conn, Ok(())));
            }
        }
    }

    /// Writes as much of `conn`'s frame as the socket takes; its end, whole
    /// or failed, waits for the core in a later step.
    fn write_some(&mut self, conn: ConnId) {
        let Some(socket) = self.sockets.get_mut(&conn) else { return };
        let Some(mut writing) = socket.write.take() else { return };
        let failed = loop {
            if writing.written == writing.length() {
                break false;
            }
            match writing.write_to(&mut socket.stream) {
                Ok(0) => break true,
                Ok(count) => writing.written += count,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    socket.writable = false;
                    socket.write = Some(writing);
                    return;
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(_) => break true,
            }
        };
        self.ready.push_back(Ready::WriteDone(conn, writing.written, failed));
    }

    /// A pass over the sockets: each one that reads gives its next frame,
    /// the start of a large one, or the end of its reading, into the queue;
    /// the next pass starts at the next socket.
    fn pull_pass(&mut self) {
        for turn in 0..self.turns.len() {
            let conn = self.turns[turn];
            if let Some(pulled) = self.pull_from(conn) {
                self.ready.push_back(Ready::Pulled(conn, pulled));
            }
        }
        if let Some(first) = self.turns.pop_front() {
            self.turns.push_back(first);
        }
    }

    /// What `conn`'s socket gives next, a read made only when its buffer
    /// holds nothing whole; None when it has nothing now.
    fn pull_from(&mut self, conn: ConnId) -> Option<Pulled> {
        let socket = self.sockets.get_mut(&conn)?;
        if !socket.reading || socket.read_ended {
            return None;
        }
        // Each turn of the loop reads at least a byte or ends it: bounded by
        // what the socket holds.
        loop {
            match socket.reader.next() {
                Next::Frame(body) => return Some(Pulled::Frame(body)),
                Next::Started => return Some(Pulled::Started),
                Next::BadHeader => {
                    socket.read_ended = true;
                    return Some(Pulled::End(ReadEnd::BadHeader));
                }
                Next::BadChecksum => {
                    socket.read_ended = true;
                    return Some(Pulled::End(ReadEnd::BadChecksum));
                }
                Next::Nothing => {}
            }
            if !socket.readable {
                return None;
            }
            match socket.reader.read_from(&mut socket.stream) {
                Ok(0) => {
                    socket.read_ended = true;
                    return Some(Pulled::End(ReadEnd::Eof));
                }
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    socket.readable = false;
                    return None;
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => {
                    socket.read_ended = true;
                    return Some(Pulled::End(ReadEnd::Error(error.to_string())));
                }
            }
        }
    }

    /// Carries out what a call into the core asked for, and takes the work
    /// it posted into the queue.
    pub(crate) fn carry_out_actions(&mut self, core: &mut Core) {
        self.apply(core);
        self.ready.extend(core.posted.drain(..).map(Ready::Posted));
    }

    /// Carries out every action the core asked for, in order.
    fn apply(&mut self, core: &mut Core) {
        while !core.actions.is_empty() {
            for action in std::mem::take(&mut core.actions) {
                self.carry_out(core, action);
            }
        }
    }

    /// Carries out one action; see `core::Action`.
    fn carry_out(&mut self, core: &mut Core, action: Action) {
        match action {
            Action::Resolve(conn, host, port) => self.resolve(conn, host, port),
            Action::Open(conn, address) => {
                self.close(core, conn);
                match TcpStream::connect(address) {
                    Ok(mut stream) => {
                        let token = Token(self.next_token);
                        self.next_token += 1;
                        let fd = stream.as_raw_fd();
                        if let Err(error) =
                            self.poll.registry().register(&mut stream, token, Interest::READABLE | Interest::WRITABLE)
                        {
                            core.open_failed(conn, error.to_string());
                            return;
                        }
                        core.client.descriptors.add(fd);
                        self.by_token.insert(token, conn);
                        self.turns.push_back(conn);
                        self.sockets.insert(
                            conn,
                            Socket {
                                stream,
                                token,
                                fd,
                                connecting: true,
                                reading: false,
                                readable: false,
                                writable: false,
                                read_ended: false,
                                reader: FrameReader::default(),
                                write: None,
                            },
                        );
                    }
                    Err(error) => core.open_failed(conn, error.to_string()),
                }
            }
            Action::CancelConnect(conn) => {
                if self.sockets.get(&conn).is_some_and(|socket| socket.connecting) {
                    self.close(core, conn);
                    self.ready.push_back(Ready::Connected(conn, Err("operation cancelled".to_string())));
                }
            }
            Action::StartReading(conn) => {
                if let Some(socket) = self.sockets.get_mut(&conn) {
                    socket.reading = true;
                }
            }
            Action::Write(conn, head, bytes) => match self.sockets.get_mut(&conn) {
                Some(socket) => {
                    socket.write = Some(Writing { head, bytes, written: 0 });
                    socket.writable = true; // tried at once: a write that would block waits for the event
                    self.write_some(conn);
                }
                None => self.ready.push_back(Ready::WriteDone(conn, 0, true)),
            },
            Action::ShutdownWrite(conn) => {
                self.end_write(conn);
                if let Some(socket) = self.sockets.get_mut(&conn) {
                    let _ = socket.stream.shutdown(Shutdown::Write);
                }
            }
            Action::Shutdown(conn) => {
                self.end_write(conn);
                if let Some(socket) = self.sockets.get_mut(&conn) {
                    let _ = socket.stream.shutdown(Shutdown::Both);
                    socket.reading = false;
                }
            }
            Action::Close(conn) => {
                self.end_write(conn);
                self.close(core, conn);
            }
            Action::CancelResolves => self.resolving.clear(),
        }
    }

    /// A write under way ends now, as written so far: the socket is shut.
    fn end_write(&mut self, conn: ConnId) {
        if let Some(socket) = self.sockets.get_mut(&conn) {
            if let Some(writing) = socket.write.take() {
                self.ready.push_back(Ready::WriteDone(conn, writing.written, true));
            }
        }
    }

    /// Closes `conn`'s socket, if it has one, forgotten by the table a
    /// forked child reads first.
    fn close(&mut self, core: &mut Core, conn: ConnId) {
        let Some(mut socket) = self.sockets.remove(&conn) else { return };
        let _ = self.poll.registry().deregister(&mut socket.stream);
        self.by_token.remove(&socket.token);
        self.turns.retain(|each| *each != conn);
        core.client.descriptors.remove(socket.fd);
        drop(socket);
    }

    /// Hands a name to the resolver thread, started now if it is not yet.
    fn resolve(&mut self, conn: ConnId, host: String, port: u16) {
        if self.resolver.is_none() {
            let (requests, incoming) = mpsc::channel::<(ConnId, String, u16)>();
            let results: Resolved = Arc::default();
            let answers = Arc::clone(&results);
            let answered = Arc::new(AtomicBool::new(false));
            let flag = Arc::clone(&answered);
            let waker = Arc::clone(&self.waker);
            let started = std::thread::Builder::new().name("mx-resolver".to_string()).spawn(move || {
                for (conn, host, port) in incoming {
                    let resolved = (host.as_str(), port)
                        .to_socket_addrs()
                        .map(|addresses| addresses.collect::<Vec<SocketAddr>>())
                        .map_err(|error| error.to_string());
                    answers.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).push((conn, resolved));
                    flag.store(true, Ordering::Release);
                    let _ = waker.wake();
                }
            });
            if let Err(error) = started {
                self.ready.push_back(Ready::Resolved(conn, Err(error.to_string())));
                return;
            }
            self.resolver = Some(ResolverThread { requests, results, answered });
        }
        let Some(resolver) = &self.resolver else { return };
        if resolver.requests.send((conn, host, port)).is_ok() {
            self.resolving.insert(conn);
        }
    }

    /// Moves the resolver's answers to the queue, those still waited for;
    /// the others, cancelled, go.
    fn take_resolved(&mut self) {
        let Some(resolver) = &self.resolver else { return };
        if !resolver.answered.swap(false, Ordering::Acquire) {
            return;
        }
        let answers = std::mem::take(&mut *resolver.results.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
        for (conn, resolved) in answers {
            if self.resolving.remove(&conn) {
                self.ready.push_back(Ready::Resolved(conn, resolved));
            }
        }
    }
}

#[cfg(test)]
#[path = "driver_test.rs"]
mod tests;
