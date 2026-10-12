//! The I/O half of a client: its sockets, the waits for its timers, its
//! resolver thread and the wakeups from other threads, on mio, the reactor
//! tokio is built on, where the C++ library has asio. It carries out the
//! core's actions (`core::Action`) and feeds the core what happened, one
//! event per step, as asio runs one handler per `run_one()`: a synchronous
//! class steps it inside its calls until the call's condition holds, and a
//! threaded class's io thread steps it for good.
//!
//! Work reaches the core in passes, as asio runs its queue of handlers:
//! when the queue is empty, a pass adds each reading socket's next frame,
//! then a turn for the timers that came due, and every step runs the
//! queue's first entry; what the core posts, and a write's or a connect's
//! end, joins the back as it happens. So a frame already waiting in a socket
//! is handed over before a timer that came due in the same pass, which a
//! synchronous client that wakes after a long idle relies on: the frame
//! re-arms the silence timer before that timer can act. A timer cancelled
//! meanwhile never runs: the turn takes the timers due when it comes.
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
use std::io::{ErrorKind, Write};
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
use crate::frame::{FrameReader, Next};

/// The waker's token; sockets get the ones after it.
const WAKER: Token = Token(0);

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
    /// The frame being written and how much of it is written.
    write: Option<(Bytes, usize)>,
}

/// Commands from other threads, for a threaded class's io thread: what the
/// C++ client posts to its io_service.
#[derive(Default)]
pub(crate) struct Inbox {
    commands: Mutex<VecDeque<Posted>>,
    pending: AtomicBool,
}

impl Inbox {
    /// Queues `posted` for the io thread; the caller wakes it.
    pub(crate) fn push(&self, posted: Posted) {
        let mut commands = self.commands.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        commands.push_back(posted);
        self.pending.store(true, Ordering::Release);
    }

    /// Moves what was queued to the driver's queue, in order; one load when
    /// nothing was.
    fn drain_into(&self, ready: &mut VecDeque<Ready>) {
        if !self.pending.load(Ordering::Acquire) {
            return;
        }
        let mut commands = self.commands.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        self.pending.store(false, Ordering::Relaxed);
        ready.extend(commands.drain(..).map(Ready::Posted));
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
    /// Nothing can happen any more: no socket, no name being resolved, no
    /// timer and no posted work, and the call has no deadline.
    NoWork,
}

/// The driver; see the module comment.
pub(crate) struct Driver {
    poll: Poll,
    events: Events,
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
            waker,
            sockets: HashMap::new(),
            by_token: HashMap::new(),
            next_token: 1,
            turns: VecDeque::new(),
            ready: VecDeque::new(),
            resolving: HashSet::new(),
            resolver: None,
            inbox,
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
            if let Some(ready) = self.ready.pop_front() {
                self.run(core, ready);
                return Step::Ran;
            }
            if let Some(inbox) = &self.inbox {
                inbox.drain_into(&mut self.ready);
            }
            self.take_resolved();
            self.pull_pass();
            if core.timers.next_deadline().is_some_and(|due| due <= core.now) {
                self.ready.push_back(Ready::Timers);
            }
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
            self.wait(until.map(|until| until.saturating_duration_since(core.now)));
        }
    }

    /// Hands one entry of the queue to the core, then carries out what the
    /// core asked for.
    fn run(&mut self, core: &mut Core, ready: Ready) {
        match ready {
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
        if let Some(inbox) = &self.inbox {
            inbox.drain_into(&mut self.ready);
        }
        self.ready.extend(core.posted.drain(..).map(Ready::Posted));
    }

    /// Waits for readiness, `timeout` at most, and notes what is ready:
    /// connects ended, writes that may go on, sockets to read.
    fn wait(&mut self, timeout: Option<Duration>) {
        if let Err(error) = self.poll.poll(&mut self.events, timeout) {
            if error.kind() != ErrorKind::Interrupted {
                log::error!("waiting for the sockets failed: {error}");
            }
            return;
        }
        let ready: Vec<(Token, bool, bool)> = self
            .events
            .iter()
            .map(|event| {
                let failed = event.is_error();
                (
                    event.token(),
                    event.is_readable() || event.is_read_closed() || failed,
                    event.is_writable() || event.is_write_closed() || failed,
                )
            })
            .collect();
        for (token, readable, writable) in ready {
            if token == WAKER {
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
        let Some((bytes, mut written)) = socket.write.take() else { return };
        let failed = loop {
            if written == bytes.len() {
                break false;
            }
            match socket.stream.write(&bytes[written..]) {
                Ok(0) => break true,
                Ok(count) => written += count,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    socket.writable = false;
                    socket.write = Some((bytes, written));
                    return;
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(_) => break true,
            }
        };
        self.ready.push_back(Ready::WriteDone(conn, written, failed));
    }

    /// A pass over the sockets: each one that reads gives its next frame,
    /// the start of a large one, or the end of its reading, into the queue;
    /// the next pass starts at the next socket.
    fn pull_pass(&mut self) {
        let sockets: Vec<ConnId> = self.turns.iter().copied().collect();
        for conn in sockets {
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
                Next::BadCrc => {
                    socket.read_ended = true;
                    return Some(Pulled::End(ReadEnd::BadCrc));
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
            Action::Write(conn, bytes) => match self.sockets.get_mut(&conn) {
                Some(socket) => {
                    socket.write = Some((bytes, 0));
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
            if let Some((_, written)) = socket.write.take() {
                self.ready.push_back(Ready::WriteDone(conn, written, true));
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
