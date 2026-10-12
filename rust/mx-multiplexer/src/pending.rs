//! What a threaded class's caller waits on, written on `std` alone so that
//! the crate imposes no runtime: `Pending`, one result the io thread
//! delivers later, waited for with `wait()` from any thread but the io
//! thread, or awaited on any executor; and `Messages`, a queue of messages
//! the io thread fills, read with `recv()` or awaited with `next()`.
//!
//! A result is delivered by its `Completer`; one dropped without
//! delivering, the io thread gone, delivers `NotConnected`, so that no
//! caller waits for good. A queue holds `DEFAULT_INCOMING_QUEUE_MAX_SIZE`
//! messages and drops its oldest, with a warning, when nobody reads it, as
//! the Python `AsyncClient.messages()` does; once the client is shut down
//! its readers get what it holds and then the end.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::handles::Incoming;

/// Locks `mutex`, which nothing panics under.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One result and who waits for it.
struct Slot<T> {
    state: Mutex<(Option<Result<T>>, Option<Waker>)>,
    ready: Condvar,
}

/// What the io thread delivers a `Pending`'s result through, once.
pub(crate) struct Completer<T>(Option<Arc<Slot<T>>>);

impl<T> Completer<T> {
    /// Delivers `result`, waking whoever waits.
    pub(crate) fn complete(mut self, result: Result<T>) {
        if let Some(slot) = self.0.take() {
            deliver(&slot, result);
        }
    }
}

impl<T> Drop for Completer<T> {
    /// Dropped without a result: the io thread is gone, NotConnected.
    fn drop(&mut self) {
        if let Some(slot) = self.0.take() {
            deliver(&slot, Err(Error::NotConnected));
        }
    }
}

/// Puts `result` in `slot`, a first result kept, and wakes the waiters.
fn deliver<T>(slot: &Slot<T>, result: Result<T>) {
    let waker = {
        let mut state = lock(&slot.state);
        if state.0.is_none() {
            state.0 = Some(result);
        }
        state.1.take()
    };
    slot.ready.notify_all();
    if let Some(waker) = waker {
        waker.wake();
    }
}

/// A result the client's io thread delivers later: `wait()` blocks for it,
/// and `.await` waits for it on any executor.
#[must_use = "a result to wait for or await"]
pub struct Pending<T> {
    slot: Arc<Slot<T>>,
    io_thread: Option<ThreadId>,
}

impl<T> std::fmt::Debug for Pending<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Pending").field("ready", &lock(&self.slot.state).0.is_some()).finish()
    }
}

/// A pending result and its completer; `io_thread` is the thread whose
/// `wait()` would wait for good.
pub(crate) fn pending<T>(io_thread: Option<ThreadId>) -> (Pending<T>, Completer<T>) {
    let slot = Arc::new(Slot { state: Mutex::new((None, None)), ready: Condvar::new() });
    (Pending { slot: Arc::clone(&slot), io_thread }, Completer(Some(slot)))
}

/// A result known at once.
pub(crate) fn ready<T>(result: Result<T>) -> Pending<T> {
    let (pending, completer) = pending(None);
    completer.complete(result);
    pending
}

impl<T> Pending<T> {
    /// Blocks until the result is there. `OnIoThread` on the client's own
    /// io thread, from one of its callbacks, where it would wait for good.
    pub fn wait(self) -> Result<T> {
        if self.io_thread == Some(std::thread::current().id()) && lock(&self.slot.state).0.is_none() {
            return Err(Error::OnIoThread);
        }
        let mut state = lock(&self.slot.state);
        loop {
            if let Some(result) = state.0.take() {
                return result;
            }
            state = self.slot.ready.wait(state).unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// Whether the result is there.
    pub fn is_ready(&self) -> bool {
        lock(&self.slot.state).0.is_some()
    }
}

impl<T> Future for Pending<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<T>> {
        let mut state = lock(&self.slot.state);
        if let Some(result) = state.0.take() {
            return Poll::Ready(result);
        }
        state.1 = Some(context.waker().clone());
        Poll::Pending
    }
}

/// A queue of messages the io thread fills; see the module comment.
pub(crate) struct Queue {
    state: Mutex<QueueState>,
    ready: Condvar,
    capacity: usize,
    /// What the warning about a dropped message names it: the inbox, or a
    /// subscription.
    name: String,
}

struct QueueState {
    messages: VecDeque<Incoming>,
    wakers: Vec<Waker>,
    closed: bool,
    dropped: u64,
}

impl Queue {
    /// An empty queue of `capacity`, `name`d for its warnings.
    pub(crate) fn new(capacity: usize, name: String) -> Arc<Queue> {
        Arc::new(Queue {
            state: Mutex::new(QueueState { messages: VecDeque::new(), wakers: Vec::new(), closed: false, dropped: 0 }),
            ready: Condvar::new(),
            capacity: capacity.max(1),
            name,
        })
    }

    /// Adds `message`, the oldest dropped with a warning when the queue is
    /// full; nothing once it is closed.
    pub(crate) fn push(&self, message: Incoming) {
        let wakers = {
            let mut state = lock(&self.state);
            if state.closed {
                return;
            }
            if state.messages.len() >= self.capacity {
                if let Some(oldest) = state.messages.pop_front() {
                    state.dropped += 1;
                    log::warn!(
                        "{} is full ({} messages): the oldest, #{} of type {}, dropped",
                        self.name,
                        self.capacity,
                        oldest.message.id,
                        oldest.message.r#type
                    );
                }
            }
            state.messages.push_back(message);
            std::mem::take(&mut state.wakers)
        };
        self.ready.notify_one();
        for waker in wakers {
            waker.wake();
        }
    }

    /// Closes the queue: its readers get what it holds, then the end.
    pub(crate) fn close(&self) {
        let wakers = {
            let mut state = lock(&self.state);
            state.closed = true;
            std::mem::take(&mut state.wakers)
        };
        self.ready.notify_all();
        for waker in wakers {
            waker.wake();
        }
    }
}

/// A stream of the messages a threaded client hands out: its inbox,
/// `messages()`, or a subscription, `subscribe()`. Several readers share
/// one, each message going to one of them. `recv()` blocks for the next,
/// `next().await` awaits it; both give None once the client is shut down
/// and the stream is empty. Dropping a subscription ends it.
pub struct Messages {
    queue: Arc<Queue>,
}

impl std::fmt::Debug for Messages {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Messages").field("name", &self.queue.name).finish_non_exhaustive()
    }
}

impl Messages {
    /// The stream of `queue`.
    pub(crate) fn new(queue: Arc<Queue>) -> Messages {
        Messages { queue }
    }

    /// The next message, waiting `timeout` at most, None for no deadline;
    /// None when the time ran out, or the client was shut down and the
    /// stream is empty.
    pub fn recv(&self, timeout: Option<Duration>) -> Option<Incoming> {
        let until = crate::core::deadline_after(Instant::now(), timeout);
        let mut state = lock(&self.queue.state);
        loop {
            if let Some(message) = state.messages.pop_front() {
                return Some(message);
            }
            if state.closed {
                return None;
            }
            match until {
                None => state = self.queue.ready.wait(state).unwrap_or_else(|poisoned| poisoned.into_inner()),
                Some(until) => {
                    let left = until.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return None;
                    }
                    state =
                        self.queue.ready.wait_timeout(state, left).unwrap_or_else(|poisoned| poisoned.into_inner()).0;
                }
            }
        }
    }

    /// The next message now, if one is there.
    pub fn try_recv(&self) -> Option<Incoming> {
        lock(&self.queue.state).messages.pop_front()
    }

    /// The next message, awaited on any executor; None once the client was
    /// shut down and the stream is empty.
    pub fn next(&self) -> Next<'_> {
        Next { messages: self }
    }

    /// How many messages the stream dropped, its oldest, for want of a
    /// reader.
    pub fn dropped(&self) -> u64 {
        lock(&self.queue.state).dropped
    }
}

/// The future `Messages::next()` returns.
#[must_use = "a future to await"]
pub struct Next<'stream> {
    messages: &'stream Messages,
}

impl Future for Next<'_> {
    type Output = Option<Incoming>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Incoming>> {
        let mut state = lock(&self.messages.queue.state);
        if let Some(message) = state.messages.pop_front() {
            return Poll::Ready(Some(message));
        }
        if state.closed {
            return Poll::Ready(None);
        }
        state.wakers.push(context.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    //! The one-shot results and the queues, without a client: delivered
    //! once, given up for, bounded; awaited through a waker the test counts.

    use super::{pending, ready, Messages, Queue};
    use crate::error::Error;
    use std::future::Future;
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    /// A waker that counts its wakes.
    struct Counting(AtomicUsize);

    impl Wake for Counting {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_result_is_delivered_once_and_a_completer_dropped_says_not_connected() {
        let (first, completer) = pending::<u32>(None);
        completer.complete(Ok(7));
        assert_eq!(Ok(7), first.wait());
        let (second, completer) = pending::<u32>(None);
        drop(completer);
        assert_eq!(Err(Error::NotConnected), second.wait());
        assert_eq!(Ok(3), ready(Ok(3)).wait());
    }

    #[test]
    fn a_wait_on_the_io_thread_itself_is_refused() {
        let (pending, _completer) = pending::<u32>(Some(std::thread::current().id()));
        assert_eq!(Err(Error::OnIoThread), pending.wait());
    }

    #[test]
    fn an_awaited_result_wakes_its_task_once() {
        let counting = Arc::new(Counting(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counting));
        let mut context = Context::from_waker(&waker);
        let (pending, completer) = pending::<u32>(None);
        let mut pending = pin!(pending);
        assert!(pending.as_mut().poll(&mut context).is_pending());
        completer.complete(Ok(5));
        assert_eq!(1, counting.0.load(Ordering::SeqCst));
        assert_eq!(Poll::Ready(Ok(5)), pending.as_mut().poll(&mut context));
    }

    #[test]
    fn a_full_queue_drops_its_oldest_and_a_closed_one_ends_after_what_it_holds() {
        let queue = Queue::new(2, "the test's inbox".to_string());
        let messages = Messages::new(Arc::clone(&queue));
        for id in 1..=3 {
            queue.push(crate::handles::tests::incoming(id));
        }
        assert_eq!(1, messages.dropped());
        queue.close();
        queue.push(crate::handles::tests::incoming(4));
        assert_eq!(Some(2), messages.recv(None).map(|incoming| incoming.message.id));
        assert_eq!(Some(3), messages.recv(None).map(|incoming| incoming.message.id));
        assert_eq!(None, messages.recv(None));
    }
}
