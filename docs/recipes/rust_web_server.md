# Recipe: use the client from a Rust web server

A Rust web server on tokio, axum here, runs every request as a task on a
few worker threads, and a call that blocks one of them stalls every task
queued behind it. `ThreadedClient` fits: its queries and sends are
awaited, on tokio's executor as on any other, while its own io thread
keeps the connections; it needs no runtime of its own and starts none.
This page is the Rust counterpart of [the async web
server](async_web_server.md), with the calls of [the Rust
API](../api_rust.md#threadedclient-awaited-rusts-asyncclient).

```toml
[dependencies]
axum = "0.8"
futures-util = "0.3"
mx-multiplexer = "3.0"
tokio = { version = "1", features = ["macros", "net", "rt-multi-thread", "signal", "sync"] }
```

`multiplexer_constants.rs` is generated from your rules file, `mxcontrol
generate_constants your.rules --rust src/multiplexer_constants.rs`: a
peer type `WEB` the server connects as, a request type `LESSON_REQUEST`
its backends answer, and an event type `LESSON_EVENT` routed to every
`WEB` peer (`whom: ALL`).

## One client per process

A `ThreadedClient` is made once, before the server listens, and every
handler gets a handle: its handles are `Clone + Send + Sync`, clones of
one client and one io thread. `build()` starts the io thread without
waiting; the connections are awaited, so the runtime's thread is never
held. The peer type may be an active one: the io thread heartbeats.

```rust
use std::convert::Infallible;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use axum::routing::{get, post};
use axum::Router;
use futures_util::stream::{self, Stream, StreamExt};
use mx_multiplexer::{Error, ThreadedClient};
use tokio::sync::watch;

mod multiplexer_constants;
use multiplexer_constants::{peers, types};

/// What every handler gets: the process's one client, and whether the
/// server is leaving.
#[derive(Clone)]
struct App {
    mx: ThreadedClient,
    leaving: watch::Receiver<bool>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addresses: Vec<String> = std::env::var("MULTIPLEXERS")?.split(',').map(str::to_string).collect();
    let mx = ThreadedClient::builder(peers::WEB).build()?;
    mx.connect_all(&addresses, Some(Duration::from_secs(10))).await?;
    let (leave, leaving) = watch::channel(false);
    let app = Router::new()
        .route("/lesson", post(lesson))
        .route("/events/{session}", get(events))
        .with_state(App { mx: mx.clone(), leaving });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    let shutdown = async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = leave.send(true); // the event streams end; the queries in flight finish
    };
    axum::serve(listener, app).with_graceful_shutdown(shutdown).await?;
    mx.shutdown(Some(Duration::from_secs(1))).await?;
    Ok(())
}
```

## A handler

```rust
/// Answers the HTTP request with a query to a lesson backend.
async fn lesson(State(app): State<App>, body: Bytes) -> Result<Bytes, StatusCode> {
    match app.mx.query(types::LESSON_REQUEST, body).timeout(Duration::from_secs(10)).await {
        Ok(reply) => Ok(reply.message.payload),
        Err(Error::OperationTimedOut) => Err(StatusCode::GATEWAY_TIMEOUT),
        Err(Error::Backend(_)) => Err(StatusCode::BAD_GATEWAY),
        Err(_) => Err(StatusCode::SERVICE_UNAVAILABLE),
    }
}
```

The handler awaits the backend's reply without holding a worker thread:
the query runs on the client's io thread, which wakes the task when the
reply comes. axum's `Bytes` is the crate's `Bytes`, so the request's body
goes out as the payload and the reply's payload comes back as the
response's body, neither copied. The errors are those of [every
query](../api_rust.md#errors): `OperationTimedOut` when a stage ran out
of time, `OperationFailed` when no backend takes the request,
`NotConnected` with no multiplexer, `Backend` when the backend's handler
failed; each is an HTTP status here rather than a hang.

## Pushing to the browser

```rust
/// Streams the events of one session to the browser, as server-sent
/// events: those whose payload starts with the session's key, until the
/// server leaves.
async fn events(
    State(app): State<App>,
    Path(session): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let prefix = format!("{session} ").into_bytes();
    let subscription = app.mx.subscribe(types::LESSON_EVENT);
    let events = stream::unfold((subscription, prefix), |(subscription, prefix)| async move {
        loop {
            let incoming = subscription.next().await?; // None once the client was shut down
            if let Some(text) = incoming.message.payload.strip_prefix(prefix.as_slice()) {
                let event = Event::default().data(String::from_utf8_lossy(text));
                return Some((Ok(event), (subscription, prefix)));
            }
        }
    });
    let mut leaving = app.leaving;
    Sse::new(events.take_until(async move {
        let _ = leaving.wait_for(|leaving| *leaving).await;
    }))
}
```

Every browser shares the process's one client, so a backend sending an
event reaches the process, not the browser. The payload carries what
identifies the browser's session, its key here, and each event stream
takes the events of its session from a subscription of its own:
`subscribe()` gives every subscription of the type each event, and the
stream keeps those it wants. A subscription ends when its stream is
dropped, the browser gone. The event type goes to every `WEB` peer, so
whichever process holds the browser's stream gets the event. With many
processes, the [keys](../keys.md) of the events can do the choosing in
the multiplexer instead, each process taking the keys of its own
sessions with `set_key_filters()`.

## Backpressure and shutdown

The io thread hands messages to the streams and never waits for them. A
subscription holds `DEFAULT_INCOMING_QUEUE_MAX_SIZE`, 1024 messages, and
drops its oldest, with a warning, when nobody reads it, a browser that
reads slowly losing its oldest events rather than holding the process's.
A query waits on nothing but its reply: tasks that query faster than the
backends answer pile up as tasks, which the server's own limits bound.

At shutdown the order matters: axum's graceful shutdown waits for every
request in flight, and an event stream is one that never ends by itself,
so the server tells the streams first, through the `watch` channel, and
they end, while a query in flight still gets its reply. Once `serve()`
has returned, `shutdown()` writes what was sent, a second at most, and
closes the connections; awaiting it holds no thread. A process that exits
with the client alive and without dropping it loses what it had not
written ([lifetimes](../api_rust.md#lifetimes)).

## Testing

A test starts a multiplexer of its own and a backend on a thread, a
`BaseMultiplexerServer` serving a `Handler` that answers as the real
backend would, and then drives the router, over HTTP or with tower's
`oneshot`; [testing](../api_rust.md#testing) says how the crate's own
tests start a multiplexer. [Tests that hold up under
load](../api_python.md#tests-that-hold-up-under-load) applies unchanged.
