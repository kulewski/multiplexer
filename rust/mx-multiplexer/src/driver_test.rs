//! The driver's writes of a frame whose header is kept apart from its body
//! (`Writing`): whatever a socket takes at each call, the header goes
//! first, then the body, each byte once. And its reads: every whole frame
//! one read brought is handed over, the last with the word that the read is
//! drained, so that a threaded client confirms the confirmed batches of one
//! read in one CONFIRMATION, the test playing the multiplexer on a socket
//! of its own (C++ ConfirmedSends.WhatOneReadBroughtIsConfirmedTogether).

use std::io::{ErrorKind, IoSlice, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use prost::Message as _;

use super::Writing;
use crate::constants::{peers, types};
use crate::frame::{self, head_of, FrameReader, Next};
use crate::proto::multiplexer::{Confirmation, MessageBatch, MultiplexerMessage, WelcomeMessage};
use crate::ThreadedClient;

/// A socket that takes `each` bytes a call at most, vectored or not.
struct Trickle {
    taken: Vec<u8>,
    each: usize,
}

impl Write for Trickle {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let count = bytes.len().min(self.each);
        self.taken.extend_from_slice(&bytes[..count]);
        Ok(count)
    }

    fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> std::io::Result<usize> {
        let mut left = self.each;
        for slice in slices {
            let count = slice.len().min(left);
            self.taken.extend_from_slice(&slice[..count]);
            left -= count;
        }
        Ok(self.each - left)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_header_kept_apart_goes_first_however_the_socket_splits_the_frame() {
    let body = Bytes::from_static(b"the body of a frame");
    let head = head_of(&body);
    let whole = [&head[..], &body[..]].concat();
    for each in 1..=whole.len() {
        let mut writing = Writing { head: Some(head), bytes: body.clone(), written: 0 };
        let mut socket = Trickle { taken: Vec::new(), each };
        // Each call takes a byte at least: bounded by the frame's length.
        while writing.written < writing.length() {
            writing.written += writing.write_to(&mut socket).expect("written");
        }
        assert_eq!(whole, socket.taken, "{each} bytes a call");
    }
}

#[test]
fn a_whole_frame_is_written_as_it_is() {
    let frame = Bytes::from_static(b"header and body in one buffer");
    let mut writing = Writing { head: None, bytes: frame.clone(), written: 0 };
    let mut socket = Trickle { taken: Vec::new(), each: 4 };
    while writing.written < writing.length() {
        writing.written += writing.write_to(&mut socket).expect("written");
    }
    assert_eq!(&frame[..], &socket.taken[..]);
}

/// The next frame a client wrote on `stream`, read through `reader`: None
/// when none came within the stream's read timeout. Each turn reads a byte
/// at least or ends: bounded by what one frame holds.
fn next_frame(stream: &mut TcpStream, reader: &mut FrameReader) -> Option<MultiplexerMessage> {
    loop {
        match reader.next() {
            Next::Frame(body) => return frame::decode(body).ok(),
            Next::Started | Next::Nothing => {}
            Next::BadHeader | Next::BadChecksum => return None,
        }
        match reader.read_from(stream) {
            Ok(0) | Err(_) => return None,
            Ok(_) => {}
        }
    }
}

#[test]
fn what_one_read_brought_is_confirmed_together() {
    // A ThreadedClient, whose on_message runs as messages arrive, confirms
    // what one read brought in one CONFIRMATION: twenty confirmed batches
    // of one message each, from a producer that does not batch, which the
    // multiplexer the test plays writes in one write with its welcome,
    // where each batch cost a CONFIRMATION of its own.
    const PRODUCER: u64 = 0x7e57;
    const EVENT: u32 = 300;
    const COUNT: u64 = 20;
    let bound = Duration::from_secs(10); // what only a failure reaches
    crate::test_log::install();
    let listener = TcpListener::bind("127.0.0.1:0").expect("a listener");
    let address = listener.local_addr().expect("its address");
    let handled = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&handled);
    let consumer = ThreadedClient::builder(201)
        .on_message(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
        })
        .build()
        .expect("a client");
    let connected = consumer.connect(&address.to_string(), Some(bound)); // goes on as the test answers it
    listener.set_nonblocking(true).expect("a listener that does not block");
    let until = Instant::now() + bound;
    let mut multiplexer = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == ErrorKind::WouldBlock && Instant::now() < until => {
                std::thread::sleep(Duration::from_millis(1)); // a failure detector's bound
            }
            Err(error) => panic!("no connection from the client: {error}"),
        }
    };
    multiplexer.set_nonblocking(false).expect("a socket that blocks");
    multiplexer.set_read_timeout(Some(bound)).expect("a read timeout");
    let mut reader = FrameReader::default();
    let theirs = next_frame(&mut multiplexer, &mut reader).expect("the client's welcome");
    assert_eq!(types::CONNECTION_WELCOME, theirs.r#type);
    // The welcome and the twenty batches, in one write.
    let welcome = WelcomeMessage { r#type: peers::MULTIPLEXER, id: 0x5119, ..Default::default() };
    let welcome = MultiplexerMessage {
        id: Some(0),
        sender: Some(0x5119),
        r#type: types::CONNECTION_WELCOME,
        message: Some(welcome.encode_to_vec().into()),
        ..Default::default()
    };
    let mut bytes = frame::encode(&welcome).expect("a frame").to_vec();
    for index in 0..COUNT {
        let event = MultiplexerMessage {
            id: Some(0x2000 + index),
            sender: Some(PRODUCER),
            r#type: EVENT,
            message: Some(Bytes::from(format!("event {index}"))),
            ..Default::default()
        };
        let envelope = MultiplexerMessage {
            id: Some(0xe1e1_e1e1_0000_0000 + index),
            sender: Some(PRODUCER),
            r#type: EVENT,
            batch: Some(MessageBatch { message: vec![event.encode_to_vec().into()] }.encode_to_vec().into()),
            confirm: vec![index + 1, 1],
            ..Default::default()
        };
        bytes.extend_from_slice(&frame::encode(&envelope).expect("a frame"));
    }
    assert_eq!(bytes.len(), multiplexer.write(&bytes).expect("written"), "in one write");
    assert!(connected.wait().expect("the connect's end"), "registered");
    let mut numbers = Vec::new();
    let mut confirmations = 0;
    let until = Instant::now() + bound; // heartbeats come all the time: a deadline bounds the wait
    while (numbers.len() as u64) < COUNT {
        assert!(Instant::now() < until, "confirmed {} of {COUNT}", numbers.len());
        let message = next_frame(&mut multiplexer, &mut reader)
            .unwrap_or_else(|| panic!("confirmed {} of {COUNT}", numbers.len()));
        if message.r#type != types::CONFIRMATION {
            continue; // a heartbeat
        }
        assert_eq!(Some(PRODUCER), message.to);
        confirmations += 1;
        let confirmation = Confirmation::decode(message.message.unwrap_or_default()).expect("a Confirmation");
        for pair in confirmation.processed.chunks(2) {
            numbers.extend(pair[0]..pair[0] + pair[1]);
        }
    }
    assert_eq!((1..=COUNT).collect::<Vec<u64>>(), numbers);
    assert_eq!(1, confirmations, "one for what one read brought");
    assert_eq!(COUNT as usize, handled.load(Ordering::SeqCst));
    drop(multiplexer); // first: a shutdown would wait a while for an end that never closes
    consumer.shutdown(Some(Duration::ZERO)).wait().expect("shut down");
}
