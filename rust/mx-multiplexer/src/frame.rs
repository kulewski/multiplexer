//! One frame on the wire: a header of two little-endian u32, the body's
//! length and its CRC-32, then the body, a serialized MultiplexerMessage.
//! Mirrors multiplexer/io/raw_message.h; docs/wire_format.md is the
//! reference.
//!
//! A frame is built where the message is sent, on the sender's thread, so
//! that a message over `MAX_MESSAGE_SIZE` or a key over `MAX_KEY_SIZE` is
//! refused at the call, as C++'s `RawMessage::FromMessage` refuses them; the
//! size is the one prost measures on the message itself. The frame is one
//! buffer, header and body, which every connection it goes to shares. A
//! body read from a connection decodes into a message whose payload is a
//! slice of that body, never a copy.

use bytes::{BufMut, Bytes, BytesMut};
use prost::Message as _;

use crate::crc32::crc32;
use crate::defaults::{MAX_KEY_SIZE, MAX_MESSAGE_SIZE};
use crate::error::{Error, Limit, Result};
use crate::proto::multiplexer::MultiplexerMessage;

/// Bytes of the header: the body's length and its CRC-32.
pub(crate) const HEADER_LENGTH: usize = 8;

/// Refuses a body over `MAX_MESSAGE_SIZE`, which the receiving side would
/// close the connection for (C++ `check_message_size`).
pub(crate) fn check_message_size(size: usize) -> Result<()> {
    if size > MAX_MESSAGE_SIZE {
        return Err(Error::TooLarge { what: Limit::Message, size, limit: MAX_MESSAGE_SIZE });
    }
    Ok(())
}

/// Refuses a key over `MAX_KEY_SIZE`, which the multiplexer would drop the
/// message for.
pub(crate) fn check_key_size(key: Option<&str>) -> Result<()> {
    let size = key.map_or(0, str::len);
    if size > MAX_KEY_SIZE {
        return Err(Error::TooLarge { what: Limit::Key, size, limit: MAX_KEY_SIZE });
    }
    Ok(())
}

/// `message` serialized into a frame, header and body in one buffer.
pub(crate) fn encode(message: &MultiplexerMessage) -> Result<Bytes> {
    check_key_size(message.key.as_deref())?;
    let size = message.encoded_len();
    check_message_size(size)?;
    let mut buffer = BytesMut::with_capacity(HEADER_LENGTH + size);
    buffer.put_bytes(0, HEADER_LENGTH);
    // A BytesMut grows as it is written to, so prost never runs out of room;
    // the error is its only other way to say so.
    message.encode(&mut buffer).map_err(|_| Error::TooLarge { what: Limit::Message, size, limit: MAX_MESSAGE_SIZE })?;
    let crc = crc32(&buffer[HEADER_LENGTH..]);
    buffer[..4].copy_from_slice(&(size as u32).to_le_bytes());
    buffer[4..HEADER_LENGTH].copy_from_slice(&crc.to_le_bytes());
    Ok(buffer.freeze())
}

/// A frame's header as read: the body's length and its CRC-32.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) length: usize,
    pub(crate) crc: u32,
}

/// Decodes a header; `None` for a length of 0 or above `MAX_MESSAGE_SIZE`,
/// after which the stream cannot be resynchronized and the connection is
/// closed (C++ `RawMessage::unpack_header`).
pub(crate) fn unpack_header(header: [u8; HEADER_LENGTH]) -> Option<Header> {
    let length = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    if length == 0 || length > MAX_MESSAGE_SIZE {
        return None;
    }
    Some(Header { length, crc })
}

/// Whether `body` is the body `header` announced, its CRC-32 the header's
/// (C++ `RawMessage::verify`).
pub(crate) fn verify(header: Header, body: &[u8]) -> bool {
    body.len() == header.length && crc32(body) == header.crc
}

/// The message in a verified body, its payload a slice of `body`; an error
/// for a body that is not a MultiplexerMessage, which closes the connection.
pub(crate) fn decode(body: Bytes) -> std::result::Result<MultiplexerMessage, prost::DecodeError> {
    MultiplexerMessage::decode(body)
}

/// Bytes of the reading buffer: frames whose body fits are copied out of
/// it, larger ones read into a buffer of their own.
pub(crate) const READ_BUFFER: usize = 64 * 1024;

/// What a reader has next: a frame's body, verified; the start of a frame
/// whose body is on its way; nothing yet; or input that ends the
/// connection.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Next {
    Frame(Bytes),
    Started,
    Nothing,
    BadHeader,
    BadCrc,
}

/// A large frame read into a buffer of its own: its header, the buffer and
/// how much of it is filled.
#[derive(Debug)]
struct Large {
    header: Header,
    body: Vec<u8>,
    filled: usize,
}

/// Frames out of a byte stream. One buffer, zeroed once, which a read
/// fills and frames are copied out of, so that one read may bring several
/// small frames and nothing is zeroed per read; a frame too large for it
/// gets a buffer of exactly its size, which reads fill directly and which
/// becomes the frame's body without a copy, as C++ reads each body into a
/// string of its size. Pure: the driver hands it what it reads. After a bad
/// header or a bad CRC, which end the connection, it gives nothing more:
/// the stream cannot be resynchronized.
#[derive(Debug)]
pub(crate) struct FrameReader {
    buffer: Vec<u8>,
    start: usize,
    end: usize,
    large: Option<Large>,
    started: bool,
    broken: bool,
}

impl Default for FrameReader {
    fn default() -> Self {
        FrameReader { buffer: vec![0; READ_BUFFER], start: 0, end: 0, large: None, started: false, broken: false }
    }
}

impl FrameReader {
    /// One read from `source` into the reader: the bytes read, 0 at the end
    /// of the stream.
    pub(crate) fn read_from(&mut self, source: &mut impl std::io::Read) -> std::io::Result<usize> {
        if let Some(large) = self.large.as_mut() {
            let read = source.read(&mut large.body[large.filled..])?;
            large.filled += read;
            return Ok(read);
        }
        if self.end == self.buffer.len() {
            self.compact();
        }
        let read = source.read(&mut self.buffer[self.end..])?;
        self.end += read;
        Ok(read)
    }

    /// Moves what is unread to the front of the buffer.
    fn compact(&mut self) {
        self.buffer.copy_within(self.start..self.end, 0);
        self.end -= self.start;
        self.start = 0;
    }

    /// What the reader has next; see `Next`. An error once, then nothing.
    pub(crate) fn next(&mut self) -> Next {
        if self.broken {
            return Next::Nothing;
        }
        let next = self.take();
        self.broken = matches!(next, Next::BadHeader | Next::BadCrc);
        next
    }

    /// `next`, before an error breaks the reader.
    fn take(&mut self) -> Next {
        if let Some(large) = &self.large {
            if large.filled < large.body.len() {
                return Next::Nothing;
            }
            let large = self.large.take().expect("checked");
            self.started = false;
            if !verify(large.header, &large.body) {
                return Next::BadCrc;
            }
            return Next::Frame(Bytes::from(large.body));
        }
        let available = self.end - self.start;
        if available < HEADER_LENGTH {
            return Next::Nothing;
        }
        let mut raw = [0u8; HEADER_LENGTH];
        raw.copy_from_slice(&self.buffer[self.start..self.start + HEADER_LENGTH]);
        let Some(header) = unpack_header(raw) else {
            return Next::BadHeader;
        };
        let whole = HEADER_LENGTH + header.length;
        if whole > self.buffer.len() {
            // A frame of its own: what of its body is here goes in first.
            let mut body = vec![0u8; header.length];
            let here = available - HEADER_LENGTH;
            body[..here].copy_from_slice(&self.buffer[self.start + HEADER_LENGTH..self.end]);
            self.start = 0;
            self.end = 0;
            self.large = Some(Large { header, body, filled: here });
            return self.next_started();
        }
        if available < whole {
            if self.start + whole > self.buffer.len() {
                self.compact();
            }
            return self.next_started();
        }
        let body = &self.buffer[self.start + HEADER_LENGTH..self.start + whole];
        self.start += whole;
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
        }
        self.started = false;
        if !verify(header, body) {
            return Next::BadCrc;
        }
        Next::Frame(Bytes::copy_from_slice(body))
    }

    /// `Started` once per frame whose body is on its way, `Nothing` after.
    fn next_started(&mut self) -> Next {
        if self.started {
            return Next::Nothing;
        }
        self.started = true;
        if let Some(large) = &self.large {
            if large.filled == large.body.len() {
                return self.take();
            }
        }
        Next::Started
    }
}

#[cfg(test)]
#[path = "frame_test.rs"]
mod tests;
