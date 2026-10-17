//! One frame on the wire: a header of two little-endian u32, the body's
//! length and its checksum, the low 32 bits of its XXH3-64 (`xxh3`), then
//! the body, a serialized MultiplexerMessage.
//! Mirrors multiplexer/io/raw_message.h; docs/wire_format.md is the
//! reference.
//!
//! A frame is built where the message is sent, on the sender's thread, so
//! that a message over `MAX_MESSAGE_SIZE` or a key over `MAX_KEY_SIZE` is
//! refused at the call, as C++'s `RawMessage::FromMessage` refuses them; the
//! size is the one prost measures on the message itself. The frame is one
//! buffer, header and body, which every connection it goes to shares, but
//! for a big body the batching sends alone (`Core::alone_frame`): its
//! header is kept apart (`Head`) and written in the same call, so that the
//! body is not copied, as C++ writes every frame. A body read from a
//! connection decodes into a message whose payload is a slice of that body,
//! never a copy.
//!
//! A client that batches frames an event that may go in a batch without its
//! checksum (`encode_unsealed`): in a batch only its body is read, and its
//! own checksum would be a pass over every byte for nothing. One that goes alone
//! after all is sealed as it is placed (`seal`, in the outbox's
//! `Core::out_frame`), in place, the buffer being its own by then. A
//! batch's frame is serialized into a buffer the client keeps from an
//! earlier batch frame (`encode_into`).

use bytes::{BufMut, Bytes, BytesMut};
use prost::Message as _;

use crate::defaults::{MAX_KEY_SIZE, MAX_MESSAGE_SIZE};
use crate::error::{Error, Limit, Result};
use crate::proto::multiplexer::MultiplexerMessage;
use crate::xxh3::xxh3_64;

/// Bytes of the header: the body's length and its checksum.
pub(crate) const HEADER_LENGTH: usize = 8;

/// A frame's header kept apart from its body, which is then the frame's own
/// buffer: the length and the checksum `head_of` writes, written with the
/// body in one call (`Core::alone_frame`).
pub(crate) type Head = [u8; HEADER_LENGTH];

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
    let mut buffer = encode_body(message, message.key.as_deref())?;
    write_checksum(&mut buffer);
    Ok(buffer.freeze())
}

/// `message`, whose key is `key`, serialized into a frame in `buffer`,
/// which is emptied first and keeps what it can of its capacity: a
/// client's batch, whose envelope holds the batch itself, so that its
/// messages are encoded once, straight into the frame, the same bytes as a
/// MultiplexerMessage whose `batch` holds them encoded (`BatchEnvelope`,
/// multiplexer/Multiplexer.proto). Its sizes are checked as any frame's.
pub(crate) fn encode_into(message: &impl prost::Message, key: Option<&str>, mut buffer: Vec<u8>) -> Result<Vec<u8>> {
    check_key_size(key)?;
    let size = message.encoded_len();
    check_message_size(size)?;
    buffer.clear();
    buffer.reserve(HEADER_LENGTH + size);
    buffer.extend_from_slice(&[0; HEADER_LENGTH]);
    // A vector grows as it is written to, so prost never runs out of room;
    // the error is its only other way to say so.
    message.encode(&mut buffer).map_err(|_| Error::TooLarge { what: Limit::Message, size, limit: MAX_MESSAGE_SIZE })?;
    write_header(&mut buffer);
    Ok(buffer)
}

/// The header of a whole frame in `frame`, written over its first
/// HEADER_LENGTH bytes: the length of the body after them, and its
/// checksum.
pub(crate) fn write_header(frame: &mut [u8]) {
    let (header, body) = frame.split_at_mut(HEADER_LENGTH);
    header.copy_from_slice(&header_of(body));
}

/// The header of a frame whose body is `body`: its length and its
/// checksum.
fn header_of(body: &[u8]) -> Head {
    let mut header = [0; HEADER_LENGTH];
    header[..4].copy_from_slice(&(body.len() as u32).to_le_bytes());
    header[4..].copy_from_slice(&checksum(body).to_le_bytes());
    header
}

/// `encode` without the checksum, which `seal` writes if the frame goes alone:
/// the frame of an event that may go in a batch (see the module comment).
/// Its sizes are checked here, as any frame's.
pub(crate) fn encode_unsealed(message: &MultiplexerMessage) -> Result<Bytes> {
    Ok(encode_body(message, message.key.as_deref())?.freeze())
}

/// The frame of `encode_unsealed` with its checksum written: in place when
/// nothing else holds its buffer, as nothing does once a frame goes alone,
/// else in a copy.
pub(crate) fn seal(bytes: Bytes) -> Bytes {
    let mut buffer = BytesMut::from(bytes);
    write_checksum(&mut buffer);
    #[cfg(test)]
    SEALED.with(|sealed| sealed.set(sealed.get() + 1));
    buffer.freeze()
}

/// The frame of a serialized message, header and body, sealed: what a
/// message the batching kept as its body goes alone as, sealed as `seal`
/// seals.
pub(crate) fn frame_of_body(body: &[u8]) -> Bytes {
    let mut buffer = Vec::with_capacity(HEADER_LENGTH + body.len());
    buffer.extend_from_slice(&[0; HEADER_LENGTH]);
    buffer.extend_from_slice(body);
    write_header(&mut buffer);
    #[cfg(test)]
    SEALED.with(|sealed| sealed.set(sealed.get() + 1));
    Bytes::from(buffer)
}

/// The header of a body that goes alone, kept apart from it (`Head`):
/// sealed as `frame_of_body` seals, the body not copied.
pub(crate) fn head_of(body: &[u8]) -> Head {
    #[cfg(test)]
    SEALED.with(|sealed| sealed.set(sealed.get() + 1));
    header_of(body)
}

#[cfg(test)]
thread_local! {
    /// The frames `seal`, `frame_of_body` and `head_of` sealed on this
    /// thread, for the tests that count them.
    pub(crate) static SEALED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The header with the body's length and a checksum left 0, then the body.
fn encode_body(message: &impl prost::Message, key: Option<&str>) -> Result<BytesMut> {
    check_key_size(key)?;
    let size = message.encoded_len();
    check_message_size(size)?;
    let mut buffer = BytesMut::with_capacity(HEADER_LENGTH + size);
    buffer.put_bytes(0, HEADER_LENGTH);
    // A BytesMut grows as it is written to, so prost never runs out of room;
    // the error is its only other way to say so.
    message.encode(&mut buffer).map_err(|_| Error::TooLarge { what: Limit::Message, size, limit: MAX_MESSAGE_SIZE })?;
    buffer[..4].copy_from_slice(&(size as u32).to_le_bytes());
    Ok(buffer)
}

/// The checksum a frame's header carries for `body`: the low 32 bits of
/// its XXH3-64.
pub(crate) fn checksum(body: &[u8]) -> u32 {
    xxh3_64(body) as u32
}

/// The checksum of the frame's body, into its header.
fn write_checksum(buffer: &mut BytesMut) {
    let checksum = checksum(&buffer[HEADER_LENGTH..]);
    buffer[4..HEADER_LENGTH].copy_from_slice(&checksum.to_le_bytes());
}

/// A frame's header as read: the body's length and its checksum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) length: usize,
    pub(crate) checksum: u32,
}

/// Decodes a header; `None` for a length of 0 or above `MAX_MESSAGE_SIZE`,
/// after which the stream cannot be resynchronized and the connection is
/// closed (C++ `RawMessage::unpack_header`).
pub(crate) fn unpack_header(header: [u8; HEADER_LENGTH]) -> Option<Header> {
    let length = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let checksum = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    if length == 0 || length > MAX_MESSAGE_SIZE {
        return None;
    }
    Some(Header { length, checksum })
}

/// Whether `body` is the body `header` announced, its checksum the
/// header's (C++ `RawMessage::verify`).
pub(crate) fn verify(header: Header, body: &[u8]) -> bool {
    body.len() == header.length && checksum(body) == header.checksum
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
    BadChecksum,
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
/// header or a bad checksum, which end the connection, it gives nothing more:
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
        self.broken = matches!(next, Next::BadHeader | Next::BadChecksum);
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
                return Next::BadChecksum;
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
            return Next::BadChecksum;
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
