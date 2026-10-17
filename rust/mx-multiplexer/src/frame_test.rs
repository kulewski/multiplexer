//! The frame tests: the golden frames the Python package's protobuf wrote
//! (testdata/golden.py), encoded byte for byte and decoded back; the limits
//! a frame is refused at, where the message is built; the header's refusals
//! and the checksum check a received body goes through; a header kept apart
//! from its body, the header a whole frame carries.

use bytes::Bytes;
use prost::Message as _;

use super::{
    check_key_size, decode, encode, encode_into, frame_of_body, head_of, unpack_header, verify, Header, HEADER_LENGTH,
};
use crate::defaults::{MAX_KEY_SIZE, MAX_MESSAGE_SIZE};
use crate::error::{Error, Limit};
use crate::proto::multiplexer::multiplexer_message_description::routing_rule::Whom;
use crate::proto::multiplexer::multiplexer_message_description::RoutingRule;
use crate::proto::multiplexer::{
    compression, logging_method, BackendForPacketSearch, BatchEnvelope, DeliveryError, KeyFilter, MessageBatch,
    MultiplexerMessage, PeerControl, PeerStatus, Routing, WelcomeMessage,
};

const MAX_U64: u64 = u64::MAX;

/// `size` bytes counting up from 0, wrapping at 256, as golden.py's `payload`.
fn payload(size: usize) -> Bytes {
    (0..size).map(|index| (index % 256) as u8).collect::<Vec<u8>>().into()
}

/// golden.py's `key_filters`.
fn key_filters() -> Vec<KeyFilter> {
    vec![
        KeyFilter { r#type: 200, pattern: vec!["user-[0-9]+".into(), String::new()] },
        KeyFilter { r#type: 4_294_967_295, pattern: vec![] },
    ]
}

/// A message with its id, sender, type and payload, the rest unset.
fn message(id: u64, sender: u64, r#type: u32, payload: Vec<u8>) -> MultiplexerMessage {
    MultiplexerMessage {
        id: Some(id),
        sender: Some(sender),
        r#type,
        message: Some(payload.into()),
        ..Default::default()
    }
}

/// golden.py's `messages`, case for case, by the same names.
fn golden_messages() -> Vec<(&'static str, MultiplexerMessage)> {
    let every_field = MultiplexerMessage {
        id: Some(MAX_U64),
        sender: Some(0x0123_4567_89AB_CDEF),
        to: Some(MAX_U64 - 1),
        report_delivery_error: Some(true),
        include_original_packet_in_report: Some(false),
        r#type: 4_294_967_295,
        message: Some(payload(300)),
        compression: Some(compression::Values::Gzip as i32),
        timestamp: Some(4_294_967_295),
        references: Some(1),
        workflow: Some("\u{0}\u{ff}workflow".into()),
        override_rrules: vec![
            RoutingRule {
                peer: Some("backend".into()),
                peer_type: Some(7),
                whom: Some(Whom::All as i32),
                delivery_error_is_error: Some(false),
                report_delivery_error: Some(false),
                include_original_packet_in_report: Some(true),
            },
            RoutingRule::default(),
        ],
        logging_method: Some(logging_method::Values::Console as i32),
        key: Some("ключ-κλειδί".into()),
        batch: None, // golden.py's every_field has none either
    };
    let welcome = WelcomeMessage {
        r#type: 65,
        id: MAX_U64,
        routing: Some(Routing { any: Some(false), all: Some(true), last_resort: Some(true) }),
        key_filter: key_filters(),
    };
    let original = MultiplexerMessage {
        id: Some(3),
        sender: Some(4),
        to: Some(5),
        r#type: 300,
        message: Some(Bytes::from_static(b"original")),
        key: Some("k".into()),
        ..Default::default()
    };
    let delivery_error = DeliveryError {
        packet_id: MAX_U64,
        failed_type: vec![1, 300, 4_294_967_295],
        failed_to: Some(9),
        is_known_type: Some(true),
        original_message: Some(original),
        original_message_omitted: Some(false),
    };
    let peer_control = PeerControl { routing: Routing::default(), key_filter: key_filters() };
    let peer_status = PeerStatus {
        multiplexer_id: Some(20),
        routing: Some(Routing { last_resort: Some(true), ..Default::default() }),
        error: Some("refused".into()),
        key_filter: key_filters(),
    };
    let mut search = message(21, 22, 3, BackendForPacketSearch { packet_type: 300 }.encode_to_vec());
    search.key = Some("a".into());
    vec![
        ("minimal", MultiplexerMessage { r#type: 0, ..Default::default() }),
        ("ping", MultiplexerMessage { id: Some(1), sender: Some(2), r#type: 1, ..Default::default() }),
        ("empty_payload", message(1, 2, 100, vec![])),
        ("every_field", every_field),
        ("large_payload", message(10, 11, 101, payload(20000).to_vec())),
        ("welcome", message(12, 13, 2, welcome.encode_to_vec())),
        ("welcome_bare", message(14, 15, 2, WelcomeMessage { r#type: 1, id: 0, ..Default::default() }.encode_to_vec())),
        ("peer_control", message(16, 17, 11, peer_control.encode_to_vec())),
        ("peer_status", message(18, 19, 12, peer_status.encode_to_vec())),
        ("search", search),
        ("delivery_error", MultiplexerMessage { to: Some(25), ..message(23, 24, 5, delivery_error.encode_to_vec()) }),
        (
            "batch",
            MultiplexerMessage {
                id: Some(26),
                sender: Some(27),
                r#type: 300,
                key: Some("shard.1".into()),
                batch: Some(golden_batch().encode_to_vec().into()),
                ..Default::default()
            },
        ),
    ]
}

/// The batch of golden.py's "batch" case: two events of type 300, keyed.
fn golden_batch() -> MessageBatch {
    let event = |id: u64, payload: &[u8]| MultiplexerMessage {
        key: Some("shard.1".into()),
        ..message(id, 27, 300, payload.to_vec())
    };
    MessageBatch {
        message: vec![event(28, b"first").encode_to_vec().into(), event(29, b"second").encode_to_vec().into()],
    }
}

/// The bytes of a line's hex.
fn unhex(text: &str) -> Vec<u8> {
    (0..text.len()).step_by(2).map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap()).collect()
}

/// testdata/golden_frames.txt's frames by name.
fn golden_frames() -> Vec<(String, Vec<u8>)> {
    include_str!("../testdata/golden_frames.txt")
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| {
            let (name, hex) = line.split_once(' ').unwrap();
            (name.to_string(), unhex(hex))
        })
        .collect()
}

#[test]
fn every_golden_frame_is_encoded_byte_for_byte() {
    let frames = golden_frames();
    let messages = golden_messages();
    let names: Vec<&str> = frames.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(names, messages.iter().map(|(name, _)| *name).collect::<Vec<_>>(), "the same cases in the same order");
    for ((name, frame), (_, message)) in frames.iter().zip(&messages) {
        assert_eq!(&encode(message).unwrap()[..], &frame[..], "{name}");
    }
}

/// A client's batch goes from a BatchEnvelope, its MessageBatch inside it
/// encoded once, straight into the frame: byte for byte the frame of the
/// MultiplexerMessage whose `batch` holds the same MessageBatch encoded,
/// which the Python package's protobuf wrote; and so does the envelope the
/// client keeps, whose messages are the vectors they were serialized into,
/// written into a buffer that held a frame before.
#[test]
fn a_batch_envelope_is_the_frame_of_its_message() {
    use crate::core::bodies::{BodiesBatch, BodiesEnvelope};
    let (_, golden) = golden_frames().into_iter().find(|(name, _)| name == "batch").expect("golden.py's batch");
    let key = Some("shard.1".to_string());
    let envelope =
        BatchEnvelope { id: Some(26), sender: Some(27), r#type: 300, key: key.clone(), batch: Some(golden_batch()) };
    assert_eq!(&encode_into(&envelope, key.as_deref(), Vec::new()).unwrap()[..], &golden[..]);
    let bodies = golden_batch().message.iter().map(|body| body.to_vec()).collect();
    let kept = BodiesEnvelope {
        id: Some(26),
        sender: Some(27),
        r#type: 300,
        key: key.clone(),
        batch: Some(BodiesBatch { message: bodies }),
    };
    let used = vec![0xEE; golden.len() * 2]; // what an earlier frame left in it is overwritten
    assert_eq!(&encode_into(&kept, key.as_deref(), used).unwrap()[..], &golden[..]);
}

#[test]
fn every_golden_frame_decodes_to_its_message() {
    for ((name, frame), (_, message)) in golden_frames().into_iter().zip(golden_messages()) {
        let header = unpack_header(frame[..HEADER_LENGTH].try_into().unwrap()).unwrap();
        let body = Bytes::from(frame[HEADER_LENGTH..].to_vec());
        assert!(verify(header, &body), "{name}");
        assert_eq!(decode(body).unwrap(), message, "{name}");
    }
}

#[test]
fn a_decoded_payload_is_a_slice_of_the_body() {
    let frame = encode(&message(1, 2, 100, vec![7; 1000])).unwrap();
    let body = frame.slice(HEADER_LENGTH..);
    let range = body.as_ptr() as usize..body.as_ptr() as usize + body.len();
    let payload = decode(body).unwrap().message.unwrap();
    assert!(range.contains(&(payload.as_ptr() as usize)), "the payload points into the body");
}

#[test]
fn the_declared_defaults_come_back_through_the_getters() {
    let routing = Routing::decode(&b""[..]).unwrap();
    assert!(routing.any() && routing.all() && !routing.last_resort());
    let empty = MultiplexerMessage::decode(&[0x20, 0x01][..]).unwrap();
    assert_eq!(logging_method::Values::Both, empty.logging_method());
    assert!(!empty.report_delivery_error() && empty.to.is_none() && empty.message.is_none());
}

#[test]
fn a_message_at_the_limit_is_encoded_and_one_over_it_refused() {
    let mut message = message(1, 2, 100, vec![]);
    // The payload's length takes four bytes at this size, three more than an empty one's.
    let overhead = message.encoded_len() + 3;
    message.message = Some(vec![0; MAX_MESSAGE_SIZE - overhead].into());
    assert_eq!(MAX_MESSAGE_SIZE, message.encoded_len());
    assert_eq!(HEADER_LENGTH + MAX_MESSAGE_SIZE, encode(&message).unwrap().len());
    message.references = Some(0); // two bytes more: the field's tag and its value
    assert_eq!(
        Err(Error::TooLarge { what: Limit::Message, size: MAX_MESSAGE_SIZE + 2, limit: MAX_MESSAGE_SIZE }),
        encode(&message)
    );
}

#[test]
fn a_key_at_the_limit_is_taken_and_one_byte_over_it_refused() {
    let mut message = message(1, 2, 100, vec![]);
    message.key = Some("k".repeat(MAX_KEY_SIZE));
    assert!(encode(&message).is_ok());
    // A key's limit is its UTF-8 bytes: 129 two-byte letters are 258 bytes.
    message.key = Some("ж".repeat(MAX_KEY_SIZE / 2 + 1));
    assert_eq!(
        Err(Error::TooLarge { what: Limit::Key, size: MAX_KEY_SIZE + 2, limit: MAX_KEY_SIZE }),
        encode(&message)
    );
    assert!(check_key_size(None).is_ok());
}

#[test]
fn a_header_of_length_zero_or_over_the_limit_is_refused() {
    let header = |length: u32| {
        let mut bytes = [0u8; HEADER_LENGTH];
        bytes[..4].copy_from_slice(&length.to_le_bytes());
        bytes[4..].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        unpack_header(bytes)
    };
    assert_eq!(None, header(0));
    assert_eq!(Some(Header { length: 1, checksum: 0xDEAD_BEEF }), header(1));
    assert_eq!(Some(Header { length: MAX_MESSAGE_SIZE, checksum: 0xDEAD_BEEF }), header(MAX_MESSAGE_SIZE as u32));
    assert_eq!(None, header(MAX_MESSAGE_SIZE as u32 + 1));
    assert_eq!(None, header(u32::MAX));
}

#[test]
fn a_body_whose_checksum_or_length_differs_is_refused() {
    let frame = encode(&message(1, 2, 100, b"payload".to_vec())).unwrap();
    let header = unpack_header(frame[..HEADER_LENGTH].try_into().unwrap()).unwrap();
    let mut body = frame[HEADER_LENGTH..].to_vec();
    assert!(verify(header, &body));
    body[3] ^= 1;
    assert!(!verify(header, &body));
    body[3] ^= 1;
    body.push(0);
    assert!(!verify(header, &body));
}

#[test]
fn a_body_that_is_not_a_message_is_an_error() {
    // A field of wire type 7, which does not exist.
    assert!(decode(Bytes::from_static(&[0x0F])).is_err());
    // A length past the end of the body.
    assert!(decode(Bytes::from_static(&[0x2A, 0x05, 0x01])).is_err());
}

/// A reader fed `chunks`, and every frame and event it gave, up to the
/// first error, which ends a connection. Every read takes at least a byte,
/// and the reader gives nothing after an error, so both loops end.
fn read_in_chunks(chunks: &[&[u8]]) -> Vec<super::Next> {
    let mut reader = super::FrameReader::default();
    let mut seen = Vec::new();
    for chunk in chunks {
        let mut source = *chunk;
        while !source.is_empty() {
            assert!(reader.read_from(&mut source).unwrap() > 0, "a read took nothing");
            loop {
                match reader.next() {
                    super::Next::Nothing => break,
                    error @ (super::Next::BadHeader | super::Next::BadChecksum) => {
                        seen.push(error);
                        return seen;
                    }
                    next => seen.push(next),
                }
            }
        }
    }
    seen
}

#[test]
fn a_reader_gives_every_frame_however_the_stream_is_cut() {
    let frames: Vec<Bytes> =
        (0..20).map(|size| encode(&message(size, 2, 100, vec![size as u8; size as usize * 37])).unwrap()).collect();
    let stream: Vec<u8> = frames.iter().flat_map(|frame| frame.to_vec()).collect();
    for cut in [1, 7, 8, 9, 100, 1000, stream.len()] {
        let chunks: Vec<&[u8]> = stream.chunks(cut).collect();
        let bodies: Vec<Bytes> = read_in_chunks(&chunks)
            .into_iter()
            .filter_map(|next| match next {
                super::Next::Frame(body) => Some(body),
                super::Next::Started => None,
                other => panic!("{other:?}"),
            })
            .collect();
        let expected: Vec<Bytes> = frames.iter().map(|frame| frame.slice(HEADER_LENGTH..)).collect();
        assert_eq!(expected, bodies, "cut every {cut} bytes");
    }
}

#[test]
fn a_frame_larger_than_the_buffer_gets_one_of_its_own() {
    let big = encode(&message(1, 2, 100, vec![9; super::READ_BUFFER * 3])).unwrap();
    let small = encode(&message(2, 2, 100, b"after".to_vec())).unwrap();
    let mut stream = big.to_vec();
    stream.extend_from_slice(&small);
    let seen = read_in_chunks(&stream.chunks(10_000).collect::<Vec<_>>());
    assert_eq!(3, seen.len(), "started, the big frame, the small one");
    assert_eq!(super::Next::Started, seen[0]);
    assert_eq!(super::Next::Frame(big.slice(HEADER_LENGTH..)), seen[1]);
    assert_eq!(super::Next::Frame(small.slice(HEADER_LENGTH..)), seen[2]);
}

#[test]
fn a_reader_refuses_a_bad_header_and_a_bad_checksum() {
    assert_eq!(vec![super::Next::BadHeader], read_in_chunks(&[&[0, 0, 0, 0, 1, 2, 3, 4]]));
    let mut frame = encode(&message(1, 2, 100, b"payload".to_vec())).unwrap().to_vec();
    let last = frame.len() - 1;
    frame[last] ^= 1;
    assert_eq!(vec![super::Next::BadChecksum], read_in_chunks(&[&frame]));
}

#[test]
fn a_reader_gives_nothing_after_an_error() {
    let mut reader = super::FrameReader::default();
    let good = encode(&message(1, 2, 100, b"after".to_vec())).unwrap();
    let mut stream = vec![0u8; HEADER_LENGTH];
    stream.extend_from_slice(&good);
    let mut source = &stream[..];
    reader.read_from(&mut source).unwrap();
    assert_eq!(super::Next::BadHeader, reader.next());
    assert_eq!(super::Next::Nothing, reader.next(), "the frame behind a bad header is not read");
}

#[test]
fn a_header_kept_apart_is_the_header_a_whole_frame_carries() {
    for size in [1, 7, 8, 100, 70_000] {
        let body = payload(size);
        let whole = frame_of_body(&body);
        assert_eq!(&whole[..HEADER_LENGTH], &head_of(&body)[..], "a body of {size}");
        assert_eq!(&whole[HEADER_LENGTH..], &body[..]);
    }
}
