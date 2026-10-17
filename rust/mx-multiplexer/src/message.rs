//! The crate's own types for what goes over the wire: `Message`, the
//! envelope every peer sends, `Routing`, `DeliveryError` and the routing
//! rules a message may carry, with the fields of multiplexer/Multiplexer.proto
//! (docs/wire_format.md says what each is for). The generated prost types
//! stay inside the crate, converted here at the edge, so that a change of
//! prost is never a change of this crate's API. Converting moves the
//! fields; a payload's `Bytes` is reference-counted, so a received payload
//! is a slice of its frame, never a copy.
//!
//! Presence is kept where the protocol uses it: `to`, the delivery-error
//! flags, `key`, `timestamp`, `workflow`. An `id`, a `sender` or a
//! `references` of 0 is the field left out, which is what the libraries
//! read 0 as.

use std::collections::BTreeMap;

use bytes::Bytes;
use prost::Message as _;

use crate::core::batching::{plain_event, BatchMark};
use crate::proto::multiplexer as proto;

/// The key filters of a peer: per message type, the patterns, RE2 regular
/// expressions of which one must match a key whole; a type left out takes
/// every key, one with no pattern none (docs/keys.md).
pub type KeyFilters = BTreeMap<u32, Vec<String>>;

/// A message, the protocol's MultiplexerMessage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Message {
    /// The message's id, 0 for one to be given its id when it is sent.
    pub id: u64,
    /// The sender's instance id, 0 for this client's, filled in when sent.
    pub sender: u64,
    /// The instance it is addressed to; the multiplexer then consults no
    /// rule.
    pub to: Option<u64>,
    /// For a message with `to`: whether the multiplexer reports, with a
    /// DELIVERY_ERROR, an addressee that is gone.
    pub report_delivery_error: Option<bool>,
    /// Whether a delivery error carries this message back.
    pub include_original_packet_in_report: Option<bool>,
    /// The message type, which the rules route by.
    pub r#type: u32,
    /// The payload, opaque bytes: the protocol's field `message`.
    pub payload: Bytes,
    /// Carried, not acted on.
    pub compression: Option<i32>,
    /// Carried, not acted on.
    pub timestamp: Option<u32>,
    /// The id of the message this one answers, 0 for none.
    pub references: u64,
    /// The work flow, a trace or request id, text, copied from a request
    /// to its reply; never routed by. A binary value goes as hex or base64.
    pub workflow: Option<String>,
    /// Routing rules carried by the message, replacing the rules file's.
    pub override_rrules: Vec<RoutingRule>,
    /// Gates the multiplexer's own log of the message.
    pub logging_method: Option<i32>,
    /// What the message is about, at most MAX_KEY_SIZE bytes: routed by
    /// the rules, it reaches a peer whose key filter for its type takes it.
    pub key: Option<String>,
}

/// Whom a routing rule reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Whom {
    /// Every peer of the type.
    All,
    /// One peer of the type, round robin.
    Any,
}

/// A routing rule a message carries in `override_rrules`, as a rules
/// file's `to { ... }` entry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoutingRule {
    /// The receiving peer type by its name in a rules file; `ALL_TYPES`
    /// for every peer type that has a connection. Names are resolved where
    /// the multiplexer reads a rules file: a message's own rules are routed
    /// by `peer_type` alone, a rule given by name only reaching nobody.
    pub peer: Option<String>,
    /// The receiving peer type by its number.
    pub peer_type: Option<u32>,
    /// One peer of the type, round robin, or every one; one by default.
    pub whom: Option<Whom>,
    /// Whether the multiplexer logs an undelivered message as an error
    /// rather than a warning; true by default.
    pub delivery_error_is_error: Option<bool>,
    /// Whether the sender gets a DELIVERY_ERROR when nobody received the
    /// message by this rule; true by default.
    pub report_delivery_error: Option<bool>,
    /// Whether that DELIVERY_ERROR carries the message itself; false by
    /// default.
    pub include_original_packet_in_report: Option<bool>,
}

/// Which of a multiplexer's routing paths reach a peer: `any` the messages
/// routed to one peer of its type, `all` those routed to every peer of its
/// type, and `last_resort`, with `any` off, whether it still takes one when
/// no other peer of its type does. Every path by default; a draining
/// backend turns `any` and `all` off (docs/leaving.md).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Routing {
    /// The messages routed to one peer of the type, `whom: ANY`.
    pub any: bool,
    /// The messages routed to every peer of the type, `whom: ALL`.
    pub all: bool,
    /// With `any` off, one routed to one peer anyway when no other peer of
    /// the type takes it.
    pub last_resort: bool,
}

impl Default for Routing {
    fn default() -> Self {
        Routing { any: true, all: true, last_resort: false }
    }
}

/// The payload of a DELIVERY_ERROR: the message nobody took, by its id,
/// and why.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeliveryError {
    /// The id of the message that was not delivered.
    pub packet_id: u64,
    /// The peer types a rule named that had nobody to take it.
    pub failed_type: Vec<u32>,
    /// The addressee that is gone, for a message with `to`.
    pub failed_to: Option<u64>,
    /// Whether the rules name the message's type.
    pub is_known_type: Option<bool>,
    /// The message itself, when its rule or its sender asked for it.
    pub original_message: Option<Box<Message>>,
    /// The message was too large to carry back.
    pub original_message_omitted: Option<bool>,
}

impl DeliveryError {
    /// The delivery error a DELIVERY_ERROR's payload holds; None for a
    /// payload that is not one.
    pub fn decode(payload: &Bytes) -> Option<DeliveryError> {
        proto::DeliveryError::decode(payload.clone()).ok().map(DeliveryError::from)
    }

    /// The payload a DELIVERY_ERROR carries.
    pub fn encode(&self) -> Bytes {
        proto::DeliveryError::from(self.clone()).encode_to_vec().into()
    }
}

/// `message` as a frame to send: its empty id given by `next_id`, its
/// empty sender `sender`, its size and key checked before anything is
/// queued; for a client that batches, `batching`, marked a plain event,
/// with its key, when it may go in a batch, the key moved out of the
/// encoded message rather than copied; a client that does not looks at
/// nothing for it.
pub(crate) fn frame_of(
    mut message: Message,
    next_id: impl FnOnce() -> u64,
    sender: u64,
    batching: bool,
) -> crate::error::Result<crate::core::outbox::Frame> {
    if message.id == 0 {
        message.id = next_id();
    }
    if message.sender == 0 {
        message.sender = sender;
    }
    let (id, r#type) = (message.id, message.r#type);
    let mut encoded = proto::MultiplexerMessage::from(message);
    // An event that may go in a batch is framed unsealed, its checksum
    // written only if it goes alone (`frame::seal`): what its mark says.
    if batching && plain_event(&encoded) {
        let bytes = crate::frame::encode_unsealed(&encoded)?;
        return Ok(crate::core::outbox::Frame { bytes, id, r#type, batch: BatchMark::Event(encoded.key.take()) });
    }
    let bytes = crate::frame::encode(&encoded)?;
    Ok(crate::core::outbox::Frame { bytes, id, r#type, batch: BatchMark::None })
}

/// A u64 field the libraries read as absent when 0.
fn nonzero(value: u64) -> Option<u64> {
    (value != 0).then_some(value)
}

impl From<proto::MultiplexerMessage> for Message {
    fn from(message: proto::MultiplexerMessage) -> Message {
        Message {
            id: message.id.unwrap_or(0),
            sender: message.sender.unwrap_or(0),
            to: message.to,
            report_delivery_error: message.report_delivery_error,
            include_original_packet_in_report: message.include_original_packet_in_report,
            r#type: message.r#type,
            payload: message.message.unwrap_or_default(),
            compression: message.compression,
            timestamp: message.timestamp,
            references: message.references.unwrap_or(0),
            workflow: message.workflow,
            override_rrules: message.override_rrules.into_iter().map(RoutingRule::from).collect(),
            logging_method: message.logging_method,
            key: message.key,
        }
    }
}

impl From<Message> for proto::MultiplexerMessage {
    fn from(message: Message) -> proto::MultiplexerMessage {
        proto::MultiplexerMessage {
            id: nonzero(message.id),
            sender: nonzero(message.sender),
            to: message.to,
            report_delivery_error: message.report_delivery_error,
            include_original_packet_in_report: message.include_original_packet_in_report,
            r#type: message.r#type,
            message: Some(message.payload),
            compression: message.compression,
            timestamp: message.timestamp,
            references: nonzero(message.references),
            workflow: message.workflow,
            override_rrules: message
                .override_rrules
                .into_iter()
                .map(proto::multiplexer_message_description::RoutingRule::from)
                .collect(),
            logging_method: message.logging_method,
            key: message.key,
            batch: None,
        }
    }
}

impl From<proto::multiplexer_message_description::RoutingRule> for RoutingRule {
    fn from(rule: proto::multiplexer_message_description::RoutingRule) -> RoutingRule {
        use proto::multiplexer_message_description::routing_rule::Whom as ProtoWhom;
        RoutingRule {
            peer: rule.peer,
            peer_type: rule.peer_type,
            whom: rule.whom.and_then(|whom| match ProtoWhom::try_from(whom) {
                Ok(ProtoWhom::All) => Some(Whom::All),
                Ok(ProtoWhom::Any) => Some(Whom::Any),
                Err(_) => None,
            }),
            delivery_error_is_error: rule.delivery_error_is_error,
            report_delivery_error: rule.report_delivery_error,
            include_original_packet_in_report: rule.include_original_packet_in_report,
        }
    }
}

impl From<RoutingRule> for proto::multiplexer_message_description::RoutingRule {
    fn from(rule: RoutingRule) -> proto::multiplexer_message_description::RoutingRule {
        use proto::multiplexer_message_description::routing_rule::Whom as ProtoWhom;
        proto::multiplexer_message_description::RoutingRule {
            peer: rule.peer,
            peer_type: rule.peer_type,
            whom: rule.whom.map(|whom| match whom {
                Whom::All => ProtoWhom::All as i32,
                Whom::Any => ProtoWhom::Any as i32,
            }),
            delivery_error_is_error: rule.delivery_error_is_error,
            report_delivery_error: rule.report_delivery_error,
            include_original_packet_in_report: rule.include_original_packet_in_report,
        }
    }
}

impl From<proto::Routing> for Routing {
    fn from(routing: proto::Routing) -> Routing {
        Routing { any: routing.any(), all: routing.all(), last_resort: routing.last_resort() }
    }
}

impl From<Routing> for proto::Routing {
    fn from(routing: Routing) -> proto::Routing {
        proto::Routing { any: Some(routing.any), all: Some(routing.all), last_resort: Some(routing.last_resort) }
    }
}

impl From<proto::DeliveryError> for DeliveryError {
    fn from(error: proto::DeliveryError) -> DeliveryError {
        DeliveryError {
            packet_id: error.packet_id,
            failed_type: error.failed_type,
            failed_to: error.failed_to,
            is_known_type: error.is_known_type,
            original_message: error.original_message.map(|message| Box::new(Message::from(message))),
            original_message_omitted: error.original_message_omitted,
        }
    }
}

impl From<DeliveryError> for proto::DeliveryError {
    fn from(error: DeliveryError) -> proto::DeliveryError {
        proto::DeliveryError {
            packet_id: error.packet_id,
            failed_type: error.failed_type,
            failed_to: error.failed_to,
            is_known_type: error.is_known_type,
            original_message: error.original_message.map(|message| proto::MultiplexerMessage::from(*message)),
            original_message_omitted: error.original_message_omitted,
        }
    }
}

#[cfg(test)]
mod tests {
    //! Every field of every type, converted both ways: a field added to the
    //! .proto files and forgotten here fails the first test, its value lost
    //! on the way.

    use super::{DeliveryError, Message, Routing, RoutingRule, Whom};
    use crate::proto::multiplexer as proto;
    use bytes::Bytes;

    #[test]
    fn a_frame_is_marked_for_batching_only_by_a_client_that_batches() {
        use crate::core::batching::BatchMark;
        let keyed = Message { r#type: 300, key: Some("k".to_string()), ..Default::default() };
        let marked = super::frame_of(keyed.clone(), || 1, 2, true).unwrap();
        assert_eq!(BatchMark::Event(Some("k".to_string())), marked.batch);
        let unmarked = super::frame_of(keyed.clone(), || 1, 2, false).unwrap();
        assert_eq!(BatchMark::None, unmarked.batch, "no look for a client that does not batch");
        let addressed = Message { to: Some(9), ..keyed };
        assert_eq!(BatchMark::None, super::frame_of(addressed, || 1, 2, true).unwrap().batch, "no plain event");
    }

    #[test]
    fn a_frame_marked_for_batching_is_made_unsealed_and_sealed_alone() {
        use crate::frame::{seal, unpack_header, verify, HEADER_LENGTH};
        let event = Message { r#type: 300, payload: Bytes::from_static(b"event"), ..Default::default() };
        let header = |bytes: &Bytes| unpack_header(bytes[..HEADER_LENGTH].try_into().unwrap()).expect("a length");
        let marked = super::frame_of(event.clone(), || 1, 2, true).unwrap();
        assert_eq!(0, header(&marked.bytes).checksum, "no checksum: in a batch only the body is read");
        let sealed = seal(marked.bytes);
        assert!(verify(header(&sealed), &sealed[HEADER_LENGTH..]), "sealed, its checksum the body's");
        let unmarked = super::frame_of(event, || 1, 2, false).unwrap();
        assert!(verify(header(&unmarked.bytes), &unmarked.bytes[HEADER_LENGTH..]), "sealed as it is made");
        assert_eq!(sealed, unmarked.bytes, "the same frame either way");
    }

    /// A generated message with every field set.
    fn every_field() -> proto::MultiplexerMessage {
        proto::MultiplexerMessage {
            id: Some(1),
            sender: Some(2),
            to: Some(3),
            report_delivery_error: Some(true),
            include_original_packet_in_report: Some(false),
            r#type: 4,
            message: Some(Bytes::from_static(b"payload")),
            compression: Some(1),
            timestamp: Some(5),
            references: Some(6),
            workflow: Some("workflow".into()),
            override_rrules: vec![proto::multiplexer_message_description::RoutingRule {
                peer: Some("backend".into()),
                peer_type: Some(7),
                whom: Some(1),
                delivery_error_is_error: Some(false),
                report_delivery_error: Some(true),
                include_original_packet_in_report: Some(true),
            }],
            logging_method: Some(2),
            key: Some("key".into()),
            batch: None, // a batch never becomes a Message: it is split into its messages first
        }
    }

    #[test]
    fn a_message_converts_both_ways_with_every_field() {
        let generated = every_field();
        let converted = Message::from(generated.clone());
        assert_eq!(Some(Whom::All), converted.override_rrules[0].whom);
        assert_eq!(generated, proto::MultiplexerMessage::from(converted));
        // The debug form names every field of the generated type: the same
        // count of them here, but for `batch`, which no Message has, a
        // batch being split into its messages before any becomes one.
        let fields = |text: String| text.matches(": ").count();
        assert_eq!(
            fields(format!("{:?}", proto::MultiplexerMessage::default())),
            fields(format!("{:?}", Message::default())) + 1,
        );
        assert_eq!(
            fields(format!("{:?}", proto::multiplexer_message_description::RoutingRule::default())),
            fields(format!("{:?}", RoutingRule::default())),
        );
    }

    #[test]
    fn a_delivery_error_converts_both_ways_with_every_field() {
        let generated = proto::DeliveryError {
            packet_id: 1,
            failed_type: vec![2, 3],
            failed_to: Some(4),
            is_known_type: Some(true),
            original_message: Some(every_field()),
            original_message_omitted: Some(false),
        };
        let converted = DeliveryError::from(generated.clone());
        assert_eq!(generated, proto::DeliveryError::from(converted.clone()));
        assert_eq!(Some(converted.clone()), DeliveryError::decode(&converted.encode()));
    }

    #[test]
    fn routing_takes_the_declared_defaults() {
        assert_eq!(Routing { any: true, all: true, last_resort: false }, Routing::default());
        assert_eq!(Routing::default(), Routing::from(proto::Routing::default()));
        let drain = Routing { any: false, all: false, last_resort: true };
        assert_eq!(drain, Routing::from(proto::Routing::from(drain)));
    }
}
