//
// this file is generated from tests/testing.rules (mxcontrol generate_constants); do not edit
//

/// CRC-32 of the rules file these constants were generated from; a recording's header carries the same.
#[allow(dead_code)]
pub const RULES_FINGERPRINT: &str = "ee2b4249";

/// The message types of the rules file, by name.
#[allow(dead_code, non_upper_case_globals)]
pub mod types {
    pub const PING: u32 = 1;
    pub const CONNECTION_WELCOME: u32 = 2;
    pub const BACKEND_FOR_PACKET_SEARCH: u32 = 3;
    pub const HEARTBIT: u32 = 4;
    pub const DELIVERY_ERROR: u32 = 5;
    pub const RECORDING_CONTROL: u32 = 6;
    pub const RECORDING_STATUS: u32 = 7;
    pub const RECORDING_RECORD: u32 = 8;
    pub const RULES_CONTROL: u32 = 9;
    pub const RULES_STATUS: u32 = 10;
    pub const PEER_CONTROL: u32 = 11;
    pub const PEER_STATUS: u32 = 12;
    pub const MAX_MULTIPLEXER_META_PACKET: u32 = 99;
    pub const PYTHON_TEST_REQUEST: u32 = 110;
    pub const PYTHON_TEST_RESPONSE: u32 = 111;
    pub const PICKLE_RESPONSE: u32 = 112;
    pub const REQUEST_RECEIVED: u32 = 113;
    pub const BACKEND_ERROR: u32 = 114;
    pub const LOGS_STREAM: u32 = 115;
    pub const TEST_REQUEST_A: u32 = 201;
    pub const TEST_REQUEST_B: u32 = 202;
    pub const TEST_RESPONSE: u32 = 203;
    pub const TEST_EVENT: u32 = 204;
    pub const TEST_EVENT_ANY: u32 = 205;
    pub const TEST_UNROUTED: u32 = 206;
    pub const TEST_TINY_QUEUE_EVENT: u32 = 207;

    /// The name of `number`, or `fallback` when the rules file has none.
    pub fn get_name(number: u32, fallback: &'static str) -> &'static str {
        match number {
            PING => "PING",
            CONNECTION_WELCOME => "CONNECTION_WELCOME",
            BACKEND_FOR_PACKET_SEARCH => "BACKEND_FOR_PACKET_SEARCH",
            HEARTBIT => "HEARTBIT",
            DELIVERY_ERROR => "DELIVERY_ERROR",
            RECORDING_CONTROL => "RECORDING_CONTROL",
            RECORDING_STATUS => "RECORDING_STATUS",
            RECORDING_RECORD => "RECORDING_RECORD",
            RULES_CONTROL => "RULES_CONTROL",
            RULES_STATUS => "RULES_STATUS",
            PEER_CONTROL => "PEER_CONTROL",
            PEER_STATUS => "PEER_STATUS",
            MAX_MULTIPLEXER_META_PACKET => "MAX_MULTIPLEXER_META_PACKET",
            PYTHON_TEST_REQUEST => "PYTHON_TEST_REQUEST",
            PYTHON_TEST_RESPONSE => "PYTHON_TEST_RESPONSE",
            PICKLE_RESPONSE => "PICKLE_RESPONSE",
            REQUEST_RECEIVED => "REQUEST_RECEIVED",
            BACKEND_ERROR => "BACKEND_ERROR",
            LOGS_STREAM => "LOGS_STREAM",
            TEST_REQUEST_A => "TEST_REQUEST_A",
            TEST_REQUEST_B => "TEST_REQUEST_B",
            TEST_RESPONSE => "TEST_RESPONSE",
            TEST_EVENT => "TEST_EVENT",
            TEST_EVENT_ANY => "TEST_EVENT_ANY",
            TEST_UNROUTED => "TEST_UNROUTED",
            TEST_TINY_QUEUE_EVENT => "TEST_TINY_QUEUE_EVENT",
            _ => fallback,
        }
    }
}

/// The peer types of the rules file, by name.
#[allow(dead_code, non_upper_case_globals)]
pub mod peers {
    pub const MULTIPLEXER: u32 = 1;
    pub const ALL_TYPES: u32 = 2;
    pub const MAX_MULTIPLEXER_SPECIAL_PEER_TYPE: u32 = 99;
    pub const WEBSITE: u32 = 102;
    pub const PYTHON_TEST_SERVER: u32 = 106;
    pub const PYTHON_TEST_CLIENT: u32 = 107;
    pub const LOG_STREAMER: u32 = 108;
    pub const LOG_RECEIVER_EXAMPLE: u32 = 111;
    pub const TEST_BACKEND_A: u32 = 201;
    pub const TEST_BACKEND_B: u32 = 202;
    pub const TEST_CLIENT: u32 = 203;
    pub const TEST_EVENT_CLIENT: u32 = 204;
    pub const TEST_EVENT_BACKEND: u32 = 205;
    pub const TEST_ACTIVE_CLIENT: u32 = 206;
    pub const TEST_TINY_QUEUE: u32 = 207;

    /// The name of `number`, or `fallback` when the rules file has none.
    pub fn get_name(number: u32, fallback: &'static str) -> &'static str {
        match number {
            MULTIPLEXER => "MULTIPLEXER",
            ALL_TYPES => "ALL_TYPES",
            MAX_MULTIPLEXER_SPECIAL_PEER_TYPE => "MAX_MULTIPLEXER_SPECIAL_PEER_TYPE",
            WEBSITE => "WEBSITE",
            PYTHON_TEST_SERVER => "PYTHON_TEST_SERVER",
            PYTHON_TEST_CLIENT => "PYTHON_TEST_CLIENT",
            LOG_STREAMER => "LOG_STREAMER",
            LOG_RECEIVER_EXAMPLE => "LOG_RECEIVER_EXAMPLE",
            TEST_BACKEND_A => "TEST_BACKEND_A",
            TEST_BACKEND_B => "TEST_BACKEND_B",
            TEST_CLIENT => "TEST_CLIENT",
            TEST_EVENT_CLIENT => "TEST_EVENT_CLIENT",
            TEST_EVENT_BACKEND => "TEST_EVENT_BACKEND",
            TEST_ACTIVE_CLIENT => "TEST_ACTIVE_CLIENT",
            TEST_TINY_QUEUE => "TEST_TINY_QUEUE",
            _ => fallback,
        }
    }
}
