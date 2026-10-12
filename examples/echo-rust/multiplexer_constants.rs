//
// this file is generated from echo.rules (mxcontrol generate_constants); do not edit
//

/// CRC-32 of the rules file these constants were generated from; a recording's header carries the same.
#[allow(dead_code)]
pub const RULES_FINGERPRINT: &str = "cf6eea04";

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
    pub const PICKLE_RESPONSE: u32 = 112;
    pub const REQUEST_RECEIVED: u32 = 113;
    pub const BACKEND_ERROR: u32 = 114;
    pub const LOGS_STREAM: u32 = 115;
    pub const ECHO_REQUEST: u32 = 300;
    pub const ECHO_RESPONSE: u32 = 301;

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
            PICKLE_RESPONSE => "PICKLE_RESPONSE",
            REQUEST_RECEIVED => "REQUEST_RECEIVED",
            BACKEND_ERROR => "BACKEND_ERROR",
            LOGS_STREAM => "LOGS_STREAM",
            ECHO_REQUEST => "ECHO_REQUEST",
            ECHO_RESPONSE => "ECHO_RESPONSE",
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
    pub const LOG_STREAMER: u32 = 108;
    pub const LOG_RECEIVER_EXAMPLE: u32 = 111;
    pub const ECHO_BACKEND: u32 = 300;
    pub const ECHO_CLIENT: u32 = 301;

    /// The name of `number`, or `fallback` when the rules file has none.
    pub fn get_name(number: u32, fallback: &'static str) -> &'static str {
        match number {
            MULTIPLEXER => "MULTIPLEXER",
            ALL_TYPES => "ALL_TYPES",
            MAX_MULTIPLEXER_SPECIAL_PEER_TYPE => "MAX_MULTIPLEXER_SPECIAL_PEER_TYPE",
            LOG_STREAMER => "LOG_STREAMER",
            LOG_RECEIVER_EXAMPLE => "LOG_RECEIVER_EXAMPLE",
            ECHO_BACKEND => "ECHO_BACKEND",
            ECHO_CLIENT => "ECHO_CLIENT",
            _ => fallback,
        }
    }
}
