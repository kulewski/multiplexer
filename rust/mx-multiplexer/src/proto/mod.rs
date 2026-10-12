//! The protocol's messages as prost generated them from
//! multiplexer/Multiplexer.proto and multiplexer/Logging.proto
//! (rust/codegen). Inside the crate only, so that a change of prost, or a
//! move to Google's Rust protobuf, is never a change for the crate's users.
//!
//! The modules follow the .proto packages, multiplexer and mx.logging,
//! because prost names one from the other by those paths
//! (`super::mx::logging::LogEntry`). The files are include!d, not modules
//! of their own, so that rustfmt leaves prost's layout alone and the
//! generator's check compares them as they were written.

#[allow(clippy::all, clippy::pedantic, dead_code, missing_docs)]
pub(crate) mod multiplexer {
    include!("multiplexer.rs");
}

#[allow(clippy::all, clippy::pedantic, dead_code, missing_docs)]
pub(crate) mod mx {
    pub(crate) mod logging {
        include!("mx.logging.rs");
    }
}
