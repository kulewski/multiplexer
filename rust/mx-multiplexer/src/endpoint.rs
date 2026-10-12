//! A multiplexer's address as text: `host:port`, or `[address]:port` for an
//! IPv6 address, the form URLs use, since the address has colons of its
//! own. Mirrors multiplexer/endpoint.cc: [`parse_endpoint`] reads it and
//! [`format_endpoint`] writes it, as the C++ and Python libraries do, for
//! every address a program is given and every log line that names one.
//!
//! An IPv6 address out of brackets is refused rather than split at its last
//! colon: `::1:1980` could be `::1` port 1980 or `::1:1980` with no port.

use crate::error::{Error, Result};

/// Refuses `text` for `why`, as the C++ `std::invalid_argument` says it.
fn refuse<T>(text: &str, why: &str) -> Result<T> {
    Err(Error::InvalidArgument(format!("'{text}': {why}")))
}

/// `port`, what follows the colon in `text`, as a port number: digits only,
/// so no sign and no space, at most 65535.
fn port_number(text: &str, port: &str) -> Result<u16> {
    if port.is_empty() {
        return refuse(text, "no port after the colon");
    }
    let mut value: u32 = 0;
    for digit in port.bytes() {
        if !digit.is_ascii_digit() {
            return refuse(text, "the port is a number from 0 to 65535");
        }
        value = value * 10 + u32::from(digit - b'0');
        if value > 65535 {
            return refuse(text, "the port is a number from 0 to 65535");
        }
    }
    Ok(value as u16)
}

/// `text` as (host, port): `host:port`, or `[address]:port` for an IPv6
/// address, whose host comes back without the brackets. The host may be
/// empty, as in `:1980`, for the caller to fill in. Without a port, `host`
/// or `[address]`, the port is `default_port`, and the text is refused when
/// there is none. Refused with [`Error::InvalidArgument`] naming the text
/// and what is wrong: an IPv6 address out of brackets, brackets around
/// anything but an IPv6 address or anywhere but at the start, a port that
/// is not a number from 0 to 65535, empty text.
pub fn parse_endpoint(text: &str, default_port: Option<u16>) -> Result<(String, u16)> {
    if text.is_empty() {
        return refuse(text, "no address");
    }
    let host;
    let colon; // the one before the port, or text.len() when there is no port
    if let Some(rest) = text.strip_prefix('[') {
        let Some(close) = rest.find(']') else {
            return refuse(text, "no ] after the IPv6 address");
        };
        host = &rest[..close];
        if !host.contains(':') || host.contains('[') {
            return refuse(text, "brackets hold an IPv6 address");
        }
        colon = close + 2;
        if colon < text.len() && text.as_bytes()[colon] != b':' {
            return refuse(text, "only :port may follow the ]");
        }
    } else {
        if text.contains(['[', ']']) {
            return refuse(text, "brackets go around an IPv6 address, at the start");
        }
        match text.find(':') {
            Some(found) if text[found + 1..].contains(':') => {
                return refuse(text, "an IPv6 address goes in brackets, as in [::1]:1980");
            }
            Some(found) => colon = found,
            None => colon = text.len(),
        }
        host = &text[..colon];
    }
    if colon == text.len() {
        return match default_port {
            Some(port) => Ok((host.to_string(), port)),
            None => refuse(text, "no port: host:port, or [address]:port for an IPv6 address"),
        };
    }
    Ok((host.to_string(), port_number(text, &text[colon + 1..])?))
}

/// `host` and `port` as [`parse_endpoint`] reads them: `[host]:port` when
/// the host has a colon, an IPv6 address, `host:port` otherwise.
pub fn format_endpoint(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    //! multiplexer/endpoint_test.cc, case for case.

    use super::{format_endpoint, parse_endpoint};
    use crate::error::Error;

    /// What `parse_endpoint` refuses `text` with, without a default port
    /// unless given; "accepted" when it takes it.
    fn refusal(text: &str, default_port: Option<u16>) -> String {
        match parse_endpoint(text, default_port) {
            Err(Error::InvalidArgument(why)) => why,
            Err(other) => panic!("{text:?} gave {other:?}"),
            Ok(_) => "accepted".to_string(),
        }
    }

    /// `host` and `port` as an endpoint.
    fn endpoint(host: &str, port: u16) -> (String, u16) {
        (host.to_string(), port)
    }

    #[test]
    fn host_and_port() {
        assert_eq!(endpoint("127.0.0.1", 1980), parse_endpoint("127.0.0.1:1980", None).unwrap());
        assert_eq!(endpoint("mx.example.com", 1980), parse_endpoint("mx.example.com:1980", None).unwrap());
        assert_eq!(endpoint("0.0.0.0", 0), parse_endpoint("0.0.0.0:0", None).unwrap());
        assert_eq!(endpoint("host", 65535), parse_endpoint("host:65535", None).unwrap());
        assert_eq!(endpoint("host", 80), parse_endpoint("host:0080", None).unwrap());
    }

    #[test]
    fn empty_host_is_the_callers() {
        assert_eq!(endpoint("", 1980), parse_endpoint(":1980", None).unwrap());
    }

    #[test]
    fn ipv6_in_brackets() {
        assert_eq!(endpoint("::1", 1980), parse_endpoint("[::1]:1980", None).unwrap());
        assert_eq!(endpoint("::", 0), parse_endpoint("[::]:0", None).unwrap());
        assert_eq!(endpoint("fe80::1%eth0", 1980), parse_endpoint("[fe80::1%eth0]:1980", None).unwrap());
        assert_eq!(endpoint("::ffff:127.0.0.1", 1980), parse_endpoint("[::ffff:127.0.0.1]:1980", None).unwrap());
    }

    #[test]
    fn default_port() {
        assert_eq!(endpoint("0.0.0.0", 1980), parse_endpoint("0.0.0.0", Some(1980)).unwrap());
        assert_eq!(endpoint("::", 1980), parse_endpoint("[::]", Some(1980)).unwrap());
        assert_eq!(endpoint("host", 7), parse_endpoint("host:7", Some(1980)).unwrap());
        assert_eq!("'0.0.0.0': no port: host:port, or [address]:port for an IPv6 address", refusal("0.0.0.0", None));
        assert_eq!("'[::1]': no port: host:port, or [address]:port for an IPv6 address", refusal("[::1]", None));
    }

    #[test]
    fn ipv6_out_of_brackets_is_refused() {
        assert_eq!("'::1:1980': an IPv6 address goes in brackets, as in [::1]:1980", refusal("::1:1980", None));
        assert_eq!("'::1': an IPv6 address goes in brackets, as in [::1]:1980", refusal("::1", Some(1980)));
        assert_eq!("'fe80::1': an IPv6 address goes in brackets, as in [::1]:1980", refusal("fe80::1", Some(1980)));
    }

    #[test]
    fn brackets_hold_an_ipv6_address_at_the_start() {
        assert_eq!("'[::1': no ] after the IPv6 address", refusal("[::1", None));
        assert_eq!("'[::1]1980': only :port may follow the ]", refusal("[::1]1980", None));
        assert_eq!("'[::1]x:1980': only :port may follow the ]", refusal("[::1]x:1980", None));
        assert_eq!("'[127.0.0.1]:1980': brackets hold an IPv6 address", refusal("[127.0.0.1]:1980", None));
        assert_eq!("'[host]:1980': brackets hold an IPv6 address", refusal("[host]:1980", None));
        assert_eq!("'[]:1980': brackets hold an IPv6 address", refusal("[]:1980", None));
        assert_eq!("'[[::1]]:1980': brackets hold an IPv6 address", refusal("[[::1]]:1980", None));
        assert_eq!("'host[1]:1980': brackets go around an IPv6 address, at the start", refusal("host[1]:1980", None));
        assert_eq!("'host]:1980': brackets go around an IPv6 address, at the start", refusal("host]:1980", None));
    }

    #[test]
    fn port_is_a_number_from_0_to_65535() {
        for text in [
            "host:-1",
            "host:+80",
            "host: 80",
            "host:80 ",
            "host:8o",
            "host:65536",
            "host:99999999999",
            "[::1]:-1",
            "[::1]:65536",
        ] {
            assert_eq!(format!("'{text}': the port is a number from 0 to 65535"), refusal(text, None), "{text}");
        }
        assert_eq!("'host:': no port after the colon", refusal("host:", None));
        assert_eq!("'[::1]:': no port after the colon", refusal("[::1]:", None));
        assert_eq!("':': no port after the colon", refusal(":", None));
    }

    #[test]
    fn empty_text_is_refused() {
        assert_eq!("'': no address", refusal("", None));
        assert_eq!("'': no address", refusal("", Some(1980)));
    }

    #[test]
    fn format() {
        assert_eq!("127.0.0.1:1980", format_endpoint("127.0.0.1", 1980));
        assert_eq!("mx.example.com:0", format_endpoint("mx.example.com", 0));
        assert_eq!("[::1]:1980", format_endpoint("::1", 1980));
        assert_eq!("[fe80::1%eth0]:65535", format_endpoint("fe80::1%eth0", 65535));
        assert_eq!(":1980", format_endpoint("", 1980));
    }

    #[test]
    fn formatted_parses_back() {
        for (host, port) in
            [("127.0.0.1", 1980), ("::1", 0), ("::", 1980), ("mx.example.com", 65535), ("", 1980), ("fe80::1%eth0", 7)]
        {
            assert_eq!(endpoint(host, port), parse_endpoint(&format_endpoint(host, port), None).unwrap(), "{host}");
        }
    }
}
