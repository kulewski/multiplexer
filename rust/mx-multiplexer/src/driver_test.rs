//! The driver's writes of a frame whose header is kept apart from its body
//! (`Writing`): whatever a socket takes at each call, the header goes
//! first, then the body, each byte once.

use std::io::{IoSlice, Write};

use bytes::Bytes;

use super::Writing;
use crate::frame::head_of;

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
