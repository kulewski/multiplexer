//! The CRC-32 of a frame's body, eight bytes at a time ("slicing-by-8"):
//! the standard one, zlib's and Python's `zlib.crc32`, the same value as
//! lib/crc32.cc computes, by the same method. Every frame's CRC is computed
//! by its sender and checked by the multiplexer and by the receiver.
//!
//! `TABLES[0]` is the usual table, the remainder of each byte value by the
//! reflected polynomial; `TABLES[slice]` is `TABLES[slice - 1]` advanced by
//! one more byte of zeros, so the eight lookups of a step are independent of
//! each other and their results xor together. Here the tables are made at
//! compile time, which C++ cannot do in lib/crc32.cc. Bytes are read as
//! little-endian numbers, so neither the machine's byte order nor the
//! buffer's alignment matters; a tail of fewer than eight bytes takes the
//! byte-at-a-time step.

/// The reflected polynomial of the standard CRC-32.
const POLYNOMIAL: u32 = 0xEDB8_8320;

/// The eight tables; see the module comment. A static, built at compile
/// time all the same, at one address: a const would be copied to the stack
/// at every lookup in an unoptimized build, 8 KiB a time.
static TABLES: [[u32; 256]; 8] = make_tables();

/// Builds the tables, at compile time.
const fn make_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
    let mut index = 0;
    while index < 256 {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 != 0 { (value >> 1) ^ POLYNOMIAL } else { value >> 1 };
            bit += 1;
        }
        tables[0][index] = value;
        index += 1;
    }
    let mut index = 0;
    while index < 256 {
        let mut slice = 1;
        while slice < 8 {
            let previous = tables[slice - 1][index];
            tables[slice][index] = (previous >> 8) ^ tables[0][(previous & 0xFF) as usize];
            slice += 1;
        }
        index += 1;
    }
    tables
}

/// The CRC-32 of `bytes`.
pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    let mut steps = bytes.chunks_exact(8);
    for step in &mut steps {
        let low = u32::from_le_bytes([step[0], step[1], step[2], step[3]]) ^ crc;
        let high = u32::from_le_bytes([step[4], step[5], step[6], step[7]]);
        crc = TABLES[7][(low & 0xFF) as usize]
            ^ TABLES[6][((low >> 8) & 0xFF) as usize]
            ^ TABLES[5][((low >> 16) & 0xFF) as usize]
            ^ TABLES[4][(low >> 24) as usize]
            ^ TABLES[3][(high & 0xFF) as usize]
            ^ TABLES[2][((high >> 8) & 0xFF) as usize]
            ^ TABLES[1][((high >> 16) & 0xFF) as usize]
            ^ TABLES[0][(high >> 24) as usize];
    }
    for byte in steps.remainder() {
        crc = TABLES[0][((crc ^ u32::from(*byte)) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    //! The CRC against published vectors and against its definition,
    //! computed bit by bit, at every length up to 512 from every start
    //! within eight bytes and over a megabyte, as lib/digest_test.cc checks
    //! the C++ one: the eight-byte steps, the tail and every alignment.

    use super::crc32;

    /// The standard CRC-32 one bit at a time, straight from its definition.
    fn crc32_bitwise(bytes: &[u8]) -> u32 {
        let mut crc = 0xFFFF_FFFFu32;
        for byte in bytes {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0xEDB8_8320 & 0u32.wrapping_sub(crc & 1));
            }
        }
        crc ^ 0xFFFF_FFFF
    }

    /// `size` bytes that are not a short repeating pattern, from a linear
    /// congruential generator's high bits.
    fn scrambled(size: usize) -> Vec<u8> {
        let mut state = 12345u32;
        (0..size)
            .map(|_| {
                state = state.wrapping_mul(1_103_515_245).wrapping_add(12345);
                (state >> 24) as u8
            })
            .collect()
    }

    #[test]
    fn matches_the_standard_vectors() {
        assert_eq!(0, crc32(b""));
        assert_eq!(0xCBF4_3926, crc32(b"123456789"));
        assert_eq!(0x414F_A339, crc32(b"The quick brown fox jumps over the lazy dog"));
    }

    #[test]
    fn matches_the_definition_at_every_length_and_start() {
        let buffer = scrambled(512 + 8);
        for start in 0..8 {
            for size in 0..=512 {
                let bytes = &buffer[start..start + size];
                assert_eq!(crc32_bitwise(bytes), crc32(bytes), "start {start}, size {size}");
            }
        }
    }

    #[test]
    fn matches_the_definition_over_a_megabyte() {
        let buffer = scrambled(1 << 20);
        assert_eq!(crc32_bitwise(&buffer), crc32(&buffer));
    }
}
