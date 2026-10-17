//! XXH3, the 64-bit hash of xxHash 0.8 (seed 0, its default secret), whose
//! low 32 bits are every frame's checksum: the value lib/xxh3.cc computes,
//! the same way. Every frame's checksum is computed by its sender and
//! checked by the multiplexer and by the receiver; docs/wire_format.md says
//! how a peer in another language computes it.
//!
//! A body of up to 240 bytes takes a few multiplications whatever its
//! length; a longer one goes in stripes of 64 bytes, each mixed into eight
//! 64-bit accumulators by one multiplication and two additions a lane, the
//! accumulators scrambled after every 1024 bytes and folded into one value
//! at the end. A long body's time is its stripes, whose lanes are
//! independent, so on x86-64 they go 16, 32 or 64 bytes at a time by SSE2,
//! AVX2 or AVX-512, the widest the processor says it has, asked at the call
//! (std caches the answer); elsewhere one lane at a time. Each path's
//! function is compiled for its instructions and runs the one loop,
//! `long_hash`, inlined, with closures that call its stripe, combine and
//! scramble functions, which inline in turn. The odd stripes of a block sum
//! apart and join before its scramble, the same sums in another order, so
//! that a stripe waits for the one two before it, not the one before. Bytes are read as little-endian numbers,
//! so neither the machine's byte order nor the buffer's alignment matters.

const PRIME32_1: u64 = 0x9E37_79B1;
const PRIME32_2: u64 = 0x85EB_CA77;
const PRIME32_3: u64 = 0xC2B2_AE3D;
const PRIME64_1: u64 = 0x9E37_79B1_85EB_CA87;
const PRIME64_2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const PRIME64_3: u64 = 0x1656_67B1_9E37_79F9;
const PRIME64_4: u64 = 0x85EB_CA77_C2B2_AE63;
const PRIME64_5: u64 = 0x27D4_EB2F_1656_67C5;

/// The default secret, which the specification fixes: every input is mixed
/// with some of its bytes. A static, at one address: a const array would be
/// copied to the stack at every use in an unoptimized build.
static SECRET: [u8; 192] = [
    0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c, //
    0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f, //
    0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21, //
    0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c, //
    0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3, //
    0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8, //
    0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d, //
    0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64, //
    0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb, //
    0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e, //
    0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce, //
    0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e, //
];

/// The bytes one step mixes in.
const STRIPE: usize = 64;
/// A stripe moves 8 bytes along the secret: a block of 16 stripes, 1024
/// bytes, uses it all, and the accumulators are scrambled after each.
const STRIPES_PER_BLOCK: usize = (SECRET.len() - STRIPE) / 8;
/// The bytes of a block.
const BLOCK: usize = STRIPE * STRIPES_PER_BLOCK;

/// XXH3-64 of `bytes`.
pub(crate) fn xxh3_64(bytes: &[u8]) -> u64 {
    match bytes.len() {
        0..=16 => up_to_16(bytes),
        17..=128 => up_to_128(bytes),
        129..=240 => up_to_240(bytes),
        _ => long(bytes),
    }
}

/// The 8 bytes of `bytes` at `at` as a little-endian number.
fn read64(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("eight bytes"))
}

/// The 4 bytes of `bytes` at `at` as a little-endian number.
fn read32(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("four bytes"))
}

/// The 128-bit product of the two, its halves xored together.
fn multiply_fold(left: u64, right: u64) -> u64 {
    let product = u128::from(left) * u128::from(right);
    (product as u64) ^ ((product >> 64) as u64)
}

/// The last mix of a body of up to 3 bytes: XXH64's.
fn xxh64_avalanche(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(PRIME64_2);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(PRIME64_3);
    hash ^ (hash >> 32)
}

/// The last mix of every body over 8 bytes.
fn avalanche(mut hash: u64) -> u64 {
    hash ^= hash >> 37;
    hash = hash.wrapping_mul(0x1656_6791_9E37_79F9);
    hash ^ (hash >> 32)
}

/// The last mix of a body of 4 to 8 bytes.
fn rrmxmx(mut hash: u64, length: u64) -> u64 {
    hash ^= hash.rotate_left(49) ^ hash.rotate_left(24);
    hash = hash.wrapping_mul(0x9FB2_1C65_1E98_DF25);
    hash ^= (hash >> 35).wrapping_add(length);
    hash = hash.wrapping_mul(0x9FB2_1C65_1E98_DF25);
    hash ^ (hash >> 28)
}

/// 16 bytes of `bytes` at `at` against 16 of the secret at `secret`, into
/// one number.
fn mix16(bytes: &[u8], at: usize, secret: usize) -> u64 {
    multiply_fold(read64(bytes, at) ^ read64(&SECRET, secret), read64(bytes, at + 8) ^ read64(&SECRET, secret + 8))
}

/// A body of up to 16 bytes: its first and last bytes, overlapping when it
/// is short, against fixed bytes of the secret.
fn up_to_16(bytes: &[u8]) -> u64 {
    let length = bytes.len();
    if length > 8 {
        let low = read64(bytes, 0) ^ (read64(&SECRET, 24) ^ read64(&SECRET, 32));
        let high = read64(bytes, length - 8) ^ (read64(&SECRET, 40) ^ read64(&SECRET, 48));
        let sum = (length as u64).wrapping_add(low.swap_bytes()).wrapping_add(high);
        return avalanche(sum.wrapping_add(multiply_fold(low, high)));
    }
    if length >= 4 {
        let both = u64::from(read32(bytes, length - 4)) | u64::from(read32(bytes, 0)) << 32;
        return rrmxmx(both ^ (read64(&SECRET, 8) ^ read64(&SECRET, 16)), length as u64);
    }
    if length > 0 {
        let combined = u32::from(bytes[0]) << 16
            | u32::from(bytes[length >> 1]) << 24
            | u32::from(bytes[length - 1])
            | (length as u32) << 8;
        return xxh64_avalanche(u64::from(combined) ^ u64::from(read32(&SECRET, 0) ^ read32(&SECRET, 4)));
    }
    xxh64_avalanche(read64(&SECRET, 56) ^ read64(&SECRET, 64))
}

/// A body of 17 to 128 bytes: 16 bytes from each end, then 16 more from each
/// end for every 32 the body has over 32.
fn up_to_128(bytes: &[u8]) -> u64 {
    let length = bytes.len();
    let mut accumulator = (length as u64).wrapping_mul(PRIME64_1);
    if length > 32 {
        if length > 64 {
            if length > 96 {
                accumulator = accumulator.wrapping_add(mix16(bytes, 48, 96));
                accumulator = accumulator.wrapping_add(mix16(bytes, length - 64, 112));
            }
            accumulator = accumulator.wrapping_add(mix16(bytes, 32, 64));
            accumulator = accumulator.wrapping_add(mix16(bytes, length - 48, 80));
        }
        accumulator = accumulator.wrapping_add(mix16(bytes, 16, 32));
        accumulator = accumulator.wrapping_add(mix16(bytes, length - 32, 48));
    }
    accumulator = accumulator.wrapping_add(mix16(bytes, 0, 0));
    accumulator = accumulator.wrapping_add(mix16(bytes, length - 16, 16));
    avalanche(accumulator)
}

/// A body of 129 to 240 bytes: every whole 16 bytes, mixed after the first
/// eight, then the last 16.
fn up_to_240(bytes: &[u8]) -> u64 {
    let length = bytes.len();
    let mut accumulator = (length as u64).wrapping_mul(PRIME64_1);
    for round in 0..8 {
        accumulator = accumulator.wrapping_add(mix16(bytes, 16 * round, 16 * round));
    }
    accumulator = avalanche(accumulator);
    for round in 8..length / 16 {
        accumulator = accumulator.wrapping_add(mix16(bytes, 16 * round, 16 * (round - 8) + 3));
    }
    accumulator = accumulator.wrapping_add(mix16(bytes, length - 16, 136 - 17));
    avalanche(accumulator)
}

/// A body over 240 bytes, by the widest path this processor runs.
#[allow(unsafe_code)]
fn long(bytes: &[u8]) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx512f") {
            // SAFETY: long_avx512 needs AVX-512F, which the processor has,
            // as just asked.
            return unsafe { x86::long_avx512(bytes) };
        }
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: long_avx2 needs AVX2, which the processor has, as
            // just asked.
            return unsafe { x86::long_avx2(bytes) };
        }
        x86::long_sse2(bytes)
    }
    #[cfg(not(target_arch = "x86_64"))]
    long_portable(bytes)
}

/// The 64 bytes of `bytes` at `at`: a stripe of a body, or of the secret.
fn stripe_at(bytes: &[u8], at: usize) -> &[u8; STRIPE] {
    bytes[at..at + STRIPE].try_into().expect("a stripe")
}

/// The stripe of the secret that keys the stripe `index` of a block.
fn secret_stripe(index: usize) -> &'static [u8; STRIPE] {
    stripe_at(&SECRET, 8 * index)
}

/// A body over 240 bytes: `stripe` mixes the stripe of the body it is given
/// into the accumulators it is given, keyed by the stripe of the secret it
/// is given; `combine` adds the odd stripes' sums into the accumulators and
/// starts them again, in the path's own instructions, since a loop that
/// touched the arrays a lane at a time as well as whole would keep them in
/// memory rather than in registers; `scramble` scrambles the accumulators
/// with the secret's last stripe after every block. Inlined into each
/// path's function, so that its closures inline in turn.
#[inline(always)]
fn long_hash(
    bytes: &[u8],
    stripe: impl Fn(&mut [u64; 8], &[u8; STRIPE], &[u8; STRIPE]),
    combine: impl Fn(&mut [u64; 8], &mut [u64; 8]),
    scramble: impl Fn(&mut [u64; 8], &[u8; STRIPE]),
) -> u64 {
    let mut accumulators = [PRIME32_3, PRIME64_1, PRIME64_2, PRIME64_3, PRIME64_4, PRIME32_2, PRIME64_5, PRIME32_1];
    // The odd stripes' sums; see the module comment.
    let mut odd = [0u64; 8];
    // Blocks, stripes and pairs of stripes as arrays of their length, so
    // that the compiler checks no bounds per stripe.
    let blocks = (bytes.len() - 1) / BLOCK;
    for block in &bytes.as_chunks::<BLOCK>().0[..blocks] {
        for (index, [even, odd_stripe]) in block.as_chunks::<STRIPE>().0.as_chunks::<2>().0.iter().enumerate() {
            stripe(&mut accumulators, even, secret_stripe(2 * index));
            stripe(&mut odd, odd_stripe, secret_stripe(2 * index + 1));
        }
        combine(&mut accumulators, &mut odd);
        scramble(&mut accumulators, stripe_at(&SECRET, SECRET.len() - STRIPE));
    }
    // The whole stripes of the last block, which is never empty, and its
    // last 64 bytes, which may overlap them, against bytes of the secret no
    // other stripe takes.
    let (pairs, rest) = bytes[blocks * BLOCK..bytes.len() - 1].as_chunks::<STRIPE>().0.as_chunks::<2>();
    for (index, [even, odd_stripe]) in pairs.iter().enumerate() {
        stripe(&mut accumulators, even, secret_stripe(2 * index));
        stripe(&mut odd, odd_stripe, secret_stripe(2 * index + 1));
    }
    if let [single] = rest {
        stripe(&mut accumulators, single, secret_stripe(2 * pairs.len()));
    }
    combine(&mut accumulators, &mut odd);
    stripe(&mut accumulators, stripe_at(bytes, bytes.len() - STRIPE), stripe_at(&SECRET, SECRET.len() - STRIPE - 7));
    let mut result = (bytes.len() as u64).wrapping_mul(PRIME64_1);
    for pair in 0..4 {
        let secret = 11 + 16 * pair;
        let folded = multiply_fold(
            accumulators[2 * pair] ^ read64(&SECRET, secret),
            accumulators[2 * pair + 1] ^ read64(&SECRET, secret + 8),
        );
        result = result.wrapping_add(folded);
    }
    avalanche(result)
}

/// One stripe, a lane at a time: each lane's bytes go into the lane next to
/// it, and the product of their two 32-bit halves, xored with the secret's,
/// into their own.
fn stripe_portable(accumulators: &mut [u64; 8], input: &[u8; STRIPE], secret: &[u8; STRIPE]) {
    for lane in 0..8 {
        let data = read64(input, 8 * lane);
        let keyed = data ^ read64(secret, 8 * lane);
        accumulators[lane ^ 1] = accumulators[lane ^ 1].wrapping_add(data);
        accumulators[lane] = accumulators[lane].wrapping_add((keyed & 0xFFFF_FFFF) * (keyed >> 32));
    }
}

/// The scramble after a block, a lane at a time.
fn scramble_portable(accumulators: &mut [u64; 8], secret: &[u8; STRIPE]) {
    for (lane, accumulator) in accumulators.iter_mut().enumerate() {
        let value = *accumulator ^ (*accumulator >> 47) ^ read64(secret, 8 * lane);
        *accumulator = value.wrapping_mul(PRIME32_1);
    }
}

/// The odd stripes' sums into the accumulators, a lane at a time.
fn combine_portable(accumulators: &mut [u64; 8], odd: &mut [u64; 8]) {
    for (accumulator, sum) in accumulators.iter_mut().zip(odd.iter_mut()) {
        *accumulator = accumulator.wrapping_add(*sum);
        *sum = 0;
    }
}

/// A body over 240 bytes, a lane at a time.
#[cfg_attr(target_arch = "x86_64", allow(dead_code))]
fn long_portable(bytes: &[u8]) -> u64 {
    long_hash(bytes, stripe_portable, combine_portable, scramble_portable)
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_code)]
mod x86 {
    //! The vector paths: per 64-bit lane, the product of the keyed data's
    //! low half (`mul_epu32` takes each lane's low 32 bits) and its high
    //! half (moved down by the shuffle 0x31), plus the data with each pair
    //! of lanes swapped (the shuffle 0x4E); a scramble multiplies by
    //! PRIME32_1 as two 32-bit products, the high one shifted up. The
    //! accumulators take product and data summed first, one addition a
    //! stripe on the chain from one stripe to the next. Loads and stores are
    //! unaligned, of the arrays' own bytes.

    use std::arch::x86_64::*;

    use super::{long_hash, PRIME32_1, STRIPE};

    /// One stripe by SSE2, 16 bytes at a time.
    #[inline]
    fn stripe_sse2(accumulators: &mut [u64; 8], input: &[u8; STRIPE], secret: &[u8; STRIPE]) {
        for part in 0..4 {
            // SAFETY: SSE2's instructions, which every x86-64 has, and the
            // 16 bytes at 16 * part of each 64-byte array, 2 * part of the
            // eight u64, read and written unaligned.
            unsafe {
                let lanes = accumulators.as_mut_ptr().add(2 * part).cast::<__m128i>();
                let data = _mm_loadu_si128(input.as_ptr().add(16 * part).cast());
                let keyed = _mm_xor_si128(data, _mm_loadu_si128(secret.as_ptr().add(16 * part).cast()));
                let product = _mm_mul_epu32(keyed, _mm_shuffle_epi32::<0x31>(keyed));
                let swapped = _mm_shuffle_epi32::<0x4E>(data);
                _mm_storeu_si128(lanes, _mm_add_epi64(_mm_loadu_si128(lanes), _mm_add_epi64(product, swapped)));
            }
        }
    }

    /// The scramble by SSE2.
    #[inline]
    fn scramble_sse2(accumulators: &mut [u64; 8], secret: &[u8; STRIPE]) {
        for part in 0..4 {
            // SAFETY: as in stripe_sse2.
            unsafe {
                let prime = _mm_set1_epi32(PRIME32_1 as u32 as i32);
                let lanes = accumulators.as_mut_ptr().add(2 * part).cast::<__m128i>();
                let value = _mm_loadu_si128(lanes);
                let keyed = _mm_xor_si128(
                    _mm_xor_si128(value, _mm_srli_epi64::<47>(value)),
                    _mm_loadu_si128(secret.as_ptr().add(16 * part).cast()),
                );
                let low = _mm_mul_epu32(keyed, prime);
                let high = _mm_mul_epu32(_mm_shuffle_epi32::<0x31>(keyed), prime);
                _mm_storeu_si128(lanes, _mm_add_epi64(low, _mm_slli_epi64::<32>(high)));
            }
        }
    }

    /// The odd stripes' sums into the accumulators by SSE2.
    #[inline]
    fn combine_sse2(accumulators: &mut [u64; 8], odd: &mut [u64; 8]) {
        for part in 0..4 {
            // SAFETY: as in stripe_sse2, the 16 bytes at 2 * part of each
            // array of eight u64.
            unsafe {
                let lanes = accumulators.as_mut_ptr().add(2 * part).cast::<__m128i>();
                let odd_lanes = odd.as_mut_ptr().add(2 * part).cast::<__m128i>();
                _mm_storeu_si128(lanes, _mm_add_epi64(_mm_loadu_si128(lanes), _mm_loadu_si128(odd_lanes)));
                _mm_storeu_si128(odd_lanes, _mm_setzero_si128());
            }
        }
    }

    /// A body over 240 bytes by SSE2, which every x86-64 has.
    pub(super) fn long_sse2(bytes: &[u8]) -> u64 {
        long_hash(bytes, stripe_sse2, combine_sse2, scramble_sse2)
    }

    /// One stripe by AVX2, 32 bytes at a time.
    #[target_feature(enable = "avx2")]
    #[inline]
    fn stripe_avx2(accumulators: &mut [u64; 8], input: &[u8; STRIPE], secret: &[u8; STRIPE]) {
        for part in 0..2 {
            // SAFETY: the 32 bytes at 32 * part of each 64-byte array, 4 *
            // part of the eight u64, read and written unaligned.
            unsafe {
                let lanes = accumulators.as_mut_ptr().add(4 * part).cast::<__m256i>();
                let data = _mm256_loadu_si256(input.as_ptr().add(32 * part).cast());
                let keyed = _mm256_xor_si256(data, _mm256_loadu_si256(secret.as_ptr().add(32 * part).cast()));
                let product = _mm256_mul_epu32(keyed, _mm256_shuffle_epi32::<0x31>(keyed));
                let swapped = _mm256_shuffle_epi32::<0x4E>(data);
                _mm256_storeu_si256(
                    lanes,
                    _mm256_add_epi64(_mm256_loadu_si256(lanes), _mm256_add_epi64(product, swapped)),
                );
            }
        }
    }

    /// The scramble by AVX2.
    #[target_feature(enable = "avx2")]
    #[inline]
    fn scramble_avx2(accumulators: &mut [u64; 8], secret: &[u8; STRIPE]) {
        let prime = _mm256_set1_epi32(PRIME32_1 as u32 as i32);
        for part in 0..2 {
            // SAFETY: as in stripe_avx2.
            unsafe {
                let lanes = accumulators.as_mut_ptr().add(4 * part).cast::<__m256i>();
                let value = _mm256_loadu_si256(lanes);
                let keyed = _mm256_xor_si256(
                    _mm256_xor_si256(value, _mm256_srli_epi64::<47>(value)),
                    _mm256_loadu_si256(secret.as_ptr().add(32 * part).cast()),
                );
                let low = _mm256_mul_epu32(keyed, prime);
                let high = _mm256_mul_epu32(_mm256_shuffle_epi32::<0x31>(keyed), prime);
                _mm256_storeu_si256(lanes, _mm256_add_epi64(low, _mm256_slli_epi64::<32>(high)));
            }
        }
    }

    /// The odd stripes' sums into the accumulators by AVX2.
    #[target_feature(enable = "avx2")]
    #[inline]
    fn combine_avx2(accumulators: &mut [u64; 8], odd: &mut [u64; 8]) {
        for part in 0..2 {
            // SAFETY: the 32 bytes at 4 * part of each array of eight u64,
            // read and written unaligned.
            unsafe {
                let lanes = accumulators.as_mut_ptr().add(4 * part).cast::<__m256i>();
                let odd_lanes = odd.as_mut_ptr().add(4 * part).cast::<__m256i>();
                _mm256_storeu_si256(lanes, _mm256_add_epi64(_mm256_loadu_si256(lanes), _mm256_loadu_si256(odd_lanes)));
                _mm256_storeu_si256(odd_lanes, _mm256_setzero_si256());
            }
        }
    }

    /// A body over 240 bytes by AVX2.
    #[target_feature(enable = "avx2")]
    pub(super) fn long_avx2(bytes: &[u8]) -> u64 {
        long_hash(
            bytes,
            |accumulators, input, secret| stripe_avx2(accumulators, input, secret),
            |accumulators, odd| combine_avx2(accumulators, odd),
            |accumulators, secret| scramble_avx2(accumulators, secret),
        )
    }

    /// One stripe by AVX-512, all 64 bytes at once.
    #[target_feature(enable = "avx512f")]
    #[inline]
    fn stripe_avx512(accumulators: &mut [u64; 8], input: &[u8; STRIPE], secret: &[u8; STRIPE]) {
        // SAFETY: each array's 64 bytes, read and written unaligned.
        unsafe {
            let lanes = accumulators.as_mut_ptr().cast::<__m512i>();
            let data = _mm512_loadu_si512(input.as_ptr().cast());
            let keyed = _mm512_xor_si512(data, _mm512_loadu_si512(secret.as_ptr().cast()));
            let product = _mm512_mul_epu32(keyed, _mm512_shuffle_epi32::<0x31>(keyed));
            let swapped = _mm512_shuffle_epi32::<0x4E>(data);
            _mm512_storeu_si512(lanes, _mm512_add_epi64(_mm512_loadu_si512(lanes), _mm512_add_epi64(product, swapped)));
        }
    }

    /// The scramble by AVX-512: value ^ (value >> 47) ^ secret in one
    /// instruction, 0x96 being the three-way xor.
    #[target_feature(enable = "avx512f")]
    #[inline]
    fn scramble_avx512(accumulators: &mut [u64; 8], secret: &[u8; STRIPE]) {
        let prime = _mm512_set1_epi32(PRIME32_1 as u32 as i32);
        // SAFETY: as in stripe_avx512.
        unsafe {
            let lanes = accumulators.as_mut_ptr().cast::<__m512i>();
            let value = _mm512_loadu_si512(lanes);
            let keyed = _mm512_ternarylogic_epi64::<0x96>(
                value,
                _mm512_srli_epi64::<47>(value),
                _mm512_loadu_si512(secret.as_ptr().cast()),
            );
            let low = _mm512_mul_epu32(keyed, prime);
            let high = _mm512_mul_epu32(_mm512_shuffle_epi32::<0x31>(keyed), prime);
            _mm512_storeu_si512(lanes, _mm512_add_epi64(low, _mm512_slli_epi64::<32>(high)));
        }
    }

    /// The odd stripes' sums into the accumulators by AVX-512.
    #[target_feature(enable = "avx512f")]
    #[inline]
    fn combine_avx512(accumulators: &mut [u64; 8], odd: &mut [u64; 8]) {
        // SAFETY: each array's 64 bytes, read and written unaligned.
        unsafe {
            let lanes = accumulators.as_mut_ptr().cast::<__m512i>();
            let odd_lanes = odd.as_mut_ptr().cast::<__m512i>();
            _mm512_storeu_si512(lanes, _mm512_add_epi64(_mm512_loadu_si512(lanes), _mm512_loadu_si512(odd_lanes)));
            _mm512_storeu_si512(odd_lanes, _mm512_setzero_si512());
        }
    }

    /// A body over 240 bytes by AVX-512.
    #[target_feature(enable = "avx512f")]
    pub(super) fn long_avx512(bytes: &[u8]) -> u64 {
        long_hash(
            bytes,
            |accumulators, input, secret| stripe_avx512(accumulators, input, secret),
            |accumulators, odd| combine_avx512(accumulators, odd),
            |accumulators, secret| scramble_avx512(accumulators, secret),
        )
    }
}

#[cfg(test)]
mod tests {
    //! Against the values of xxHash's own sanity check, which xxHash 0.8.1
    //! and LLVM 18's implementation agree on, as lib/xxh3_test.cc: at the
    //! lengths it takes, which reach every way XXH3 hashes a body, at the
    //! edges between those ways and over a megabyte; and every path this
    //! processor runs against the others at every length up to five blocks
    //! from every start within eight bytes.

    use super::{long_portable, xxh3_64};

    /// A way a body over 240 bytes is hashed: its name and its function.
    type Path = (&'static str, fn(&[u8]) -> u64);

    /// The ways a body over 240 bytes is hashed that this processor runs.
    #[allow(unsafe_code)]
    fn paths() -> Vec<Path> {
        #[allow(unused_mut)]
        let mut paths: Vec<Path> = vec![("portable", long_portable)];
        #[cfg(target_arch = "x86_64")]
        {
            paths.push(("sse2", super::x86::long_sse2));
            if std::arch::is_x86_feature_detected!("avx2") {
                paths.push(("avx2", |bytes| {
                    // SAFETY: the processor has AVX2, as asked above.
                    unsafe { super::x86::long_avx2(bytes) }
                }));
            }
            if std::arch::is_x86_feature_detected!("avx512f") {
                paths.push(("avx512", |bytes| {
                    // SAFETY: the processor has AVX-512F, as asked above.
                    unsafe { super::x86::long_avx512(bytes) }
                }));
            }
        }
        paths
    }

    /// The bytes xxHash's sanity check hashes: the top byte of a number
    /// multiplied by a 64-bit prime at each step.
    fn sanity_buffer(length: usize) -> Vec<u8> {
        let mut state: u64 = 2_654_435_761;
        (0..length)
            .map(|_| {
                let byte = (state >> 56) as u8;
                state = state.wrapping_mul(11_400_714_785_074_694_797);
                byte
            })
            .collect()
    }

    #[test]
    fn matches_xxhash_on_its_own_sanity_check() {
        let known: [(usize, u64); 27] = [
            // xxHash's sanity check, seed 0.
            (0, 0x2D06_8005_38D3_94C2),
            (1, 0xC44B_DFF4_074E_ECDB),
            (6, 0x27B5_6A84_CD2D_7325),
            (12, 0xA713_DAF0_DFBB_77E7),
            (24, 0xA3FE_70BF_9D35_10EB),
            (48, 0x397D_A259_ECBA_1F11),
            (80, 0xBCDE_FBBB_2C47_C90A),
            (195, 0xCD94_217E_E362_EC3A),
            (403, 0xCDEB_804D_65C6_DEA4),
            (512, 0x617E_4959_9013_CB6B),
            (2048, 0xDD59_E2C3_A5F0_38E0),
            (2099, 0xC6B9_D9B3_FC9A_C765),
            (2240, 0x6E73_A905_39CF_2948),
            (2367, 0xCB37_AEB9_E5D3_61ED),
            // Either side of where XXH3 changes its way, and a megabyte.
            (3, 0x5424_7382_A8D6_B94D),
            (4, 0xE5DC_74BC_5184_8A51),
            (8, 0x24CC_C9AC_AA9F_65E4),
            (9, 0x14D5_001C_15DD_3F2B),
            (16, 0x981B_17D3_6C74_98C9),
            (17, 0x796F_5ACD_3A60_F862),
            (128, 0xFCFF_2412_6754_D861),
            (129, 0x98F1_B0A6_79A2_CA29),
            (240, 0x81C3_C2B6_7F56_8CCF),
            (241, 0xC5A6_39EC_D203_0E5E),
            (1024, 0xDD85_C9B5_C110_9C5C),
            (1025, 0xD870_C0FA_1321_1C6A),
            (1 << 20, 0x7ADE_52BB_FBD6_97BA),
        ];
        let buffer = sanity_buffer(1 << 20);
        for (length, hash) in known {
            assert_eq!(hash, xxh3_64(&buffer[..length]), "length {length}");
            if length > 240 {
                for (name, path) in paths() {
                    assert_eq!(hash, path(&buffer[..length]), "length {length}, {name}");
                }
            }
        }
    }

    #[test]
    fn every_path_agrees_at_every_length_and_start() {
        // Five blocks of 1024 and a part: the stripes of a block, the
        // scramble after it and the last stripe that overlaps the ones
        // before.
        let buffer = sanity_buffer(5 * 1024 + 200);
        let paths = paths();
        for start in 0..8 {
            for length in 241..=buffer.len() - start {
                let bytes = &buffer[start..start + length];
                let expected = long_portable(bytes);
                assert_eq!(expected, xxh3_64(bytes), "start {start}, length {length}");
                for (name, path) in &paths {
                    assert_eq!(expected, path(bytes), "start {start}, length {length}, {name}");
                }
            }
        }
    }
}
