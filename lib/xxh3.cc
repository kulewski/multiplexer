// mx::xxh3_64 as XXH3's specification describes it, seed 0 and the default
// secret: a body of up to 240 bytes by a few multiplications, a longer one
// in stripes of 64 bytes, each mixed into eight 64-bit accumulators by one
// multiplication and two additions a lane, the accumulators scrambled after
// every 1024 bytes and folded into one value at the end. See xxh3.h for what
// is computed and who uses it.
//
// A long body's time is its stripes, whose eight lanes are independent, so
// they go 16, 32 or 64 bytes at a time by SSE2, AVX2 or AVX-512: the widest
// the processor says it has, asked once, the first time a long body comes,
// so that the same binary runs on any x86-64, every one of which has SSE2.
// Other processors take one lane at a time. Within a block the odd stripes
// sum apart and join before the scramble (long_hash), so that a stripe
// waits for the one two before it rather than the one before.
//
// Measured on 2026-10-15 on an AMD Ryzen Threadripper PRO 9965WX, built by
// GCC for any x86-64, one call after another: a 125-byte body 6 ns, a
// kilobyte 16 ns, a megabyte 7.6 µs (138 GB/s), by AVX-512; by SSE2 alone a
// kilobyte 36 ns and a megabyte 34 µs. zlib's CRC-32 through tables, which
// the frames carried up to 3.0.0, took 46 ns, 320 ns and 330 µs; CRC-32C by
// the SSE4.2 instruction in three streams, the fastest CRC short of
// AVX-512 folding, 13 ns, 41 ns and 25 µs. Clang's build of this file takes
// about 1.5 ns more for 17 to 240 bytes one call after another, its
// vectorizer packing the two halves of each mix16 into one register, and
// less for bodies hashed apart; its long hash takes what GCC's does, but for
// the register a stripe's sum is held in (stripe_avx512) a third more.
//
// Each path's long hash is a function compiled for its instructions, which
// passes its stripe, combine and scramble functions to one loop, long_hash,
// inlined into it: an intrinsic inlines only into a function compiled for
// it, and the loop, compiled for none, calls them through pointers that are
// constants once it is inlined, as xxHash's own dispatch does.
#include "lib/xxh3.h"

#include <atomic>
#include <cstring>

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#include <immintrin.h>
#define MX_XXH3_X86 1
#endif

namespace mx {

namespace {

const std::uint64_t PRIME32_1 = 0x9E3779B1u;
const std::uint64_t PRIME32_2 = 0x85EBCA77u;
const std::uint64_t PRIME32_3 = 0xC2B2AE3Du;
const std::uint64_t PRIME64_1 = 0x9E3779B185EBCA87ull;
const std::uint64_t PRIME64_2 = 0xC2B2AE3D27D4EB4Full;
const std::uint64_t PRIME64_3 = 0x165667B19E3779F9ull;
const std::uint64_t PRIME64_4 = 0x85EBCA77C2B2AE63ull;
const std::uint64_t PRIME64_5 = 0x27D4EB2F165667C5ull;

// The default secret, which the specification fixes: every input is mixed
// with some of its bytes.
alignas(64) const unsigned char SECRET[192] = {
    0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c, 0xde, 0xd4,
    0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f, 0xcb, 0x79, 0xe6, 0x4e,
    0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21, 0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43,
    0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c, 0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb,
    0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3, 0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19,
    0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8, 0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7,
    0xc7, 0x0b, 0x4f, 0x1d, 0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78,
    0x73, 0x64, 0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb,
    0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e, 0x2b, 0x16,
    0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce, 0x45, 0xcb, 0x3a, 0x8f,
    0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e,
};

const std::size_t STRIPE = 64;  // the bytes one step mixes in
// A stripe moves 8 bytes along the secret: a block of 16 stripes, 1024
// bytes, uses it all, and the accumulators are scrambled after each.
const std::size_t STRIPES_PER_BLOCK = (sizeof(SECRET) - STRIPE) / 8;
const std::size_t BLOCK = STRIPE * STRIPES_PER_BLOCK;

// The eight bytes at `bytes` as a little-endian number, whatever the
// machine's byte order and the pointer's alignment.
inline std::uint64_t read64(const unsigned char* bytes) {
  std::uint64_t value;
  std::memcpy(&value, bytes, sizeof(value));
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  value = __builtin_bswap64(value);
#endif
  return value;
}

// The four bytes at `bytes` as a little-endian number.
inline std::uint32_t read32(const unsigned char* bytes) {
  std::uint32_t value;
  std::memcpy(&value, bytes, sizeof(value));
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  value = __builtin_bswap32(value);
#endif
  return value;
}

inline std::uint64_t rotate_left(std::uint64_t value, int bits) { return (value << bits) | (value >> (64 - bits)); }

// The 128-bit product of the two, its halves xored together.
inline std::uint64_t multiply_fold(std::uint64_t left, std::uint64_t right) {
#ifdef __SIZEOF_INT128__
  const unsigned __int128 product = static_cast<unsigned __int128>(left) * right;
  return static_cast<std::uint64_t>(product) ^ static_cast<std::uint64_t>(product >> 64);
#else
  // Four 32-bit products, as a processor without 64-bit multiplication
  // into 128 bits needs.
  const std::uint64_t low_low = (left & 0xFFFFFFFFu) * (right & 0xFFFFFFFFu);
  const std::uint64_t high_low = (left >> 32) * (right & 0xFFFFFFFFu);
  const std::uint64_t low_high = (left & 0xFFFFFFFFu) * (right >> 32);
  const std::uint64_t high_high = (left >> 32) * (right >> 32);
  const std::uint64_t cross = (low_low >> 32) + (high_low & 0xFFFFFFFFu) + low_high;
  const std::uint64_t upper = (high_low >> 32) + (cross >> 32) + high_high;
  const std::uint64_t lower = (cross << 32) | (low_low & 0xFFFFFFFFu);
  return lower ^ upper;
#endif
}

// The last mix of a body of up to 3 bytes: XXH64's.
inline std::uint64_t xxh64_avalanche(std::uint64_t hash) {
  hash ^= hash >> 33;
  hash *= PRIME64_2;
  hash ^= hash >> 29;
  hash *= PRIME64_3;
  return hash ^ (hash >> 32);
}

// The last mix of every body over 8 bytes.
inline std::uint64_t avalanche(std::uint64_t hash) {
  hash ^= hash >> 37;
  hash *= 0x165667919E3779F9ull;
  return hash ^ (hash >> 32);
}

// The last mix of a body of 4 to 8 bytes.
inline std::uint64_t rrmxmx(std::uint64_t hash, std::uint64_t size) {
  hash ^= rotate_left(hash, 49) ^ rotate_left(hash, 24);
  hash *= 0x9FB21C651E98DF25ull;
  hash ^= (hash >> 35) + size;
  hash *= 0x9FB21C651E98DF25ull;
  return hash ^ (hash >> 28);
}

// 16 bytes of input against 16 of the secret, into one number.
inline std::uint64_t mix16(const unsigned char* input, const unsigned char* secret) {
  return multiply_fold(read64(input) ^ read64(secret), read64(input + 8) ^ read64(secret + 8));
}

// A body of up to 16 bytes: its first and last bytes, overlapping when
// it is short, against fixed bytes of the secret.
std::uint64_t up_to_16(const unsigned char* input, std::size_t size) {
  if (size > 8) {
    const std::uint64_t low = read64(input) ^ (read64(SECRET + 24) ^ read64(SECRET + 32));
    const std::uint64_t high = read64(input + size - 8) ^ (read64(SECRET + 40) ^ read64(SECRET + 48));
    return avalanche(size + __builtin_bswap64(low) + high + multiply_fold(low, high));
  }
  if (size >= 4) {
    const std::uint64_t both = read32(input + size - 4) + (static_cast<std::uint64_t>(read32(input)) << 32);
    return rrmxmx(both ^ (read64(SECRET + 8) ^ read64(SECRET + 16)), size);
  }
  if (size > 0) {
    const std::uint32_t combined = static_cast<std::uint32_t>(input[0]) << 16 |
                                   static_cast<std::uint32_t>(input[size >> 1]) << 24 | input[size - 1] |
                                   static_cast<std::uint32_t>(size) << 8;
    return xxh64_avalanche(combined ^ static_cast<std::uint64_t>(read32(SECRET) ^ read32(SECRET + 4)));
  }
  return xxh64_avalanche(read64(SECRET + 56) ^ read64(SECRET + 64));
}

// A body of 17 to 128 bytes: 16 bytes from each end, then 16 more from
// each end for every 32 the body has over 32.
std::uint64_t up_to_128(const unsigned char* input, std::size_t size) {
  std::uint64_t accumulator = size * PRIME64_1;
  if (size > 32) {
    if (size > 64) {
      if (size > 96) {
        accumulator += mix16(input + 48, SECRET + 96);
        accumulator += mix16(input + size - 64, SECRET + 112);
      }
      accumulator += mix16(input + 32, SECRET + 64);
      accumulator += mix16(input + size - 48, SECRET + 80);
    }
    accumulator += mix16(input + 16, SECRET + 32);
    accumulator += mix16(input + size - 32, SECRET + 48);
  }
  accumulator += mix16(input, SECRET);
  accumulator += mix16(input + size - 16, SECRET + 16);
  return avalanche(accumulator);
}

// A body of 129 to 240 bytes: every whole 16 bytes, mixed after the first
// eight, then the last 16.
std::uint64_t up_to_240(const unsigned char* input, std::size_t size) {
  std::uint64_t accumulator = size * PRIME64_1;
  for (std::size_t round = 0; round < 8; ++round) {
    accumulator += mix16(input + 16 * round, SECRET + 16 * round);
  }
  accumulator = avalanche(accumulator);
  const std::size_t rounds = size / 16;
  for (std::size_t round = 8; round < rounds; ++round) {
    accumulator += mix16(input + 16 * round, SECRET + 16 * (round - 8) + 3);
  }
  accumulator += mix16(input + size - 16, SECRET + 136 - 17);
  return avalanche(accumulator);
}

// Mixes one stripe of 64 bytes into the eight accumulators: each lane's
// bytes go into the lane next to it, and the product of their two 32-bit
// halves, xored with the secret's, into their own.
typedef void (*Stripe)(std::uint64_t* accumulators, const unsigned char* input, const unsigned char* secret);
// Scrambles the accumulators with 64 bytes of the secret, after a block.
typedef void (*Scramble)(std::uint64_t* accumulators, const unsigned char* secret);

// Adds the odd stripes' sums into the accumulators and starts them again,
// in the path's own instructions: a loop that touched the arrays a lane at a
// time as well as whole would keep them in memory rather than in registers.
typedef void (*Combine)(std::uint64_t* accumulators, std::uint64_t* odd);

// A body over 240 bytes, by `stripe`, `combine` and `scramble`: inlined into
// each path's function, where they inline in turn.
__attribute__((always_inline)) inline std::uint64_t long_hash(const unsigned char* input, std::size_t size,
                                                              Stripe stripe, Combine combine, Scramble scramble) {
  alignas(64) std::uint64_t accumulators[8] = {PRIME32_3, PRIME64_1, PRIME64_2, PRIME64_3,
                                               PRIME64_4, PRIME32_2, PRIME64_5, PRIME32_1};
  // The odd stripes' sums, added to the accumulators before each scramble:
  // the same sums, addition modulo 2^64 taking any order, on a chain of
  // their own, so that a stripe waits for the one two before it, not the
  // one before, however the compiler orders a stripe's two additions.
  alignas(64) std::uint64_t odd[8] = {};
  const std::size_t blocks = (size - 1) / BLOCK;
  for (std::size_t block = 0; block < blocks; ++block) {
    const unsigned char* const start = input + block * BLOCK;
    for (std::size_t index = 0; index < STRIPES_PER_BLOCK; index += 2) {
      stripe(accumulators, start + index * STRIPE, SECRET + index * 8);
      stripe(odd, start + (index + 1) * STRIPE, SECRET + (index + 1) * 8);
    }
    combine(accumulators, odd);
    scramble(accumulators, SECRET + sizeof(SECRET) - STRIPE);
  }
  // The whole stripes of the last block, which is never empty, and its last
  // 64 bytes, which may overlap them, against bytes of the secret no other
  // stripe takes.
  const unsigned char* const start = input + blocks * BLOCK;
  const std::size_t stripes = (size - 1 - blocks * BLOCK) / STRIPE;
  std::size_t index = 0;
  for (; index + 1 < stripes; index += 2) {
    stripe(accumulators, start + index * STRIPE, SECRET + index * 8);
    stripe(odd, start + (index + 1) * STRIPE, SECRET + (index + 1) * 8);
  }
  if (index < stripes) {
    stripe(accumulators, start + index * STRIPE, SECRET + index * 8);
  }
  combine(accumulators, odd);
  stripe(accumulators, input + size - STRIPE, SECRET + sizeof(SECRET) - STRIPE - 7);
  std::uint64_t result = size * PRIME64_1;
  for (std::size_t pair = 0; pair < 4; ++pair) {
    const unsigned char* const secret = SECRET + 11 + 16 * pair;
    result += multiply_fold(accumulators[2 * pair] ^ read64(secret), accumulators[2 * pair + 1] ^ read64(secret + 8));
  }
  return avalanche(result);
}

__attribute__((always_inline)) inline void stripe_portable(std::uint64_t* accumulators, const unsigned char* input,
                                                           const unsigned char* secret) {
  for (std::size_t lane = 0; lane < 8; ++lane) {
    const std::uint64_t data = read64(input + 8 * lane);
    const std::uint64_t keyed = data ^ read64(secret + 8 * lane);
    accumulators[lane ^ 1] += data;
    accumulators[lane] += (keyed & 0xFFFFFFFFu) * (keyed >> 32);
  }
}

__attribute__((always_inline)) inline void scramble_portable(std::uint64_t* accumulators, const unsigned char* secret) {
  for (std::size_t lane = 0; lane < 8; ++lane) {
    std::uint64_t value = accumulators[lane];
    value ^= value >> 47;
    value ^= read64(secret + 8 * lane);
    accumulators[lane] = value * PRIME32_1;
  }
}

__attribute__((always_inline)) inline void combine_portable(std::uint64_t* accumulators, std::uint64_t* odd) {
  for (std::size_t lane = 0; lane < 8; ++lane) {
    accumulators[lane] += odd[lane];
    odd[lane] = 0;
  }
}

std::uint64_t long_portable(const unsigned char* input, std::size_t size) {
  return long_hash(input, size, stripe_portable, combine_portable, scramble_portable);
}

#ifdef MX_XXH3_X86

// The vector paths: per 64-bit lane, the product of the keyed data's low
// half (_mm_mul_epu32 takes each lane's low 32 bits) and its high half
// (moved down by the shuffle), plus the data with each pair of lanes
// swapped; a scramble multiplies by PRIME32_1 as two 32-bit products, the
// high one shifted up. The accumulators take product and data summed first,
// one addition a stripe on the chain from one stripe to the next.

__attribute__((always_inline)) inline void stripe_sse2(std::uint64_t* accumulators, const unsigned char* input,
                                                       const unsigned char* secret) {
  for (int part = 0; part < 4; ++part) {
    __m128i* const lanes = reinterpret_cast<__m128i*>(accumulators) + part;
    const __m128i data = _mm_loadu_si128(reinterpret_cast<const __m128i*>(input) + part);
    const __m128i keyed = _mm_xor_si128(data, _mm_loadu_si128(reinterpret_cast<const __m128i*>(secret) + part));
    const __m128i product = _mm_mul_epu32(keyed, _mm_shuffle_epi32(keyed, _MM_SHUFFLE(0, 3, 0, 1)));
    const __m128i swapped = _mm_shuffle_epi32(data, _MM_SHUFFLE(1, 0, 3, 2));
    _mm_store_si128(lanes, _mm_add_epi64(_mm_load_si128(lanes), _mm_add_epi64(product, swapped)));
  }
}

__attribute__((always_inline)) inline void scramble_sse2(std::uint64_t* accumulators, const unsigned char* secret) {
  const __m128i prime = _mm_set1_epi32(static_cast<int>(static_cast<std::uint32_t>(PRIME32_1)));
  for (int part = 0; part < 4; ++part) {
    __m128i* const lanes = reinterpret_cast<__m128i*>(accumulators) + part;
    const __m128i value = _mm_load_si128(lanes);
    const __m128i keyed = _mm_xor_si128(_mm_xor_si128(value, _mm_srli_epi64(value, 47)),
                                        _mm_loadu_si128(reinterpret_cast<const __m128i*>(secret) + part));
    const __m128i low = _mm_mul_epu32(keyed, prime);
    const __m128i high = _mm_mul_epu32(_mm_shuffle_epi32(keyed, _MM_SHUFFLE(0, 3, 0, 1)), prime);
    _mm_store_si128(lanes, _mm_add_epi64(low, _mm_slli_epi64(high, 32)));
  }
}

__attribute__((always_inline)) inline void combine_sse2(std::uint64_t* accumulators, std::uint64_t* odd) {
  for (int part = 0; part < 4; ++part) {
    __m128i* const lanes = reinterpret_cast<__m128i*>(accumulators) + part;
    __m128i* const odd_lanes = reinterpret_cast<__m128i*>(odd) + part;
    _mm_store_si128(lanes, _mm_add_epi64(_mm_load_si128(lanes), _mm_load_si128(odd_lanes)));
    _mm_store_si128(odd_lanes, _mm_setzero_si128());
  }
}

std::uint64_t long_sse2(const unsigned char* input, std::size_t size) {
  return long_hash(input, size, stripe_sse2, combine_sse2, scramble_sse2);
}

__attribute__((always_inline, target("avx2"))) inline void stripe_avx2(std::uint64_t* accumulators,
                                                                       const unsigned char* input,
                                                                       const unsigned char* secret) {
  for (int part = 0; part < 2; ++part) {
    __m256i* const lanes = reinterpret_cast<__m256i*>(accumulators) + part;
    const __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(input) + part);
    const __m256i keyed = _mm256_xor_si256(data, _mm256_loadu_si256(reinterpret_cast<const __m256i*>(secret) + part));
    const __m256i product = _mm256_mul_epu32(keyed, _mm256_shuffle_epi32(keyed, _MM_SHUFFLE(0, 3, 0, 1)));
    const __m256i swapped = _mm256_shuffle_epi32(data, _MM_SHUFFLE(1, 0, 3, 2));
    _mm256_store_si256(lanes, _mm256_add_epi64(_mm256_load_si256(lanes), _mm256_add_epi64(product, swapped)));
  }
}

__attribute__((always_inline, target("avx2"))) inline void scramble_avx2(std::uint64_t* accumulators,
                                                                         const unsigned char* secret) {
  const __m256i prime = _mm256_set1_epi32(static_cast<int>(static_cast<std::uint32_t>(PRIME32_1)));
  for (int part = 0; part < 2; ++part) {
    __m256i* const lanes = reinterpret_cast<__m256i*>(accumulators) + part;
    const __m256i value = _mm256_load_si256(lanes);
    const __m256i keyed = _mm256_xor_si256(_mm256_xor_si256(value, _mm256_srli_epi64(value, 47)),
                                           _mm256_loadu_si256(reinterpret_cast<const __m256i*>(secret) + part));
    const __m256i low = _mm256_mul_epu32(keyed, prime);
    const __m256i high = _mm256_mul_epu32(_mm256_shuffle_epi32(keyed, _MM_SHUFFLE(0, 3, 0, 1)), prime);
    _mm256_store_si256(lanes, _mm256_add_epi64(low, _mm256_slli_epi64(high, 32)));
  }
}

__attribute__((always_inline, target("avx2"))) inline void combine_avx2(std::uint64_t* accumulators,
                                                                        std::uint64_t* odd) {
  for (int part = 0; part < 2; ++part) {
    __m256i* const lanes = reinterpret_cast<__m256i*>(accumulators) + part;
    __m256i* const odd_lanes = reinterpret_cast<__m256i*>(odd) + part;
    _mm256_store_si256(lanes, _mm256_add_epi64(_mm256_load_si256(lanes), _mm256_load_si256(odd_lanes)));
    _mm256_store_si256(odd_lanes, _mm256_setzero_si256());
  }
}

__attribute__((target("avx2"))) std::uint64_t long_avx2(const unsigned char* input, std::size_t size) {
  return long_hash(input, size, stripe_avx2, combine_avx2, scramble_avx2);
}

// _mm512_shuffle_epi32, _mm512_mul_epu32 and the shifts as GCC 12 has
// them give their instruction an undefined vector that their full mask
// never uses, which GCC 12's -Wmaybe-uninitialized then reports where they
// inline here, a false positive that no pragma silences and GCC 13 no
// longer makes; under a full mask their zeroing forms are the same
// instructions. Elsewhere the intrinsics themselves.
template <int Order>
__attribute__((always_inline, target("avx512f"))) inline __m512i shuffle_epi32(__m512i value) {
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ == 12
  return _mm512_maskz_shuffle_epi32(static_cast<__mmask16>(0xFFFF), value, static_cast<_MM_PERM_ENUM>(Order));
#else
  return _mm512_shuffle_epi32(value, static_cast<_MM_PERM_ENUM>(Order));
#endif
}

__attribute__((always_inline, target("avx512f"))) inline __m512i mul_epu32(__m512i left, __m512i right) {
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ == 12
  return _mm512_maskz_mul_epu32(static_cast<__mmask8>(0xFF), left, right);
#else
  return _mm512_mul_epu32(left, right);
#endif
}

template <unsigned int Bits>
__attribute__((always_inline, target("avx512f"))) inline __m512i srli_epi64(__m512i value) {
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ == 12
  return _mm512_maskz_srli_epi64(static_cast<__mmask8>(0xFF), value, Bits);
#else
  return _mm512_srli_epi64(value, Bits);
#endif
}

template <unsigned int Bits>
__attribute__((always_inline, target("avx512f"))) inline __m512i slli_epi64(__m512i value) {
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ == 12
  return _mm512_maskz_slli_epi64(static_cast<__mmask8>(0xFF), value, Bits);
#else
  return _mm512_slli_epi64(value, Bits);
#endif
}

__attribute__((always_inline, target("avx512f"))) inline void stripe_avx512(std::uint64_t* accumulators,
                                                                            const unsigned char* input,
                                                                            const unsigned char* secret) {
  const __m512i data = _mm512_loadu_si512(input);
  const __m512i keyed = _mm512_xor_si512(data, _mm512_loadu_si512(secret));
  const __m512i product = mul_epu32(keyed, shuffle_epi32<_MM_SHUFFLE(0, 3, 0, 1)>(keyed));
  const __m512i swapped = shuffle_epi32<_MM_SHUFFLE(1, 0, 3, 2)>(data);
  // The sum held in a register, so that it joins the accumulators in one
  // addition: clang would add the product and the data to them one after
  // the other, two additions on the chain from stripe to stripe, a megabyte
  // in 10.3 µs where GCC's build takes 7.8. The other paths' sums, measured
  // the same way, need nothing.
  __m512i sum = _mm512_add_epi64(product, swapped);
  __asm__("" : "+v"(sum));
  _mm512_store_si512(accumulators, _mm512_add_epi64(_mm512_load_si512(accumulators), sum));
}

__attribute__((always_inline, target("avx512f"))) inline void scramble_avx512(std::uint64_t* accumulators,
                                                                              const unsigned char* secret) {
  const __m512i prime = _mm512_set1_epi32(static_cast<int>(static_cast<std::uint32_t>(PRIME32_1)));
  const __m512i value = _mm512_load_si512(accumulators);
  // value ^ (value >> 47) ^ secret in one instruction: 0x96 is three-way xor.
  const __m512i keyed = _mm512_ternarylogic_epi64(value, srli_epi64<47>(value), _mm512_loadu_si512(secret), 0x96);
  const __m512i low = mul_epu32(keyed, prime);
  const __m512i high = mul_epu32(shuffle_epi32<_MM_SHUFFLE(0, 3, 0, 1)>(keyed), prime);
  _mm512_store_si512(accumulators, _mm512_add_epi64(low, slli_epi64<32>(high)));
}

__attribute__((always_inline, target("avx512f"))) inline void combine_avx512(std::uint64_t* accumulators,
                                                                             std::uint64_t* odd) {
  _mm512_store_si512(accumulators, _mm512_add_epi64(_mm512_load_si512(accumulators), _mm512_load_si512(odd)));
  _mm512_store_si512(odd, _mm512_setzero_si512());
}

__attribute__((target("avx512f"))) std::uint64_t long_avx512(const unsigned char* input, std::size_t size) {
  return long_hash(input, size, stripe_avx512, combine_avx512, scramble_avx512);
}

#endif  // MX_XXH3_X86

typedef std::uint64_t (*LongHash)(const unsigned char* input, std::size_t size);

LongHash long_hash_by(xxh3_detail::Path path) {
  switch (path) {
#ifdef MX_XXH3_X86
    case xxh3_detail::Path::SSE2:
      return long_sse2;
    case xxh3_detail::Path::AVX2:
      return long_avx2;
    case xxh3_detail::Path::AVX512:
      return long_avx512;
#endif
    default:
      return long_portable;
  }
}

// The chosen path's long hash, asked the first time a long body comes and
// kept in an atomic that needs no initializer run, so that no other file's
// static initializer finds it unset. Not a function's static: its guard,
// held while it is set, would stay held for good in a child a fork made
// meanwhile, whose first long body would then wait forever. Two threads
// that ask at once store the same answer.
std::atomic<LongHash> chosen_long_hash{nullptr};

LongHash implementation() {
  LongHash chosen = chosen_long_hash.load(std::memory_order_relaxed);
  if (!chosen) {
    chosen = long_hash_by(xxh3_detail::chosen());
    chosen_long_hash.store(chosen, std::memory_order_relaxed);
  }
  return chosen;
}

}  // namespace

std::uint64_t xxh3_64(const void* data, std::size_t size) {
  const unsigned char* const input = static_cast<const unsigned char*>(data);
  if (size <= 16) {
    return up_to_16(input, size);
  }
  if (size <= 128) {
    return up_to_128(input, size);
  }
  if (size <= 240) {
    return up_to_240(input, size);
  }
  return implementation()(input, size);
}

namespace xxh3_detail {

bool runs(Path path) {
  switch (path) {
    case Path::PORTABLE:
      return true;
#ifdef MX_XXH3_X86
    case Path::SSE2:
      return true;
    case Path::AVX2:
      return __builtin_cpu_supports("avx2");
    case Path::AVX512:
      return __builtin_cpu_supports("avx512f");
#endif
    default:
      return false;
  }
}

Path chosen() {
  if (runs(Path::AVX512)) {
    return Path::AVX512;
  }
  if (runs(Path::AVX2)) {
    return Path::AVX2;
  }
  return runs(Path::SSE2) ? Path::SSE2 : Path::PORTABLE;
}

std::uint64_t by(Path path, const void* data, std::size_t size) {
  if (size <= 240) {
    return xxh3_64(data, size);
  }
  return long_hash_by(path)(static_cast<const unsigned char*>(data), size);
}

}  // namespace xxh3_detail

}  // namespace mx
