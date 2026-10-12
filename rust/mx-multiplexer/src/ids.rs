//! Message and instance ids: 64-bit numbers that only need to be distinct,
//! not unpredictable (docs/faq.md on the threat model). Mirrors
//! lib/random.cc's seeding, eight bytes of /dev/urandom mixed with the pid
//! and the time, once per generator, never a system call per id.
//!
//! The generator is SplitMix64 over an atomic counter, where C++ takes
//! mt19937_64 under a mutex: every call adds a constant to the counter and
//! mixes the result, a bijection, so one generator gives no id twice in
//! 2^64 calls, from any number of threads at once without a lock. Zero, which
//! every receiver drops as a message without an id, is never given.

use std::fs::File;
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// The counter's step, 2^64 over the golden ratio, as SplitMix64 has it.
const GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// A generator of ids, seeded once; see the module comment.
#[derive(Debug)]
pub(crate) struct Ids {
    counter: AtomicU64,
}

impl Ids {
    /// A generator seeded from /dev/urandom, the pid and the time.
    pub(crate) fn new() -> Ids {
        let mut seed = [0u8; 8];
        // A read that comes up short leaves zeros, which the pid and the time
        // still vary, as lib/random.cc does.
        if let Ok(mut urandom) = File::open("/dev/urandom") {
            let _ = urandom.read(&mut seed);
        }
        let mut seed = u64::from_le_bytes(seed);
        seed ^= u64::from(std::process::id()) << 32;
        seed ^= SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |since| since.as_nanos() as u64);
        Ids { counter: AtomicU64::new(seed) }
    }

    /// A generator whose ids follow from `seed`: for tests that need the
    /// same ids every run.
    #[cfg(test)]
    pub(crate) fn seeded(seed: u64) -> Ids {
        Ids { counter: AtomicU64::new(seed) }
    }

    /// The next id, never 0; from any thread.
    pub(crate) fn next(&self) -> u64 {
        loop {
            let mut mixed = self.counter.fetch_add(GOLDEN_GAMMA, Ordering::Relaxed).wrapping_add(GOLDEN_GAMMA);
            mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            mixed ^= mixed >> 31;
            if mixed != 0 {
                return mixed;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! The generator against SplitMix64's published first outputs, and its
    //! ids distinct across threads.

    use super::Ids;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn gives_splitmix64s_sequence() {
        // The first outputs of SplitMix64 seeded with 1234567, from its
        // reference implementation.
        let ids = Ids::seeded(1_234_567);
        assert_eq!(6_457_827_717_110_365_317, ids.next());
        assert_eq!(3_203_168_211_198_807_973, ids.next());
        assert_eq!(9_817_491_932_198_370_423, ids.next());
    }

    #[test]
    fn gives_distinct_ids_from_many_threads_at_once() {
        let ids = Arc::new(Ids::new());
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let ids = Arc::clone(&ids);
                std::thread::spawn(move || (0..10_000).map(|_| ids.next()).collect::<Vec<u64>>())
            })
            .collect();
        let mut seen = HashSet::new();
        for thread in threads {
            for id in thread.join().unwrap() {
                assert!(id != 0 && seen.insert(id), "id {id} given twice");
            }
        }
        assert_eq!(40_000, seen.len());
    }

    #[test]
    fn two_generators_start_apart() {
        assert_ne!(Ids::new().next(), Ids::new().next());
    }
}
