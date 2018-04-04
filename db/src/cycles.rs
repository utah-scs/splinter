/* Copyright (c) 2018 University of Utah
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

use time::PreciseTime;
use std::sync::{Once, ONCE_INIT};

static mut CYCLES_PER_SECOND: u64 = 0;
static INIT: Once = ONCE_INIT;

/// Perform once-only overall initialization for the cycles module, such
/// as calibrating the clock frequency.  This method is invoked automatically
/// during initialization.
/// Stolen from the RAMCloud code base. Thanks, John.
fn init() -> u64 {
    // Compute the frequency of the fine-grained CPU timer: to do this,
    // take parallel time readings using both rdtsc and PreciseTime.
    // After 10ms have elapsed, take the ratio between these readings.

    // There is one tricky aspect, which is that we could get interrupted
    // between calling gettimeofday and reading the cycle counter, in which
    // case we won't have corresponding readings.  To handle this (unlikely)
    // case, compute the overall result repeatedly, and wait until we get
    // two successive calculations that are within 0.1% of each other.
    let mut old_cycles = 0.;
    let mut cycles_per_second;
    loop {
        let start_time = PreciseTime::now();
        let start_cycles = rdtsc();
        loop {
            let nanos = start_time.to(PreciseTime::now()).num_nanoseconds().unwrap() as f64;
            if nanos > 10000000. {
                cycles_per_second = (rdtsc() - start_cycles) as f64 * 1000000000.0 / nanos;
                break;
            }
        }
        let delta = cycles_per_second / 1000.0;
        if (old_cycles > (cycles_per_second - delta)) && (old_cycles < (cycles_per_second + delta)) {
            return cycles_per_second as u64;
        }
        old_cycles = cycles_per_second;
    }
}

pub fn cycles_per_second() -> u64 {
    unsafe {
        INIT.call_once(|| {
            CYCLES_PER_SECOND = init();
        });
        CYCLES_PER_SECOND
    }
}

/// Return a 64-bit timestamp using the rdtsc instruction.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
pub fn rdtsc() -> u64 {
    unsafe {
        let lo: u32;
        let hi: u32;
        asm!("rdtsc" : "={eax}"(lo), "={edx}"(hi) : : : "volatile");
        (((hi as u64) << 32) | lo as u64)
    }
}

pub fn to_seconds(cycles: u64) -> f64 {
    cycles as f64 / cycles_per_second() as f64
}

#[cfg(test)]
mod test {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_init() {
        assert!(cycles_per_second() > 1000000000);
        assert!(cycles_per_second() < 5000000000);
    }

    #[test]
    fn test_rdtsc_sanity() {
        let start = rdtsc();
        thread::sleep(Duration::from_secs(1));
        let stop = rdtsc();
        assert!(to_seconds(stop - start) - 1.0 < 0.0001);
    }
}
