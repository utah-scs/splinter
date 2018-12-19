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

use super::cycles;

pub struct CycleCounter {
    total: u64,
    start_time: u64,
    event_count: u64,
    measurement_count: u64,
}

impl CycleCounter {
    // Creates and returns the CycleCounter Object. This object can be used to measure the
    // number of cycles for measurement_count of events.
    //
    // # Argument
    //
    // The maximum number of events to average out the time spend in that particular stage.
    //
    // # Return
    //
    // New instance of CycleCounter.
    pub fn new(m_count: u64) -> CycleCounter {
        CycleCounter {
            total: 0,
            start_time: 0,
            event_count: 0,
            measurement_count: m_count,
        }
    }

    // Starts the CPU cycle counting and store it in self.start_time.
    #[inline]
    pub fn start(&mut self) {
        self.start_time = cycles::rdtsc();
    }

    // Stops the cycle counting and reset the counter when self.measurement_count number of events
    // are completed.
    //
    // #Return
    //
    // The number of CPU cycles spent for the current event.
    #[inline]
    pub fn stop(&mut self, events:u64) -> u64 {
        let elapsed = cycles::rdtsc() - self.start_time;
        self.total += elapsed;
        self.event_count += events;

        if self.event_count >= self.measurement_count {
            info!("{}", self.total/self.event_count);
            self.event_count = 0;
            self.total = 0;
        }
        elapsed
    }

    /// Count the total number of cycles for a function or a code block.
    /// The caller pass the CPU cycles and this function sum those cycles.
    /// And averages for 1M such invocations.
    #[inline]
    pub fn total_cycles(&mut self, cycles:u64) {
        self.total += cycles;
        self.event_count += 1;
        if self.event_count >= self.measurement_count {
            info!("{}", self.total/self.event_count);
            self.event_count = 0;
            self.total = 0;
        }
    }
}
