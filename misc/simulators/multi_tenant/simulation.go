/* Copyright (c) 2017 University of Utah
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

package main

import (
    "fmt"
    "math/rand"
    "sync"
)

type Core struct {
    processing_ns  int
    ctxt_switch_ns int
    last_tenant int
}

// Returns total processing delay.
func (c *Core) process(tenant int) int {
    d := c.processing_ns

    // Add a context switch if the last tenant to execute on this core was
    // different from this tenant.
    if c.last_tenant != tenant {
        d += c.ctxt_switch_ns
        c.last_tenant = tenant
    }

    return d
}

// Returns TPS
func simulate(
    processing_ns int,
    ctxt_switch_ns int,
    tenants int,
    rps int,
    use_zipf bool) int {

    c := Core{processing_ns, ctxt_switch_ns, -1}

    requests := rps * 20
    total_ns := 0

    source := rand.NewSource(0)
    rnd := rand.New(source)
    zipf := rand.NewZipf(rnd, 1.01, 1.0, uint64(tenants-1))

    for r := 0; r < requests; r++ {
        // Pick the tenant sending the request.
        tenant := 0
        if use_zipf {
            tenant = int(zipf.Uint64())
        } else {
            tenant = rnd.Int() % tenants
        }

        // TODO: Pick a core for the request. How??

        total_ns += c.process(tenant)
    }

    return int(float64(requests) / (float64(total_ns) / 1e9))
}

func main() {
    // The execution time of an extension inside the key-value service.
    processing_ns := []int{ 2000, 16000, 64000 }

    // The overhead of switching between extensions/procedures.
    ctxt_switch_ns := []int { 0,    /* No context switch overhead. What we can
                                       theoretically do with language level
                                       isolation. */
                              1400, /* Process context switch overhead as
                                       measured on CloudLab machines. */
                            }

    // Number of tenants multiplexed on the key-value service. This is the
    // primary induction variable in the simulation.
    tenants := []int{ 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 }

    // Rate at which requests are issued to the key-value service.
    rps := 1000000

    // Header for the output file.
    fmt.Println("# Dist ProcessingNS ContextSwitchNS Tenants TPS")

    // Set to true to choose tenants from a zipfian distribution.
    // If false, the simulation first runs for a uniform distribution followed
    // by a zipfian distribution.
    use_zipf := true
    var wg sync.WaitGroup
    for {
        for _, p_ns := range processing_ns { // Vary the extension processing time.
            for _, cs_ns := range ctxt_switch_ns { // Vary the context switch time.
                for _, t := range tenants { // Vary the number of tenants.
                    wg.Add(1)
                    go func(p_ns int,
                            cs_ns int,
                            t int,
                            rps int,
                            use_zipf bool) {
                        // Run the simulation.
                        tps := simulate(p_ns, cs_ns, t, rps, use_zipf)

                        // Generate output.
                        dist := "Uniform"
                        if use_zipf {
                            dist = "Zipf"
                        }
                        fmt.Printf("%v %v %v %v %v\n", dist, p_ns, cs_ns, t, tps)
                        wg.Done()
                    }(p_ns, cs_ns, t, rps, use_zipf)
                }
            }
        }

        // Check if a zipfian tenant distribution has to be simulated next.
        if use_zipf {
            break
        } else {
            use_zipf = true
        }
    }
    wg.Wait()
}
