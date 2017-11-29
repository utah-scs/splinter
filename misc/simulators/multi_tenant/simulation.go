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
    "os"
)

/*
 * This type represents a core in the simulator.
 */
type Core struct {
    // The amount of time in nanoseconds required to process a request.
    processing_ns  int

    // The amount of time in nanoseconds required to context switch between two
    // different tenants.
    ctxt_switch_ns int

    // The total time in nanoseconds spent executing requests (including context
    // switches).
    total_ns int

    // The last tenant to have executed on this core.
    last_tenant int
}

/*
 * Initialize a core for the simulation.
 *
 * \param processing_ns
 *      The amount of time in nanoseconds to spend processing a request.
 *
 * \param ctxt_switch_ns
 *      The overhead in nanoseconds of switching between two tenants.
 */
func (c *Core) initialize(processing_ns int, ctxt_switch_ns int) {
    c.processing_ns = processing_ns
    c.ctxt_switch_ns = ctxt_switch_ns
    c.total_ns = 0

    // The very first request will induce a context switch on the core.
    c.last_tenant = -1
}

/*
 * This function processes a tenant request and returns the amount of time
 * in nanoseconds spent in doing so including context switch overhead.
 *
 * If the request is from a different tenant than what last executed on the
 * core, one context switch is added to the processing time.
 *
 * \param tenant
 *     The tenant sending the request. Required to determine context switching
 *     overhead.
 *
 * \return
 *     The amount of time in nanoseconds spent processing the request including
 *     context switch overhead.
 */
func (c *Core) process(tenant int) int {
    d := c.processing_ns

    // Add a context switch if the last tenant to execute on this core was
    // different from this tenant.
    if c.last_tenant != tenant {
        d += c.ctxt_switch_ns
        c.last_tenant = tenant
    }

    // Update the total time spent processing requests.
    c.total_ns += d

    return d
}

/*
 * Simulate a multi-tenant key-value service, and return it's throughput.
 *
 * \param processing_ns
 *      The amount of time the key-value service spends processing a single
 *      request in nanoseconds.
 * \param ctxt_switch_ns
 *      The overhead of switching from a request from one tenant to a request
 *      from a different tenant in nanoseconds.
 * \param tenants
 *      The number of tenants using the key-value service.
 * \param use_zipf
 *      If true, choose tenants from a zipfian distribution. If false, choose
 *      tenants from a uniform distribution.
 * \param num_cores
 *      The number of cores the key-value service can process requests on.
 *
 * \return
 *      The throughput of the key-value service in requests per second.
 */
func simulate(
    processing_ns int,
    ctxt_switch_ns int,
    tenants int,
    use_zipf bool,
    num_cores int) int {

    cores := make([]Core, num_cores)

    for i := 0; i < num_cores; i++ {
        cores[i].initialize(processing_ns, ctxt_switch_ns)
    }

    // Simulate the key-value service for 20 million requests.
    requests := 20 * 1000000

    // The total amount of time the system takes to service all
    // requests.
    total_ns := 0

    // Setup tenant id generator.
    source := rand.NewSource(0)
    rnd := rand.New(source)
    zipf := rand.NewZipf(rnd, 1.01, 1.0, uint64(tenants-1))

    // Service requests FIFO in groups of size num_cores. Requests inside a
    // group are effectively serviced in parallel. This is not batching in the
    // conventional sense because the network and dispatch are not simulated.
    for r := 0; r < requests; r += num_cores {
        // Initially, all cores are free to service requests.
        freeCores := make([]int, num_cores)

        // Assign each request to a core, and process it.
        for request := 0; request < num_cores; request++ {
            // Pick the tenant sending the request.
            tenant := 0
            if use_zipf {
                tenant = int(zipf.Uint64())
            } else {
                tenant = rnd.Int() % tenants
            }

            // Assign this request to a free core.
            core := -1
            for i := 0; i < num_cores; i++ {
                // If there is a free core that last executed a request from
                // the same tenant, then chose it.
                if freeCores[i] == 0 && cores[i].last_tenant == tenant {
                    core = i
                    break
                }

                if freeCores[i] == 0 && core == -1 {
                    core = i
                }
            }
            // The assigned core is now busy.
            freeCores[core] = 1

            // Process the request.
            cores[core].process(tenant)
        }
    }

    // Find the total time taken to process all requests.
    for i := 0; i < num_cores; i++ {
        if total_ns < cores[i].total_ns {
            total_ns = cores[i].total_ns
        }
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

    // The number of cores available to the simulated key-value service.
    num_cores := 8

    // Number of tenants multiplexed on the key-value service. This is the
    // primary induction variable in the simulation.
    tenants := []int{ 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 }

    // Create the file into which samples will be written.
    output_file, _ := os.Create("./samples.data")
    defer output_file.Close()

    // Header for the output file.
    output_file.WriteString("# Dist ProcessingNS ContextSwitchNS Tenants TPS\n")

    // Set to true to choose tenants from a zipfian distribution.
    // If false, the simulation uses a uniform distribution.
    use_zipf := true

    // Barrier to synchronize simulations.
    var wg sync.WaitGroup

    // Run simulations on the cross product of processing times, context switch
    // times and number of tenants. Each simulation is run inside it's own go
    // thread.
    for _, p_ns := range processing_ns {
        for _, cs_ns := range ctxt_switch_ns {
            for _, t := range tenants {
                wg.Add(1)

                go func(p_ns int,
                        cs_ns int,
                        t int,
                        use_zipf bool,
                        num_cores int) {
                    // Run the simulation.
                    tps := simulate(p_ns, cs_ns, t, use_zipf, num_cores)

                    // Generate output.
                    dist := "Uniform"
                    if use_zipf {
                        dist = "Zipf"
                    }
                    output_file.WriteString(fmt.Sprintf("%v %v %v %v %v\n",
                                            dist, p_ns, cs_ns, t, tps))
                    wg.Done()
                }(p_ns, cs_ns, t, use_zipf, num_cores)
            }
        }
    }

    // Wait for all simulations to complete.
    wg.Wait()
}
