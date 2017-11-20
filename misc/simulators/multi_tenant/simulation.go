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
	p := d/1000 //ps in micro seconds
	cont := 2*c.ctxt_switch_ns
	p = p*cont
	if c.last_tenant != tenant {
		d += p
		c.last_tenant = tenant
	}
	return d
}

// Returns TPS
func simulate(processing_ns int,
	ctxt_switch_ns int,
	tenants int,
	rps int,
	use_zipf bool) int {

	c := Core{processing_ns, ctxt_switch_ns, -1}

	requests := 1000000
	total_ns := 0

	source := rand.NewSource(0)
	rnd := rand.New(source)
	zipf := rand.NewZipf(rnd, 1.01, 1.0, uint64(tenants-1))

	for r := 0; r < requests; r++ {
		tenant := 0
		if use_zipf {
			tenant = int(zipf.Uint64())
		} else {
			tenant = rnd.Int() % tenants
		}
		total_ns += c.process(tenant)
	}

	return int(float64(requests) / (float64(total_ns) / 1e9))
}

func main() {
	processing_ns := []int{2000,4000,8000,16000,32000,64000}
	ctxt_switch_ns := []int{0, 1000, 2000, 4000, 8000, 16000, 32000}
	tenants := []int{64, 128, 256, 512, 1024}
	rps := 1000000

	fmt.Println("Dist ProcessingNS ContextSwitchNS Tenants TPS")
	use_zipf := false
	var wg sync.WaitGroup
	for {
		for _, p_ns := range processing_ns {
			for _, cs_ns := range ctxt_switch_ns {
				for _, t := range tenants {
					wg.Add(1)
					go func(p_ns int, cs_ns int, t int, rps int, use_zipf bool) {
						tps := simulate(p_ns, cs_ns, t, rps, use_zipf)
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
		if use_zipf {
			break
		}
		use_zipf = true
	}
	wg.Wait()
}
