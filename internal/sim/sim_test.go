package sim_test

import (
	"flag"
	"testing"

	"github.com/mtfuller/go-finjournal/internal/sim"
)

var seedFlag = flag.Int64("seed", 0, "replay a specific simulation seed")

func TestDeterministicSimulation(t *testing.T) {
	if *seedFlag != 0 {
		if err := sim.RunSeed(sim.Config{Seed: *seedFlag, Steps: 60}); err != nil {
			t.Fatalf("simulation failed (reproduce with: go test ./internal/sim -seed=%d): %v", *seedFlag, err)
		}
		return
	}

	for seed := int64(1); seed <= 50; seed++ {
		if err := sim.RunSeed(sim.Config{Seed: seed, Steps: 40}); err != nil {
			t.Fatalf("simulation failed (reproduce with: go test ./internal/sim -seed=%d): %v", seed, err)
		}
	}

	// Phase 3: also run 25 seeds with BatchedMode=true (StepMode BatchedCommitter with batch splits & faults)
	for seed := int64(1); seed <= 25; seed++ {
		if err := sim.RunSeed(sim.Config{Seed: seed, Steps: 30, BatchedMode: true}); err != nil {
			t.Fatalf("batched simulation failed (seed=%d): %v", seed, err)
		}
	}
}
