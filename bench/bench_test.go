package bench_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mtfuller/go-finjournal/bench"
)

func TestBenchmarkWorkloadsW1ToW6(t *testing.T) {
	ctx := context.Background()
	workloads := []string{"W1", "W2", "W3", "W4", "W5", "W6"}
	modes := []string{"direct", "deferred", "batched"}

	for _, w := range workloads {
		for _, m := range modes {
			s, err := bench.Run(ctx, bench.Options{
				Workload: w,
				Mode:     m,
				Accounts: 30,
				Workers:  2,
				Duration: 15 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("bench.Run(%s, %s): %v", w, m, err)
			}
			if s.CommittedOps == 0 {
				t.Fatalf("expected CommittedOps > 0 for (%s, %s), got %+v", w, m, s)
			}
			if !strings.Contains(s.String(), "workload="+w) {
				t.Fatalf("unexpected Summary.String(): %s", s.String())
			}
		}
	}

	// Also verify SQLite mode
	s, err := bench.Run(ctx, bench.Options{
		Workload: "W1",
		Mode:     "sqlite",
		Accounts: 20,
		Workers:  2,
		Duration: 20 * time.Millisecond,
	})
	if err != nil || s.CommittedOps == 0 {
		t.Fatalf("bench.Run(W1, sqlite): %+v (%v)", s, err)
	}
}
