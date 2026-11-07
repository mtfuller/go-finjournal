// Package bench implements the open-loop load generator and benchmark matrix
// for Workloads W1–W6 across direct, deferred, batched, and SQLite modes (RFC Section 13).
package bench

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mtfuller/go-finjournal/modules/fees/rules"
	"github.com/mtfuller/go-finjournal/modules/rates/static"
	"github.com/mtfuller/go-finjournal/pkg/commit"
	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/module"
	"github.com/mtfuller/go-finjournal/pkg/storage"
	"github.com/mtfuller/go-finjournal/pkg/storage/memory"
	"github.com/mtfuller/go-finjournal/pkg/storage/sqlite"
)

// Options configures a benchmark run.
type Options struct {
	Workload    string        // "W1", "W2", "W3", "W4", "W5", "W6"
	Mode        string        // "direct", "deferred", "batched", "sqlite"
	Accounts    int           // number of customer accounts (default 1000 for local runs)
	Workers     int           // concurrent workers (default 8)
	Duration    time.Duration // benchmark duration (default 2s)
}

// Summary reports benchmark throughput and latency percentiles.
type Summary struct {
	Workload     string        `json:"workload"`
	Mode         string        `json:"mode"`
	Duration     time.Duration `json:"duration"`
	CommittedOps uint64        `json:"committed_ops"`
	RejectedOps  uint64        `json:"rejected_ops"`
	TPS          float64       `json:"tps"`
	P50          time.Duration `json:"p50"`
	P99          time.Duration `json:"p99"`
	P999         time.Duration `json:"p999"`
}

// Run executes the specified workload and mode and returns its Summary.
func Run(ctx context.Context, opts Options) (*Summary, error) {
	if opts.Workload == "" {
		opts.Workload = "W1"
	}
	if opts.Mode == "" {
		opts.Mode = "direct"
	}
	if opts.Accounts <= 0 {
		opts.Accounts = 1000
	}
	if opts.Workers <= 0 {
		opts.Workers = 8
	}
	if opts.Duration <= 0 {
		opts.Duration = 2 * time.Second
	}

	var store storage.Store
	var err error
	if opts.Mode == "sqlite" {
		store, err = sqlite.Open(ctx, ":memory:")
		if err != nil {
			return nil, err
		}
	} else {
		store = memory.New()
	}
	defer func() { _ = store.Close() }()

	var mods []module.Module
	if opts.Workload == "W3" {
		feeMod, _ := rules.New(rules.Config{
			Schedules: []rules.Schedule{{
				ID:      "xfer_fee",
				Version: 1,
				Match:   rules.MatchCriteria{Kind: "transfer", Asset: "USD/2"},
				Payer:   "debit_of_posting_0",
				Payee:   "revenue:fees:platform:usd",
				Fixed:   "0.05",
			}},
		})
		mods = append(mods, feeMod)
	}
	if opts.Workload == "W4" {
		fxMod, _ := static.New(static.Config{
			Pairs: []static.PairConfig{{
				Base: "USD/2", Quote: "EUR/2", Mid: "0.9213", SpreadPercent: "0.5",
			}},
		})
		mods = append(mods, fxMod)
	}

	var engOpts []ledger.Option
	engOpts = append(engOpts, ledger.WithModules(mods...))
	if opts.Mode == "batched" {
		engOpts = append(engOpts, ledger.WithBatchCommitter(commit.BatchConfig{
			MaxBatch:    100,
			MaxWait:     time.Millisecond,
			LockTimeout: 100 * time.Millisecond,
		}))
	}

	eng, err := ledger.New(store, engOpts...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = eng.Close(ctx) }()

	_, _ = eng.CreateLedger(ctx, "bench", "Benchmark Ledger", nil)
	_, _ = eng.RegisterAsset(ctx, "bench", "USD", 2)
	_, _ = eng.RegisterAsset(ctx, "bench", "EUR", 2)

	feeMode := ledger.Locked
	if opts.Mode == "deferred" || opts.Mode == "batched" {
		feeMode = ledger.Deferred
	}
	_, _ = eng.CreateAccount(ctx, ledger.Account{
		Ledger:      "bench",
		Code:        "revenue:fees:platform:usd",
		Asset:       "USD/2",
		Normal:      ledger.Credit,
		BalanceMode: feeMode,
	})
	_, _ = eng.CreateAccount(ctx, ledger.Account{
		Ledger:      "bench",
		Code:        "system:fx:position:usd",
		Asset:       "USD/2",
		Normal:      ledger.Credit,
		BalanceMode: ledger.Deferred,
	})

	var usdAccounts []ledger.AccountID
	var eurAccounts []ledger.AccountID
	for i := 0; i < opts.Accounts; i++ {
		u, err := eng.CreateAccount(ctx, ledger.Account{
			Ledger: "bench",
			Code:   fmt.Sprintf("liabilities:wallets:u%d:usd", i),
			Asset:  "USD/2",
			Normal: ledger.Credit,
		})
		if err != nil {
			return nil, err
		}
		usdAccounts = append(usdAccounts, u.ID)
		if opts.Workload == "W4" {
			e, err := eng.CreateAccount(ctx, ledger.Account{
				Ledger: "bench",
				Code:   fmt.Sprintf("liabilities:wallets:u%d:eur", i),
				Asset:  "EUR/2",
				Normal: ledger.Credit,
			})
			if err != nil {
				return nil, err
			}
			eurAccounts = append(eurAccounts, e.ID)
		}
	}

	var committed atomic.Uint64
	var rejected atomic.Uint64
	var latMu sync.Mutex
	var latencies []time.Duration

	deadline := time.Now().Add(opts.Duration)
	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < opts.Workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID+1) * 1337))
			zipf := rand.NewZipf(rng, 1.1, 1.0, uint64(len(usdAccounts)-1))
			localLats := make([]time.Duration, 0, 2048)
			seq := 0

			for time.Now().Before(deadline) {
				seq++
				t0 := time.Now()
				var opErr error

				switch opts.Workload {
				case "W1", "W3":
					i := rng.Intn(len(usdAccounts))
					j := (i + 1 + rng.Intn(len(usdAccounts)-1)) % len(usdAccounts)
					_, opErr = eng.Post(ctx, ledger.Intent{
						Ledger:         "bench",
						Kind:           "transfer",
						IdempotencyKey: fmt.Sprintf("w%d-%d", workerID, seq),
						Postings: []ledger.Posting{{
							Debit:  usdAccounts[i],
							Credit: usdAccounts[j],
							Amount: money.FromInt64(100),
							Asset:  "USD/2",
						}},
					})
				case "W2":
					i := int(zipf.Uint64())
					j := int(zipf.Uint64())
					if i == j {
						j = (i + 1) % len(usdAccounts)
					}
					_, opErr = eng.Post(ctx, ledger.Intent{
						Ledger:         "bench",
						Kind:           "transfer",
						IdempotencyKey: fmt.Sprintf("w%d-%d", workerID, seq),
						Postings: []ledger.Posting{{
							Debit:  usdAccounts[i],
							Credit: usdAccounts[j],
							Amount: money.FromInt64(100),
							Asset:  "USD/2",
						}},
					})
				case "W4":
					i := rng.Intn(len(usdAccounts))
					_, opErr = eng.Post(ctx, ledger.Intent{
						Ledger:         "bench",
						Kind:           "fx_convert",
						IdempotencyKey: fmt.Sprintf("w%d-%d", workerID, seq),
						Postings: []ledger.Posting{{
							Debit:  usdAccounts[i],
							Credit: "system:fx:position:usd",
							Amount: money.FromInt64(1000),
							Asset:  "USD/2",
						}},
						Params: ledger.Metadata{"fx.to_account": string(eurAccounts[i])},
					})
				case "W5":
					i := rng.Intn(len(usdAccounts))
					j := (i + 1) % len(usdAccounts)
					holdTx, err := eng.Post(ctx, ledger.Intent{
						Ledger:         "bench",
						Kind:           "hold",
						IdempotencyKey: fmt.Sprintf("w%d-h-%d", workerID, seq),
						Pending:        true,
						Postings: []ledger.Posting{{
							Debit:  usdAccounts[i],
							Credit: usdAccounts[j],
							Amount: money.FromInt64(100),
							Asset:  "USD/2",
						}},
					})
					opErr = err
					if err == nil {
						if rng.Intn(10) < 8 {
							_, opErr = eng.SettleHold(ctx, holdTx.ID, fmt.Sprintf("w%d-s-%d", workerID, seq), nil)
						} else {
							_, opErr = eng.VoidHold(ctx, holdTx.ID, fmt.Sprintf("w%d-v-%d", workerID, seq))
						}
					}
				case "W6":
					if rng.Intn(10) < 9 {
						_, opErr = store.Balance(ctx, usdAccounts[rng.Intn(len(usdAccounts))])
					} else {
						i := rng.Intn(len(usdAccounts))
						j := (i + 1) % len(usdAccounts)
						_, opErr = eng.Post(ctx, ledger.Intent{
							Ledger:         "bench",
							Kind:           "transfer",
							IdempotencyKey: fmt.Sprintf("w%d-%d", workerID, seq),
							Postings: []ledger.Posting{{
								Debit:  usdAccounts[i],
								Credit: usdAccounts[j],
								Amount: money.FromInt64(100),
								Asset:  "USD/2",
							}},
						})
					}
				}

				elapsed := time.Since(t0)
				if opErr == nil {
					committed.Add(1)
					localLats = append(localLats, elapsed)
				} else {
					rejected.Add(1)
				}
			}

			latMu.Lock()
			latencies = append(latencies, localLats...)
			latMu.Unlock()
		}(w)
	}
	wg.Wait()

	totalDur := time.Since(start)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	var p50, p99, p999 time.Duration
	if n := len(latencies); n > 0 {
		p50 = latencies[n*50/100]
		p99 = latencies[n*99/100]
		p999 = latencies[n*999/1000]
	}

	comm := committed.Load()
	return &Summary{
		Workload:     opts.Workload,
		Mode:         opts.Mode,
		Duration:     totalDur,
		CommittedOps: comm,
		RejectedOps:  rejected.Load(),
		TPS:          float64(comm) / totalDur.Seconds(),
		P50:          p50,
		P99:          p99,
		P999:         p999,
	}, nil
}
