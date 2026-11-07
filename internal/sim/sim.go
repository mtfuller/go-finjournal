// Package sim implements the deterministic simulator and fault-injection test
// harness for go-finjournal (RFC Section 12).
package sim

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/mtfuller/go-finjournal/modules/fees/rules"
	"github.com/mtfuller/go-finjournal/modules/rates/static"
	"github.com/mtfuller/go-finjournal/pkg/commit"
	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/module"
	"github.com/mtfuller/go-finjournal/pkg/storage/memory"
)

// Config controls a deterministic simulation run.
type Config struct {
	Seed        int64
	Steps       int
	BatchedMode bool
}

type simFaultyEnricher struct {
	panicNext   bool
	timeoutNext bool
}

func (e *simFaultyEnricher) Name() string                      { return "sim/faulty" }
func (e *simFaultyEnricher) Init(context.Context, module.Host) error { return nil }
func (e *simFaultyEnricher) Close(context.Context) error       { return nil }
func (e *simFaultyEnricher) Matches(string) bool               { return true }
func (e *simFaultyEnricher) Enrich(ctx context.Context, _ *module.Draft) error {
	if e.panicNext {
		e.panicNext = false
		panic("simulated enricher panic")
	}
	if e.timeoutNext {
		e.timeoutNext = false
		return context.DeadlineExceeded
	}
	return nil
}

// RunSeed executes a deterministic simulation for a given seed and number of steps,
// asserting invariants I1–I8 after every step.
func RunSeed(cfg Config) error {
	if cfg.Steps <= 0 {
		cfg.Steps = 40
	}
	rng := rand.New(rand.NewSource(cfg.Seed))
	ctx := context.Background()

	store := memory.New()
	clock := ledger.NewManualClock(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	idGen := ledger.NewDeterministicIDGenerator(uint64(cfg.Seed))
	faultMod := &simFaultyEnricher{}

	feeMod, err := rules.New(rules.Config{
		Schedules: []rules.Schedule{{
			ID:            "card_capture_usd",
			Version:       1,
			EffectiveFrom: "2026-01-01",
			Match:         rules.MatchCriteria{Kind: "card_capture", Asset: "USD/2"},
			Payer:         "credit_of_posting_0",
			Payee:         "revenue:fees:card:usd",
			Percent:       "2.9",
			Fixed:         "0.30",
			Cap:           "25.00",
		}},
	})
	if err != nil {
		return fmt.Errorf("seed %d: rules.New: %w", cfg.Seed, err)
	}
	fxMod, err := static.New(static.Config{
		QuoteTTL: 60 * time.Second,
		Pairs: []static.PairConfig{{
			Base:          "USD/2",
			Quote:         "EUR/2",
			Mid:           "0.9213",
			SpreadPercent: "0.5",
			Source:        "static",
		}},
	})
	if err != nil {
		return fmt.Errorf("seed %d: static.New: %w", cfg.Seed, err)
	}

	var bc *commit.BatchedCommitter
	opts := []ledger.Option{
		ledger.WithClock(clock),
		ledger.WithIDGenerator(idGen),
		ledger.WithModules(feeMod, fxMod, faultMod),
	}
	if cfg.BatchedMode {
		bc, err = commit.NewBatched(store, commit.BatchConfig{
			MaxBatch:    16,
			LockTimeout: 50 * time.Millisecond,
			StepMode:    true,
		})
		if err != nil {
			return fmt.Errorf("seed %d: commit.NewBatched: %w", cfg.Seed, err)
		}
		opts = append(opts, ledger.WithCommitter(bc))
	}

	eng, err := ledger.New(store, opts...)
	if err != nil {
		return fmt.Errorf("seed %d: ledger.New: %w", cfg.Seed, err)
	}
	defer func() { _ = eng.Close(ctx) }()

	runStepOp := func(fn func() (*ledger.Transaction, error)) (*ledger.Transaction, error) {
		if !cfg.BatchedMode {
			return fn()
		}
		var (
			tx  *ledger.Transaction
			err error
			wg  sync.WaitGroup
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err = fn()
		}()
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			if bc.FlushBatch() > 0 {
				break
			}
			// Check if goroutine already finished (e.g. pre-commit validation/enricher error)
			time.Sleep(50 * time.Microsecond)
		}
		wg.Wait()
		return tx, err
	}

	postOne := func(intent ledger.Intent) (*ledger.Transaction, error) {
		return runStepOp(func() (*ledger.Transaction, error) {
			return eng.Post(ctx, intent)
		})
	}

	if _, err := eng.CreateLedger(ctx, "sim", "Simulation Ledger", nil); err != nil {
		return fmt.Errorf("seed %d: CreateLedger: %w", cfg.Seed, err)
	}
	if _, err := eng.RegisterAsset(ctx, "sim", "USD", 2); err != nil {
		return fmt.Errorf("seed %d: RegisterAsset USD: %w", cfg.Seed, err)
	}
	if _, err := eng.RegisterAsset(ctx, "sim", "EUR", 2); err != nil {
		return fmt.Errorf("seed %d: RegisterAsset EUR: %w", cfg.Seed, err)
	}

	// Create 1 unlimited funding account, 4 customer Locked USD accounts (Min = 0),
	// 4 customer Locked EUR accounts (Min = 0), and 1 Deferred fee account
	zero := money.Zero
	bank, err := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "sim",
		Code:   "assets:bank:usd",
		Asset:  "USD/2",
		Normal: ledger.Debit,
	})
	if err != nil {
		return err
	}
	fxPosUSD, err := eng.CreateAccount(ctx, ledger.Account{
		Ledger:      "sim",
		Code:        "system:fx:position:usd",
		Asset:       "USD/2",
		Normal:      ledger.Credit,
		BalanceMode: ledger.Deferred,
	})
	if err != nil {
		return err
	}
	var wallets []ledger.AccountID
	var eurWallets []ledger.AccountID
	for i := 0; i < 4; i++ {
		w, err := eng.CreateAccount(ctx, ledger.Account{
			Ledger: "sim",
			Code:   fmt.Sprintf("liabilities:wallets:u%d:usd", i),
			Asset:  "USD/2",
			Normal: ledger.Credit,
			Limits: ledger.Limits{Min: &zero},
		})
		if err != nil {
			return err
		}
		wallets = append(wallets, w.ID)

		ew, err := eng.CreateAccount(ctx, ledger.Account{
			Ledger: "sim",
			Code:   fmt.Sprintf("liabilities:wallets:u%d:eur", i),
			Asset:  "EUR/2",
			Normal: ledger.Credit,
			Limits: ledger.Limits{Min: &zero},
		})
		if err != nil {
			return err
		}
		eurWallets = append(eurWallets, ew.ID)

		// Initial funding of 100.00 USD each
		_, err = postOne(ledger.Intent{
			Ledger:         "sim",
			Kind:           "seed_fund",
			IdempotencyKey: fmt.Sprintf("init-fund-%d", i),
			Postings: []ledger.Posting{{
				Debit:  bank.ID,
				Credit: w.ID,
				Amount: money.FromInt64(10_000),
				Asset:  "USD/2",
			}},
		})
		if err != nil {
			return fmt.Errorf("seed %d: initial fund %d: %w", cfg.Seed, i, err)
		}
	}
	feeAcct, err := eng.CreateAccount(ctx, ledger.Account{
		Ledger:      "sim",
		Code:        "revenue:fees:usd",
		Asset:       "USD/2",
		Normal:      ledger.Credit,
		BalanceMode: ledger.Deferred,
	})
	if err != nil {
		return err
	}

	var postedTxs []*ledger.Transaction
	var pendingHolds []*ledger.Transaction

	for step := 0; step < cfg.Steps; step++ {
		clock.Advance(time.Duration(rng.Intn(5)+1) * time.Second)
		op := rng.Intn(100)

		switch {
		case op < 28:
			// Transfer between two wallets (plus optional fee leg to Deferred account)
			from := wallets[rng.Intn(len(wallets))]
			to := wallets[rng.Intn(len(wallets))]
			if from == to {
				to = wallets[(rng.Intn(len(wallets)-1)+1)%len(wallets)]
			}
			amt := money.FromInt64(int64(rng.Intn(4000) + 1))
			postings := []ledger.Posting{{
				Debit:  from,
				Credit: to,
				Amount: amt,
				Asset:  "USD/2",
			}}
			if rng.Intn(2) == 0 {
				postings = append(postings, ledger.Posting{
					Debit:  from,
					Credit: feeAcct.ID,
					Amount: money.FromInt64(int64(rng.Intn(50) + 1)),
					Asset:  "USD/2",
				})
			}
			idemKey := fmt.Sprintf("step-%d-xfer", step)
			intent := ledger.Intent{
				Ledger:         "sim",
				Kind:           "transfer",
				IdempotencyKey: idemKey,
				Postings:       postings,
			}

			// Fault injection on some transfers
			faultRoll := rng.Intn(20)
			switch faultRoll {
			case 0:
				// Commit fails -> rolls back cleanly
				store.FailNext("Commit", errors.New("simulated commit failure"))
			case 1:
				// Crash after commit -> client retries with the same idempotency key!
				store.CrashAfterCommit()
			case 2:
				// Module panic -> recovered as 503 module_unavailable
				faultMod.panicNext = true
			case 3:
				// Module timeout -> 503 module_unavailable
				faultMod.timeoutNext = true
			}

			tx, err := postOne(intent)
			if errors.Is(err, memory.ErrSimulatedCrash) {
				// Client retries with the exact same idempotency key after crash-after-commit!
				tx, err = postOne(intent)
				if err != nil {
					return fmt.Errorf("seed %d step %d: retry after CrashAfterCommit failed: %w", cfg.Seed, step, err)
				}
				if !tx.IdempotentReplay {
					return fmt.Errorf("seed %d step %d: expected IdempotentReplay=true after CrashAfterCommit", cfg.Seed, step)
				}
			}
			if err == nil && tx != nil {
				postedTxs = append(postedTxs, tx)
			}

		case op < 45:
			// Phase 2: Card capture with fees/rules module enrichment or FX conversion with rates/static module
			if rng.Intn(2) == 0 {
				to := wallets[rng.Intn(len(wallets))]
				amt := money.FromInt64(int64(rng.Intn(3000) + 500))
				intent := ledger.Intent{
					Ledger:         "sim",
					Kind:           "card_capture",
					IdempotencyKey: fmt.Sprintf("step-%d-capture", step),
					Postings: []ledger.Posting{{
						Debit:  bank.ID,
						Credit: to,
						Amount: amt,
						Asset:  "USD/2",
					}},
				}
				tx, err := postOne(intent)
				if err == nil && tx != nil {
					postedTxs = append(postedTxs, tx)
				}
			} else {
				uIdx := rng.Intn(len(wallets))
				amt := money.FromInt64(int64(rng.Intn(1500) + 200))
				intent := ledger.Intent{
					Ledger:         "sim",
					Kind:           "fx_convert",
					IdempotencyKey: fmt.Sprintf("step-%d-fx", step),
					Params:         ledger.Metadata{"fx.to_account": string(eurWallets[uIdx])},
					Postings: []ledger.Posting{{
						Debit:  wallets[uIdx],
						Credit: fxPosUSD.ID,
						Amount: amt,
						Asset:  "USD/2",
					}},
				}
				tx, err := postOne(intent)
				if err == nil && tx != nil {
					postedTxs = append(postedTxs, tx)
				}
			}

		case op < 60 && cfg.BatchedMode:
			// Phase 3: Multi-intent batch window in BatchedMode (including lock-timeout split retry)
			batchSize := 3
			if rng.Intn(4) == 0 {
				store.FailNext("LockBalances", ledger.NewError(ledger.CodeLockTimeout, "simulated batch lock timeout", nil))
			}
			var wg sync.WaitGroup
			txs := make([]*ledger.Transaction, batchSize)
			for bIdx := 0; bIdx < batchSize; bIdx++ {
				from := wallets[(step+bIdx)%len(wallets)]
				to := wallets[(step+bIdx+1)%len(wallets)]
				amt := money.FromInt64(int64(rng.Intn(2500) + 50))
				wg.Add(1)
				go func(idx int, f, t ledger.AccountID, a money.Amount) {
					defer wg.Done()
					txs[idx], _ = eng.Post(ctx, ledger.Intent{
						Ledger:         "sim",
						Kind:           "transfer",
						IdempotencyKey: fmt.Sprintf("step-%d-batch-%d", step, idx),
						Postings: []ledger.Posting{{
							Debit:  f,
							Credit: t,
							Amount: a,
							Asset:  "USD/2",
						}},
					})
				}(bIdx, from, to, amt)
			}
			deadline := time.Now().Add(500 * time.Millisecond)
			flushedTotal := 0
			for time.Now().Before(deadline) && flushedTotal < batchSize {
				flushedTotal += bc.FlushBatch()
				time.Sleep(100 * time.Microsecond)
			}
			wg.Wait()
			for _, tx := range txs {
				if tx != nil {
					postedTxs = append(postedTxs, tx)
				}
			}

		case op < 72:
			// Place a Pending Hold with expiry
			from := wallets[rng.Intn(len(wallets))]
			to := wallets[rng.Intn(len(wallets))]
			if from == to {
				continue
			}
			exp := clock.Now().Add(30 * time.Second)
			amt := money.FromInt64(int64(rng.Intn(2000) + 10))
			tx, err := postOne(ledger.Intent{
				Ledger:         "sim",
				Kind:           "hold",
				IdempotencyKey: fmt.Sprintf("step-%d-hold", step),
				Pending:        true,
				ExpiresAt:      &exp,
				Postings: []ledger.Posting{{
					Debit:  from,
					Credit: to,
					Amount: amt,
					Asset:  "USD/2",
				}},
			})
			if err == nil && tx != nil {
				pendingHolds = append(pendingHolds, tx)
			}

		case op < 84 && len(pendingHolds) > 0:
			// Settle or Void a pending hold
			hold := pendingHolds[0]
			pendingHolds = pendingHolds[1:]
			if rng.Intn(2) == 0 {
				// Partial or full settle
				origAmt, _ := hold.Postings[0].Amount.Int64()
				partAmt := money.FromInt64(origAmt/2 + 1)
				if partAmt.Cmp(hold.Postings[0].Amount) > 0 {
					partAmt = hold.Postings[0].Amount
				}
				tx, err := runStepOp(func() (*ledger.Transaction, error) {
					return eng.SettleHold(ctx, hold.ID, fmt.Sprintf("step-%d-settle", step), &partAmt)
				})
				if err == nil && tx != nil {
					postedTxs = append(postedTxs, tx)
				}
			} else {
				_, _ = runStepOp(func() (*ledger.Transaction, error) {
					return eng.VoidHold(ctx, hold.ID, fmt.Sprintf("step-%d-void", step))
				})
			}

		case op < 94 && len(postedTxs) > 0:
			// Reverse a previously posted transaction
			target := postedTxs[rng.Intn(len(postedTxs))]
			_, _ = runStepOp(func() (*ledger.Transaction, error) {
				return eng.Reverse(ctx, target.ID, fmt.Sprintf("step-%d-rev", step), nil)
			})

		default:
			// Clock jump past hold expiry and sweep expired holds
			clock.Advance(45 * time.Second)
			if !cfg.BatchedMode {
				_, _ = eng.SweepExpiredHolds(ctx)
			}
		}

		// Seal journal and check all 8 invariants (I1-I8) at every step
		if _, _, err := eng.SealOnce(ctx, "sim"); err != nil {
			return fmt.Errorf("seed %d step %d: SealOnce failed: %w", cfg.Seed, step, err)
		}
		rep, err := ledger.CheckInvariants(ctx, store, "sim")
		if err != nil {
			return fmt.Errorf("seed %d step %d: CheckInvariants error: %w", cfg.Seed, step, err)
		}
		if !rep.OK() {
			return fmt.Errorf("seed %d step %d: invariant violations: %+v", cfg.Seed, step, rep.Violations)
		}
	}
	return nil
}
