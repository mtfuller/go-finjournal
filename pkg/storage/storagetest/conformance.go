// Package storagetest provides the shared conformance test suite that every
// storage.Store adapter (memory, SQLite, PostgreSQL) must pass.
package storagetest

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/commit"
	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/module"
	"github.com/mtfuller/go-finjournal/pkg/storage"
)

// Factory creates a fresh storage.Store for testing.
type Factory func(t *testing.T) storage.Store

// Run executes the full storage conformance test suite against factory.
func Run(t *testing.T, factory Factory) {
	t.Run("BasicLifecycleAndInvariants", func(t *testing.T) {
		ctx := context.Background()
		store := factory(t)
		defer func() { _ = store.Close() }()

		clock := ledger.NewManualClock(time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC))
		eng, err := ledger.New(store, ledger.WithClock(clock))
		if err != nil {
			t.Fatalf("ledger.New: %v", err)
		}
		defer func() { _ = eng.Close(ctx) }()

		if _, err := eng.CreateLedger(ctx, "main", "Main Ledger", ledger.Metadata{"env": "test"}); err != nil {
			t.Fatalf("CreateLedger: %v", err)
		}
		if _, err := eng.RegisterAsset(ctx, "main", "USD", 2); err != nil {
			t.Fatalf("RegisterAsset: %v", err)
		}

		holder, err := eng.CreateHolder(ctx, ledger.AccountHolder{
			Ledger:      "main",
			Kind:        "customer",
			ExternalRef: "cust_alice",
		})
		if err != nil {
			t.Fatalf("CreateHolder: %v", err)
		}

		zero := money.Zero
		psp, err := eng.CreateAccount(ctx, ledger.Account{
			Ledger: "main",
			Code:   "assets:psp:usd",
			Asset:  "USD/2",
			Normal: ledger.Debit,
		})
		if err != nil {
			t.Fatalf("CreateAccount psp: %v", err)
		}
		alice, err := eng.CreateAccount(ctx, ledger.Account{
			Ledger: "main",
			Holder: &holder.ID,
			Code:   "liabilities:wallets:alice:usd",
			Asset:  "USD/2",
			Normal: ledger.Credit,
			Limits: ledger.Limits{Min: &zero},
		})
		if err != nil {
			t.Fatalf("CreateAccount alice: %v", err)
		}
		bob, err := eng.CreateAccount(ctx, ledger.Account{
			Ledger: "main",
			Code:   "liabilities:wallets:bob:usd",
			Asset:  "USD/2",
			Normal: ledger.Credit,
			Limits: ledger.Limits{Min: &zero},
		})
		if err != nil {
			t.Fatalf("CreateAccount bob: %v", err)
		}
		feeAcct, err := eng.CreateAccount(ctx, ledger.Account{
			Ledger:      "main",
			Code:        "revenue:fees:usd",
			Asset:       "USD/2",
			Normal:      ledger.Credit,
			BalanceMode: ledger.Deferred,
		})
		if err != nil {
			t.Fatalf("CreateAccount feeAcct: %v", err)
		}

		// 1. Deposit 100.00 USD (10000) into Alice's wallet
		tx1, err := eng.Post(ctx, ledger.Intent{
			Ledger:         "main",
			Kind:           "deposit",
			IdempotencyKey: "dep-1",
			Postings: []ledger.Posting{{
				Debit:  psp.ID,
				Credit: alice.ID,
				Amount: money.FromInt64(10_000),
				Asset:  "USD/2",
			}},
		})
		if err != nil {
			t.Fatalf("Post dep-1: %v", err)
		}
		if tx1.IdempotentReplay {
			t.Fatalf("expected fresh commit, got replay")
		}

		// Idempotent replay with same key & payload
		replayTx, err := eng.Post(ctx, ledger.Intent{
			Ledger:         "main",
			Kind:           "deposit",
			IdempotencyKey: "dep-1",
			Postings: []ledger.Posting{{
				Debit:  psp.ID,
				Credit: alice.ID,
				Amount: money.FromInt64(10_000),
				Asset:  "USD/2",
			}},
		})
		if err != nil || !replayTx.IdempotentReplay || replayTx.ID != tx1.ID {
			t.Fatalf("expected idempotent replay of %s, got %+v (err=%v)", tx1.ID, replayTx, err)
		}

		tAfterDeposit := clock.Now()
		clock.Advance(10 * time.Second)

		// 2. Transfer 30.00 USD from Alice to Bob + 1.00 USD fee to Deferred feeAcct
		_, err = eng.Post(ctx, ledger.Intent{
			Ledger:         "main",
			Kind:           "transfer",
			IdempotencyKey: "xfer-1",
			Postings: []ledger.Posting{
				{
					Debit:  alice.ID,
					Credit: bob.ID,
					Amount: money.FromInt64(3_000),
					Asset:  "USD/2",
				},
				{
					Debit:  alice.ID,
					Credit: feeAcct.ID,
					Amount: money.FromInt64(100),
					Asset:  "USD/2",
				},
			},
		})
		if err != nil {
			t.Fatalf("Post xfer-1: %v", err)
		}

		// Point-in-time balance check for Alice at tAfterDeposit (should be 10000) vs now (6900)
		balAtDeposit, err := store.BalanceAt(ctx, alice.ID, tAfterDeposit)
		if err != nil || balAtDeposit.Cmp(money.FromInt64(10_000)) != 0 {
			t.Fatalf("expected BalanceAt=%s, got %s (err=%v)", "10000", balAtDeposit, err)
		}
		curBal, err := store.Balance(ctx, alice.ID)
		if err != nil || curBal.Posted(alice.Normal).Cmp(money.FromInt64(6_900)) != 0 {
			t.Fatalf("expected current Alice balance 6900, got %s (err=%v)", curBal.Posted(alice.Normal), err)
		}

		// 3. Place a Hold for 50.00 USD on Alice -> Bob, then settle 40.00 USD
		clock.Advance(10 * time.Second)
		holdTx, err := eng.Post(ctx, ledger.Intent{
			Ledger:         "main",
			Kind:           "hold",
			IdempotencyKey: "hold-1",
			Pending:        true,
			Postings: []ledger.Posting{{
				Debit:  alice.ID,
				Credit: bob.ID,
				Amount: money.FromInt64(5_000),
				Asset:  "USD/2",
			}},
		})
		if err != nil {
			t.Fatalf("Post hold-1: %v", err)
		}
		// Alice's available should now be 6900 - 5000 = 1900; trying to spend 2000 must fail with insufficient_funds
		_, err = eng.Post(ctx, ledger.Intent{
			Ledger:         "main",
			Kind:           "transfer",
			IdempotencyKey: "overdraft-attempt",
			Postings: []ledger.Posting{{
				Debit:  alice.ID,
				Credit: bob.ID,
				Amount: money.FromInt64(2_000),
				Asset:  "USD/2",
			}},
		})
		if err == nil {
			t.Fatalf("expected insufficient_funds error")
		}

		// Settle 40.00 USD (4000) of the 50.00 USD hold
		settleAmt := money.FromInt64(4_000)
		settledTx, err := eng.SettleHold(ctx, holdTx.ID, "settle-1", &settleAmt)
		if err != nil {
			t.Fatalf("SettleHold: %v", err)
		}

		// Reverse the settled transaction in full
		clock.Advance(10 * time.Second)
		if _, err := eng.Reverse(ctx, settledTx.ID, "rev-1", nil); err != nil {
			t.Fatalf("Reverse: %v", err)
		}
		// Second reversal of the same settledTx must fail (Invariant I7)
		if _, err := eng.Reverse(ctx, settledTx.ID, "rev-2", nil); err == nil {
			t.Fatalf("expected double reversal to be rejected")
		}

		// HolderBalances check
		hbs, err := eng.HolderBalances(ctx, holder.ID, "")
		if err != nil || len(hbs) != 1 || hbs[0].Posted.Cmp(money.FromInt64(6_900)) != 0 {
			t.Fatalf("unexpected HolderBalances: %+v (err=%v)", hbs, err)
		}

		// Seal journal and check all 8 invariants (I1-I8)
		if _, _, err := eng.SealOnce(ctx, "main"); err != nil {
			t.Fatalf("SealOnce: %v", err)
		}
		report, err := ledger.CheckInvariants(ctx, store, "main")
		if err != nil {
			t.Fatalf("CheckInvariants: %v", err)
		}
		if !report.OK() {
			t.Fatalf("invariant violations detected: %+v", report.Violations)
		}

		// Verify idempotent ExecModuleMigration
		migrationDDL := `CREATE TABLE IF NOT EXISTS test_mod_table (id TEXT PRIMARY KEY);`
		if err := store.Admin().ExecModuleMigration(ctx, "test_mod", "0001_init.sql", migrationDDL); err != nil {
			t.Fatalf("ExecModuleMigration 1: %v", err)
		}
		if err := store.Admin().ExecModuleMigration(ctx, "test_mod", "0001_init.sql", migrationDDL); err != nil {
			t.Fatalf("ExecModuleMigration 2 (idempotent): %v", err)
		}
	})

	t.Run("PropertyStateMachineInvariants", func(t *testing.T) {
		ctx := context.Background()
		store := factory(t)
		defer func() { _ = store.Close() }()

		clock := ledger.NewManualClock(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
		eng, err := ledger.New(store, ledger.WithClock(clock))
		if err != nil {
			t.Fatalf("ledger.New: %v", err)
		}
		defer func() { _ = eng.Close(ctx) }()

		if _, err := eng.CreateLedger(ctx, "prop", "Property Test Ledger", nil); err != nil {
			t.Fatalf("CreateLedger: %v", err)
		}
		if _, err := eng.RegisterAsset(ctx, "prop", "USD", 2); err != nil {
			t.Fatalf("RegisterAsset: %v", err)
		}

		zero := money.Zero
		bank, err := eng.CreateAccount(ctx, ledger.Account{
			Ledger: "prop",
			Code:   "assets:bank:usd",
			Asset:  "USD/2",
			Normal: ledger.Debit,
		})
		if err != nil {
			t.Fatalf("CreateAccount bank: %v", err)
		}
		revAcct, err := eng.CreateAccount(ctx, ledger.Account{
			Ledger:      "prop",
			Code:        "revenue:platform:usd",
			Asset:       "USD/2",
			Normal:      ledger.Credit,
			BalanceMode: ledger.Deferred,
		})
		if err != nil {
			t.Fatalf("CreateAccount rev: %v", err)
		}

		wallets := make([]*ledger.Account, 4)
		for i := range wallets {
			w, err := eng.CreateAccount(ctx, ledger.Account{
				Ledger: "prop",
				Code:   fmt.Sprintf("liabilities:wallets:w%d:usd", i),
				Asset:  "USD/2",
				Normal: ledger.Credit,
				Limits: ledger.Limits{Min: &zero},
			})
			if err != nil {
				t.Fatalf("CreateAccount w%d: %v", i, err)
			}
			wallets[i] = w

			// Seed each wallet with 100.00 USD (10,000 minor units)
			if _, err := eng.Post(ctx, ledger.Intent{
				Ledger:         "prop",
				Kind:           "seed",
				IdempotencyKey: fmt.Sprintf("seed-%d", i),
				Postings: []ledger.Posting{{
					Debit:  bank.ID,
					Credit: w.ID,
					Amount: money.FromInt64(10_000),
					Asset:  "USD/2",
				}},
			}); err != nil {
				t.Fatalf("seed w%d: %v", i, err)
			}
		}

		rng := rand.New(rand.NewSource(42))
		var settledTxs []*ledger.Transaction
		var pendingHolds []*ledger.Transaction

		for step := 0; step < 50; step++ {
			clock.Advance(time.Second)
			op := rng.Intn(6)
			switch op {
			case 0: // Deposit from bank to random wallet
				w := wallets[rng.Intn(len(wallets))]
				amt := money.FromInt64(int64(100 + rng.Intn(2_000)))
				tx, err := eng.Post(ctx, ledger.Intent{
					Ledger:         "prop",
					Kind:           "deposit",
					IdempotencyKey: fmt.Sprintf("prop-dep-%d", step),
					Postings: []ledger.Posting{{
						Debit:  bank.ID,
						Credit: w.ID,
						Amount: amt,
						Asset:  "USD/2",
					}},
				})
				if err == nil {
					settledTxs = append(settledTxs, tx)
				}

			case 1: // Wallet-to-wallet transfer with optional deferred fee leg (may overdraft and fail cleanly)
				src := wallets[rng.Intn(len(wallets))]
				dst := wallets[(rng.Intn(len(wallets)-1)+1)%len(wallets)]
				if src.ID == dst.ID {
					dst = wallets[(0+1)%len(wallets)]
				}
				amt := money.FromInt64(int64(50 + rng.Intn(4_000)))
				fee := money.FromInt64(int64(1 + rng.Intn(25)))
				tx, err := eng.Post(ctx, ledger.Intent{
					Ledger:         "prop",
					Kind:           "transfer",
					IdempotencyKey: fmt.Sprintf("prop-xfer-%d", step),
					Postings: []ledger.Posting{
						{Debit: src.ID, Credit: dst.ID, Amount: amt, Asset: "USD/2"},
						{Debit: src.ID, Credit: revAcct.ID, Amount: fee, Asset: "USD/2"},
					},
				})
				if err == nil {
					settledTxs = append(settledTxs, tx)
				}

			case 2: // Place a pending hold
				src := wallets[rng.Intn(len(wallets))]
				dst := wallets[rng.Intn(len(wallets))]
				if src.ID == dst.ID {
					continue
				}
				amt := money.FromInt64(int64(100 + rng.Intn(2_500)))
				exp := clock.Now().Add(10 * time.Minute)
				tx, err := eng.Post(ctx, ledger.Intent{
					Ledger:         "prop",
					Kind:           "hold",
					Pending:        true,
					ExpiresAt:      &exp,
					IdempotencyKey: fmt.Sprintf("prop-hold-%d", step),
					Postings: []ledger.Posting{{
						Debit:  src.ID,
						Credit: dst.ID,
						Amount: amt,
						Asset:  "USD/2",
					}},
				})
				if err == nil {
					pendingHolds = append(pendingHolds, tx)
				}

			case 3: // Settle a pending hold (partial or full)
				if len(pendingHolds) == 0 {
					continue
				}
				idx := rng.Intn(len(pendingHolds))
				h := pendingHolds[idx]
				pendingHolds = append(pendingHolds[:idx], pendingHolds[idx+1:]...)
				var settleAmt *money.Amount
				if rng.Intn(2) == 0 {
					// Partial settlement (half of held amount)
					half, _, _ := money.MulRate(h.Postings[0].Amount, money.Rate{Mantissa: money.FromInt64(5), Scale: 1}, money.HalfEven)
					if half.IsPositive() {
						settleAmt = &half
					}
				}
				stx, err := eng.SettleHold(ctx, h.ID, fmt.Sprintf("prop-settle-%d", step), settleAmt)
				if err == nil {
					settledTxs = append(settledTxs, stx)
				}

			case 4: // Void a pending hold
				if len(pendingHolds) == 0 {
					continue
				}
				idx := rng.Intn(len(pendingHolds))
				h := pendingHolds[idx]
				pendingHolds = append(pendingHolds[:idx], pendingHolds[idx+1:]...)
				_, _ = eng.VoidHold(ctx, h.ID, fmt.Sprintf("prop-void-%d", step))

			case 5: // Reverse a settled transaction (full or partial)
				if len(settledTxs) == 0 {
					continue
				}
				orig := settledTxs[rng.Intn(len(settledTxs))]
				_, _ = eng.Reverse(ctx, orig.ID, fmt.Sprintf("prop-rev-%d", step), nil)
			}

			// Every 10 steps, seal journal and check all 8 invariants (I1-I8)
			if step%10 == 9 {
				if _, _, err := eng.SealOnce(ctx, "prop"); err != nil {
					t.Fatalf("step %d SealOnce: %v", step, err)
				}
				report, err := ledger.CheckInvariants(ctx, store, "prop")
				if err != nil {
					t.Fatalf("step %d CheckInvariants: %v", step, err)
				}
				if !report.OK() {
					t.Fatalf("step %d invariant violations: %+v", step, report.Violations)
				}
			}
		}

		if _, _, err := eng.SealOnce(ctx, "prop"); err != nil {
			t.Fatalf("final SealOnce: %v", err)
		}
		report, err := ledger.CheckInvariants(ctx, store, "prop")
		if err != nil {
			t.Fatalf("final CheckInvariants: %v", err)
		}
		if !report.OK() {
			t.Fatalf("final invariant violations: %+v", report.Violations)
		}
	})

	t.Run("BatchedCommitterIsolationAndInvariants", func(t *testing.T) {
		ctx := context.Background()
		store := factory(t)
		defer func() { _ = store.Close() }()

		eng, err := ledger.New(store, ledger.WithBatchCommitter(commit.BatchConfig{
			MaxBatch:    32,
			MaxWait:     3 * time.Millisecond,
			LockTimeout: 200 * time.Millisecond,
		}))
		if err != nil {
			t.Fatalf("ledger.New: %v", err)
		}
		defer func() { _ = eng.Close(ctx) }()

		_, _ = eng.CreateLedger(ctx, "batch", "Batch Ledger", nil)
		_, _ = eng.RegisterAsset(ctx, "batch", "USD", 2)

		zero := money.Zero
		bank, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "batch", Code: "assets:bank:usd", Asset: "USD/2", Normal: ledger.Debit})
		alice, _ := eng.CreateAccount(ctx, ledger.Account{
			Ledger: "batch", Code: "liabilities:wallets:alice:usd", Asset: "USD/2", Normal: ledger.Credit,
			Limits: ledger.Limits{Min: &zero},
		})
		bob, _ := eng.CreateAccount(ctx, ledger.Account{
			Ledger: "batch", Code: "liabilities:wallets:bob:usd", Asset: "USD/2", Normal: ledger.Credit,
			Limits: ledger.Limits{Min: &zero},
		})

		if _, err := eng.Post(ctx, ledger.Intent{
			Ledger: "batch", Kind: "deposit", IdempotencyKey: "batch-fund",
			Postings: []ledger.Posting{{Debit: bank.ID, Credit: alice.ID, Amount: money.FromInt64(5_000), Asset: "USD/2"}},
		}); err != nil {
			t.Fatalf("batch-fund: %v", err)
		}

		var wg sync.WaitGroup
		errs := make([]error, 3)
		amts := []int64{2_000, 99_000, 1_500}
		keys := []string{"bc-1", "bc-overdraft", "bc-2"}
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, errs[idx] = eng.Post(ctx, ledger.Intent{
					Ledger:         "batch",
					Kind:           "transfer",
					IdempotencyKey: keys[idx],
					Postings: []ledger.Posting{{
						Debit:  alice.ID,
						Credit: bob.ID,
						Amount: money.FromInt64(amts[idx]),
						Asset:  "USD/2",
					}},
				})
			}(i)
		}
		wg.Wait()

		if errs[0] != nil || errs[2] != nil {
			t.Fatalf("expected valid batch neighbours to succeed, got errs=%v", errs)
		}
		if errs[1] == nil {
			t.Fatalf("expected overdraft intent in batch to fail")
		}

		if _, _, err := eng.SealOnce(ctx, "batch"); err != nil {
			t.Fatalf("SealOnce: %v", err)
		}
		report, err := ledger.CheckInvariants(ctx, store, "batch")
		if err != nil || !report.OK() {
			t.Fatalf("batch CheckInvariants failed: err=%v violations=%+v", err, report.Violations)
		}
	})

	t.Run("VelocityRelayIsolationAndAdvisoryLock", func(t *testing.T) {
		ctx := context.Background()
		store := factory(t)
		defer func() { _ = store.Close() }()

		velMod := ledger.NewVelocityValidator(ledger.VelocityConfig{
			MaxSingleAmount: 5_000,
			MaxWindowAmount: 8_000,
			WindowSeconds:   3600,
		})
		failSub := &testSubscriber{name: "failing-sub", shouldFail: true}
		okSub := &testSubscriber{name: "healthy-sub", shouldFail: false}

		eng, err := ledger.New(store, ledger.WithModules(velMod, failSub, okSub))
		if err != nil {
			t.Fatalf("ledger.New: %v", err)
		}
		defer func() { _ = eng.Close(ctx) }()

		_, _ = eng.CreateLedger(ctx, "vel", "Velocity Ledger", nil)
		_, _ = eng.RegisterAsset(ctx, "vel", "USD", 2)
		a1, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "vel", Code: "assets:bank:usd", Asset: "USD/2", Normal: ledger.Debit})
		a2, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "vel", Code: "liabilities:wallets:u1", Asset: "USD/2", Normal: ledger.Credit})

		// 1. Exceeds MaxSingleAmount (6000 > 5000) -> rejected by VelocityValidator
		if _, err := eng.Post(ctx, ledger.Intent{
			Ledger: "vel", Kind: "deposit", IdempotencyKey: "vel-too-big",
			Postings: []ledger.Posting{{Debit: a1.ID, Credit: a2.ID, Amount: money.FromInt64(6_000), Asset: "USD/2"}},
		}); err == nil {
			t.Fatalf("expected single velocity limit rejection")
		}

		// 2. Within limit (4500 <= 5000) -> succeeds
		if _, err := eng.Post(ctx, ledger.Intent{
			Ledger: "vel", Kind: "deposit", IdempotencyKey: "vel-ok-1",
			Postings: []ledger.Posting{{Debit: a1.ID, Credit: a2.ID, Amount: money.FromInt64(4_500), Asset: "USD/2"}},
		}); err != nil {
			t.Fatalf("vel-ok-1: %v", err)
		}

		// 3. Second transfer (4000) pushes rolling window to 8500 > 8000 -> rejected
		if _, err := eng.Post(ctx, ledger.Intent{
			Ledger: "vel", Kind: "deposit", IdempotencyKey: "vel-window-exceed",
			Postings: []ledger.Posting{{Debit: a1.ID, Credit: a2.ID, Amount: money.FromInt64(4_000), Asset: "USD/2"}},
		}); err == nil {
			t.Fatalf("expected rolling window velocity limit rejection")
		}

		// 4. Outbox per-subscriber isolation: failing-sub errors, but healthy-sub still receives events and advances cursor
		delivered, relayErr := eng.RelayOnce(ctx)
		if relayErr == nil {
			t.Fatalf("expected error from failing-sub")
		}
		if delivered == 0 || len(okSub.events) == 0 {
			t.Fatalf("expected healthy-sub to receive events despite failing-sub error, got delivered=%d", delivered)
		}
		failCur, _ := store.Outbox().GetCursor(ctx, "failing-sub")
		okCur, _ := store.Outbox().GetCursor(ctx, "healthy-sub")
		if failCur != 0 || okCur == 0 {
			t.Fatalf("expected failCur=0 and okCur>0, got failCur=%d okCur=%d", failCur, okCur)
		}

		// 5. Sealer advisory lock: while lock is held, SealOnce is a no-op (returns 0, nil, nil)
		unlock, acquired, err := store.Journal().TryAdvisoryLock(ctx, "sealer:vel")
		if err != nil || !acquired {
			t.Fatalf("TryAdvisoryLock failed: acquired=%v err=%v", acquired, err)
		}
		n, _, err := eng.SealOnce(ctx, "vel")
		if err != nil || n != 0 {
			unlock()
			t.Fatalf("expected SealOnce while lock held to return 0, got n=%d err=%v", n, err)
		}
		unlock()
		n, _, err = eng.SealOnce(ctx, "vel")
		if err != nil || n != 1 {
			t.Fatalf("expected SealOnce after unlock to seal 1 tx, got n=%d err=%v", n, err)
		}
	})
}

type testSubscriber struct {
	name       string
	shouldFail bool
	events     []events.Event
}

func (s *testSubscriber) Name() string                      { return s.name }
func (s *testSubscriber) Init(context.Context, module.Host) error { return nil }
func (s *testSubscriber) Close(context.Context) error       { return nil }
func (s *testSubscriber) Topics() []string                  { return []string{"transaction.*"} }
func (s *testSubscriber) Handle(_ context.Context, evs []events.Event) error {
	if s.shouldFail {
		return fmt.Errorf("simulated subscriber failure")
	}
	s.events = append(s.events, evs...)
	return nil
}
