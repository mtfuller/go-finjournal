// Package storagetest provides the shared conformance test suite that every
// storage.Store adapter (memory, SQLite, PostgreSQL) must pass.
package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
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
	})
}
