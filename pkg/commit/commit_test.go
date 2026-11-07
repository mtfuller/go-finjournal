package commit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage/memory"
)

func setupCommitTestStore(t *testing.T) (*memory.Store, core.Account, core.Account, map[core.AccountID]core.Account) {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)

	if err := store.Admin().CreateLedger(ctx, core.Ledger{ID: "main", Name: "Main", CreatedAt: now}); err != nil {
		t.Fatalf("CreateLedger: %v", err)
	}
	if err := store.Admin().RegisterAsset(ctx, "main", money.Asset{Code: "USD", Scale: 2}); err != nil {
		t.Fatalf("RegisterAsset: %v", err)
	}

	bank := core.Account{
		ID:          "acct-bank",
		Ledger:      "main",
		Code:        "assets:bank:usd",
		Asset:       "USD/2",
		Normal:      core.Debit,
		BalanceMode: core.Locked,
		Status:      core.AccountOpen,
		CreatedAt:   now,
	}
	zero := money.Zero
	alice := core.Account{
		ID:          "acct-alice",
		Ledger:      "main",
		Code:        "liabilities:wallets:alice:usd",
		Asset:       "USD/2",
		Normal:      core.Credit,
		BalanceMode: core.Locked,
		Limits:      core.Limits{Min: &zero},
		Status:      core.AccountOpen,
		CreatedAt:   now,
	}
	if err := store.Admin().CreateAccount(ctx, bank); err != nil {
		t.Fatalf("CreateAccount bank: %v", err)
	}
	if err := store.Admin().CreateAccount(ctx, alice); err != nil {
		t.Fatalf("CreateAccount alice: %v", err)
	}

	accts := map[core.AccountID]core.Account{
		bank.ID:  bank,
		alice.ID: alice,
	}
	return store, bank, alice, accts
}

func TestBatchedCommitterSplitOnLockTimeout(t *testing.T) {
	ctx := context.Background()
	store, bank, alice, accts := setupCommitTestStore(t)
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)

	bc, err := NewBatched(store, BatchConfig{
		MaxBatch:    10,
		LockTimeout: 50 * time.Millisecond,
		StepMode:    true,
	})
	if err != nil {
		t.Fatalf("NewBatched: %v", err)
	}
	defer func() { _ = bc.Close() }()

	// Arm a one-shot CodeLockTimeout on the first LockBalances call.
	// BatchedCommitter should catch the lock timeout on the 4-item batch, split it into
	// two 2-item sub-batches, and succeed on both halves.
	store.FailNext("LockBalances", core.NewError(core.CodeLockTimeout, "simulated lock timeout", nil))

	var chs []<-chan itemResult
	for i := 0; i < 4; i++ {
		txID := core.TransactionID("tx-split-" + string(rune('a'+i)))
		key := "idem-split-" + string(rune('a'+i))
		req := CommitRequest{
			Op:          OpPost,
			RequestHash: []byte(key),
			Accounts:    accts,
			EventID:     "ev-" + key,
			Tx: &core.Transaction{
				ID:             txID,
				Ledger:         "main",
				Kind:           "deposit",
				Status:         core.StatusPosted,
				IdempotencyKey: key,
				EffectiveAt:    now,
				CommittedAt:    now,
				Postings: []core.Posting{{
					Debit:  bank.ID,
					Credit: alice.ID,
					Amount: money.FromInt64(1_000),
					Asset:  "USD/2",
				}},
			},
		}
		chs = append(chs, bc.EnqueueAsync(ctx, req))
	}

	flushed := bc.FlushBatch()
	if flushed != 4 {
		t.Fatalf("expected 4 items flushed, got %d", flushed)
	}

	for i, ch := range chs {
		res := <-ch
		if res.err != nil {
			t.Fatalf("item %d failed after batch split retry: %v", i, res.err)
		}
		if res.tx == nil {
			t.Fatalf("item %d returned nil tx", i)
		}
	}

	bal, err := store.Balance(ctx, alice.ID)
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if bal.Posted(alice.Normal).Cmp(money.FromInt64(4_000)) != 0 {
		t.Fatalf("expected Alice balance 4000 after split batch, got %s", bal.Posted(alice.Normal))
	}
}

func TestBatchedCommitterInBatchIdempotencyReplayAndConflict(t *testing.T) {
	ctx := context.Background()
	store, bank, alice, accts := setupCommitTestStore(t)
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)

	bc, err := NewBatched(store, BatchConfig{
		MaxBatch:    10,
		LockTimeout: 50 * time.Millisecond,
		StepMode:    true,
	})
	if err != nil {
		t.Fatalf("NewBatched: %v", err)
	}
	defer func() { _ = bc.Close() }()

	makeReq := func(txID string, key string, hash string, amt int64) CommitRequest {
		return CommitRequest{
			Op:          OpPost,
			RequestHash: []byte(hash),
			Accounts:    accts,
			EventID:     "ev-" + txID,
			Tx: &core.Transaction{
				ID:             core.TransactionID(txID),
				Ledger:         "main",
				Kind:           "deposit",
				Status:         core.StatusPosted,
				IdempotencyKey: key,
				EffectiveAt:    now,
				CommittedAt:    now,
				Postings: []core.Posting{{
					Debit:  bank.ID,
					Credit: alice.ID,
					Amount: money.FromInt64(amt),
					Asset:  "USD/2",
				}},
			},
		}
	}

	// Enqueue 3 items with the SAME idempotency key in the same batch:
	// 0: original request (succeeds)
	// 1: exact replay with same hash (returns IdempotentReplay = true, does not double-post)
	// 2: different hash with same key (returns CodeIdempotencyConflict)
	ch0 := bc.EnqueueAsync(ctx, makeReq("tx-idem-0", "dup-key", "hash-v1", 2_500))
	ch1 := bc.EnqueueAsync(ctx, makeReq("tx-idem-1", "dup-key", "hash-v1", 2_500))
	ch2 := bc.EnqueueAsync(ctx, makeReq("tx-idem-2", "dup-key", "hash-v2-different", 9_900))

	bc.FlushBatch()

	r0 := <-ch0
	r1 := <-ch1
	r2 := <-ch2

	if r0.err != nil || r0.tx.ID != "tx-idem-0" || r0.tx.IdempotentReplay {
		t.Fatalf("unexpected r0: %+v", r0)
	}
	if r1.err != nil || r1.tx.ID != "tx-idem-0" || !r1.tx.IdempotentReplay {
		t.Fatalf("expected r1 to be idempotent replay of tx-idem-0, got %+v", r1)
	}
	var cErr *core.Error
	if !errors.As(r2.err, &cErr) || cErr.Code != core.CodeIdempotencyConflict {
		t.Fatalf("expected r2 CodeIdempotencyConflict, got %v", r2.err)
	}

	bal, _ := store.Balance(ctx, alice.ID)
	if bal.Posted(alice.Normal).Cmp(money.FromInt64(2_500)) != 0 {
		t.Fatalf("expected Alice balance 2500 (posted once), got %s", bal.Posted(alice.Normal))
	}
}
