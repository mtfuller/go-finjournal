package journal

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage/memory"
)

func TestJCSAndChainVerification(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

	if err := store.Admin().CreateLedger(ctx, core.Ledger{ID: "main", Name: "Main", CreatedAt: now}); err != nil {
		t.Fatalf("CreateLedger: %v", err)
	}

	tx1 := &core.Transaction{
		ID:             "01928c4e-7a1b-7c3e-9f00-3b1d2e4f5a60",
		Ledger:         "main",
		Kind:           "transfer",
		Status:         core.StatusPosted,
		Postings:       []core.Posting{{Debit: "a1", Credit: "a2", Amount: money.FromInt64(1000), Asset: "USD/2"}},
		EffectiveAt:    now,
		CommittedAt:    now,
		IdempotencyKey: "k1",
		Metadata:       core.Metadata{"z": "last", "a": "first"},
	}

	dbTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := dbTx.InsertTransactions(ctx, []*core.Transaction{tx1}); err != nil {
		t.Fatalf("InsertTransactions: %v", err)
	}
	if err := dbTx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	sealer := NewSealer(store, SealerConfig{CheckpointEvery: 1})
	n, cp, err := sealer.SealOnce(ctx, "main")
	if err != nil {
		t.Fatalf("SealOnce: %v", err)
	}
	if n != 1 || cp == nil || cp.Seq != 1 {
		t.Fatalf("unexpected seal result: n=%d, cp=%+v", n, cp)
	}

	report, err := VerifyChain(ctx, store, "main")
	if err != nil {
		t.Fatalf("VerifyChain failed: %v", err)
	}
	if report.VerifiedRecords != 1 || report.VerifiedCheckpts != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func FuzzJCSStability(f *testing.F) {
	f.Add("tx-1", "transfer", "k1", "val1", "k2", "val2", int64(500))
	f.Fuzz(func(t *testing.T, id, kind, k1, v1, k2, v2 string, amt int64) {
		if amt <= 0 {
			amt = 1
		}
		now := time.Unix(1700000000, 123456789).UTC()
		metaA := core.Metadata{k1: v1, k2: v2}
		metaB := core.Metadata{k2: v2, k1: v1}

		txA := &core.Transaction{
			ID:             core.TransactionID(id),
			Ledger:         "main",
			Kind:           kind,
			Status:         core.StatusPosted,
			Postings:       []core.Posting{{Debit: "d", Credit: "c", Amount: money.FromInt64(amt), Asset: "USD/2"}},
			EffectiveAt:    now,
			CommittedAt:    now,
			IdempotencyKey: "idem",
			Metadata:       metaA,
		}
		txB := &core.Transaction{
			ID:             core.TransactionID(id),
			Ledger:         "main",
			Kind:           kind,
			Status:         core.StatusVoided, // Status is mutable lifecycle data, so JCS must be identical!
			Postings:       []core.Posting{{Debit: "d", Credit: "c", Amount: money.FromInt64(amt), Asset: "USD/2"}},
			EffectiveAt:    now,
			CommittedAt:    now,
			IdempotencyKey: "idem",
			Metadata:       metaB,
		}
		jcsA, err := CanonicalTransactionJSON(txA)
		if err != nil {
			t.Fatalf("CanonicalTransactionJSON A: %v", err)
		}
		jcsB, err := CanonicalTransactionJSON(txB)
		if err != nil {
			t.Fatalf("CanonicalTransactionJSON B: %v", err)
		}
		if !bytes.Equal(jcsA, jcsB) {
			t.Fatalf("JCS mismatch:\n%s\nvs\n%s", string(jcsA), string(jcsB))
		}
	})
}
