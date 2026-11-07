package storagetest_test

import (
	"context"
	"testing"

	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage"
	"github.com/mtfuller/go-finjournal/pkg/storage/memory"
	"github.com/mtfuller/go-finjournal/pkg/storage/sqlite"
	"github.com/mtfuller/go-finjournal/pkg/storage/storagetest"
)

func TestMemoryStoreConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Store {
		return memory.New()
	})
}

func TestSQLiteStoreConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Store {
		s, err := sqlite.Open(context.Background(), ":memory:")
		if err != nil {
			t.Fatalf("sqlite.Open: %v", err)
		}
		return s
	})
}

func TestSQLiteWithinTxAndImmutabilityTriggers(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, ":memory:")
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	eng, err := ledger.New(store)
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	if _, err := eng.CreateLedger(ctx, "main", "Main", nil); err != nil {
		t.Fatalf("CreateLedger: %v", err)
	}
	if _, err := eng.RegisterAsset(ctx, "main", "USD", 2); err != nil {
		t.Fatalf("RegisterAsset: %v", err)
	}
	a1, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "a1", Asset: "USD/2", Normal: ledger.Debit})
	a2, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "a2", Asset: "USD/2", Normal: ledger.Credit})

	// Test WithinTx: open caller SQL transaction, post inside it, and commit caller transaction
	sqlTx, err := store.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	tx, err := eng.Post(ctx, ledger.Intent{
		Ledger:         "main",
		Kind:           "transfer",
		IdempotencyKey: "within-tx-1",
		Postings: []ledger.Posting{{
			Debit:  a1.ID,
			Credit: a2.ID,
			Amount: money.FromInt64(500),
			Asset:  "USD/2",
		}},
	}, ledger.WithinTx(sqlTx))
	if err != nil {
		t.Fatalf("Post WithinTx: %v", err)
	}
	if err := sqlTx.Commit(); err != nil {
		t.Fatalf("sqlTx.Commit: %v", err)
	}

	// Verify immutability trigger on transactions table rejects DELETE and UPDATE
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM transactions WHERE id = ?`, string(tx.ID)); err == nil {
		t.Fatalf("expected DELETE on transactions to be blocked by immutability trigger")
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE entries SET amount = '9999'`); err == nil {
		t.Fatalf("expected UPDATE on entries to be blocked by immutability trigger")
	}
}
