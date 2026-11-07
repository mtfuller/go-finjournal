package recon_test

import (
	"context"
	"strings"
	"testing"

	"github.com/mtfuller/go-finjournal/modules/recon"
	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage/memory"
)

func TestReconcileCSV(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	reconMod := recon.New()
	eng, err := ledger.New(store, ledger.WithModules(reconMod))
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	defer func() { _ = eng.Close(ctx) }()

	_, _ = eng.CreateLedger(ctx, "main", "Main", nil)
	_, _ = eng.RegisterAsset(ctx, "main", "USD", 2)
	bank, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger:   "main",
		Code:     "assets:bank:operating:usd",
		Asset:    "USD/2",
		Normal:   ledger.Debit,
		Metadata: ledger.Metadata{"recon.source": "bank:operating-usd"},
	})
	alice, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main",
		Code:   "liabilities:wallets:alice:usd",
		Asset:  "USD/2",
		Normal: ledger.Credit,
	})

	tx, err := eng.Post(ctx, ledger.Intent{
		Ledger:         "main",
		Kind:           "deposit",
		IdempotencyKey: "wire-101",
		Postings: []ledger.Posting{{
			Debit:  bank.ID,
			Credit: alice.ID,
			Amount: money.FromInt64(25_000),
			Asset:  "USD/2",
		}},
		Metadata: ledger.Metadata{"recon.ref": "WIRE-101"},
	})
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if _, err := eng.RelayOnce(ctx); err != nil {
		t.Fatalf("RelayOnce: %v", err)
	}

	csvData := "id,reference,amount,value_date\nstmt_1,WIRE-101,250.00,2026-11-06\n"
	results, err := reconMod.ImportCSV(ctx, "bank:operating-usd", "USD/2", strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("ImportCSV: %v", err)
	}
	if len(results) != 1 || results[0].Status != recon.StatusMatched || results[0].Expected[0] != tx.ID {
		t.Fatalf("unexpected recon results: %+v", results)
	}
}
