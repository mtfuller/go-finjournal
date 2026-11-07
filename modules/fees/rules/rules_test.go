package rules_test

import (
	"context"
	"testing"
	"time"

	"github.com/mtfuller/go-finjournal/modules/fees/rules"
	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage/memory"
)

func TestCardCaptureFeeSchedule(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	clock := ledger.NewManualClock(time.Date(2026, 11, 3, 14, 2, 11, 482_000_000, time.UTC))

	feeMod, err := rules.New(rules.Config{
		Schedules: []rules.Schedule{{
			ID:            "card_capture",
			Version:       3,
			EffectiveFrom: "2026-11-01",
			Match:         rules.MatchCriteria{Kind: "card_capture", Asset: "USD/2"},
			Payer:         "credit_of_posting_0",
			Payee:         "revenue:fees:card:usd",
			Percent:       "2.9",
			Fixed:         "0.30",
			Cap:           "25.00",
		}},
	})
	if err != nil {
		t.Fatalf("rules.New: %v", err)
	}

	eng, err := ledger.New(store, ledger.WithClock(clock), ledger.WithModules(feeMod))
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	defer func() { _ = eng.Close(ctx) }()

	_, _ = eng.CreateLedger(ctx, "main", "Main", nil)
	_, _ = eng.RegisterAsset(ctx, "main", "USD", 2)

	zero := money.Zero
	_, _ = eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main",
		Code:   "assets:psp:usd",
		Asset:  "USD/2",
		Normal: ledger.Debit,
	})
	alice, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main",
		Code:   "liabilities:wallets:alice:usd",
		Asset:  "USD/2",
		Normal: ledger.Credit,
		Limits: ledger.Limits{Min: &zero},
	})

	tx, err := eng.Post(ctx, ledger.Intent{
		Ledger:         "main",
		Kind:           "card_capture",
		IdempotencyKey: "cap_8f2c41",
		Postings: []ledger.Posting{{
			Debit:  "assets:psp:usd",
			Credit: "liabilities:wallets:alice:usd",
			Amount: money.FromInt64(1001),
			Asset:  "USD/2",
		}},
		Metadata: ledger.Metadata{"psp_charge_id": "ch_3Q9"},
	})
	if err != nil {
		t.Fatalf("Post card_capture: %v", err)
	}
	if len(tx.Postings) != 2 {
		t.Fatalf("expected 2 postings, got %d", len(tx.Postings))
	}
	if tx.Postings[1].Amount.Cmp(money.FromInt64(59)) != 0 || tx.Postings[1].Source != "fees/rules" {
		t.Fatalf("unexpected fee posting: %+v", tx.Postings[1])
	}
	if tx.Metadata["fees/rules.schedule"] != "card_capture@3" {
		t.Fatalf("expected schedule metadata card_capture@3, got %q", tx.Metadata["fees/rules.schedule"])
	}

	aliceBal, err := store.Balance(ctx, alice.ID)
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if aliceBal.Posted(alice.Normal).Cmp(money.FromInt64(942)) != 0 {
		t.Fatalf("expected Alice net balance 942 (9.42 USD), got %s", aliceBal.Posted(alice.Normal))
	}
}
