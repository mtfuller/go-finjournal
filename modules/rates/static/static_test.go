package static_test

import (
	"context"
	"testing"
	"time"

	"github.com/mtfuller/go-finjournal/modules/rates/static"
	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage/memory"
)

func TestStaticFXConvertWorkedExample(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	clock := ledger.NewManualClock(time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC))

	fxMod, err := static.New(static.Config{
		QuoteTTL: 30 * time.Second,
		Pairs: []static.PairConfig{{
			Base:          "USD/2",
			Quote:         "EUR/2",
			Mid:           "0.9213",
			SpreadPercent: "0.5",
			Source:        "ecb",
		}},
	})
	if err != nil {
		t.Fatalf("static.New: %v", err)
	}

	eng, err := ledger.New(store, ledger.WithClock(clock), ledger.WithModules(fxMod))
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	defer func() { _ = eng.Close(ctx) }()

	_, _ = eng.CreateLedger(ctx, "main", "Main", nil)
	_, _ = eng.RegisterAsset(ctx, "main", "USD", 2)
	_, _ = eng.RegisterAsset(ctx, "main", "EUR", 2)

	holder, _ := eng.CreateHolder(ctx, ledger.AccountHolder{Ledger: "main", Kind: "customer", ExternalRef: "alice"})
	_, _ = eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "assets:bank:usd", Asset: "USD/2", Normal: ledger.Debit})
	_, _ = eng.CreateAccount(ctx, ledger.Account{
		Ledger:      "main",
		Code:        "system:fx:position:usd",
		Asset:       "USD/2",
		Normal:      ledger.Credit,
		BalanceMode: ledger.Deferred,
	})
	aliceUSD, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main",
		Holder: &holder.ID,
		Code:   "liabilities:wallets:alice:usd",
		Asset:  "USD/2",
		Normal: ledger.Credit,
	})
	aliceEUR, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main",
		Holder: &holder.ID,
		Code:   "liabilities:wallets:alice:eur",
		Asset:  "EUR/2",
		Normal: ledger.Credit,
	})

	_, err = eng.Post(ctx, ledger.Intent{
		Ledger:         "main",
		Kind:           "deposit",
		IdempotencyKey: "fund-alice",
		Postings: []ledger.Posting{{
			Debit:  "assets:bank:usd",
			Credit: aliceUSD.ID,
			Amount: money.FromInt64(10_000),
			Asset:  "USD/2",
		}},
	})
	if err != nil {
		t.Fatalf("deposit: %v", err)
	}

	quote, err := fxMod.Quote(ctx, "USD/2", "EUR/2", clock.Now())
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}

	fxTx, err := eng.Post(ctx, ledger.Intent{
		Ledger:         "main",
		Kind:           "fx_convert",
		IdempotencyKey: "fx-1",
		Postings: []ledger.Posting{{
			Debit:  aliceUSD.ID,
			Credit: "system:fx:position:usd",
			Amount: money.FromInt64(10_000),
			Asset:  "USD/2",
		}},
		Params: ledger.Metadata{
			"fx.to_account": string(aliceEUR.ID),
			"fx.quote_id":   quote.ID,
		},
	})
	if err != nil {
		t.Fatalf("Post fx_convert: %v", err)
	}
	if len(fxTx.Postings) != 3 {
		t.Fatalf("expected 3 postings on fx_convert, got %d", len(fxTx.Postings))
	}
	if fxTx.Postings[1].Amount.Cmp(money.FromInt64(9_167)) != 0 {
		t.Fatalf("expected leg 2 = 9167 (91.67 EUR), got %s", fxTx.Postings[1].Amount)
	}
	if fxTx.Postings[2].Amount.Cmp(money.FromInt64(46)) != 0 {
		t.Fatalf("expected leg 3 = 46 (0.46 EUR spread), got %s", fxTx.Postings[2].Amount)
	}
}
