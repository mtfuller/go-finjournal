package ledger_test

import (
	"context"
	"errors"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mtfuller/go-finjournal/modules/fees/rules"
	"github.com/mtfuller/go-finjournal/modules/rates/static"
	"github.com/mtfuller/go-finjournal/modules/recon"
	"github.com/mtfuller/go-finjournal/modules/sinks/nats"
	"github.com/mtfuller/go-finjournal/modules/sinks/webhook"
	"github.com/mtfuller/go-finjournal/pkg/commit"
	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/module"
	"github.com/mtfuller/go-finjournal/pkg/storage/memory"
)

func TestCardCaptureWithFeesRulesWorkedExample(t *testing.T) {
	// RFC Section 7 & 10 worked example:
	// 10.01 USD (1001 minor units) card_capture with 2.9% + 0.30 USD fee (cap 25.00 USD)
	// -> fee = 29 + 30 = 59 minor units (0.59 USD), net credit to Alice = 9.42 USD (942).
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

func TestFXConvertWorkedExampleAndQuoteExpiry(t *testing.T) {
	// RFC Section 8 worked example:
	// Alice converts 100.00 USD (10000) to EUR at mid rate 0.9213, customer spread 0.5%.
	// Customer rate = 0.9213 * 0.995 -> 91.67 EUR (9167) to Alice, 0.46 EUR (46) to revenue:fx:spread:eur.
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

	// Fund Alice with 100.00 USD
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

	// Issue a quote and convert 100.00 USD -> EUR
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

	// Advance clock past quote expiry (31s) and verify executing the expired quote fails with 422 module_rejected: quote_expired
	clock.Advance(31 * time.Second)
	_, err = eng.Post(ctx, ledger.Intent{
		Ledger:         "main",
		Kind:           "fx_convert",
		IdempotencyKey: "fx-expired",
		Postings: []ledger.Posting{{
			Debit:  aliceUSD.ID,
			Credit: "system:fx:position:usd",
			Amount: money.FromInt64(1_000),
			Asset:  "USD/2",
		}},
		Params: ledger.Metadata{
			"fx.to_account": string(aliceEUR.ID),
			"fx.quote_id":   quote.ID,
		},
	})
	var lErr *ledger.Error
	if !errors.As(err, &lErr) || lErr.Code != ledger.CodeModuleRejected || lErr.Details["reason"] != "quote_expired" {
		t.Fatalf("expected module_rejected quote_expired, got %v", err)
	}
}

func TestBatchedCommitterPerIntentIsolation(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	eng, err := ledger.New(store, ledger.WithBatchCommitter(commit.BatchConfig{
		MaxBatch:    50,
		MaxWait:     5 * time.Millisecond,
		LockTimeout: 100 * time.Millisecond,
	}))
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	defer func() { _ = eng.Close(ctx) }()

	_, _ = eng.CreateLedger(ctx, "main", "Main", nil)
	_, _ = eng.RegisterAsset(ctx, "main", "USD", 2)

	zero := money.Zero
	bank, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "assets:bank:usd", Asset: "USD/2", Normal: ledger.Debit})
	alice, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main", Code: "liabilities:wallets:alice:usd", Asset: "USD/2", Normal: ledger.Credit,
		Limits: ledger.Limits{Min: &zero},
	})
	bob, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main", Code: "liabilities:wallets:bob:usd", Asset: "USD/2", Normal: ledger.Credit,
		Limits: ledger.Limits{Min: &zero},
	})

	// Fund Alice with 50.00 USD (5000)
	_, err = eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "deposit", IdempotencyKey: "fund-alice",
		Postings: []ledger.Posting{{Debit: bank.ID, Credit: alice.ID, Amount: money.FromInt64(5_000), Asset: "USD/2"}},
	})
	if err != nil {
		t.Fatalf("fund-alice: %v", err)
	}

	// Submit 3 concurrent transfers in the same batch window:
	// 1. 20.00 USD (valid)
	// 2. 999.00 USD (invalid: insufficient_funds - must NOT fail neighbours!)
	// 3. 15.00 USD (valid)
	var wg sync.WaitGroup
	errs := make([]error, 3)
	amts := []int64{2_000, 99_900, 1_500}
	keys := []string{"b-1", "b-bad", "b-2"}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = eng.Post(ctx, ledger.Intent{
				Ledger:         "main",
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

	if errs[0] != nil {
		t.Fatalf("expected transfer 0 to succeed, got %v", errs[0])
	}
	if errs[1] == nil {
		t.Fatalf("expected transfer 1 (overdraft) to fail")
	}
	if errs[2] != nil {
		t.Fatalf("expected transfer 2 to succeed despite neighbour failure, got %v", errs[2])
	}

	bobBal, _ := store.Balance(ctx, bob.ID)
	if bobBal.Posted(bob.Normal).Cmp(money.FromInt64(3_500)) != 0 {
		t.Fatalf("expected Bob balance 3500, got %s", bobBal.Posted(bob.Normal))
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestOutboxRelayWebhookNATSAndRecon(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	var receivedSig string
	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			receivedSig = r.Header.Get("X-Finjournal-Signature")
			rec := httptest.NewRecorder()
			rec.WriteHeader(http.StatusOK)
			return rec.Result(), nil
		}),
	}

	whMod := webhook.New(webhook.Config{
		Endpoint: "http://webhook.internal/events",
		Secret:   "whsec_test",
	}, httpClient)
	natsPub := &nats.MemoryPublisher{}
	natsMod := nats.New(nats.Config{SubjectPrefix: "finjournal"}, natsPub)
	reconMod := recon.New()

	eng, err := ledger.New(store, ledger.WithModules(whMod, natsMod, reconMod))
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

	// Relay outbox events to webhook, nats, and recon subscribers
	if _, err := eng.RelayOnce(ctx); err != nil {
		t.Fatalf("RelayOnce: %v", err)
	}
	if !strings.HasPrefix(receivedSig, "sha256=") {
		t.Fatalf("expected webhook signature, got %q", receivedSig)
	}
	if len(natsPub.Messages) == 0 {
		t.Fatalf("expected NATS messages to be published")
	}

	// Import CSV statement into recon module and verify match
	csvData := "id,reference,amount,value_date\nstmt_1,WIRE-101,250.00,2026-11-06\n"
	results, err := reconMod.ImportCSV(ctx, "bank:operating-usd", "USD/2", strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("ImportCSV: %v", err)
	}
	if len(results) != 1 || results[0].Status != recon.StatusMatched || results[0].Expected[0] != tx.ID {
		t.Fatalf("unexpected recon results: %+v", results)
	}
}

// Ensure custom FeeEngine works seamlessly via WithModules (RFC Section 9 Example 2)
type tieredFeeEngine struct {
	revenue ledger.AccountID
}

func (e *tieredFeeEngine) Name() string                { return "acme/tiered-fees" }
func (e *tieredFeeEngine) Matches(kind string) bool    { return kind == "p2p_transfer" }
func (e *tieredFeeEngine) Close(context.Context) error { return nil }
func (e *tieredFeeEngine) Init(ctx context.Context, h module.Host) error {
	id, err := h.EnsureSystemAccount(ctx, "main", "revenue:fees:p2p:usd", "USD/2", ledger.Deferred)
	e.revenue = id
	return err
}
func (e *tieredFeeEngine) Fees(_ context.Context, req module.FeeRequest) ([]module.Fee, error) {
	p := req.Postings[0]
	rate := money.Rate{Mantissa: money.FromInt64(10), Scale: 3} // 1.0%
	if p.Amount.Cmp(money.FromInt64(100_000)) >= 0 {
		rate = money.Rate{Mantissa: money.FromInt64(5), Scale: 3} // 0.5%
	}
	fee, _, err := money.MulRate(p.Amount, rate, money.HalfEven)
	if err != nil || fee.IsZero() {
		return nil, err
	}
	return []module.Fee{{Payer: p.Debit, Payee: e.revenue, Amount: fee, Asset: p.Asset, Code: "p2p_pct"}}, nil
}

func TestTieredFeeEngineNetOverdraftProtection(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	eng, err := ledger.New(store, ledger.WithModules(&tieredFeeEngine{}))
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	zero := money.Zero
	bank, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "assets:bank:usd", Asset: "USD/2", Normal: ledger.Debit})
	alice, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main", Code: "liabilities:wallets:alice:usd", Asset: "USD/2", Normal: ledger.Credit,
		Limits: ledger.Limits{Min: &zero},
	})
	bob, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main", Code: "liabilities:wallets:bob:usd", Asset: "USD/2", Normal: ledger.Credit,
		Limits: ledger.Limits{Min: &zero},
	})

	// Fund Alice with 100.00 USD (10000)
	_, _ = eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "deposit", IdempotencyKey: "fund",
		Postings: []ledger.Posting{{Debit: bank.ID, Credit: alice.ID, Amount: money.FromInt64(10_000), Asset: "USD/2"}},
	})

	// Attempting to P2P transfer 100.00 USD (which incurs a 1.00 USD fee on sender Alice) must fail
	// because net debit is 101.00 USD > 100.00 USD balance!
	_, err = eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "p2p_transfer", IdempotencyKey: "p2p-overdraft",
		Postings: []ledger.Posting{{Debit: alice.ID, Credit: bob.ID, Amount: money.FromInt64(10_000), Asset: "USD/2"}},
	})
	if err == nil {
		t.Fatalf("expected p2p transfer of entire balance to fail due to fee overdraft")
	}
}

func TestArchitectureDependencyBoundaries(t *testing.T) {
	// RFC Section 2: pkg/ledger never imports a concrete storage adapter, server, or any official module.
	forbiddenPrefixes := []string{
		"github.com/mtfuller/go-finjournal/pkg/storage/memory",
		"github.com/mtfuller/go-finjournal/pkg/storage/sqlite",
		"github.com/mtfuller/go-finjournal/pkg/storage/postgres",
		"github.com/mtfuller/go-finjournal/server",
		"github.com/mtfuller/go-finjournal/modules/",
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("ParseFile(%s): %v", name, err)
		}
		for _, imp := range f.Imports {
			impPath := strings.Trim(imp.Path.Value, `"`)
			for _, forbidden := range forbiddenPrefixes {
				if strings.HasPrefix(impPath, forbidden) {
					t.Fatalf("pkg/ledger file %s imports forbidden package %s", name, impPath)
				}
			}
		}
	}
}
