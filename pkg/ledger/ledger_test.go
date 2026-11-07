package ledger_test

import (
	"context"
	"errors"
	"go/parser"
	"go/token"
	"log/slog"
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
	"github.com/mtfuller/go-finjournal/pkg/storage"
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

func TestHoldsVoidSettleExpireAndReversals(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	clock := ledger.NewManualClock(time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC))
	eng, err := ledger.New(store,
		ledger.WithClock(clock),
		ledger.WithIDGenerator(ledger.NewDeterministicIDGenerator(99)),
		ledger.WithRoundingMode(money.HalfEven),
		ledger.WithLogger(slog.Default()),
		ledger.WithHookTimeouts(100*time.Millisecond, 40*time.Millisecond),
		ledger.WithCommitter(commit.NewDirect(store)),
	)
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	defer func() { _ = eng.Close(ctx) }()

	if eng.Store() != store || eng.Clock() != clock || eng.RoundingMode() != money.HalfEven {
		t.Fatalf("unexpected engine accessors")
	}

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

	// Fund Alice with 200.00 USD (20,000)
	_, err = eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "deposit", IdempotencyKey: "fund-1",
		Postings: []ledger.Posting{{Debit: bank.ID, Credit: alice.ID, Amount: money.FromInt64(20_000), Asset: "USD/2"}},
	})
	if err != nil {
		t.Fatalf("deposit: %v", err)
	}

	// 1. Place Hold 1 (50.00 USD) and settle partially (35.00 USD)
	hold1, err := eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "auth", Pending: true, IdempotencyKey: "hold-1",
		Postings: []ledger.Posting{{Debit: alice.ID, Credit: bob.ID, Amount: money.FromInt64(5_000), Asset: "USD/2"}},
	})
	if err != nil {
		t.Fatalf("hold-1: %v", err)
	}

	// SettleHold validation errors
	if _, err := eng.SettleHold(ctx, hold1.ID, "", nil); err == nil {
		t.Fatalf("expected error on empty idempotency_key")
	}
	negAmt := money.FromInt64(-10)
	if _, err := eng.SettleHold(ctx, hold1.ID, "bad-settle-neg", &negAmt); err == nil {
		t.Fatalf("expected error on negative settle amount")
	}
	tooMuch := money.FromInt64(9_000)
	if _, err := eng.SettleHold(ctx, hold1.ID, "bad-settle-over", &tooMuch); err == nil {
		t.Fatalf("expected error on settle amount > held amount")
	}

	settleAmt := money.FromInt64(3_500)
	settled1, err := eng.SettleHold(ctx, hold1.ID, "settle-1", &settleAmt)
	if err != nil {
		t.Fatalf("SettleHold: %v", err)
	}
	// Idempotent replay of SettleHold
	replaySettle, err := eng.SettleHold(ctx, hold1.ID, "settle-1", &settleAmt)
	if err != nil || !replaySettle.IdempotentReplay || replaySettle.ID != settled1.ID {
		t.Fatalf("expected idempotent replay of SettleHold, got %+v (%v)", replaySettle, err)
	}
	// Conflict on SettleHold with different amount
	otherAmt := money.FromInt64(3_000)
	if _, err := eng.SettleHold(ctx, hold1.ID, "settle-1", &otherAmt); err == nil {
		t.Fatalf("expected idempotency conflict on SettleHold")
	}

	// 2. Place Hold 2 (20.00 USD) and VoidHold
	hold2, err := eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "auth", Pending: true, IdempotencyKey: "hold-2",
		Postings: []ledger.Posting{{Debit: alice.ID, Credit: bob.ID, Amount: money.FromInt64(2_000), Asset: "USD/2"}},
	})
	if err != nil {
		t.Fatalf("hold-2: %v", err)
	}
	if _, err := eng.VoidHold(ctx, hold2.ID, ""); err == nil {
		t.Fatalf("expected error on empty VoidHold key")
	}
	voided, err := eng.VoidHold(ctx, hold2.ID, "void-2")
	if err != nil || voided.Status != ledger.StatusVoided {
		t.Fatalf("VoidHold: %+v, %v", voided, err)
	}
	// Idempotent replay of VoidHold
	replayVoid, err := eng.VoidHold(ctx, hold2.ID, "void-2")
	if err != nil || !replayVoid.IdempotentReplay {
		t.Fatalf("expected VoidHold replay, got %+v (%v)", replayVoid, err)
	}
	// Cannot settle a voided hold
	if _, err := eng.SettleHold(ctx, hold2.ID, "settle-voided", nil); err == nil {
		t.Fatalf("expected error settling voided hold")
	}

	// 3. Place Hold 3 with ExpiresAt and test SweepExpiredHolds
	exp := clock.Now().Add(5 * time.Minute)
	hold3, err := eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "auth", Pending: true, ExpiresAt: &exp, IdempotencyKey: "hold-3",
		Postings: []ledger.Posting{{Debit: alice.ID, Credit: bob.ID, Amount: money.FromInt64(1_500), Asset: "USD/2"}},
	})
	if err != nil {
		t.Fatalf("hold-3: %v", err)
	}
	clock.Advance(6 * time.Minute)
	swept, err := eng.SweepExpiredHolds(ctx)
	if err != nil || swept != 1 {
		t.Fatalf("expected 1 swept hold, got %d (%v)", swept, err)
	}
	h3After, _ := store.Transaction(ctx, hold3.ID)
	if h3After.Status != ledger.StatusExpired {
		t.Fatalf("expected hold-3 status expired, got %s", h3After.Status)
	}

	// 4. Partial Reversals on settled1 (3,500 minor units: alice -> bob)
	if _, err := eng.Reverse(ctx, settled1.ID, "", nil); err == nil {
		t.Fatalf("expected error on empty Reverse key")
	}
	partialLeg := []ledger.Posting{{Debit: bob.ID, Credit: alice.ID, Amount: money.FromInt64(1_500), Asset: "USD/2"}}
	rev1, err := eng.Reverse(ctx, settled1.ID, "rev-part-1", partialLeg)
	if err != nil {
		t.Fatalf("partial Reverse 1: %v", err)
	}
	// Replay of partial Reverse 1
	rev1Replay, err := eng.Reverse(ctx, settled1.ID, "rev-part-1", partialLeg)
	if err != nil || !rev1Replay.IdempotentReplay || rev1Replay.ID != rev1.ID {
		t.Fatalf("expected replay of Reverse, got %+v (%v)", rev1Replay, err)
	}
	// Conflict on Reverse with different amount
	diffLeg := []ledger.Posting{{Debit: bob.ID, Credit: alice.ID, Amount: money.FromInt64(1_000), Asset: "USD/2"}}
	if _, err := eng.Reverse(ctx, settled1.ID, "rev-part-1", diffLeg); err == nil {
		t.Fatalf("expected idempotency conflict on Reverse")
	}
	// Over-reversal (2,500 > remaining 2,000) must fail
	overLeg := []ledger.Posting{{Debit: bob.ID, Credit: alice.ID, Amount: money.FromInt64(2_500), Asset: "USD/2"}}
	if _, err := eng.Reverse(ctx, settled1.ID, "rev-too-much", overLeg); err == nil {
		t.Fatalf("expected over-reversal to fail")
	}
	// Reverse the remaining 2,000 on settled1, and full Reverse on fundTx
	remLeg := []ledger.Posting{{Debit: bob.ID, Credit: alice.ID, Amount: money.FromInt64(2_000), Asset: "USD/2"}}
	if _, err := eng.Reverse(ctx, settled1.ID, "rev-rest", remLeg); err != nil {
		t.Fatalf("Reverse remaining: %v", err)
	}
	txExtra, _ := eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "deposit", IdempotencyKey: "fund-2",
		Postings: []ledger.Posting{{Debit: bank.ID, Credit: alice.ID, Amount: money.FromInt64(500), Asset: "USD/2"}},
	})
	if _, err := eng.Reverse(ctx, txExtra.ID, "rev-full-extra", nil); err != nil {
		t.Fatalf("Reverse full: %v", err)
	}
}

type staticRateMod struct{}

func (m *staticRateMod) Name() string                            { return "test/rates" }
func (m *staticRateMod) Init(context.Context, module.Host) error { return nil }
func (m *staticRateMod) Close(context.Context) error             { return nil }
func (m *staticRateMod) Quote(_ context.Context, base, quote money.AssetID, _ time.Time) (module.Quote, error) {
	if base == "EUR/2" && quote == "USD/2" {
		return module.Quote{
			ID:       "q-1",
			Base:     base,
			Quote:    quote,
			Mid:      money.Rate{Mantissa: money.FromInt64(110), Scale: 2}, // 1.10
			Customer: money.Rate{Mantissa: money.FromInt64(110), Scale: 2},
		}, nil
	}
	return module.Quote{}, errors.New("unsupported pair")
}
func (m *staticRateMod) GetQuote(_ context.Context, id string) (module.Quote, error) {
	return module.Quote{}, errors.New("not found")
}

type slowOptionalEnricher struct {
	decoded map[string]any
}

func (m *slowOptionalEnricher) Name() string                { return "test/optional-slow" }
func (m *slowOptionalEnricher) Matches(string) bool         { return true }
func (m *slowOptionalEnricher) Close(context.Context) error { return nil }
func (m *slowOptionalEnricher) Init(_ context.Context, h module.Host) error {
	_ = h.Reader()
	_ = h.Logger()
	_ = h.Clock()
	_ = h.Migrate(context.Background(), nil)
	return h.DecodeConfig(&m.decoded)
}
func (m *slowOptionalEnricher) Enrich(ctx context.Context, _ *module.Draft) error {
	return context.DeadlineExceeded
}

type customValidatorMod struct{}

func (m *customValidatorMod) Name() string                            { return "test/validator" }
func (m *customValidatorMod) Init(context.Context, module.Host) error { return nil }
func (m *customValidatorMod) Close(context.Context) error             { return nil }
func (m *customValidatorMod) Validate(_ context.Context, d *module.Draft) error {
	if d.Intent().Kind == "restricted" {
		return d.Reject("restricted_kind", "restricted kind denied")
	}
	return nil
}

func TestAccountLifecyclePeriodsHolderBalancesAndOptionalModule(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	clock := ledger.NewManualClock(time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC))
	optMod := &slowOptionalEnricher{}

	eng, err := ledger.New(store,
		ledger.WithClock(clock),
		ledger.WithModules(&staticRateMod{}, &customValidatorMod{}),
		ledger.WithOptionalModule(optMod),
		ledger.WithModuleConfigs(map[string]any{
			"test/optional-slow": map[string]any{"key": "val"},
		}),
	)
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	defer func() { _ = eng.Close(ctx) }()

	if optMod.decoded["key"] != "val" {
		t.Fatalf("expected DecodeConfig to populate map, got %+v", optMod.decoded)
	}

	// Test Principal RBAC & DefaultAllow
	readOnlyPrincipal := module.Principal{ID: "reader", Scopes: []string{"ledger:read"}}
	adminPrincipal := module.Principal{ID: "admin", Scopes: []string{"admin"}, Ledgers: []ledger.LedgerID{"main"}}

	_, _ = eng.CreateLedger(ctx, "main", "Main", nil)
	_, _ = eng.RegisterAsset(ctx, "main", "USD", 2)
	_, _ = eng.RegisterAsset(ctx, "main", "EUR", 2)

	holder, _ := eng.CreateHolder(ctx, ledger.AccountHolder{Ledger: "main", Kind: "customer", ExternalRef: "cust_1"})
	bankUSD, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "assets:bank:usd", Asset: "USD/2", Normal: ledger.Debit})
	bankEUR, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "assets:bank:eur", Asset: "EUR/2", Normal: ledger.Debit})
	defAcct, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main", Code: "revenue:misc:usd", Asset: "USD/2", Normal: ledger.Credit, BalanceMode: ledger.Deferred,
	})

	zero := money.Zero
	maxCap := money.FromInt64(50_000)
	wUSD, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main", Holder: &holder.ID, Code: "liabilities:w:usd", Asset: "USD/2", Normal: ledger.Credit,
		Limits: ledger.Limits{Min: &zero, Max: &maxCap},
	})
	wEUR, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main", Holder: &holder.ID, Code: "liabilities:w:eur", Asset: "EUR/2", Normal: ledger.Credit,
		Limits: ledger.Limits{Min: &zero},
	})

	// Unauthorized principal must fail with 403 forbidden
	if _, err := eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "deposit", IdempotencyKey: "unauth-1",
		Postings: []ledger.Posting{{Debit: bankUSD.ID, Credit: wUSD.ID, Amount: money.FromInt64(1_000), Asset: "USD/2"}},
	}, ledger.WithPrincipal(readOnlyPrincipal)); err == nil {
		t.Fatalf("expected forbidden error for readOnlyPrincipal")
	}

	// Authorized admin principal succeeds, and optional slow module is skipped with metadata recorded
	tx1, err := eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "deposit", IdempotencyKey: "dep-usd",
		Postings: []ledger.Posting{
			{Debit: bankUSD.ID, Credit: wUSD.ID, Amount: money.FromInt64(10_000), Asset: "USD/2"},
			{Debit: bankUSD.ID, Credit: defAcct.ID, Amount: money.FromInt64(50), Asset: "USD/2"},
		},
	}, ledger.WithPrincipal(adminPrincipal))
	if err != nil {
		t.Fatalf("Post dep-usd: %v", err)
	}
	if tx1.Metadata["test/optional-slow.skipped"] == "" {
		t.Fatalf("expected test/optional-slow.skipped metadata, got %+v", tx1.Metadata)
	}

	// Fund EUR wallet with 100.00 EUR (10,000)
	_, err = eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "deposit", IdempotencyKey: "dep-eur",
		Postings: []ledger.Posting{{Debit: bankEUR.ID, Credit: wEUR.ID, Amount: money.FromInt64(10_000), Asset: "EUR/2"}},
	})
	if err != nil {
		t.Fatalf("Post dep-eur: %v", err)
	}

	// FoldDeferredOnce and SealOnce
	if folded, err := eng.FoldDeferredOnce(ctx, "main"); err != nil || folded == 0 {
		t.Fatalf("FoldDeferredOnce: %d, %v", folded, err)
	}
	if sealed, _, err := eng.SealOnce(ctx, "main"); err != nil || sealed == 0 {
		t.Fatalf("SealOnce: %d, %v", sealed, err)
	}

	// HolderBalances converted to USD/2 (EUR/2 -> ReportedPosted 11000, USD/2 -> ReportedPosted 10000)
	hbs, err := eng.HolderBalances(ctx, holder.ID, "USD/2")
	if err != nil || len(hbs) != 2 || hbs[0].ReportedPosted.Cmp(money.FromInt64(11_000)) != 0 || hbs[1].ReportedPosted.Cmp(money.FromInt64(10_000)) != 0 {
		t.Fatalf("unexpected converted HolderBalances: %+v (%v)", hbs, err)
	}

	// UpdateAccount: freeze wUSD -> inflows allowed, outflows rejected with 409 account_frozen
	frozen := ledger.AccountFrozen
	if _, err := eng.UpdateAccount(ctx, wUSD.ID, storage.AccountPatch{
		Status:   &frozen,
		Metadata: ledger.Metadata{"tier": "vip"},
	}); err != nil {
		t.Fatalf("UpdateAccount frozen: %v", err)
	}
	// Outflow from frozen wUSD fails
	if _, err := eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "withdrawal", IdempotencyKey: "frozen-out",
		Postings: []ledger.Posting{{Debit: wUSD.ID, Credit: bankUSD.ID, Amount: money.FromInt64(100), Asset: "USD/2"}},
	}); err == nil {
		t.Fatalf("expected frozen account outflow to be rejected")
	}
	// Close wUSD -> all postings rejected with 409 account_closed
	closed := ledger.AccountClosed
	_, _ = eng.UpdateAccount(ctx, wUSD.ID, storage.AccountPatch{Status: &closed})
	if _, err := eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "deposit", IdempotencyKey: "closed-in",
		Postings: []ledger.Posting{{Debit: bankUSD.ID, Credit: wUSD.ID, Amount: money.FromInt64(100), Asset: "USD/2"}},
	}); err == nil {
		t.Fatalf("expected closed account inflow to be rejected")
	}
	// Re-open wUSD
	openSt := ledger.AccountOpen
	_, _ = eng.UpdateAccount(ctx, wUSD.ID, storage.AccountPatch{Status: &openSt})

	// ClosePeriod for October 2026 and verify backdated EffectiveAt in October is rejected with 409 period_closed
	octStart := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	if _, err := eng.ClosePeriod(ctx, "main", octStart); err != nil {
		t.Fatalf("ClosePeriod: %v", err)
	}
	octMid := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	if _, err := eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "deposit", EffectiveAt: &octMid, IdempotencyKey: "backdated-oct",
		Postings: []ledger.Posting{{Debit: bankUSD.ID, Credit: wUSD.ID, Amount: money.FromInt64(100), Asset: "USD/2"}},
	}); err == nil {
		t.Fatalf("expected period_closed error for backdated posting")
	}

	// Custom validator rejection
	if _, err := eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "restricted", IdempotencyKey: "val-reject",
		Postings: []ledger.Posting{{Debit: bankUSD.ID, Credit: wUSD.ID, Amount: money.FromInt64(100), Asset: "USD/2"}},
	}); err == nil {
		t.Fatalf("expected validator module rejection")
	}

	// Seal and verify TrialBalance & CheckInvariants
	_, _, _ = eng.SealOnce(ctx, "main")
	tb, err := ledger.TrialBalance(ctx, store, "main")
	if err != nil || len(tb) != 2 || !tb[0].Balanced || !tb[1].Balanced {
		t.Fatalf("unexpected TrialBalance: %+v (%v)", tb, err)
	}
	rep, err := ledger.CheckInvariants(ctx, store, "main")
	if err != nil || !rep.OK() {
		t.Fatalf("CheckInvariants: %+v (%v)", rep, err)
	}
}

type customAuthMod struct{}

func (m *customAuthMod) Name() string                            { return "test/auth" }
func (m *customAuthMod) Init(context.Context, module.Host) error { return nil }
func (m *customAuthMod) Close(context.Context) error             { return nil }
func (m *customAuthMod) Allow(_ context.Context, p module.Principal, action string, r module.Resource) error {
	if p.ID == "blocked" {
		return errors.New("blocked by custom authorizer")
	}
	return ledger.DefaultAllow(p, action, r)
}

type panicEnricher struct{}

func (m *panicEnricher) Name() string                { return "test/panic" }
func (m *panicEnricher) Matches(kind string) bool    { return kind == "panic_kind" }
func (m *panicEnricher) Close(context.Context) error { return nil }
func (m *panicEnricher) Init(ctx context.Context, h module.Host) error {
	// Call EnsureSystemAccount twice to cover both creation and already-exists lookup
	_, _ = h.EnsureSystemAccount(ctx, "main", "system:test:usd", "USD/2", ledger.Deferred)
	_, _ = h.EnsureSystemAccount(ctx, "main", "system:test:usd", "USD/2", ledger.Deferred)
	return nil
}
func (m *panicEnricher) Enrich(context.Context, *module.Draft) error {
	panic("simulated module panic")
}

func TestValidationBranchesAuthorizerAndInvariantViolations(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	_ = store.Admin().CreateLedger(ctx, ledger.Ledger{ID: "main", Name: "Main", CreatedAt: time.Now().UTC()})
	_ = store.Admin().RegisterAsset(ctx, "main", money.Asset{Code: "USD", Scale: 2})

	eng, err := ledger.New(store, ledger.WithModules(&customAuthMod{}, &panicEnricher{}))
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	defer func() { _ = eng.Close(ctx) }()

	// Admin validation branches
	if _, err := eng.CreateLedger(ctx, "", "Name", nil); err == nil {
		t.Fatalf("expected error on empty ledger id")
	}
	// Duplicate ledger -> already_exists
	if _, err := eng.CreateLedger(ctx, "main", "Main", nil); err == nil {
		t.Fatalf("expected error on duplicate ledger")
	}
	if _, err := eng.RegisterAsset(ctx, "main", "", 2); err == nil {
		t.Fatalf("expected error on empty asset code")
	}
	_, _ = eng.RegisterAsset(ctx, "main", "EUR", 2)

	if _, err := eng.CreateHolder(ctx, ledger.AccountHolder{}); err == nil {
		t.Fatalf("expected error on empty holder")
	}
	parentID := ledger.HolderID("parent")
	if _, err := eng.CreateHolder(ctx, ledger.AccountHolder{Ledger: "main", Kind: "c", ExternalRef: "r", ParentID: &parentID}); err == nil {
		t.Fatalf("expected error on non-nil ParentID in v1")
	}
	if _, err := eng.CreateAccount(ctx, ledger.Account{}); err == nil {
		t.Fatalf("expected error on empty account")
	}
	zero := money.Zero
	if _, err := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main", Code: "def:bad", Asset: "USD/2", Normal: ledger.Credit,
		BalanceMode: ledger.Deferred, Limits: ledger.Limits{Min: &zero},
	}); err == nil {
		t.Fatalf("expected error on deferred account with limits")
	}

	bank, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "assets:bank:usd", Asset: "USD/2", Normal: ledger.Debit})
	alice, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "liabilities:wallets:alice:usd", Asset: "USD/2", Normal: ledger.Credit})
	eurAcct, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "liabilities:wallets:alice:eur", Asset: "EUR/2", Normal: ledger.Credit})

	// Post validation errors: empty fields, self-transfer, zero amount, >MaxPostingAmount, unregistered asset, debit/credit asset mismatch, unknown account
	for _, badIntent := range []ledger.Intent{
		{},
		{Ledger: "main", Kind: "x"},
		{Ledger: "main", Kind: "x", IdempotencyKey: "k1"},
		{Ledger: "main", Kind: "x", IdempotencyKey: "k2", Postings: []ledger.Posting{{Debit: bank.ID, Credit: bank.ID, Amount: money.FromInt64(10), Asset: "USD/2"}}},
		{Ledger: "main", Kind: "x", IdempotencyKey: "k3", Postings: []ledger.Posting{{Debit: bank.ID, Credit: alice.ID, Amount: money.Zero, Asset: "USD/2"}}},
		{Ledger: "main", Kind: "x", IdempotencyKey: "k-max", Postings: []ledger.Posting{{Debit: bank.ID, Credit: alice.ID, Amount: money.MaxAmount, Asset: "USD/2"}}},
		{Ledger: "main", Kind: "x", IdempotencyKey: "k-unreg", Postings: []ledger.Posting{{Debit: bank.ID, Credit: alice.ID, Amount: money.FromInt64(10), Asset: "GBP/2"}}},
		{Ledger: "main", Kind: "x", IdempotencyKey: "k4", Postings: []ledger.Posting{{Debit: bank.ID, Credit: eurAcct.ID, Amount: money.FromInt64(10), Asset: "USD/2"}}},
		{Ledger: "main", Kind: "x", IdempotencyKey: "k4b", Postings: []ledger.Posting{{Debit: eurAcct.ID, Credit: alice.ID, Amount: money.FromInt64(10), Asset: "USD/2"}}},
		{Ledger: "main", Kind: "x", IdempotencyKey: "k5", Postings: []ledger.Posting{{Debit: "unknown:code", Credit: alice.ID, Amount: money.FromInt64(10), Asset: "USD/2"}}},
		{Ledger: "main", Kind: "panic_kind", IdempotencyKey: "k-panic", Postings: []ledger.Posting{{Debit: bank.ID, Credit: alice.ID, Amount: money.FromInt64(10), Asset: "USD/2"}}},
	} {
		if _, err := eng.Post(ctx, badIntent); err == nil {
			t.Fatalf("expected validation/panic error for %+v", badIntent)
		}
	}

	// Not-found errors on SettleHold, VoidHold, Reverse
	if _, err := eng.SettleHold(ctx, "missing-tx", "k", nil); err == nil {
		t.Fatalf("expected not_found on SettleHold")
	}
	if _, err := eng.VoidHold(ctx, "missing-tx", "k"); err == nil {
		t.Fatalf("expected not_found on VoidHold")
	}
	if _, err := eng.Reverse(ctx, "missing-tx", "k", nil); err == nil {
		t.Fatalf("expected not_found on Reverse")
	}

	// HolderBalances without RateProvider when reportIn is set
	holder, _ := eng.CreateHolder(ctx, ledger.AccountHolder{Ledger: "main", Kind: "customer", ExternalRef: "h1"})
	_, _ = eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Holder: &holder.ID, Code: "liabilities:wallets:h1:usd", Asset: "USD/2", Normal: ledger.Credit})
	if _, err := eng.HolderBalances(ctx, holder.ID, "INVALID"); err == nil {
		t.Fatalf("expected error on invalid reportIn asset")
	}
	if _, err := eng.HolderBalances(ctx, holder.ID, "EUR/2"); err == nil {
		t.Fatalf("expected error when no RateProvider is registered")
	}

	// Create a pending hold and test reversing a pending hold (must fail) + principal checks on SettleHold/VoidHold
	holdTx, _ := eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "hold", Pending: true, IdempotencyKey: "h-pend-1",
		Postings: []ledger.Posting{{Debit: bank.ID, Credit: alice.ID, Amount: money.FromInt64(200), Asset: "USD/2"}},
	})
	if _, err := eng.Reverse(ctx, holdTx.ID, "rev-pend", nil); err == nil {
		t.Fatalf("expected error reversing pending hold")
	}
	blockedP := module.Principal{ID: "blocked", Scopes: []string{"tx:post"}}
	if _, err := eng.SettleHold(ctx, holdTx.ID, "settle-blocked", nil, ledger.WithPrincipal(blockedP)); err == nil {
		t.Fatalf("expected SettleHold blocked by principal")
	}
	if _, err := eng.VoidHold(ctx, holdTx.ID, "void-blocked", ledger.WithPrincipal(blockedP)); err == nil {
		t.Fatalf("expected VoidHold blocked by principal")
	}

	// Post a settled tx and test Reverse with unmatched partial posting and blocked principal
	postedTx, _ := eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "deposit", IdempotencyKey: "dep-for-rev",
		Postings: []ledger.Posting{{Debit: bank.ID, Credit: alice.ID, Amount: money.FromInt64(300), Asset: "USD/2"}},
	})
	if _, err := eng.Reverse(ctx, postedTx.ID, "rev-unmatched", []ledger.Posting{
		{Debit: eurAcct.ID, Credit: alice.ID, Amount: money.FromInt64(100), Asset: "EUR/2"},
	}); err == nil {
		t.Fatalf("expected error on unmatched partial reversal posting")
	}
	if _, err := eng.Reverse(ctx, postedTx.ID, "rev-blocked", nil, ledger.WithPrincipal(blockedP)); err == nil {
		t.Fatalf("expected Reverse blocked by principal")
	}

	// Test DefaultAllow with AccountAllowlist, wrong ledger, and custom authorizer block
	for _, badP := range []module.Principal{
		{ID: "blocked", Scopes: []string{"tx:post"}},
		{ID: "wrong-ledger", Scopes: []string{"tx:post"}, Ledgers: []ledger.LedgerID{"other"}},
		{ID: "wrong-acct", Scopes: []string{"tx:post"}, AccountAllowlist: []string{"liabilities:wallets:*"}}, // bank is assets:bank:usd!
	} {
		if _, err := eng.Post(ctx, ledger.Intent{
			Ledger: "main", Kind: "deposit", IdempotencyKey: "auth-check",
			Postings: []ledger.Posting{{Debit: bank.ID, Credit: alice.ID, Amount: money.FromInt64(100), Asset: "USD/2"}},
		}, ledger.WithPrincipal(badP)); err == nil {
			t.Fatalf("expected principal %s to be denied", badP.ID)
		}
	}

	// Allowed with matching AccountAllowlist
	goodP := module.Principal{
		ID:               "good",
		Scopes:           []string{"tx:post"},
		Ledgers:          []ledger.LedgerID{"main"},
		AccountAllowlist: []string{"assets:*", "liabilities:wallets:*"},
	}
	if _, err := eng.Post(ctx, ledger.Intent{
		Ledger: "main", Kind: "deposit", IdempotencyKey: "auth-ok",
		Postings: []ledger.Posting{{Debit: bank.ID, Credit: alice.ID, Amount: money.FromInt64(100), Asset: "USD/2"}},
	}, ledger.WithPrincipal(goodP), ledger.WithinTx(nil)); err != nil {
		t.Fatalf("expected goodP to succeed: %v", err)
	}

	// Inject raw storage corruptions to verify CheckInvariants catches I2/I3/I4/I5/I7/I8
	if _, err := ledger.New(nil); err == nil {
		t.Fatalf("expected error on ledger.New(nil)")
	}
	if _, err := eng.UpdateAccount(ctx, alice.ID, storage.AccountPatch{Metadata: ledger.Metadata{"": "empty-key"}}); err == nil {
		t.Fatalf("expected error on invalid metadata key in UpdateAccount")
	}

	rawTx, _ := store.Begin(ctx)
	_ = rawTx.WriteBalances(ctx, []ledger.Balance{{
		Account:       alice.ID,
		Asset:         "USD/2",
		PostedDebits:  money.FromInt64(999),
		PostedCredits: money.Zero,
		Version:       5,
	}})
	_ = rawTx.SetTxState(ctx, []storage.TxState{{
		TxID:     postedTx.ID,
		Status:   ledger.StatusPosted,
		Reversed: []money.Amount{money.FromInt64(999_999)},
	}})
	badPostedAfter := money.FromInt64(777_777)
	_ = rawTx.InsertEntries(ctx, []ledger.Entry{{
		Transaction:  "extra-tx",
		PostingIndex: 0,
		Account:      alice.ID,
		Side:         ledger.Credit,
		Amount:       money.FromInt64(10),
		PostedAfter:  &badPostedAfter,
		Version:      0, // non-increasing version -> triggers I4
		EffectiveAt:  time.Now().UTC(),
		CommittedAt:  time.Now().UTC(),
	}})
	_ = rawTx.InsertTransactions(ctx, []*ledger.Transaction{{
		ID:             "bad-tx-i1-i6",
		Ledger:         "main",
		Kind:           "tampered",
		Status:         ledger.StatusPosted,
		IdempotencyKey: "dep-for-rev", // duplicate idempotency key -> triggers I6
		Postings: []ledger.Posting{{
			Debit:  bank.ID,
			Credit: alice.ID,
			Amount: money.Zero, // non-positive amount -> triggers I1
			Asset:  "USD/2",
		}},
		EffectiveAt: time.Now().UTC(),
		CommittedAt: time.Now().UTC(),
	}})
	_ = rawTx.Commit(ctx)

	rep, err := ledger.CheckInvariants(ctx, store, "main")
	if err != nil || rep.OK() {
		t.Fatalf("expected CheckInvariants to detect tampered balance/state/entry violations, got %+v (%v)", rep, err)
	}
}

func TestMonthlyInterestAccrualPayoutAndBackgroundFolder(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	eng, err := ledger.New(store, ledger.WithDeferredFoldInterval(10*time.Millisecond))
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	defer func() { _ = eng.Close(ctx) }()

	_, _ = eng.CreateLedger(ctx, "main", "Main", nil)
	_, _ = eng.RegisterAsset(ctx, "main", "USD", 6)
	_, _ = eng.RegisterAsset(ctx, "main", "USD", 2)

	// Verify automatic Deferred-mode system rounding accounts exist after RegisterAsset
	round6, err := store.AccountByCode(ctx, "main", "system:rounding:usd_6")
	if err != nil || round6.BalanceMode != ledger.Deferred {
		t.Fatalf("expected system:rounding:usd_6 Deferred account, got %+v (%v)", round6, err)
	}
	round2, err := store.AccountByCode(ctx, "main", "system:rounding:usd_2")
	if err != nil || round2.BalanceMode != ledger.Deferred {
		t.Fatalf("expected system:rounding:usd_2 Deferred account, got %+v (%v)", round2, err)
	}

	interestExpense, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main", Code: "expenses:interest:usd6", Asset: "USD/6", Normal: ledger.Debit,
	})
	aliceAccrued, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main", Code: "liabilities:interest-accrued:alice:usd6", Asset: "USD/6", Normal: ledger.Credit,
	})
	aliceWallet, _ := eng.CreateAccount(ctx, ledger.Account{
		Ledger: "main", Code: "liabilities:wallets:alice:usd", Asset: "USD/2", Normal: ledger.Credit,
	})

	// RFC Section 7 Worked Example: accrue 1.234567 USD/6 (1_234_567 minor units)
	_, err = eng.Post(ctx, ledger.Intent{
		Ledger:         "main",
		Kind:           "interest_accrual",
		IdempotencyKey: "accrue-m1",
		Postings: []ledger.Posting{{
			Debit:  interestExpense.ID,
			Credit: aliceAccrued.ID,
			Amount: money.FromInt64(1_234_567),
			Asset:  "USD/6",
		}},
	})
	if err != nil {
		t.Fatalf("accrue-m1: %v", err)
	}

	// Settle accrual with rounding routed to system:rounding:usd_6
	res, err := eng.SettleAccrual(ctx, ledger.AccrualSettleRequest{
		Ledger:         "main",
		IdempotencyKey: "payout-m1",
		FromAccount:    aliceAccrued.ID,
		ToAccount:      aliceWallet.ID,
	})
	if err != nil {
		t.Fatalf("SettleAccrual: %v", err)
	}
	if res.PayableAmount.Cmp(money.FromInt64(123)) != 0 {
		t.Fatalf("expected payable 123 (1.23 USD/2), got %s", res.PayableAmount)
	}
	if res.Remainder.Cmp(money.FromInt64(4567)) != 0 {
		t.Fatalf("expected remainder 4567 (0.004567 USD/6), got %s", res.Remainder)
	}
	if len(res.Transaction.Postings) != 3 {
		t.Fatalf("expected 3 postings in accrual settlement, got %d", len(res.Transaction.Postings))
	}

	// Wait briefly for the background 10ms deferred balance folder worker to fold system:rounding:usd_6
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		bal, _ := store.Balance(ctx, round6.ID)
		if bal.Posted(round6.Normal).Cmp(money.FromInt64(4567)) == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	r6Bal, _ := store.Balance(ctx, round6.ID)
	if r6Bal.Posted(round6.Normal).Cmp(money.FromInt64(4567)) != 0 {
		t.Fatalf("expected system:rounding:usd_6 balance 4567, got %s", r6Bal.Posted(round6.Normal))
	}
	accBal, _ := store.Balance(ctx, aliceAccrued.ID)
	if !accBal.Posted(aliceAccrued.Normal).IsZero() {
		t.Fatalf("expected aliceAccrued balance 0 after full payout, got %s", accBal.Posted(aliceAccrued.Normal))
	}
	walBal, _ := store.Balance(ctx, aliceWallet.ID)
	if walBal.Posted(aliceWallet.Normal).Cmp(money.FromInt64(123)) != 0 {
		t.Fatalf("expected aliceWallet balance 123, got %s", walBal.Posted(aliceWallet.Normal))
	}

	// Also verify CarryForward mode (leaves remainder in aliceAccrued instead of rounding account)
	_, err = eng.Post(ctx, ledger.Intent{
		Ledger:         "main",
		Kind:           "interest_accrual",
		IdempotencyKey: "accrue-m2",
		Postings: []ledger.Posting{{
			Debit:  interestExpense.ID,
			Credit: aliceAccrued.ID,
			Amount: money.FromInt64(2_509_876),
			Asset:  "USD/6",
		}},
	})
	if err != nil {
		t.Fatalf("accrue-m2: %v", err)
	}
	downMode := money.Down
	res2, err := eng.SettleAccrual(ctx, ledger.AccrualSettleRequest{
		Ledger:         "main",
		IdempotencyKey: "payout-m2",
		FromAccount:    aliceAccrued.ID,
		ToAccount:      aliceWallet.ID,
		CarryForward:   true,
		RoundingMode:   &downMode,
	})
	if err != nil {
		t.Fatalf("SettleAccrual CarryForward: %v", err)
	}
	if res2.PayableAmount.Cmp(money.FromInt64(250)) != 0 || res2.Remainder.Cmp(money.FromInt64(9876)) != 0 {
		t.Fatalf("unexpected CarryForward result: %+v", res2)
	}
	accBal2, _ := store.Balance(ctx, aliceAccrued.ID)
	if accBal2.Posted(aliceAccrued.Normal).Cmp(money.FromInt64(9876)) != 0 {
		t.Fatalf("expected carried-forward balance 9876 in aliceAccrued, got %s", accBal2.Posted(aliceAccrued.Normal))
	}

	rep, err := ledger.CheckInvariants(ctx, store, "main")
	if err != nil || !rep.OK() {
		t.Fatalf("CheckInvariants failed: %+v (%v)", rep, err)
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
