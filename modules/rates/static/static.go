// Package static implements the official rates/static module providing fixed/configured
// FX rates, two-step expiring quotes, and the fx_convert Enricher through position accounts.
package static

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/module"
)

// PairConfig defines a static FX rate and optional customer spread percentage.
type PairConfig struct {
	Base          money.AssetID `json:"base" yaml:"base"`                       // e.g. "USD/2"
	Quote         money.AssetID `json:"quote" yaml:"quote"`                     // e.g. "EUR/2"
	Mid           string        `json:"mid" yaml:"mid"`                         // e.g. "0.9213"
	SpreadPercent string        `json:"spread_percent" yaml:"spread_percent"`   // e.g. "0.5" (0.5% spread)
	Source        string        `json:"source" yaml:"source"`                   // default "static"
}

// Config configures the rates/static module.
type Config struct {
	QuoteTTL time.Duration `json:"quote_ttl" yaml:"quote_ttl"` // default 30s
	Pairs    []PairConfig  `json:"pairs" yaml:"pairs"`
}

type pairKey struct {
	base  money.AssetID
	quote money.AssetID
}

type compiledPair struct {
	base     money.AssetID
	quote    money.AssetID
	mid      money.Rate
	customer money.Rate
	source   string
}

// Module implements module.RateProvider and module.Enricher for "fx_convert".
type Module struct {
	cfg     Config
	host    module.Host
	pairs   map[pairKey]compiledPair
	mu      sync.RWMutex
	quotes  map[string]module.Quote
	qSeq    uint64
}

// New creates a rates/static Module from Config.
func New(cfg Config) (*Module, error) {
	if cfg.QuoteTTL <= 0 {
		cfg.QuoteTTL = 30 * time.Second
	}
	m := &Module{
		cfg:    cfg,
		pairs:  make(map[pairKey]compiledPair),
		quotes: make(map[string]module.Quote),
	}
	if err := m.compile(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Module) Name() string {
	return "fx"
}

func (m *Module) Init(_ context.Context, h module.Host) error {
	m.host = h
	if len(m.cfg.Pairs) == 0 {
		if err := h.DecodeConfig(&m.cfg); err != nil {
			return err
		}
		if m.cfg.QuoteTTL <= 0 {
			m.cfg.QuoteTTL = 30 * time.Second
		}
		if err := m.compile(); err != nil {
			return err
		}
	}
	return nil
}

func (m *Module) Close(_ context.Context) error {
	return nil
}

func (m *Module) Matches(kind string) bool {
	return kind == "fx_convert"
}

// Quote issues and stores a Quote for base -> quote at time `at`.
func (m *Module) Quote(_ context.Context, base, quote money.AssetID, at time.Time) (module.Quote, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp, ok := m.pairs[pairKey{base: base, quote: quote}]
	if !ok {
		return module.Quote{}, fmt.Errorf("no static rate for %s -> %s", base, quote)
	}
	m.qSeq++
	id := fmt.Sprintf("q_%06d", m.qSeq)
	q := module.Quote{
		ID:         id,
		Base:       base,
		Quote:      quote,
		Mid:        cp.mid,
		Customer:   cp.customer,
		Source:     cp.source,
		ObservedAt: at.UTC(),
		ExpiresAt:  at.UTC().Add(m.cfg.QuoteTTL),
	}
	m.quotes[id] = q
	return q, nil
}

// GetQuote retrieves a previously issued Quote by ID.
func (m *Module) GetQuote(_ context.Context, quoteID string) (module.Quote, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.quotes[quoteID]
	if !ok {
		return module.Quote{}, fmt.Errorf("quote %q not found", quoteID)
	}
	return q, nil
}

// Enrich turns an "fx_convert" intent into balanced same-asset postings through FX position
// accounts and records rate provenance (RFC section 8).
func (m *Module) Enrich(ctx context.Context, d *module.Draft) error {
	intent := d.Intent()
	postings := d.Postings()
	if len(postings) != 1 {
		return d.Reject("invalid_fx_intent", "fx_convert intent must supply exactly 1 base posting")
	}
	leg1 := postings[0]
	toAcctParam := intent.Params["fx.to_account"]
	if toAcctParam == "" {
		return d.Reject("missing_to_account", "fx.to_account parameter is required")
	}
	toAcctID, err := m.host.ResolveAccount(ctx, intent.Ledger, toAcctParam)
	if err != nil {
		return err
	}
	accts, err := m.host.Reader().Accounts(ctx, []core.AccountID{toAcctID})
	if err != nil {
		return err
	}
	toAcct := accts[0]
	baseAsset, err := money.ParseAsset(leg1.Asset)
	if err != nil {
		return err
	}
	quoteAsset, err := money.ParseAsset(toAcct.Asset)
	if err != nil {
		return err
	}

	now := m.host.Clock().Now()
	var q module.Quote
	if quoteID := intent.Params["fx.quote_id"]; quoteID != "" {
		q, err = m.GetQuote(ctx, quoteID)
		if err != nil {
			return d.Reject("quote_not_found", err.Error())
		}
		if now.After(q.ExpiresAt) {
			return d.Reject("quote_expired", fmt.Sprintf("quote %s expired at %s", q.ID, q.ExpiresAt.Format(time.RFC3339)))
		}
	} else {
		q, err = m.Quote(ctx, leg1.Asset, toAcct.Asset, now)
		if err != nil {
			return err
		}
	}

	// Rescale base amount to quote asset's scale before applying mid and customer rates
	scaledBase, _, err := money.Rescale(leg1.Amount, baseAsset.Scale, quoteAsset.Scale, money.HalfEven)
	if err != nil {
		return err
	}
	midQuoteAmt, _, err := money.MulRate(scaledBase, q.Mid, money.HalfEven)
	if err != nil {
		return err
	}
	custQuoteAmt, _, err := money.MulRate(scaledBase, q.Customer, money.HalfEven)
	if err != nil {
		return err
	}
	spreadAmt, err := midQuoteAmt.Sub(custQuoteAmt)
	if err != nil {
		return err
	}

	quoteCodeLower := strings.ToLower(quoteAsset.Code)
	posQuoteCode := fmt.Sprintf("system:fx:position:%s", quoteCodeLower)
	posQuoteID, err := m.host.EnsureSystemAccount(ctx, intent.Ledger, posQuoteCode, toAcct.Asset, core.Deferred)
	if err != nil {
		return err
	}

	// Leg 2: system:fx:position:<quote> -> customer quote account
	d.AddPosting(core.Posting{
		Debit:  posQuoteID,
		Credit: toAcct.ID,
		Amount: custQuoteAmt,
		Asset:  toAcct.Asset,
		Memo:   "fx conversion proceeds",
	})

	// Leg 3: system:fx:position:<quote> -> revenue:fx:spread:<quote> (if spread > 0)
	if spreadAmt.IsPositive() {
		spreadCode := fmt.Sprintf("revenue:fx:spread:%s", quoteCodeLower)
		spreadID, err := m.host.EnsureSystemAccount(ctx, intent.Ledger, spreadCode, toAcct.Asset, core.Deferred)
		if err != nil {
			return err
		}
		d.AddPosting(core.Posting{
			Debit:  posQuoteID,
			Credit: spreadID,
			Amount: spreadAmt,
			Asset:  toAcct.Asset,
			Memo:   "fx spread revenue",
		})
	}

	d.SetMeta("quote_id", q.ID)
	d.SetMeta("mid", q.Mid.String())
	d.SetMeta("customer_rate", q.Customer.String())
	d.SetMeta("source", q.Source)
	return nil
}

func (m *Module) compile() error {
	for _, p := range m.cfg.Pairs {
		mid, err := money.ParseRate(p.Mid)
		if err != nil {
			return fmt.Errorf("pair %s->%s mid: %w", p.Base, p.Quote, err)
		}
		cust := mid
		if p.SpreadPercent != "" && p.SpreadPercent != "0" {
			// customer_rate = mid * (1 - spread_percent/100)
			spRate, err := money.ParseRate(p.SpreadPercent)
			if err != nil {
				return fmt.Errorf("pair %s->%s spread: %w", p.Base, p.Quote, err)
			}
			spRate.Scale += 2
			// Exact product: mid.Mantissa * (10^spRate.Scale - spRate.Mantissa) * 10^-(mid.Scale + spRate.Scale)
			oneScaled, _, err := money.Rescale(money.FromInt64(1), 0, spRate.Scale, money.HalfEven)
			if err != nil {
				return err
			}
			factor, err := oneScaled.Sub(spRate.Mantissa)
			if err != nil {
				return err
			}
			prodBig := mid.Mantissa.BigInt()
			prodBig.Mul(prodBig, factor.BigInt())
			newMantissa, err := money.FromBigInt(prodBig)
			if err != nil {
				return err
			}
			cust = money.Rate{
				Mantissa: newMantissa,
				Scale:    mid.Scale + spRate.Scale,
			}
		}
		src := p.Source
		if src == "" {
			src = "static"
		}
		m.pairs[pairKey{base: p.Base, quote: p.Quote}] = compiledPair{
			base:     p.Base,
			quote:    p.Quote,
			mid:      mid,
			customer: cust,
			source:   src,
		}
	}
	return nil
}
