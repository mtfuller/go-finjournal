// Package rules implements the official fees/rules module providing versioned,
// YAML-configurable fee schedules (percentage + fixed + cap).
package rules

import (
	"context"
	"embed"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/module"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func init() {
	module.Register("fees/rules", func() (module.Module, error) {
		return New(Config{})
	})
}

// MatchCriteria defines when a Schedule applies to an Intent.
type MatchCriteria struct {
	Kind  string        `json:"kind" yaml:"kind"`
	Asset money.AssetID `json:"asset" yaml:"asset"`
}

// Schedule defines one versioned fee rule.
type Schedule struct {
	ID            string        `json:"id" yaml:"id"`
	Version       int           `json:"version" yaml:"version"`
	EffectiveFrom string        `json:"effective_from" yaml:"effective_from"` // YYYY-MM-DD or RFC3339
	Match         MatchCriteria `json:"match" yaml:"match"`
	Payer         string        `json:"payer" yaml:"payer"`   // "credit_of_posting_0", "debit_of_posting_0", or account code
	Payee         string        `json:"payee" yaml:"payee"`   // e.g. "revenue:fees:card:usd"
	Percent       string        `json:"percent" yaml:"percent"` // e.g. "2.9" (meaning 2.9%)
	Fixed         string        `json:"fixed" yaml:"fixed"`     // e.g. "0.30"
	Cap           string        `json:"cap" yaml:"cap"`         // e.g. "25.00"
}

// Config holds the configuration for the fees/rules module.
type Config struct {
	Schedules []Schedule `json:"schedules" yaml:"schedules"`
}

type compiledSchedule struct {
	raw           Schedule
	effectiveFrom time.Time
	asset         money.Asset
	hasRate       bool
	rate          money.Rate
	hasFixed      bool
	fixed         money.Amount
	hasCap        bool
	capAmt        money.Amount
}

// Module is the fees/rules Enricher module.
type Module struct {
	cfg       Config
	schedules []compiledSchedule
	host      module.Host
}

// New creates a fees/rules Module from Config.
func New(cfg Config) (*Module, error) {
	m := &Module{cfg: cfg}
	if err := m.compile(); err != nil {
		return nil, err
	}
	return m, nil
}

// NewFromYAML parses a YAML config block and returns a fees/rules Module.
func NewFromYAML(data []byte) (*Module, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return New(cfg)
}

func (m *Module) Name() string {
	return "fees/rules"
}

func (m *Module) Init(ctx context.Context, h module.Host) error {
	m.host = h
	if err := h.Migrate(ctx, migrationsFS); err != nil {
		return err
	}
	if len(m.cfg.Schedules) == 0 {
		if err := h.DecodeConfig(&m.cfg); err != nil {
			return err
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
	for _, s := range m.schedules {
		if s.raw.Match.Kind == kind || s.raw.Match.Kind == "*" {
			return true
		}
	}
	return false
}

func (m *Module) Enrich(ctx context.Context, d *module.Draft) error {
	intent := d.Intent()
	postings := d.Postings()
	if len(postings) == 0 {
		return nil
	}
	eff := m.host.Clock().Now()
	if intent.EffectiveAt != nil {
		eff = *intent.EffectiveAt
	}

	basePosting := postings[0]
	// Find the latest effective schedule version matching (kind, asset, effective_from <= eff)
	var chosen *compiledSchedule
	for i := range m.schedules {
		cs := &m.schedules[i]
		if cs.raw.Match.Kind != intent.Kind && cs.raw.Match.Kind != "*" {
			continue
		}
		if cs.raw.Match.Asset != "" && cs.raw.Match.Asset != basePosting.Asset {
			continue
		}
		if !cs.effectiveFrom.IsZero() && eff.Before(cs.effectiveFrom) {
			continue
		}
		if chosen == nil || cs.raw.Version > chosen.raw.Version ||
			(cs.raw.Version == chosen.raw.Version && cs.effectiveFrom.After(chosen.effectiveFrom)) {
			chosen = cs
		}
	}
	if chosen == nil {
		return nil
	}

	feeAmt := money.Zero
	if chosen.hasRate {
		pctFee, _, err := money.MulRate(basePosting.Amount, chosen.rate, money.HalfEven)
		if err != nil {
			return err
		}
		feeAmt = pctFee
	}
	if chosen.hasFixed {
		var err error
		feeAmt, err = feeAmt.Add(chosen.fixed)
		if err != nil {
			return err
		}
	}
	if chosen.hasCap && feeAmt.Cmp(chosen.capAmt) > 0 {
		feeAmt = chosen.capAmt
	}
	if !feeAmt.IsPositive() {
		return nil
	}

	payerID, err := m.resolveParty(ctx, intent.Ledger, chosen.raw.Payer, postings)
	if err != nil {
		return err
	}
	payeeID, err := m.host.EnsureSystemAccount(ctx, intent.Ledger, chosen.raw.Payee, basePosting.Asset, core.Deferred)
	if err != nil {
		return err
	}

	d.AddPosting(core.Posting{
		Debit:  payerID,
		Credit: payeeID,
		Amount: feeAmt,
		Asset:  basePosting.Asset,
		Memo:   fmt.Sprintf("%s@%d", chosen.raw.ID, chosen.raw.Version),
	})
	d.SetMeta("schedule", fmt.Sprintf("%s@%d", chosen.raw.ID, chosen.raw.Version))
	return nil
}

func (m *Module) resolveParty(ctx context.Context, l core.LedgerID, spec string, postings []core.Posting) (core.AccountID, error) {
	if strings.HasPrefix(spec, "credit_of_posting_") {
		idxStr := strings.TrimPrefix(spec, "credit_of_posting_")
		idx, err := strconv.Atoi(idxStr)
		if err != nil || idx < 0 || idx >= len(postings) {
			return "", fmt.Errorf("invalid payer selector %q", spec)
		}
		return postings[idx].Credit, nil
	}
	if strings.HasPrefix(spec, "debit_of_posting_") {
		idxStr := strings.TrimPrefix(spec, "debit_of_posting_")
		idx, err := strconv.Atoi(idxStr)
		if err != nil || idx < 0 || idx >= len(postings) {
			return "", fmt.Errorf("invalid payer selector %q", spec)
		}
		return postings[idx].Debit, nil
	}
	return m.host.ResolveAccount(ctx, l, spec)
}

func (m *Module) compile() error {
	m.schedules = make([]compiledSchedule, 0, len(m.cfg.Schedules))
	for _, s := range m.cfg.Schedules {
		asset, err := money.ParseAsset(s.Match.Asset)
		if err != nil {
			return fmt.Errorf("schedule %s: %w", s.ID, err)
		}
		var eff time.Time
		if s.EffectiveFrom != "" {
			if t, err := time.Parse("2006-01-02", s.EffectiveFrom); err == nil {
				eff = t.UTC()
			} else if t, err := time.Parse(time.RFC3339, s.EffectiveFrom); err == nil {
				eff = t.UTC()
			} else {
				return fmt.Errorf("schedule %s: invalid effective_from %q", s.ID, s.EffectiveFrom)
			}
		}
		cs := compiledSchedule{
			raw:           s,
			effectiveFrom: eff,
			asset:         asset,
		}
		if s.Percent != "" {
			// Convert percentage string (e.g. "2.9") to Rate{Mantissa: 29, Scale: 3}
			r, err := money.ParseRate(s.Percent)
			if err != nil {
				return fmt.Errorf("schedule %s percent: %w", s.ID, err)
			}
			r.Scale += 2
			cs.hasRate = true
			cs.rate = r
		}
		if s.Fixed != "" {
			f, err := money.Parse(s.Fixed, asset)
			if err != nil {
				return fmt.Errorf("schedule %s fixed: %w", s.ID, err)
			}
			cs.hasFixed = true
			cs.fixed = f
		}
		if s.Cap != "" {
			c, err := money.Parse(s.Cap, asset)
			if err != nil {
				return fmt.Errorf("schedule %s cap: %w", s.ID, err)
			}
			cs.hasCap = true
			cs.capAmt = c
		}
		m.schedules = append(m.schedules, cs)
	}
	return nil
}
