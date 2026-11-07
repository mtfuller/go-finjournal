package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/module"
	"github.com/mtfuller/go-finjournal/pkg/storage"
)

func init() {
	module.Register("limits/velocity", func() (module.Module, error) {
		return NewVelocityValidator(VelocityConfig{}), nil
	})
}

// VelocityConfig configures the per-account velocity-limit Validator module (RFC section 11).
type VelocityConfig struct {
	// MaxSingleAmount caps the maximum amount of any single posting (in atomic units, if > 0).
	MaxSingleAmount int64 `json:"max_single_amount"`
	// MaxWindowAmount caps the cumulative debit outflow from an account within WindowSeconds (if > 0).
	MaxWindowAmount int64 `json:"max_window_amount"`
	// WindowSeconds is the rolling lookback window in seconds (default 86400 = 24h).
	WindowSeconds int64 `json:"window_seconds"`
}

// VelocityValidator enforces per-posting and rolling-window outflow caps on accounts.
type VelocityValidator struct {
	cfg  VelocityConfig
	host module.Host
}

// NewVelocityValidator creates a new VelocityValidator module.
func NewVelocityValidator(cfg VelocityConfig) *VelocityValidator {
	return &VelocityValidator{cfg: cfg}
}

func (v *VelocityValidator) Name() string { return "limits/velocity" }

func (v *VelocityValidator) Init(_ context.Context, h module.Host) error {
	v.host = h
	var decoded VelocityConfig
	if err := h.DecodeConfig(&decoded); err != nil {
		return err
	}
	if decoded.MaxSingleAmount > 0 {
		v.cfg.MaxSingleAmount = decoded.MaxSingleAmount
	}
	if decoded.MaxWindowAmount > 0 {
		v.cfg.MaxWindowAmount = decoded.MaxWindowAmount
	}
	if decoded.WindowSeconds > 0 {
		v.cfg.WindowSeconds = decoded.WindowSeconds
	}
	if v.cfg.WindowSeconds <= 0 {
		v.cfg.WindowSeconds = 86400
	}
	return nil
}

func (v *VelocityValidator) Close(_ context.Context) error { return nil }

// Validate checks single-posting and rolling-window velocity limits on draft postings.
func (v *VelocityValidator) Validate(ctx context.Context, d *module.Draft) error {
	maxSingle := money.FromInt64(v.cfg.MaxSingleAmount)
	maxWindow := money.FromInt64(v.cfg.MaxWindowAmount)

	// Accumulate per-debit-account outflows in this draft
	draftOutflows := make(map[core.AccountID]money.Amount)
	for _, p := range d.Postings() {
		if v.cfg.MaxSingleAmount > 0 && p.Amount.Cmp(maxSingle) > 0 {
			return &module.RejectError{
				Code:    "VELOCITY_SINGLE_LIMIT_EXCEEDED",
				Message: fmt.Sprintf("posting amount %s exceeds max_single_amount %s", p.Amount, maxSingle),
			}
		}
		cur := draftOutflows[p.Debit]
		sum, err := cur.Add(p.Amount)
		if err != nil {
			return err
		}
		draftOutflows[p.Debit] = sum
	}

	if v.cfg.MaxWindowAmount <= 0 || v.host == nil {
		return nil
	}

	now := v.host.Clock().Now()
	windowStart := now.Add(-time.Duration(v.cfg.WindowSeconds) * time.Second)

	for acctID, draftAmt := range draftOutflows {
		resolvedID, err := v.host.ResolveAccount(ctx, d.Intent().Ledger, string(acctID))
		if err != nil {
			resolvedID = acctID
		}
		page, err := v.host.Reader().Entries(ctx, storage.EntryQuery{
			Account: resolvedID,
			Limit:   1000,
		})
		if err != nil {
			return err
		}
		total := draftAmt
		for _, e := range page.Items {
			if e.Side == core.Debit && !e.CommittedAt.Before(windowStart) && !e.CommittedAt.After(now) {
				total, err = total.Add(e.Amount)
				if err != nil {
					return err
				}
			}
		}
		if total.Cmp(maxWindow) > 0 {
			return &module.RejectError{
				Code:    "VELOCITY_WINDOW_LIMIT_EXCEEDED",
				Message: fmt.Sprintf("account %s rolling window outflow %s exceeds max_window_amount %s", resolvedID, total, maxWindow),
			}
		}
	}
	return nil
}
