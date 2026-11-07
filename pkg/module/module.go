// Package module defines the extension interfaces (Module, Enricher, Validator,
// Authorizer, FeeEngine, RateProvider, Subscriber, Host, and Draft) for the
// synchronous and asynchronous hook pipelines.
package module

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage"
)

// Factory constructs a new instance of a registered Module.
type Factory func() (Module, error)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]Factory)
)

// Register registers a module Factory by name so custom binaries built with xfinjournald
// can discover and instantiate third-party modules from init() functions.
func Register(name string, f Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[name] = f
}

// Lookup returns a registered module Factory by name.
func Lookup(name string) (Factory, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok := registry[name]
	return f, ok
}

// RegisteredNames returns all registered module names in sorted order.
func RegisteredNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(registry))
	for k := range registry {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// Module is the base interface implemented by every ledger module.
type Module interface {
	Name() string
	Init(ctx context.Context, h Host) error
	Close(ctx context.Context) error
}

// Host is everything the engine lends a module during Init and execution.
type Host interface {
	Reader() storage.Reader
	ResolveAccount(ctx context.Context, l core.LedgerID, codeOrID string) (core.AccountID, error)
	EnsureSystemAccount(ctx context.Context, l core.LedgerID, code string, asset money.AssetID, mode core.BalanceMode) (core.AccountID, error)
	Migrate(ctx context.Context, migrations fs.FS) error
	DecodeConfig(v any) error
	Logger() *slog.Logger
	Clock() core.Clock
}

// RejectError is returned by Draft.Reject to signal a business rejection (422 module_rejected).
type RejectError struct {
	Module  string
	Code    string
	Message string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("%s rejected (%s): %s", e.Module, e.Code, e.Message)
}

// Draft wraps an Intent while synchronous hooks (Enrichers and Validators) run.
// Modules can append postings and metadata, or reject the draft, but cannot mutate
// or remove caller-provided postings.
type Draft struct {
	intent        core.Intent
	postings      []core.Posting
	metadata      core.Metadata
	currentModule string
}

// NewDraft creates a Draft from a caller's Intent.
func NewDraft(intent core.Intent) *Draft {
	postings := make([]core.Posting, len(intent.Postings))
	copy(postings, intent.Postings)
	meta := intent.Metadata.Clone()
	if meta == nil {
		meta = make(core.Metadata)
	}
	return &Draft{
		intent:   intent,
		postings: postings,
		metadata: meta,
	}
}

// SetCurrentModule sets the active module name used to stamp Added postings and metadata keys.
func (d *Draft) SetCurrentModule(name string) {
	d.currentModule = name
}

// Intent returns a read-only copy of the caller's original Intent.
func (d *Draft) Intent() core.Intent {
	cp := d.intent
	cp.Postings = make([]core.Posting, len(d.intent.Postings))
	copy(cp.Postings, d.intent.Postings)
	cp.Params = d.intent.Params.Clone()
	cp.Metadata = d.intent.Metadata.Clone()
	return cp
}

// Postings returns a copy of all postings currently on the Draft (caller + earlier modules).
func (d *Draft) Postings() []core.Posting {
	cp := make([]core.Posting, len(d.postings))
	copy(cp, d.postings)
	return cp
}

// AddPosting appends a posting to the Draft, stamping p.Source with the current module's name.
func (d *Draft) AddPosting(p core.Posting) {
	if d.currentModule != "" {
		p.Source = d.currentModule
	}
	d.postings = append(d.postings, p)
}

// Metadata returns a copy of the Draft's accumulated metadata.
func (d *Draft) Metadata() core.Metadata {
	return d.metadata.Clone()
}

// SetMeta records metadata under "<module>.<key>" (or "<key>" if no module is active).
func (d *Draft) SetMeta(key, value string) {
	fullKey := key
	if d.currentModule != "" {
		fullKey = d.currentModule + "." + key
	}
	d.metadata[fullKey] = value
}

// Reject aborts the intent with a 422 module_rejected business error.
func (d *Draft) Reject(code, msg string) error {
	return &RejectError{
		Module:  d.currentModule,
		Code:    code,
		Message: msg,
	}
}

// Enricher runs synchronously before commit and may append postings and metadata to a Draft.
type Enricher interface {
	Module
	Matches(kind string) bool
	Enrich(ctx context.Context, d *Draft) error
}

// Validator runs synchronously after Enrichers and may inspect (but not modify) a Draft.
type Validator interface {
	Module
	Validate(ctx context.Context, d *Draft) error
}

// Principal represents an authenticated caller in server mode.
type Principal struct {
	ID               string
	Ledgers          []core.LedgerID
	Scopes           []string
	AccountAllowlist []string // optional glob patterns like "liabilities:wallets:*"
}

// Resource identifies the target of an authorization check.
type Resource struct {
	Ledger       core.LedgerID
	AccountIDs   []core.AccountID
	AccountCodes []string
}

// Authorizer checks whether a Principal is permitted to perform an action on a Resource.
type Authorizer interface {
	Module
	Allow(ctx context.Context, p Principal, action string, r Resource) error
}

// FeeRequest is passed to FeeEngine.Fees.
type FeeRequest struct {
	Ledger      core.LedgerID
	Kind        string
	EffectiveAt time.Time
	Postings    []core.Posting
	Params      core.Metadata
}

// Fee describes a fee posting to be added by a FeeEngine.
type Fee struct {
	Payer    core.AccountID
	Payee    core.AccountID
	Amount   money.Amount
	Asset    money.AssetID
	Code     string
	Metadata map[string]string
}

// FeeEngine is a convenience interface that the engine automatically wraps as an Enricher.
type FeeEngine interface {
	Module
	Matches(kind string) bool
	Fees(ctx context.Context, req FeeRequest) ([]Fee, error)
}

type feeEngineEnricher struct {
	FeeEngine
}

// WrapFeeEngine adapts a FeeEngine into an Enricher.
func WrapFeeEngine(fe FeeEngine) Enricher {
	if e, ok := fe.(Enricher); ok {
		return e
	}
	return &feeEngineEnricher{FeeEngine: fe}
}

func (w *feeEngineEnricher) Enrich(ctx context.Context, d *Draft) error {
	intent := d.Intent()
	eff := time.Now().UTC()
	if intent.EffectiveAt != nil {
		eff = *intent.EffectiveAt
	}
	req := FeeRequest{
		Ledger:      intent.Ledger,
		Kind:        intent.Kind,
		EffectiveAt: eff,
		Postings:    d.Postings(),
		Params:      intent.Params,
	}
	fees, err := w.FeeEngine.Fees(ctx, req)
	if err != nil {
		return err
	}
	for _, f := range fees {
		if f.Amount.IsZero() {
			continue
		}
		d.AddPosting(core.Posting{
			Debit:  f.Payer,
			Credit: f.Payee,
			Amount: f.Amount,
			Asset:  f.Asset,
			Memo:   f.Code,
		})
		for k, v := range f.Metadata {
			d.SetMeta(k, v)
		}
	}
	return nil
}

// Subscriber receives committed events asynchronously from the outbox relay (at-least-once).
type Subscriber interface {
	Module
	Topics() []string
	Handle(ctx context.Context, evs []events.Event) error
}

// Quote represents an FX or asset conversion quote with rate provenance (RFC section 8).
type Quote struct {
	ID         string        `json:"id"`
	Base       money.AssetID `json:"base"`
	Quote      money.AssetID `json:"quote"`
	Mid        money.Rate    `json:"mid"`
	Customer   money.Rate    `json:"customer"`
	Source     string        `json:"source"`
	ObservedAt time.Time     `json:"observed_at"`
	ExpiresAt  time.Time     `json:"expires_at"`
}

// RateProvider supplies conversion quotes for FX enrichment and reporting.
type RateProvider interface {
	Module
	Quote(ctx context.Context, base, quote money.AssetID, at time.Time) (Quote, error)
	GetQuote(ctx context.Context, quoteID string) (Quote, error)
}
