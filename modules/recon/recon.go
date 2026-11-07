// Package recon implements the official reconciliation module (RFC section 9 Example 3):
// expected item capture from transaction.committed events, CSV/structured statement import,
// ordered matching rules (exact ref + amount, amount + 2-day value date window, many-to-one
// payout batch sum), and unmatched exception reporting.
package recon

import (
	"context"
	"embed"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/module"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func init() {
	module.Register("recon", func() (module.Module, error) {
		return New(), nil
	})
}

// Status represents the reconciliation status of an external statement item.
type Status string

const (
	StatusMatched   Status = "matched"
	StatusPartial   Status = "partial"
	StatusAmbiguous Status = "ambiguous"
	StatusUnmatched Status = "unmatched"
)

// ExpectedItem is a ledger movement on an account tagged with "recon.source".
type ExpectedItem struct {
	TxID        core.TransactionID `json:"tx_id"`
	Ledger      core.LedgerID      `json:"ledger"`
	AccountID   core.AccountID     `json:"account_id"`
	Source      string             `json:"source"` // e.g. "bank:operating-usd"
	Reference   string             `json:"reference"`
	Amount      money.Amount       `json:"amount"`
	Asset       money.AssetID      `json:"asset"`
	EffectiveAt time.Time          `json:"effective_at"`
	Matched     bool               `json:"matched"`
}

// ExternalItem is an imported bank or processor statement line.
type ExternalItem struct {
	ID        string        `json:"id"`
	Source    string        `json:"source"`
	Reference string        `json:"reference"`
	Amount    money.Amount  `json:"amount"`
	Asset     money.AssetID `json:"asset"`
	ValueDate time.Time     `json:"value_date"`
}

// Result is the outcome of matching an ExternalItem against ExpectedItems.
type Result struct {
	ExternalID string               `json:"external_id"`
	Status     Status               `json:"status"`
	Expected   []core.TransactionID `json:"expected,omitempty"`
	Diff       money.Amount         `json:"diff"`
	Rule       string               `json:"rule,omitempty"`
}

// Matcher defines a pluggable reconciliation rule.
type Matcher interface {
	Name() string
	Match(ctx context.Context, ext ExternalItem, candidates []ExpectedItem) (Result, error)
}

// Module is the official reconciliation module.
type Module struct {
	host     module.Host
	matchers []Matcher

	mu       sync.Mutex
	expected []ExpectedItem
	results  []Result
}

// New creates a recon Module with default matching rules (exact ref + amount,
// amount + 2-day date window, and many-to-one batch sum).
func New(customMatchers ...Matcher) *Module {
	matchers := customMatchers
	if len(matchers) == 0 {
		matchers = []Matcher{
			exactRefAndAmountMatcher{},
			amountAndDateWindowMatcher{maxWindow: 48 * time.Hour},
			manyToOneBatchSumMatcher{},
		}
	}
	return &Module{
		matchers: matchers,
	}
}

func (m *Module) Name() string {
	return "recon"
}

func (m *Module) Init(ctx context.Context, h module.Host) error {
	m.host = h
	return h.Migrate(ctx, migrationsFS)
}

func (m *Module) Close(_ context.Context) error {
	return nil
}

func (m *Module) Topics() []string {
	return []string{events.TypeTransactionCommitted, events.TypeTransactionPosted}
}

// Handle inspects committed transactions and records expected items for any account
// carrying metadata["recon.source"].
func (m *Module) Handle(ctx context.Context, evs []events.Event) error {
	for _, ev := range evs {
		var tx core.Transaction
		if err := json.Unmarshal(ev.Payload, &tx); err != nil {
			continue
		}
		var acctIDs []core.AccountID
		for _, p := range tx.Postings {
			acctIDs = append(acctIDs, p.Debit, p.Credit)
		}
		accts, err := m.host.Reader().Accounts(ctx, acctIDs)
		if err != nil {
			continue
		}
		acctMap := make(map[core.AccountID]core.Account, len(accts))
		for _, a := range accts {
			acctMap[a.ID] = a
		}

		ref := tx.Metadata["recon.ref"]
		if ref == "" {
			ref = tx.IdempotencyKey
		}

		m.mu.Lock()
		for _, p := range tx.Postings {
			if da, ok := acctMap[p.Debit]; ok && da.Metadata["recon.source"] != "" {
				m.expected = append(m.expected, ExpectedItem{
					TxID:        tx.ID,
					Ledger:      tx.Ledger,
					AccountID:   da.ID,
					Source:      da.Metadata["recon.source"],
					Reference:   ref,
					Amount:      p.Amount,
					Asset:       p.Asset,
					EffectiveAt: tx.EffectiveAt,
				})
			}
			if ca, ok := acctMap[p.Credit]; ok && ca.Metadata["recon.source"] != "" {
				m.expected = append(m.expected, ExpectedItem{
					TxID:        tx.ID,
					Ledger:      tx.Ledger,
					AccountID:   ca.ID,
					Source:      ca.Metadata["recon.source"],
					Reference:   ref,
					Amount:      p.Amount,
					Asset:       p.Asset,
					EffectiveAt: tx.EffectiveAt,
				})
			}
		}
		m.mu.Unlock()
	}
	return nil
}

// ImportStatementCSV parses a CSV statement and returns the JSON-encoded results slice and the count of unmatched items.
func (m *Module) ImportStatementCSV(ctx context.Context, source string, asset money.AssetID, r io.Reader) ([]byte, int, error) {
	res, err := m.ImportCSV(ctx, source, asset, r)
	if err != nil {
		return nil, 0, err
	}
	unmatched := 0
	for _, item := range res {
		if item.Status != StatusMatched {
			unmatched++
		}
	}
	b, err := json.Marshal(res)
	if err != nil {
		return nil, 0, err
	}
	return b, unmatched, nil
}

// ImportCSV parses a CSV statement with columns: id,reference,amount,value_date (RFC3339 or YYYY-MM-DD)
// for a given source and asset, and runs reconciliation matching.
func (m *Module) ImportCSV(ctx context.Context, source string, asset money.AssetID, r io.Reader) ([]Result, error) {
	parsedAsset, err := money.ParseAsset(asset)
	if err != nil {
		return nil, err
	}
	cr := csv.NewReader(r)
	rows, err := cr.ReadAll()
	if err != nil {
		return nil, err
	}
	var items []ExternalItem
	for i, row := range rows {
		if len(row) < 4 {
			continue
		}
		// Skip header row
		if i == 0 && strings.EqualFold(strings.TrimSpace(row[0]), "id") {
			continue
		}
		amt, err := money.Parse(strings.TrimSpace(row[2]), parsedAsset)
		if err != nil {
			amt, err = money.ParseMinor(strings.TrimSpace(row[2]))
			if err != nil {
				return nil, fmt.Errorf("row %d invalid amount %q: %w", i, row[2], err)
			}
		}
		valDateStr := strings.TrimSpace(row[3])
		valDate, err := time.Parse("2006-01-02", valDateStr)
		if err != nil {
			valDate, err = time.Parse(time.RFC3339, valDateStr)
			if err != nil {
				return nil, fmt.Errorf("row %d invalid value_date %q: %w", i, valDateStr, err)
			}
		}
		items = append(items, ExternalItem{
			ID:        strings.TrimSpace(row[0]),
			Source:    source,
			Reference: strings.TrimSpace(row[1]),
			Amount:    amt,
			Asset:     asset,
			ValueDate: valDate.UTC(),
		})
	}
	return m.Reconcile(ctx, items)
}

// Reconcile matches external statement items against unmatched expected items in order.
func (m *Module) Reconcile(ctx context.Context, items []ExternalItem) ([]Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	results := make([]Result, 0, len(items))
	for _, ext := range items {
		var candidates []ExpectedItem
		for _, exp := range m.expected {
			if !exp.Matched && exp.Source == ext.Source && exp.Asset == ext.Asset {
				candidates = append(candidates, exp)
			}
		}

		matchedRes := Result{
			ExternalID: ext.ID,
			Status:     StatusUnmatched,
			Diff:       ext.Amount,
		}

		for _, matcher := range m.matchers {
			res, err := matcher.Match(ctx, ext, candidates)
			if err != nil {
				return nil, err
			}
			if res.Status == StatusMatched || res.Status == StatusPartial || res.Status == StatusAmbiguous {
				res.ExternalID = ext.ID
				res.Rule = matcher.Name()
				matchedRes = res
				if res.Status == StatusMatched {
					for _, matchedTxID := range res.Expected {
						for idx := range m.expected {
							if m.expected[idx].TxID == matchedTxID && !m.expected[idx].Matched {
								m.expected[idx].Matched = true
								break
							}
						}
					}
				}
				break
			}
		}
		results = append(results, matchedRes)
		m.results = append(m.results, matchedRes)
	}
	return results, nil
}

// UnmatchedExpected returns expected items older than maxAge that remain unmatched.
func (m *Module) UnmatchedExpected(now time.Time, maxAge time.Duration) []ExpectedItem {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []ExpectedItem
	for _, exp := range m.expected {
		if !exp.Matched && now.Sub(exp.EffectiveAt) >= maxAge {
			out = append(out, exp)
		}
	}
	return out
}

// --- Built-in Matcher rules ---

type exactRefAndAmountMatcher struct{}

func (exactRefAndAmountMatcher) Name() string { return "exact_ref_and_amount" }

func (exactRefAndAmountMatcher) Match(_ context.Context, ext ExternalItem, candidates []ExpectedItem) (Result, error) {
	if ext.Reference == "" {
		return Result{Status: StatusUnmatched}, nil
	}
	for _, c := range candidates {
		if c.Reference == ext.Reference && c.Amount.Cmp(ext.Amount) == 0 {
			return Result{
				Status:   StatusMatched,
				Expected: []core.TransactionID{c.TxID},
				Diff:     money.Zero,
			}, nil
		}
	}
	return Result{Status: StatusUnmatched}, nil
}

type amountAndDateWindowMatcher struct {
	maxWindow time.Duration
}

func (amountAndDateWindowMatcher) Name() string { return "amount_and_date_window" }

func (m amountAndDateWindowMatcher) Match(_ context.Context, ext ExternalItem, candidates []ExpectedItem) (Result, error) {
	var hits []core.TransactionID
	for _, c := range candidates {
		if c.Amount.Cmp(ext.Amount) != 0 {
			continue
		}
		diff := ext.ValueDate.Sub(c.EffectiveAt)
		if diff < 0 {
			diff = -diff
		}
		if diff <= m.maxWindow {
			hits = append(hits, c.TxID)
		}
	}
	if len(hits) == 1 {
		return Result{
			Status:   StatusMatched,
			Expected: hits,
			Diff:     money.Zero,
		}, nil
	}
	if len(hits) > 1 {
		return Result{
			Status:   StatusAmbiguous,
			Expected: hits,
			Diff:     money.Zero,
		}, nil
	}
	return Result{Status: StatusUnmatched}, nil
}

type manyToOneBatchSumMatcher struct{}

func (manyToOneBatchSumMatcher) Name() string { return "many_to_one_batch_sum" }

func (manyToOneBatchSumMatcher) Match(_ context.Context, ext ExternalItem, candidates []ExpectedItem) (Result, error) {
	if len(candidates) < 2 {
		return Result{Status: StatusUnmatched}, nil
	}
	sum := money.Zero
	var ids []core.TransactionID
	for _, c := range candidates {
		if ext.Reference != "" && c.Reference != ext.Reference {
			continue
		}
		sum, _ = sum.Add(c.Amount)
		ids = append(ids, c.TxID)
	}
	if len(ids) >= 2 && sum.Cmp(ext.Amount) == 0 {
		return Result{
			Status:   StatusMatched,
			Expected: ids,
			Diff:     money.Zero,
		}, nil
	}
	return Result{Status: StatusUnmatched}, nil
}
