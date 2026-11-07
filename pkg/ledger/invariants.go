package ledger

import (
	"context"
	"fmt"
	"sort"

	"github.com/mtfuller/go-finjournal/pkg/journal"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage"
)

// AssetTrialBalance holds the total posted debits and credits for an asset across a ledger.
type AssetTrialBalance struct {
	Asset         money.AssetID `json:"asset"`
	PostedDebits  money.Amount  `json:"posted_debits"`
	PostedCredits money.Amount  `json:"posted_credits"`
	Balanced      bool          `json:"balanced"`
}

// InvariantViolation describes a detected invariant failure (I1..I8).
type InvariantViolation struct {
	Invariant   string `json:"invariant"` // "I1".."I8"
	Description string `json:"description"`
}

// InvariantReport summarizes a CheckInvariants run across a Ledger.
type InvariantReport struct {
	Ledger       LedgerID             `json:"ledger"`
	TrialBalance []AssetTrialBalance  `json:"trial_balance"`
	Violations   []InvariantViolation `json:"violations,omitempty"`
}

// OK reports whether zero invariant violations were detected.
func (r *InvariantReport) OK() bool {
	return len(r.Violations) == 0
}

// TrialBalance computes per-asset total posted debits and credits across all accounts in ledger l (Invariant I2).
func TrialBalance(ctx context.Context, store storage.Store, l LedgerID) ([]AssetTrialBalance, error) {
	accts, err := store.ListAccounts(ctx, l)
	if err != nil {
		return nil, err
	}
	totals := make(map[money.AssetID]*AssetTrialBalance)
	for _, a := range accts {
		bal, err := store.Balance(ctx, a.ID)
		if err != nil {
			return nil, err
		}
		tb, exists := totals[a.Asset]
		if !exists {
			tb = &AssetTrialBalance{
				Asset:         a.Asset,
				PostedDebits:  money.Zero,
				PostedCredits: money.Zero,
			}
			totals[a.Asset] = tb
		}
		tb.PostedDebits, _ = tb.PostedDebits.Add(bal.PostedDebits)
		tb.PostedCredits, _ = tb.PostedCredits.Add(bal.PostedCredits)
	}
	out := make([]AssetTrialBalance, 0, len(totals))
	for _, tb := range totals {
		tb.Balanced = tb.PostedDebits.Cmp(tb.PostedCredits) == 0
		out = append(out, *tb)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Asset < out[j].Asset })
	return out, nil
}

// CheckInvariants verifies all eight correctness invariants (I1–I8) from RFC Section 12.
// Note: Callers should fold deferred balances before running CheckInvariants if Deferred accounts were written.
func CheckInvariants(ctx context.Context, store storage.Store, l LedgerID) (*InvariantReport, error) {
	report := &InvariantReport{Ledger: l}

	// Fold deferred balances first so I2 and I3 see up-to-date Deferred account balances
	_, _ = store.FoldDeferredBalances(ctx, l)

	txs, err := store.ListTransactions(ctx, l)
	if err != nil {
		return nil, err
	}

	// I1 (per-tx balance), I6 (unique idempotency keys & tx IDs), I7 (reversals <= original)
	seenTxID := make(map[TransactionID]bool, len(txs))
	seenIdem := make(map[string]TransactionID, len(txs))

	for _, tx := range txs {
		// I6: unique transaction ID and idempotency key mapping
		if seenTxID[tx.ID] {
			report.Violations = append(report.Violations, InvariantViolation{
				Invariant:   "I6",
				Description: fmt.Sprintf("duplicate transaction id %s", tx.ID),
			})
		}
		seenTxID[tx.ID] = true

		if tx.IdempotencyKey != "" {
			if prevTx, dup := seenIdem[tx.IdempotencyKey]; dup && prevTx != tx.ID {
				report.Violations = append(report.Violations, InvariantViolation{
					Invariant:   "I6",
					Description: fmt.Sprintf("idempotency key %q maps to multiple transactions (%s and %s)", tx.IdempotencyKey, prevTx, tx.ID),
				})
			}
			seenIdem[tx.IdempotencyKey] = tx.ID

			if idemRec, err := store.Idempotency(ctx, l, tx.IdempotencyKey); err != nil || idemRec.TxID != tx.ID {
				report.Violations = append(report.Violations, InvariantViolation{
					Invariant:   "I6",
					Description: fmt.Sprintf("idempotency table mismatch for key %q (tx %s)", tx.IdempotencyKey, tx.ID),
				})
			}
		}

		// I1: Every transaction balances per asset (each Posting has a Debit and Credit of the same Asset & Amount > 0)
		debitsByAsset := make(map[money.AssetID]money.Amount)
		creditsByAsset := make(map[money.AssetID]money.Amount)
		for idx, p := range tx.Postings {
			if !p.Amount.IsPositive() {
				report.Violations = append(report.Violations, InvariantViolation{
					Invariant:   "I1",
					Description: fmt.Sprintf("tx %s posting %d has non-positive amount %s", tx.ID, idx, p.Amount),
				})
			}
			debitsByAsset[p.Asset], _ = debitsByAsset[p.Asset].Add(p.Amount)
			creditsByAsset[p.Asset], _ = creditsByAsset[p.Asset].Add(p.Amount)
		}
		for asset, dSum := range debitsByAsset {
			cSum := creditsByAsset[asset]
			if dSum.Cmp(cSum) != 0 {
				report.Violations = append(report.Violations, InvariantViolation{
					Invariant:   "I1",
					Description: fmt.Sprintf("tx %s unbalanced for %s: debits %s != credits %s", tx.ID, asset, dSum, cSum),
				})
			}
		}

		// I7: Reversed amounts never exceed the original, per posting
		st, err := store.TxState(ctx, tx.ID)
		if err == nil && st != nil {
			for idx, revAmt := range st.Reversed {
				if idx >= len(tx.Postings) {
					break
				}
				origAmt := tx.Postings[idx].Amount
				if revAmt.IsNegative() || revAmt.Cmp(origAmt) > 0 {
					report.Violations = append(report.Violations, InvariantViolation{
						Invariant: "I7",
						Description: fmt.Sprintf("tx %s posting %d reversed amount %s exceeds original %s",
							tx.ID, idx, revAmt, origAmt),
					})
				}
			}
		}
	}

	// I2: Trial balance across the ledger
	tb, err := TrialBalance(ctx, store, l)
	if err != nil {
		return nil, err
	}
	report.TrialBalance = tb
	for _, assetTB := range tb {
		if !assetTB.Balanced {
			report.Violations = append(report.Violations, InvariantViolation{
				Invariant: "I2",
				Description: fmt.Sprintf("trial balance mismatch for %s: posted_debits %s != posted_credits %s",
					assetTB.Asset, assetTB.PostedDebits, assetTB.PostedCredits),
			})
		}
	}

	// I3, I4, I5: Per-account entry sum, version monotonicity, and limit enforcement
	accts, err := store.ListAccounts(ctx, l)
	if err != nil {
		return nil, err
	}
	for _, a := range accts {
		bal, err := store.Balance(ctx, a.ID)
		if err != nil {
			return nil, err
		}

		// Collect all entries for this account in chronological order (oldest to newest)
		var allEntries []Entry
		cursor := ""
		for {
			page, err := store.Entries(ctx, storage.EntryQuery{Account: a.ID, Cursor: cursor, Limit: 500})
			if err != nil {
				return nil, err
			}
			allEntries = append(allEntries, page.Items...)
			if page.NextCursor == "" {
				break
			}
			cursor = page.NextCursor
		}
		// Reverse allEntries so oldest is first
		for i, j := 0, len(allEntries)-1; i < j; i, j = i+1, j-1 {
			allEntries[i], allEntries[j] = allEntries[j], allEntries[i]
		}

		sumDebits := money.Zero
		sumCredits := money.Zero
		for idx, e := range allEntries {
			if e.Side == Debit {
				sumDebits, _ = sumDebits.Add(e.Amount)
			} else {
				sumCredits, _ = sumCredits.Add(e.Amount)
			}
			if a.BalanceMode == Locked {
				// I4: PostedAfter matches running balance and Version increases strictly
				var expectedPosted money.Amount
				if a.Normal == Debit {
					expectedPosted, _ = sumDebits.Sub(sumCredits)
				} else {
					expectedPosted, _ = sumCredits.Sub(sumDebits)
				}
				if e.PostedAfter == nil || e.PostedAfter.Cmp(expectedPosted) != 0 {
					report.Violations = append(report.Violations, InvariantViolation{
						Invariant: "I4",
						Description: fmt.Sprintf("account %s entry %d posted_after mismatch: got %v, expected %s",
							a.Code, idx, e.PostedAfter, expectedPosted),
					})
				}
				if idx > 0 && e.Version <= allEntries[idx-1].Version {
					report.Violations = append(report.Violations, InvariantViolation{
						Invariant: "I4",
						Description: fmt.Sprintf("account %s entry version did not increase (%d -> %d)",
							a.Code, allEntries[idx-1].Version, e.Version),
					})
				}
			}
		}

		// I3: Balance row equals sum of entries
		if bal.PostedDebits.Cmp(sumDebits) != 0 || bal.PostedCredits.Cmp(sumCredits) != 0 {
			report.Violations = append(report.Violations, InvariantViolation{
				Invariant: "I3",
				Description: fmt.Sprintf("account %s balance row (%s / %s) != sum of entries (%s / %s)",
					a.Code, bal.PostedDebits, bal.PostedCredits, sumDebits, sumCredits),
			})
		}

		// I5: Locked account available balance within limits
		if a.BalanceMode == Locked {
			avail := bal.Available(a.Normal)
			if a.Limits.Min != nil && avail.Cmp(*a.Limits.Min) < 0 {
				report.Violations = append(report.Violations, InvariantViolation{
					Invariant: "I5",
					Description: fmt.Sprintf("account %s available balance %s < min limit %s",
						a.Code, avail, *a.Limits.Min),
				})
			}
			if a.Limits.Max != nil && avail.Cmp(*a.Limits.Max) > 0 {
				report.Violations = append(report.Violations, InvariantViolation{
					Invariant: "I5",
					Description: fmt.Sprintf("account %s available balance %s > max limit %s",
						a.Code, avail, *a.Limits.Max),
				})
			}
		}
	}

	// I8: Hash chain verifies from genesis to latest checkpoint
	if _, err := journal.VerifyChain(ctx, store, l); err != nil {
		report.Violations = append(report.Violations, InvariantViolation{
			Invariant:   "I8",
			Description: err.Error(),
		})
	}

	return report, nil
}
