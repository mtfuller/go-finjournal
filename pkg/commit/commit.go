// Package commit implements DirectCommitter and BatchedCommitter (steps 7–10 of
// the posting pipeline: ordered row locking, balance application, net limit checks,
// entry/outbox/idempotency persistence, and batch split-on-timeout).
package commit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage"
)

// OperationKind distinguishes normal postings, hold settlements, hold voids/expiries, and reversals.
type OperationKind uint8

const (
	OpPost OperationKind = iota
	OpSettleHold
	OpVoidHold
	OpExpireHold
	OpReverse
)

// CommitRequest contains a validated transaction draft and resolved account definitions
// ready for the critical section (steps 7–10).
type CommitRequest struct {
	Op           OperationKind
	Tx           *core.Transaction
	RequestHash  []byte
	Accounts     map[core.AccountID]core.Account
	TargetTx     *core.Transaction // original transaction for SettleHold, VoidHold, ExpireHold, Reverse
	ReversalAmts []money.Amount    // per-original-posting reversal amounts when Op == OpReverse
	ExtraEvents  []events.Event
	ExternalTx   any // non-nil when caller passed ledger.WithinTx(tx)
	EventID      string
}

// Committer runs steps 7–10 of the posting algorithm.
type Committer interface {
	Commit(ctx context.Context, req CommitRequest) (*core.Transaction, error)
	Close() error
}

// DirectCommitter commits each request in its own database transaction (or joined ExternalTx).
type DirectCommitter struct {
	store storage.Store
}

// NewDirect creates a DirectCommitter.
func NewDirect(store storage.Store) *DirectCommitter {
	return &DirectCommitter{store: store}
}

func (c *DirectCommitter) Close() error { return nil }

// Commit runs steps 7–10 for a single CommitRequest.
func (c *DirectCommitter) Commit(ctx context.Context, req CommitRequest) (*core.Transaction, error) {
	results, err := executeBatchInTx(ctx, c.store, []CommitRequest{req}, req.ExternalTx)
	if err != nil {
		return nil, err
	}
	res := results[0]
	if res.err != nil {
		return nil, res.err
	}
	return res.tx, nil
}

// BatchConfig configures the BatchedCommitter.
type BatchConfig struct {
	MaxBatch    int           // default 200 intents
	MaxWait     time.Duration // default 2ms
	LockTimeout time.Duration // default 50ms; on timeout the batch splits in half and retries
	StepMode    bool          // when true, background loop does not auto-drain; simulator calls FlushBatch
}

type batchItem struct {
	ctx  context.Context
	req  CommitRequest
	resp chan itemResult
}

type itemResult struct {
	tx  *core.Transaction
	err error
}

// BatchedCommitter collects incoming CommitRequests and commits them in a single database transaction
// locking only the touched Locked-mode accounts in ascending AccountID order.
type BatchedCommitter struct {
	store  storage.Store
	cfg    BatchConfig
	direct *DirectCommitter

	mu      sync.Mutex
	pending []batchItem
	notify  chan struct{}
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewBatched creates a BatchedCommitter.
func NewBatched(store storage.Store, cfg BatchConfig) (*BatchedCommitter, error) {
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 200
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 2 * time.Millisecond
	}
	if cfg.LockTimeout <= 0 {
		cfg.LockTimeout = 50 * time.Millisecond
	}
	bc := &BatchedCommitter{
		store:  store,
		cfg:    cfg,
		direct: NewDirect(store),
		notify: make(chan struct{}, 1),
		stopCh: make(chan struct{}),
	}
	if !cfg.StepMode {
		bc.wg.Add(1)
		go bc.loop()
	}
	return bc, nil
}

func (bc *BatchedCommitter) Close() error {
	select {
	case <-bc.stopCh:
		return nil
	default:
		close(bc.stopCh)
	}
	bc.wg.Wait()
	return nil
}

func (bc *BatchedCommitter) Commit(ctx context.Context, req CommitRequest) (*core.Transaction, error) {
	// ExternalTx cannot join a shared batch; fall back to direct mode for WithinTx callers.
	if req.ExternalTx != nil {
		return bc.direct.Commit(ctx, req)
	}
	ch := make(chan itemResult, 1)
	bc.mu.Lock()
	bc.pending = append(bc.pending, batchItem{ctx: ctx, req: req, resp: ch})
	shouldFlushNow := len(bc.pending) >= bc.cfg.MaxBatch
	bc.mu.Unlock()

	if bc.cfg.StepMode {
		// In deterministic simulation step mode, if caller invokes Commit synchronously,
		// flush if batch is full; otherwise caller or simulator calls FlushBatch.
		if shouldFlushNow {
			bc.FlushBatch()
		}
	} else {
		select {
		case bc.notify <- struct{}{}:
		default:
		}
	}

	select {
	case res := <-ch:
		return res.tx, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// EnqueueAsync enqueues a CommitRequest in StepMode without blocking, returning a channel for the result.
func (bc *BatchedCommitter) EnqueueAsync(ctx context.Context, req CommitRequest) <-chan itemResult {
	ch := make(chan itemResult, 1)
	bc.mu.Lock()
	bc.pending = append(bc.pending, batchItem{ctx: ctx, req: req, resp: ch})
	bc.mu.Unlock()
	return ch
}

// FlushBatch synchronously processes up to MaxBatch pending requests (used by background loop and simulator).
func (bc *BatchedCommitter) FlushBatch() int {
	bc.mu.Lock()
	if len(bc.pending) == 0 {
		bc.mu.Unlock()
		return 0
	}
	n := len(bc.pending)
	if n > bc.cfg.MaxBatch {
		n = bc.cfg.MaxBatch
	}
	batch := make([]batchItem, n)
	copy(batch, bc.pending[:n])
	bc.pending = bc.pending[n:]
	bc.mu.Unlock()

	bc.processBatchWithRetry(batch, true)
	return len(batch)
}

func (bc *BatchedCommitter) loop() {
	defer bc.wg.Done()
	for {
		select {
		case <-bc.stopCh:
			for bc.FlushBatch() > 0 {
			}
			return
		case <-bc.notify:
			bc.mu.Lock()
			count := len(bc.pending)
			bc.mu.Unlock()
			if count < bc.cfg.MaxBatch {
				timer := time.NewTimer(bc.cfg.MaxWait)
				select {
				case <-timer.C:
				case <-bc.stopCh:
					timer.Stop()
				}
			}
			for bc.FlushBatch() > 0 {
			}
		}
	}
}

func (bc *BatchedCommitter) processBatchWithRetry(batch []batchItem, allowSplit bool) {
	if len(batch) == 0 {
		return
	}
	reqs := make([]CommitRequest, len(batch))
	for i := range batch {
		reqs[i] = batch[i].req
	}

	ctx, cancel := context.WithTimeout(context.Background(), bc.cfg.LockTimeout)
	results, err := executeBatchInTx(ctx, bc.store, reqs, nil)
	cancel()

	if err != nil {
		// On timeout or transient lock error, split the batch in half and retry once (RFC section 4 & 5).
		if allowSplit && len(batch) > 1 && isLockOrTimeoutErr(err) {
			mid := len(batch) / 2
			bc.processBatchWithRetry(batch[:mid], false)
			bc.processBatchWithRetry(batch[mid:], false)
			return
		}
		wrappedErr := err
		if isLockOrTimeoutErr(err) {
			wrappedErr = core.NewError(core.CodeLockTimeout, "batch lock timeout", map[string]any{"cause": err.Error()})
		}
		for _, item := range batch {
			item.resp <- itemResult{err: wrappedErr}
		}
		return
	}

	for i, item := range batch {
		item.resp <- results[i]
	}
}

func isLockOrTimeoutErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var cErr *core.Error
	if errors.As(err, &cErr) && cErr.Code == core.CodeLockTimeout {
		return true
	}
	return false
}

// executeBatchInTx executes a slice of CommitRequests atomically inside one storage.Tx.
// Any individual request that fails a limit or state check is skipped and gets its own error,
// while valid requests in the batch commit together.
func executeBatchInTx(
	ctx context.Context,
	store storage.Store,
	reqs []CommitRequest,
	externalTx any,
) ([]itemResult, error) {
	results := make([]itemResult, len(reqs))

	if externalTx == nil {
		if bp := store.Batch(); bp != nil {
			if len(reqs) > 1 {
				batchCommitTime := time.Now().UTC()
				for _, r := range reqs {
					if r.Tx.CommittedAt.After(batchCommitTime) {
						batchCommitTime = r.Tx.CommittedAt
					}
				}
				for i := range reqs {
					reqs[i].Tx.CommittedAt = batchCommitTime
				}
			}
			items := make([]storage.BatchCommitItem, len(reqs))
			for i, req := range reqs {
				evs := make([]events.Event, 0, 1+len(req.ExtraEvents))
				evs = append(evs, buildOutboxEvent(req))
				evs = append(evs, req.ExtraEvents...)
				var targetID *core.TransactionID
				if req.TargetTx != nil {
					id := req.TargetTx.ID
					targetID = &id
				}
				items[i] = storage.BatchCommitItem{
					Op:           uint8(req.Op),
					Tx:           req.Tx,
					RequestHash:  req.RequestHash,
					AccountDefs:  req.Accounts,
					TargetTxID:   targetID,
					ReversalAmts: req.ReversalAmts,
					Events:       evs,
				}
			}
			batchRes, err := bp.PostBatch(ctx, items)
			if err != nil {
				return nil, err
			}
			for i, br := range batchRes {
				if br.Err != nil {
					results[i] = itemResult{err: br.Err}
				} else {
					tx := br.Tx
					if tx == nil {
						tx = reqs[i].Tx
					}
					if br.IdempotentReplay {
						cp := *tx
						cp.IdempotentReplay = true
						tx = &cp
					}
					results[i] = itemResult{tx: tx}
				}
			}
			return results, nil
		}
	}

	var tx storage.Tx
	var err error
	if externalTx != nil {
		tx, err = store.Join(ctx, externalTx)
	} else {
		tx, err = store.Begin(ctx)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 1. Collect all Locked-mode account IDs and target TxIDs across the batch
	lockedIDSet := make(map[core.AccountID]bool)
	targetTxIDSet := make(map[core.TransactionID]bool)
	for _, r := range reqs {
		for _, p := range r.Tx.Postings {
			if a, ok := r.Accounts[p.Debit]; ok && a.BalanceMode == core.Locked {
				lockedIDSet[p.Debit] = true
			}
			if a, ok := r.Accounts[p.Credit]; ok && a.BalanceMode == core.Locked {
				lockedIDSet[p.Credit] = true
			}
		}
		if r.TargetTx != nil {
			targetTxIDSet[r.TargetTx.ID] = true
			for _, p := range r.TargetTx.Postings {
				if a, ok := r.Accounts[p.Debit]; ok && a.BalanceMode == core.Locked {
					lockedIDSet[p.Debit] = true
				}
				if a, ok := r.Accounts[p.Credit]; ok && a.BalanceMode == core.Locked {
					lockedIDSet[p.Credit] = true
				}
			}
		}
	}

	lockedIDs := make([]core.AccountID, 0, len(lockedIDSet))
	for id := range lockedIDSet {
		lockedIDs = append(lockedIDs, id)
	}
	sort.Slice(lockedIDs, func(i, j int) bool { return lockedIDs[i] < lockedIDs[j] })

	lockedBals, err := tx.LockBalances(ctx, lockedIDs)
	if err != nil {
		return nil, err
	}
	balMap := make(map[core.AccountID]core.Balance, len(lockedBals))
	for _, b := range lockedBals {
		balMap[b.Account] = b
	}

	var targetTxIDs []core.TransactionID
	for id := range targetTxIDSet {
		targetTxIDs = append(targetTxIDs, id)
	}
	sort.Slice(targetTxIDs, func(i, j int) bool { return targetTxIDs[i] < targetTxIDs[j] })

	var stateMap map[core.TransactionID]storage.TxState
	if len(targetTxIDs) > 0 {
		stateMap, err = tx.GetTxStates(ctx, targetTxIDs)
		if err != nil {
			return nil, err
		}
	} else {
		stateMap = make(map[core.TransactionID]storage.TxState)
	}

	// 2. Process each request in order on working copies so a failed request leaves neighbours intact
	type idemKey struct {
		ledger core.LedgerID
		key    string
	}
	seenIdemInBatch := make(map[idemKey]int) // maps to index of winning req in reqs

	var validTxs []*core.Transaction
	var validEntries []core.Entry
	var validIdem []storage.IdemRecord
	var validStates []storage.TxState
	var validEvents []events.Event
	modifiedBals := make(map[core.AccountID]bool)
	batchCommitTime := time.Now().UTC()
	if len(reqs) > 0 && !reqs[0].Tx.CommittedAt.IsZero() {
		for _, r := range reqs {
			if r.Tx.CommittedAt.After(batchCommitTime) {
				batchCommitTime = r.Tx.CommittedAt
			}
		}
	}

	for i, req := range reqs {
		ik := idemKey{ledger: req.Tx.Ledger, key: req.Tx.IdempotencyKey}
		if req.Tx.IdempotencyKey != "" {
			if prevIdx, dup := seenIdemInBatch[ik]; dup {
				prevRes := results[prevIdx]
				if prevRes.err != nil {
					results[i] = prevRes
					continue
				}
				if !bytes.Equal(reqs[prevIdx].RequestHash, req.RequestHash) {
					results[i] = itemResult{
						err: core.NewError(core.CodeIdempotencyConflict,
							"idempotency key already used with different parameters",
							map[string]any{"idempotency_key": req.Tx.IdempotencyKey}),
					}
					continue
				}
				replayCopy := *prevRes.tx
				replayCopy.IdempotentReplay = true
				results[i] = itemResult{tx: &replayCopy}
				continue
			}

			// Check if idempotency key already exists in store
			if ex, err := store.Idempotency(ctx, req.Tx.Ledger, req.Tx.IdempotencyKey); err == nil && ex != nil {
				if !bytes.Equal(ex.RequestHash, req.RequestHash) {
					results[i] = itemResult{
						err: core.NewError(core.CodeIdempotencyConflict,
							"idempotency key already used with different parameters",
							map[string]any{"idempotency_key": req.Tx.IdempotencyKey}),
					}
					continue
				}
				storedTx, err := store.Transaction(ctx, ex.TxID)
				if err != nil {
					return nil, err
				}
				storedTx.IdempotentReplay = true
				results[i] = itemResult{tx: storedTx}
				continue
			}
		}

		// Snapshot working balances & target state for this single intent
		workBals := make(map[core.AccountID]core.Balance)
		touchedAccounts := make(map[core.AccountID]core.Account)
		for _, p := range req.Tx.Postings {
			if a, ok := req.Accounts[p.Debit]; ok {
				touchedAccounts[p.Debit] = a
				if a.BalanceMode == core.Locked {
					workBals[p.Debit] = balMap[p.Debit]
				}
			}
			if a, ok := req.Accounts[p.Credit]; ok {
				touchedAccounts[p.Credit] = a
				if a.BalanceMode == core.Locked {
					workBals[p.Credit] = balMap[p.Credit]
				}
			}
		}
		if req.TargetTx != nil {
			for _, p := range req.TargetTx.Postings {
				if a, ok := req.Accounts[p.Debit]; ok {
					touchedAccounts[p.Debit] = a
					if a.BalanceMode == core.Locked {
						workBals[p.Debit] = balMap[p.Debit]
					}
				}
				if a, ok := req.Accounts[p.Credit]; ok {
					touchedAccounts[p.Credit] = a
					if a.BalanceMode == core.Locked {
						workBals[p.Credit] = balMap[p.Credit]
					}
				}
			}
		}

		var itemEntries []core.Entry
		var updatedState *storage.TxState
		var itemErr error

		switch req.Op {
		case OpPost:
			itemEntries, itemErr = applyPost(req, workBals)
		case OpSettleHold:
			st := stateMap[req.TargetTx.ID]
			itemEntries, updatedState, itemErr = applySettleHold(req, workBals, st)
		case OpVoidHold, OpExpireHold:
			st := stateMap[req.TargetTx.ID]
			updatedState, itemErr = applyVoidOrExpireHold(req, workBals, st)
		case OpReverse:
			st := stateMap[req.TargetTx.ID]
			itemEntries, updatedState, itemErr = applyReverse(req, workBals, st)
		}

		if itemErr != nil {
			results[i] = itemResult{err: itemErr}
			continue
		}

		// Check net limits on all touched Locked accounts (RFC section 4 step 8:
		// "Apply all postings to in-memory balance copies ... then check limits on the net result.")
		for acctID, bal := range workBals {
			acct := touchedAccounts[acctID]
			avail := bal.Available(acct.Normal)
			if acct.Limits.Min != nil && avail.Cmp(*acct.Limits.Min) < 0 {
				itemErr = core.NewError(core.CodeInsufficientFunds,
					fmt.Sprintf("available balance %s on account %s (%s) would fall below min %s",
						avail, acct.Code, acct.ID, *acct.Limits.Min),
					map[string]any{
						"account_id":   acct.ID,
						"account_code": acct.Code,
						"available":    avail.String(),
						"min_limit":    acct.Limits.Min.String(),
					})
				break
			}
			if acct.Limits.Max != nil && avail.Cmp(*acct.Limits.Max) > 0 {
				itemErr = core.NewError(core.CodeValidationFailed,
					fmt.Sprintf("available balance %s on account %s (%s) would exceed max %s",
						avail, acct.Code, acct.ID, *acct.Limits.Max),
					map[string]any{
						"account_id":   acct.ID,
						"account_code": acct.Code,
						"available":    avail.String(),
						"max_limit":    acct.Limits.Max.String(),
					})
				break
			}
		}

		if itemErr != nil {
			results[i] = itemResult{err: itemErr}
			continue
		}

		if req.Tx.IdempotencyKey != "" {
			validIdem = append(validIdem, storage.IdemRecord{
				Ledger:      req.Tx.Ledger,
				Key:         req.Tx.IdempotencyKey,
				RequestHash: req.RequestHash,
				TxID:        req.Tx.ID,
				CreatedAt:   req.Tx.CommittedAt,
			})
		}

		// Intent succeeded: promote working balances and state
		if len(reqs) > 1 {
			req.Tx.CommittedAt = batchCommitTime
			for idx := range itemEntries {
				itemEntries[idx].CommittedAt = batchCommitTime
			}
		}
		for acctID, bal := range workBals {
			balMap[acctID] = bal
			modifiedBals[acctID] = true
		}
		if updatedState != nil {
			stateMap[updatedState.TxID] = *updatedState
			validStates = append(validStates, *updatedState)
		}
		if req.Op == OpPost || req.Op == OpSettleHold || req.Op == OpReverse {
			validTxs = append(validTxs, req.Tx)
		}
		validEntries = append(validEntries, itemEntries...)
		validEvents = append(validEvents, buildOutboxEvent(req))
		validEvents = append(validEvents, req.ExtraEvents...)

		results[i] = itemResult{tx: req.Tx}
		if req.Tx.IdempotencyKey != "" {
			seenIdemInBatch[ik] = i
		}
	}

	// If no intents in the batch succeeded, roll back without committing an empty transaction
	if len(validTxs) == 0 && len(validStates) == 0 && len(validIdem) == 0 {
		_ = tx.Rollback(ctx)
		return results, nil
	}

	// 3. Write all valid rows in one batch to the storage transaction
	if len(validIdem) > 0 {
		if _, err := tx.ClaimIdempotency(ctx, validIdem); err != nil {
			return nil, err
		}
	}
	if len(validTxs) > 0 {
		if err := tx.InsertTransactions(ctx, validTxs); err != nil {
			return nil, err
		}
	}
	if len(validEntries) > 0 {
		if err := tx.InsertEntries(ctx, validEntries); err != nil {
			return nil, err
		}
	}
	if len(validStates) > 0 {
		if err := tx.SetTxState(ctx, validStates); err != nil {
			return nil, err
		}
	}
	if len(modifiedBals) > 0 {
		balsToWrite := make([]core.Balance, 0, len(modifiedBals))
		for id := range modifiedBals {
			balsToWrite = append(balsToWrite, balMap[id])
		}
		sort.Slice(balsToWrite, func(i, j int) bool { return balsToWrite[i].Account < balsToWrite[j].Account })
		if err := tx.WriteBalances(ctx, balsToWrite); err != nil {
			return nil, err
		}
	}
	if len(validEvents) > 0 {
		if err := tx.AppendOutbox(ctx, validEvents); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return results, nil
}

func applyPost(req CommitRequest, workBals map[core.AccountID]core.Balance) ([]core.Entry, error) {
	tx := req.Tx
	if tx.Status == core.StatusPending {
		// Hold: move amounts into PendingDebits / PendingCredits
		for _, p := range tx.Postings {
			if a := req.Accounts[p.Debit]; a.BalanceMode == core.Locked {
				b := workBals[p.Debit]
				sum, err := b.PendingDebits.Add(p.Amount)
				if err != nil {
					return nil, core.NewError(core.CodeValidationFailed, "balance overflow", nil)
				}
				b.PendingDebits = sum
				b.Version++
				workBals[p.Debit] = b
			}
			if a := req.Accounts[p.Credit]; a.BalanceMode == core.Locked {
				b := workBals[p.Credit]
				sum, err := b.PendingCredits.Add(p.Amount)
				if err != nil {
					return nil, core.NewError(core.CodeValidationFailed, "balance overflow", nil)
				}
				b.PendingCredits = sum
				b.Version++
				workBals[p.Credit] = b
			}
		}
		return nil, nil
	}

	// Settled transaction: update PostedDebits / PostedCredits and generate Entries
	entries := make([]core.Entry, 0, len(tx.Postings)*2)
	for idx, p := range tx.Postings {
		// Debit leg
		debitAcct := req.Accounts[p.Debit]
		var debitPostedAfter *money.Amount
		var debitVer uint64
		if debitAcct.BalanceMode == core.Locked {
			b := workBals[p.Debit]
			sum, err := b.PostedDebits.Add(p.Amount)
			if err != nil {
				return nil, core.NewError(core.CodeValidationFailed, "balance overflow", nil)
			}
			b.PostedDebits = sum
			b.Version++
			workBals[p.Debit] = b
			pa := b.Posted(debitAcct.Normal)
			debitPostedAfter = &pa
			debitVer = b.Version
		}
		entries = append(entries, core.Entry{
			Transaction:  tx.ID,
			PostingIndex: uint16(idx),
			Account:      p.Debit,
			Side:         core.Debit,
			Amount:       p.Amount,
			PostedAfter:  debitPostedAfter,
			Version:      debitVer,
			EffectiveAt:  tx.EffectiveAt,
			CommittedAt:  tx.CommittedAt,
		})

		// Credit leg
		creditAcct := req.Accounts[p.Credit]
		var creditPostedAfter *money.Amount
		var creditVer uint64
		if creditAcct.BalanceMode == core.Locked {
			b := workBals[p.Credit]
			sum, err := b.PostedCredits.Add(p.Amount)
			if err != nil {
				return nil, core.NewError(core.CodeValidationFailed, "balance overflow", nil)
			}
			b.PostedCredits = sum
			b.Version++
			workBals[p.Credit] = b
			pa := b.Posted(creditAcct.Normal)
			creditPostedAfter = &pa
			creditVer = b.Version
		}
		entries = append(entries, core.Entry{
			Transaction:  tx.ID,
			PostingIndex: uint16(idx),
			Account:      p.Credit,
			Side:         core.Credit,
			Amount:       p.Amount,
			PostedAfter:  creditPostedAfter,
			Version:      creditVer,
			EffectiveAt:  tx.EffectiveAt,
			CommittedAt:  tx.CommittedAt,
		})
	}
	return entries, nil
}

func applySettleHold(
	req CommitRequest,
	workBals map[core.AccountID]core.Balance,
	st storage.TxState,
) ([]core.Entry, *storage.TxState, error) {
	if st.Status != core.StatusPending {
		return nil, nil, core.NewError(core.CodeValidationFailed,
			fmt.Sprintf("hold %s is not pending (status: %s)", req.TargetTx.ID, st.Status),
			map[string]any{"tx_id": req.TargetTx.ID, "status": st.Status.String()})
	}
	// Release all pending amounts from the original hold first
	for _, hp := range req.TargetTx.Postings {
		if a := req.Accounts[hp.Debit]; a.BalanceMode == core.Locked {
			b := workBals[hp.Debit]
			b.PendingDebits, _ = b.PendingDebits.Sub(hp.Amount)
			workBals[hp.Debit] = b
		}
		if a := req.Accounts[hp.Credit]; a.BalanceMode == core.Locked {
			b := workBals[hp.Credit]
			b.PendingCredits, _ = b.PendingCredits.Sub(hp.Amount)
			workBals[hp.Credit] = b
		}
	}
	// Apply the settled postings
	entries, err := applyPost(req, workBals)
	if err != nil {
		return nil, nil, err
	}
	st.Status = core.StatusPosted
	return entries, &st, nil
}

func applyVoidOrExpireHold(
	req CommitRequest,
	workBals map[core.AccountID]core.Balance,
	st storage.TxState,
) (*storage.TxState, error) {
	if st.Status != core.StatusPending {
		return nil, core.NewError(core.CodeValidationFailed,
			fmt.Sprintf("hold %s is not pending (status: %s)", req.TargetTx.ID, st.Status),
			map[string]any{"tx_id": req.TargetTx.ID, "status": st.Status.String()})
	}
	for _, hp := range req.TargetTx.Postings {
		if a := req.Accounts[hp.Debit]; a.BalanceMode == core.Locked {
			b := workBals[hp.Debit]
			b.PendingDebits, _ = b.PendingDebits.Sub(hp.Amount)
			b.Version++
			workBals[hp.Debit] = b
		}
		if a := req.Accounts[hp.Credit]; a.BalanceMode == core.Locked {
			b := workBals[hp.Credit]
			b.PendingCredits, _ = b.PendingCredits.Sub(hp.Amount)
			b.Version++
			workBals[hp.Credit] = b
		}
	}
	if req.Op == OpExpireHold {
		st.Status = core.StatusExpired
	} else {
		st.Status = core.StatusVoided
	}
	return &st, nil
}

func applyReverse(
	req CommitRequest,
	workBals map[core.AccountID]core.Balance,
	st storage.TxState,
) ([]core.Entry, *storage.TxState, error) {
	if st.Status != core.StatusPosted {
		return nil, nil, core.NewError(core.CodeValidationFailed,
			fmt.Sprintf("transaction %s cannot be reversed (status: %s)", req.TargetTx.ID, st.Status),
			map[string]any{"tx_id": req.TargetTx.ID})
	}
	if len(st.Reversed) < len(req.TargetTx.Postings) {
		padded := make([]money.Amount, len(req.TargetTx.Postings))
		copy(padded, st.Reversed)
		st.Reversed = padded
	}
	// Check Invariant I7: Reversed amounts never exceed the original, per posting.
	newReversed := make([]money.Amount, len(st.Reversed))
	copy(newReversed, st.Reversed)
	for idx, addRev := range req.ReversalAmts {
		if addRev.IsZero() {
			continue
		}
		origAmt := req.TargetTx.Postings[idx].Amount
		cumRev, err := newReversed[idx].Add(addRev)
		if err != nil || cumRev.Cmp(origAmt) > 0 {
			return nil, nil, core.NewError(core.CodeValidationFailed,
				fmt.Sprintf("reversal on posting %d exceeds original amount %s (already reversed %s, requested %s)",
					idx, origAmt, newReversed[idx], addRev),
				map[string]any{
					"tx_id":         req.TargetTx.ID,
					"posting_index": idx,
					"original":      origAmt.String(),
					"reversed":      newReversed[idx].String(),
					"requested":     addRev.String(),
				})
		}
		newReversed[idx] = cumRev
	}

	entries, err := applyPost(req, workBals)
	if err != nil {
		return nil, nil, err
	}
	st.Reversed = newReversed
	return entries, &st, nil
}

func buildOutboxEvent(req CommitRequest) events.Event {
	var evType string
	switch req.Op {
	case OpPost:
		if req.Tx.Status == core.StatusPending {
			evType = events.TypeTransactionPending
		} else {
			evType = events.TypeTransactionCommitted
		}
	case OpSettleHold:
		evType = events.TypeTransactionPosted
	case OpVoidHold:
		evType = events.TypeTransactionVoided
	case OpExpireHold:
		evType = events.TypeTransactionExpired
	case OpReverse:
		evType = events.TypeTransactionReversed
	}
	payload, _ := json.Marshal(req.Tx)
	return events.Event{
		ID:        req.EventID,
		Ledger:    req.Tx.Ledger,
		Type:      evType,
		Payload:   payload,
		CreatedAt: req.Tx.CommittedAt,
	}
}
