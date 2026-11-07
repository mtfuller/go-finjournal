// Package memory provides an in-memory reference implementation of storage.Store
// with deterministic fault-injection hooks for simulation testing.
package memory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage"
)

// ErrSimulatedCrash is returned when CrashAfterCommit is armed: the transaction
// has been durably committed, but the caller sees an error (simulating a network drop after commit).
var ErrSimulatedCrash = errors.New("memory: simulated crash after commit")

type idemKey struct {
	ledger core.LedgerID
	key    string
}

type assetKey struct {
	ledger core.LedgerID
	id     money.AssetID
}

type codeKey struct {
	ledger core.LedgerID
	code   string
}

type holderRefKey struct {
	ledger core.LedgerID
	ref    string
}

// Store is a thread-safe in-memory implementation of storage.Store.
type Store struct {
	txMu sync.Mutex // serialises open write transactions (like SQLite BEGIN IMMEDIATE / row locks)
	mu   sync.RWMutex

	ledgers       map[core.LedgerID]core.Ledger
	assets        map[assetKey]money.Asset
	holders       map[core.HolderID]core.AccountHolder
	holderRefs    map[holderRefKey]core.HolderID
	accounts      map[core.AccountID]core.Account
	accountCodes  map[codeKey]core.AccountID
	balances      map[core.AccountID]core.Balance
	transactions  map[core.TransactionID]*core.Transaction
	txOrder       []core.TransactionID
	txStates      map[core.TransactionID]storage.TxState
	entriesByAcct map[core.AccountID][]core.Entry
	foldedIndex   map[core.AccountID]int // index in entriesByAcct up to which Deferred entries are folded
	idempotency   map[idemKey]storage.IdemRecord
	outbox        []events.Event
	outboxSeq     uint64
	cursors       map[string]uint64
	journal       map[core.LedgerID][]core.JournalRecord
	sealedTxs     map[core.TransactionID]bool
	checkpoints   map[core.LedgerID][]core.Checkpoint
	periods       map[core.LedgerID][]core.Period

	// Fault injection state
	faultMu          sync.Mutex
	failNext         map[string]error
	delays           map[string]time.Duration
	crashAfterCommit bool
}

// New creates a new in-memory Store.
func New() *Store {
	return &Store{
		ledgers:       make(map[core.LedgerID]core.Ledger),
		assets:        make(map[assetKey]money.Asset),
		holders:       make(map[core.HolderID]core.AccountHolder),
		holderRefs:    make(map[holderRefKey]core.HolderID),
		accounts:      make(map[core.AccountID]core.Account),
		accountCodes:  make(map[codeKey]core.AccountID),
		balances:      make(map[core.AccountID]core.Balance),
		transactions:  make(map[core.TransactionID]*core.Transaction),
		txStates:      make(map[core.TransactionID]storage.TxState),
		entriesByAcct: make(map[core.AccountID][]core.Entry),
		foldedIndex:   make(map[core.AccountID]int),
		idempotency:   make(map[idemKey]storage.IdemRecord),
		cursors:       make(map[string]uint64),
		journal:       make(map[core.LedgerID][]core.JournalRecord),
		sealedTxs:     make(map[core.TransactionID]bool),
		checkpoints:   make(map[core.LedgerID][]core.Checkpoint),
		periods:       make(map[core.LedgerID][]core.Period),
		failNext:      make(map[string]error),
		delays:        make(map[string]time.Duration),
	}
}

// FailNext arms a one-shot error for operation op (e.g. "Commit", "LockBalances", "InsertTransactions").
func (s *Store) FailNext(op string, err error) {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	if err == nil {
		err = errors.New("memory: injected fault on " + op)
	}
	s.failNext[op] = err
}

// CrashAfterCommit arms a one-shot simulated crash right after the next Tx commits its data.
func (s *Store) CrashAfterCommit() {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	s.crashAfterCommit = true
}

// Delay sets a latency injection for operation op. Pass 0 to clear.
func (s *Store) Delay(op string, d time.Duration) {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	if d <= 0 {
		delete(s.delays, op)
	} else {
		s.delays[op] = d
	}
}

func (s *Store) checkFault(op string) error {
	s.faultMu.Lock()
	d := s.delays[op]
	err, hasErr := s.failNext[op]
	if hasErr {
		delete(s.failNext, op)
	}
	s.faultMu.Unlock()

	if d > 0 {
		time.Sleep(d)
	}
	if hasErr {
		return err
	}
	return nil
}

func (s *Store) Admin() storage.Admin         { return s }
func (s *Store) Outbox() storage.Outbox       { return s }
func (s *Store) Journal() storage.Journal     { return s }
func (s *Store) Batch() storage.BatchPoster   { return nil }
func (s *Store) Close() error                 { return nil }

func (s *Store) Join(_ context.Context, _ any) (storage.Tx, error) {
	return nil, storage.ErrJoinUnsupported
}

func (s *Store) Begin(ctx context.Context) (storage.Tx, error) {
	if err := s.checkFault("Begin"); err != nil {
		return nil, err
	}
	s.txMu.Lock()
	return &memTx{
		store:       s,
		stagedIdem:  make(map[idemKey]storage.IdemRecord),
		stagedState: make(map[core.TransactionID]storage.TxState),
		stagedBal:   make(map[core.AccountID]core.Balance),
	}, nil
}

type memTx struct {
	store       *Store
	done        bool
	stagedIdem  map[idemKey]storage.IdemRecord
	stagedTxs   []*core.Transaction
	stagedEnts  []core.Entry
	stagedState map[core.TransactionID]storage.TxState
	stagedBal   map[core.AccountID]core.Balance
	stagedEvs   []events.Event
}

func (tx *memTx) LockBalances(_ context.Context, ids []core.AccountID) ([]core.Balance, error) {
	if err := tx.store.checkFault("LockBalances"); err != nil {
		return nil, err
	}
	sorted := make([]core.AccountID, len(ids))
	copy(sorted, ids)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	tx.store.mu.RLock()
	defer tx.store.mu.RUnlock()

	out := make([]core.Balance, 0, len(sorted))
	seen := make(map[core.AccountID]bool, len(sorted))
	for _, id := range sorted {
		if seen[id] {
			continue
		}
		seen[id] = true
		if b, ok := tx.stagedBal[id]; ok {
			out = append(out, b)
			continue
		}
		b, ok := tx.store.balances[id]
		if !ok {
			return nil, fmt.Errorf("%w: account %s", storage.ErrNotFound, id)
		}
		out = append(out, b)
	}
	return out, nil
}

func (tx *memTx) ClaimIdempotency(_ context.Context, recs []storage.IdemRecord) ([]storage.IdemRecord, error) {
	if err := tx.store.checkFault("ClaimIdempotency"); err != nil {
		return nil, err
	}
	tx.store.mu.RLock()
	defer tx.store.mu.RUnlock()

	var existing []storage.IdemRecord
	for _, r := range recs {
		k := idemKey{ledger: r.Ledger, key: r.Key}
		if ex, ok := tx.stagedIdem[k]; ok {
			existing = append(existing, copyIdemRecord(ex))
			continue
		}
		if ex, ok := tx.store.idempotency[k]; ok {
			existing = append(existing, copyIdemRecord(ex))
			continue
		}
		tx.stagedIdem[k] = copyIdemRecord(r)
	}
	return existing, nil
}

func (tx *memTx) InsertTransactions(_ context.Context, txs []*core.Transaction) error {
	if err := tx.store.checkFault("InsertTransactions"); err != nil {
		return err
	}
	for _, t := range txs {
		tx.stagedTxs = append(tx.stagedTxs, cloneTransaction(t))
	}
	return nil
}

func (tx *memTx) InsertEntries(_ context.Context, es []core.Entry) error {
	if err := tx.store.checkFault("InsertEntries"); err != nil {
		return err
	}
	for _, e := range es {
		tx.stagedEnts = append(tx.stagedEnts, cloneEntry(e))
	}
	return nil
}

func (tx *memTx) GetTxStates(_ context.Context, ids []core.TransactionID) (map[core.TransactionID]storage.TxState, error) {
	if err := tx.store.checkFault("GetTxStates"); err != nil {
		return nil, err
	}
	tx.store.mu.RLock()
	defer tx.store.mu.RUnlock()

	out := make(map[core.TransactionID]storage.TxState, len(ids))
	for _, id := range ids {
		if st, ok := tx.stagedState[id]; ok {
			out[id] = cloneTxState(st)
			continue
		}
		st, ok := tx.store.txStates[id]
		if !ok {
			return nil, fmt.Errorf("%w: tx_state %s", storage.ErrNotFound, id)
		}
		out[id] = cloneTxState(st)
	}
	return out, nil
}

func (tx *memTx) SetTxState(_ context.Context, st []storage.TxState) error {
	if err := tx.store.checkFault("SetTxState"); err != nil {
		return err
	}
	for _, s := range st {
		tx.stagedState[s.TxID] = cloneTxState(s)
	}
	return nil
}

func (tx *memTx) WriteBalances(_ context.Context, bs []core.Balance) error {
	if err := tx.store.checkFault("WriteBalances"); err != nil {
		return err
	}
	for _, b := range bs {
		tx.stagedBal[b.Account] = b
	}
	return nil
}

func (tx *memTx) AppendOutbox(_ context.Context, evs []events.Event) error {
	if err := tx.store.checkFault("AppendOutbox"); err != nil {
		return err
	}
	tx.stagedEvs = append(tx.stagedEvs, evs...)
	return nil
}

func (tx *memTx) Commit(_ context.Context) error {
	if tx.done {
		return errors.New("memory: transaction already finished")
	}
	tx.done = true
	defer tx.store.txMu.Unlock()

	if err := tx.store.checkFault("Commit"); err != nil {
		return err
	}

	tx.store.mu.Lock()
	for k, r := range tx.stagedIdem {
		tx.store.idempotency[k] = r
	}
	for _, t := range tx.stagedTxs {
		tx.store.transactions[t.ID] = t
		tx.store.txOrder = append(tx.store.txOrder, t.ID)
		if _, hasState := tx.stagedState[t.ID]; !hasState {
			tx.store.txStates[t.ID] = storage.TxState{
				TxID:     t.ID,
				Status:   t.Status,
				Reversed: make([]money.Amount, len(t.Postings)),
			}
		}
	}
	for id, st := range tx.stagedState {
		tx.store.txStates[id] = st
		if t, ok := tx.store.transactions[id]; ok {
			t.Status = st.Status
		}
	}
	for _, e := range tx.stagedEnts {
		tx.store.entriesByAcct[e.Account] = append(tx.store.entriesByAcct[e.Account], e)
	}
	for id, b := range tx.stagedBal {
		tx.store.balances[id] = b
	}
	for _, ev := range tx.stagedEvs {
		tx.store.outboxSeq++
		ev.Seq = tx.store.outboxSeq
		cp := make([]byte, len(ev.Payload))
		copy(cp, ev.Payload)
		ev.Payload = cp
		tx.store.outbox = append(tx.store.outbox, ev)
	}
	tx.store.mu.Unlock()

	tx.store.faultMu.Lock()
	crash := tx.store.crashAfterCommit
	tx.store.crashAfterCommit = false
	tx.store.faultMu.Unlock()
	if crash {
		return ErrSimulatedCrash
	}
	return nil
}

func (tx *memTx) Rollback(_ context.Context) error {
	if tx.done {
		return nil
	}
	tx.done = true
	tx.store.txMu.Unlock()
	return nil
}

// --- Admin methods ---

func (s *Store) CreateLedger(_ context.Context, l core.Ledger) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.ledgers[l.ID]; exists {
		return fmt.Errorf("%w: ledger %s", storage.ErrAlreadyExists, l.ID)
	}
	l.Metadata = l.Metadata.Clone()
	s.ledgers[l.ID] = l
	return nil
}

func (s *Store) RegisterAsset(_ context.Context, l core.LedgerID, a money.Asset) error {
	if err := a.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ledgers[l]; !ok {
		return fmt.Errorf("%w: ledger %s", storage.ErrNotFound, l)
	}
	k := assetKey{ledger: l, id: a.ID()}
	if _, exists := s.assets[k]; exists {
		return fmt.Errorf("%w: asset %s", storage.ErrAlreadyExists, a.ID())
	}
	s.assets[k] = a
	return nil
}

func (s *Store) CreateHolder(_ context.Context, h core.AccountHolder) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ledgers[h.Ledger]; !ok {
		return fmt.Errorf("%w: ledger %s", storage.ErrNotFound, h.Ledger)
	}
	if _, exists := s.holders[h.ID]; exists {
		return fmt.Errorf("%w: holder %s", storage.ErrAlreadyExists, h.ID)
	}
	rk := holderRefKey{ledger: h.Ledger, ref: h.ExternalRef}
	if _, exists := s.holderRefs[rk]; exists {
		return fmt.Errorf("%w: holder external_ref %s", storage.ErrAlreadyExists, h.ExternalRef)
	}
	h.Metadata = h.Metadata.Clone()
	s.holders[h.ID] = h
	s.holderRefs[rk] = h.ID
	return nil
}

func (s *Store) CreateAccount(_ context.Context, a core.Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ledgers[a.Ledger]; !ok {
		return fmt.Errorf("%w: ledger %s", storage.ErrNotFound, a.Ledger)
	}
	if _, ok := s.assets[assetKey{ledger: a.Ledger, id: a.Asset}]; !ok {
		return fmt.Errorf("%w: asset %s", storage.ErrNotFound, a.Asset)
	}
	if a.Holder != nil {
		if _, ok := s.holders[*a.Holder]; !ok {
			return fmt.Errorf("%w: holder %s", storage.ErrNotFound, *a.Holder)
		}
	}
	if _, exists := s.accounts[a.ID]; exists {
		return fmt.Errorf("%w: account %s", storage.ErrAlreadyExists, a.ID)
	}
	ck := codeKey{ledger: a.Ledger, code: a.Code}
	if _, exists := s.accountCodes[ck]; exists {
		return fmt.Errorf("%w: account code %s", storage.ErrAlreadyExists, a.Code)
	}
	if a.BalanceMode == core.Deferred && (a.Limits.Min != nil || a.Limits.Max != nil) {
		return core.NewError(core.CodeValidationFailed, "deferred account cannot have limits", nil)
	}
	cp := cloneAccount(a)
	s.accounts[cp.ID] = cp
	s.accountCodes[ck] = cp.ID
	s.balances[cp.ID] = core.Balance{
		Account: cp.ID,
		Asset:   cp.Asset,
	}
	return nil
}

func (s *Store) UpdateAccount(_ context.Context, id core.AccountID, patch storage.AccountPatch) (*core.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.accounts[id]
	if !ok {
		return nil, fmt.Errorf("%w: account %s", storage.ErrNotFound, id)
	}
	if patch.Status != nil {
		a.Status = *patch.Status
	}
	if patch.Limits != nil {
		if a.BalanceMode == core.Deferred && (patch.Limits.Min != nil || patch.Limits.Max != nil) {
			return nil, core.NewError(core.CodeValidationFailed, "deferred account cannot have limits", nil)
		}
		a.Limits = cloneLimits(*patch.Limits)
	}
	if patch.Metadata != nil {
		if a.Metadata == nil {
			a.Metadata = make(core.Metadata)
		}
		for k, v := range patch.Metadata {
			a.Metadata[k] = v
		}
	}
	s.accounts[id] = a
	cp := cloneAccount(a)
	return &cp, nil
}

func (s *Store) CreatePeriod(_ context.Context, p core.Period) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ex := range s.periods[p.Ledger] {
		if ex.StartsAt.Equal(p.StartsAt) {
			return fmt.Errorf("%w: period starting at %s", storage.ErrAlreadyExists, p.StartsAt)
		}
	}
	s.periods[p.Ledger] = append(s.periods[p.Ledger], p)
	return nil
}

func (s *Store) ClosePeriod(_ context.Context, l core.LedgerID, startsAt time.Time, closedAt time.Time) (*core.Period, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ps := s.periods[l]
	for i := range ps {
		if ps[i].StartsAt.Equal(startsAt) {
			c := closedAt.UTC()
			ps[i].ClosedAt = &c
			cp := ps[i]
			return &cp, nil
		}
	}
	p := core.Period{
		Ledger:   l,
		StartsAt: startsAt.UTC(),
		EndsAt:   startsAt.UTC().AddDate(0, 1, 0),
		ClosedAt: &closedAt,
	}
	s.periods[l] = append(s.periods[l], p)
	return &p, nil
}

func (s *Store) PruneIdempotency(_ context.Context, before time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pruned := 0
	for k, r := range s.idempotency {
		if r.CreatedAt.Before(before) {
			delete(s.idempotency, k)
			pruned++
		}
	}
	return pruned, nil
}

// --- Reader methods ---

func (s *Store) Ledger(_ context.Context, id core.LedgerID) (*core.Ledger, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l, ok := s.ledgers[id]
	if !ok {
		return nil, fmt.Errorf("%w: ledger %s", storage.ErrNotFound, id)
	}
	cp := l
	cp.Metadata = l.Metadata.Clone()
	return &cp, nil
}

func (s *Store) Asset(_ context.Context, l core.LedgerID, id money.AssetID) (*money.Asset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.assets[assetKey{ledger: l, id: id}]
	if !ok {
		return nil, fmt.Errorf("%w: asset %s", storage.ErrNotFound, id)
	}
	cp := a
	return &cp, nil
}

func (s *Store) Assets(_ context.Context, l core.LedgerID) ([]money.Asset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []money.Asset
	for k, a := range s.assets {
		if k.ledger == l {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out, nil
}

func (s *Store) Holder(_ context.Context, id core.HolderID) (*core.AccountHolder, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.holders[id]
	if !ok {
		return nil, fmt.Errorf("%w: holder %s", storage.ErrNotFound, id)
	}
	cp := h
	cp.Metadata = h.Metadata.Clone()
	return &cp, nil
}

func (s *Store) Accounts(_ context.Context, ids []core.AccountID) ([]core.Account, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]core.Account, 0, len(ids))
	for _, id := range ids {
		a, ok := s.accounts[id]
		if !ok {
			return nil, fmt.Errorf("%w: account %s", storage.ErrNotFound, id)
		}
		out = append(out, cloneAccount(a))
	}
	return out, nil
}

func (s *Store) AccountByCode(_ context.Context, l core.LedgerID, code string) (*core.Account, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.accountCodes[codeKey{ledger: l, code: code}]
	if !ok {
		return nil, fmt.Errorf("%w: account code %s", storage.ErrNotFound, code)
	}
	a := cloneAccount(s.accounts[id])
	return &a, nil
}

func (s *Store) ListAccounts(_ context.Context, l core.LedgerID) ([]core.Account, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []core.Account
	for _, a := range s.accounts {
		if a.Ledger == l {
			out = append(out, cloneAccount(a))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *Store) Balance(_ context.Context, id core.AccountID) (core.Balance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.balances[id]
	if !ok {
		return core.Balance{}, fmt.Errorf("%w: account %s", storage.ErrNotFound, id)
	}
	return b, nil
}

func (s *Store) BalanceAt(_ context.Context, id core.AccountID, at time.Time) (money.Amount, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.accounts[id]
	if !ok {
		return money.Zero, fmt.Errorf("%w: account %s", storage.ErrNotFound, id)
	}
	entries := s.entriesByAcct[id]
	if a.BalanceMode == core.Locked {
		var best *core.Entry
		for i := range entries {
			e := &entries[i]
			if !e.CommittedAt.After(at) {
				if best == nil || e.CommittedAt.After(best.CommittedAt) ||
					(e.CommittedAt.Equal(best.CommittedAt) && e.Version > best.Version) {
					best = e
				}
			}
		}
		if best == nil || best.PostedAfter == nil {
			return money.Zero, nil
		}
		return *best.PostedAfter, nil
	}
	// Deferred account: sum entries at or before `at`
	debits := money.Zero
	credits := money.Zero
	for _, e := range entries {
		if !e.CommittedAt.After(at) {
			if e.Side == core.Debit {
				debits, _ = debits.Add(e.Amount)
			} else {
				credits, _ = credits.Add(e.Amount)
			}
		}
	}
	if a.Normal == core.Debit {
		res, _ := debits.Sub(credits)
		return res, nil
	}
	res, _ := credits.Sub(debits)
	return res, nil
}

func (s *Store) HolderBalances(_ context.Context, h core.HolderID) ([]core.HolderBalance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.holders[h]; !ok {
		return nil, fmt.Errorf("%w: holder %s", storage.ErrNotFound, h)
	}
	type groupKey struct {
		asset  money.AssetID
		normal core.Side
	}
	groups := make(map[groupKey]*core.HolderBalance)
	for _, a := range s.accounts {
		if a.Holder == nil || *a.Holder != h {
			continue
		}
		b := s.balances[a.ID]
		k := groupKey{asset: a.Asset, normal: a.Normal}
		gb, exists := groups[k]
		if !exists {
			gb = &core.HolderBalance{
				Holder:    h,
				Asset:     a.Asset,
				Normal:    a.Normal,
				Posted:    money.Zero,
				Available: money.Zero,
			}
			groups[k] = gb
		}
		gb.Posted, _ = gb.Posted.Add(b.Posted(a.Normal))
		gb.Available, _ = gb.Available.Add(b.Available(a.Normal))
		gb.Accounts++
	}
	out := make([]core.HolderBalance, 0, len(groups))
	for _, gb := range groups {
		out = append(out, *gb)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Asset != out[j].Asset {
			return out[i].Asset < out[j].Asset
		}
		return out[i].Normal < out[j].Normal
	})
	return out, nil
}

func (s *Store) Transaction(_ context.Context, id core.TransactionID) (*core.Transaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.transactions[id]
	if !ok {
		return nil, fmt.Errorf("%w: transaction %s", storage.ErrNotFound, id)
	}
	cp := cloneTransaction(t)
	if st, ok := s.txStates[id]; ok {
		cp.Status = st.Status
	}
	return cp, nil
}

func (s *Store) ListTransactions(_ context.Context, l core.LedgerID) ([]*core.Transaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*core.Transaction
	for _, id := range s.txOrder {
		t := s.transactions[id]
		if t.Ledger == l {
			cp := cloneTransaction(t)
			if st, ok := s.txStates[id]; ok {
				cp.Status = st.Status
			}
			out = append(out, cp)
		}
	}
	return out, nil
}

func (s *Store) TxState(_ context.Context, id core.TransactionID) (*storage.TxState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.txStates[id]
	if !ok {
		return nil, fmt.Errorf("%w: tx_state %s", storage.ErrNotFound, id)
	}
	cp := cloneTxState(st)
	return &cp, nil
}

func (s *Store) PendingExpiredTransactions(_ context.Context, now time.Time, limit int) ([]*core.Transaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*core.Transaction
	for _, id := range s.txOrder {
		st, ok := s.txStates[id]
		if !ok || st.Status != core.StatusPending {
			continue
		}
		t := s.transactions[id]
		if t.ExpiresAt != nil && !t.ExpiresAt.After(now) {
			cp := cloneTransaction(t)
			cp.Status = st.Status
			out = append(out, cp)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *Store) Entries(_ context.Context, q storage.EntryQuery) (storage.Page[core.Entry], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.accounts[q.Account]; !ok {
		return storage.Page[core.Entry]{}, fmt.Errorf("%w: account %s", storage.ErrNotFound, q.Account)
	}
	all := s.entriesByAcct[q.Account]
	// Newest first (reverse order)
	rev := make([]core.Entry, len(all))
	for i := range all {
		rev[i] = cloneEntry(all[len(all)-1-i])
	}
	start := 0
	if q.Cursor != "" {
		idx, err := strconv.Atoi(q.Cursor)
		if err == nil && idx >= 0 && idx <= len(rev) {
			start = idx
		}
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	end := start + limit
	var nextCursor string
	if end < len(rev) {
		nextCursor = strconv.Itoa(end)
	} else {
		end = len(rev)
	}
	return storage.Page[core.Entry]{
		Items:      rev[start:end],
		NextCursor: nextCursor,
	}, nil
}

func (s *Store) Idempotency(_ context.Context, l core.LedgerID, key string) (*storage.IdemRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.idempotency[idemKey{ledger: l, key: key}]
	if !ok {
		return nil, fmt.Errorf("%w: idempotency key %s", storage.ErrNotFound, key)
	}
	cp := copyIdemRecord(r)
	return &cp, nil
}

func (s *Store) Periods(_ context.Context, l core.LedgerID) ([]core.Period, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ps := s.periods[l]
	out := make([]core.Period, len(ps))
	copy(out, ps)
	return out, nil
}

// FoldDeferredBalances folds new entries for Deferred accounts in ledger l into their balance rows.
func (s *Store) FoldDeferredBalances(_ context.Context, l core.LedgerID) (int, error) {
	s.txMu.Lock()
	defer s.txMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	folded := 0
	for id, a := range s.accounts {
		if a.Ledger != l || a.BalanceMode != core.Deferred {
			continue
		}
		entries := s.entriesByAcct[id]
		start := s.foldedIndex[id]
		if start >= len(entries) {
			continue
		}
		bal := s.balances[id]
		for i := start; i < len(entries); i++ {
			e := &entries[i]
			if e.Side == core.Debit {
				bal.PostedDebits, _ = bal.PostedDebits.Add(e.Amount)
			} else {
				bal.PostedCredits, _ = bal.PostedCredits.Add(e.Amount)
			}
			bal.Version++
			postedAfter := bal.Posted(a.Normal)
			e.PostedAfter = &postedAfter
			e.Version = bal.Version
			folded++
		}
		s.balances[id] = bal
		s.foldedIndex[id] = len(entries)
	}
	return folded, nil
}

// --- Outbox methods ---

func (s *Store) FetchAfter(_ context.Context, l core.LedgerID, afterSeq uint64, limit int) ([]events.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 {
		limit = 100
	}
	var out []events.Event
	for _, ev := range s.outbox {
		if ev.Ledger == l && ev.Seq > afterSeq {
			out = append(out, ev)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *Store) FetchAllAfter(_ context.Context, afterSeq uint64, limit int) ([]events.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 {
		limit = 100
	}
	var out []events.Event
	for _, ev := range s.outbox {
		if ev.Seq > afterSeq {
			out = append(out, ev)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *Store) GetCursor(_ context.Context, subscriber string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cursors[subscriber], nil
}

func (s *Store) SetCursor(_ context.Context, subscriber string, seq uint64, updatedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursors[subscriber] = seq
	// Mark events delivered when all cursors have passed them
	if len(s.cursors) > 0 {
		minSeq := ^uint64(0)
		for _, c := range s.cursors {
			if c < minSeq {
				minSeq = c
			}
		}
		for i := range s.outbox {
			if s.outbox[i].Seq <= minSeq && s.outbox[i].DeliveredAt == nil {
				t := updatedAt
				s.outbox[i].DeliveredAt = &t
			}
		}
	}
	return nil
}

func (s *Store) PruneDelivered(_ context.Context, before time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pruned := 0
	kept := s.outbox[:0]
	for _, ev := range s.outbox {
		if ev.DeliveredAt != nil && ev.DeliveredAt.Before(before) {
			pruned++
			continue
		}
		kept = append(kept, ev)
	}
	s.outbox = kept
	return pruned, nil
}

// --- Journal methods ---

func (s *Store) UnsealedTransactions(_ context.Context, l core.LedgerID, limit int) ([]*core.Transaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 {
		limit = 500
	}
	var out []*core.Transaction
	for _, id := range s.txOrder {
		if s.sealedTxs[id] {
			continue
		}
		t := s.transactions[id]
		if t.Ledger == l {
			out = append(out, cloneTransaction(t))
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *Store) LatestRecord(_ context.Context, l core.LedgerID) (*core.JournalRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	recs := s.journal[l]
	if len(recs) == 0 {
		return nil, nil
	}
	last := cloneJournalRecord(recs[len(recs)-1])
	return &last, nil
}

func (s *Store) AppendRecords(_ context.Context, records []core.JournalRecord, checkpoint *core.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range records {
		if s.sealedTxs[r.TxID] {
			continue
		}
		s.sealedTxs[r.TxID] = true
		s.journal[r.Ledger] = append(s.journal[r.Ledger], cloneJournalRecord(r))
	}
	if checkpoint != nil {
		cp := *checkpoint
		cp.ChainHash = bytes.Clone(checkpoint.ChainHash)
		s.checkpoints[cp.Ledger] = append(s.checkpoints[cp.Ledger], cp)
	}
	return nil
}

func (s *Store) Records(_ context.Context, l core.LedgerID, afterSeq uint64, limit int) ([]core.JournalRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	recs := s.journal[l]
	var out []core.JournalRecord
	for _, r := range recs {
		if r.Seq > afterSeq {
			out = append(out, cloneJournalRecord(r))
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *Store) Checkpoints(_ context.Context, l core.LedgerID) ([]core.Checkpoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cps := s.checkpoints[l]
	out := make([]core.Checkpoint, len(cps))
	for i, c := range cps {
		out[i] = c
		out[i].ChainHash = bytes.Clone(c.ChainHash)
	}
	return out, nil
}

// --- Cloning helpers ---

func copyIdemRecord(r storage.IdemRecord) storage.IdemRecord {
	r.RequestHash = bytes.Clone(r.RequestHash)
	return r
}

func cloneLimits(l core.Limits) core.Limits {
	var out core.Limits
	if l.Min != nil {
		v := *l.Min
		out.Min = &v
	}
	if l.Max != nil {
		v := *l.Max
		out.Max = &v
	}
	return out
}

func cloneAccount(a core.Account) core.Account {
	a.Limits = cloneLimits(a.Limits)
	a.Metadata = a.Metadata.Clone()
	if a.Holder != nil {
		h := *a.Holder
		a.Holder = &h
	}
	return a
}

func cloneTransaction(t *core.Transaction) *core.Transaction {
	cp := *t
	cp.Postings = make([]core.Posting, len(t.Postings))
	copy(cp.Postings, t.Postings)
	cp.Metadata = t.Metadata.Clone()
	if t.ExpiresAt != nil {
		v := *t.ExpiresAt
		cp.ExpiresAt = &v
	}
	if t.Reverses != nil {
		v := *t.Reverses
		cp.Reverses = &v
	}
	if t.PendingID != nil {
		v := *t.PendingID
		cp.PendingID = &v
	}
	return &cp
}

func cloneEntry(e core.Entry) core.Entry {
	if e.PostedAfter != nil {
		v := *e.PostedAfter
		e.PostedAfter = &v
	}
	return e
}

func cloneTxState(st storage.TxState) storage.TxState {
	cp := st
	cp.Reversed = make([]money.Amount, len(st.Reversed))
	copy(cp.Reversed, st.Reversed)
	return cp
}

func cloneJournalRecord(r core.JournalRecord) core.JournalRecord {
	r.TxHash = bytes.Clone(r.TxHash)
	r.ChainHash = bytes.Clone(r.ChainHash)
	return r
}
