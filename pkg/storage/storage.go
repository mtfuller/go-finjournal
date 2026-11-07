// Package storage defines the batch-shaped Store, Tx, Reader, Admin, Outbox, and
// Journal interfaces implemented by memory, SQLite, and PostgreSQL adapters.
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/money"
)

var (
	// ErrJoinUnsupported is returned when an adapter does not support joining an external transaction.
	ErrJoinUnsupported = errors.New("storage: joining external transaction is unsupported")
	// ErrNotFound is returned when a requested record does not exist.
	ErrNotFound = errors.New("storage: record not found")
	// ErrAlreadyExists is returned when a unique constraint is violated.
	ErrAlreadyExists = errors.New("storage: record already exists")
)

// IdemRecord represents a stored idempotency claim for a ledger and key.
type IdemRecord struct {
	Ledger      core.LedgerID      `json:"ledger"`
	Key         string             `json:"key"`
	RequestHash []byte             `json:"request_hash"`
	TxID        core.TransactionID `json:"tx_id"`
	CreatedAt   time.Time          `json:"created_at"`
}

// TxState holds the mutable lifecycle state of a Transaction (separated so transactions remain insert-only).
type TxState struct {
	TxID     core.TransactionID `json:"tx_id"`
	Status   core.TxStatus      `json:"status"`
	Reversed []money.Amount     `json:"reversed"` // per-posting cumulative reversed amounts
}

// EntryQuery specifies filtering and cursor pagination for account entries.
type EntryQuery struct {
	Account core.AccountID
	Cursor  string // opaque cursor (e.g. version or index)
	Limit   int
}

// Page represents a cursor-paginated slice of items.
type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// AccountPatch specifies mutable fields on an Account (status, limits, metadata).
type AccountPatch struct {
	Status   *core.AccountStatus
	Limits   *core.Limits
	Metadata core.Metadata
}

// BatchCommitItem describes one validated transaction to commit inside a batch.
type BatchCommitItem struct {
	Tx          *core.Transaction
	Idem        IdemRecord
	AccountDefs map[core.AccountID]core.Account
	Events      []events.Event
	// Optional hold settlement/void or reversal state updates
	TargetTxID *core.TransactionID
	Reversal   bool
	SettleHold bool
	VoidHold   bool
}

// BatchCommitResult holds the per-item outcome of a batch commit.
type BatchCommitResult struct {
	Tx               *core.Transaction
	IdempotentReplay bool
	Err              error
}

// BatchPoster executes a batch of intents in a single round-trip when supported by the adapter.
type BatchPoster interface {
	PostBatch(ctx context.Context, items []BatchCommitItem) ([]BatchCommitResult, error)
}

// Store is the top-level storage interface.
type Store interface {
	Reader
	Begin(ctx context.Context) (Tx, error)
	// Join wraps a transaction the caller already opened (library mode).
	// Adapters that cannot join return ErrJoinUnsupported.
	Join(ctx context.Context, external any) (Tx, error)
	Admin() Admin
	Outbox() Outbox
	Journal() Journal
	Batch() BatchPoster
	// FoldDeferredBalances folds newly committed entries for Deferred accounts into their balance rows.
	FoldDeferredBalances(ctx context.Context, l core.LedgerID) (int, error)
	Close() error
}

// Tx is an atomic storage transaction. Every write method is batch-shaped (takes a slice).
type Tx interface {
	// LockBalances returns current balances for Locked accounts, locked until commit,
	// acquired in ascending AccountID order.
	LockBalances(ctx context.Context, ids []core.AccountID) ([]core.Balance, error)
	// ClaimIdempotency inserts keys; keys already present come back with their stored
	// request hash and transaction id instead of failing the Tx.
	ClaimIdempotency(ctx context.Context, recs []IdemRecord) (existing []IdemRecord, err error)
	InsertTransactions(ctx context.Context, txs []*core.Transaction) error
	InsertEntries(ctx context.Context, es []core.Entry) error
	GetTxStates(ctx context.Context, ids []core.TransactionID) (map[core.TransactionID]TxState, error)
	SetTxState(ctx context.Context, st []TxState) error
	WriteBalances(ctx context.Context, bs []core.Balance) error
	AppendOutbox(ctx context.Context, evs []events.Event) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Reader provides read-only queries against the ledger store.
type Reader interface {
	Ledger(ctx context.Context, id core.LedgerID) (*core.Ledger, error)
	Asset(ctx context.Context, l core.LedgerID, id money.AssetID) (*money.Asset, error)
	Assets(ctx context.Context, l core.LedgerID) ([]money.Asset, error)
	Holder(ctx context.Context, id core.HolderID) (*core.AccountHolder, error)
	Accounts(ctx context.Context, ids []core.AccountID) ([]core.Account, error)
	AccountByCode(ctx context.Context, l core.LedgerID, code string) (*core.Account, error)
	ListAccounts(ctx context.Context, l core.LedgerID) ([]core.Account, error)
	Balance(ctx context.Context, id core.AccountID) (core.Balance, error)
	BalanceAt(ctx context.Context, id core.AccountID, at time.Time) (money.Amount, error)
	HolderBalances(ctx context.Context, h core.HolderID) ([]core.HolderBalance, error)
	Transaction(ctx context.Context, id core.TransactionID) (*core.Transaction, error)
	ListTransactions(ctx context.Context, l core.LedgerID) ([]*core.Transaction, error)
	TxState(ctx context.Context, id core.TransactionID) (*TxState, error)
	PendingExpiredTransactions(ctx context.Context, now time.Time, limit int) ([]*core.Transaction, error)
	Entries(ctx context.Context, q EntryQuery) (Page[core.Entry], error)
	Idempotency(ctx context.Context, l core.LedgerID, key string) (*IdemRecord, error)
	Periods(ctx context.Context, l core.LedgerID) ([]core.Period, error)
}

// Admin provides DDL/setup operations for ledgers, assets, holders, accounts, and periods.
type Admin interface {
	CreateLedger(ctx context.Context, l core.Ledger) error
	RegisterAsset(ctx context.Context, l core.LedgerID, a money.Asset) error
	CreateHolder(ctx context.Context, h core.AccountHolder) error
	CreateAccount(ctx context.Context, a core.Account) error
	UpdateAccount(ctx context.Context, id core.AccountID, patch AccountPatch) (*core.Account, error)
	CreatePeriod(ctx context.Context, p core.Period) error
	ClosePeriod(ctx context.Context, l core.LedgerID, startsAt time.Time, closedAt time.Time) (*core.Period, error)
	PruneIdempotency(ctx context.Context, before time.Time) (int, error)
}

// Outbox manages transactional outbox events and per-subscriber cursors.
type Outbox interface {
	FetchAfter(ctx context.Context, l core.LedgerID, afterSeq uint64, limit int) ([]events.Event, error)
	FetchAllAfter(ctx context.Context, afterSeq uint64, limit int) ([]events.Event, error)
	GetCursor(ctx context.Context, subscriber string) (uint64, error)
	SetCursor(ctx context.Context, subscriber string, seq uint64, updatedAt time.Time) error
	PruneDelivered(ctx context.Context, before time.Time) (int, error)
}

// Journal manages the tamper-evident hash chain and checkpoints.
type Journal interface {
	UnsealedTransactions(ctx context.Context, l core.LedgerID, limit int) ([]*core.Transaction, error)
	LatestRecord(ctx context.Context, l core.LedgerID) (*core.JournalRecord, error)
	AppendRecords(ctx context.Context, records []core.JournalRecord, checkpoint *core.Checkpoint) error
	Records(ctx context.Context, l core.LedgerID, afterSeq uint64, limit int) ([]core.JournalRecord, error)
	Checkpoints(ctx context.Context, l core.LedgerID) ([]core.Checkpoint, error)
}
