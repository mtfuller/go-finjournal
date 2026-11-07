// Package ledger provides the double-entry financial ledger engine, posting
// pipeline, hold lifecycle, reversals, outbox relay, and invariant checker.
package ledger

import (
	"github.com/mtfuller/go-finjournal/pkg/core"
)

// Domain type aliases re-exported from pkg/core so callers only need to import pkg/ledger.
type (
	LedgerID        = core.LedgerID
	HolderID        = core.HolderID
	AccountID       = core.AccountID
	TransactionID   = core.TransactionID
	Side            = core.Side
	Metadata        = core.Metadata
	Ledger          = core.Ledger
	AccountHolder   = core.AccountHolder
	BalanceMode     = core.BalanceMode
	AccountStatus   = core.AccountStatus
	Limits          = core.Limits
	Account         = core.Account
	Balance         = core.Balance
	Posting         = core.Posting
	TxStatus        = core.TxStatus
	Transaction     = core.Transaction
	Entry           = core.Entry
	Intent          = core.Intent
	HolderBalance   = core.HolderBalance
	Period          = core.Period
	JournalRecord        = core.JournalRecord
	Checkpoint           = core.Checkpoint
	AccrualSettleRequest = core.AccrualSettleRequest
	AccrualSettleResult  = core.AccrualSettleResult
	Error                = core.Error
	Clock           = core.Clock
	RealClock       = core.RealClock
	ManualClock     = core.ManualClock
	IDGenerator     = core.IDGenerator
	UUIDv7Generator = core.UUIDv7Generator
)

const (
	Debit  = core.Debit
	Credit = core.Credit

	Locked   = core.Locked
	Deferred = core.Deferred

	AccountOpen   = core.AccountOpen
	AccountFrozen = core.AccountFrozen
	AccountClosed = core.AccountClosed

	StatusPosted  = core.StatusPosted
	StatusPending = core.StatusPending
	StatusVoided  = core.StatusVoided
	StatusExpired = core.StatusExpired

	CodeValidationFailed    = core.CodeValidationFailed
	CodeInsufficientFunds   = core.CodeInsufficientFunds
	CodeAccountFrozen       = core.CodeAccountFrozen
	CodeAccountClosed       = core.CodeAccountClosed
	CodePeriodClosed        = core.CodePeriodClosed
	CodeIdempotencyConflict = core.CodeIdempotencyConflict
	CodeModuleRejected      = core.CodeModuleRejected
	CodeModuleUnavailable   = core.CodeModuleUnavailable
	CodeLockTimeout         = core.CodeLockTimeout
	CodeNotFound            = core.CodeNotFound
	CodeAlreadyExists       = core.CodeAlreadyExists
	CodeUnauthorized        = core.CodeUnauthorized
	CodeForbidden           = core.CodeForbidden
)

var (
	NewError                    = core.NewError
	NewManualClock              = core.NewManualClock
	NewUUIDv7Generator          = core.NewUUIDv7Generator
	NewDeterministicIDGenerator = core.NewDeterministicIDGenerator
)
