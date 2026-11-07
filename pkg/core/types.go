// Package core defines the domain types, error model, clock, and UUIDv7 generator
// shared across pkg/ledger, pkg/storage, pkg/module, pkg/commit, pkg/journal, and pkg/events.
package core

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/money"
)

type (
	// LedgerID identifies an independent book.
	LedgerID string
	// HolderID is a UUIDv7 identifying an AccountHolder.
	HolderID string
	// AccountID is a UUIDv7 identifying an Account.
	AccountID string
	// TransactionID is a time-ordered UUIDv7 identifying a Transaction.
	TransactionID string
)

// Side represents Debit or Credit.
type Side uint8

const (
	// Debit side (1).
	Debit Side = iota + 1
	// Credit side (2).
	Credit
)

// Opposite returns Credit for Debit and Debit for Credit.
func (s Side) Opposite() Side {
	if s == Debit {
		return Credit
	}
	return Debit
}

// Valid reports whether s is Debit or Credit.
func (s Side) Valid() bool {
	return s == Debit || s == Credit
}

func (s Side) String() string {
	switch s {
	case Debit:
		return "debit"
	case Credit:
		return "credit"
	default:
		return fmt.Sprintf("Side(%d)", s)
	}
}

func (s Side) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *Side) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		switch str {
		case "debit", "DEBIT", "1":
			*s = Debit
			return nil
		case "credit", "CREDIT", "2":
			*s = Credit
			return nil
		}
		return fmt.Errorf("invalid side %q", str)
	}
	var num uint8
	if err := json.Unmarshal(data, &num); err != nil {
		return err
	}
	if num != 1 && num != 2 {
		return fmt.Errorf("invalid side %d", num)
	}
	*s = Side(num)
	return nil
}

const (
	// MaxMetadataKeys is the maximum number of entries in Metadata.
	MaxMetadataKeys = 64
	// MaxMetadataBytes is the maximum total byte length of keys and values in Metadata.
	MaxMetadataBytes = 4096
	// MaxPostings is the maximum number of postings in a single transaction.
	MaxPostings = 256
)

// Metadata holds key/value reference strings (at most 64 keys, 4 KiB total).
type Metadata map[string]string

// Clone returns a shallow copy of Metadata.
func (m Metadata) Clone() Metadata {
	if m == nil {
		return nil
	}
	out := make(Metadata, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Validate checks key count and byte size limits on Metadata.
func (m Metadata) Validate() error {
	if len(m) > MaxMetadataKeys {
		return NewError(CodeValidationFailed, fmt.Sprintf("metadata exceeds %d keys (got %d)", MaxMetadataKeys, len(m)), nil)
	}
	total := 0
	for k, v := range m {
		if k == "" {
			return NewError(CodeValidationFailed, "metadata key must not be empty", nil)
		}
		total += len(k) + len(v)
		if total > MaxMetadataBytes {
			return NewError(CodeValidationFailed, fmt.Sprintf("metadata exceeds %d bytes", MaxMetadataBytes), nil)
		}
	}
	return nil
}

// Ledger represents an independent book (one per tenant or legal entity).
type Ledger struct {
	ID        LedgerID  `json:"id"`
	Name      string    `json:"name"`
	Metadata  Metadata  `json:"metadata,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// AccountHolder represents a party that owns accounts (customer, merchant, platform).
type AccountHolder struct {
	ID          HolderID  `json:"id"`
	Ledger      LedgerID  `json:"ledger"`
	Kind        string    `json:"kind"`
	ExternalRef string    `json:"external_ref"`
	ParentID    *HolderID `json:"parent_id,omitempty"` // reserved for v2; must be nil in v1
	Metadata    Metadata  `json:"metadata,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// BalanceMode controls whether an account's balance row is updated synchronously under lock
// or folded asynchronously from entries.
type BalanceMode uint8

const (
	// Locked updates the balance row in the commit transaction and enforces limits (default).
	Locked BalanceMode = iota
	// Deferred inserts entries without a balance row lock and folds balances asynchronously; cannot have limits.
	Deferred
)

func (m BalanceMode) String() string {
	switch m {
	case Locked:
		return "locked"
	case Deferred:
		return "deferred"
	default:
		return fmt.Sprintf("BalanceMode(%d)", m)
	}
}

func (m BalanceMode) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.String())
}

func (m *BalanceMode) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		switch str {
		case "", "locked", "LOCKED", "0":
			*m = Locked
			return nil
		case "deferred", "DEFERRED", "1":
			*m = Deferred
			return nil
		}
		return fmt.Errorf("invalid balance mode %q", str)
	}
	var num uint8
	if err := json.Unmarshal(data, &num); err != nil {
		return err
	}
	if num > 1 {
		return fmt.Errorf("invalid balance mode %d", num)
	}
	*m = BalanceMode(num)
	return nil
}

// AccountStatus controls whether an account may send or receive postings.
type AccountStatus uint8

const (
	// AccountOpen allows both inflows and outflows.
	AccountOpen AccountStatus = iota
	// AccountFrozen allows inflows (credits on credit-normal, debits on debit-normal? wait: RFC section 3 & 4:
	// "Frozen (no outflows)", "A Frozen account may receive but not send.")
	AccountFrozen
	// AccountClosed forbids all postings.
	AccountClosed
)

func (s AccountStatus) String() string {
	switch s {
	case AccountOpen:
		return "open"
	case AccountFrozen:
		return "frozen"
	case AccountClosed:
		return "closed"
	default:
		return fmt.Sprintf("AccountStatus(%d)", s)
	}
}

func (s AccountStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *AccountStatus) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		switch str {
		case "", "open", "OPEN", "0":
			*s = AccountOpen
			return nil
		case "frozen", "FROZEN", "1":
			*s = AccountFrozen
			return nil
		case "closed", "CLOSED", "2":
			*s = AccountClosed
			return nil
		}
		return fmt.Errorf("invalid account status %q", str)
	}
	var num uint8
	if err := json.Unmarshal(data, &num); err != nil {
		return err
	}
	if num > 2 {
		return fmt.Errorf("invalid account status %d", num)
	}
	*s = AccountStatus(num)
	return nil
}

// Limits bound the available balance measured on the account's normal side.
// nil means unbounded.
type Limits struct {
	Min *money.Amount `json:"min,omitempty"`
	Max *money.Amount `json:"max,omitempty"`
}

// Account represents a single-asset balance book under a Ledger.
type Account struct {
	ID          AccountID     `json:"id"`
	Ledger      LedgerID      `json:"ledger"`
	Holder      *HolderID     `json:"holder,omitempty"`
	Code        string        `json:"code"`
	Asset       money.AssetID `json:"asset"`
	Normal      Side          `json:"normal"`
	Limits      Limits        `json:"limits,omitempty"`
	BalanceMode BalanceMode   `json:"balance_mode"`
	Status      AccountStatus `json:"status"`
	Metadata    Metadata      `json:"metadata,omitempty"`
	CreatedAt   time.Time     `json:"created_at"`
}

// Balance holds the posted and pending totals and version for an Account.
type Balance struct {
	Account        AccountID     `json:"account"`
	Asset          money.AssetID `json:"asset"`
	PostedDebits   money.Amount  `json:"posted_debits"`
	PostedCredits  money.Amount  `json:"posted_credits"`
	PendingDebits  money.Amount  `json:"pending_debits"`
	PendingCredits money.Amount  `json:"pending_credits"`
	Version        uint64        `json:"version"`
}

// Posted returns the settled balance on the account's normal side.
func (b Balance) Posted(normal Side) money.Amount {
	if normal == Debit {
		res, _ := b.PostedDebits.Sub(b.PostedCredits)
		return res
	}
	res, _ := b.PostedCredits.Sub(b.PostedDebits)
	return res
}

// Available is Posted minus pending outflows; this is what Limits check.
// On a Credit-normal account (e.g. a customer liability wallet), debits decrease the balance,
// so PendingDebits are pending outflows. On a Debit-normal account, PendingCredits are pending outflows.
func (b Balance) Available(normal Side) money.Amount {
	posted := b.Posted(normal)
	if normal == Debit {
		res, _ := posted.Sub(b.PendingCredits)
		return res
	}
	res, _ := posted.Sub(b.PendingDebits)
	return res
}

// Posting moves Amount (> 0) of Asset from Debit to Credit.
type Posting struct {
	Debit  AccountID     `json:"debit"`
	Credit AccountID     `json:"credit"`
	Amount money.Amount  `json:"amount"`
	Asset  money.AssetID `json:"asset"`
	Source string        `json:"source,omitempty"`
	Memo   string        `json:"memo,omitempty"`
}

// TxStatus represents the lifecycle state of a Transaction.
type TxStatus uint8

const (
	// StatusPosted indicates a settled transaction.
	StatusPosted TxStatus = iota + 1
	// StatusPending indicates an active hold reserving funds.
	StatusPending
	// StatusVoided indicates a released hold.
	StatusVoided
	// StatusExpired indicates an expired hold.
	StatusExpired
)

func (s TxStatus) String() string {
	switch s {
	case StatusPosted:
		return "posted"
	case StatusPending:
		return "pending"
	case StatusVoided:
		return "voided"
	case StatusExpired:
		return "expired"
	default:
		return fmt.Sprintf("TxStatus(%d)", s)
	}
}

func (s TxStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *TxStatus) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		switch str {
		case "posted", "POSTED":
			*s = StatusPosted
			return nil
		case "pending", "PENDING":
			*s = StatusPending
			return nil
		case "voided", "VOIDED":
			*s = StatusVoided
			return nil
		case "expired", "EXPIRED":
			*s = StatusExpired
			return nil
		}
		return fmt.Errorf("invalid tx status %q", str)
	}
	var num uint8
	if err := json.Unmarshal(data, &num); err != nil {
		return err
	}
	if num < 1 || num > 4 {
		return fmt.Errorf("invalid tx status %d", num)
	}
	*s = TxStatus(num)
	return nil
}

// Transaction is an atomic set of double-entry postings.
type Transaction struct {
	ID               TransactionID  `json:"id"`
	Ledger           LedgerID       `json:"ledger"`
	Kind             string         `json:"kind"`
	Status           TxStatus       `json:"status"`
	Postings         []Posting      `json:"postings"`
	EffectiveAt      time.Time      `json:"effective_at"`
	CommittedAt      time.Time      `json:"committed_at"`
	ExpiresAt        *time.Time     `json:"expires_at,omitempty"`
	Reverses         *TransactionID `json:"reverses,omitempty"`
	PendingID        *TransactionID `json:"pending_id,omitempty"`
	IdempotencyKey   string         `json:"idempotency_key"`
	Metadata         Metadata       `json:"metadata,omitempty"`
	IdempotentReplay bool           `json:"-"`
}

// Entry represents one account side of a Posting in a settled Transaction.
type Entry struct {
	Transaction  TransactionID `json:"transaction"`
	PostingIndex uint16        `json:"posting_index"`
	Account      AccountID     `json:"account"`
	Side         Side          `json:"side"`
	Amount       money.Amount  `json:"amount"`
	PostedAfter  *money.Amount `json:"posted_after,omitempty"` // nil for Deferred accounts
	Version      uint64        `json:"version"`
	EffectiveAt  time.Time     `json:"effective_at"`
	CommittedAt  time.Time     `json:"committed_at"`
}

// Intent is what callers submit to Engine.Post. Modules may append postings before commit.
type Intent struct {
	Ledger         LedgerID   `json:"ledger"`
	Kind           string     `json:"kind"`
	IdempotencyKey string     `json:"idempotency_key"`
	Postings       []Posting  `json:"postings"`
	Params         Metadata   `json:"params,omitempty"`
	Pending        bool       `json:"pending,omitempty"`
	ExpiresAt      *time.Time `json:"expires_at,omitempty"`
	EffectiveAt    *time.Time `json:"effective_at,omitempty"`
	Metadata       Metadata   `json:"metadata,omitempty"`
}

// HolderBalance is one asset total across an AccountHolder's accounts, grouped by Normal side.
type HolderBalance struct {
	Holder            HolderID      `json:"holder"`
	Asset             money.AssetID `json:"asset"`
	Normal            Side          `json:"normal"`
	Posted            money.Amount  `json:"posted"`
	Available         money.Amount  `json:"available"`
	Accounts          int           `json:"accounts"`
	ReportedAsset     money.AssetID `json:"reported_asset,omitempty"`
	ReportedPosted    *money.Amount `json:"reported_posted,omitempty"`
	ReportedAvailable *money.Amount `json:"reported_available,omitempty"`
	QuoteID           string        `json:"quote_id,omitempty"`
	Rate              string        `json:"rate,omitempty"`
}

// Period represents an accounting period on a Ledger.
type Period struct {
	Ledger   LedgerID   `json:"ledger"`
	StartsAt time.Time  `json:"starts_at"`
	EndsAt   time.Time  `json:"ends_at"`
	ClosedAt *time.Time `json:"closed_at,omitempty"`
}

// JournalRecord is a sealed entry in the tamper-evident hash chain.
type JournalRecord struct {
	Ledger    LedgerID      `json:"ledger"`
	Seq       uint64        `json:"seq"`
	TxID      TransactionID `json:"tx_id"`
	TxHash    []byte        `json:"tx_hash"`
	ChainHash []byte        `json:"chain_hash"`
	SealedAt  time.Time     `json:"sealed_at"`
}

// Checkpoint is a periodic hash-chain snapshot for external verification.
type Checkpoint struct {
	Ledger    LedgerID  `json:"ledger"`
	Seq       uint64    `json:"seq"`
	ChainHash []byte    `json:"chain_hash"`
	CreatedAt time.Time `json:"created_at"`
}

// Error codes defined by RFC section 4 & 10.
const (
	CodeValidationFailed    = "validation_failed"
	CodeInsufficientFunds   = "insufficient_funds"
	CodeAccountFrozen       = "account_frozen"
	CodeAccountClosed       = "account_closed"
	CodePeriodClosed        = "period_closed"
	CodeIdempotencyConflict = "idempotency_conflict"
	CodeModuleRejected      = "module_rejected"
	CodeModuleUnavailable   = "module_unavailable"
	CodeLockTimeout         = "lock_timeout"
	CodeNotFound            = "not_found"
	CodeAlreadyExists       = "already_exists"
	CodeUnauthorized        = "unauthorized"
	CodeForbidden           = "forbidden"
)

// Error is the structured error returned by the ledger engine and REST API:
// {"code", "message", "details"}.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
	Cause   error          `json:"-"`
}

func (e *Error) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return e.Code
}

func (e *Error) Unwrap() error {
	return e.Cause
}

// HTTPStatus maps the structured error code to its RFC HTTP status code.
func (e *Error) HTTPStatus() int {
	switch e.Code {
	case CodeValidationFailed, CodeInsufficientFunds, CodeModuleRejected:
		return http.StatusUnprocessableEntity // 422
	case CodeAccountFrozen, CodeAccountClosed, CodePeriodClosed, CodeIdempotencyConflict, CodeAlreadyExists:
		return http.StatusConflict // 409
	case CodeModuleUnavailable, CodeLockTimeout:
		return http.StatusServiceUnavailable // 503
	case CodeNotFound:
		return http.StatusNotFound // 404
	case CodeUnauthorized:
		return http.StatusUnauthorized // 401
	case CodeForbidden:
		return http.StatusForbidden // 403
	default:
		return http.StatusInternalServerError
	}
}

// NewError creates a structured *Error.
func NewError(code, msg string, details map[string]any) *Error {
	return &Error{
		Code:    code,
		Message: msg,
		Details: details,
	}
}
