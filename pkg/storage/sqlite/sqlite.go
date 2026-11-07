// Package sqlite implements storage.Store backed by SQLite (modernc.org/sqlite)
// in WAL mode with canonical decimal TEXT amounts, RAISE(ABORT) immutability
// triggers, and external *sql.Tx joining.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"

	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage"
)

var memDBCounter atomic.Uint64

const sqliteSchemaSQL = `
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS ledgers (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  metadata    TEXT NOT NULL DEFAULT '{}',
  created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS assets (
  ledger_id  TEXT NOT NULL REFERENCES ledgers(id),
  id         TEXT NOT NULL,
  code       TEXT NOT NULL,
  scale      INTEGER NOT NULL CHECK (scale BETWEEN 0 AND 30),
  PRIMARY KEY (ledger_id, id)
);

CREATE TABLE IF NOT EXISTS holders (
  id            TEXT PRIMARY KEY,
  ledger_id     TEXT NOT NULL REFERENCES ledgers(id),
  kind          TEXT NOT NULL,
  external_ref  TEXT NOT NULL,
  parent_id     TEXT CHECK (parent_id IS NULL),
  metadata      TEXT NOT NULL DEFAULT '{}',
  created_at    TEXT NOT NULL,
  UNIQUE (ledger_id, external_ref)
);

CREATE TABLE IF NOT EXISTS accounts (
  id             TEXT PRIMARY KEY,
  ledger_id      TEXT NOT NULL REFERENCES ledgers(id),
  holder_id      TEXT REFERENCES holders(id),
  code           TEXT NOT NULL,
  asset          TEXT NOT NULL,
  normal_side    INTEGER NOT NULL CHECK (normal_side IN (1, 2)),
  balance_mode   INTEGER NOT NULL DEFAULT 0,
  min_available  TEXT,
  max_available  TEXT,
  status         INTEGER NOT NULL DEFAULT 0,
  metadata       TEXT NOT NULL DEFAULT '{}',
  created_at     TEXT NOT NULL,
  UNIQUE (ledger_id, code),
  FOREIGN KEY (ledger_id, asset) REFERENCES assets (ledger_id, id),
  CHECK (balance_mode = 0 OR (min_available IS NULL AND max_available IS NULL))
);
CREATE INDEX IF NOT EXISTS accounts_holder_asset ON accounts (holder_id, asset);

CREATE TABLE IF NOT EXISTS balances (
  account_id       TEXT PRIMARY KEY REFERENCES accounts(id),
  posted_debits    TEXT NOT NULL DEFAULT '0',
  posted_credits   TEXT NOT NULL DEFAULT '0',
  pending_debits   TEXT NOT NULL DEFAULT '0',
  pending_credits  TEXT NOT NULL DEFAULT '0',
  version          INTEGER NOT NULL DEFAULT 0,
  updated_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
  id               TEXT PRIMARY KEY,
  ledger_id        TEXT NOT NULL REFERENCES ledgers(id),
  kind             TEXT NOT NULL,
  pending          INTEGER NOT NULL,
  postings         TEXT NOT NULL,
  effective_at     TEXT NOT NULL,
  committed_at     TEXT NOT NULL,
  expires_at       TEXT,
  reverses         TEXT REFERENCES transactions(id),
  pending_id       TEXT REFERENCES transactions(id),
  idempotency_key  TEXT NOT NULL,
  metadata         TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS tx_ledger_committed ON transactions (ledger_id, committed_at DESC);

CREATE TABLE IF NOT EXISTS tx_state (
  tx_id     TEXT PRIMARY KEY REFERENCES transactions(id),
  status    INTEGER NOT NULL,
  reversed  TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS tx_state_pending ON tx_state (status) WHERE status = 2;

CREATE TABLE IF NOT EXISTS entries (
  tx_id          TEXT NOT NULL REFERENCES transactions(id),
  posting_index  INTEGER NOT NULL,
  side           INTEGER NOT NULL,
  account_id     TEXT NOT NULL REFERENCES accounts(id),
  amount         TEXT NOT NULL,
  posted_after   TEXT,
  version        INTEGER NOT NULL DEFAULT 0,
  effective_at   TEXT NOT NULL,
  committed_at   TEXT NOT NULL,
  PRIMARY KEY (tx_id, posting_index, side)
);
CREATE INDEX IF NOT EXISTS entries_account_time ON entries (account_id, committed_at DESC, version DESC);

CREATE TABLE IF NOT EXISTS idempotency (
  ledger_id     TEXT NOT NULL,
  key           TEXT NOT NULL,
  request_hash  BLOB NOT NULL,
  tx_id         TEXT NOT NULL,
  created_at    TEXT NOT NULL,
  PRIMARY KEY (ledger_id, key)
);

CREATE TABLE IF NOT EXISTS outbox (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id      TEXT NOT NULL,
  ledger_id     TEXT NOT NULL,
  type          TEXT NOT NULL,
  payload       TEXT NOT NULL,
  created_at    TEXT NOT NULL,
  delivered_at  TEXT
);

CREATE TABLE IF NOT EXISTS subscriber_cursors (
  subscriber  TEXT PRIMARY KEY,
  seq         INTEGER NOT NULL DEFAULT 0,
  updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS journal (
  ledger_id   TEXT NOT NULL,
  seq         INTEGER NOT NULL,
  tx_id       TEXT NOT NULL UNIQUE REFERENCES transactions(id),
  tx_hash     BLOB NOT NULL,
  chain_hash  BLOB NOT NULL,
  sealed_at   TEXT NOT NULL,
  PRIMARY KEY (ledger_id, seq)
);

CREATE TABLE IF NOT EXISTS checkpoints (
  ledger_id   TEXT NOT NULL,
  seq         INTEGER NOT NULL,
  chain_hash  BLOB NOT NULL,
  created_at  TEXT NOT NULL,
  PRIMARY KEY (ledger_id, seq)
);

CREATE TABLE IF NOT EXISTS periods (
  ledger_id  TEXT NOT NULL,
  starts_at  TEXT NOT NULL,
  ends_at    TEXT NOT NULL,
  closed_at  TEXT,
  PRIMARY KEY (ledger_id, starts_at)
);

CREATE TABLE IF NOT EXISTS module_migrations (
  module        TEXT NOT NULL,
  migration_id  TEXT NOT NULL,
  applied_at    TEXT NOT NULL,
  PRIMARY KEY (module, migration_id)
);

CREATE TRIGGER IF NOT EXISTS transactions_no_update BEFORE UPDATE ON transactions
BEGIN SELECT RAISE(ABORT, 'ledger rows are immutable: UPDATE on transactions'); END;

CREATE TRIGGER IF NOT EXISTS transactions_no_delete BEFORE DELETE ON transactions
BEGIN SELECT RAISE(ABORT, 'ledger rows are immutable: DELETE on transactions'); END;

CREATE TRIGGER IF NOT EXISTS entries_no_update BEFORE UPDATE ON entries
BEGIN SELECT RAISE(ABORT, 'ledger rows are immutable: UPDATE on entries'); END;

CREATE TRIGGER IF NOT EXISTS entries_no_delete BEFORE DELETE ON entries
BEGIN SELECT RAISE(ABORT, 'ledger rows are immutable: DELETE on entries'); END;

CREATE TRIGGER IF NOT EXISTS journal_no_update BEFORE UPDATE ON journal
BEGIN SELECT RAISE(ABORT, 'ledger rows are immutable: UPDATE on journal'); END;

CREATE TRIGGER IF NOT EXISTS journal_no_delete BEFORE DELETE ON journal
BEGIN SELECT RAISE(ABORT, 'ledger rows are immutable: DELETE on journal'); END;
`

// MigrationSQL returns the SQLite schema DDL.
func MigrationSQL() string {
	return sqliteSchemaSQL
}

// Store is a SQLite-backed implementation of storage.Store.
type Store struct {
	db            *sql.DB
	keepConn      *sql.Conn // keeps shared in-memory DB alive across pool connections
	writeMu       sync.Mutex // SQLite has one writer per file
	advisoryMu    sync.Mutex
	advisoryLocks map[string]bool
}

// Open opens a SQLite database at dsn (e.g., ":memory:" or a file path) and applies the schema.
func Open(ctx context.Context, dsn string) (*Store, error) {
	actualDSN := dsn
	isMem := dsn == "" || dsn == ":memory:"
	if isMem {
		id := memDBCounter.Add(1)
		actualDSN = fmt.Sprintf("file:finjournal_mem_%d?mode=memory&cache=shared", id)
	}
	db, err := sql.Open("sqlite", actualDSN)
	if err != nil {
		return nil, err
	}
	var keepConn *sql.Conn
	if isMem {
		keepConn, err = db.Conn(ctx)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if _, err := db.ExecContext(ctx, sqliteSchemaSQL); err != nil {
		if keepConn != nil {
			_ = keepConn.Close()
		}
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db, keepConn: keepConn}, nil
}

// New wraps an existing *sql.DB and applies the SQLite schema.
func New(ctx context.Context, db *sql.DB) (*Store, error) {
	if _, err := db.ExecContext(ctx, sqliteSchemaSQL); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// DB returns the underlying *sql.DB (useful for testing Join with external *sql.Tx).
func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) Admin() storage.Admin       { return s }
func (s *Store) Outbox() storage.Outbox     { return s }
func (s *Store) Journal() storage.Journal   { return s }
func (s *Store) Batch() storage.BatchPoster { return nil }
func (s *Store) Close() error {
	if s.keepConn != nil {
		_ = s.keepConn.Close()
	}
	return s.db.Close()
}

func (s *Store) Begin(ctx context.Context) (storage.Tx, error) {
	s.writeMu.Lock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.writeMu.Unlock()
		return nil, err
	}
	return &sqliteTx{store: s, tx: tx, external: false}, nil
}

// Join wraps a caller-opened *sql.Tx.
func (s *Store) Join(_ context.Context, external any) (storage.Tx, error) {
	sqlTx, ok := external.(*sql.Tx)
	if !ok || sqlTx == nil {
		return nil, storage.ErrJoinUnsupported
	}
	return &sqliteTx{store: s, tx: sqlTx, external: true}, nil
}

type sqliteTx struct {
	store    *Store
	tx       *sql.Tx
	external bool
	done     bool
}

func (t *sqliteTx) LockBalances(ctx context.Context, ids []core.AccountID) ([]core.Balance, error) {
	sorted := make([]core.AccountID, len(ids))
	copy(sorted, ids)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var out []core.Balance
	seen := make(map[core.AccountID]bool, len(sorted))
	for _, id := range sorted {
		if seen[id] {
			continue
		}
		seen[id] = true
		row := t.tx.QueryRowContext(ctx, `
			SELECT b.account_id, a.asset, b.posted_debits, b.posted_credits,
			       b.pending_debits, b.pending_credits, b.version
			FROM balances b
			JOIN accounts a ON a.id = b.account_id
			WHERE b.account_id = ?`, string(id))
		b, err := scanBalance(row)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("%w: account %s", storage.ErrNotFound, id)
			}
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

func (t *sqliteTx) ClaimIdempotency(ctx context.Context, recs []storage.IdemRecord) ([]storage.IdemRecord, error) {
	var existing []storage.IdemRecord
	for _, r := range recs {
		res, err := t.tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO idempotency (ledger_id, key, request_hash, tx_id, created_at)
			VALUES (?, ?, ?, ?, ?)`,
			string(r.Ledger), r.Key, r.RequestHash, string(r.TxID), fmtTime(r.CreatedAt))
		if err != nil {
			return nil, err
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			var ex storage.IdemRecord
			var lID, txID, createdStr string
			err := t.tx.QueryRowContext(ctx, `
				SELECT ledger_id, key, request_hash, tx_id, created_at
				FROM idempotency WHERE ledger_id = ? AND key = ?`,
				string(r.Ledger), r.Key).Scan(&lID, &ex.Key, &ex.RequestHash, &txID, &createdStr)
			if err != nil {
				return nil, err
			}
			ex.Ledger = core.LedgerID(lID)
			ex.TxID = core.TransactionID(txID)
			ex.CreatedAt = parseTime(createdStr)
			existing = append(existing, ex)
		}
	}
	return existing, nil
}

func (t *sqliteTx) InsertTransactions(ctx context.Context, txs []*core.Transaction) error {
	for _, tx := range txs {
		postingsJSON, err := json.Marshal(tx.Postings)
		if err != nil {
			return err
		}
		metaJSON, err := json.Marshal(nonNilMeta(tx.Metadata))
		if err != nil {
			return err
		}
		pendingInt := 0
		if tx.Status == core.StatusPending {
			pendingInt = 1
		}
		_, err = t.tx.ExecContext(ctx, `
			INSERT INTO transactions
			(id, ledger_id, kind, pending, postings, effective_at, committed_at, expires_at, reverses, pending_id, idempotency_key, metadata)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(tx.ID), string(tx.Ledger), tx.Kind, pendingInt,
			string(postingsJSON), fmtTime(tx.EffectiveAt), fmtTime(tx.CommittedAt),
			fmtOptTime(tx.ExpiresAt), nullableTxID(tx.Reverses), nullableTxID(tx.PendingID),
			tx.IdempotencyKey, string(metaJSON))
		if err != nil {
			return err
		}
		rev := make([]money.Amount, len(tx.Postings))
		revJSON, _ := json.Marshal(rev)
		_, err = t.tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO tx_state (tx_id, status, reversed)
			VALUES (?, ?, ?)`,
			string(tx.ID), int(tx.Status), string(revJSON))
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *sqliteTx) InsertEntries(ctx context.Context, es []core.Entry) error {
	for _, e := range es {
		var postedAfter *string
		if e.PostedAfter != nil {
			s := e.PostedAfter.String()
			postedAfter = &s
		}
		_, err := t.tx.ExecContext(ctx, `
			INSERT INTO entries
			(tx_id, posting_index, side, account_id, amount, posted_after, version, effective_at, committed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(e.Transaction), int(e.PostingIndex), int(e.Side),
			string(e.Account), e.Amount.String(), postedAfter, e.Version,
			fmtTime(e.EffectiveAt), fmtTime(e.CommittedAt))
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *sqliteTx) GetTxStates(ctx context.Context, ids []core.TransactionID) (map[core.TransactionID]storage.TxState, error) {
	out := make(map[core.TransactionID]storage.TxState, len(ids))
	for _, id := range ids {
		var status int
		var revStr string
		err := t.tx.QueryRowContext(ctx, `SELECT status, reversed FROM tx_state WHERE tx_id = ?`, string(id)).
			Scan(&status, &revStr)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("%w: tx_state %s", storage.ErrNotFound, id)
			}
			return nil, err
		}
		var rev []money.Amount
		if err := json.Unmarshal([]byte(revStr), &rev); err != nil {
			return nil, err
		}
		out[id] = storage.TxState{
			TxID:     id,
			Status:   core.TxStatus(status),
			Reversed: rev,
		}
	}
	return out, nil
}

func (t *sqliteTx) SetTxState(ctx context.Context, st []storage.TxState) error {
	for _, s := range st {
		revJSON, err := json.Marshal(s.Reversed)
		if err != nil {
			return err
		}
		_, err = t.tx.ExecContext(ctx, `
			INSERT INTO tx_state (tx_id, status, reversed)
			VALUES (?, ?, ?)
			ON CONFLICT (tx_id) DO UPDATE SET status = excluded.status, reversed = excluded.reversed`,
			string(s.TxID), int(s.Status), string(revJSON))
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *sqliteTx) WriteBalances(ctx context.Context, bs []core.Balance) error {
	nowStr := fmtTime(time.Now().UTC())
	for _, b := range bs {
		_, err := t.tx.ExecContext(ctx, `
			UPDATE balances
			SET posted_debits = ?, posted_credits = ?, pending_debits = ?, pending_credits = ?,
			    version = ?, updated_at = ?
			WHERE account_id = ?`,
			b.PostedDebits.String(), b.PostedCredits.String(),
			b.PendingDebits.String(), b.PendingCredits.String(),
			b.Version, nowStr, string(b.Account))
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *sqliteTx) AppendOutbox(ctx context.Context, evs []events.Event) error {
	for _, ev := range evs {
		_, err := t.tx.ExecContext(ctx, `
			INSERT INTO outbox (event_id, ledger_id, type, payload, created_at)
			VALUES (?, ?, ?, ?, ?)`,
			ev.ID, string(ev.Ledger), ev.Type, string(ev.Payload), fmtTime(ev.CreatedAt))
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *sqliteTx) Commit(_ context.Context) error {
	if t.done {
		return nil
	}
	t.done = true
	if t.external {
		return nil
	}
	defer t.store.writeMu.Unlock()
	return t.tx.Commit()
}

func (t *sqliteTx) Rollback(_ context.Context) error {
	if t.done {
		return nil
	}
	t.done = true
	if t.external {
		return nil
	}
	defer t.store.writeMu.Unlock()
	return t.tx.Rollback()
}

// --- Admin methods ---

func (s *Store) CreateLedger(ctx context.Context, l core.Ledger) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	metaJSON, _ := json.Marshal(nonNilMeta(l.Metadata))
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ledgers (id, name, metadata, created_at) VALUES (?, ?, ?, ?)`,
		string(l.ID), l.Name, string(metaJSON), fmtTime(l.CreatedAt))
	return mapSqliteErr(err)
}

func (s *Store) RegisterAsset(ctx context.Context, l core.LedgerID, a money.Asset) error {
	if err := a.Validate(); err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO assets (ledger_id, id, code, scale) VALUES (?, ?, ?, ?)`,
		string(l), string(a.ID()), a.Code, int(a.Scale))
	return mapSqliteErr(err)
}

func (s *Store) CreateHolder(ctx context.Context, h core.AccountHolder) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	metaJSON, _ := json.Marshal(nonNilMeta(h.Metadata))
	var parent *string
	if h.ParentID != nil {
		p := string(*h.ParentID)
		parent = &p
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO holders (id, ledger_id, kind, external_ref, parent_id, metadata, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		string(h.ID), string(h.Ledger), h.Kind, h.ExternalRef, parent, string(metaJSON), fmtTime(h.CreatedAt))
	return mapSqliteErr(err)
}

func (s *Store) CreateAccount(ctx context.Context, a core.Account) error {
	if a.BalanceMode == core.Deferred && (a.Limits.Min != nil || a.Limits.Max != nil) {
		return core.NewError(core.CodeValidationFailed, "deferred account cannot have limits", nil)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	metaJSON, _ := json.Marshal(nonNilMeta(a.Metadata))
	var holder *string
	if a.Holder != nil {
		h := string(*a.Holder)
		holder = &h
	}
	var minStr, maxStr *string
	if a.Limits.Min != nil {
		v := a.Limits.Min.String()
		minStr = &v
	}
	if a.Limits.Max != nil {
		v := a.Limits.Max.String()
		maxStr = &v
	}
	createdStr := fmtTime(a.CreatedAt)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO accounts
		(id, ledger_id, holder_id, code, asset, normal_side, balance_mode, min_available, max_available, status, metadata, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(a.ID), string(a.Ledger), holder, a.Code, string(a.Asset),
		int(a.Normal), int(a.BalanceMode), minStr, maxStr, int(a.Status), string(metaJSON), createdStr)
	if err != nil {
		return mapSqliteErr(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO balances (account_id, updated_at) VALUES (?, ?)`, string(a.ID), createdStr)
	if err != nil {
		return mapSqliteErr(err)
	}
	return tx.Commit()
}

func (s *Store) UpdateAccount(ctx context.Context, id core.AccountID, patch storage.AccountPatch) (*core.Account, error) {
	accts, err := s.Accounts(ctx, []core.AccountID{id})
	if err != nil {
		return nil, err
	}
	a := accts[0]
	if patch.Status != nil {
		a.Status = *patch.Status
	}
	if patch.Limits != nil {
		if a.BalanceMode == core.Deferred && (patch.Limits.Min != nil || patch.Limits.Max != nil) {
			return nil, core.NewError(core.CodeValidationFailed, "deferred account cannot have limits", nil)
		}
		a.Limits = *patch.Limits
	}
	if patch.Metadata != nil {
		if a.Metadata == nil {
			a.Metadata = make(core.Metadata)
		}
		for k, v := range patch.Metadata {
			a.Metadata[k] = v
		}
	}
	var minStr, maxStr *string
	if a.Limits.Min != nil {
		v := a.Limits.Min.String()
		minStr = &v
	}
	if a.Limits.Max != nil {
		v := a.Limits.Max.String()
		maxStr = &v
	}
	metaJSON, _ := json.Marshal(nonNilMeta(a.Metadata))
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err = s.db.ExecContext(ctx, `
		UPDATE accounts SET status = ?, min_available = ?, max_available = ?, metadata = ? WHERE id = ?`,
		int(a.Status), minStr, maxStr, string(metaJSON), string(id))
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *Store) CreatePeriod(ctx context.Context, p core.Period) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO periods (ledger_id, starts_at, ends_at, closed_at) VALUES (?, ?, ?, ?)`,
		string(p.Ledger), fmtTime(p.StartsAt), fmtTime(p.EndsAt), fmtOptTime(p.ClosedAt))
	return mapSqliteErr(err)
}

func (s *Store) ClosePeriod(ctx context.Context, l core.LedgerID, startsAt time.Time, closedAt time.Time) (*core.Period, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	endsAt := startsAt.UTC().AddDate(0, 1, 0)
	c := closedAt.UTC()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO periods (ledger_id, starts_at, ends_at, closed_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (ledger_id, starts_at) DO UPDATE SET closed_at = excluded.closed_at`,
		string(l), fmtTime(startsAt), fmtTime(endsAt), fmtTime(c))
	if err != nil {
		return nil, err
	}
	return &core.Period{Ledger: l, StartsAt: startsAt.UTC(), EndsAt: endsAt, ClosedAt: &c}, nil
}

func (s *Store) PruneIdempotency(ctx context.Context, before time.Time) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx, `DELETE FROM idempotency WHERE created_at < ?`, fmtTime(before))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *Store) ExecModuleMigration(ctx context.Context, moduleName, migrationID, sqlText string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var exists int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM module_migrations WHERE module = ? AND migration_id = ?`, moduleName, migrationID).Scan(&exists)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if strings.TrimSpace(sqlText) != "" {
		if _, err := tx.ExecContext(ctx, sqlText); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO module_migrations (module, migration_id, applied_at) VALUES (?, ?, ?)`,
		moduleName, migrationID, fmtTime(time.Now().UTC())); err != nil {
		return err
	}
	return tx.Commit()
}

// --- Reader methods ---

func (s *Store) Ledger(ctx context.Context, id core.LedgerID) (*core.Ledger, error) {
	var l core.Ledger
	var lID, metaStr, createdStr string
	err := s.db.QueryRowContext(ctx, `SELECT id, name, metadata, created_at FROM ledgers WHERE id = ?`, string(id)).
		Scan(&lID, &l.Name, &metaStr, &createdStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: ledger %s", storage.ErrNotFound, id)
		}
		return nil, err
	}
	l.ID = core.LedgerID(lID)
	l.CreatedAt = parseTime(createdStr)
	_ = json.Unmarshal([]byte(metaStr), &l.Metadata)
	return &l, nil
}

func (s *Store) Asset(ctx context.Context, l core.LedgerID, id money.AssetID) (*money.Asset, error) {
	var a money.Asset
	var scale int
	err := s.db.QueryRowContext(ctx, `SELECT code, scale FROM assets WHERE ledger_id = ? AND id = ?`, string(l), string(id)).
		Scan(&a.Code, &scale)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: asset %s", storage.ErrNotFound, id)
		}
		return nil, err
	}
	a.Scale = uint8(scale)
	return &a, nil
}

func (s *Store) Assets(ctx context.Context, l core.LedgerID) ([]money.Asset, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT code, scale FROM assets WHERE ledger_id = ? ORDER BY id`, string(l))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []money.Asset
	for rows.Next() {
		var a money.Asset
		var scale int
		if err := rows.Scan(&a.Code, &scale); err != nil {
			return nil, err
		}
		a.Scale = uint8(scale)
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) Holder(ctx context.Context, id core.HolderID) (*core.AccountHolder, error) {
	var h core.AccountHolder
	var hID, lID, metaStr, createdStr string
	var parent *string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, ledger_id, kind, external_ref, parent_id, metadata, created_at
		FROM holders WHERE id = ?`, string(id)).
		Scan(&hID, &lID, &h.Kind, &h.ExternalRef, &parent, &metaStr, &createdStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: holder %s", storage.ErrNotFound, id)
		}
		return nil, err
	}
	h.ID = core.HolderID(hID)
	h.Ledger = core.LedgerID(lID)
	if parent != nil {
		p := core.HolderID(*parent)
		h.ParentID = &p
	}
	h.CreatedAt = parseTime(createdStr)
	_ = json.Unmarshal([]byte(metaStr), &h.Metadata)
	return &h, nil
}

func (s *Store) Accounts(ctx context.Context, ids []core.AccountID) ([]core.Account, error) {
	out := make([]core.Account, 0, len(ids))
	for _, id := range ids {
		row := s.db.QueryRowContext(ctx, `
			SELECT id, ledger_id, holder_id, code, asset, normal_side, balance_mode,
			       min_available, max_available, status, metadata, created_at
			FROM accounts WHERE id = ?`, string(id))
		a, err := scanAccount(row)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("%w: account %s", storage.ErrNotFound, id)
			}
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

func (s *Store) AccountByCode(ctx context.Context, l core.LedgerID, code string) (*core.Account, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, ledger_id, holder_id, code, asset, normal_side, balance_mode,
		       min_available, max_available, status, metadata, created_at
		FROM accounts WHERE ledger_id = ? AND code = ?`, string(l), code)
	a, err := scanAccount(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: account code %s", storage.ErrNotFound, code)
		}
		return nil, err
	}
	return &a, nil
}

func (s *Store) ListAccounts(ctx context.Context, l core.LedgerID) ([]core.Account, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, ledger_id, holder_id, code, asset, normal_side, balance_mode,
		       min_available, max_available, status, metadata, created_at
		FROM accounts WHERE ledger_id = ? ORDER BY id`, string(l))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.Account
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) Balance(ctx context.Context, id core.AccountID) (core.Balance, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT b.account_id, a.asset, b.posted_debits, b.posted_credits,
		       b.pending_debits, b.pending_credits, b.version
		FROM balances b
		JOIN accounts a ON a.id = b.account_id
		WHERE b.account_id = ?`, string(id))
	b, err := scanBalance(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return core.Balance{}, fmt.Errorf("%w: account %s", storage.ErrNotFound, id)
		}
		return core.Balance{}, err
	}
	return b, nil
}

func (s *Store) BalanceAt(ctx context.Context, id core.AccountID, at time.Time) (money.Amount, error) {
	accts, err := s.Accounts(ctx, []core.AccountID{id})
	if err != nil {
		return money.Zero, err
	}
	a := accts[0]
	atStr := fmtTime(at)
	if a.BalanceMode == core.Locked {
		var postedAfter *string
		err := s.db.QueryRowContext(ctx, `
			SELECT posted_after FROM entries
			WHERE account_id = ? AND committed_at <= ?
			ORDER BY committed_at DESC, version DESC LIMIT 1`, string(id), atStr).Scan(&postedAfter)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return money.Zero, nil
			}
			return money.Zero, err
		}
		if postedAfter == nil {
			return money.Zero, nil
		}
		return money.ParseMinor(*postedAfter)
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT side, amount FROM entries WHERE account_id = ? AND committed_at <= ?`, string(id), atStr)
	if err != nil {
		return money.Zero, err
	}
	defer rows.Close()
	debits := money.Zero
	credits := money.Zero
	for rows.Next() {
		var side int
		var amtStr string
		if err := rows.Scan(&side, &amtStr); err != nil {
			return money.Zero, err
		}
		amt, _ := money.ParseMinor(amtStr)
		if core.Side(side) == core.Debit {
			debits, _ = debits.Add(amt)
		} else {
			credits, _ = credits.Add(amt)
		}
	}
	if a.Normal == core.Debit {
		res, _ := debits.Sub(credits)
		return res, nil
	}
	res, _ := credits.Sub(debits)
	return res, nil
}

func (s *Store) HolderBalances(ctx context.Context, h core.HolderID) ([]core.HolderBalance, error) {
	if _, err := s.Holder(ctx, h); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT a.id, a.asset, a.normal_side,
		       b.posted_debits, b.posted_credits, b.pending_debits, b.pending_credits, b.version
		FROM accounts a
		JOIN balances b ON b.account_id = a.id
		WHERE a.holder_id = ?`, string(h))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type gKey struct {
		asset  money.AssetID
		normal core.Side
	}
	groups := make(map[gKey]*core.HolderBalance)
	for rows.Next() {
		var acctID, assetStr, pd, pc, pendD, pendC string
		var normal int
		var ver uint64
		if err := rows.Scan(&acctID, &assetStr, &normal, &pd, &pc, &pendD, &pendC, &ver); err != nil {
			return nil, err
		}
		b := core.Balance{
			Account: core.AccountID(acctID),
			Asset:   money.AssetID(assetStr),
			Version: ver,
		}
		b.PostedDebits, _ = money.ParseMinor(pd)
		b.PostedCredits, _ = money.ParseMinor(pc)
		b.PendingDebits, _ = money.ParseMinor(pendD)
		b.PendingCredits, _ = money.ParseMinor(pendC)

		normSide := core.Side(normal)
		k := gKey{asset: b.Asset, normal: normSide}
		gb, exists := groups[k]
		if !exists {
			gb = &core.HolderBalance{
				Holder:    h,
				Asset:     b.Asset,
				Normal:    normSide,
				Posted:    money.Zero,
				Available: money.Zero,
			}
			groups[k] = gb
		}
		gb.Posted, _ = gb.Posted.Add(b.Posted(normSide))
		gb.Available, _ = gb.Available.Add(b.Available(normSide))
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
	return out, rows.Err()
}

func (s *Store) Transaction(ctx context.Context, id core.TransactionID) (*core.Transaction, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT t.id, t.ledger_id, t.kind, COALESCE(st.status, CASE WHEN t.pending != 0 THEN 2 ELSE 1 END),
		       t.postings, t.effective_at, t.committed_at, t.expires_at, t.reverses, t.pending_id,
		       t.idempotency_key, t.metadata
		FROM transactions t
		LEFT JOIN tx_state st ON st.tx_id = t.id
		WHERE t.id = ?`, string(id))
	tx, err := scanTransaction(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: transaction %s", storage.ErrNotFound, id)
		}
		return nil, err
	}
	return tx, nil
}

func (s *Store) ListTransactions(ctx context.Context, l core.LedgerID) ([]*core.Transaction, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT t.id, t.ledger_id, t.kind, COALESCE(st.status, CASE WHEN t.pending != 0 THEN 2 ELSE 1 END),
		       t.postings, t.effective_at, t.committed_at, t.expires_at, t.reverses, t.pending_id,
		       t.idempotency_key, t.metadata
		FROM transactions t
		LEFT JOIN tx_state st ON st.tx_id = t.id
		WHERE t.ledger_id = ?
		ORDER BY t.committed_at ASC, t.id ASC`, string(l))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*core.Transaction
	for rows.Next() {
		tx, err := scanTransaction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, tx)
	}
	return out, rows.Err()
}

func (s *Store) TxState(ctx context.Context, id core.TransactionID) (*storage.TxState, error) {
	var status int
	var revStr string
	err := s.db.QueryRowContext(ctx, `SELECT status, reversed FROM tx_state WHERE tx_id = ?`, string(id)).
		Scan(&status, &revStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: tx_state %s", storage.ErrNotFound, id)
		}
		return nil, err
	}
	var rev []money.Amount
	_ = json.Unmarshal([]byte(revStr), &rev)
	return &storage.TxState{TxID: id, Status: core.TxStatus(status), Reversed: rev}, nil
}

func (s *Store) PendingExpiredTransactions(ctx context.Context, now time.Time, limit int) ([]*core.Transaction, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT t.id, t.ledger_id, t.kind, st.status,
		       t.postings, t.effective_at, t.committed_at, t.expires_at, t.reverses, t.pending_id,
		       t.idempotency_key, t.metadata
		FROM transactions t
		JOIN tx_state st ON st.tx_id = t.id
		WHERE st.status = 2 AND t.expires_at IS NOT NULL AND t.expires_at <= ?
		LIMIT ?`, fmtTime(now), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*core.Transaction
	for rows.Next() {
		tx, err := scanTransaction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, tx)
	}
	return out, rows.Err()
}

func (s *Store) Entries(ctx context.Context, q storage.EntryQuery) (storage.Page[core.Entry], error) {
	if _, err := s.Accounts(ctx, []core.AccountID{q.Account}); err != nil {
		return storage.Page[core.Entry]{}, err
	}
	offset := 0
	if q.Cursor != "" {
		if v, err := strconv.Atoi(q.Cursor); err == nil && v >= 0 {
			offset = v
		}
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT tx_id, posting_index, side, account_id, amount, posted_after,
		       version, effective_at, committed_at
		FROM entries
		WHERE account_id = ?
		ORDER BY committed_at DESC, version DESC
		LIMIT ? OFFSET ?`, string(q.Account), limit+1, offset)
	if err != nil {
		return storage.Page[core.Entry]{}, err
	}
	defer rows.Close()
	var items []core.Entry
	for rows.Next() {
		var e core.Entry
		var txID, acctID, amtStr, effStr, commStr string
		var pIdx, side int
		var postedAfter *string
		if err := rows.Scan(&txID, &pIdx, &side, &acctID, &amtStr, &postedAfter, &e.Version, &effStr, &commStr); err != nil {
			return storage.Page[core.Entry]{}, err
		}
		e.Transaction = core.TransactionID(txID)
		e.PostingIndex = uint16(pIdx)
		e.Side = core.Side(side)
		e.Account = core.AccountID(acctID)
		e.Amount, _ = money.ParseMinor(amtStr)
		if postedAfter != nil {
			pa, _ := money.ParseMinor(*postedAfter)
			e.PostedAfter = &pa
		}
		e.EffectiveAt = parseTime(effStr)
		e.CommittedAt = parseTime(commStr)
		items = append(items, e)
	}
	var nextCursor string
	if len(items) > limit {
		items = items[:limit]
		nextCursor = strconv.Itoa(offset + limit)
	}
	return storage.Page[core.Entry]{Items: items, NextCursor: nextCursor}, rows.Err()
}

func (s *Store) Idempotency(ctx context.Context, l core.LedgerID, key string) (*storage.IdemRecord, error) {
	var r storage.IdemRecord
	var lID, txID, createdStr string
	err := s.db.QueryRowContext(ctx, `
		SELECT ledger_id, key, request_hash, tx_id, created_at
		FROM idempotency WHERE ledger_id = ? AND key = ?`, string(l), key).
		Scan(&lID, &r.Key, &r.RequestHash, &txID, &createdStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: idempotency key %s", storage.ErrNotFound, key)
		}
		return nil, err
	}
	r.Ledger = core.LedgerID(lID)
	r.TxID = core.TransactionID(txID)
	r.CreatedAt = parseTime(createdStr)
	return &r, nil
}

func (s *Store) Periods(ctx context.Context, l core.LedgerID) ([]core.Period, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ledger_id, starts_at, ends_at, closed_at
		FROM periods WHERE ledger_id = ? ORDER BY starts_at`, string(l))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.Period
	for rows.Next() {
		var p core.Period
		var lID, sStr, eStr string
		var cStr *string
		if err := rows.Scan(&lID, &sStr, &eStr, &cStr); err != nil {
			return nil, err
		}
		p.Ledger = core.LedgerID(lID)
		p.StartsAt = parseTime(sStr)
		p.EndsAt = parseTime(eStr)
		if cStr != nil {
			ct := parseTime(*cStr)
			p.ClosedAt = &ct
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) FoldDeferredBalances(ctx context.Context, l core.LedgerID) (int, error) {
	accts, err := s.ListAccounts(ctx, l)
	if err != nil {
		return 0, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	folded := 0
	for _, a := range accts {
		if a.BalanceMode != core.Deferred {
			continue
		}
		rows, err := s.db.QueryContext(ctx, `
			SELECT side, amount FROM entries WHERE account_id = ? ORDER BY committed_at ASC`, string(a.ID))
		if err != nil {
			return folded, err
		}
		debits := money.Zero
		credits := money.Zero
		var ver uint64
		for rows.Next() {
			var side int
			var amtStr string
			if err := rows.Scan(&side, &amtStr); err != nil {
				rows.Close()
				return folded, err
			}
			amt, _ := money.ParseMinor(amtStr)
			if core.Side(side) == core.Debit {
				debits, _ = debits.Add(amt)
			} else {
				credits, _ = credits.Add(amt)
			}
			ver++
		}
		rows.Close()
		_, err = s.db.ExecContext(ctx, `
			UPDATE balances SET posted_debits = ?, posted_credits = ?, version = ?, updated_at = ?
			WHERE account_id = ?`,
			debits.String(), credits.String(), ver, fmtTime(time.Now().UTC()), string(a.ID))
		if err != nil {
			return folded, err
		}
		folded++
	}
	return folded, nil
}

// --- Outbox methods ---

func (s *Store) FetchAfter(ctx context.Context, l core.LedgerID, afterSeq uint64, limit int) ([]events.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, event_id, ledger_id, type, payload, created_at, delivered_at
		FROM outbox WHERE ledger_id = ? AND id > ? ORDER BY id ASC LIMIT ?`, string(l), afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *Store) FetchAllAfter(ctx context.Context, afterSeq uint64, limit int) ([]events.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, event_id, ledger_id, type, payload, created_at, delivered_at
		FROM outbox WHERE id > ? ORDER BY id ASC LIMIT ?`, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *Store) GetCursor(ctx context.Context, subscriber string) (uint64, error) {
	var seq uint64
	err := s.db.QueryRowContext(ctx, `SELECT seq FROM subscriber_cursors WHERE subscriber = ?`, subscriber).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

func (s *Store) SetCursor(ctx context.Context, subscriber string, seq uint64, updatedAt time.Time) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO subscriber_cursors (subscriber, seq, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT (subscriber) DO UPDATE SET seq = excluded.seq, updated_at = excluded.updated_at`,
		subscriber, seq, fmtTime(updatedAt))
	return err
}

func (s *Store) PruneDelivered(ctx context.Context, before time.Time) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx, `DELETE FROM outbox WHERE delivered_at IS NOT NULL AND delivered_at < ?`, fmtTime(before))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// --- Journal methods ---

func (s *Store) TryAdvisoryLock(_ context.Context, lockKey string) (func(), bool, error) {
	s.advisoryMu.Lock()
	defer s.advisoryMu.Unlock()
	if s.advisoryLocks == nil {
		s.advisoryLocks = make(map[string]bool)
	}
	if s.advisoryLocks[lockKey] {
		return nil, false, nil
	}
	s.advisoryLocks[lockKey] = true
	var once sync.Once
	unlock := func() {
		once.Do(func() {
			s.advisoryMu.Lock()
			delete(s.advisoryLocks, lockKey)
			s.advisoryMu.Unlock()
		})
	}
	return unlock, true, nil
}

func (s *Store) UnsealedTransactions(ctx context.Context, l core.LedgerID, limit int) ([]*core.Transaction, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT t.id, t.ledger_id, t.kind, COALESCE(st.status, CASE WHEN t.pending != 0 THEN 2 ELSE 1 END),
		       t.postings, t.effective_at, t.committed_at, t.expires_at, t.reverses, t.pending_id,
		       t.idempotency_key, t.metadata
		FROM transactions t
		LEFT JOIN tx_state st ON st.tx_id = t.id
		LEFT JOIN journal j ON j.tx_id = t.id
		WHERE t.ledger_id = ? AND j.tx_id IS NULL
		ORDER BY t.committed_at ASC, t.id ASC LIMIT ?`, string(l), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*core.Transaction
	for rows.Next() {
		tx, err := scanTransaction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, tx)
	}
	return out, rows.Err()
}

func (s *Store) LatestRecord(ctx context.Context, l core.LedgerID) (*core.JournalRecord, error) {
	var r core.JournalRecord
	var lID, txID, sealedStr string
	err := s.db.QueryRowContext(ctx, `
		SELECT ledger_id, seq, tx_id, tx_hash, chain_hash, sealed_at
		FROM journal WHERE ledger_id = ? ORDER BY seq DESC LIMIT 1`, string(l)).
		Scan(&lID, &r.Seq, &txID, &r.TxHash, &r.ChainHash, &sealedStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	r.Ledger = core.LedgerID(lID)
	r.TxID = core.TransactionID(txID)
	r.SealedAt = parseTime(sealedStr)
	return &r, nil
}

func (s *Store) AppendRecords(ctx context.Context, records []core.JournalRecord, checkpoint *core.Checkpoint) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, r := range records {
		_, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO journal (ledger_id, seq, tx_id, tx_hash, chain_hash, sealed_at)
			VALUES (?, ?, ?, ?, ?, ?)`,
			string(r.Ledger), r.Seq, string(r.TxID), r.TxHash, r.ChainHash, fmtTime(r.SealedAt))
		if err != nil {
			return err
		}
	}
	if checkpoint != nil {
		_, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO checkpoints (ledger_id, seq, chain_hash, created_at)
			VALUES (?, ?, ?, ?)`,
			string(checkpoint.Ledger), checkpoint.Seq, checkpoint.ChainHash, fmtTime(checkpoint.CreatedAt))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) Records(ctx context.Context, l core.LedgerID, afterSeq uint64, limit int) ([]core.JournalRecord, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT ledger_id, seq, tx_id, tx_hash, chain_hash, sealed_at
		FROM journal WHERE ledger_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?`,
		string(l), afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.JournalRecord
	for rows.Next() {
		var r core.JournalRecord
		var lID, txID, sealedStr string
		if err := rows.Scan(&lID, &r.Seq, &txID, &r.TxHash, &r.ChainHash, &sealedStr); err != nil {
			return nil, err
		}
		r.Ledger = core.LedgerID(lID)
		r.TxID = core.TransactionID(txID)
		r.SealedAt = parseTime(sealedStr)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Checkpoints(ctx context.Context, l core.LedgerID) ([]core.Checkpoint, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ledger_id, seq, chain_hash, created_at
		FROM checkpoints WHERE ledger_id = ? ORDER BY seq ASC`, string(l))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.Checkpoint
	for rows.Next() {
		var c core.Checkpoint
		var lID, createdStr string
		if err := rows.Scan(&lID, &c.Seq, &c.ChainHash, &createdStr); err != nil {
			return nil, err
		}
		c.Ledger = core.LedgerID(lID)
		c.CreatedAt = parseTime(createdStr)
		out = append(out, c)
	}
	return out, rows.Err()
}

// --- Helpers ---

type rowScanner interface {
	Scan(dest ...any) error
}

func scanBalance(r rowScanner) (core.Balance, error) {
	var acctID, assetStr, pd, pc, pendD, pendC string
	var ver uint64
	if err := r.Scan(&acctID, &assetStr, &pd, &pc, &pendD, &pendC, &ver); err != nil {
		return core.Balance{}, err
	}
	b := core.Balance{
		Account: core.AccountID(acctID),
		Asset:   money.AssetID(assetStr),
		Version: ver,
	}
	b.PostedDebits, _ = money.ParseMinor(pd)
	b.PostedCredits, _ = money.ParseMinor(pc)
	b.PendingDebits, _ = money.ParseMinor(pendD)
	b.PendingCredits, _ = money.ParseMinor(pendC)
	return b, nil
}

func scanAccount(r rowScanner) (core.Account, error) {
	var a core.Account
	var id, lID, assetStr, metaStr, createdStr string
	var holder, minStr, maxStr *string
	var normal, mode, status int
	if err := r.Scan(&id, &lID, &holder, &a.Code, &assetStr, &normal, &mode, &minStr, &maxStr, &status, &metaStr, &createdStr); err != nil {
		return core.Account{}, err
	}
	a.ID = core.AccountID(id)
	a.Ledger = core.LedgerID(lID)
	if holder != nil {
		h := core.HolderID(*holder)
		a.Holder = &h
	}
	a.Asset = money.AssetID(assetStr)
	a.Normal = core.Side(normal)
	a.BalanceMode = core.BalanceMode(mode)
	a.Status = core.AccountStatus(status)
	if minStr != nil {
		v, _ := money.ParseMinor(*minStr)
		a.Limits.Min = &v
	}
	if maxStr != nil {
		v, _ := money.ParseMinor(*maxStr)
		a.Limits.Max = &v
	}
	a.CreatedAt = parseTime(createdStr)
	_ = json.Unmarshal([]byte(metaStr), &a.Metadata)
	return a, nil
}

func scanTransaction(r rowScanner) (*core.Transaction, error) {
	var tx core.Transaction
	var id, lID, postingsStr, effStr, commStr, metaStr string
	var status int
	var expStr, reverses, pendingID *string
	if err := r.Scan(&id, &lID, &tx.Kind, &status, &postingsStr, &effStr, &commStr,
		&expStr, &reverses, &pendingID, &tx.IdempotencyKey, &metaStr); err != nil {
		return nil, err
	}
	tx.ID = core.TransactionID(id)
	tx.Ledger = core.LedgerID(lID)
	tx.Status = core.TxStatus(status)
	_ = json.Unmarshal([]byte(postingsStr), &tx.Postings)
	tx.EffectiveAt = parseTime(effStr)
	tx.CommittedAt = parseTime(commStr)
	if expStr != nil {
		et := parseTime(*expStr)
		tx.ExpiresAt = &et
	}
	if reverses != nil {
		rv := core.TransactionID(*reverses)
		tx.Reverses = &rv
	}
	if pendingID != nil {
		pi := core.TransactionID(*pendingID)
		tx.PendingID = &pi
	}
	_ = json.Unmarshal([]byte(metaStr), &tx.Metadata)
	return &tx, nil
}

func scanEvents(rows *sql.Rows) ([]events.Event, error) {
	var out []events.Event
	for rows.Next() {
		var ev events.Event
		var lID, payloadStr, createdStr string
		var delivStr *string
		if err := rows.Scan(&ev.Seq, &ev.ID, &lID, &ev.Type, &payloadStr, &createdStr, &delivStr); err != nil {
			return nil, err
		}
		ev.Ledger = core.LedgerID(lID)
		ev.Payload = json.RawMessage(payloadStr)
		ev.CreatedAt = parseTime(createdStr)
		if delivStr != nil {
			dt := parseTime(*delivStr)
			ev.DeliveredAt = &dt
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

func fmtTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func fmtOptTime(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := fmtTime(*t)
	return &s
}

func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t.UTC()
}

func nullableTxID(id *core.TransactionID) *string {
	if id == nil {
		return nil
	}
	s := string(*id)
	return &s
}

func nonNilMeta(m core.Metadata) core.Metadata {
	if m == nil {
		return core.Metadata{}
	}
	return m
}

func mapSqliteErr(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "UNIQUE constraint failed") {
		return fmt.Errorf("%w: %v", storage.ErrAlreadyExists, err)
	}
	if strings.Contains(msg, "FOREIGN KEY constraint failed") {
		return fmt.Errorf("%w: %v", storage.ErrNotFound, err)
	}
	return err
}
