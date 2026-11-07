// Package postgres implements storage.Store backed by PostgreSQL (pgx/v5 and database/sql),
// with embedded SQL migrations, single-round-trip PL/pgSQL batch posting, and external
// transaction joining (supporting both pgx.Tx and *sql.Tx).
package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage"
)

//go:embed migrations/0001_init.sql
var initMigrationSQL string

// MigrationSQL returns the canonical PostgreSQL schema DDL.
func MigrationSQL() string {
	return initMigrationSQL
}

// Migrate applies the embedded PostgreSQL migrations to db.
func Migrate(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, initMigrationSQL)
	return err
}

// MigratePool applies the embedded PostgreSQL migrations to a *pgxpool.Pool.
func MigratePool(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, initMigrationSQL)
	return err
}

// Store is a PostgreSQL-backed implementation of storage.Store.
type Store struct {
	db     *sql.DB
	pool   *pgxpool.Pool
	schema string
}

// Open connects to PostgreSQL via pgxpool, applies migrations, verifies schema version, and returns a Store.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := MigratePool(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	st := NewPool(pool)
	if _, err := st.CheckSchemaVersion(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return st, nil
}

// ExpectedSchemaVersion is the current schema version required by this release of go-finjournal.
const ExpectedSchemaVersion = 1

// CheckSchemaVersion verifies that ledger.schema_meta matches ExpectedSchemaVersion (RFC Section 14).
func (s *Store) CheckSchemaVersion(ctx context.Context) (int, error) {
	var ver int
	err := s.db.QueryRowContext(ctx, `SELECT schema_version FROM ledger.schema_meta WHERE id = 1`).Scan(&ver)
	if err != nil {
		return 0, fmt.Errorf("postgres: check schema version: %w", err)
	}
	if ver != ExpectedSchemaVersion {
		return ver, fmt.Errorf("postgres: schema version mismatch: got %d, expected %d", ver, ExpectedSchemaVersion)
	}
	return ver, nil
}

// NewPool creates a PostgreSQL Store backed by a *pgxpool.Pool.
func NewPool(pool *pgxpool.Pool) *Store {
	return &Store{
		db:     stdlib.OpenDBFromPool(pool),
		pool:   pool,
		schema: "ledger",
	}
}

// New creates a PostgreSQL Store using an existing *sql.DB connection pool.
func New(db *sql.DB) *Store {
	return &Store{
		db:     db,
		schema: "ledger",
	}
}

// Pool returns the underlying *pgxpool.Pool if the Store was opened via Open or NewPool.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// DB returns the underlying *sql.DB handle.
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Admin() storage.Admin       { return s }
func (s *Store) Outbox() storage.Outbox     { return s }
func (s *Store) Journal() storage.Journal   { return s }
func (s *Store) Batch() storage.BatchPoster { return s }

func (s *Store) Close() error {
	err := s.db.Close()
	if s.pool != nil {
		s.pool.Close()
	}
	return err
}

// PostBatch executes a batch of intents in a single network round-trip via ledger.post_batch(intents jsonb).
func (s *Store) PostBatch(ctx context.Context, items []storage.BatchCommitItem) ([]storage.BatchCommitResult, error) {
	if len(items) == 0 {
		return nil, nil
	}
	type jsonBatchItem struct {
		Op           uint8             `json:"op"`
		Tx           *core.Transaction `json:"tx"`
		RequestHash  string            `json:"request_hash"`
		TargetTxID   string            `json:"target_tx_id,omitempty"`
		ReversalAmts []string          `json:"reversal_amts,omitempty"`
		Events       []events.Event    `json:"events"`
	}
	payloadItems := make([]jsonBatchItem, len(items))
	for i, it := range items {
		var targetID string
		if it.TargetTxID != nil {
			targetID = string(*it.TargetTxID)
		}
		var revAmts []string
		if len(it.ReversalAmts) > 0 {
			revAmts = make([]string, len(it.ReversalAmts))
			for j, a := range it.ReversalAmts {
				revAmts[j] = a.String()
			}
		}
		payloadItems[i] = jsonBatchItem{
			Op:           it.Op,
			Tx:           it.Tx,
			RequestHash:  hex.EncodeToString(it.RequestHash),
			TargetTxID:   targetID,
			ReversalAmts: revAmts,
			Events:       it.Events,
		}
	}
	batchJSON, err := json.Marshal(payloadItems)
	if err != nil {
		return nil, err
	}

	var respJSON []byte
	err = s.db.QueryRowContext(ctx, `SELECT ledger.post_batch($1::jsonb)`, batchJSON).Scan(&respJSON)
	if err != nil {
		return nil, mapPgErr(err)
	}

	type jsonBatchResult struct {
		OK               bool   `json:"ok"`
		IdempotentReplay bool   `json:"idempotent_replay"`
		TxID             string `json:"tx_id"`
		ErrorCode        string `json:"error_code"`
		Message          string `json:"message"`
	}
	var rawResults []jsonBatchResult
	if err := json.Unmarshal(respJSON, &rawResults); err != nil {
		return nil, err
	}
	if len(rawResults) != len(items) {
		return nil, fmt.Errorf("postgres: post_batch returned %d results for %d items", len(rawResults), len(items))
	}

	out := make([]storage.BatchCommitResult, len(items))
	for i, r := range rawResults {
		if !r.OK {
			code := r.ErrorCode
			if code == "" {
				code = core.CodeValidationFailed
			}
			out[i] = storage.BatchCommitResult{
				Err: core.NewError(code, r.Message, nil),
			}
			continue
		}
		if r.IdempotentReplay {
			storedTx, err := s.Transaction(ctx, core.TransactionID(r.TxID))
			if err != nil {
				return nil, err
			}
			storedTx.IdempotentReplay = true
			out[i] = storage.BatchCommitResult{
				Tx:               storedTx,
				IdempotentReplay: true,
			}
			continue
		}
		out[i] = storage.BatchCommitResult{
			Tx: items[i].Tx,
		}
	}
	return out, nil
}

func (s *Store) Begin(ctx context.Context) (storage.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &pgTx{runner: &sqlTxRunner{tx: tx}, external: false}, nil
}

// Join wraps a caller-opened pgx.Tx or *sql.Tx so library users can post inside their own transaction.
func (s *Store) Join(_ context.Context, external any) (storage.Tx, error) {
	switch tx := external.(type) {
	case *sql.Tx:
		if tx == nil {
			return nil, storage.ErrJoinUnsupported
		}
		return &pgTx{runner: &sqlTxRunner{tx: tx}, external: true}, nil
	case pgx.Tx:
		if tx == nil {
			return nil, storage.ErrJoinUnsupported
		}
		return &pgTx{runner: &pgxTxRunner{tx: tx}, external: true}, nil
	default:
		return nil, storage.ErrJoinUnsupported
	}
}

type txRunner interface {
	exec(ctx context.Context, query string, args ...any) (int64, error)
	queryRow(ctx context.Context, query string, args ...any) rowScanner
	commit(ctx context.Context) error
	rollback(ctx context.Context) error
}

type sqlTxRunner struct {
	tx *sql.Tx
}

func (r *sqlTxRunner) exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := r.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (r *sqlTxRunner) queryRow(ctx context.Context, query string, args ...any) rowScanner {
	return r.tx.QueryRowContext(ctx, query, args...)
}

func (r *sqlTxRunner) commit(_ context.Context) error   { return r.tx.Commit() }
func (r *sqlTxRunner) rollback(_ context.Context) error { return r.tx.Rollback() }

type pgxTxRunner struct {
	tx pgx.Tx
}

func (r *pgxTxRunner) exec(ctx context.Context, query string, args ...any) (int64, error) {
	tag, err := r.tx.Exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (r *pgxTxRunner) queryRow(ctx context.Context, query string, args ...any) rowScanner {
	return r.tx.QueryRow(ctx, query, args...)
}

func (r *pgxTxRunner) commit(ctx context.Context) error   { return r.tx.Commit(ctx) }
func (r *pgxTxRunner) rollback(ctx context.Context) error { return r.tx.Rollback(ctx) }

type pgTx struct {
	runner   txRunner
	external bool
}

func (t *pgTx) LockBalances(ctx context.Context, ids []core.AccountID) ([]core.Balance, error) {
	if len(ids) == 0 {
		return nil, nil
	}
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
		row := t.runner.queryRow(ctx, `
			SELECT b.account_id, a.asset, b.posted_debits::text, b.posted_credits::text,
			       b.pending_debits::text, b.pending_credits::text, b.version
			FROM ledger.balances b
			JOIN ledger.accounts a ON a.id = b.account_id
			WHERE b.account_id = $1
			FOR UPDATE OF b`, string(id))
		b, err := scanBalance(row)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf("%w: account %s", storage.ErrNotFound, id)
			}
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

func (t *pgTx) ClaimIdempotency(ctx context.Context, recs []storage.IdemRecord) ([]storage.IdemRecord, error) {
	var existing []storage.IdemRecord
	for _, r := range recs {
		affected, err := t.runner.exec(ctx, `
			INSERT INTO ledger.idempotency (ledger_id, key, request_hash, tx_id, created_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (ledger_id, key) DO NOTHING`,
			string(r.Ledger), r.Key, r.RequestHash, string(r.TxID), r.CreatedAt)
		if err != nil {
			return nil, err
		}
		if affected == 0 {
			var ex storage.IdemRecord
			var lID, txID string
			err := t.runner.queryRow(ctx, `
				SELECT ledger_id, key, request_hash, tx_id, created_at
				FROM ledger.idempotency WHERE ledger_id = $1 AND key = $2`,
				string(r.Ledger), r.Key).Scan(&lID, &ex.Key, &ex.RequestHash, &txID, &ex.CreatedAt)
			if err != nil {
				return nil, err
			}
			ex.Ledger = core.LedgerID(lID)
			ex.TxID = core.TransactionID(txID)
			existing = append(existing, ex)
		}
	}
	return existing, nil
}

func (t *pgTx) InsertTransactions(ctx context.Context, txs []*core.Transaction) error {
	for _, tx := range txs {
		postingsJSON, err := json.Marshal(tx.Postings)
		if err != nil {
			return err
		}
		metaJSON, err := json.Marshal(nonNilMeta(tx.Metadata))
		if err != nil {
			return err
		}
		_, err = t.runner.exec(ctx, `
			INSERT INTO ledger.transactions
			(id, ledger_id, kind, pending, postings, effective_at, committed_at, expires_at, reverses, pending_id, idempotency_key, metadata)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			string(tx.ID), string(tx.Ledger), tx.Kind, tx.Status == core.StatusPending,
			postingsJSON, tx.EffectiveAt, tx.CommittedAt, tx.ExpiresAt,
			nullableTxID(tx.Reverses), nullableTxID(tx.PendingID), tx.IdempotencyKey, metaJSON)
		if err != nil {
			return err
		}
		rev := make([]money.Amount, len(tx.Postings))
		revJSON, _ := json.Marshal(rev)
		_, err = t.runner.exec(ctx, `
			INSERT INTO ledger.tx_state (tx_id, status, reversed)
			VALUES ($1, $2, $3)
			ON CONFLICT (tx_id) DO NOTHING`,
			string(tx.ID), int16(tx.Status), revJSON)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *pgTx) InsertEntries(ctx context.Context, es []core.Entry) error {
	for _, e := range es {
		var postedAfter *string
		if e.PostedAfter != nil {
			s := e.PostedAfter.String()
			postedAfter = &s
		}
		_, err := t.runner.exec(ctx, `
			INSERT INTO ledger.entries
			(tx_id, posting_index, side, account_id, amount, posted_after, version, effective_at, committed_at)
			VALUES ($1, $2, $3, $4, $5::numeric, $6::numeric, $7, $8, $9)`,
			string(e.Transaction), int16(e.PostingIndex), int16(e.Side),
			string(e.Account), e.Amount.String(), postedAfter, e.Version, e.EffectiveAt, e.CommittedAt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *pgTx) GetTxStates(ctx context.Context, ids []core.TransactionID) (map[core.TransactionID]storage.TxState, error) {
	out := make(map[core.TransactionID]storage.TxState, len(ids))
	for _, id := range ids {
		var status int16
		var revBytes []byte
		err := t.runner.queryRow(ctx, `
			SELECT status, reversed FROM ledger.tx_state WHERE tx_id = $1 FOR UPDATE`, string(id)).
			Scan(&status, &revBytes)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf("%w: tx_state %s", storage.ErrNotFound, id)
			}
			return nil, err
		}
		var rev []money.Amount
		if err := json.Unmarshal(revBytes, &rev); err != nil {
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

func (t *pgTx) SetTxState(ctx context.Context, st []storage.TxState) error {
	for _, s := range st {
		revJSON, err := json.Marshal(s.Reversed)
		if err != nil {
			return err
		}
		_, err = t.runner.exec(ctx, `
			INSERT INTO ledger.tx_state (tx_id, status, reversed)
			VALUES ($1, $2, $3)
			ON CONFLICT (tx_id) DO UPDATE SET status = EXCLUDED.status, reversed = EXCLUDED.reversed`,
			string(s.TxID), int16(s.Status), revJSON)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *pgTx) WriteBalances(ctx context.Context, bs []core.Balance) error {
	for _, b := range bs {
		_, err := t.runner.exec(ctx, `
			UPDATE ledger.balances
			SET posted_debits = $2::numeric,
			    posted_credits = $3::numeric,
			    pending_debits = $4::numeric,
			    pending_credits = $5::numeric,
			    version = $6,
			    updated_at = now()
			WHERE account_id = $1`,
			string(b.Account), b.PostedDebits.String(), b.PostedCredits.String(),
			b.PendingDebits.String(), b.PendingCredits.String(), b.Version)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *pgTx) AppendOutbox(ctx context.Context, evs []events.Event) error {
	for _, ev := range evs {
		_, err := t.runner.exec(ctx, `
			INSERT INTO ledger.outbox (event_id, ledger_id, type, payload, created_at)
			VALUES ($1, $2, $3, $4, $5)`,
			ev.ID, string(ev.Ledger), ev.Type, []byte(ev.Payload), ev.CreatedAt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *pgTx) Commit(ctx context.Context) error {
	if t.external {
		// Caller owns the transaction lifecycle when using WithinTx / Join
		return nil
	}
	return t.runner.commit(ctx)
}

func (t *pgTx) Rollback(ctx context.Context) error {
	if t.external {
		return nil
	}
	return t.runner.rollback(ctx)
}

// --- Admin methods ---

func (s *Store) CreateLedger(ctx context.Context, l core.Ledger) error {
	metaJSON, _ := json.Marshal(nonNilMeta(l.Metadata))
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ledger.ledgers (id, name, metadata, created_at)
		VALUES ($1, $2, $3, $4)`,
		string(l.ID), l.Name, metaJSON, l.CreatedAt)
	return mapPgErr(err)
}

func (s *Store) RegisterAsset(ctx context.Context, l core.LedgerID, a money.Asset) error {
	if err := a.Validate(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ledger.assets (ledger_id, id, code, scale)
		VALUES ($1, $2, $3, $4)`,
		string(l), string(a.ID()), a.Code, int16(a.Scale))
	return mapPgErr(err)
}

func (s *Store) CreateHolder(ctx context.Context, h core.AccountHolder) error {
	metaJSON, _ := json.Marshal(nonNilMeta(h.Metadata))
	var parent *string
	if h.ParentID != nil {
		p := string(*h.ParentID)
		parent = &p
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ledger.holders (id, ledger_id, kind, external_ref, parent_id, metadata, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		string(h.ID), string(h.Ledger), h.Kind, h.ExternalRef, parent, metaJSON, h.CreatedAt)
	return mapPgErr(err)
}

func (s *Store) CreateAccount(ctx context.Context, a core.Account) error {
	if a.BalanceMode == core.Deferred && (a.Limits.Min != nil || a.Limits.Max != nil) {
		return core.NewError(core.CodeValidationFailed, "deferred account cannot have limits", nil)
	}
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
	_, err = tx.ExecContext(ctx, `
		INSERT INTO ledger.accounts
		(id, ledger_id, holder_id, code, asset, normal_side, balance_mode, min_available, max_available, status, metadata, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::numeric, $9::numeric, $10, $11, $12)`,
		string(a.ID), string(a.Ledger), holder, a.Code, string(a.Asset),
		int16(a.Normal), int16(a.BalanceMode), minStr, maxStr, int16(a.Status), metaJSON, a.CreatedAt)
	if err != nil {
		return mapPgErr(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO ledger.balances (account_id) VALUES ($1)`, string(a.ID))
	if err != nil {
		return mapPgErr(err)
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
	_, err = s.db.ExecContext(ctx, `
		UPDATE ledger.accounts
		SET status = $2, min_available = $3::numeric, max_available = $4::numeric, metadata = $5
		WHERE id = $1`,
		string(id), int16(a.Status), minStr, maxStr, metaJSON)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *Store) CreatePeriod(ctx context.Context, p core.Period) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ledger.periods (ledger_id, starts_at, ends_at, closed_at)
		VALUES ($1, $2, $3, $4)`,
		string(p.Ledger), p.StartsAt, p.EndsAt, p.ClosedAt)
	return mapPgErr(err)
}

func (s *Store) ClosePeriod(ctx context.Context, l core.LedgerID, startsAt time.Time, closedAt time.Time) (*core.Period, error) {
	endsAt := startsAt.UTC().AddDate(0, 1, 0)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ledger.periods (ledger_id, starts_at, ends_at, closed_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (ledger_id, starts_at) DO UPDATE SET closed_at = EXCLUDED.closed_at`,
		string(l), startsAt.UTC(), endsAt, closedAt.UTC())
	if err != nil {
		return nil, err
	}
	c := closedAt.UTC()
	return &core.Period{Ledger: l, StartsAt: startsAt.UTC(), EndsAt: endsAt, ClosedAt: &c}, nil
}

func (s *Store) PruneIdempotency(ctx context.Context, before time.Time) (int, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM ledger.idempotency WHERE created_at < $1`, before)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *Store) ExecModuleMigration(ctx context.Context, moduleName, migrationID, sqlText string) error {
	var exists int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM ledger.module_migrations WHERE module = $1 AND migration_id = $2`,
		moduleName, migrationID).Scan(&exists)
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
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO ledger.module_migrations (module, migration_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		moduleName, migrationID); err != nil {
		return err
	}
	return tx.Commit()
}

// --- Reader methods ---

func (s *Store) Ledger(ctx context.Context, id core.LedgerID) (*core.Ledger, error) {
	var l core.Ledger
	var lID string
	var metaBytes []byte
	err := s.db.QueryRowContext(ctx, `SELECT id, name, metadata, created_at FROM ledger.ledgers WHERE id = $1`, string(id)).
		Scan(&lID, &l.Name, &metaBytes, &l.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: ledger %s", storage.ErrNotFound, id)
		}
		return nil, err
	}
	l.ID = core.LedgerID(lID)
	_ = json.Unmarshal(metaBytes, &l.Metadata)
	return &l, nil
}

func (s *Store) Asset(ctx context.Context, l core.LedgerID, id money.AssetID) (*money.Asset, error) {
	var a money.Asset
	var scale int16
	err := s.db.QueryRowContext(ctx, `SELECT code, scale FROM ledger.assets WHERE ledger_id = $1 AND id = $2`, string(l), string(id)).
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
	rows, err := s.db.QueryContext(ctx, `SELECT code, scale FROM ledger.assets WHERE ledger_id = $1 ORDER BY id`, string(l))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []money.Asset
	for rows.Next() {
		var a money.Asset
		var scale int16
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
	var hID, lID string
	var parent *string
	var metaBytes []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT id, ledger_id, kind, external_ref, parent_id, metadata, created_at
		FROM ledger.holders WHERE id = $1`, string(id)).
		Scan(&hID, &lID, &h.Kind, &h.ExternalRef, &parent, &metaBytes, &h.CreatedAt)
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
	_ = json.Unmarshal(metaBytes, &h.Metadata)
	return &h, nil
}

func (s *Store) Accounts(ctx context.Context, ids []core.AccountID) ([]core.Account, error) {
	out := make([]core.Account, 0, len(ids))
	for _, id := range ids {
		row := s.db.QueryRowContext(ctx, `
			SELECT id, ledger_id, holder_id, code, asset, normal_side, balance_mode,
			       min_available::text, max_available::text, status, metadata, created_at
			FROM ledger.accounts WHERE id = $1`, string(id))
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
		       min_available::text, max_available::text, status, metadata, created_at
		FROM ledger.accounts WHERE ledger_id = $1 AND code = $2`, string(l), code)
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
		       min_available::text, max_available::text, status, metadata, created_at
		FROM ledger.accounts WHERE ledger_id = $1 ORDER BY id`, string(l))
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
		SELECT b.account_id, a.asset, b.posted_debits::text, b.posted_credits::text,
		       b.pending_debits::text, b.pending_credits::text, b.version
		FROM ledger.balances b
		JOIN ledger.accounts a ON a.id = b.account_id
		WHERE b.account_id = $1`, string(id))
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
	if a.BalanceMode == core.Locked {
		var postedAfter *string
		err := s.db.QueryRowContext(ctx, `
			SELECT posted_after::text
			FROM ledger.entries
			WHERE account_id = $1 AND committed_at <= $2
			ORDER BY committed_at DESC, version DESC
			LIMIT 1`, string(id), at).Scan(&postedAfter)
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
	// Deferred account: sum entries at or before `at`
	rows, err := s.db.QueryContext(ctx, `
		SELECT side, amount::text FROM ledger.entries WHERE account_id = $1 AND committed_at <= $2`, string(id), at)
	if err != nil {
		return money.Zero, err
	}
	defer rows.Close()
	debits := money.Zero
	credits := money.Zero
	for rows.Next() {
		var side int16
		var amtStr string
		if err := rows.Scan(&side, &amtStr); err != nil {
			return money.Zero, err
		}
		amt, err := money.ParseMinor(amtStr)
		if err != nil {
			return money.Zero, err
		}
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
		       b.posted_debits::text, b.posted_credits::text,
		       b.pending_debits::text, b.pending_credits::text, b.version
		FROM ledger.accounts a
		JOIN ledger.balances b ON b.account_id = a.id
		WHERE a.holder_id = $1`, string(h))
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
		var normal int16
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
		SELECT t.id, t.ledger_id, t.kind, COALESCE(st.status, CASE WHEN t.pending THEN 2 ELSE 1 END),
		       t.postings, t.effective_at, t.committed_at, t.expires_at, t.reverses, t.pending_id,
		       t.idempotency_key, t.metadata
		FROM ledger.transactions t
		LEFT JOIN ledger.tx_state st ON st.tx_id = t.id
		WHERE t.id = $1`, string(id))
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
		SELECT t.id, t.ledger_id, t.kind, COALESCE(st.status, CASE WHEN t.pending THEN 2 ELSE 1 END),
		       t.postings, t.effective_at, t.committed_at, t.expires_at, t.reverses, t.pending_id,
		       t.idempotency_key, t.metadata
		FROM ledger.transactions t
		LEFT JOIN ledger.tx_state st ON st.tx_id = t.id
		WHERE t.ledger_id = $1
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
	var status int16
	var revBytes []byte
	err := s.db.QueryRowContext(ctx, `SELECT status, reversed FROM ledger.tx_state WHERE tx_id = $1`, string(id)).
		Scan(&status, &revBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: tx_state %s", storage.ErrNotFound, id)
		}
		return nil, err
	}
	var rev []money.Amount
	_ = json.Unmarshal(revBytes, &rev)
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
		FROM ledger.transactions t
		JOIN ledger.tx_state st ON st.tx_id = t.id
		WHERE st.status = 2 AND t.expires_at IS NOT NULL AND t.expires_at <= $1
		LIMIT $2`, now, limit)
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
		SELECT tx_id, posting_index, side, account_id, amount::text, posted_after::text,
		       COALESCE(version, 0), effective_at, committed_at
		FROM ledger.entries
		WHERE account_id = $1
		ORDER BY committed_at DESC, version DESC
		LIMIT $2 OFFSET $3`, string(q.Account), limit+1, offset)
	if err != nil {
		return storage.Page[core.Entry]{}, err
	}
	defer rows.Close()
	var items []core.Entry
	for rows.Next() {
		var e core.Entry
		var txID, acctID, amtStr string
		var pIdx, side int16
		var postedAfter *string
		if err := rows.Scan(&txID, &pIdx, &side, &acctID, &amtStr, &postedAfter, &e.Version, &e.EffectiveAt, &e.CommittedAt); err != nil {
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
	var lID, txID string
	err := s.db.QueryRowContext(ctx, `
		SELECT ledger_id, key, request_hash, tx_id, created_at
		FROM ledger.idempotency WHERE ledger_id = $1 AND key = $2`, string(l), key).
		Scan(&lID, &r.Key, &r.RequestHash, &txID, &r.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: idempotency key %s", storage.ErrNotFound, key)
		}
		return nil, err
	}
	r.Ledger = core.LedgerID(lID)
	r.TxID = core.TransactionID(txID)
	return &r, nil
}

func (s *Store) Periods(ctx context.Context, l core.LedgerID) ([]core.Period, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ledger_id, starts_at, ends_at, closed_at
		FROM ledger.periods WHERE ledger_id = $1 ORDER BY starts_at`, string(l))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.Period
	for rows.Next() {
		var p core.Period
		var lID string
		if err := rows.Scan(&lID, &p.StartsAt, &p.EndsAt, &p.ClosedAt); err != nil {
			return nil, err
		}
		p.Ledger = core.LedgerID(lID)
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) FoldDeferredBalances(ctx context.Context, l core.LedgerID) (int, error) {
	accts, err := s.ListAccounts(ctx, l)
	if err != nil {
		return 0, err
	}
	folded := 0
	for _, a := range accts {
		if a.BalanceMode != core.Deferred {
			continue
		}
		rows, err := s.db.QueryContext(ctx, `
			SELECT side, amount::text
			FROM ledger.entries
			WHERE account_id = $1
			ORDER BY committed_at ASC`, string(a.ID))
		if err != nil {
			return folded, err
		}
		debits := money.Zero
		credits := money.Zero
		var ver uint64
		for rows.Next() {
			var side int16
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
			UPDATE ledger.balances
			SET posted_debits = $2::numeric, posted_credits = $3::numeric, version = $4, updated_at = now()
			WHERE account_id = $1`, string(a.ID), debits.String(), credits.String(), ver)
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
		FROM ledger.outbox
		WHERE ledger_id = $1 AND id > $2
		ORDER BY id ASC LIMIT $3`, string(l), afterSeq, limit)
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
		FROM ledger.outbox
		WHERE id > $1
		ORDER BY id ASC LIMIT $2`, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *Store) GetCursor(ctx context.Context, subscriber string) (uint64, error) {
	var seq uint64
	err := s.db.QueryRowContext(ctx, `SELECT seq FROM ledger.subscriber_cursors WHERE subscriber = $1`, subscriber).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

func (s *Store) SetCursor(ctx context.Context, subscriber string, seq uint64, updatedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ledger.subscriber_cursors (subscriber, seq, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (subscriber) DO UPDATE SET seq = EXCLUDED.seq, updated_at = EXCLUDED.updated_at`,
		subscriber, seq, updatedAt)
	return err
}

func (s *Store) PruneDelivered(ctx context.Context, before time.Time) (int, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM ledger.outbox WHERE delivered_at IS NOT NULL AND delivered_at < $1`, before)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// --- Journal methods ---

func (s *Store) TryAdvisoryLock(ctx context.Context, lockKey string) (func(), bool, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1)::bigint)`, lockKey).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, false, err
	}
	if !acquired {
		_ = conn.Close()
		return nil, false, nil
	}
	unlock := func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext($1)::bigint)`, lockKey)
		_ = conn.Close()
	}
	return unlock, true, nil
}

func (s *Store) UnsealedTransactions(ctx context.Context, l core.LedgerID, limit int) ([]*core.Transaction, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT t.id, t.ledger_id, t.kind, COALESCE(st.status, CASE WHEN t.pending THEN 2 ELSE 1 END),
		       t.postings, t.effective_at, t.committed_at, t.expires_at, t.reverses, t.pending_id,
		       t.idempotency_key, t.metadata
		FROM ledger.transactions t
		LEFT JOIN ledger.tx_state st ON st.tx_id = t.id
		LEFT JOIN ledger.journal j ON j.tx_id = t.id
		WHERE t.ledger_id = $1 AND j.tx_id IS NULL
		ORDER BY t.committed_at ASC, t.id ASC
		LIMIT $2`, string(l), limit)
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
	var lID, txID string
	err := s.db.QueryRowContext(ctx, `
		SELECT ledger_id, seq, tx_id, tx_hash, chain_hash, sealed_at
		FROM ledger.journal WHERE ledger_id = $1 ORDER BY seq DESC LIMIT 1`, string(l)).
		Scan(&lID, &r.Seq, &txID, &r.TxHash, &r.ChainHash, &r.SealedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	r.Ledger = core.LedgerID(lID)
	r.TxID = core.TransactionID(txID)
	return &r, nil
}

func (s *Store) AppendRecords(ctx context.Context, records []core.JournalRecord, checkpoint *core.Checkpoint) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, r := range records {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO ledger.journal (ledger_id, seq, tx_id, tx_hash, chain_hash, sealed_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (tx_id) DO NOTHING`,
			string(r.Ledger), r.Seq, string(r.TxID), r.TxHash, r.ChainHash, r.SealedAt)
		if err != nil {
			return err
		}
	}
	if checkpoint != nil {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO ledger.checkpoints (ledger_id, seq, chain_hash, created_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (ledger_id, seq) DO NOTHING`,
			string(checkpoint.Ledger), checkpoint.Seq, checkpoint.ChainHash, checkpoint.CreatedAt)
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
		FROM ledger.journal WHERE ledger_id = $1 AND seq > $2 ORDER BY seq ASC LIMIT $3`,
		string(l), afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.JournalRecord
	for rows.Next() {
		var r core.JournalRecord
		var lID, txID string
		if err := rows.Scan(&lID, &r.Seq, &txID, &r.TxHash, &r.ChainHash, &r.SealedAt); err != nil {
			return nil, err
		}
		r.Ledger = core.LedgerID(lID)
		r.TxID = core.TransactionID(txID)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Checkpoints(ctx context.Context, l core.LedgerID) ([]core.Checkpoint, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ledger_id, seq, chain_hash, created_at
		FROM ledger.checkpoints WHERE ledger_id = $1 ORDER BY seq ASC`, string(l))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.Checkpoint
	for rows.Next() {
		var c core.Checkpoint
		var lID string
		if err := rows.Scan(&lID, &c.Seq, &c.ChainHash, &c.CreatedAt); err != nil {
			return nil, err
		}
		c.Ledger = core.LedgerID(lID)
		out = append(out, c)
	}
	return out, rows.Err()
}

// --- Row scanners & helpers ---

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
	var id, lID, assetStr string
	var holder, minStr, maxStr *string
	var normal, mode, status int16
	var metaBytes []byte
	if err := r.Scan(&id, &lID, &holder, &a.Code, &assetStr, &normal, &mode, &minStr, &maxStr, &status, &metaBytes, &a.CreatedAt); err != nil {
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
	_ = json.Unmarshal(metaBytes, &a.Metadata)
	return a, nil
}

func scanTransaction(r rowScanner) (*core.Transaction, error) {
	var tx core.Transaction
	var id, lID string
	var status int16
	var postingsBytes, metaBytes []byte
	var reverses, pendingID *string
	if err := r.Scan(&id, &lID, &tx.Kind, &status, &postingsBytes, &tx.EffectiveAt, &tx.CommittedAt,
		&tx.ExpiresAt, &reverses, &pendingID, &tx.IdempotencyKey, &metaBytes); err != nil {
		return nil, err
	}
	tx.ID = core.TransactionID(id)
	tx.Ledger = core.LedgerID(lID)
	tx.Status = core.TxStatus(status)
	_ = json.Unmarshal(postingsBytes, &tx.Postings)
	if reverses != nil {
		rv := core.TransactionID(*reverses)
		tx.Reverses = &rv
	}
	if pendingID != nil {
		pi := core.TransactionID(*pendingID)
		tx.PendingID = &pi
	}
	_ = json.Unmarshal(metaBytes, &tx.Metadata)
	return &tx, nil
}

func scanEvents(rows *sql.Rows) ([]events.Event, error) {
	var out []events.Event
	for rows.Next() {
		var ev events.Event
		var lID string
		var payload []byte
		if err := rows.Scan(&ev.Seq, &ev.ID, &lID, &ev.Type, &payload, &ev.CreatedAt, &ev.DeliveredAt); err != nil {
			return nil, err
		}
		ev.Ledger = core.LedgerID(lID)
		ev.Payload = payload
		out = append(out, ev)
	}
	return out, rows.Err()
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

func mapPgErr(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "duplicate key") || strings.Contains(msg, "23505") {
		return fmt.Errorf("%w: %v", storage.ErrAlreadyExists, err)
	}
	if strings.Contains(msg, "foreign key") || strings.Contains(msg, "23503") {
		return fmt.Errorf("%w: %v", storage.ErrNotFound, err)
	}
	if strings.Contains(msg, "55P03") || strings.Contains(msg, "40P01") || strings.Contains(msg, "40001") ||
		strings.Contains(msg, "lock timeout") || strings.Contains(msg, "deadlock") {
		return core.NewError(core.CodeLockTimeout, msg, map[string]any{"cause": msg})
	}
	return err
}
