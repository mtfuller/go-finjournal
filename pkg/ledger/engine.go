package ledger

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/commit"
	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/journal"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/module"
	"github.com/mtfuller/go-finjournal/pkg/storage"
)

// Option configures an Engine at construction time.
type Option func(*Engine) error

// WithModules registers one or more modules in execution order.
func WithModules(mods ...module.Module) Option {
	return func(e *Engine) error {
		e.modules = append(e.modules, mods...)
		return nil
	}
}

// WithOptionalModule registers a module whose sync-hook failures or timeouts are skipped
// rather than failing closed (RFC section 9).
func WithOptionalModule(mod module.Module) Option {
	return func(e *Engine) error {
		e.modules = append(e.modules, mod)
		e.optionalModules[mod.Name()] = true
		return nil
	}
}

// WithModuleConfigs provides per-module configuration values decoded via Host.DecodeConfig.
func WithModuleConfigs(cfgs map[string]any) Option {
	return func(e *Engine) error {
		for k, v := range cfgs {
			e.moduleConfigs[k] = v
		}
		return nil
	}
}

// WithCommitter sets a custom Committer (DirectCommitter is the default).
func WithCommitter(c commit.Committer) Option {
	return func(e *Engine) error {
		e.committer = c
		return nil
	}
}

// WithBatchCommitter configures the engine to use BatchedCommitter.
func WithBatchCommitter(cfg commit.BatchConfig) Option {
	return func(e *Engine) error {
		bc, err := commit.NewBatched(e.store, cfg)
		if err != nil {
			return err
		}
		e.committer = bc
		return nil
	}
}

// WithRoundingMode sets the engine's default rounding mode (money.HalfEven by default).
func WithRoundingMode(m money.RoundingMode) Option {
	return func(e *Engine) error {
		e.roundingMode = m
		return nil
	}
}

// WithClock overrides the engine's Clock (used for deterministic simulation and tests).
func WithClock(c Clock) Option {
	return func(e *Engine) error {
		e.clock = c
		return nil
	}
}

// WithIDGenerator overrides the engine's UUIDv7 generator (used for deterministic simulation).
func WithIDGenerator(g IDGenerator) Option {
	return func(e *Engine) error {
		e.idGen = g
		return nil
	}
}

// WithLogger sets the engine's structured logger.
func WithLogger(l *slog.Logger) Option {
	return func(e *Engine) error {
		e.logger = l
		return nil
	}
}

// WithHookTimeouts sets the total sync hook budget (default 50ms) and per-module timeout (default 20ms).
func WithHookTimeouts(totalBudget, perModule time.Duration) Option {
	return func(e *Engine) error {
		if totalBudget > 0 {
			e.hookTotalTimeout = totalBudget
		}
		if perModule > 0 {
			e.hookModuleTimeout = perModule
		}
		return nil
	}
}

// WithDeferredFoldInterval configures a background worker that folds Deferred account balances
// across known ledgers every interval (e.g. 100ms per RFC section 6).
func WithDeferredFoldInterval(interval time.Duration) Option {
	return func(e *Engine) error {
		e.deferredFoldInterval = interval
		return nil
	}
}

// PostOption configures a single Post, SettleHold, VoidHold, or Reverse call.
type PostOption func(*postCallOptions)

type postCallOptions struct {
	externalTx any
	principal  *module.Principal
}

// WithinTx runs the posting inside the caller's open database transaction (e.g. *sql.Tx).
func WithinTx(tx any) PostOption {
	return func(o *postCallOptions) {
		o.externalTx = tx
	}
}

// WithPrincipal attaches a caller Principal for server-mode authorization.
func WithPrincipal(p module.Principal) PostOption {
	return func(o *postCallOptions) {
		o.principal = &p
	}
}

// Engine is the core double-entry ledger engine.
type Engine struct {
	store             storage.Store
	committer         commit.Committer
	sealer            *journal.Sealer
	clock             Clock
	idGen             IDGenerator
	roundingMode      money.RoundingMode
	logger            *slog.Logger
	hookTotalTimeout     time.Duration
	hookModuleTimeout    time.Duration
	deferredFoldInterval time.Duration
	foldStopCh           chan struct{}
	foldWG               sync.WaitGroup
	knownLedgersMu       sync.RWMutex
	knownLedgers         map[LedgerID]bool

	modules         []module.Module
	optionalModules map[string]bool
	moduleConfigs   map[string]any
	enrichers       []module.Enricher
	validators      []module.Validator
	authorizers     []module.Authorizer
	subscribers     []module.Subscriber
	rateProviders   []module.RateProvider
	telemetry       *engineTelemetry

	cacheMu          sync.RWMutex
	accountByIDCache map[AccountID]Account
	accountByCode    map[string]AccountID // "<ledger>:<code_or_id>"
}

// New constructs and initializes a ledger Engine.
// Module Init methods run in registration order; any Init error stops startup.
func New(store storage.Store, opts ...Option) (*Engine, error) {
	if store == nil {
		return nil, errors.New("ledger: store must not be nil")
	}
	e := &Engine{
		store:             store,
		clock:             RealClock{},
		idGen:             NewUUIDv7Generator(),
		roundingMode:      money.HalfEven,
		logger:            slog.New(NewRedactingHandler(slog.Default().Handler())),
		hookTotalTimeout:  50 * time.Millisecond,
		hookModuleTimeout: 20 * time.Millisecond,
		optionalModules:   make(map[string]bool),
		moduleConfigs:     make(map[string]any),
		accountByIDCache:  make(map[AccountID]Account),
		accountByCode:     make(map[string]AccountID),
		knownLedgers:      make(map[LedgerID]bool),
		telemetry:         newEngineTelemetry(),
	}

	for _, opt := range opts {
		if err := opt(e); err != nil {
			return nil, err
		}
	}
	if e.committer == nil {
		e.committer = commit.NewDirect(store)
	}
	e.sealer = journal.NewSealer(store, journal.SealerConfig{
		Clock: e.clock,
		IDGen: e.idGen,
	})

	ctx := context.Background()
	for _, m := range e.modules {
		host := &moduleHost{engine: e, modName: m.Name()}
		if err := m.Init(ctx, host); err != nil {
			return nil, fmt.Errorf("ledger: init module %q: %w", m.Name(), err)
		}
		if enr, ok := m.(module.Enricher); ok {
			e.enrichers = append(e.enrichers, enr)
		} else if fe, ok := m.(module.FeeEngine); ok {
			e.enrichers = append(e.enrichers, module.WrapFeeEngine(fe))
		}
		if val, ok := m.(module.Validator); ok {
			e.validators = append(e.validators, val)
		}
		if auth, ok := m.(module.Authorizer); ok {
			e.authorizers = append(e.authorizers, auth)
		}
		if sub, ok := m.(module.Subscriber); ok {
			e.subscribers = append(e.subscribers, sub)
		}
		if rp, ok := m.(module.RateProvider); ok {
			e.rateProviders = append(e.rateProviders, rp)
		}
	}

	if e.deferredFoldInterval > 0 {
		e.foldStopCh = make(chan struct{})
		e.foldWG.Add(1)
		go e.deferredFoldLoop()
	}

	return e, nil
}

// Store returns the underlying storage.Store.
func (e *Engine) Store() storage.Store {
	return e.store
}

// Clock returns the engine's Clock.
func (e *Engine) Clock() Clock {
	return e.clock
}

// RoundingMode returns the engine's default RoundingMode.
func (e *Engine) RoundingMode() money.RoundingMode {
	return e.roundingMode
}

// Modules returns the registered modules on this engine.
func (e *Engine) Modules() []module.Module {
	out := make([]module.Module, len(e.modules))
	copy(out, e.modules)
	return out
}

// Metrics returns a point-in-time snapshot of engine telemetry counters and latencies.
func (e *Engine) Metrics() MetricsSnapshot {
	return e.telemetry.snapshot()
}

// QuoteFX requests a rate quote for (from, to) from the primary RateProvider module.
// If the RateProvider supports two-step quote creation with a custom TTL (default 30s per RFC section 8),
// it invokes CreateQuote; otherwise it falls back to Quote(ctx, from, to, now).
func (e *Engine) QuoteFX(ctx context.Context, from, to money.AssetID, ttl time.Duration) (*module.Quote, error) {
	if _, err := money.ParseAsset(from); err != nil {
		return nil, NewError(CodeValidationFailed, fmt.Sprintf("invalid from_asset: %v", err), nil)
	}
	if _, err := money.ParseAsset(to); err != nil {
		return nil, NewError(CodeValidationFailed, fmt.Sprintf("invalid to_asset: %v", err), nil)
	}
	if len(e.rateProviders) == 0 {
		return nil, NewError(CodeModuleUnavailable, "no RateProvider module registered", nil)
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	now := e.clock.Now()
	rp := e.rateProviders[0]
	type twoStepQuoter interface {
		CreateQuote(ctx context.Context, from, to money.AssetID, at time.Time, ttl time.Duration) (module.Quote, error)
	}
	if tsq, ok := rp.(twoStepQuoter); ok {
		q, err := tsq.CreateQuote(ctx, from, to, now, ttl)
		if err != nil {
			return nil, NewError(CodeModuleUnavailable, err.Error(), nil)
		}
		return &q, nil
	}
	q, err := rp.Quote(ctx, from, to, now)
	if err != nil {
		return nil, NewError(CodeModuleUnavailable, err.Error(), nil)
	}
	if q.ExpiresAt.IsZero() {
		q.ExpiresAt = now.Add(ttl)
	}
	return &q, nil
}

// Close shuts down the committer, stops background workers, and closes registered modules in reverse registration order.
func (e *Engine) Close(ctx context.Context) error {
	if e.foldStopCh != nil {
		select {
		case <-e.foldStopCh:
		default:
			close(e.foldStopCh)
		}
		e.foldWG.Wait()
	}
	var firstErr error
	if err := e.committer.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	for i := len(e.modules) - 1; i >= 0; i-- {
		if err := e.modules[i].Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (e *Engine) trackLedger(l LedgerID) {
	if l == "" {
		return
	}
	e.knownLedgersMu.RLock()
	exists := e.knownLedgers[l]
	e.knownLedgersMu.RUnlock()
	if exists {
		return
	}
	e.knownLedgersMu.Lock()
	e.knownLedgers[l] = true
	e.knownLedgersMu.Unlock()
}

func (e *Engine) deferredFoldLoop() {
	defer e.foldWG.Done()
	ticker := time.NewTicker(e.deferredFoldInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.foldStopCh:
			return
		case <-ticker.C:
			e.knownLedgersMu.RLock()
			ledgers := make([]LedgerID, 0, len(e.knownLedgers))
			for l := range e.knownLedgers {
				ledgers = append(ledgers, l)
			}
			e.knownLedgersMu.RUnlock()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			for _, l := range ledgers {
				_, _ = e.store.FoldDeferredBalances(ctx, l)
			}
			cancel()
		}
	}
}

// --- Admin & Setup helpers ---

// CreateLedger creates a new Ledger book.
func (e *Engine) CreateLedger(ctx context.Context, id LedgerID, name string, meta Metadata) (*Ledger, error) {
	if id == "" || name == "" {
		return nil, NewError(CodeValidationFailed, "ledger id and name are required", nil)
	}
	if err := meta.Validate(); err != nil {
		return nil, err
	}
	l := Ledger{
		ID:        id,
		Name:      name,
		Metadata:  meta.Clone(),
		CreatedAt: e.clock.Now(),
	}
	if err := e.store.Admin().CreateLedger(ctx, l); err != nil {
		return nil, mapStorageErr(err)
	}
	e.trackLedger(id)
	return &l, nil
}

// RegisterAsset registers an Asset (such as USD/2) in ledger l and ensures its system rounding
// and FX position accounts exist.
func (e *Engine) RegisterAsset(ctx context.Context, l LedgerID, code string, scale uint8) (*money.Asset, error) {
	a := money.Asset{Code: code, Scale: scale}
	if err := a.Validate(); err != nil {
		return nil, NewError(CodeValidationFailed, err.Error(), nil)
	}
	if err := e.store.Admin().RegisterAsset(ctx, l, a); err != nil {
		return nil, mapStorageErr(err)
	}
	// Automatically create Deferred-mode system rounding account for the asset (RFC Section 7)
	roundingCode := "system:rounding:" + assetCodeSlug(a)
	_, _ = e.ensureSystemAccount(ctx, l, roundingCode, a.ID(), Deferred, "core")
	return &a, nil
}

func assetCodeSlug(a money.Asset) string {
	low := strings.ToLower(a.Code)
	return fmt.Sprintf("%s_%d", low, a.Scale)
}

func (e *Engine) ensureSystemAccount(
	ctx context.Context,
	l LedgerID,
	code string,
	asset money.AssetID,
	mode BalanceMode,
	modName string,
) (AccountID, error) {
	if existing, err := e.store.AccountByCode(ctx, l, code); err == nil && existing != nil {
		return existing.ID, nil
	}
	if _, err := e.store.Ledger(ctx, l); errors.Is(err, storage.ErrNotFound) {
		_, _ = e.CreateLedger(ctx, l, string(l), nil)
	}
	if _, err := e.store.Asset(ctx, l, asset); errors.Is(err, storage.ErrNotFound) {
		if parsed, pErr := money.ParseAsset(asset); pErr == nil {
			_ = e.store.Admin().RegisterAsset(ctx, l, parsed)
		}
	}
	now := e.clock.Now()
	acct := Account{
		ID:          AccountID(e.idGen.NewID(now)),
		Ledger:      l,
		Code:        code,
		Asset:       asset,
		Normal:      Credit,
		BalanceMode: mode,
		Status:      AccountOpen,
		Metadata:    Metadata{"system": "true", "module": modName},
		CreatedAt:   now,
	}
	if err := e.store.Admin().CreateAccount(ctx, acct); err != nil {
		if existing, rErr := e.store.AccountByCode(ctx, l, code); rErr == nil && existing != nil {
			return existing.ID, nil
		}
		return "", mapStorageErr(err)
	}
	e.invalidateAccountCache(acct.ID, acct.Ledger, acct.Code)
	return acct.ID, nil
}

// CreateHolder creates an AccountHolder in a ledger.
func (e *Engine) CreateHolder(ctx context.Context, h AccountHolder) (*AccountHolder, error) {
	if h.Ledger == "" || h.Kind == "" || h.ExternalRef == "" {
		return nil, NewError(CodeValidationFailed, "ledger, kind, and external_ref are required", nil)
	}
	if h.ParentID != nil {
		return nil, NewError(CodeValidationFailed, "parent_id is reserved for v2 and must be nil in v1", nil)
	}
	if err := h.Metadata.Validate(); err != nil {
		return nil, err
	}
	now := e.clock.Now()
	if h.ID == "" {
		h.ID = HolderID(e.idGen.NewID(now))
	}
	if h.CreatedAt.IsZero() {
		h.CreatedAt = now
	}
	if err := e.store.Admin().CreateHolder(ctx, h); err != nil {
		return nil, mapStorageErr(err)
	}
	return &h, nil
}

// CreateAccount creates an Account in a ledger and emits an account.created outbox event.
func (e *Engine) CreateAccount(ctx context.Context, a Account) (*Account, error) {
	if a.Ledger == "" || a.Code == "" || a.Asset == "" {
		return nil, NewError(CodeValidationFailed, "ledger, code, and asset are required", nil)
	}
	if !a.Normal.Valid() {
		return nil, NewError(CodeValidationFailed, "normal side must be debit or credit", nil)
	}
	if a.BalanceMode == Deferred && (a.Limits.Min != nil || a.Limits.Max != nil) {
		return nil, NewError(CodeValidationFailed, "deferred account cannot have limits", nil)
	}
	if err := a.Metadata.Validate(); err != nil {
		return nil, err
	}
	now := e.clock.Now()
	if a.ID == "" {
		a.ID = AccountID(e.idGen.NewID(now))
	}
	if a.CreatedAt.IsZero() {
		a.CreatedAt = now
	}
	if err := e.store.Admin().CreateAccount(ctx, a); err != nil {
		return nil, mapStorageErr(err)
	}
	e.invalidateAccountCache(a.ID, a.Ledger, a.Code)

	// Emit account.created event
	tx, err := e.store.Begin(ctx)
	if err == nil {
		payload, _ := json.Marshal(a)
		_ = tx.AppendOutbox(ctx, []events.Event{{
			ID:        e.idGen.NewID(now),
			Ledger:    a.Ledger,
			Type:      events.TypeAccountCreated,
			Payload:   payload,
			CreatedAt: now,
		}})
		_ = tx.Commit(ctx)
	}

	return &a, nil
}

// UpdateAccount updates an Account's status, limits, or metadata.
func (e *Engine) UpdateAccount(ctx context.Context, id AccountID, patch storage.AccountPatch) (*Account, error) {
	if patch.Metadata != nil {
		if err := patch.Metadata.Validate(); err != nil {
			return nil, err
		}
	}
	updated, err := e.store.Admin().UpdateAccount(ctx, id, patch)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	e.invalidateAccountCache(updated.ID, updated.Ledger, updated.Code)

	if patch.Status != nil {
		now := e.clock.Now()
		tx, err := e.store.Begin(ctx)
		if err == nil {
			payload, _ := json.Marshal(updated)
			_ = tx.AppendOutbox(ctx, []events.Event{{
				ID:        e.idGen.NewID(now),
				Ledger:    updated.Ledger,
				Type:      events.TypeAccountStatusChanged,
				Payload:   payload,
				CreatedAt: now,
			}})
			_ = tx.Commit(ctx)
		}
	}
	return updated, nil
}

// ClosePeriod closes an accounting period starting at startsAt on ledger l.
func (e *Engine) ClosePeriod(ctx context.Context, l LedgerID, startsAt time.Time) (*Period, error) {
	p, err := e.store.Admin().ClosePeriod(ctx, l, startsAt, e.clock.Now())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return p, nil
}

// --- Posting Pipeline (RFC Section 4) ---

// Post executes the 10-step posting algorithm for an Intent.
func (e *Engine) Post(ctx context.Context, intent Intent, opts ...PostOption) (*Transaction, error) {
	var callOpts postCallOptions
	for _, opt := range opts {
		opt(&callOpts)
	}

	if intent.Ledger == "" {
		return nil, NewError(CodeValidationFailed, "ledger is required", nil)
	}
	e.trackLedger(intent.Ledger)
	if intent.Kind == "" {
		return nil, NewError(CodeValidationFailed, "kind is required", nil)
	}
	if intent.IdempotencyKey == "" {
		return nil, NewError(CodeValidationFailed, "idempotency_key is required", nil)
	}
	if err := intent.Metadata.Validate(); err != nil {
		return nil, err
	}
	if err := intent.Params.Validate(); err != nil {
		return nil, err
	}

	now := e.clock.Now()
	effectiveAt := now
	if intent.EffectiveAt != nil {
		effectiveAt = intent.EffectiveAt.UTC()
	}

	// Normalize caller postings (resolve account codes to AccountIDs if needed) so request hash is canonical
	resolvedInitialPostings, err := e.resolvePostingAccounts(ctx, intent.Ledger, intent.Postings)
	if err != nil {
		return nil, err
	}
	intent.Postings = resolvedInitialPostings
	reqHash := computeIntentHash(intent)

	// Step 2: Idempotency pre-check
	if existing, err := e.store.Idempotency(ctx, intent.Ledger, intent.IdempotencyKey); err == nil && existing != nil {
		if !bytes.Equal(existing.RequestHash, reqHash) {
			return nil, NewError(CodeIdempotencyConflict,
				"idempotency key already used with different request parameters",
				map[string]any{"idempotency_key": intent.IdempotencyKey})
		}
		tx, err := e.store.Transaction(ctx, existing.TxID)
		if err != nil {
			return nil, mapStorageErr(err)
		}
		tx.IdempotentReplay = true
		return tx, nil
	}

	// Check open period for EffectiveAt
	if err := e.checkOpenPeriod(ctx, intent.Ledger, effectiveAt); err != nil {
		return nil, err
	}

	hasMatchingEnricher := false
	for _, enr := range e.enrichers {
		if enr.Matches(intent.Kind) {
			hasMatchingEnricher = true
			break
		}
	}
	if len(intent.Postings) == 0 && !hasMatchingEnricher {
		return nil, NewError(CodeValidationFailed, "postings must not be empty when no enricher matches kind", nil)
	}

	// Validate caller-provided postings before running enrichers
	if len(intent.Postings) > 0 {
		if _, err := e.validateAndLoadAccounts(ctx, intent.Ledger, intent.Postings); err != nil {
			return nil, err
		}
	}

	// Step 5: Enrich
	enrichStart := time.Now()
	draft := module.NewDraft(intent)
	var extraEvents []events.Event
	if len(e.enrichers) > 0 {
		hookCtx, cancelTotal := context.WithTimeout(ctx, e.hookTotalTimeout)
		defer cancelTotal()

		for _, enr := range e.enrichers {
			if !enr.Matches(intent.Kind) {
				continue
			}
			draft.SetCurrentModule(enr.Name())
			modErr := e.runSyncHook(hookCtx, enr.Name(), func(mCtx context.Context) error {
				return enr.Enrich(mCtx, draft)
			})
			if modErr != nil {
				if e.optionalModules[enr.Name()] {
					reason := classifySkipReason(modErr)
					draft.SetMeta("skipped", reason)
					payload, _ := json.Marshal(map[string]any{
						"module": enr.Name(),
						"kind":   intent.Kind,
						"reason": reason,
					})
					extraEvents = append(extraEvents, events.Event{
						ID:        e.idGen.NewID(now),
						Ledger:    intent.Ledger,
						Type:      events.TypeModuleSkipped,
						Payload:   payload,
						CreatedAt: now,
					})
					continue
				}
				e.telemetry.postsFailed.Add(1)
				return nil, modErr
			}
		}
		draft.SetCurrentModule("")
	}
	e.telemetry.enrichDurationMicros.Add(uint64(time.Since(enrichStart).Microseconds()))

	// Resolve any account codes added by enrichers and re-run steps 3 & 4
	finalPostings, err := e.resolvePostingAccounts(ctx, intent.Ledger, draft.Postings())
	if err != nil {
		e.telemetry.postsFailed.Add(1)
		return nil, err
	}
	if len(finalPostings) < 1 || len(finalPostings) > core.MaxPostings {
		e.telemetry.postsFailed.Add(1)
		return nil, NewError(CodeValidationFailed,
			fmt.Sprintf("transaction must have between 1 and %d postings (got %d)", core.MaxPostings, len(finalPostings)),
			nil)
	}
	accountsMap, err := e.validateAndLoadAccounts(ctx, intent.Ledger, finalPostings)
	if err != nil {
		e.telemetry.postsFailed.Add(1)
		return nil, err
	}

	finalMeta := draft.Metadata()
	if err := finalMeta.Validate(); err != nil {
		e.telemetry.postsFailed.Add(1)
		return nil, err
	}

	// Step 1: Authorize (checked with full resolved account list)
	if callOpts.principal != nil {
		if err := e.authorize(ctx, *callOpts.principal, "tx:post", intent.Ledger, accountsMap); err != nil {
			e.telemetry.postsFailed.Add(1)
			return nil, err
		}
	}

	// Step 6: Validate (registered Validator modules)
	validateStart := time.Now()
	for _, val := range e.validators {
		draft.SetCurrentModule(val.Name())
		valErr := e.runSyncHook(ctx, val.Name(), func(mCtx context.Context) error {
			return val.Validate(mCtx, draft)
		})
		if valErr != nil {
			if e.optionalModules[val.Name()] {
				continue
			}
			e.telemetry.postsFailed.Add(1)
			return nil, valErr
		}
	}
	e.telemetry.validateDurationMicros.Add(uint64(time.Since(validateStart).Microseconds()))

	status := StatusPosted
	if intent.Pending {
		status = StatusPending
	}
	tx := &Transaction{
		ID:             TransactionID(e.idGen.NewID(now)),
		Ledger:         intent.Ledger,
		Kind:           intent.Kind,
		Status:         status,
		Postings:       finalPostings,
		EffectiveAt:    effectiveAt,
		CommittedAt:    now,
		ExpiresAt:      intent.ExpiresAt,
		IdempotencyKey: intent.IdempotencyKey,
		Metadata:       finalMeta,
	}

	// Steps 7–10: Critical section via Committer
	commitStart := time.Now()
	committed, err := e.committer.Commit(ctx, commit.CommitRequest{
		Op:          commit.OpPost,
		Tx:          tx,
		RequestHash: reqHash,
		Accounts:    accountsMap,
		ExtraEvents: extraEvents,
		ExternalTx:  callOpts.externalTx,
		EventID:     e.idGen.NewID(now),
	})
	e.telemetry.commitDurationMicros.Add(uint64(time.Since(commitStart).Microseconds()))
	if err != nil {
		e.telemetry.postsFailed.Add(1)
		return nil, mapStorageErr(err)
	}
	e.telemetry.postsCommitted.Add(1)
	return committed, nil
}

// SettleHold settles a pending hold transaction, either in full (when settleAmount is nil)
// or partially up to the held amount on the first posting, releasing any remainder.
func (e *Engine) SettleHold(
	ctx context.Context,
	holdID TransactionID,
	idempotencyKey string,
	settleAmount *money.Amount,
	opts ...PostOption,
) (*Transaction, error) {
	var callOpts postCallOptions
	for _, opt := range opts {
		opt(&callOpts)
	}
	if idempotencyKey == "" {
		return nil, NewError(CodeValidationFailed, "idempotency_key is required", nil)
	}
	holdTx, err := e.store.Transaction(ctx, holdID)
	if err != nil {
		return nil, mapStorageErr(err)
	}

	settledPostings := make([]Posting, len(holdTx.Postings))
	copy(settledPostings, holdTx.Postings)
	if settleAmount != nil {
		if !settleAmount.IsPositive() {
			return nil, NewError(CodeValidationFailed, "settle amount must be > 0", nil)
		}
		if len(settledPostings) != 1 {
			return nil, NewError(CodeValidationFailed, "partial settle amount requires a single-posting hold", nil)
		}
		if settleAmount.Cmp(settledPostings[0].Amount) > 0 {
			return nil, NewError(CodeValidationFailed,
				fmt.Sprintf("settle amount %s exceeds held amount %s", *settleAmount, settledPostings[0].Amount),
				nil)
		}
		settledPostings[0].Amount = *settleAmount
	}

	reqHash := computeOpHash("settle_hold", holdTx.Ledger, holdID, idempotencyKey, settledPostings)
	if existing, err := e.store.Idempotency(ctx, holdTx.Ledger, idempotencyKey); err == nil && existing != nil {
		if !bytes.Equal(existing.RequestHash, reqHash) {
			return nil, NewError(CodeIdempotencyConflict, "idempotency key conflict", nil)
		}
		tx, err := e.store.Transaction(ctx, existing.TxID)
		if err != nil {
			return nil, mapStorageErr(err)
		}
		tx.IdempotentReplay = true
		return tx, nil
	}

	accountsMap, err := e.validateAndLoadAccounts(ctx, holdTx.Ledger, holdTx.Postings)
	if err != nil {
		return nil, err
	}
	if callOpts.principal != nil {
		if err := e.authorize(ctx, *callOpts.principal, "tx:post", holdTx.Ledger, accountsMap); err != nil {
			return nil, err
		}
	}

	now := e.clock.Now()
	settledTx := &Transaction{
		ID:             TransactionID(e.idGen.NewID(now)),
		Ledger:         holdTx.Ledger,
		Kind:           holdTx.Kind,
		Status:         StatusPosted,
		Postings:       settledPostings,
		EffectiveAt:    now,
		CommittedAt:    now,
		PendingID:      &holdID,
		IdempotencyKey: idempotencyKey,
		Metadata:       holdTx.Metadata.Clone(),
	}

	committed, err := e.committer.Commit(ctx, commit.CommitRequest{
		Op:          commit.OpSettleHold,
		Tx:          settledTx,
		RequestHash: reqHash,
		Accounts:    accountsMap,
		TargetTx:    holdTx,
		ExternalTx:  callOpts.externalTx,
		EventID:     e.idGen.NewID(now),
	})
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return committed, nil
}

// VoidHold releases all funds reserved by a pending hold transaction.
func (e *Engine) VoidHold(
	ctx context.Context,
	holdID TransactionID,
	idempotencyKey string,
	opts ...PostOption,
) (*Transaction, error) {
	var callOpts postCallOptions
	for _, opt := range opts {
		opt(&callOpts)
	}
	if idempotencyKey == "" {
		return nil, NewError(CodeValidationFailed, "idempotency_key is required", nil)
	}
	holdTx, err := e.store.Transaction(ctx, holdID)
	if err != nil {
		return nil, mapStorageErr(err)
	}

	reqHash := computeOpHash("void_hold", holdTx.Ledger, holdID, idempotencyKey, nil)
	if existing, err := e.store.Idempotency(ctx, holdTx.Ledger, idempotencyKey); err == nil && existing != nil {
		if !bytes.Equal(existing.RequestHash, reqHash) {
			return nil, NewError(CodeIdempotencyConflict, "idempotency key conflict", nil)
		}
		tx, err := e.store.Transaction(ctx, existing.TxID)
		if err != nil {
			return nil, mapStorageErr(err)
		}
		tx.IdempotentReplay = true
		return tx, nil
	}

	accountsMap, err := e.loadAccountsMap(ctx, holdTx.Postings)
	if err != nil {
		return nil, err
	}
	if callOpts.principal != nil {
		if err := e.authorize(ctx, *callOpts.principal, "tx:post", holdTx.Ledger, accountsMap); err != nil {
			return nil, err
		}
	}

	now := e.clock.Now()
	voidMarker := cloneTx(holdTx)
	voidMarker.ID = holdTx.ID
	voidMarker.Status = StatusVoided
	voidMarker.PendingID = &holdID
	voidMarker.IdempotencyKey = idempotencyKey
	voidMarker.CommittedAt = now

	_, err = e.committer.Commit(ctx, commit.CommitRequest{
		Op:          commit.OpVoidHold,
		Tx:          voidMarker,
		RequestHash: reqHash,
		Accounts:    accountsMap,
		TargetTx:    holdTx,
		ExternalTx:  callOpts.externalTx,
		EventID:     e.idGen.NewID(now),
	})
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return e.store.Transaction(ctx, holdID)
}

// SweepExpiredHolds expires any pending holds whose ExpiresAt is at or before now.
func (e *Engine) SweepExpiredHolds(ctx context.Context) (int, error) {
	now := e.clock.Now()
	expired, err := e.store.PendingExpiredTransactions(ctx, now, 100)
	if err != nil {
		return 0, err
	}
	swept := 0
	for _, holdTx := range expired {
		accountsMap, err := e.loadAccountsMap(ctx, holdTx.Postings)
		if err != nil {
			continue
		}
		expMarker := cloneTx(holdTx)
		expMarker.Status = StatusExpired
		expMarker.CommittedAt = now
		expMarker.IdempotencyKey = fmt.Sprintf("sys:expire:%s", holdTx.ID)
		_, err = e.committer.Commit(ctx, commit.CommitRequest{
			Op:          commit.OpExpireHold,
			Tx:          expMarker,
			RequestHash: []byte(expMarker.IdempotencyKey),
			Accounts:    accountsMap,
			TargetTx:    holdTx,
			EventID:     e.idGen.NewID(now),
		})
		if err == nil {
			swept++
		}
	}
	return swept, nil
}

// Reverse creates a new reversal transaction with each posting's debit and credit swapped
// and Reverses set to origID. If partialPostings is empty, a full reversal of all postings
// (including module-added postings such as fees) is performed.
func (e *Engine) Reverse(
	ctx context.Context,
	origID TransactionID,
	idempotencyKey string,
	partialPostings []Posting,
	opts ...PostOption,
) (*Transaction, error) {
	var callOpts postCallOptions
	for _, opt := range opts {
		opt(&callOpts)
	}
	if idempotencyKey == "" {
		return nil, NewError(CodeValidationFailed, "idempotency_key is required", nil)
	}
	origTx, err := e.store.Transaction(ctx, origID)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	if origTx.Status != StatusPosted {
		return nil, NewError(CodeValidationFailed,
			fmt.Sprintf("only posted transactions can be reversed (status: %s)", origTx.Status),
			map[string]any{"tx_id": origID, "status": origTx.Status.String()})
	}

	var revPostings []Posting
	reversalAmts := make([]money.Amount, len(origTx.Postings))

	if len(partialPostings) == 0 {
		// Full reversal: swap Debit and Credit on every original posting
		revPostings = make([]Posting, len(origTx.Postings))
		for i, p := range origTx.Postings {
			revPostings[i] = Posting{
				Debit:  p.Credit,
				Credit: p.Debit,
				Amount: p.Amount,
				Asset:  p.Asset,
				Source: p.Source,
				Memo:   "reversal of " + string(origID),
			}
			reversalAmts[i] = p.Amount
		}
	} else {
		resolvedPartial, err := e.resolvePostingAccounts(ctx, origTx.Ledger, partialPostings)
		if err != nil {
			return nil, err
		}
		revPostings = make([]Posting, len(resolvedPartial))
		for i, rp := range resolvedPartial {
			// Match against an original posting (remembering reversal swaps Debit/Credit,
			// or caller may specify swapped legs directly)
			matched := false
			for origIdx, op := range origTx.Postings {
				isSwapped := rp.Debit == op.Credit && rp.Credit == op.Debit && rp.Asset == op.Asset
				isSameOrder := rp.Debit == op.Debit && rp.Credit == op.Credit && rp.Asset == op.Asset
				if isSwapped || isSameOrder {
					sum, err := reversalAmts[origIdx].Add(rp.Amount)
					if err != nil {
						return nil, NewError(CodeValidationFailed, "reversal amount overflow", nil)
					}
					reversalAmts[origIdx] = sum
					revPostings[i] = Posting{
						Debit:  op.Credit,
						Credit: op.Debit,
						Amount: rp.Amount,
						Asset:  op.Asset,
						Source: rp.Source,
						Memo:   rp.Memo,
					}
					matched = true
					break
				}
			}
			if !matched {
				return nil, NewError(CodeValidationFailed,
					fmt.Sprintf("partial reversal posting %d does not match any posting on transaction %s", i, origID),
					nil)
			}
		}
	}

	reqHash := computeOpHash("reverse", origTx.Ledger, origID, idempotencyKey, revPostings)
	if existing, err := e.store.Idempotency(ctx, origTx.Ledger, idempotencyKey); err == nil && existing != nil {
		if !bytes.Equal(existing.RequestHash, reqHash) {
			return nil, NewError(CodeIdempotencyConflict, "idempotency key conflict", nil)
		}
		tx, err := e.store.Transaction(ctx, existing.TxID)
		if err != nil {
			return nil, mapStorageErr(err)
		}
		tx.IdempotentReplay = true
		return tx, nil
	}

	accountsMap, err := e.validateAndLoadAccounts(ctx, origTx.Ledger, revPostings)
	if err != nil {
		return nil, err
	}
	if callOpts.principal != nil {
		if err := e.authorize(ctx, *callOpts.principal, "tx:reverse", origTx.Ledger, accountsMap); err != nil {
			return nil, err
		}
	}

	now := e.clock.Now()
	if err := e.checkOpenPeriod(ctx, origTx.Ledger, now); err != nil {
		return nil, err
	}

	revTx := &Transaction{
		ID:             TransactionID(e.idGen.NewID(now)),
		Ledger:         origTx.Ledger,
		Kind:           "reversal",
		Status:         StatusPosted,
		Postings:       revPostings,
		EffectiveAt:    now,
		CommittedAt:    now,
		Reverses:       &origID,
		IdempotencyKey: idempotencyKey,
		Metadata:       Metadata{"reverses": string(origID)},
	}

	committed, err := e.committer.Commit(ctx, commit.CommitRequest{
		Op:           commit.OpReverse,
		Tx:           revTx,
		RequestHash:  reqHash,
		Accounts:     accountsMap,
		TargetTx:     origTx,
		ReversalAmts: reversalAmts,
		ExternalTx:   callOpts.externalTx,
		EventID:      e.idGen.NewID(now),
	})
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return committed, nil
}

// SettleAccrual converts an accrued balance from a higher-precision asset (e.g. USD/6) to a
// coarser payable asset of the same currency (e.g. USD/2) using FX position accounts at a rate of 1,
// either routing the remainder to system:rounding:<asset> or carrying it forward in FromAccount (RFC Section 7).
func (e *Engine) SettleAccrual(ctx context.Context, req AccrualSettleRequest, opts ...PostOption) (*AccrualSettleResult, error) {
	if req.Ledger == "" || req.IdempotencyKey == "" || req.FromAccount == "" || req.ToAccount == "" {
		return nil, NewError(CodeValidationFailed, "ledger, idempotency_key, from_account, and to_account are required", nil)
	}
	fromID, err := e.resolveAccountID(ctx, req.Ledger, string(req.FromAccount))
	if err != nil {
		return nil, err
	}
	toID, err := e.resolveAccountID(ctx, req.Ledger, string(req.ToAccount))
	if err != nil {
		return nil, err
	}
	accts, err := e.store.Accounts(ctx, []AccountID{fromID, toID})
	if err != nil {
		return nil, mapStorageErr(err)
	}
	var fromAcct, toAcct Account
	for _, a := range accts {
		if a.ID == fromID {
			fromAcct = a
		}
		if a.ID == toID {
			toAcct = a
		}
	}
	fromAsset, err := money.ParseAsset(fromAcct.Asset)
	if err != nil {
		return nil, NewError(CodeValidationFailed, err.Error(), nil)
	}
	toAsset, err := money.ParseAsset(toAcct.Asset)
	if err != nil {
		return nil, NewError(CodeValidationFailed, err.Error(), nil)
	}
	if fromAsset.Code != toAsset.Code {
		return nil, NewError(CodeValidationFailed,
			fmt.Sprintf("SettleAccrual requires same currency code (%s vs %s)", fromAsset.Code, toAsset.Code), nil)
	}
	if fromAsset.Scale <= toAsset.Scale {
		return nil, NewError(CodeValidationFailed,
			fmt.Sprintf("accrual asset scale (%d) must be greater than payable asset scale (%d)", fromAsset.Scale, toAsset.Scale), nil)
	}

	var grossAmt money.Amount
	if req.Amount != nil {
		grossAmt = *req.Amount
	} else {
		bal, err := e.store.Balance(ctx, fromID)
		if err != nil {
			return nil, mapStorageErr(err)
		}
		grossAmt = bal.Posted(fromAcct.Normal)
	}
	if !grossAmt.IsPositive() {
		return nil, NewError(CodeValidationFailed, "accrual amount to settle must be positive", nil)
	}

	mode := e.roundingMode
	if req.RoundingMode != nil {
		mode = *req.RoundingMode
	}
	payableAmt, remAmt, err := money.Rescale(grossAmt, fromAsset.Scale, toAsset.Scale, mode)
	if err != nil {
		return nil, NewError(CodeValidationFailed, err.Error(), nil)
	}

	posFromCode := "system:fx:position:" + assetCodeSlug(fromAsset)
	posFromID, err := e.ensureSystemAccount(ctx, req.Ledger, posFromCode, fromAcct.Asset, Deferred, "core")
	if err != nil {
		return nil, err
	}
	posToCode := "system:fx:position:" + assetCodeSlug(toAsset)
	posToID, err := e.ensureSystemAccount(ctx, req.Ledger, posToCode, toAcct.Asset, Deferred, "core")
	if err != nil {
		return nil, err
	}

	var postings []Posting
	if req.CarryForward {
		settledHighAmt, err := grossAmt.Sub(remAmt)
		if err != nil {
			return nil, NewError(CodeValidationFailed, err.Error(), nil)
		}
		if settledHighAmt.IsPositive() {
			postings = append(postings, Posting{
				Debit:  fromID,
				Credit: posFromID,
				Amount: settledHighAmt,
				Asset:  fromAcct.Asset,
				Memo:   "accrual settlement (carry forward)",
			})
		}
		if payableAmt.IsPositive() {
			postings = append(postings, Posting{
				Debit:  posToID,
				Credit: toID,
				Amount: payableAmt,
				Asset:  toAcct.Asset,
				Memo:   "accrual payout",
			})
		}
	} else {
		postings = append(postings, Posting{
			Debit:  fromID,
			Credit: posFromID,
			Amount: grossAmt,
			Asset:  fromAcct.Asset,
			Memo:   "accrual settlement",
		})
		if payableAmt.IsPositive() {
			postings = append(postings, Posting{
				Debit:  posToID,
				Credit: toID,
				Amount: payableAmt,
				Asset:  toAcct.Asset,
				Memo:   "accrual payout",
			})
		}
		if !remAmt.IsZero() {
			roundingCode := "system:rounding:" + assetCodeSlug(fromAsset)
			roundingID, err := e.ensureSystemAccount(ctx, req.Ledger, roundingCode, fromAcct.Asset, Deferred, "core")
			if err != nil {
				return nil, err
			}
			if remAmt.IsPositive() {
				postings = append(postings, Posting{
					Debit:  posFromID,
					Credit: roundingID,
					Amount: remAmt,
					Asset:  fromAcct.Asset,
					Memo:   "accrual rounding remainder",
				})
			} else {
				absRem, _ := remAmt.Neg()
				postings = append(postings, Posting{
					Debit:  roundingID,
					Credit: posFromID,
					Amount: absRem,
					Asset:  fromAcct.Asset,
					Memo:   "accrual rounding remainder",
				})
			}
		}
	}
	if len(postings) == 0 {
		return nil, NewError(CodeValidationFailed, "accrual settlement produced no non-zero postings", nil)
	}

	meta := req.Metadata.Clone()
	if meta == nil {
		meta = make(Metadata)
	}
	meta["accrual.from_asset"] = string(fromAcct.Asset)
	meta["accrual.to_asset"] = string(toAcct.Asset)
	meta["accrual.payable_amount"] = payableAmt.String()
	meta["accrual.remainder"] = remAmt.String()
	meta["accrual.rounding_mode"] = mode.String()

	tx, err := e.Post(ctx, Intent{
		Ledger:         req.Ledger,
		Kind:           "accrual_payout",
		IdempotencyKey: req.IdempotencyKey,
		Postings:       postings,
		EffectiveAt:    req.EffectiveAt,
		Metadata:       meta,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &AccrualSettleResult{
		Transaction:   tx,
		PayableAmount: payableAmt,
		Remainder:     remAmt,
	}, nil
}

// HolderBalances returns per-asset totals across a holder's accounts, optionally converting
// to reportIn (e.g. "USD/2") using the registered RateProvider's mid rate (RFC section 8).
func (e *Engine) HolderBalances(ctx context.Context, h HolderID, reportIn money.AssetID) ([]HolderBalance, error) {
	hbs, err := e.store.HolderBalances(ctx, h)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	if reportIn == "" || len(hbs) == 0 {
		return hbs, nil
	}
	targetAsset, err := money.ParseAsset(reportIn)
	if err != nil {
		return nil, NewError(CodeValidationFailed, err.Error(), nil)
	}
	if len(e.rateProviders) == 0 {
		return nil, NewError(CodeModuleUnavailable, "no RateProvider module registered for report_in conversion", nil)
	}
	rp := e.rateProviders[0]
	now := e.clock.Now()

	for i := range hbs {
		hb := &hbs[i]
		hb.ReportedAsset = reportIn
		if hb.Asset == reportIn {
			p := hb.Posted
			a := hb.Available
			hb.ReportedPosted = &p
			hb.ReportedAvailable = &a
			hb.Rate = "1"
			continue
		}
		srcAsset, err := money.ParseAsset(hb.Asset)
		if err != nil {
			return nil, err
		}
		q, err := rp.Quote(ctx, hb.Asset, reportIn, now)
		if err != nil {
			return nil, NewError(CodeModuleUnavailable, fmt.Sprintf("rate quote failed for %s->%s: %v", hb.Asset, reportIn, err), nil)
		}
		convPosted, err := convertAmountWithRate(hb.Posted, srcAsset.Scale, targetAsset.Scale, q.Mid, e.roundingMode)
		if err != nil {
			return nil, err
		}
		convAvail, err := convertAmountWithRate(hb.Available, srcAsset.Scale, targetAsset.Scale, q.Mid, e.roundingMode)
		if err != nil {
			return nil, err
		}
		hb.ReportedPosted = &convPosted
		hb.ReportedAvailable = &convAvail
		hb.QuoteID = q.ID
		hb.Rate = q.Mid.String()
	}
	return hbs, nil
}

func convertAmountWithRate(amt money.Amount, fromScale, toScale uint8, rate money.Rate, mode money.RoundingMode) (money.Amount, error) {
	scaled, _, err := money.Rescale(amt, fromScale, toScale, mode)
	if err != nil {
		return money.Zero, err
	}
	res, _, err := money.MulRate(scaled, rate, mode)
	return res, err
}

// RelayOnce delivers pending outbox events to all registered Subscriber modules in commit order.
// Subscriber failures are isolated: one slow or failing subscriber logs/records its error
// without preventing other subscribers from advancing their cursors (RFC section 6).
func (e *Engine) RelayOnce(ctx context.Context) (int, error) {
	if len(e.subscribers) == 0 {
		return 0, nil
	}
	outbox := e.store.Outbox()
	deliveredCount := 0
	now := e.clock.Now()
	var relayErrs []error

	for _, sub := range e.subscribers {
		cursor, err := outbox.GetCursor(ctx, sub.Name())
		if err != nil {
			relayErrs = append(relayErrs, fmt.Errorf("subscriber %s get cursor: %w", sub.Name(), err))
			continue
		}
		evs, err := outbox.FetchAllAfter(ctx, cursor, 100)
		if err != nil {
			relayErrs = append(relayErrs, fmt.Errorf("subscriber %s fetch events: %w", sub.Name(), err))
			continue
		}
		if len(evs) == 0 {
			continue
		}
		var matching []events.Event
		var maxSeq uint64
		for _, ev := range evs {
			if ev.Seq > maxSeq {
				maxSeq = ev.Seq
			}
			if events.AnyTopicMatches(sub.Topics(), ev.Type) {
				matching = append(matching, ev)
			}
		}
		if len(matching) > 0 {
			start := time.Now()
			err := sub.Handle(ctx, matching)
			e.telemetry.recordModule(sub.Name(), time.Since(start))
			if err != nil {
				e.logger.WarnContext(ctx, "outbox subscriber delivery failed; cursor not advanced",
					"subscriber", sub.Name(),
					"error", err)
				relayErrs = append(relayErrs, fmt.Errorf("subscriber %s failed: %w", sub.Name(), err))
				continue
			}
			deliveredCount += len(matching)
		}
		if maxSeq > cursor {
			if err := outbox.SetCursor(ctx, sub.Name(), maxSeq, now); err != nil {
				relayErrs = append(relayErrs, fmt.Errorf("subscriber %s set cursor: %w", sub.Name(), err))
			}
		}
	}
	return deliveredCount, errors.Join(relayErrs...)
}

// SealOnce runs one pass of the hash-chain sealer for ledger l.
func (e *Engine) SealOnce(ctx context.Context, l LedgerID) (int, *Checkpoint, error) {
	return e.sealer.SealOnce(ctx, l)
}

// FoldDeferredOnce folds new entries for Deferred accounts in ledger l into their balance rows.
func (e *Engine) FoldDeferredOnce(ctx context.Context, l LedgerID) (int, error) {
	return e.store.FoldDeferredBalances(ctx, l)
}

// --- Internal helpers ---

func (e *Engine) resolvePostingAccounts(ctx context.Context, l LedgerID, postings []Posting) ([]Posting, error) {
	out := make([]Posting, len(postings))
	for i, p := range postings {
		out[i] = p
		if p.Debit == "" || p.Credit == "" {
			return nil, NewError(CodeValidationFailed, fmt.Sprintf("posting %d must specify debit and credit accounts", i), nil)
		}
		debitID, err := e.resolveAccountID(ctx, l, string(p.Debit))
		if err != nil {
			return nil, err
		}
		creditID, err := e.resolveAccountID(ctx, l, string(p.Credit))
		if err != nil {
			return nil, err
		}
		out[i].Debit = debitID
		out[i].Credit = creditID
	}
	return out, nil
}

func (e *Engine) resolveAccountID(ctx context.Context, l LedgerID, codeOrID string) (AccountID, error) {
	cacheKey := string(l) + ":" + codeOrID
	e.cacheMu.RLock()
	if id, ok := e.accountByCode[cacheKey]; ok {
		e.cacheMu.RUnlock()
		return id, nil
	}
	e.cacheMu.RUnlock()

	// Try by ID first
	if accts, err := e.store.Accounts(ctx, []AccountID{AccountID(codeOrID)}); err == nil && len(accts) == 1 {
		a := accts[0]
		if a.Ledger != l {
			return "", NewError(CodeValidationFailed, fmt.Sprintf("account %s belongs to ledger %s, not %s", a.ID, a.Ledger, l), nil)
		}
		e.cacheAccount(a)
		return a.ID, nil
	}

	// Try by Code in ledger l
	a, err := e.store.AccountByCode(ctx, l, codeOrID)
	if err != nil {
		return "", NewError(CodeValidationFailed, fmt.Sprintf("account %q not found in ledger %s", codeOrID, l), nil)
	}
	e.cacheAccount(*a)
	return a.ID, nil
}

func (e *Engine) cacheAccount(a Account) {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	e.accountByIDCache[a.ID] = a
	e.accountByCode[string(a.Ledger)+":"+a.Code] = a.ID
	e.accountByCode[string(a.Ledger)+":"+string(a.ID)] = a.ID
}

func (e *Engine) invalidateAccountCache(id AccountID, l LedgerID, code string) {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	delete(e.accountByIDCache, id)
	delete(e.accountByCode, string(l)+":"+code)
	delete(e.accountByCode, string(l)+":"+string(id))
}

func (e *Engine) loadAccountsMap(ctx context.Context, postings []Posting) (map[AccountID]Account, error) {
	idSet := make(map[AccountID]bool)
	for _, p := range postings {
		idSet[p.Debit] = true
		idSet[p.Credit] = true
	}
	ids := make([]AccountID, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}
	accts, err := e.store.Accounts(ctx, ids)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	out := make(map[AccountID]Account, len(accts))
	for _, a := range accts {
		out[a.ID] = a
	}
	return out, nil
}

func (e *Engine) validateAndLoadAccounts(ctx context.Context, l LedgerID, postings []Posting) (map[AccountID]Account, error) {
	if len(postings) > core.MaxPostings {
		return nil, NewError(CodeValidationFailed, fmt.Sprintf("too many postings (%d > %d)", len(postings), core.MaxPostings), nil)
	}
	accountsMap, err := e.loadAccountsMap(ctx, postings)
	if err != nil {
		return nil, err
	}

	checkedAssets := make(map[money.AssetID]bool)
	for i, p := range postings {
		if !p.Amount.IsPositive() {
			return nil, NewError(CodeValidationFailed, fmt.Sprintf("posting %d amount must be > 0", i), nil)
		}
		if p.Amount.Cmp(money.MaxPostingAmount) > 0 {
			return nil, NewError(CodeValidationFailed, fmt.Sprintf("posting %d amount exceeds maximum 2^126-1", i), nil)
		}
		if p.Debit == p.Credit {
			return nil, NewError(CodeValidationFailed, fmt.Sprintf("posting %d debit and credit accounts must differ (%s)", i, p.Debit), nil)
		}
		if !checkedAssets[p.Asset] {
			if _, err := e.store.Asset(ctx, l, p.Asset); err != nil {
				return nil, NewError(CodeValidationFailed, fmt.Sprintf("asset %q is not registered in ledger %s", p.Asset, l), nil)
			}
			checkedAssets[p.Asset] = true
		}

		debitAcct := accountsMap[p.Debit]
		creditAcct := accountsMap[p.Credit]
		if debitAcct.Ledger != l || creditAcct.Ledger != l {
			return nil, NewError(CodeValidationFailed, fmt.Sprintf("posting %d references account outside ledger %s", i, l), nil)
		}
		if debitAcct.Asset != p.Asset {
			return nil, NewError(CodeValidationFailed,
				fmt.Sprintf("posting %d debit account %s has asset %s, posting has %s", i, debitAcct.Code, debitAcct.Asset, p.Asset),
				nil)
		}
		if creditAcct.Asset != p.Asset {
			return nil, NewError(CodeValidationFailed,
				fmt.Sprintf("posting %d credit account %s has asset %s, posting has %s", i, creditAcct.Code, creditAcct.Asset, p.Asset),
				nil)
		}
		if debitAcct.Status == AccountClosed {
			return nil, NewError(CodeAccountClosed,
				fmt.Sprintf("account %s (%s) is closed", debitAcct.Code, debitAcct.ID),
				map[string]any{"account_id": debitAcct.ID, "account_code": debitAcct.Code})
		}
		if creditAcct.Status == AccountClosed {
			return nil, NewError(CodeAccountClosed,
				fmt.Sprintf("account %s (%s) is closed", creditAcct.Code, creditAcct.ID),
				map[string]any{"account_id": creditAcct.ID, "account_code": creditAcct.Code})
		}
		// RFC section 3 & 4: "Frozen (no outflows)", "A Frozen account may receive but not send."
		// In a posting from Debit -> Credit, the Debit leg is the sender (outflow on credit-normal wallets,
		// or sender in a transfer) and on a debit-normal account Credit is an outflow.
		if debitAcct.Status == AccountFrozen {
			return nil, NewError(CodeAccountFrozen,
				fmt.Sprintf("account %s (%s) is frozen and cannot send outflows", debitAcct.Code, debitAcct.ID),
				map[string]any{"account_id": debitAcct.ID, "account_code": debitAcct.Code})
		}
		if creditAcct.Status == AccountFrozen && creditAcct.Normal == Debit {
			return nil, NewError(CodeAccountFrozen,
				fmt.Sprintf("debit-normal account %s (%s) is frozen and cannot be credited", creditAcct.Code, creditAcct.ID),
				map[string]any{"account_id": creditAcct.ID, "account_code": creditAcct.Code})
		}
	}
	return accountsMap, nil
}

func (e *Engine) checkOpenPeriod(ctx context.Context, l LedgerID, effectiveAt time.Time) error {
	periods, err := e.store.Periods(ctx, l)
	if err != nil {
		return mapStorageErr(err)
	}
	for _, p := range periods {
		if p.ClosedAt != nil && !effectiveAt.Before(p.StartsAt) && effectiveAt.Before(p.EndsAt) {
			return NewError(CodePeriodClosed,
				fmt.Sprintf("effective_at %s falls in closed period [%s, %s)",
					effectiveAt.Format(time.RFC3339), p.StartsAt.Format(time.RFC3339), p.EndsAt.Format(time.RFC3339)),
				map[string]any{
					"effective_at": effectiveAt,
					"starts_at":    p.StartsAt,
					"ends_at":      p.EndsAt,
					"closed_at":    p.ClosedAt,
				})
		}
	}
	return nil
}

func (e *Engine) runSyncHook(parentCtx context.Context, modName string, fn func(context.Context) error) (retErr error) {
	ctx, cancel := context.WithTimeout(parentCtx, e.hookModuleTimeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			e.logger.Error("module panicked during sync hook",
				"module", modName,
				"panic", r,
				"stack", string(debug.Stack()))
			retErr = NewError(CodeModuleUnavailable,
				fmt.Sprintf("module %s panicked: %v", modName, r),
				map[string]any{"module": modName})
		}
	}()

	start := time.Now()
	err := fn(ctx)
	e.telemetry.recordModule(modName, time.Since(start))
	if err == nil {
		return nil
	}
	var rejErr *module.RejectError
	if errors.As(err, &rejErr) {
		return NewError(CodeModuleRejected, rejErr.Message, map[string]any{
			"module": modName,
			"reason": rejErr.Code,
		})
	}
	var cErr *Error
	if errors.As(err, &cErr) {
		return cErr
	}
	return NewError(CodeModuleUnavailable,
		fmt.Sprintf("module %s unavailable: %v", modName, err),
		map[string]any{"module": modName, "cause": err.Error()})
}

func classifySkipReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "error"
}

func (e *Engine) authorize(ctx context.Context, p module.Principal, action string, l LedgerID, accts map[AccountID]Account) error {
	var ids []AccountID
	var codes []string
	for id, a := range accts {
		ids = append(ids, id)
		codes = append(codes, a.Code)
	}
	res := module.Resource{
		Ledger:       l,
		AccountIDs:   ids,
		AccountCodes: codes,
	}
	if len(e.authorizers) > 0 {
		for _, auth := range e.authorizers {
			if err := auth.Allow(ctx, p, action, res); err != nil {
				return NewError(CodeForbidden, err.Error(), nil)
			}
		}
		return nil
	}
	// Default built-in scope & allowlist check
	return DefaultAllow(p, action, res)
}

// DefaultAllow checks Principal scopes, ledger bindings, and optional account-code allowlist globs.
func DefaultAllow(p module.Principal, action string, r module.Resource) error {
	if len(p.Ledgers) > 0 {
		allowedLedger := false
		for _, l := range p.Ledgers {
			if l == "*" || l == r.Ledger {
				allowedLedger = true
				break
			}
		}
		if !allowedLedger {
			return NewError(CodeForbidden, fmt.Sprintf("principal %q is not authorized for ledger %q", p.ID, r.Ledger), nil)
		}
	}
	scopeOK := false
	for _, s := range p.Scopes {
		if s == "admin" || s == "*" || s == action {
			scopeOK = true
			break
		}
	}
	if !scopeOK {
		return NewError(CodeForbidden, fmt.Sprintf("principal %q lacks required scope %q", p.ID, action), nil)
	}
	if len(p.AccountAllowlist) > 0 {
		for _, code := range r.AccountCodes {
			matched := false
			for _, pat := range p.AccountAllowlist {
				if ok, _ := path.Match(pat, code); ok || pat == "*" {
					matched = true
					break
				}
			}
			if !matched {
				return NewError(CodeForbidden, fmt.Sprintf("account %q is not in principal allowlist", code), nil)
			}
		}
	}
	return nil
}

// --- module.Host implementation ---

type moduleHost struct {
	engine  *Engine
	modName string
}

func (h *moduleHost) Reader() storage.Reader {
	return h.engine.store
}

func (h *moduleHost) ResolveAccount(ctx context.Context, l LedgerID, codeOrID string) (AccountID, error) {
	return h.engine.resolveAccountID(ctx, l, codeOrID)
}

func (h *moduleHost) EnsureSystemAccount(
	ctx context.Context,
	l LedgerID,
	code string,
	asset money.AssetID,
	mode BalanceMode,
) (AccountID, error) {
	if existing, err := h.engine.store.AccountByCode(ctx, l, code); err == nil && existing != nil {
		return existing.ID, nil
	}
	// Ensure ledger and asset exist if possible
	if _, err := h.engine.store.Ledger(ctx, l); errors.Is(err, storage.ErrNotFound) {
		_, _ = h.engine.CreateLedger(ctx, l, string(l), nil)
	}
	if _, err := h.engine.store.Asset(ctx, l, asset); errors.Is(err, storage.ErrNotFound) {
		if parsed, pErr := money.ParseAsset(asset); pErr == nil {
			_, _ = h.engine.RegisterAsset(ctx, l, parsed.Code, parsed.Scale)
		}
	}
	acct, err := h.engine.CreateAccount(ctx, Account{
		Ledger:      l,
		Code:        code,
		Asset:       asset,
		Normal:      Credit,
		BalanceMode: mode,
		Status:      AccountOpen,
		Metadata:    Metadata{"system": "true", "module": h.modName},
	})
	if err != nil {
		if existing, rErr := h.engine.store.AccountByCode(ctx, l, code); rErr == nil && existing != nil {
			return existing.ID, nil
		}
		return "", err
	}
	return acct.ID, nil
}

func (h *moduleHost) Migrate(ctx context.Context, migrations fs.FS) error {
	if migrations == nil {
		return nil
	}
	var files []string
	err := fs.WalkDir(migrations, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && path.Ext(p) == ".sql" {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, f := range files {
		data, err := fs.ReadFile(migrations, f)
		if err != nil {
			return err
		}
		if err := h.engine.store.Admin().ExecModuleMigration(ctx, h.modName, f, string(data)); err != nil {
			return err
		}
	}
	return nil
}

func (h *moduleHost) DecodeConfig(v any) error {
	raw, ok := h.engine.moduleConfigs[h.modName]
	if !ok || raw == nil {
		return nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func (h *moduleHost) Logger() *slog.Logger {
	return h.engine.logger.With("module", h.modName)
}

func (h *moduleHost) Clock() Clock {
	return h.engine.clock
}

// --- Hashing & error mapping helpers ---

func computeIntentHash(intent Intent) []byte {
	h := sha256.New()
	h.Write([]byte(intent.Ledger))
	h.Write([]byte{0})
	h.Write([]byte(intent.Kind))
	h.Write([]byte{0})
	if intent.Pending {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	for _, p := range intent.Postings {
		h.Write([]byte(p.Debit))
		h.Write([]byte{0})
		h.Write([]byte(p.Credit))
		h.Write([]byte{0})
		h.Write([]byte(p.Amount.String()))
		h.Write([]byte{0})
		h.Write([]byte(p.Asset))
		h.Write([]byte{0})
		h.Write([]byte(p.Memo))
		h.Write([]byte{0})
	}
	writeSortedMapHash(h, intent.Params)
	writeSortedMapHash(h, intent.Metadata)
	return h.Sum(nil)
}

func computeOpHash(op string, l LedgerID, targetID TransactionID, idemKey string, postings []Posting) []byte {
	h := sha256.New()
	h.Write([]byte(op))
	h.Write([]byte{0})
	h.Write([]byte(l))
	h.Write([]byte{0})
	h.Write([]byte(targetID))
	h.Write([]byte{0})
	h.Write([]byte(idemKey))
	for _, p := range postings {
		h.Write([]byte(p.Debit))
		h.Write([]byte{0})
		h.Write([]byte(p.Credit))
		h.Write([]byte{0})
		h.Write([]byte(p.Amount.String()))
		h.Write([]byte{0})
		h.Write([]byte(p.Asset))
	}
	return h.Sum(nil)
}

func writeSortedMapHash(h interface{ Write([]byte) (int, error) }, m Metadata) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte{'='})
		_, _ = h.Write([]byte(m[k]))
		_, _ = h.Write([]byte{0})
	}
}

func cloneTx(t *Transaction) *Transaction {
	cp := *t
	cp.Postings = make([]Posting, len(t.Postings))
	copy(cp.Postings, t.Postings)
	cp.Metadata = t.Metadata.Clone()
	return &cp
}

func mapStorageErr(err error) error {
	if err == nil {
		return nil
	}
	var cErr *Error
	if errors.As(err, &cErr) {
		return cErr
	}
	if errors.Is(err, storage.ErrNotFound) {
		return NewError(CodeNotFound, err.Error(), nil)
	}
	if errors.Is(err, storage.ErrAlreadyExists) {
		return NewError(CodeAlreadyExists, err.Error(), nil)
	}
	return err
}
