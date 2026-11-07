// Package server implements the REST + OpenAPI v1 HTTP handlers, API-key and
// mTLS authentication middleware, and Prometheus/health operational endpoints.
package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/module"
	"github.com/mtfuller/go-finjournal/pkg/storage"
)

//go:embed openapi.yaml
var openAPISpec []byte

// OpenAPISpec returns the embedded OpenAPI 3.1 YAML specification.
func OpenAPISpec() []byte {
	return openAPISpec
}

// MaxRequestBytes caps incoming HTTP request bodies at 1 MiB (RFC section 11).
const MaxRequestBytes = 1 << 20

// APIKeyConfig defines an API key principal in configuration.
type APIKeyConfig struct {
	ID               string            `json:"id" yaml:"id"`
	Key              string            `json:"key,omitempty" yaml:"key,omitempty"`           // raw key (for dev/tests)
	KeySHA256        string            `json:"key_sha256,omitempty" yaml:"key_sha256"`       // hex SHA-256 of key
	Ledgers          []ledger.LedgerID `json:"ledgers" yaml:"ledgers"`
	Scopes           []string          `json:"scopes" yaml:"scopes"`
	AccountAllowlist []string          `json:"account_allowlist,omitempty" yaml:"account_allowlist,omitempty"`
}

// Config configures the HTTP Server.
type Config struct {
	APIKeys        []APIKeyConfig `json:"api_keys" yaml:"api_keys"`
	RateLimitRPS   int            `json:"rate_limit_rps,omitempty" yaml:"rate_limit_rps,omitempty"`     // per-key requests/sec (0 = unlimited)
	RateLimitBurst int            `json:"rate_limit_burst,omitempty" yaml:"rate_limit_burst,omitempty"` // token bucket burst
}

// BuildTLSConfig constructs a TLS 1.3 configuration with optional mTLS client CA verification (RFC section 11).
func BuildTLSConfig(certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("server: load TLS keypair: %w", err)
	}
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
	}
	if clientCAFile != "" {
		caPEM, err := os.ReadFile(clientCAFile)
		if err != nil {
			return nil, fmt.Errorf("server: read client CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("server: failed to parse client CA PEM")
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return tlsCfg, nil
}

type tokenBucket struct {
	tokens     int64 // scaled by 1000
	lastRefill time.Time
}

type keyRecord struct {
	hash      [32]byte
	principal module.Principal
}

// Server wraps a ledger.Engine with REST v1 and operational HTTP handlers.
type Server struct {
	engine      *ledger.Engine
	mux         *http.ServeMux
	opsMux      *http.ServeMux
	keys        []keyRecord
	requireAuth bool

	rateLimitRPS   int
	rateLimitBurst int
	rateMu         sync.Mutex
	buckets        map[string]*tokenBucket

	// Operational metrics counters
	txCommittedTotal         atomic.Uint64
	txRejectedTotal          atomic.Uint64
	lockTimeoutsTotal        atomic.Uint64
	invariantViolationsTotal atomic.Uint64
	rateLimitedTotal         atomic.Uint64
}

// New constructs a Server for engine.
func New(engine *ledger.Engine, cfg Config) *Server {
	burst := cfg.RateLimitBurst
	if burst <= 0 && cfg.RateLimitRPS > 0 {
		burst = cfg.RateLimitRPS
	}
	s := &Server{
		engine:         engine,
		mux:            http.NewServeMux(),
		opsMux:         http.NewServeMux(),
		requireAuth:    len(cfg.APIKeys) > 0,
		rateLimitRPS:   cfg.RateLimitRPS,
		rateLimitBurst: burst,
		buckets:        make(map[string]*tokenBucket),
	}
	for _, k := range cfg.APIKeys {
		var h [32]byte
		if k.KeySHA256 != "" {
			decoded, err := hex.DecodeString(k.KeySHA256)
			if err == nil && len(decoded) == 32 {
				copy(h[:], decoded)
			}
		} else if k.Key != "" {
			h = sha256.Sum256([]byte(k.Key))
		}
		s.keys = append(s.keys, keyRecord{
			hash: h,
			principal: module.Principal{
				ID:               k.ID,
				Ledgers:          k.Ledgers,
				Scopes:           k.Scopes,
				AccountAllowlist: k.AccountAllowlist,
			},
		})
	}
	s.registerRoutes()
	return s
}

// Handler returns the main REST API http.Handler with trace propagation middleware.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := strings.TrimSpace(r.Header.Get("X-Trace-ID"))
		if traceID == "" {
			if tp := strings.TrimSpace(r.Header.Get("Traceparent")); tp != "" {
				parts := strings.Split(tp, "-")
				if len(parts) >= 2 {
					traceID = parts[1]
				} else {
					traceID = tp
				}
			}
		}
		if traceID != "" {
			w.Header().Set("X-Trace-ID", traceID)
			r = r.WithContext(ledger.WithTraceID(r.Context(), traceID))
		}
		s.mux.ServeHTTP(w, r)
	})
}

// OpsHandler returns the operational (/healthz, /readyz, /metrics) http.Handler.
func (s *Server) OpsHandler() http.Handler {
	return s.opsMux
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(openAPISpec)
	})

	s.mux.HandleFunc("POST /v1/ledgers", s.handleCreateLedger)
	s.mux.HandleFunc("POST /v1/ledgers/{l}/assets", s.handleRegisterAsset)
	s.mux.HandleFunc("POST /v1/ledgers/{l}/holders", s.handleCreateHolder)
	s.mux.HandleFunc("GET /v1/ledgers/{l}/holders/{id}/balances", s.handleGetHolderBalances)
	s.mux.HandleFunc("POST /v1/ledgers/{l}/accounts", s.handleCreateAccount)
	s.mux.HandleFunc("PATCH /v1/ledgers/{l}/accounts/{id}", s.handlePatchAccount)
	s.mux.HandleFunc("GET /v1/ledgers/{l}/accounts/{id}/balance", s.handleGetAccountBalance)
	s.mux.HandleFunc("GET /v1/ledgers/{l}/accounts/{id}/entries", s.handleGetAccountEntries)
	s.mux.HandleFunc("POST /v1/ledgers/{l}/transactions", s.handlePostTransaction)
	s.mux.HandleFunc("GET /v1/ledgers/{l}/transactions/{id}", s.handleGetTransaction)
	s.mux.HandleFunc("POST /v1/ledgers/{l}/transactions/{id}/post", s.handleSettleHold)
	s.mux.HandleFunc("POST /v1/ledgers/{l}/transactions/{id}/void", s.handleVoidHold)
	s.mux.HandleFunc("POST /v1/ledgers/{l}/transactions/{id}/reverse", s.handleReverseTransaction)
	s.mux.HandleFunc("GET /v1/ledgers/{l}/events", s.handleGetEvents)
	s.mux.HandleFunc("POST /v1/ledgers/{l}/fx/quotes", s.handleCreateFXQuote)
	s.mux.HandleFunc("POST /v1/fx/quotes", s.handleCreateFXQuote)
	s.mux.HandleFunc("POST /v1/ledgers/{l}/recon/statements", s.handleImportReconStatement)
	s.mux.HandleFunc("POST /v1/recon/statements", s.handleImportReconStatement)
	s.mux.HandleFunc("GET /v1/ledgers/{l}/journal/checkpoints", s.handleGetCheckpoints)
	s.mux.HandleFunc("POST /v1/ledgers/{l}/periods/{start}/close", s.handleClosePeriod)

	// Also mount operational endpoints on both opsMux and main mux for convenience
	for _, m := range []*http.ServeMux{s.opsMux, s.mux} {
		m.HandleFunc("GET /healthz", s.handleHealthz)
		m.HandleFunc("GET /readyz", s.handleReadyz)
		m.HandleFunc("GET /metrics", s.handleMetrics)
	}
}

// --- Auth & Request helpers ---

func (s *Server) checkRateLimit(principalID string) error {
	if s.rateLimitRPS <= 0 {
		return nil
	}
	s.rateMu.Lock()
	defer s.rateMu.Unlock()
	now := s.engine.Clock().Now()
	maxTokens := int64(s.rateLimitBurst) * 1000
	b, ok := s.buckets[principalID]
	if !ok {
		b = &tokenBucket{
			tokens:     maxTokens,
			lastRefill: now,
		}
		s.buckets[principalID] = b
	}
	elapsedMs := now.Sub(b.lastRefill).Milliseconds()
	if elapsedMs > 0 {
		refill := elapsedMs * int64(s.rateLimitRPS)
		b.tokens += refill
		if b.tokens > maxTokens {
			b.tokens = maxTokens
		}
		b.lastRefill = now
	}
	if b.tokens < 1000 {
		s.rateLimitedTotal.Add(1)
		return ledger.NewError("rate_limited", "API key rate limit exceeded", nil)
	}
	b.tokens -= 1000
	return nil
}

func (s *Server) authenticate(r *http.Request, requiredScope string, targetLedger ledger.LedgerID) (module.Principal, error) {
	if !s.requireAuth {
		p := module.Principal{
			ID:      "anonymous",
			Ledgers: []ledger.LedgerID{"*"},
			Scopes:  []string{"*"},
		}
		if err := s.checkRateLimit(p.ID); err != nil {
			return module.Principal{}, err
		}
		return p, nil
	}

	// Check mTLS client certificate SAN first if present
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		cert := r.TLS.PeerCertificates[0]
		san := cert.Subject.CommonName
		if len(cert.DNSNames) > 0 {
			san = cert.DNSNames[0]
		}
		for _, kr := range s.keys {
			if kr.principal.ID == san {
				if err := s.checkRateLimit(kr.principal.ID); err != nil {
					return module.Principal{}, err
				}
				if err := ledger.DefaultAllow(kr.principal, requiredScope, module.Resource{Ledger: targetLedger}); err != nil {
					return module.Principal{}, err
				}
				return kr.principal, nil
			}
		}
	}

	// Extract Bearer token or X-API-Key header
	rawKey := r.Header.Get("X-API-Key")
	if rawKey == "" {
		authHdr := r.Header.Get("Authorization")
		if strings.HasPrefix(authHdr, "Bearer ") {
			rawKey = strings.TrimPrefix(authHdr, "Bearer ")
		}
	}
	if rawKey == "" {
		return module.Principal{}, ledger.NewError(ledger.CodeUnauthorized, "missing API key or mTLS certificate", nil)
	}

	sum := sha256.Sum256([]byte(rawKey))
	for _, kr := range s.keys {
		if subtle.ConstantTimeCompare(sum[:], kr.hash[:]) == 1 {
			if err := s.checkRateLimit(kr.principal.ID); err != nil {
				return module.Principal{}, err
			}
			if err := ledger.DefaultAllow(kr.principal, requiredScope, module.Resource{Ledger: targetLedger}); err != nil {
				return module.Principal{}, err
			}
			return kr.principal, nil
		}
	}
	return module.Principal{}, ledger.NewError(ledger.CodeUnauthorized, "invalid API key", nil)
}

func requireIdempotencyKey(r *http.Request) (string, error) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		return "", ledger.NewError(ledger.CodeValidationFailed, "Idempotency-Key header is required", nil)
	}
	if len(key) > 128 {
		return "", ledger.NewError(ledger.CodeValidationFailed, "Idempotency-Key exceeds 128 characters", nil)
	}
	return key, nil
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, MaxRequestBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return ledger.NewError(ledger.CodeValidationFailed, fmt.Sprintf("invalid JSON body: %v", err), nil)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) writeError(w http.ResponseWriter, err error) {
	var lErr *ledger.Error
	if errors.As(err, &lErr) {
		if lErr.Code == ledger.CodeLockTimeout {
			s.lockTimeoutsTotal.Add(1)
		} else {
			s.txRejectedTotal.Add(1)
		}
		status := lErr.HTTPStatus()
		if lErr.Code == "rate_limited" {
			status = http.StatusTooManyRequests
		}
		writeJSON(w, status, lErr)
		return
	}
	s.txRejectedTotal.Add(1)
	writeJSON(w, http.StatusInternalServerError, ledger.NewError("internal_error", err.Error(), nil))
}

// --- Handlers ---

func (s *Server) handleCreateLedger(w http.ResponseWriter, r *http.Request) {
	if _, err := s.authenticate(r, "admin", "*"); err != nil {
		s.writeError(w, err)
		return
	}
	if _, err := requireIdempotencyKey(r); err != nil {
		s.writeError(w, err)
		return
	}
	var req struct {
		ID       ledger.LedgerID `json:"id"`
		Name     string          `json:"name"`
		Metadata ledger.Metadata `json:"metadata"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		s.writeError(w, err)
		return
	}
	l, err := s.engine.CreateLedger(r.Context(), req.ID, req.Name, req.Metadata)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, l)
}

func (s *Server) handleRegisterAsset(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	if _, err := s.authenticate(r, "admin", lID); err != nil {
		s.writeError(w, err)
		return
	}
	if _, err := requireIdempotencyKey(r); err != nil {
		s.writeError(w, err)
		return
	}
	var req struct {
		ID    money.AssetID `json:"id"`
		Code  string        `json:"code"`
		Scale uint8         `json:"scale"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		s.writeError(w, err)
		return
	}
	if req.ID != "" && req.Code == "" {
		parsed, err := money.ParseAsset(req.ID)
		if err != nil {
			s.writeError(w, ledger.NewError(ledger.CodeValidationFailed, err.Error(), nil))
			return
		}
		req.Code = parsed.Code
		req.Scale = parsed.Scale
	}
	a, err := s.engine.RegisterAsset(r.Context(), lID, req.Code, req.Scale)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"id":    a.ID(),
		"code":  a.Code,
		"scale": a.Scale,
	})
}

func (s *Server) handleCreateHolder(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	if _, err := s.authenticate(r, "accounts:write", lID); err != nil {
		s.writeError(w, err)
		return
	}
	if _, err := requireIdempotencyKey(r); err != nil {
		s.writeError(w, err)
		return
	}
	var h ledger.AccountHolder
	if err := decodeJSON(w, r, &h); err != nil {
		s.writeError(w, err)
		return
	}
	h.Ledger = lID
	created, err := s.engine.CreateHolder(r.Context(), h)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) handleGetHolderBalances(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	if _, err := s.authenticate(r, "accounts:read", lID); err != nil {
		s.writeError(w, err)
		return
	}
	hID := ledger.HolderID(r.PathValue("id"))
	reportIn := money.AssetID(r.URL.Query().Get("report_in"))
	hbs, err := s.engine.HolderBalances(r.Context(), hID, reportIn)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"holder":   hID,
		"balances": hbs,
	})
}

func (s *Server) handleCreateAccount(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	if _, err := s.authenticate(r, "accounts:write", lID); err != nil {
		s.writeError(w, err)
		return
	}
	if _, err := requireIdempotencyKey(r); err != nil {
		s.writeError(w, err)
		return
	}
	var a ledger.Account
	if err := decodeJSON(w, r, &a); err != nil {
		s.writeError(w, err)
		return
	}
	a.Ledger = lID
	created, err := s.engine.CreateAccount(r.Context(), a)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) handlePatchAccount(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	if _, err := s.authenticate(r, "accounts:write", lID); err != nil {
		s.writeError(w, err)
		return
	}
	if _, err := requireIdempotencyKey(r); err != nil {
		s.writeError(w, err)
		return
	}
	acctID := ledger.AccountID(r.PathValue("id"))
	var req struct {
		Status   *ledger.AccountStatus `json:"status"`
		Limits   *ledger.Limits        `json:"limits"`
		Metadata ledger.Metadata       `json:"metadata"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		s.writeError(w, err)
		return
	}
	updated, err := s.engine.UpdateAccount(r.Context(), acctID, storage.AccountPatch{
		Status:   req.Status,
		Limits:   req.Limits,
		Metadata: req.Metadata,
	})
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) handleGetAccountBalance(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	if _, err := s.authenticate(r, "accounts:read", lID); err != nil {
		s.writeError(w, err)
		return
	}
	acctID := ledger.AccountID(r.PathValue("id"))
	accts, err := s.engine.Store().Accounts(r.Context(), []ledger.AccountID{acctID})
	if err != nil {
		if acctByCode, cErr := s.engine.Store().AccountByCode(r.Context(), lID, string(acctID)); cErr == nil {
			accts = []ledger.Account{*acctByCode}
			acctID = acctByCode.ID
		} else {
			s.writeError(w, ledger.NewError(ledger.CodeNotFound, err.Error(), nil))
			return
		}
	}
	acct := accts[0]

	if atStr := r.URL.Query().Get("at"); atStr != "" {
		atTime, err := time.Parse(time.RFC3339, atStr)
		if err != nil {
			s.writeError(w, ledger.NewError(ledger.CodeValidationFailed, "invalid 'at' RFC3339 timestamp", nil))
			return
		}
		amt, err := s.engine.Store().BalanceAt(r.Context(), acctID, atTime)
		if err != nil {
			s.writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"account": acctID,
			"asset":   acct.Asset,
			"at":      atTime.UTC(),
			"posted":  amt,
		})
		return
	}

	bal, err := s.engine.Store().Balance(r.Context(), acctID)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"account":         bal.Account,
		"asset":           bal.Asset,
		"normal":          acct.Normal,
		"posted":          bal.Posted(acct.Normal),
		"available":       bal.Available(acct.Normal),
		"posted_debits":   bal.PostedDebits,
		"posted_credits":  bal.PostedCredits,
		"pending_debits":  bal.PendingDebits,
		"pending_credits": bal.PendingCredits,
		"version":         bal.Version,
	})
}

func (s *Server) handleGetAccountEntries(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	if _, err := s.authenticate(r, "accounts:read", lID); err != nil {
		s.writeError(w, err)
		return
	}
	acctID := ledger.AccountID(r.PathValue("id"))
	limit := 50
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if v, err := strconv.Atoi(lStr); err == nil && v > 0 && v <= 500 {
			limit = v
		}
	}
	page, err := s.engine.Store().Entries(r.Context(), storage.EntryQuery{
		Account: acctID,
		Cursor:  r.URL.Query().Get("cursor"),
		Limit:   limit,
	})
	if err != nil {
		s.writeError(w, ledger.NewError(ledger.CodeNotFound, err.Error(), nil))
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handlePostTransaction(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	principal, err := s.authenticate(r, "tx:post", lID)
	if err != nil {
		s.writeError(w, err)
		return
	}
	idemKey, err := requireIdempotencyKey(r)
	if err != nil {
		s.writeError(w, err)
		return
	}

	var intent ledger.Intent
	if err := decodeJSON(w, r, &intent); err != nil {
		s.writeError(w, err)
		return
	}
	intent.Ledger = lID
	intent.IdempotencyKey = idemKey

	tx, err := s.engine.Post(r.Context(), intent, ledger.WithPrincipal(principal))
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.txCommittedTotal.Add(1)
	if tx.IdempotentReplay {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, tx)
		return
	}
	writeJSON(w, http.StatusCreated, tx)
}

func (s *Server) handleGetTransaction(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	if _, err := s.authenticate(r, "tx:read", lID); err != nil {
		s.writeError(w, err)
		return
	}
	txID := ledger.TransactionID(r.PathValue("id"))
	tx, err := s.engine.Store().Transaction(r.Context(), txID)
	if err != nil {
		s.writeError(w, ledger.NewError(ledger.CodeNotFound, err.Error(), nil))
		return
	}
	writeJSON(w, http.StatusOK, tx)
}

func (s *Server) handleSettleHold(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	principal, err := s.authenticate(r, "tx:post", lID)
	if err != nil {
		s.writeError(w, err)
		return
	}
	idemKey, err := requireIdempotencyKey(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	holdID := ledger.TransactionID(r.PathValue("id"))
	var req struct {
		Amount *money.Amount `json:"amount"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		s.writeError(w, err)
		return
	}
	tx, err := s.engine.SettleHold(r.Context(), holdID, idemKey, req.Amount, ledger.WithPrincipal(principal))
	if err != nil {
		s.writeError(w, err)
		return
	}
	if tx.IdempotentReplay {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, http.StatusOK, tx)
}

func (s *Server) handleVoidHold(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	principal, err := s.authenticate(r, "tx:post", lID)
	if err != nil {
		s.writeError(w, err)
		return
	}
	idemKey, err := requireIdempotencyKey(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	holdID := ledger.TransactionID(r.PathValue("id"))
	tx, err := s.engine.VoidHold(r.Context(), holdID, idemKey, ledger.WithPrincipal(principal))
	if err != nil {
		s.writeError(w, err)
		return
	}
	if tx.IdempotentReplay {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, http.StatusOK, tx)
}

func (s *Server) handleReverseTransaction(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	principal, err := s.authenticate(r, "tx:reverse", lID)
	if err != nil {
		s.writeError(w, err)
		return
	}
	idemKey, err := requireIdempotencyKey(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	origID := ledger.TransactionID(r.PathValue("id"))
	var req struct {
		Postings []ledger.Posting `json:"postings"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		s.writeError(w, err)
		return
	}
	tx, err := s.engine.Reverse(r.Context(), origID, idemKey, req.Postings, ledger.WithPrincipal(principal))
	if err != nil {
		s.writeError(w, err)
		return
	}
	if tx.IdempotentReplay {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, tx)
		return
	}
	writeJSON(w, http.StatusCreated, tx)
}

func (s *Server) handleGetEvents(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	if _, err := s.authenticate(r, "events:read", lID); err != nil {
		s.writeError(w, err)
		return
	}
	var after uint64
	if aStr := r.URL.Query().Get("after"); aStr != "" {
		after, _ = strconv.ParseUint(aStr, 10, 64)
	}
	var waitDur time.Duration
	if wStr := r.URL.Query().Get("wait"); wStr != "" {
		if parsed, err := time.ParseDuration(wStr); err == nil && parsed > 0 {
			if parsed > 30*time.Second {
				parsed = 30 * time.Second
			}
			waitDur = parsed
		}
	}

	isSSE := strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	if isSSE {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)

		deadline := time.Now().Add(waitDur)
		for {
			evs, err := s.engine.Store().Outbox().FetchAfter(r.Context(), lID, after, 100)
			if err != nil {
				return
			}
			for _, ev := range evs {
				b, _ := json.Marshal(ev)
				_, _ = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.Seq, ev.Type, string(b))
				if ev.Seq > after {
					after = ev.Seq
				}
			}
			if flusher != nil {
				flusher.Flush()
			}
			if waitDur == 0 || time.Now().After(deadline) {
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(15 * time.Millisecond):
			}
		}
	}

	deadline := time.Now().Add(waitDur)
	for {
		evs, err := s.engine.Store().Outbox().FetchAfter(r.Context(), lID, after, 100)
		if err != nil {
			s.writeError(w, err)
			return
		}
		if len(evs) > 0 || waitDur == 0 || time.Now().After(deadline) {
			writeJSON(w, http.StatusOK, map[string]any{"items": evs})
			return
		}
		select {
		case <-r.Context().Done():
			writeJSON(w, http.StatusOK, map[string]any{"items": []events.Event{}})
			return
		case <-time.After(15 * time.Millisecond):
		}
	}
}

func (s *Server) handleCreateFXQuote(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	if lID == "" {
		lID = "*"
	}
	if _, err := s.authenticate(r, "tx:post", lID); err != nil {
		s.writeError(w, err)
		return
	}
	if _, err := requireIdempotencyKey(r); err != nil {
		s.writeError(w, err)
		return
	}
	var req struct {
		Base       money.AssetID `json:"base"`
		Quote      money.AssetID `json:"quote"`
		TTLSeconds int           `json:"ttl_seconds"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		s.writeError(w, err)
		return
	}
	ttl := 30 * time.Second
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
	}
	q, err := s.engine.QuoteFX(r.Context(), req.Base, req.Quote, ttl)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"id":            q.ID,
		"base":          q.Base,
		"quote":         q.Quote,
		"mid":           q.Mid.String(),
		"customer_rate": q.Customer.String(),
		"source":        q.Source,
		"observed_at":   q.ObservedAt,
		"expires_at":    q.ExpiresAt,
	})
}

func (s *Server) handleImportReconStatement(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	if lID == "" {
		lID = "*"
	}
	if _, err := s.authenticate(r, "admin", lID); err != nil {
		s.writeError(w, err)
		return
	}
	if _, err := requireIdempotencyKey(r); err != nil {
		s.writeError(w, err)
		return
	}
	var req struct {
		Ledger ledger.LedgerID `json:"ledger"`
		Source string          `json:"source"`
		Asset  money.AssetID   `json:"asset"`
		CSV    string          `json:"csv"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		s.writeError(w, err)
		return
	}
	if lID == "*" && req.Ledger != "" {
		lID = req.Ledger
	}
	type statementImporter interface {
		ImportStatementCSV(ctx context.Context, source string, asset money.AssetID, r io.Reader) ([]byte, int, error)
	}
	var importer statementImporter
	for _, m := range s.engine.Modules() {
		if imp, ok := m.(statementImporter); ok {
			importer = imp
			break
		}
	}
	if importer == nil {
		s.writeError(w, ledger.NewError(ledger.CodeModuleUnavailable, "recon module is not registered", nil))
		return
	}
	resJSON, unmatchedCount, err := importer.ImportStatementCSV(r.Context(), req.Source, req.Asset, strings.NewReader(req.CSV))
	if err != nil {
		s.writeError(w, ledger.NewError(ledger.CodeValidationFailed, err.Error(), nil))
		return
	}
	if unmatchedCount > 0 && lID != "" && lID != "*" {
		if tx, txErr := s.engine.Store().Begin(r.Context()); txErr == nil {
			now := s.engine.Clock().Now()
			payload, _ := json.Marshal(map[string]any{
				"source":          req.Source,
				"asset":           req.Asset,
				"unmatched_count": unmatchedCount,
			})
			_ = tx.AppendOutbox(r.Context(), []events.Event{{
				ID:        fmt.Sprintf("recon_exc_%d", now.UnixNano()),
				Ledger:    lID,
				Type:      events.TypeReconException,
				Payload:   payload,
				CreatedAt: now,
			}})
			_ = tx.Commit(r.Context())
		}
	}
	var parsedResults any
	_ = json.Unmarshal(resJSON, &parsedResults)
	writeJSON(w, http.StatusOK, map[string]any{
		"results":         parsedResults,
		"unmatched_count": unmatchedCount,
	})
}

func (s *Server) handleGetCheckpoints(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	if _, err := s.authenticate(r, "audit:read", lID); err != nil {
		s.writeError(w, err)
		return
	}
	cps, err := s.engine.Store().Journal().Checkpoints(r.Context(), lID)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": cps})
}

func (s *Server) handleClosePeriod(w http.ResponseWriter, r *http.Request) {
	lID := ledger.LedgerID(r.PathValue("l"))
	if _, err := s.authenticate(r, "admin", lID); err != nil {
		s.writeError(w, err)
		return
	}
	if _, err := requireIdempotencyKey(r); err != nil {
		s.writeError(w, err)
		return
	}
	startStr := r.PathValue("start")
	startsAt, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		startsAt, err = time.Parse("2006-01-02", startStr)
		if err != nil {
			s.writeError(w, ledger.NewError(ledger.CodeValidationFailed, "invalid period start timestamp", nil))
			return
		}
	}
	p, err := s.engine.ClosePeriod(r.Context(), lID, startsAt)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	snap := s.engine.Metrics()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintf(w, "# TYPE ledger_transactions_total counter\n")
	_, _ = fmt.Fprintf(w, "ledger_transactions_total{result=\"committed\"} %d\n", s.txCommittedTotal.Load()+snap.PostsCommittedTotal)
	_, _ = fmt.Fprintf(w, "ledger_transactions_total{result=\"rejected\"} %d\n", s.txRejectedTotal.Load()+snap.PostsFailedTotal)
	_, _ = fmt.Fprintf(w, "# TYPE ledger_lock_timeouts_total counter\n")
	_, _ = fmt.Fprintf(w, "ledger_lock_timeouts_total %d\n", s.lockTimeoutsTotal.Load())
	_, _ = fmt.Fprintf(w, "# TYPE ledger_invariant_violations_total counter\n")
	_, _ = fmt.Fprintf(w, "ledger_invariant_violations_total %d\n", s.invariantViolationsTotal.Load())
	_, _ = fmt.Fprintf(w, "# TYPE ledger_rate_limited_total counter\n")
	_, _ = fmt.Fprintf(w, "ledger_rate_limited_total %d\n", s.rateLimitedTotal.Load())
	_, _ = fmt.Fprintf(w, "# TYPE ledger_stage_duration_microseconds_total counter\n")
	_, _ = fmt.Fprintf(w, "ledger_stage_duration_microseconds_total{stage=\"enrich\"} %d\n", snap.EnrichDurationMicros)
	_, _ = fmt.Fprintf(w, "ledger_stage_duration_microseconds_total{stage=\"validate\"} %d\n", snap.ValidateDurationMicros)
	_, _ = fmt.Fprintf(w, "ledger_stage_duration_microseconds_total{stage=\"commit\"} %d\n", snap.CommitDurationMicros)
	_, _ = fmt.Fprintf(w, "# TYPE ledger_module_calls_total counter\n")
	for mod, count := range snap.ModuleCallsTotal {
		_, _ = fmt.Fprintf(w, "ledger_module_calls_total{module=%q} %d\n", mod, count)
	}
	_, _ = fmt.Fprintf(w, "# TYPE ledger_module_duration_microseconds_total counter\n")
	for mod, dur := range snap.ModuleDurationMicros {
		_, _ = fmt.Fprintf(w, "ledger_module_duration_microseconds_total{module=%q} %d\n", mod, dur)
	}
}
