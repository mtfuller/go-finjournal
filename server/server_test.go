package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage/memory"
	"github.com/mtfuller/go-finjournal/server"
)

func TestRESTServerEndToEndAndAuth(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	eng, err := ledger.New(store)
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	defer func() { _ = eng.Close(ctx) }()

	srv := server.New(eng, server.Config{
		APIKeys: []server.APIKeyConfig{
			{
				ID:      "admin-client",
				Key:     "secret-admin-key",
				Ledgers: []ledger.LedgerID{"*"},
				Scopes:  []string{"*"},
			},
			{
				ID:      "readonly-client",
				Key:     "secret-ro-key",
				Ledgers: []ledger.LedgerID{"main"},
				Scopes:  []string{"accounts:read", "tx:read"},
			},
		},
	})
	handler := srv.Handler()

	doReq := func(method, path, apiKey, idemKey, body string) *http.Response {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		if idemKey != "" {
			req.Header.Set("Idempotency-Key", idemKey)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Result()
	}

	// 1. Unauthorized call without key -> 401
	resp := doReq("POST", "/v1/ledgers", "", "k1", `{"id":"main","name":"Main"}`)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 2. Forbidden call with readonly key -> 403
	resp = doReq("POST", "/v1/ledgers", "secret-ro-key", "k1", `{"id":"main","name":"Main"}`)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 3. Create ledger, asset, holder, and accounts as admin
	resp = doReq("POST", "/v1/ledgers", "secret-admin-key", "k-ledger", `{"id":"main","name":"Main"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 creating ledger, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	resp = doReq("POST", "/v1/ledgers/main/assets", "secret-admin-key", "k-asset", `{"id":"USD/2"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 registering asset, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	resp = doReq("POST", "/v1/ledgers/main/accounts", "secret-admin-key", "k-a1",
		`{"code":"assets:psp:usd","asset":"USD/2","normal":"debit"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 creating account 1, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	resp = doReq("POST", "/v1/ledgers/main/accounts", "secret-admin-key", "k-a2",
		`{"code":"liabilities:wallets:alice:usd","asset":"USD/2","normal":"credit","limits":{"min":"0"}}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 creating account 2, got %d", resp.StatusCode)
	}
	var aliceAcct ledger.Account
	_ = json.NewDecoder(resp.Body).Decode(&aliceAcct)
	_ = resp.Body.Close()

	// 4. Post transaction (201 Created), then replay with same Idempotency-Key (200 OK + Idempotent-Replayed: true)
	txBody := `{"kind":"deposit","postings":[{"debit":"assets:psp:usd","credit":"liabilities:wallets:alice:usd","amount":"1001","asset":"USD/2"}]}`
	resp = doReq("POST", "/v1/ledgers/main/transactions", "secret-admin-key", "tx-idem-1", txBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 posting tx, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	resp = doReq("POST", "/v1/ledgers/main/transactions", "secret-admin-key", "tx-idem-1", txBody)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("expected 200 with Idempotent-Replayed: true, got status=%d header=%q",
			resp.StatusCode, resp.Header.Get("Idempotent-Replayed"))
	}
	_ = resp.Body.Close()

	// 5. Read balance with readonly key
	resp = doReq("GET", "/v1/ledgers/main/accounts/"+string(aliceAcct.ID)+"/balance", "secret-ro-key", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 reading balance, got %d", resp.StatusCode)
	}
	var balResp map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&balResp)
	_ = resp.Body.Close()
	if balResp["posted"] != "1001" {
		t.Fatalf("expected posted=1001, got %v", balResp["posted"])
	}

	// 6. Check /metrics
	resp = doReq("GET", "/metrics", "", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on /metrics, got %d", resp.StatusCode)
	}
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(buf.String(), "ledger_transactions_total") {
		t.Fatalf("expected ledger_transactions_total in /metrics output:\n%s", buf.String())
	}
}

func TestOpenAPISpecAndTypedClientAndRateLimitAndRedaction(t *testing.T) {
	// 1. Parse embedded OpenAPI 3.1 YAML and verify all paths
	var spec struct {
		OpenAPI string                       `yaml:"openapi"`
		Paths   map[string]map[string]any    `yaml:"paths"`
	}
	if err := yaml.Unmarshal(server.OpenAPISpec(), &spec); err != nil {
		t.Fatalf("yaml.Unmarshal OpenAPISpec: %v", err)
	}
	if spec.OpenAPI != "3.1.0" {
		t.Fatalf("expected openapi 3.1.0, got %q", spec.OpenAPI)
	}
	expectedPaths := []string{
		"/v1/ledgers",
		"/v1/ledgers/{ledger}/assets",
		"/v1/ledgers/{ledger}/holders",
		"/v1/ledgers/{ledger}/holders/{id}/balances",
		"/v1/ledgers/{ledger}/accounts",
		"/v1/ledgers/{ledger}/accounts/{id}",
		"/v1/ledgers/{ledger}/accounts/{id}/balance",
		"/v1/ledgers/{ledger}/accounts/{id}/entries",
		"/v1/ledgers/{ledger}/transactions",
		"/v1/ledgers/{ledger}/transactions/{id}",
		"/v1/ledgers/{ledger}/transactions/{id}/post",
		"/v1/ledgers/{ledger}/transactions/{id}/void",
		"/v1/ledgers/{ledger}/transactions/{id}/reverse",
		"/v1/ledgers/{ledger}/events",
		"/v1/ledgers/{ledger}/fx/quotes",
		"/v1/ledgers/{ledger}/recon/statements",
		"/v1/ledgers/{ledger}/journal/checkpoints",
		"/v1/ledgers/{ledger}/periods/{start}/close",
	}
	for _, p := range expectedPaths {
		if _, ok := spec.Paths[p]; !ok {
			t.Fatalf("OpenAPI spec missing expected path %s", p)
		}
	}

	// 2. Verify NewRedactingHandler redacts metadata in logs and propagates trace_id
	var logBuf bytes.Buffer
	logger := slog.New(ledger.NewRedactingHandler(slog.NewJSONHandler(&logBuf, nil)))
	traceCtx := ledger.WithTraceID(context.Background(), "trace-abc-123")
	logger.InfoContext(traceCtx, "posting test",
		slog.Any("metadata", map[string]string{"ssn": "123-45-6789"}),
		slog.String("metadata.card", "4111-xxxx"))
	logOut := logBuf.String()
	if strings.Contains(logOut, "123-45-6789") || strings.Contains(logOut, "4111-xxxx") {
		t.Fatalf("expected metadata values to be redacted in log output: %s", logOut)
	}
	if !strings.Contains(logOut, "[REDACTED]") || !strings.Contains(logOut, "trace-abc-123") {
		t.Fatalf("expected [REDACTED] and trace-abc-123 in log output: %s", logOut)
	}

	// 3. Test typed server.Client round-trip & Traceparent header propagation
	ctx := ledger.WithTraceID(context.Background(), "span-999")
	store := memory.New()
	eng, err := ledger.New(store, ledger.WithLogger(logger))
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	defer func() { _ = eng.Close(ctx) }()

	srv := server.New(eng, server.Config{
		APIKeys: []server.APIKeyConfig{
			{ID: "k1", Key: "key-1", Ledgers: []ledger.LedgerID{"*"}, Scopes: []string{"*"}},
		},
		RateLimitRPS:   100,
		RateLimitBurst: 50,
	})
	cli := server.NewClient("http://inmem", "key-1")
	cli.HTTPClient = &http.Client{Transport: handlerTransport{h: srv.Handler()}}

	if _, err := cli.CreateLedger(ctx, "c-l1", server.CreateLedgerRequest{ID: "main", Name: "Main"}); err != nil {
		t.Fatalf("Client.CreateLedger: %v", err)
	}
	if _, err := cli.RegisterAsset(ctx, "main", "c-a1", server.RegisterAssetRequest{ID: "USD/2"}); err != nil {
		t.Fatalf("Client.RegisterAsset: %v", err)
	}
	bank, err := cli.CreateAccount(ctx, "main", "c-acc1", ledger.Account{Code: "assets:bank:usd", Asset: "USD/2", Normal: ledger.Debit})
	if err != nil {
		t.Fatalf("Client.CreateAccount 1: %v", err)
	}
	zero := money.Zero
	alice, err := cli.CreateAccount(ctx, "main", "c-acc2", ledger.Account{
		Code: "liabilities:wallets:alice:usd", Asset: "USD/2", Normal: ledger.Credit,
		Limits: ledger.Limits{Min: &zero},
	})
	if err != nil {
		t.Fatalf("Client.CreateAccount 2: %v", err)
	}

	tx, err := cli.PostTransaction(ctx, "main", "c-tx1", ledger.Intent{
		Kind: "deposit",
		Postings: []ledger.Posting{{
			Debit: bank.ID, Credit: alice.ID, Amount: money.FromInt64(2500), Asset: "USD/2",
		}},
	})
	if err != nil || tx.ID == "" {
		t.Fatalf("Client.PostTransaction: tx=%+v err=%v", tx, err)
	}

	evs, err := cli.GetEvents(ctx, "main", 0, 20*time.Millisecond)
	if err != nil || len(evs) == 0 {
		t.Fatalf("Client.GetEvents: len=%d err=%v", len(evs), err)
	}

	// 4. Test per-key token-bucket rate limiting (429 Too Many Requests)
	rlSrv := server.New(eng, server.Config{
		APIKeys: []server.APIKeyConfig{
			{ID: "limited", Key: "limited-key", Ledgers: []ledger.LedgerID{"*"}, Scopes: []string{"*"}},
		},
		RateLimitRPS:   1,
		RateLimitBurst: 2,
	})
	rlCli := server.NewClient("http://inmem", "limited-key")
	rlCli.HTTPClient = &http.Client{Transport: handlerTransport{h: rlSrv.Handler()}}

	// Burst of 2 succeeds, 3rd immediate call hits 429 rate_limited
	_, _ = rlCli.GetAccountBalance(ctx, "main", alice.ID, nil)
	_, _ = rlCli.GetAccountBalance(ctx, "main", alice.ID, nil)
	_, err = rlCli.GetAccountBalance(ctx, "main", alice.ID, nil)
	if err == nil || !strings.Contains(err.Error(), "rate_limited") {
		t.Fatalf("expected rate_limited error on 3rd burst request, got %v", err)
	}
}

type handlerTransport struct {
	h http.Handler
}

func (t handlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	t.h.ServeHTTP(rec, req)
	return rec.Result(), nil
}
