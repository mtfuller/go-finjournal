package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mtfuller/go-finjournal/pkg/ledger"
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
