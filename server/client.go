package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage"
)

// Client is a typed REST v1 client matching server/openapi.yaml (RFC section 7).
type Client struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

// NewClient constructs a typed REST v1 Client for baseURL.
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		APIKey:     apiKey,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// CreateLedgerRequest is the request payload for POST /v1/ledgers.
type CreateLedgerRequest struct {
	ID       ledger.LedgerID `json:"id"`
	Name     string          `json:"name"`
	Metadata ledger.Metadata `json:"metadata,omitempty"`
}

// RegisterAssetRequest is the request payload for POST /v1/ledgers/{ledger}/assets.
type RegisterAssetRequest struct {
	ID    money.AssetID `json:"id,omitempty"`
	Code  string        `json:"code,omitempty"`
	Scale uint8         `json:"scale"`
}

// AssetResponse is the response payload for POST /v1/ledgers/{ledger}/assets.
type AssetResponse struct {
	ID    money.AssetID `json:"id"`
	Code  string        `json:"code"`
	Scale uint8         `json:"scale"`
}

// HolderBalancesResponse is the response payload for GET /v1/ledgers/{ledger}/holders/{id}/balances.
type HolderBalancesResponse struct {
	Holder   ledger.HolderID        `json:"holder"`
	Balances []ledger.HolderBalance `json:"balances"`
}

// PatchAccountRequest is the request payload for PATCH /v1/ledgers/{ledger}/accounts/{id}.
type PatchAccountRequest struct {
	Status   *ledger.AccountStatus `json:"status,omitempty"`
	Limits   *ledger.Limits        `json:"limits,omitempty"`
	Metadata ledger.Metadata       `json:"metadata,omitempty"`
}

// AccountBalanceResponse is the response payload for GET /v1/ledgers/{ledger}/accounts/{id}/balance.
type AccountBalanceResponse struct {
	Account        ledger.AccountID `json:"account"`
	Asset          money.AssetID    `json:"asset"`
	Normal         ledger.Side      `json:"normal,omitempty"`
	Posted         money.Amount     `json:"posted"`
	Available      money.Amount     `json:"available,omitempty"`
	PostedDebits   money.Amount     `json:"posted_debits,omitempty"`
	PostedCredits  money.Amount     `json:"posted_credits,omitempty"`
	PendingDebits  money.Amount     `json:"pending_debits,omitempty"`
	PendingCredits money.Amount     `json:"pending_credits,omitempty"`
	Version        uint64           `json:"version,omitempty"`
	At             *time.Time       `json:"at,omitempty"`
}

// SettleHoldRequest is the request payload for POST /v1/ledgers/{ledger}/transactions/{id}/post.
type SettleHoldRequest struct {
	Amount *money.Amount `json:"amount,omitempty"`
}

// ReverseTransactionRequest is the request payload for POST /v1/ledgers/{ledger}/transactions/{id}/reverse.
type ReverseTransactionRequest struct {
	Postings []ledger.Posting `json:"postings,omitempty"`
}

// CreateFXQuoteRequest is the request payload for POST /v1/ledgers/{ledger}/fx/quotes.
type CreateFXQuoteRequest struct {
	Base       money.AssetID `json:"base"`
	Quote      money.AssetID `json:"quote"`
	TTLSeconds int           `json:"ttl_seconds,omitempty"`
}

// FXQuoteResponse is the response payload for POST /v1/ledgers/{ledger}/fx/quotes.
type FXQuoteResponse struct {
	ID           string        `json:"id"`
	Base         money.AssetID `json:"base"`
	Quote        money.AssetID `json:"quote"`
	Mid          string        `json:"mid"`
	CustomerRate string        `json:"customer_rate"`
	Source       string        `json:"source"`
	ObservedAt   time.Time     `json:"observed_at"`
	ExpiresAt    time.Time     `json:"expires_at"`
}

// ImportReconStatementRequest is the request payload for POST /v1/ledgers/{ledger}/recon/statements.
type ImportReconStatementRequest struct {
	Source string        `json:"source"`
	Asset  money.AssetID `json:"asset"`
	CSV    string        `json:"csv"`
}

// ImportReconStatementResponse is the response payload for POST /v1/ledgers/{ledger}/recon/statements.
type ImportReconStatementResponse struct {
	Results        []map[string]any `json:"results"`
	UnmatchedCount int              `json:"unmatched_count"`
}

func (c *Client) CreateLedger(ctx context.Context, idemKey string, req CreateLedgerRequest) (*ledger.Ledger, error) {
	var out ledger.Ledger
	err := c.doJSON(ctx, http.MethodPost, "/v1/ledgers", idemKey, req, &out)
	return &out, err
}

func (c *Client) RegisterAsset(ctx context.Context, l ledger.LedgerID, idemKey string, req RegisterAssetRequest) (*AssetResponse, error) {
	var out AssetResponse
	err := c.doJSON(ctx, http.MethodPost, fmt.Sprintf("/v1/ledgers/%s/assets", url.PathEscape(string(l))), idemKey, req, &out)
	return &out, err
}

func (c *Client) CreateHolder(ctx context.Context, l ledger.LedgerID, idemKey string, req ledger.AccountHolder) (*ledger.AccountHolder, error) {
	var out ledger.AccountHolder
	err := c.doJSON(ctx, http.MethodPost, fmt.Sprintf("/v1/ledgers/%s/holders", url.PathEscape(string(l))), idemKey, req, &out)
	return &out, err
}

func (c *Client) GetHolderBalances(ctx context.Context, l ledger.LedgerID, holderID ledger.HolderID, reportIn money.AssetID) (*HolderBalancesResponse, error) {
	p := fmt.Sprintf("/v1/ledgers/%s/holders/%s/balances", url.PathEscape(string(l)), url.PathEscape(string(holderID)))
	if reportIn != "" {
		p += "?report_in=" + url.QueryEscape(string(reportIn))
	}
	var out HolderBalancesResponse
	err := c.doJSON(ctx, http.MethodGet, p, "", nil, &out)
	return &out, err
}

func (c *Client) CreateAccount(ctx context.Context, l ledger.LedgerID, idemKey string, req ledger.Account) (*ledger.Account, error) {
	var out ledger.Account
	err := c.doJSON(ctx, http.MethodPost, fmt.Sprintf("/v1/ledgers/%s/accounts", url.PathEscape(string(l))), idemKey, req, &out)
	return &out, err
}

func (c *Client) PatchAccount(ctx context.Context, l ledger.LedgerID, acctID ledger.AccountID, idemKey string, req PatchAccountRequest) (*ledger.Account, error) {
	var out ledger.Account
	err := c.doJSON(ctx, http.MethodPatch, fmt.Sprintf("/v1/ledgers/%s/accounts/%s", url.PathEscape(string(l)), url.PathEscape(string(acctID))), idemKey, req, &out)
	return &out, err
}

func (c *Client) GetAccountBalance(ctx context.Context, l ledger.LedgerID, acctID ledger.AccountID, at *time.Time) (*AccountBalanceResponse, error) {
	p := fmt.Sprintf("/v1/ledgers/%s/accounts/%s/balance", url.PathEscape(string(l)), url.PathEscape(string(acctID)))
	if at != nil {
		p += "?at=" + url.QueryEscape(at.UTC().Format(time.RFC3339))
	}
	var out AccountBalanceResponse
	err := c.doJSON(ctx, http.MethodGet, p, "", nil, &out)
	return &out, err
}

func (c *Client) GetAccountEntries(ctx context.Context, l ledger.LedgerID, acctID ledger.AccountID, cursor string, limit int) (*storage.Page[ledger.Entry], error) {
	q := url.Values{}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	p := fmt.Sprintf("/v1/ledgers/%s/accounts/%s/entries", url.PathEscape(string(l)), url.PathEscape(string(acctID)))
	if len(q) > 0 {
		p += "?" + q.Encode()
	}
	var out storage.Page[ledger.Entry]
	err := c.doJSON(ctx, http.MethodGet, p, "", nil, &out)
	return &out, err
}

func (c *Client) PostTransaction(ctx context.Context, l ledger.LedgerID, idemKey string, intent ledger.Intent) (*ledger.Transaction, error) {
	var out ledger.Transaction
	err := c.doJSON(ctx, http.MethodPost, fmt.Sprintf("/v1/ledgers/%s/transactions", url.PathEscape(string(l))), idemKey, intent, &out)
	return &out, err
}

func (c *Client) GetTransaction(ctx context.Context, l ledger.LedgerID, txID ledger.TransactionID) (*ledger.Transaction, error) {
	var out ledger.Transaction
	err := c.doJSON(ctx, http.MethodGet, fmt.Sprintf("/v1/ledgers/%s/transactions/%s", url.PathEscape(string(l)), url.PathEscape(string(txID))), "", nil, &out)
	return &out, err
}

func (c *Client) SettleHold(ctx context.Context, l ledger.LedgerID, holdID ledger.TransactionID, idemKey string, amt *money.Amount) (*ledger.Transaction, error) {
	var out ledger.Transaction
	err := c.doJSON(ctx, http.MethodPost, fmt.Sprintf("/v1/ledgers/%s/transactions/%s/post", url.PathEscape(string(l)), url.PathEscape(string(holdID))), idemKey, SettleHoldRequest{Amount: amt}, &out)
	return &out, err
}

func (c *Client) VoidHold(ctx context.Context, l ledger.LedgerID, holdID ledger.TransactionID, idemKey string) (*ledger.Transaction, error) {
	var out ledger.Transaction
	err := c.doJSON(ctx, http.MethodPost, fmt.Sprintf("/v1/ledgers/%s/transactions/%s/void", url.PathEscape(string(l)), url.PathEscape(string(holdID))), idemKey, map[string]any{}, &out)
	return &out, err
}

func (c *Client) ReverseTransaction(ctx context.Context, l ledger.LedgerID, origID ledger.TransactionID, idemKey string, postings []ledger.Posting) (*ledger.Transaction, error) {
	var out ledger.Transaction
	err := c.doJSON(ctx, http.MethodPost, fmt.Sprintf("/v1/ledgers/%s/transactions/%s/reverse", url.PathEscape(string(l)), url.PathEscape(string(origID))), idemKey, ReverseTransactionRequest{Postings: postings}, &out)
	return &out, err
}

func (c *Client) GetEvents(ctx context.Context, l ledger.LedgerID, after uint64, wait time.Duration) ([]events.Event, error) {
	q := url.Values{}
	if after > 0 {
		q.Set("after", strconv.FormatUint(after, 10))
	}
	if wait > 0 {
		q.Set("wait", wait.String())
	}
	p := fmt.Sprintf("/v1/ledgers/%s/events", url.PathEscape(string(l)))
	if len(q) > 0 {
		p += "?" + q.Encode()
	}
	var out struct {
		Items []events.Event `json:"items"`
	}
	err := c.doJSON(ctx, http.MethodGet, p, "", nil, &out)
	return out.Items, err
}

func (c *Client) CreateFXQuote(ctx context.Context, l ledger.LedgerID, idemKey string, req CreateFXQuoteRequest) (*FXQuoteResponse, error) {
	var out FXQuoteResponse
	err := c.doJSON(ctx, http.MethodPost, fmt.Sprintf("/v1/ledgers/%s/fx/quotes", url.PathEscape(string(l))), idemKey, req, &out)
	return &out, err
}

func (c *Client) ImportReconStatement(ctx context.Context, l ledger.LedgerID, idemKey string, req ImportReconStatementRequest) (*ImportReconStatementResponse, error) {
	var out ImportReconStatementResponse
	err := c.doJSON(ctx, http.MethodPost, fmt.Sprintf("/v1/ledgers/%s/recon/statements", url.PathEscape(string(l))), idemKey, req, &out)
	return &out, err
}

func (c *Client) GetCheckpoints(ctx context.Context, l ledger.LedgerID) ([]ledger.Checkpoint, error) {
	var out struct {
		Items []ledger.Checkpoint `json:"items"`
	}
	err := c.doJSON(ctx, http.MethodGet, fmt.Sprintf("/v1/ledgers/%s/journal/checkpoints", url.PathEscape(string(l))), "", nil, &out)
	return out.Items, err
}

func (c *Client) ClosePeriod(ctx context.Context, l ledger.LedgerID, start time.Time, idemKey string) (*ledger.Period, error) {
	var out ledger.Period
	err := c.doJSON(ctx, http.MethodPost, fmt.Sprintf("/v1/ledgers/%s/periods/%s/close", url.PathEscape(string(l)), url.PathEscape(start.UTC().Format(time.RFC3339))), idemKey, map[string]any{}, &out)
	return &out, err
}

func (c *Client) doJSON(ctx context.Context, method, path, idemKey string, reqBody any, respOut any) error {
	var bodyReader io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, bodyReader)
	if err != nil {
		return err
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	if c.APIKey != "" {
		req.Header.Set("X-API-Key", c.APIKey)
	}
	if traceID := ledger.TraceIDFromContext(ctx); traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}
	hc := c.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		var lErr ledger.Error
		if json.Unmarshal(data, &lErr) == nil && lErr.Code != "" {
			return &lErr
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if respOut != nil && len(data) > 0 {
		if err := json.Unmarshal(data, respOut); err != nil {
			return err
		}
		if tx, ok := respOut.(*ledger.Transaction); ok && resp.Header.Get("Idempotent-Replayed") == "true" {
			tx.IdempotentReplay = true
		}
	}
	return nil
}
