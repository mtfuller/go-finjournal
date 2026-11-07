package webhook_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mtfuller/go-finjournal/modules/sinks/webhook"
	"github.com/mtfuller/go-finjournal/pkg/events"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestWebhookHMACDelivery(t *testing.T) {
	var gotSig string
	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotSig = r.Header.Get("X-Finjournal-Signature")
			rec := httptest.NewRecorder()
			rec.WriteHeader(http.StatusOK)
			return rec.Result(), nil
		}),
	}
	wh := webhook.New(webhook.Config{
		Endpoint: "http://webhook.internal/events",
		Secret:   "whsec_test",
	}, httpClient)

	err := wh.Handle(context.Background(), []events.Event{{
		ID:        "evt-1",
		Seq:       1,
		Type:      events.TypeTransactionPosted,
		CreatedAt: time.Now().UTC(),
		Ledger:    "main",
	}})
	if err != nil {
		t.Fatalf("wh.Handle: %v", err)
	}
	if !strings.HasPrefix(gotSig, "sha256=") {
		t.Fatalf("expected X-Finjournal-Signature header with sha256= prefix, got %q", gotSig)
	}
}
