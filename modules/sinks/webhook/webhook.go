// Package webhook implements the official sinks/webhook module, delivering
// HMAC-SHA256 signed webhook HTTP requests from the transactional outbox.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/module"
)

func init() {
	module.Register("sinks/webhook", func() (module.Module, error) {
		return New(Config{}, nil), nil
	})
}

// Config configures the sinks/webhook module.
type Config struct {
	Endpoint string   `json:"endpoint" yaml:"endpoint"`
	Secret   string   `json:"secret" yaml:"secret"`
	Topics   []string `json:"topics" yaml:"topics"` // default ["*"]
	Timeout  string   `json:"timeout" yaml:"timeout"`
}

// Module is the sinks/webhook Subscriber and EventSink.
type Module struct {
	cfg    Config
	client *http.Client
	host   module.Host
}

// New creates a sinks/webhook Module.
func New(cfg Config, client *http.Client) *Module {
	if len(cfg.Topics) == 0 {
		cfg.Topics = []string{"*"}
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &Module{
		cfg:    cfg,
		client: client,
	}
}

func (m *Module) Name() string {
	return "sinks/webhook"
}

func (m *Module) Init(_ context.Context, h module.Host) error {
	m.host = h
	if m.cfg.Endpoint == "" {
		_ = h.DecodeConfig(&m.cfg)
	}
	if len(m.cfg.Topics) == 0 {
		m.cfg.Topics = []string{"*"}
	}
	return nil
}

func (m *Module) Close(_ context.Context) error {
	return nil
}

func (m *Module) Topics() []string {
	return m.cfg.Topics
}

func (m *Module) Handle(ctx context.Context, evs []events.Event) error {
	return m.Publish(ctx, evs)
}

// Publish sends each event as a signed HTTP POST request to Endpoint.
func (m *Module) Publish(ctx context.Context, evs []events.Event) error {
	if m.cfg.Endpoint == "" {
		return nil
	}
	for _, ev := range evs {
		body, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.cfg.Endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Finjournal-Event-ID", ev.ID)
		req.Header.Set("X-Finjournal-Event-Type", ev.Type)
		req.Header.Set("X-Finjournal-Signature", SignPayload(body, m.cfg.Secret))

		resp, err := m.client.Do(req)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("webhook endpoint returned HTTP %d for event %s", resp.StatusCode, ev.ID)
		}
	}
	return nil
}

// SignPayload computes "sha256=<hex>" HMAC-SHA256 over body with secret.
func SignPayload(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
