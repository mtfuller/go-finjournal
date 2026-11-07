// Package nats implements the official sinks/nats module, publishing committed
// outbox events to NATS JetStream subjects.
package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/module"
)

// Publisher abstracts a NATS JetStream connection for subject publishing with deduplication IDs.
type Publisher interface {
	PublishMsg(ctx context.Context, subject string, msgID string, data []byte) error
}

// MemoryPublisher is an in-memory Publisher for tests and local verification.
type MemoryPublisher struct {
	mu       sync.Mutex
	Messages []PublishedMessage
}

// PublishedMessage records a single published NATS message.
type PublishedMessage struct {
	Subject string
	MsgID   string
	Data    []byte
}

func (p *MemoryPublisher) PublishMsg(_ context.Context, subject string, msgID string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	p.Messages = append(p.Messages, PublishedMessage{
		Subject: subject,
		MsgID:   msgID,
		Data:    cp,
	})
	return nil
}

// Config configures the sinks/nats module.
type Config struct {
	SubjectPrefix string   `json:"subject_prefix" yaml:"subject_prefix"` // default "finjournal"
	Topics        []string `json:"topics" yaml:"topics"`                 // default ["*"]
}

// Module is the sinks/nats Subscriber and EventSink.
type Module struct {
	cfg  Config
	pub  Publisher
	host module.Host
}

// New creates a sinks/nats Module.
func New(cfg Config, pub Publisher) *Module {
	if cfg.SubjectPrefix == "" {
		cfg.SubjectPrefix = "finjournal"
	}
	if len(cfg.Topics) == 0 {
		cfg.Topics = []string{"*"}
	}
	if pub == nil {
		pub = &MemoryPublisher{}
	}
	return &Module{
		cfg: cfg,
		pub: pub,
	}
}

func (m *Module) Name() string {
	return "sinks/nats"
}

func (m *Module) Init(_ context.Context, h module.Host) error {
	m.host = h
	_ = h.DecodeConfig(&m.cfg)
	if m.cfg.SubjectPrefix == "" {
		m.cfg.SubjectPrefix = "finjournal"
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

func (m *Module) Publish(ctx context.Context, evs []events.Event) error {
	for _, ev := range evs {
		data, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		subj := fmt.Sprintf("%s.%s.%s", m.cfg.SubjectPrefix, ev.Ledger, ev.Type)
		if err := m.pub.PublishMsg(ctx, subj, ev.ID, data); err != nil {
			return err
		}
	}
	return nil
}
