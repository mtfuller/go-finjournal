package nats_test

import (
	"context"
	"testing"
	"time"

	"github.com/mtfuller/go-finjournal/modules/sinks/nats"
	"github.com/mtfuller/go-finjournal/pkg/events"
)

func TestNATSSinkPublish(t *testing.T) {
	pub := &nats.MemoryPublisher{}
	sink := nats.New(nats.Config{SubjectPrefix: "finjournal"}, pub)

	err := sink.Handle(context.Background(), []events.Event{{
		ID:        "evt-1",
		Seq:       1,
		Type:      events.TypeTransactionPosted,
		CreatedAt: time.Now().UTC(),
		Ledger:    "main",
	}})
	if err != nil {
		t.Fatalf("sink.Handle: %v", err)
	}
	if len(pub.Messages) != 1 || pub.Messages[0].Subject != "finjournal.main.transaction.posted" {
		t.Fatalf("unexpected NATS messages: %+v", pub.Messages)
	}
}
