// Package events defines outbox event types, topic matching, and sink interfaces.
package events

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/core"
)

// Standard event types in v1 (RFC section 9).
const (
	TypeTransactionCommitted = "transaction.committed"
	TypeTransactionPending   = "transaction.pending"
	TypeTransactionPosted    = "transaction.posted"
	TypeTransactionVoided    = "transaction.voided"
	TypeTransactionExpired   = "transaction.expired"
	TypeTransactionReversed  = "transaction.reversed"
	TypeAccountCreated       = "account.created"
	TypeAccountStatusChanged = "account.status_changed"
	TypeJournalCheckpoint    = "journal.checkpoint"
	TypeModuleSkipped        = "module.skipped"
	TypeReconException       = "recon.exception"
	TypeInvariantViolation   = "invariant.violation"
)

// Event represents an outbox event emitted by the ledger.
type Event struct {
	ID          string          `json:"id"`
	Seq         uint64          `json:"seq"`
	Ledger      core.LedgerID   `json:"ledger"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
	DeliveredAt *time.Time      `json:"delivered_at,omitempty"`
}

// EventSink is implemented by downstream event publishers (e.g. webhook, NATS).
type EventSink interface {
	Publish(ctx context.Context, evs []Event) error
}

// TopicMatches reports whether pattern (such as "transaction.*" or "*" or "transaction.committed")
// matches eventType.
func TopicMatches(pattern, eventType string) bool {
	if pattern == "*" || pattern == eventType {
		return true
	}
	if strings.HasSuffix(pattern, ".*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(eventType, prefix)
	}
	return false
}

// AnyTopicMatches reports whether any pattern in topics matches eventType.
func AnyTopicMatches(topics []string, eventType string) bool {
	for _, pat := range topics {
		if TopicMatches(pat, eventType) {
			return true
		}
	}
	return false
}
