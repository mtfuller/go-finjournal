package ledger

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type traceIDContextKey struct{}

// WithTraceID returns a new context carrying traceID for end-to-end span propagation
// across HTTP requests, engine posting stages, storage commits, and outbox events.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		return ctx
	}
	return context.WithValue(ctx, traceIDContextKey{}, traceID)
}

// TraceIDFromContext extracts the propagated trace ID from ctx, if present.
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(traceIDContextKey{}).(string)
	return v
}

// RedactingHandler wraps an slog.Handler and redacts any attribute named "metadata"
// (or prefixed with "metadata.") so sensitive PII/PCI values in transaction or account
// metadata are never leaked to structured logs by default (RFC sections 11 & 14).
type RedactingHandler struct {
	inner slog.Handler
}

// NewRedactingHandler wraps inner with default metadata value redaction.
func NewRedactingHandler(inner slog.Handler) *RedactingHandler {
	return &RedactingHandler{inner: inner}
}

func (h *RedactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *RedactingHandler) Handle(ctx context.Context, r slog.Record) error {
	cloned := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	if traceID := TraceIDFromContext(ctx); traceID != "" {
		cloned.AddAttrs(slog.String("trace_id", traceID))
	}
	r.Attrs(func(a slog.Attr) bool {
		cloned.AddAttrs(redactAttr(a))
		return true
	})
	return h.inner.Handle(ctx, cloned)
}

func (h *RedactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	redacted := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		redacted[i] = redactAttr(a)
	}
	return &RedactingHandler{inner: h.inner.WithAttrs(redacted)}
}

func (h *RedactingHandler) WithGroup(name string) slog.Handler {
	return &RedactingHandler{inner: h.inner.WithGroup(name)}
}

func redactAttr(a slog.Attr) slog.Attr {
	keyLower := strings.ToLower(a.Key)
	if keyLower == "metadata" || strings.HasPrefix(keyLower, "metadata.") {
		return slog.String(a.Key, "[REDACTED]")
	}
	if a.Value.Kind() == slog.KindGroup {
		groupAttrs := a.Value.Group()
		out := make([]slog.Attr, len(groupAttrs))
		for i, ga := range groupAttrs {
			if strings.ToLower( a.Key ) == "metadata" {
				out[i] = slog.String(ga.Key, "[REDACTED]")
			} else {
				out[i] = redactAttr(ga)
			}
		}
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(out...)}
	}
	return a
}

// MetricsSnapshot holds point-in-time engine telemetry for Prometheus exposition (RFC section 11).
type MetricsSnapshot struct {
	PostsCommittedTotal uint64
	PostsFailedTotal    uint64
	EnrichDurationMicros uint64
	ValidateDurationMicros uint64
	CommitDurationMicros uint64
	ModuleCallsTotal    map[string]uint64
	ModuleDurationMicros map[string]uint64
}

type engineTelemetry struct {
	postsCommitted       atomic.Uint64
	postsFailed          atomic.Uint64
	enrichDurationMicros atomic.Uint64
	validateDurationMicros atomic.Uint64
	commitDurationMicros atomic.Uint64

	mu                   sync.Mutex
	moduleCalls          map[string]uint64
	moduleDurationMicros map[string]uint64
}

func newEngineTelemetry() *engineTelemetry {
	return &engineTelemetry{
		moduleCalls:          make(map[string]uint64),
		moduleDurationMicros: make(map[string]uint64),
	}
}

func (t *engineTelemetry) recordModule(name string, d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.moduleCalls[name]++
	t.moduleDurationMicros[name] += uint64(d.Microseconds())
}

func (t *engineTelemetry) snapshot() MetricsSnapshot {
	t.mu.Lock()
	modCalls := make(map[string]uint64, len(t.moduleCalls))
	for k, v := range t.moduleCalls {
		modCalls[k] = v
	}
	modDur := make(map[string]uint64, len(t.moduleDurationMicros))
	for k, v := range t.moduleDurationMicros {
		modDur[k] = v
	}
	t.mu.Unlock()
	return MetricsSnapshot{
		PostsCommittedTotal:    t.postsCommitted.Load(),
		PostsFailedTotal:       t.postsFailed.Load(),
		EnrichDurationMicros:   t.enrichDurationMicros.Load(),
		ValidateDurationMicros: t.validateDurationMicros.Load(),
		CommitDurationMicros:   t.commitDurationMicros.Load(),
		ModuleCallsTotal:       modCalls,
		ModuleDurationMicros:   modDur,
	}
}
