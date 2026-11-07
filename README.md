# go-finjournal

`go-finjournal` is an Apache-2.0, double-entry financial journal and ledger engine written in Go. Import it directly as a library (`pkg/ledger`) or run it as a standalone REST + OpenAPI server (`finjournald`).

## Highlights

- **128-bit minor-unit integer arithmetic (`pkg/money`)** — Per-asset precision (`USD/2`, `JPY/0`, `ETH/18`) with exact rational `MulRate`, `Rescale`, and largest-remainder `Allocate`. No floating-point operations anywhere in core.
- **Two delivery modes, one engine (`pkg/ledger`)** — Embed in Go services (including same-transaction posting via `ledger.WithinTx(tx)`) or deploy `finjournald`.
- **Intent + Hook Pipeline (`pkg/module`)** — Synchronous pre-commit `Enricher`, `FeeEngine`, `RateProvider`, `Validator`, and `Authorizer` hooks; asynchronous post-commit `Subscriber` hooks driven by the transactional outbox.
- **Direct & Batched Committers (`pkg/commit`)** — Deadlock-free ascending `AccountID` row locking in direct mode, plus leaderless batched row locking with split-on-timeout retry and `Deferred` hot-account balance folding.
- **Tamper-Evident Hash Chain (`pkg/journal`)** — RFC 8785 Canonical JSON (`JCS`) SHA-256 transaction hashing, chained sequence sealing, checkpoints, and `finjournalctl verify-chain`.
- **Eight Verified Invariants (I1–I8)** — Enforced across unit tests, fuzzers, storage conformance tests (`pkg/storage/storagetest`), deterministic fault-injecting simulation (`internal/sim`), and `finjournalctl check`.

## Repository Layout

```
go-finjournal/
  rfcs/                  # Architecture & design RFCs (RFC-0001)
  pkg/
    ledger/              # Engine, public Go API, posting pipeline, invariants I1–I8
    core/                # Shared domain types re-exported by pkg/ledger
    money/               # Amount (int128), Asset, Rate, MulRate, Rescale, Allocate
    storage/             # Batch-shaped Store & Tx interfaces
      postgres/          # PostgreSQL adapter + embedded SQL migrations
      sqlite/            # SQLite adapter (WAL mode, RAISE(ABORT) triggers)
      memory/            # In-memory reference store with fault injection hooks
      storagetest/       # Shared storage conformance test suite
    module/              # Module, Enricher, Validator, Authorizer, FeeEngine, RateProvider, Subscriber
    events/              # Event types, topic matching, EventSink interface
    journal/             # RFC 8785 JCS, SHA-256 hash chain sealer, checkpoints, verifier
    commit/              # DirectCommitter, BatchedCommitter
  modules/               # Official modules
    fees/rules/          # Versioned YAML fee schedules (percent + fixed + cap)
    rates/static/        # Static/configured FX rates, two-step quotes, fx_convert enricher
    sinks/webhook/       # HMAC-SHA256 signed webhook delivery from outbox
    sinks/nats/          # NATS JetStream event publisher
    recon/               # Statement CSV import + matching rules + exception tracking
  server/                # REST /v1 handlers, OpenAPI 3.1 spec, API-key & mTLS auth
  cmd/
    finjournald/         # REST server binary (bundles official modules)
    xfinjournald/        # Custom binary builder (like xcaddy)
    finjournalctl/       # CLI: migrate, verify-chain, trial-balance, check, bench
  internal/sim/          # Seeded deterministic simulator with fault injection
  bench/                 # Workloads W1–W6 load generator & Terraform reference
```

## Quick Start

### Library Mode

```go
store := memory.New()
eng, err := ledger.New(store)
if err != nil {
    log.Fatal(err)
}
defer eng.Close(ctx)

_, _ = eng.CreateLedger(ctx, "main", "Main Ledger", nil)
_, _ = eng.RegisterAsset(ctx, "main", "USD", 2)

tx, err := eng.Post(ctx, ledger.Intent{
    Ledger:         "main",
    Kind:           "transfer",
    IdempotencyKey: "xfer-001",
    Postings: []ledger.Posting{{
        Debit:  "assets:psp:usd",
        Credit: "liabilities:wallets:alice:usd",
        Amount: money.FromInt64(1001),
        Asset:  "USD/2",
    }},
})
```

### CLI & Server

```bash
# Print default server configuration
go run ./cmd/finjournald config print --defaults

# Print PostgreSQL DDL migrations
go run ./cmd/finjournalctl migrate --print

# Run benchmark workload W1
go run ./cmd/finjournalctl bench --workload W1 --mode direct --duration 2s

# Run deterministic simulation
go test ./internal/sim -v
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
