# go-finjournal RFC-0001: Ledger Core Design

Oct 8, 2026 · @Thomas

## Status and answers to open questions

**Status:** accepted for v1 foundation.

| # | Decision as configured | This RFC proposes | Why |
| --- | --- | --- | --- |
| 1 | Versioned templates for authoring | A **transaction intent + hook pipeline** in core. Fee schedules live in an optional `fees/rules` module. | Matches the model: with no modules, the ledger just records transfers. Modules enrich or react. |
| 2 | Single-writer sequencer as the only commit path | **Direct mode** (ordered row locks) is the default. **Batched row locks** are opt-in for high throughput, with no leader and no ledger-wide lock. | Embedded users must be able to post inside their own DB transaction, and a sequencer can't join it. 10k TPS is reachable in direct mode on uniform load. |
| 3 | int128 postings with no decimal engine | Agreed. Core ships a small `money` package with integer `MulRate` and `Allocate` and no general decimal library. Precision is per asset; storage width is fixed. | Exact minor-unit arithmetic with explicit rounding boundaries. |

## Architecture

```
go-finjournal/
  pkg/
    ledger/          # Engine, public Go API, posting pipeline, invariants
    core/            # Shared domain types re-exported by pkg/ledger
    money/           # Amount (int128), Asset, Rate, MulRate, Rescale, Allocate
    storage/         # Store and Tx interfaces + conformance test suite
      postgres/      # pgx; production adapter; SQL migrations embedded
      sqlite/        # modernc.org/sqlite; tests, demos, edge
      memory/        # reference implementation; simulation testing
      storagetest/   # Shared storage conformance suite
    module/          # Module, Enricher, Validator, Authorizer, Subscriber, FeeEngine, RateProvider
    events/          # Event types, outbox relay, EventSink interface
    journal/         # hash chain sealer, checkpoints, verifier, RFC 8785 JCS
    commit/          # DirectCommitter, BatchedCommitter
  modules/           # official modules
    fees/rules/      # YAML fee schedules
    rates/static/    # fixed or file-based FX rates + FX enricher
    sinks/webhook/   # signed webhooks from the outbox
    sinks/nats/      # NATS JetStream publisher
    recon/           # statement import + matching
  server/            # REST handlers, OpenAPI spec, auth middleware
  cmd/
    finjournald/     # server binary (default build includes official modules)
    xfinjournald/    # builder: compiles finjournald with extra modules (like xcaddy)
    finjournalctl/   # CLI: migrate, verify-chain, trial-balance, check, bench
  internal/sim/      # deterministic simulator, fault-injecting store
  bench/             # load generator, Terraform, published results
```

**Dependency rule:** `pkg/ledger` imports `core`, `money`, `storage` (interfaces only), `module`, `commit`, `journal` and `events`. It never imports a concrete adapter, `server`, or any official module.
