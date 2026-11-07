# go-finjournal Engineering Roadmap (v0.1 → v1.0)

This roadmap tracks the six phased releases from our current foundation to the full v1.0 specification in [rfcs/0001-ledger-core-design.md](rfcs/0001-ledger-core-design.md).

## Phase 1 — `v0.1`: Core Library, `pgx` Direct Mode & Conformance (Completed)
- [x] Zero-allocation 256-bit intermediate arithmetic in `pkg/money` (`MulRate`, `Rescale`, `Allocate`, `String`/`ParseMinor`) + AST float ban lint check (`TestZeroHeapAllocationsOnHotPath`, `TestNoFloatsInMoneyAndLedgerPackages`).
- [x] Refactor `pkg/storage/postgres` to `pgx/v5` (`*pgxpool.Pool`, `pgx.Tx` and `*sql.Tx` for `ledger.WithinTx`).
- [x] Implement `ledger.post_batch(intents jsonb)` PL/pgSQL function in `pkg/storage/postgres/migrations/0001_init.sql` and wire `postgres.Store.Batch()` into `DirectCommitter` and `BatchedCommitter`.
- [x] Add state-machine property tests and Docker PostgreSQL 17 conformance runner in `pkg/storage/storagetest`.
- **Exit Gate (Verified):** `pkg/money` at `94.1%` coverage and `pkg/ledger` at `90.1%` coverage (both `≥ 90%`); `storagetest.Run` passes.

## Phase 2 — `v0.2`: Module System, `fees/rules` & Multi-Currency `fx` (Completed)
- [x] Separate `go.mod` per official module under `modules/` with root `go.work`.
- [x] Implement `Host.Migrate(ctx, fs.FS)` for module-owned schemas (`fx_quotes`, `fee_schedules`) across Memory, SQLite, and PostgreSQL (`ledger.module_migrations`).
- [x] Add accrual rescaling (`USD/6 -> USD/2`) helper (`Engine.SettleAccrual`) routing remainders to `system:rounding:<asset>` or carrying them forward.
- **Exit Gate (Verified):** Official `fees/rules` and `rates/static` modules pass deterministic fault simulation (`internal/sim`).

## Phase 3 — `v0.3`: Hot-Account Concurrency, Batched Mode & Benchmark Matrix (Completed)
- [x] Wire `BatchedCommitter` to `postgres.Store.Batch()` (`ledger.post_batch(jsonb)`) with `50ms` lock timeout and batch split-and-retry.
- [x] Add background `100ms` deferred-balance folder worker (`ledger.WithDeferredFoldInterval`).
- [x] Expand `internal/sim` to test `BatchedCommitter` in `StepMode` with multi-intent batch splits and fault injection.
- [x] Expand `bench/` matrix (`W1`–`W6`) across `direct`, `deferred`, `batched`, and `sqlite` modes with `p50`/`p95`/`p99`/`p99.9` and `AbortRate` reporting.
- **Exit Gate (Verified):** `BatchedCommitter` split-on-timeout, in-batch idempotency, `internal/sim` batched seeds, and `bench/` `W1`–`W6` suite pass.

## Phase 4 — `v0.4`: REST `/v1` Contract, Auth, Outbox Relay & Sinks (Completed)
- [x] Typed server/client bindings (`server.Client`) and schema verification against `server/openapi.yaml`.
- [x] Real-time long-poll (`?wait={duration}`) and live SSE streaming on `GET /v1/ledgers/{l}/events?after={seq}`, plus two-step FX quote endpoint (`POST /v1/ledgers/{l}/fx/quotes`).
- [x] Per-key token-bucket rate limiting (`RateLimitRPS`/`RateLimitBurst`), TLS 1.3 & mTLS client CA verification (`server.BuildTLSConfig`), and velocity-limit validator module (`ledger.VelocityValidator` / `limits/velocity`).
- [x] Per-subscriber outbox relay error isolation and `module.Register` plugin discovery across all official modules (`fees/rules`, `rates/static`, `sinks/webhook`, `sinks/nats`, `recon`, `limits/velocity`) for `finjournald` and `xfinjournald`.
- **Exit Gate (Verified):** OpenAPI conformance tests, typed client round-trip tests, rate-limiting 429 tests, and at-least-once outbox subscriber isolation tests pass.

## Phase 5 — `v0.5`: Hash-Chain Sealer, Reconciliation, Partitioning & Operations (Completed)
- [x] Per-ledger leader election (`Journal.TryAdvisoryLock` / `pg_try_advisory_lock`) and configurable lookback window (`> 5s`, default `10s`) in `journal.Sealer`.
- [x] Persist `recon_expected` and `recon_external` tables via `Host.Migrate` (`modules/recon/migrations/0001_recon.sql`) and expose `POST /v1/ledgers/{l}/recon/statements` emitting `recon.exception` events.
- [x] End-to-end trace context propagation (`Traceparent` / `X-Trace-ID` -> `ledger.WithTraceID`), stage & module duration Prometheus metrics on `/metrics`, default metadata log redaction (`ledger.NewRedactingHandler`), and shipped Grafana dashboard (`ops/grafana/finjournal-dashboard.json`).
- [x] Monthly range partition helper (`ledger.create_monthly_partition`), optional tenant RLS helper (`ledger.enable_tenant_rls`), and startup schema version checks (`ledger.schema_meta` / `postgres.Store.CheckSchemaVersion`).
- **Exit Gate (Verified):** Multi-replica advisory-lock sealer, reconciliation statement import, and full storage conformance suite pass across Memory, SQLite, and PostgreSQL 17.

## Phase 6 — `v1.0`: Jepsen Fault Testing, Supply Chain Hardening & API Freeze
- [ ] Build Docker Compose + Toxiproxy + `porcupine` Jepsen suite (testing `kill -9`, `SIGSTOP`/`SIGCONT` mid-batch, and Postgres replica promotion).
- [ ] Add reproducible release pipeline with SLSA provenance, keyless `cosign` signing, `syft` SPDX SBOMs, `govulncheck`, `gosec`, `staticcheck`, and dependency budget enforcement (`< 15` direct deps).
- [ ] Freeze `pkg/ledger`, `pkg/money`, `pkg/storage`, `pkg/module`, and REST `/v1` for v1.0.
- **Exit Gate:** 5M nightly simulator seeds and Jepsen linearizability suite pass with zero violations.
