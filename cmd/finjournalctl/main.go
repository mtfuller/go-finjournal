// Command finjournalctl is the operational CLI for go-finjournal:
//   - finjournalctl migrate [--print] [--sqlite dsn]
//   - finjournalctl verify-chain --ledger <id> [--sqlite dsn]
//   - finjournalctl trial-balance --ledger <id> [--sqlite dsn]
//   - finjournalctl check --ledger <id> [--sqlite dsn]
//   - finjournalctl bench [--workload W1..W6] [--mode direct|deferred|batched|sqlite] [--duration 2s]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/mtfuller/go-finjournal/bench"
	"github.com/mtfuller/go-finjournal/pkg/journal"
	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/storage"
	"github.com/mtfuller/go-finjournal/pkg/storage/memory"
	"github.com/mtfuller/go-finjournal/pkg/storage/postgres"
	"github.com/mtfuller/go-finjournal/pkg/storage/sqlite"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	ctx := context.Background()
	subcmd := os.Args[1]
	args := os.Args[2:]

	switch subcmd {
	case "migrate":
		fs := flag.NewFlagSet("migrate", flag.ExitOnError)
		printSQL := fs.Bool("print", false, "print embedded PostgreSQL migration SQL to stdout")
		sqliteDSN := fs.String("sqlite", "", "apply migrations to a SQLite database path")
		_ = fs.Parse(args)

		if *printSQL {
			fmt.Print(postgres.MigrationSQL())
			return
		}
		if *sqliteDSN != "" {
			s, err := sqlite.Open(ctx, *sqliteDSN)
			if err != nil {
				fatalf("sqlite migrate failed: %v", err)
			}
			_ = s.Close()
			fmt.Printf("Migrated SQLite database at %s\n", *sqliteDSN)
			return
		}
		fmt.Print(postgres.MigrationSQL())

	case "verify-chain":
		fs := flag.NewFlagSet("verify-chain", flag.ExitOnError)
		ledgerID := fs.String("ledger", "main", "ledger ID to verify")
		sqliteDSN := fs.String("sqlite", "", "SQLite database path")
		_ = fs.Parse(args)

		store := openCLIStore(ctx, *sqliteDSN)
		defer func() { _ = store.Close() }()

		rep, err := journal.VerifyChain(ctx, store, ledger.LedgerID(*ledgerID))
		if err != nil {
			fatalf("chain verification failed: %v", err)
		}
		printJSON(rep)

	case "trial-balance":
		fs := flag.NewFlagSet("trial-balance", flag.ExitOnError)
		ledgerID := fs.String("ledger", "main", "ledger ID")
		sqliteDSN := fs.String("sqlite", "", "SQLite database path")
		_ = fs.Parse(args)

		store := openCLIStore(ctx, *sqliteDSN)
		defer func() { _ = store.Close() }()

		tb, err := ledger.TrialBalance(ctx, store, ledger.LedgerID(*ledgerID))
		if err != nil {
			fatalf("trial-balance failed: %v", err)
		}
		printJSON(tb)

	case "check":
		fs := flag.NewFlagSet("check", flag.ExitOnError)
		ledgerID := fs.String("ledger", "main", "ledger ID")
		sqliteDSN := fs.String("sqlite", "", "SQLite database path")
		_ = fs.Parse(args)

		store := openCLIStore(ctx, *sqliteDSN)
		defer func() { _ = store.Close() }()

		rep, err := ledger.CheckInvariants(ctx, store, ledger.LedgerID(*ledgerID))
		if err != nil {
			fatalf("check failed: %v", err)
		}
		printJSON(rep)
		if !rep.OK() {
			os.Exit(2)
		}

	case "bench":
		fs := flag.NewFlagSet("bench", flag.ExitOnError)
		workload := fs.String("workload", "W1", "benchmark workload (W1..W6)")
		mode := fs.String("mode", "direct", "commit mode (direct, deferred, batched, sqlite)")
		accounts := fs.Int("accounts", 1000, "number of accounts")
		workers := fs.Int("workers", 8, "concurrent worker goroutines")
		duration := fs.Duration("duration", time.Second, "benchmark duration")
		_ = fs.Parse(args)

		summary, err := bench.Run(ctx, bench.Options{
			Workload: *workload,
			Mode:     *mode,
			Accounts: *accounts,
			Workers:  *workers,
			Duration: *duration,
		})
		if err != nil {
			fatalf("bench failed: %v", err)
		}
		printJSON(summary)

	default:
		printUsage()
		os.Exit(1)
	}
}

func openCLIStore(ctx context.Context, sqliteDSN string) storage.Store {
	if sqliteDSN != "" {
		s, err := sqlite.Open(ctx, sqliteDSN)
		if err != nil {
			fatalf("failed to open sqlite %q: %v", sqliteDSN, err)
		}
		return s
	}
	s := memory.New()
	_ = s.Admin().CreateLedger(ctx, ledger.Ledger{ID: "main", Name: "Main", CreatedAt: time.Now().UTC()})
	return s
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `Usage: finjournalctl <command> [flags]

Commands:
  migrate        Print or apply SQL schema migrations (--print, --sqlite)
  verify-chain   Recompute and verify the tamper-evident hash chain (--ledger)
  trial-balance  Compute per-asset trial balance across a ledger (--ledger)
  check          Verify all 8 ledger invariants I1-I8 (--ledger)
  bench          Run open-loop benchmark workloads W1-W6 (--workload, --mode, --duration)`)
}
