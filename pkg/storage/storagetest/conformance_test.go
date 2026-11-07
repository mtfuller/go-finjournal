package storagetest_test

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/storage"
	"github.com/mtfuller/go-finjournal/pkg/storage/memory"
	"github.com/mtfuller/go-finjournal/pkg/storage/postgres"
	"github.com/mtfuller/go-finjournal/pkg/storage/sqlite"
	"github.com/mtfuller/go-finjournal/pkg/storage/storagetest"
)

func TestMemoryStoreConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Store {
		return memory.New()
	})
}

func TestSQLiteStoreConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Store {
		s, err := sqlite.Open(context.Background(), ":memory:")
		if err != nil {
			t.Fatalf("sqlite.Open: %v", err)
		}
		return s
	})
}

func TestSQLiteWithinTxAndImmutabilityTriggers(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, ":memory:")
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	eng, err := ledger.New(store)
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	if _, err := eng.CreateLedger(ctx, "main", "Main", nil); err != nil {
		t.Fatalf("CreateLedger: %v", err)
	}
	if _, err := eng.RegisterAsset(ctx, "main", "USD", 2); err != nil {
		t.Fatalf("RegisterAsset: %v", err)
	}
	a1, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "a1", Asset: "USD/2", Normal: ledger.Debit})
	a2, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "a2", Asset: "USD/2", Normal: ledger.Credit})

	// Test WithinTx: open caller SQL transaction, post inside it, and commit caller transaction
	sqlTx, err := store.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	tx, err := eng.Post(ctx, ledger.Intent{
		Ledger:         "main",
		Kind:           "transfer",
		IdempotencyKey: "within-tx-1",
		Postings: []ledger.Posting{{
			Debit:  a1.ID,
			Credit: a2.ID,
			Amount: money.FromInt64(500),
			Asset:  "USD/2",
		}},
	}, ledger.WithinTx(sqlTx))
	if err != nil {
		t.Fatalf("Post WithinTx: %v", err)
	}
	if err := sqlTx.Commit(); err != nil {
		t.Fatalf("sqlTx.Commit: %v", err)
	}

	// Verify immutability trigger on transactions table rejects DELETE and UPDATE
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM transactions WHERE id = ?`, string(tx.ID)); err == nil {
		t.Fatalf("expected DELETE on transactions to be blocked by immutability trigger")
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE entries SET amount = '9999'`); err == nil {
		t.Fatalf("expected UPDATE on entries to be blocked by immutability trigger")
	}
}

func TestPostgresStoreConformance(t *testing.T) {
	dsn := os.Getenv("FINJOURNAL_PG_DSN")
	if dsn == "" {
		dsn = startDockerPostgres(t)
	}

	ctx := context.Background()
	storagetest.Run(t, func(t *testing.T) storage.Store {
		s, err := postgres.Open(ctx, dsn)
		if err != nil {
			t.Fatalf("postgres.Open: %v", err)
		}
		if _, err := s.Pool().Exec(ctx, `DROP SCHEMA IF EXISTS ledger CASCADE`); err != nil {
			t.Fatalf("drop schema: %v", err)
		}
		if err := postgres.MigratePool(ctx, s.Pool()); err != nil {
			t.Fatalf("MigratePool: %v", err)
		}
		return s
	})

	t.Run("PostgresWithinTxPgxAndSqlAndImmutability", func(t *testing.T) {
		store, err := postgres.Open(ctx, dsn)
		if err != nil {
			t.Fatalf("postgres.Open: %v", err)
		}
		defer func() { _ = store.Close() }()
		if _, err := store.Pool().Exec(ctx, `DROP SCHEMA IF EXISTS ledger CASCADE`); err != nil {
			t.Fatalf("drop schema: %v", err)
		}
		if err := postgres.MigratePool(ctx, store.Pool()); err != nil {
			t.Fatalf("MigratePool: %v", err)
		}

		eng, err := ledger.New(store)
		if err != nil {
			t.Fatalf("ledger.New: %v", err)
		}
		defer func() { _ = eng.Close(ctx) }()

		_, _ = eng.CreateLedger(ctx, "main", "Main", nil)
		_, _ = eng.RegisterAsset(ctx, "main", "USD", 2)
		a1, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "a1", Asset: "USD/2", Normal: ledger.Debit})
		a2, _ := eng.CreateAccount(ctx, ledger.Account{Ledger: "main", Code: "a2", Asset: "USD/2", Normal: ledger.Credit})

		// 1. Test WithinTx with native pgx.Tx
		pgxTx, err := store.Pool().Begin(ctx)
		if err != nil {
			t.Fatalf("Pool().Begin: %v", err)
		}
		tx1, err := eng.Post(ctx, ledger.Intent{
			Ledger:         "main",
			Kind:           "transfer",
			IdempotencyKey: "pgx-within-tx-1",
			Postings: []ledger.Posting{{
				Debit:  a1.ID,
				Credit: a2.ID,
				Amount: money.FromInt64(1_250),
				Asset:  "USD/2",
			}},
		}, ledger.WithinTx(pgxTx))
		if err != nil {
			t.Fatalf("Post WithinTx(pgxTx): %v", err)
		}
		if err := pgxTx.Commit(ctx); err != nil {
			t.Fatalf("pgxTx.Commit: %v", err)
		}

		// 2. Test WithinTx with *sql.Tx
		sqlTx, err := store.DB().BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("DB().BeginTx: %v", err)
		}
		_, err = eng.Post(ctx, ledger.Intent{
			Ledger:         "main",
			Kind:           "transfer",
			IdempotencyKey: "sql-within-tx-1",
			Postings: []ledger.Posting{{
				Debit:  a1.ID,
				Credit: a2.ID,
				Amount: money.FromInt64(750),
				Asset:  "USD/2",
			}},
		}, ledger.WithinTx(sqlTx))
		if err != nil {
			t.Fatalf("Post WithinTx(sqlTx): %v", err)
		}
		if err := sqlTx.Commit(); err != nil {
			t.Fatalf("sqlTx.Commit: %v", err)
		}

		bal, err := store.Balance(ctx, a2.ID)
		if err != nil || bal.Posted(a2.Normal).Cmp(money.FromInt64(2_000)) != 0 {
			t.Fatalf("expected balance 2000 after pgxTx + sqlTx, got %s (err=%v)", bal.Posted(a2.Normal), err)
		}

		// 3. Verify immutability triggers on ledger.transactions, ledger.entries, and ledger.journal
		if _, _, err := eng.SealOnce(ctx, "main"); err != nil {
			t.Fatalf("SealOnce: %v", err)
		}
		if _, err := store.Pool().Exec(ctx, `DELETE FROM ledger.transactions WHERE id = $1`, string(tx1.ID)); err == nil {
			t.Fatalf("expected DELETE on ledger.transactions to be blocked by immutability trigger")
		}
		if _, err := store.Pool().Exec(ctx, `UPDATE ledger.entries SET amount = 999999`); err == nil {
			t.Fatalf("expected UPDATE on ledger.entries to be blocked by immutability trigger")
		}
		if _, err := store.Pool().Exec(ctx, `DELETE FROM ledger.journal`); err == nil {
			t.Fatalf("expected DELETE on ledger.journal to be blocked by immutability trigger")
		}
	})
}

func startDockerPostgres(t *testing.T) string {
	t.Helper()
	dockerBin, err := exec.LookPath("docker")
	if err != nil {
		for _, candidate := range []string{"/usr/local/bin/docker", "/opt/homebrew/bin/docker"} {
			if _, statErr := os.Stat(candidate); statErr == nil {
				dockerBin = candidate
				break
			}
		}
	}
	if dockerBin == "" {
		t.Skip("docker not found and FINJOURNAL_PG_DSN not set; skipping PostgreSQL conformance test")
	}

	out, err := exec.Command(dockerBin, "run", "-d", "--rm",
		"-e", "POSTGRES_PASSWORD=postgres",
		"-e", "POSTGRES_DB=finjournal",
		"-p", "127.0.0.1::5432",
		"postgres:17-alpine",
	).CombinedOutput()
	if err != nil {
		t.Skipf("docker run postgres:17-alpine unavailable (%v: %s); skipping PostgreSQL conformance test", err, strings.TrimSpace(string(out)))
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	containerID := strings.TrimSpace(lines[len(lines)-1])
	t.Cleanup(func() {
		_ = exec.Command(dockerBin, "rm", "-f", containerID).Run()
	})

	portOut, err := exec.Command(dockerBin, "port", containerID, "5432/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("docker port: %v (%s)", err, string(portOut))
	}
	hostPort := strings.TrimSpace(strings.Split(string(portOut), "\n")[0])
	dsn := "postgres://postgres:postgres@" + hostPort + "/finjournal?sslmode=disable"

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		s, openErr := postgres.Open(ctx, dsn)
		if openErr == nil {
			_ = s.Close()
			return dsn
		}
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for postgres container %s at %s: %v", containerID, dsn, openErr)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
