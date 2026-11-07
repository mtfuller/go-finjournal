// Command finjournald is the REST + OpenAPI server binary for go-finjournal,
// bundling official modules (fees/rules, rates/static, sinks/webhook, sinks/nats, recon).
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/mtfuller/go-finjournal/modules/fees/rules"
	"github.com/mtfuller/go-finjournal/modules/rates/static"
	"github.com/mtfuller/go-finjournal/modules/recon"
	"github.com/mtfuller/go-finjournal/modules/sinks/nats"
	"github.com/mtfuller/go-finjournal/modules/sinks/webhook"
	"github.com/mtfuller/go-finjournal/pkg/commit"
	"github.com/mtfuller/go-finjournal/pkg/ledger"
	"github.com/mtfuller/go-finjournal/pkg/money"
	"github.com/mtfuller/go-finjournal/pkg/module"
	"github.com/mtfuller/go-finjournal/pkg/storage"
	"github.com/mtfuller/go-finjournal/pkg/storage/memory"
	"github.com/mtfuller/go-finjournal/pkg/storage/postgres"
	"github.com/mtfuller/go-finjournal/pkg/storage/sqlite"
	"github.com/mtfuller/go-finjournal/server"
)

// Config represents the finjournald YAML configuration (RFC Section 14).
type Config struct {
	Storage struct {
		Driver   string `yaml:"driver"` // "postgres", "sqlite", "memory"
		Postgres struct {
			DSN     string `yaml:"dsn"`
			DSNFile string `yaml:"dsn_file"`
		} `yaml:"postgres"`
		SQLite struct {
			DSN string `yaml:"dsn"`
		} `yaml:"sqlite"`
	} `yaml:"storage"`
	Server struct {
		Listen    string `yaml:"listen"`
		OpsListen string `yaml:"ops_listen"`
		TLS       struct {
			CertFile     string `yaml:"cert_file"`
			KeyFile      string `yaml:"key_file"`
			ClientCAFile string `yaml:"client_ca_file"`
		} `yaml:"tls"`
	} `yaml:"server"`
	Auth struct {
		APIKeysFile string               `yaml:"api_keys_file"`
		APIKeys     []server.APIKeyConfig `yaml:"api_keys,omitempty"`
	} `yaml:"auth"`
	Commit struct {
		Mode        string        `yaml:"mode"` // "direct" or "batched"
		MaxBatch    int           `yaml:"max_batch"`
		MaxWait     time.Duration `yaml:"max_wait"`
		LockTimeout time.Duration `yaml:"lock_timeout"`
	} `yaml:"commit"`
	RoundingMode string `yaml:"rounding_mode"`
	Idempotency  struct {
		Retention time.Duration `yaml:"retention"`
	} `yaml:"idempotency"`
	Journal struct {
		CheckpointEvery uint64 `yaml:"checkpoint_every"`
	} `yaml:"journal"`
	Modules []struct {
		Name     string         `yaml:"name"`
		Optional bool           `yaml:"optional"`
		Config   map[string]any `yaml:"config"`
	} `yaml:"modules"`
}

func defaultConfig() Config {
	var cfg Config
	cfg.Storage.Driver = "memory"
	cfg.Server.Listen = ":8080"
	cfg.Server.OpsListen = ":9090"
	cfg.Commit.Mode = "direct"
	cfg.Commit.MaxBatch = 200
	cfg.Commit.MaxWait = 2 * time.Millisecond
	cfg.Commit.LockTimeout = 50 * time.Millisecond
	cfg.RoundingMode = "half_even"
	cfg.Idempotency.Retention = 90 * 24 * time.Hour
	cfg.Journal.CheckpointEvery = 10_000
	return cfg
}

func main() {
	if len(os.Args) >= 3 && os.Args[1] == "config" && os.Args[2] == "print" {
		out, _ := yaml.Marshal(defaultConfig())
		fmt.Print(string(out))
		return
	}

	var configPath string
	flag.StringVar(&configPath, "config", "", "path to finjournal.yaml configuration file")
	flag.Parse()

	cfg := defaultConfig()
	if configPath != "" {
		raw, err := os.ReadFile(configPath)
		if err != nil {
			slog.Error("failed to read config file", "path", configPath, "err", err)
			os.Exit(1)
		}
		expanded := os.ExpandEnv(string(raw))
		if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
			slog.Error("failed to parse config file", "path", configPath, "err", err)
			os.Exit(1)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := openStore(ctx, cfg)
	if err != nil {
		slog.Error("failed to initialize storage", "err", err)
		os.Exit(1)
	}
	defer func() { _ = store.Close() }()

	var engOpts []ledger.Option
	if rm, err := money.ParseRoundingMode(cfg.RoundingMode); err == nil {
		engOpts = append(engOpts, ledger.WithRoundingMode(rm))
	}
	if cfg.Commit.Mode == "batched" {
		engOpts = append(engOpts, ledger.WithBatchCommitter(commit.BatchConfig{
			MaxBatch:    cfg.Commit.MaxBatch,
			MaxWait:     cfg.Commit.MaxWait,
			LockTimeout: cfg.Commit.LockTimeout,
		}))
	}

	modConfigs := make(map[string]any)
	for _, mc := range cfg.Modules {
		mod, err := instantiateOfficialModule(mc.Name)
		if err != nil {
			slog.Error("unknown module in config", "module", mc.Name, "err", err)
			os.Exit(1)
		}
		modConfigs[mc.Name] = mc.Config
		if mc.Optional {
			engOpts = append(engOpts, ledger.WithOptionalModule(mod))
		} else {
			engOpts = append(engOpts, ledger.WithModules(mod))
		}
	}
	engOpts = append(engOpts, ledger.WithModuleConfigs(modConfigs))

	eng, err := ledger.New(store, engOpts...)
	if err != nil {
		slog.Error("failed to initialize ledger engine", "err", err)
		os.Exit(1)
	}
	defer func() { _ = eng.Close(context.Background()) }()

	srv := server.New(eng, server.Config{
		APIKeys: cfg.Auth.APIKeys,
	})

	apiHTTP := &http.Server{
		Addr:    cfg.Server.Listen,
		Handler: srv.Handler(),
	}
	opsHTTP := &http.Server{
		Addr:    cfg.Server.OpsListen,
		Handler: srv.OpsHandler(),
	}

	go func() {
		slog.Info("starting finjournald ops server", "listen", cfg.Server.OpsListen)
		if err := opsHTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("ops server error", "err", err)
		}
	}()

	go func() {
		slog.Info("starting finjournald REST server", "listen", cfg.Server.Listen)
		if err := apiHTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("api server error", "err", err)
		}
	}()

	// Background maintenance ticker (outbox relay + hold expiry sweeper)
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = eng.RelayOnce(ctx)
				_, _ = eng.SweepExpiredHolds(ctx)
			}
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down finjournald...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = apiHTTP.Shutdown(shutCtx)
	_ = opsHTTP.Shutdown(shutCtx)
}

func openStore(ctx context.Context, cfg Config) (storage.Store, error) {
	switch cfg.Storage.Driver {
	case "", "memory":
		return memory.New(), nil
	case "sqlite":
		dsn := cfg.Storage.SQLite.DSN
		if dsn == "" {
			dsn = ":memory:"
		}
		return sqlite.Open(ctx, dsn)
	case "postgres":
		dsn := cfg.Storage.Postgres.DSN
		if dsn == "" && cfg.Storage.Postgres.DSNFile != "" {
			b, err := os.ReadFile(cfg.Storage.Postgres.DSNFile)
			if err != nil {
				return nil, err
			}
			dsn = string(b)
		}
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			return nil, err
		}
		if err := postgres.Migrate(ctx, db); err != nil {
			_ = db.Close()
			return nil, err
		}
		return postgres.New(db), nil
	default:
		return nil, fmt.Errorf("unsupported storage driver %q", cfg.Storage.Driver)
	}
}

func instantiateOfficialModule(name string) (module.Module, error) {
	switch name {
	case "fees/rules":
		return rules.New(rules.Config{})
	case "rates/static", "fx":
		return static.New(static.Config{})
	case "sinks/webhook":
		return webhook.New(webhook.Config{}, nil), nil
	case "sinks/nats":
		return nats.New(nats.Config{}, nil), nil
	case "recon":
		return recon.New(), nil
	default:
		return nil, fmt.Errorf("unregistered module %q", name)
	}
}
