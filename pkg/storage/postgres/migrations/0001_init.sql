CREATE SCHEMA IF NOT EXISTS ledger;
CREATE DOMAIN ledger.amount AS numeric(39,0) CHECK (VALUE >= 0);

CREATE TABLE IF NOT EXISTS ledger.ledgers (
  id          text PRIMARY KEY,
  name        text NOT NULL,
  metadata    jsonb NOT NULL DEFAULT '{}',
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger.assets (
  ledger_id  text NOT NULL REFERENCES ledger.ledgers,
  id         text NOT NULL,              -- 'USD/2'
  code       text NOT NULL,              -- 'USD'
  scale      smallint NOT NULL CHECK (scale BETWEEN 0 AND 30),
  PRIMARY KEY (ledger_id, id)
);

CREATE TABLE IF NOT EXISTS ledger.holders (
  id            uuid PRIMARY KEY,
  ledger_id     text NOT NULL REFERENCES ledger.ledgers,
  kind          text NOT NULL,
  external_ref  text NOT NULL,
  parent_id     uuid CHECK (parent_id IS NULL),      -- reserved for v2
  metadata      jsonb NOT NULL DEFAULT '{}',
  created_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (ledger_id, external_ref)
);

CREATE TABLE IF NOT EXISTS ledger.accounts (
  id             uuid PRIMARY KEY,
  ledger_id      text NOT NULL REFERENCES ledger.ledgers,
  holder_id      uuid REFERENCES ledger.holders,
  code           text NOT NULL,
  asset          text NOT NULL,
  normal_side    smallint NOT NULL CHECK (normal_side IN (1, 2)),
  balance_mode   smallint NOT NULL DEFAULT 0,      -- 0 locked, 1 deferred
  min_available  numeric(39,0),
  max_available  numeric(39,0),
  status         smallint NOT NULL DEFAULT 0,      -- 0 open, 1 frozen, 2 closed
  metadata       jsonb NOT NULL DEFAULT '{}',
  created_at     timestamptz NOT NULL DEFAULT now(),
  UNIQUE (ledger_id, code),
  FOREIGN KEY (ledger_id, asset) REFERENCES ledger.assets (ledger_id, id),
  CHECK (balance_mode = 0 OR (min_available IS NULL AND max_available IS NULL))
);
CREATE INDEX IF NOT EXISTS accounts_holder_asset ON ledger.accounts (holder_id, asset);

-- The only frequently updated table. fillfactor leaves room for HOT updates.
CREATE TABLE IF NOT EXISTS ledger.balances (
  account_id       uuid PRIMARY KEY REFERENCES ledger.accounts,
  posted_debits    ledger.amount NOT NULL DEFAULT 0,
  posted_credits   ledger.amount NOT NULL DEFAULT 0,
  pending_debits   ledger.amount NOT NULL DEFAULT 0,
  pending_credits  ledger.amount NOT NULL DEFAULT 0,
  version          bigint NOT NULL DEFAULT 0,
  updated_at       timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.01);

-- Insert-only.
CREATE TABLE IF NOT EXISTS ledger.transactions (
  id               uuid PRIMARY KEY,                 -- UUIDv7
  ledger_id        text NOT NULL REFERENCES ledger.ledgers,
  kind             text NOT NULL,
  pending          boolean NOT NULL,
  postings         jsonb NOT NULL,                   -- canonical form, hashed by the sealer
  effective_at     timestamptz NOT NULL,
  committed_at     timestamptz NOT NULL,
  expires_at       timestamptz,
  reverses         uuid REFERENCES ledger.transactions,
  pending_id       uuid REFERENCES ledger.transactions,
  idempotency_key  text NOT NULL,
  metadata         jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS tx_ledger_committed ON ledger.transactions (ledger_id, committed_at DESC);

-- The only mutable lifecycle data, kept apart so transactions stay insert-only.
CREATE TABLE IF NOT EXISTS ledger.tx_state (
  tx_id     uuid PRIMARY KEY REFERENCES ledger.transactions,
  status    smallint NOT NULL,                 -- 1 posted, 2 pending, 3 voided, 4 expired
  reversed  jsonb NOT NULL DEFAULT '[]'        -- per-posting reversed amounts
);
CREATE INDEX IF NOT EXISTS tx_state_pending ON ledger.tx_state (status) WHERE status = 2;

-- Insert-only. One row per account side of each posting.
CREATE TABLE IF NOT EXISTS ledger.entries (
  tx_id          uuid NOT NULL REFERENCES ledger.transactions,
  posting_index  smallint NOT NULL,
  side           smallint NOT NULL,
  account_id     uuid NOT NULL REFERENCES ledger.accounts,
  amount         ledger.amount NOT NULL CHECK (amount > 0),
  posted_after   numeric(39,0),                -- NULL for deferred accounts
  version        bigint,
  effective_at   timestamptz NOT NULL,
  committed_at   timestamptz NOT NULL,
  PRIMARY KEY (tx_id, posting_index, side)
);
CREATE INDEX IF NOT EXISTS entries_account_time ON ledger.entries (account_id, committed_at DESC, version DESC);

CREATE TABLE IF NOT EXISTS ledger.idempotency (
  ledger_id     text NOT NULL,
  key           text NOT NULL,
  request_hash  bytea NOT NULL,
  tx_id         uuid NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (ledger_id, key)
);

CREATE TABLE IF NOT EXISTS ledger.outbox (
  id            bigserial PRIMARY KEY,
  event_id      text NOT NULL,
  ledger_id     text NOT NULL,
  type          text NOT NULL,
  payload       jsonb NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  delivered_at  timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_undelivered ON ledger.outbox (id) WHERE delivered_at IS NULL;

CREATE TABLE IF NOT EXISTS ledger.subscriber_cursors (
  subscriber  text PRIMARY KEY,
  seq         bigint NOT NULL DEFAULT 0,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Insert-only hash chain (section 11).
CREATE TABLE IF NOT EXISTS ledger.journal (
  ledger_id   text NOT NULL,
  seq         bigint NOT NULL,
  tx_id       uuid NOT NULL UNIQUE REFERENCES ledger.transactions,
  tx_hash     bytea NOT NULL,
  chain_hash  bytea NOT NULL,
  sealed_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (ledger_id, seq)
);

CREATE TABLE IF NOT EXISTS ledger.checkpoints (
  ledger_id   text NOT NULL,
  seq         bigint NOT NULL,
  chain_hash  bytea NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (ledger_id, seq)
);

CREATE TABLE IF NOT EXISTS ledger.periods (
  ledger_id  text NOT NULL,
  starts_at  timestamptz NOT NULL,
  ends_at    timestamptz NOT NULL,
  closed_at  timestamptz,
  PRIMARY KEY (ledger_id, starts_at)
);

-- Immutability: defence in depth on top of role grants.
CREATE OR REPLACE FUNCTION ledger.forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'ledger rows are immutable: % on %', TG_OP, TG_TABLE_NAME;
END $$;

DROP TRIGGER IF EXISTS transactions_immutable ON ledger.transactions;
CREATE TRIGGER transactions_immutable BEFORE UPDATE OR DELETE ON ledger.transactions
  FOR EACH ROW EXECUTE FUNCTION ledger.forbid_mutation();

DROP TRIGGER IF EXISTS entries_immutable ON ledger.entries;
CREATE TRIGGER entries_immutable BEFORE UPDATE OR DELETE ON ledger.entries
  FOR EACH ROW EXECUTE FUNCTION ledger.forbid_mutation();

DROP TRIGGER IF EXISTS journal_immutable ON ledger.journal;
CREATE TRIGGER journal_immutable BEFORE UPDATE OR DELETE ON ledger.journal
  FOR EACH ROW EXECUTE FUNCTION ledger.forbid_mutation();
