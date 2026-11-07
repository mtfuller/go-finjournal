CREATE SCHEMA IF NOT EXISTS ledger;
DO $$ BEGIN
  CREATE DOMAIN ledger.amount AS numeric(39,0) CHECK (VALUE >= 0);
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

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

CREATE TABLE IF NOT EXISTS ledger.module_migrations (
  module        text NOT NULL,
  migration_id  text NOT NULL,
  applied_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (module, migration_id)
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

-- Single-round-trip batch posting function (RFC Section 5).
-- Acquires all row locks in ascending account_id and tx_id order, applies per-intent
-- mutations inside a subtransaction so any failing intent rolls back cleanly without
-- aborting its neighbours, checks net limits, writes entries/idempotency/outbox, and
-- returns a JSONB array of per-intent results.
CREATE OR REPLACE FUNCTION ledger.post_batch(intents jsonb) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
  v_results          jsonb := '[]'::jsonb;
  v_item             jsonb;
  v_op               int;
  v_tx               jsonb;
  v_tx_id            uuid;
  v_ledger_id        text;
  v_idem_key         text;
  v_req_hash         bytea;
  v_ex_hash          bytea;
  v_ex_tx_id         uuid;
  v_target_id        uuid;
  v_target_status    smallint;
  v_target_reversed  jsonb;
  v_target_postings  jsonb;
  v_new_reversed     jsonb;
  v_pending          boolean;
  v_status_int       smallint;
  v_postings         jsonb;
  v_init_rev         jsonb;
  v_p                jsonb;
  v_pidx             int;
  v_debit_id         uuid;
  v_credit_id        uuid;
  v_amt              numeric(39,0);
  v_mode             smallint;
  v_norm             smallint;
  v_pd               numeric(39,0);
  v_pc               numeric(39,0);
  v_pend_d           numeric(39,0);
  v_pend_c           numeric(39,0);
  v_ver              bigint;
  v_posted_after     numeric(39,0);
  v_acct_id          uuid;
  v_code             text;
  v_min_avail        numeric(39,0);
  v_max_avail        numeric(39,0);
  v_avail            numeric(39,0);
  v_orig_amt         numeric(39,0);
  v_prev_rev         numeric(39,0);
  v_add_rev          numeric(39,0);
  v_ev               jsonb;
  v_err_msg          text;
  v_err_parts        text[];
BEGIN
  IF intents IS NULL OR jsonb_typeof(intents) <> 'array' OR jsonb_array_length(intents) = 0 THEN
    RETURN '[]'::jsonb;
  END IF;

  -- Step 1: Lock all Locked-mode account balances touched across the entire batch in ascending account_id order.
  PERFORM 1
  FROM ledger.balances b
  JOIN ledger.accounts a ON a.id = b.account_id
  WHERE a.balance_mode = 0
    AND b.account_id IN (
      SELECT (p->>'debit')::uuid
      FROM jsonb_array_elements(intents) AS item,
           jsonb_array_elements(COALESCE(item->'tx'->'postings', '[]'::jsonb)) AS p
      UNION
      SELECT (p->>'credit')::uuid
      FROM jsonb_array_elements(intents) AS item,
           jsonb_array_elements(COALESCE(item->'tx'->'postings', '[]'::jsonb)) AS p
      UNION
      SELECT (tp->>'debit')::uuid
      FROM jsonb_array_elements(intents) AS item
      JOIN ledger.transactions t ON t.id = NULLIF(item->>'target_tx_id', '')::uuid
      CROSS JOIN LATERAL jsonb_array_elements(t.postings) AS tp
      UNION
      SELECT (tp->>'credit')::uuid
      FROM jsonb_array_elements(intents) AS item
      JOIN ledger.transactions t ON t.id = NULLIF(item->>'target_tx_id', '')::uuid
      CROSS JOIN LATERAL jsonb_array_elements(t.postings) AS tp
    )
  ORDER BY b.account_id ASC
  FOR UPDATE OF b;

  -- Lock any target tx_state rows in ascending tx_id order.
  PERFORM 1
  FROM ledger.tx_state st
  WHERE st.tx_id IN (
    SELECT DISTINCT NULLIF(item->>'target_tx_id', '')::uuid
    FROM jsonb_array_elements(intents) AS item
    WHERE NULLIF(item->>'target_tx_id', '') IS NOT NULL
  )
  ORDER BY st.tx_id ASC
  FOR UPDATE OF st;

  -- Step 2: Process each intent in its own subtransaction block.
  FOR v_item IN SELECT value FROM jsonb_array_elements(intents)
  LOOP
    BEGIN
      v_op        := COALESCE((v_item->>'op')::int, 0);
      v_tx        := v_item->'tx';
      v_tx_id     := (v_tx->>'id')::uuid;
      v_ledger_id := v_tx->>'ledger';
      v_idem_key  := COALESCE(v_tx->>'idempotency_key', '');
      v_req_hash  := decode(COALESCE(v_item->>'request_hash', ''), 'hex');
      v_postings  := COALESCE(v_tx->'postings', '[]'::jsonb);
      v_target_id := NULLIF(v_item->>'target_tx_id', '')::uuid;

      -- Check existing idempotency claim
      IF v_idem_key <> '' THEN
        SELECT request_hash, tx_id INTO v_ex_hash, v_ex_tx_id
        FROM ledger.idempotency
        WHERE ledger_id = v_ledger_id AND key = v_idem_key;

        IF FOUND THEN
          IF v_ex_hash <> v_req_hash THEN
            v_results := v_results || jsonb_build_array(jsonb_build_object(
              'ok', false,
              'error_code', 'idempotency_conflict',
              'message', 'idempotency key already used with different parameters'
            ));
          ELSE
            v_results := v_results || jsonb_build_array(jsonb_build_object(
              'ok', true,
              'idempotent_replay', true,
              'tx_id', v_ex_tx_id::text
            ));
          END IF;
          CONTINUE;
        END IF;
      END IF;

      -- Handle target transaction state for hold settlement/void/expiry or reversal
      v_target_postings := '[]'::jsonb;
      IF v_op IN (1, 2, 3, 4) THEN
        SELECT status, reversed INTO v_target_status, v_target_reversed
        FROM ledger.tx_state WHERE tx_id = v_target_id;
        IF NOT FOUND THEN
          RAISE EXCEPTION 'FJ_ERR|not_found|target transaction % not found', v_target_id;
        END IF;

        SELECT postings INTO v_target_postings
        FROM ledger.transactions WHERE id = v_target_id;

        IF v_op IN (1, 2, 3) THEN
          IF v_target_status <> 2 THEN
            RAISE EXCEPTION 'FJ_ERR|validation_failed|hold % is not pending', v_target_id;
          END IF;
          FOR v_p IN SELECT value FROM jsonb_array_elements(v_target_postings)
          LOOP
            v_debit_id  := (v_p->>'debit')::uuid;
            v_credit_id := (v_p->>'credit')::uuid;
            v_amt       := (v_p->>'amount')::numeric;

            UPDATE ledger.balances b
            SET pending_debits = b.pending_debits - v_amt,
                version = b.version + CASE WHEN v_op IN (2, 3) THEN 1 ELSE 0 END,
                updated_at = now()
            FROM ledger.accounts a
            WHERE b.account_id = v_debit_id AND a.id = b.account_id AND a.balance_mode = 0;

            UPDATE ledger.balances b
            SET pending_credits = b.pending_credits - v_amt,
                version = b.version + CASE WHEN v_op IN (2, 3) THEN 1 ELSE 0 END,
                updated_at = now()
            FROM ledger.accounts a
            WHERE b.account_id = v_credit_id AND a.id = b.account_id AND a.balance_mode = 0;
          END LOOP;

          UPDATE ledger.tx_state
          SET status = CASE WHEN v_op = 1 THEN 1 WHEN v_op = 2 THEN 3 ELSE 4 END
          WHERE tx_id = v_target_id;

        ELSIF v_op = 4 THEN
          IF v_target_status <> 1 THEN
            RAISE EXCEPTION 'FJ_ERR|validation_failed|transaction % cannot be reversed', v_target_id;
          END IF;
          v_new_reversed := '[]'::jsonb;
          FOR v_pidx IN 0 .. jsonb_array_length(v_target_postings) - 1
          LOOP
            v_orig_amt := (v_target_postings->v_pidx->>'amount')::numeric;
            v_prev_rev := COALESCE(NULLIF(v_target_reversed->>v_pidx, '')::numeric, 0);
            v_add_rev  := COALESCE(NULLIF(v_item->'reversal_amts'->>v_pidx, '')::numeric, 0);
            IF v_prev_rev + v_add_rev > v_orig_amt THEN
              RAISE EXCEPTION 'FJ_ERR|validation_failed|reversal on posting % exceeds original amount %', v_pidx, v_orig_amt;
            END IF;
            v_new_reversed := v_new_reversed || jsonb_build_array((v_prev_rev + v_add_rev)::text);
          END LOOP;

          UPDATE ledger.tx_state
          SET reversed = v_new_reversed
          WHERE tx_id = v_target_id;
        END IF;
      END IF;

      -- Insert new transaction & entries for OpPost (0), OpSettleHold (1), OpReverse (4)
      IF v_op IN (0, 1, 4) THEN
        v_pending    := (v_tx->>'status' = 'pending' OR v_tx->>'status' = '2');
        v_status_int := CASE WHEN v_pending THEN 2 ELSE 1 END;

        INSERT INTO ledger.transactions
          (id, ledger_id, kind, pending, postings, effective_at, committed_at, expires_at, reverses, pending_id, idempotency_key, metadata)
        VALUES (
          v_tx_id,
          v_ledger_id,
          v_tx->>'kind',
          v_pending,
          v_postings,
          (v_tx->>'effective_at')::timestamptz,
          (v_tx->>'committed_at')::timestamptz,
          NULLIF(v_tx->>'expires_at', '')::timestamptz,
          NULLIF(v_tx->>'reverses', '')::uuid,
          NULLIF(v_tx->>'pending_id', '')::uuid,
          v_idem_key,
          COALESCE(v_tx->'metadata', '{}'::jsonb)
        );

        SELECT COALESCE(jsonb_agg('0'::text), '[]'::jsonb)
        INTO v_init_rev
        FROM jsonb_array_elements(v_postings);

        INSERT INTO ledger.tx_state (tx_id, status, reversed)
        VALUES (v_tx_id, v_status_int, v_init_rev)
        ON CONFLICT (tx_id) DO NOTHING;

        IF v_pending THEN
          FOR v_p IN SELECT value FROM jsonb_array_elements(v_postings)
          LOOP
            v_debit_id  := (v_p->>'debit')::uuid;
            v_credit_id := (v_p->>'credit')::uuid;
            v_amt       := (v_p->>'amount')::numeric;

            UPDATE ledger.balances b
            SET pending_debits = b.pending_debits + v_amt,
                version = b.version + 1,
                updated_at = now()
            FROM ledger.accounts a
            WHERE b.account_id = v_debit_id AND a.id = b.account_id AND a.balance_mode = 0;

            UPDATE ledger.balances b
            SET pending_credits = b.pending_credits + v_amt,
                version = b.version + 1,
                updated_at = now()
            FROM ledger.accounts a
            WHERE b.account_id = v_credit_id AND a.id = b.account_id AND a.balance_mode = 0;
          END LOOP;
        ELSE
          FOR v_pidx IN 0 .. jsonb_array_length(v_postings) - 1
          LOOP
            v_p         := v_postings->v_pidx;
            v_debit_id  := (v_p->>'debit')::uuid;
            v_credit_id := (v_p->>'credit')::uuid;
            v_amt       := (v_p->>'amount')::numeric;

            -- Debit leg (side = 1)
            SELECT balance_mode, normal_side INTO v_mode, v_norm
            FROM ledger.accounts WHERE id = v_debit_id;
            IF v_mode = 0 THEN
              UPDATE ledger.balances
              SET posted_debits = posted_debits + v_amt,
                  version = version + 1,
                  updated_at = now()
              WHERE account_id = v_debit_id
              RETURNING posted_debits, posted_credits, version INTO v_pd, v_pc, v_ver;
              v_posted_after := CASE WHEN v_norm = 1 THEN v_pd - v_pc ELSE v_pc - v_pd END;
            ELSE
              v_posted_after := NULL;
              v_ver := 0;
            END IF;

            INSERT INTO ledger.entries
              (tx_id, posting_index, side, account_id, amount, posted_after, version, effective_at, committed_at)
            VALUES (
              v_tx_id, v_pidx, 1, v_debit_id, v_amt, v_posted_after, v_ver,
              (v_tx->>'effective_at')::timestamptz, (v_tx->>'committed_at')::timestamptz
            );

            -- Credit leg (side = 2)
            SELECT balance_mode, normal_side INTO v_mode, v_norm
            FROM ledger.accounts WHERE id = v_credit_id;
            IF v_mode = 0 THEN
              UPDATE ledger.balances
              SET posted_credits = posted_credits + v_amt,
                  version = version + 1,
                  updated_at = now()
              WHERE account_id = v_credit_id
              RETURNING posted_debits, posted_credits, version INTO v_pd, v_pc, v_ver;
              v_posted_after := CASE WHEN v_norm = 1 THEN v_pd - v_pc ELSE v_pc - v_pd END;
            ELSE
              v_posted_after := NULL;
              v_ver := 0;
            END IF;

            INSERT INTO ledger.entries
              (tx_id, posting_index, side, account_id, amount, posted_after, version, effective_at, committed_at)
            VALUES (
              v_tx_id, v_pidx, 2, v_credit_id, v_amt, v_posted_after, v_ver,
              (v_tx->>'effective_at')::timestamptz, (v_tx->>'committed_at')::timestamptz
            );
          END LOOP;
        END IF;
      END IF;

      -- Check net limits on all touched Locked accounts for this intent
      FOR v_acct_id IN
        SELECT DISTINCT acct_id FROM (
          SELECT (p->>'debit')::uuid AS acct_id FROM jsonb_array_elements(v_postings) AS p
          UNION
          SELECT (p->>'credit')::uuid AS acct_id FROM jsonb_array_elements(v_postings) AS p
          UNION
          SELECT (tp->>'debit')::uuid AS acct_id FROM jsonb_array_elements(v_target_postings) AS tp
          UNION
          SELECT (tp->>'credit')::uuid AS acct_id FROM jsonb_array_elements(v_target_postings) AS tp
        ) touched
      LOOP
        SELECT a.code, a.normal_side, a.balance_mode, a.min_available, a.max_available,
               b.posted_debits, b.posted_credits, b.pending_debits, b.pending_credits
        INTO v_code, v_norm, v_mode, v_min_avail, v_max_avail, v_pd, v_pc, v_pend_d, v_pend_c
        FROM ledger.accounts a
        JOIN ledger.balances b ON b.account_id = a.id
        WHERE a.id = v_acct_id;

        IF v_mode = 0 THEN
          v_avail := CASE WHEN v_norm = 1 THEN (v_pd - v_pc) - v_pend_c ELSE (v_pc - v_pd) - v_pend_d END;
          IF v_min_avail IS NOT NULL AND v_avail < v_min_avail THEN
            RAISE EXCEPTION 'FJ_ERR|insufficient_funds|available balance % on account % (%) would fall below min %',
              v_avail, v_code, v_acct_id, v_min_avail;
          END IF;
          IF v_max_avail IS NOT NULL AND v_avail > v_max_avail THEN
            RAISE EXCEPTION 'FJ_ERR|validation_failed|available balance % on account % (%) would exceed max %',
              v_avail, v_code, v_acct_id, v_max_avail;
          END IF;
        END IF;
      END LOOP;

      -- Claim idempotency
      IF v_idem_key <> '' THEN
        INSERT INTO ledger.idempotency (ledger_id, key, request_hash, tx_id, created_at)
        VALUES (v_ledger_id, v_idem_key, v_req_hash, v_tx_id, (v_tx->>'committed_at')::timestamptz);
      END IF;

      -- Append outbox events
      FOR v_ev IN SELECT value FROM jsonb_array_elements(COALESCE(v_item->'events', '[]'::jsonb))
      LOOP
        INSERT INTO ledger.outbox (event_id, ledger_id, type, payload, created_at)
        VALUES (
          v_ev->>'id',
          v_ev->>'ledger',
          v_ev->>'type',
          COALESCE(v_ev->'payload', '{}'::jsonb),
          (v_ev->>'created_at')::timestamptz
        );
      END LOOP;

      v_results := v_results || jsonb_build_array(jsonb_build_object(
        'ok', true,
        'idempotent_replay', false,
        'tx_id', v_tx_id::text
      ));

    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
      IF v_err_msg LIKE 'FJ_ERR|%' THEN
        v_err_parts := string_to_array(v_err_msg, '|');
        v_results := v_results || jsonb_build_array(jsonb_build_object(
          'ok', false,
          'error_code', v_err_parts[2],
          'message', COALESCE(v_err_parts[3], v_err_msg)
        ));
      ELSE
        RAISE;
      END IF;
    END;
  END LOOP;

  RETURN v_results;
END $$;

-- Schema metadata table for startup version verification (RFC Section 14).
CREATE TABLE IF NOT EXISTS ledger.schema_meta (
  id             int PRIMARY KEY CHECK (id = 1),
  schema_version int NOT NULL,
  updated_at     timestamptz NOT NULL DEFAULT now()
);
INSERT INTO ledger.schema_meta (id, schema_version)
VALUES (1, 1)
ON CONFLICT (id) DO UPDATE SET schema_version = EXCLUDED.schema_version;

-- Optional multi-tenant Row-Level Security (RLS) helper on ledger_id (RFC Section 14).
-- When invoked by an operator, enables RLS policies on tenant-scoped tables using
-- current_setting('app.current_ledger', true).
CREATE OR REPLACE FUNCTION ledger.enable_tenant_rls() RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['assets', 'holders', 'accounts', 'transactions', 'outbox', 'journal', 'checkpoints', 'periods']
  LOOP
    EXECUTE format('ALTER TABLE ledger.%I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON ledger.%I', t);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON ledger.%I USING (current_setting(''app.current_ledger'', true) IS NULL OR current_setting(''app.current_ledger'', true) = '''' OR ledger_id = current_setting(''app.current_ledger'', true))',
      t
    );
  END LOOP;
END $$;

-- Monthly range partition helper for partitioned deployments (RFC Section 6 & 14).
CREATE OR REPLACE FUNCTION ledger.create_monthly_partition(p_table text, p_year int, p_month int) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  v_start timestamptz := make_timestamptz(p_year, p_month, 1, 0, 0, 0, 'UTC');
  v_end   timestamptz := v_start + interval '1 month';
  v_part  text        := format('%s_y%04sm%02s', p_table, p_year, p_month);
BEGIN
  RETURN format(
    'CREATE TABLE IF NOT EXISTS ledger.%I PARTITION OF ledger.%I FOR VALUES FROM (%L) TO (%L)',
    v_part, p_table, v_start, v_end
  );
END $$;
