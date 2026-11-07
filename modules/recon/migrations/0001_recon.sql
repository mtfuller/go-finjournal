-- Module-owned schema for reconciliation expected and external statement items (recon).
CREATE TABLE IF NOT EXISTS recon_expected (
  tx_id         TEXT NOT NULL,
  ledger_id     TEXT NOT NULL,
  account_id    TEXT NOT NULL,
  source        TEXT NOT NULL,
  reference     TEXT NOT NULL,
  amount        TEXT NOT NULL,
  asset         TEXT NOT NULL,
  effective_at  TEXT NOT NULL,
  matched       INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tx_id, account_id)
);

CREATE TABLE IF NOT EXISTS recon_external (
  id          TEXT PRIMARY KEY,
  source      TEXT NOT NULL,
  reference   TEXT NOT NULL,
  amount      TEXT NOT NULL,
  asset       TEXT NOT NULL,
  value_date  TEXT NOT NULL,
  status      TEXT NOT NULL,
  rule        TEXT NOT NULL DEFAULT '',
  diff        TEXT NOT NULL DEFAULT '0'
);
