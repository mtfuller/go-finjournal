-- Module-owned schema for versioned fee schedules (fees/rules).
CREATE TABLE IF NOT EXISTS fee_schedules (
  id              TEXT NOT NULL,
  version         INTEGER NOT NULL,
  effective_from  TEXT NOT NULL,
  match_kind      TEXT NOT NULL,
  match_asset     TEXT NOT NULL,
  payer           TEXT NOT NULL,
  payee           TEXT NOT NULL,
  percent_rate    TEXT NOT NULL DEFAULT '',
  fixed_amount    TEXT NOT NULL DEFAULT '',
  cap_amount      TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (id, version)
);
