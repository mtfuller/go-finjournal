-- Module-owned schema for FX rate quotes (rates/static).
CREATE TABLE IF NOT EXISTS fx_quotes (
  id            TEXT PRIMARY KEY,
  base_asset    TEXT NOT NULL,
  quote_asset   TEXT NOT NULL,
  mid_rate      TEXT NOT NULL,
  customer_rate TEXT NOT NULL,
  source        TEXT NOT NULL,
  observed_at   TEXT NOT NULL,
  expires_at    TEXT NOT NULL
);
