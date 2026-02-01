CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Ledger imutável (hash-chain)
CREATE TABLE IF NOT EXISTS ledger_events (
  id            BIGSERIAL PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  event_type    TEXT NOT NULL,
  payload       JSONB NOT NULL,
  prev_hash     TEXT NOT NULL,
  event_hash    TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_ledger_created_at ON ledger_events(created_at);

-- Projeções: locations monitoradas
CREATE TABLE IF NOT EXISTS locations (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name         TEXT NOT NULL,
  lat          DOUBLE PRECISION NOT NULL,
  lon          DOUBLE PRECISION NOT NULL,
  is_active    BOOLEAN NOT NULL DEFAULT true,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Projeção: estado atual por location
CREATE TABLE IF NOT EXISTS location_state (
  location_id      UUID PRIMARY KEY REFERENCES locations(id) ON DELETE CASCADE,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  decision         TEXT NOT NULL CHECK (decision IN ('NORMAL','ALERT','CRITICAL')),
  applied_rule     JSONB NOT NULL,
  raw_weather      JSONB NOT NULL,
  ledger_hash      TEXT NOT NULL
);

-- Histórico derivado
CREATE TABLE IF NOT EXISTS weather_snapshots (
  id             BIGSERIAL PRIMARY KEY,
  location_id    UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  decision       TEXT NOT NULL CHECK (decision IN ('NORMAL','ALERT','CRITICAL')),
  applied_rule   JSONB NOT NULL,
  raw_weather    JSONB NOT NULL,
  ledger_hash    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_snap_location_time ON weather_snapshots(location_id, created_at DESC);

-- Gênesis do ledger (ponto inicial determinístico)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM ledger_events) THEN
    INSERT INTO ledger_events(event_type, payload, prev_hash, event_hash)
    VALUES (
      'GENESIS',
      jsonb_build_object('note','n29 climarisk genesis'),
      '0',
      encode(digest('0' || E'\n' || '{"event_type":"GENESIS","payload":{"note":"n29 climarisk genesis"}}', 'sha256'), 'hex')
    );
  END IF;
END $$;
