-- ═══════════════════════════════════════════════════════════════════
-- Baton Router Schema — D1
-- Event-sourced message routing for inter-agent communication
-- ═══════════════════════════════════════════════════════════════════

-- ── Agents ───────────────────────────────────────────────────────
-- Registered participants in the baton network.
CREATE TABLE IF NOT EXISTS agents (
  id            TEXT PRIMARY KEY,
  name          TEXT NOT NULL UNIQUE,
  public_key    TEXT,
  role          TEXT NOT NULL CHECK(role IN ('forge','loom','oracle','ship','crab')),
  status        TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','suspended','retired')),
  api_key_hash  TEXT NOT NULL,
  registered_at TEXT NOT NULL DEFAULT (datetime('now')),
  last_seen     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_agents_role   ON agents(role);
CREATE INDEX IF NOT EXISTS idx_agents_status ON agents(status);

-- ── Messages ─────────────────────────────────────────────────────
-- The baton payloads traveling between agents.
CREATE TABLE IF NOT EXISTS messages (
  id           TEXT PRIMARY KEY,   -- UUID v4
  from_agent   TEXT NOT NULL,
  to_agent     TEXT NOT NULL,
  type         TEXT NOT NULL CHECK(type IN ('I2I','GC_SYNC','PID_UPDATE','CONSERVATION_AUDIT','BOTTLE','SPLINE')),
  payload      TEXT NOT NULL,
  priority     INTEGER NOT NULL DEFAULT 0 CHECK(priority IN (-1, 0, 1)),
  created_at   TEXT NOT NULL DEFAULT (datetime('now')),
  delivered_at TEXT,
  acked_at     TEXT,
  status       TEXT NOT NULL DEFAULT 'pending'
               CHECK(status IN ('pending','queued','delivered','acked','expired'))
);

CREATE INDEX IF NOT EXISTS idx_messages_to_agent_status ON messages(to_agent, status);
CREATE INDEX IF NOT EXISTS idx_messages_from_agent      ON messages(from_agent);
CREATE INDEX IF NOT EXISTS idx_messages_created_at      ON messages(created_at);
CREATE INDEX IF NOT EXISTS idx_messages_priority        ON messages(priority);

-- ── Message Log (event-sourced delivery audit) ──────────────────
-- Append-only log of every lifecycle event for a message.
CREATE TABLE IF NOT EXISTS message_log (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id  TEXT NOT NULL REFERENCES messages(id),
  event       TEXT NOT NULL CHECK(event IN ('created','delivered','acked','expired','replayed')),
  timestamp   TEXT NOT NULL DEFAULT (datetime('now')),
  detail      TEXT
);

CREATE INDEX IF NOT EXISTS idx_message_log_message_id ON message_log(message_id);
CREATE INDEX IF NOT EXISTS idx_message_log_event      ON message_log(event);

-- ── Routes ───────────────────────────────────────────────────────
-- Pattern-based routing rules (e.g., "GC_SYNC:*" → loom).
CREATE TABLE IF NOT EXISTS routes (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  pattern      TEXT NOT NULL,
  target_agent TEXT NOT NULL,
  priority     INTEGER NOT NULL DEFAULT 0 CHECK(priority IN (-1, 0, 1)),
  created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_routes_target ON routes(target_agent);

-- ── Dead Letter ──────────────────────────────────────────────────
-- Messages that exhausted retries or couldn't be delivered.
CREATE TABLE IF NOT EXISTS dead_letter (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id       TEXT NOT NULL,
  reason           TEXT NOT NULL,
  original_payload TEXT NOT NULL,
  timestamp        TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_message_id ON dead_letter(message_id);

-- ═══════════════════════════════════════════════════════════════════
-- Seed: default routes
-- ═══════════════════════════════════════════════════════════════════
INSERT OR IGNORE INTO routes (pattern, target_agent, priority) VALUES
  ('I2I:*',         'loom',   0),
  ('GC_SYNC:*',     'loom',   0),
  ('PID_UPDATE:*',  'oracle',  1),
  ('CONSERVATION_AUDIT:*', 'crab', -1),
  ('BOTTLE:*',      'loom',   0),
  ('SPLINE:*',      'ship',   0);
