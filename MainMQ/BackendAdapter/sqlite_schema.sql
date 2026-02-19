PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS {{kv_table}} (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  value_type TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  expires_at INTEGER
);

CREATE TABLE IF NOT EXISTS {{msg_index_table}} (
  queue TEXT NOT NULL,
  state TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  available_at INTEGER NOT NULL,
  lease_until INTEGER,
  attempt INTEGER NOT NULL DEFAULT 0,
  message_key TEXT NOT NULL REFERENCES {{kv_table}}(key) ON DELETE CASCADE,
  target_consumer_id TEXT NOT NULL DEFAULT '',
  dlq_reason TEXT,
  dlq_at INTEGER,
  expired_at INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_msg_ready ON {{msg_index_table}}(queue, state, available_at, priority);
CREATE INDEX IF NOT EXISTS idx_msg_lease ON {{msg_index_table}}(state, lease_until);
CREATE INDEX IF NOT EXISTS idx_msg_queue ON {{msg_index_table}}(queue);
CREATE INDEX IF NOT EXISTS idx_msg_expired ON {{msg_index_table}}(expired_at) WHERE expired_at > 0;
