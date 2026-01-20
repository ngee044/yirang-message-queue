-- Yi-Rang MQ SQLite Schema
-- KV-based storage with message index for queue operations

-- Key-Value Table: Stores message content and policies as JSON
CREATE TABLE IF NOT EXISTS {{kv_table}} (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    value_type TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    expires_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_kv_type ON {{kv_table}}(value_type);
CREATE INDEX IF NOT EXISTS idx_kv_expires ON {{kv_table}}(expires_at) WHERE expires_at IS NOT NULL;

-- Message Index Table: Efficient queue operations
CREATE TABLE IF NOT EXISTS {{msg_index_table}} (
    queue TEXT NOT NULL,
    state TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    available_at INTEGER NOT NULL,
    lease_until INTEGER,
    attempt INTEGER NOT NULL DEFAULT 0,
    dlq_reason TEXT,
    dlq_at INTEGER,
    message_key TEXT NOT NULL REFERENCES {{kv_table}}(key) ON DELETE CASCADE,
    PRIMARY KEY (message_key)
);

CREATE INDEX IF NOT EXISTS idx_msg_ready ON {{msg_index_table}}(queue, state, available_at, priority DESC);
CREATE INDEX IF NOT EXISTS idx_msg_lease ON {{msg_index_table}}(state, lease_until) WHERE state = 'inflight';
CREATE INDEX IF NOT EXISTS idx_msg_delayed ON {{msg_index_table}}(state, available_at) WHERE state = 'delayed';
CREATE INDEX IF NOT EXISTS idx_msg_dlq ON {{msg_index_table}}(queue, state) WHERE state = 'dlq';
