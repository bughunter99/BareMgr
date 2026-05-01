CREATE TABLE IF NOT EXISTS object_profile (
  _rowid INTEGER PRIMARY KEY AUTOINCREMENT,
  collected_at TEXT DEFAULT CURRENT_TIMESTAMP,
  obj_id TEXT,
  source_table TEXT,
  payload_json TEXT
);

CREATE TABLE IF NOT EXISTS object_events (
  _rowid INTEGER PRIMARY KEY AUTOINCREMENT,
  collected_at TEXT DEFAULT CURRENT_TIMESTAMP,
  obj_id TEXT,
  event_name TEXT,
  event_value TEXT
);
