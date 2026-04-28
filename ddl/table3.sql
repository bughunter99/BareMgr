CREATE TABLE IF NOT EXISTS splunk_job1 (
  _rowid INTEGER PRIMARY KEY AUTOINCREMENT,
  collected_at TEXT DEFAULT CURRENT_TIMESTAMP,
  _time TEXT,
  host TEXT,
  label TEXT,
  message TEXT,
  severity TEXT
);
