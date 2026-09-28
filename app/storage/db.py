"""SQLite (WAL) connection + schema. One shared connection guarded by a lock; writes are tiny."""
from __future__ import annotations

import os
import sqlite3
import threading

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS metric_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL NOT NULL, service TEXT NOT NULL,
  total INTEGER, errors INTEGER, error_rate REAL,
  baseline_mean REAL, baseline_std REAL, z REAL, ratio REAL,
  state TEXT, severity TEXT
);
CREATE INDEX IF NOT EXISTS ix_snap_service_ts ON metric_snapshots(service, ts);

CREATE TABLE IF NOT EXISTS alerts (
  id TEXT PRIMARY KEY, dedup_key TEXT NOT NULL, service TEXT NOT NULL,
  anomaly_type TEXT NOT NULL DEFAULT 'error_rate',
  severity TEXT, peak_severity TEXT, status TEXT NOT NULL,   -- OPEN | RESOLVED
  created_at REAL NOT NULL, resolved_at REAL,
  current_rate REAL, baseline_rate REAL, z REAL, ratio REAL,
  sample_size INTEGER, reason TEXT
);

CREATE TABLE IF NOT EXISTS alert_deliveries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL, event TEXT NOT NULL, channel TEXT NOT NULL,
  status TEXT NOT NULL,                                     -- PENDING | DELIVERED | FAILED
  attempt_count INTEGER DEFAULT 0, last_attempt_at REAL, error_message TEXT,
  external_id TEXT                                          -- SNS MessageId / CloudWatch group:stream
);

CREATE TABLE IF NOT EXISTS checkpoints (
  source TEXT PRIMARY KEY, inode INTEGER, offset INTEGER, updated_at REAL
);
"""


class Database:
    """SQLite connection (WAL) with a lock, the schema and in-place migrations."""
    def __init__(self, path: str) -> None:
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        with self.lock:
            self.conn.executescript(SCHEMA)
            self._migrate()
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.commit()

    def _migrate(self) -> None:
        """CREATE TABLE IF NOT EXISTS leaves an older database untouched, so add columns introduced later.
        Idempotent and non-destructive: existing rows and data are kept."""
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(alert_deliveries)")}
        if "external_id" not in cols:
            self.conn.execute("ALTER TABLE alert_deliveries ADD COLUMN external_id TEXT")

    def close(self) -> None:
        """Close the connection."""
        with self.lock:
            self.conn.close()
