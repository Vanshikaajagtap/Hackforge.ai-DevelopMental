"""All SQL lives here. Every method degrades instead of raising: a broken DB flips `healthy` (health panel)
but never stops ingestion or detection."""
from __future__ import annotations

import functools
import logging
import sqlite3
from dataclasses import asdict
from typing import Callable, Iterable

from app.detection.models import Alert, Snapshot

from .db import Database

log = logging.getLogger("logpulse.db")

_ALERT_COLS = ("id", "dedup_key", "service", "severity", "peak_severity", "status", "created_at", "resolved_at",
               "current_rate", "baseline_rate", "z", "ratio", "sample_size", "reason")


def _safe(default: Callable[[], object]):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(self, *a, **kw):
            try:
                out = fn(self, *a, **kw)
                self.healthy = True
                return out
            except sqlite3.Error as e:
                self.healthy = False
                self.last_error = str(e)
                log.error("db error in %s: %s", fn.__name__, e)
                return default()
        return wrapper
    return deco


def _alert(row: sqlite3.Row) -> Alert:
    return Alert(**{c: row[c] for c in _ALERT_COLS})


class Repository:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.healthy = True
        self.last_error: str | None = None

    @property
    def status(self) -> str:
        return "ok" if self.healthy else "error"

    # ---- metric snapshots ------------------------------------------------------------------------
    @_safe(lambda: None)
    def save_snapshots(self, snaps: Iterable[Snapshot]) -> None:
        rows = [(s.ts, s.service, s.total, s.errors, s.error_rate, s.baseline_mean, s.baseline_std,
                 s.z, s.ratio, s.state, s.severity) for s in snaps]
        with self.db.lock:
            self.db.conn.executemany(
                "INSERT INTO metric_snapshots(ts,service,total,errors,error_rate,baseline_mean,baseline_std,z,ratio,state,severity)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
            self.db.conn.commit()

    @_safe(lambda: [])
    def history(self, service: str, since_ts: float) -> list[dict]:
        with self.db.lock:
            rows = self.db.conn.execute(
                "SELECT ts,service,total,errors,error_rate,baseline_mean,baseline_std,z,ratio,state,severity"
                " FROM metric_snapshots WHERE service=? AND ts>=? ORDER BY ts", (service, since_ts)).fetchall()
        return [dict(r) for r in rows]

    @_safe(lambda: [])
    def services(self) -> list[str]:
        with self.db.lock:
            return [r[0] for r in self.db.conn.execute("SELECT DISTINCT service FROM metric_snapshots ORDER BY 1")]

    @_safe(lambda: 0)
    def prune_snapshots(self, before_ts: float) -> int:
        with self.db.lock:
            n = self.db.conn.execute("DELETE FROM metric_snapshots WHERE ts<?", (before_ts,)).rowcount
            self.db.conn.commit()
        return n

    @_safe(lambda: [])
    def baseline_samples(self, service: str, every: float, max_samples: int, since_ts: float = 0.0,
                         min_events: int = 0, warmup_ceiling: float = 1.0) -> list[float]:
        """Rebuild a baseline after restart from snapshots the live detector would have admitted: NORMAL windows,
        plus WARMUP windows that were populated and under the warm-up ceiling - never anything taken while an alert
        was open - thinned to the sampling cadence, most recent `max_samples`."""
        with self.db.lock:
            ivals = self.db.conn.execute(
                "SELECT created_at, COALESCE(resolved_at, 1e18) FROM alerts WHERE service=?", (service,)).fetchall()
            rows = self.db.conn.execute(
                "SELECT ts, error_rate FROM metric_snapshots WHERE service=? AND ts>=? AND total>=?"
                " AND (state='NORMAL' OR (state='WARMUP' AND error_rate<=?)) ORDER BY ts",
                (service, since_ts, min_events, warmup_ceiling)).fetchall()
        picked: list[float] = []
        last = float("-inf")
        for ts, rate in rows:
            if any(a <= ts <= b for a, b in ivals) or ts - last < every:
                continue
            picked.append(rate)
            last = ts
        return picked[-max_samples:]

    # ---- alerts ----------------------------------------------------------------------------------
    @_safe(lambda: None)
    def upsert_alert(self, a: Alert) -> None:
        d = asdict(a)
        cols = ",".join(_ALERT_COLS)
        marks = ",".join("?" for _ in _ALERT_COLS)
        updates = ",".join(f"{c}=excluded.{c}" for c in _ALERT_COLS if c != "id")
        with self.db.lock:
            self.db.conn.execute(
                f"INSERT INTO alerts({cols}) VALUES ({marks}) ON CONFLICT(id) DO UPDATE SET {updates}",
                [d[c] for c in _ALERT_COLS])
            self.db.conn.commit()

    @_safe(lambda: None)
    def get_alert(self, alert_id: str) -> Alert | None:
        with self.db.lock:
            row = self.db.conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
        return _alert(row) if row else None

    @_safe(lambda: [])
    def list_alerts(self, limit: int = 50, status: str | None = None) -> list[Alert]:
        q, args = "SELECT * FROM alerts", []
        if status:
            q += " WHERE status=?"
            args.append(status)
        q += " ORDER BY created_at DESC LIMIT ?"
        args.append(limit)
        with self.db.lock:
            return [_alert(r) for r in self.db.conn.execute(q, args)]

    def open_alerts(self) -> list[Alert]:
        return self.list_alerts(limit=1000, status="OPEN")   # (not wrapped: a nested success must not clear the error flag)

    # ---- deliveries ------------------------------------------------------------------------------
    @_safe(lambda: None)
    def add_delivery(self, alert_id: str, event: str, channel: str) -> int:
        with self.db.lock:
            cur = self.db.conn.execute(
                "INSERT INTO alert_deliveries(alert_id,event,channel,status,attempt_count) VALUES (?,?,?,'PENDING',0)",
                (alert_id, event, channel))
            self.db.conn.commit()
            return cur.lastrowid

    @_safe(lambda: None)
    def update_delivery(self, delivery_id: int, status: str, attempt_count: int,
                        last_attempt_at: float | None, error_message: str | None,
                        external_id: str | None = None) -> None:
        """`external_id` (SNS MessageId, CloudWatch group:stream) is only ever set, never cleared by a later retry."""
        with self.db.lock:
            self.db.conn.execute(
                "UPDATE alert_deliveries SET status=?, attempt_count=?, last_attempt_at=?, error_message=?,"
                " external_id=COALESCE(?, external_id) WHERE id=?",
                (status, attempt_count, last_attempt_at, error_message, external_id, delivery_id))
            self.db.conn.commit()

    @_safe(lambda: [])
    def deliveries_for(self, alert_id: str) -> list[dict]:
        with self.db.lock:
            rows = self.db.conn.execute(
                "SELECT * FROM alert_deliveries WHERE alert_id=? ORDER BY id", (alert_id,)).fetchall()
        return [dict(r) for r in rows]

    @_safe(lambda: [])
    def pending_deliveries(self) -> list[dict]:
        with self.db.lock:
            rows = self.db.conn.execute("SELECT * FROM alert_deliveries WHERE status='PENDING' ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    # ---- checkpoint ------------------------------------------------------------------------------
    @_safe(lambda: None)
    def save_checkpoint(self, source: str, inode: int, offset: int, updated_at: float) -> None:
        with self.db.lock:
            self.db.conn.execute(
                "INSERT INTO checkpoints(source,inode,offset,updated_at) VALUES (?,?,?,?)"
                " ON CONFLICT(source) DO UPDATE SET inode=excluded.inode, offset=excluded.offset, updated_at=excluded.updated_at",
                (source, inode, offset, updated_at))
            self.db.conn.commit()

    @_safe(lambda: None)
    def load_checkpoint(self, source: str) -> tuple[int, int] | None:
        with self.db.lock:
            row = self.db.conn.execute("SELECT inode, offset FROM checkpoints WHERE source=?", (source,)).fetchone()
        return (row["inode"], row["offset"]) if row else None

    @_safe(lambda: False)
    def ping(self) -> bool:
        with self.db.lock:
            self.db.conn.execute("SELECT 1").fetchone()
        return True
