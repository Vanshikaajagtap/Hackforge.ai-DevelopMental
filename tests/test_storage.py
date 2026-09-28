from dataclasses import replace

import pytest

from app.storage.db import Database
from app.storage.repository import Repository

from conftest import sample_alert, snap


@pytest.fixture
def repo():
    r = Repository(Database(":memory:"))
    yield r
    r.db.close()


def test_schema_matches_the_prd_tables(repo):
    tables = {r[0] for r in repo.db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"metric_snapshots", "alerts", "alert_deliveries", "checkpoints"} <= tables
    cols = {r[1] for r in repo.db.conn.execute("PRAGMA table_info(alerts)")}
    assert {"anomaly_type", "peak_severity", "created_at", "resolved_at", "sample_size", "reason"} <= cols


def test_wal_mode_on_a_file_database(tmp_path):
    db = Database(str(tmp_path / "x.db"))
    assert db.conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    db.close()


def test_alert_roundtrip_upsert_and_listing(repo):
    a = sample_alert()
    repo.upsert_alert(a)
    repo.upsert_alert(replace(a, severity="HIGH", status="RESOLVED", resolved_at=1_790_000_090.0))   # update in place
    got = repo.get_alert("a8f31")
    assert (got.status, got.severity, got.peak_severity, got.resolved_at) == ("RESOLVED", "HIGH", "CRITICAL", 1_790_000_090.0)
    repo.upsert_alert(sample_alert(id="b2", created_at=1_790_000_500.0))
    assert [x.id for x in repo.list_alerts()] == ["b2", "a8f31"]                        # newest first
    assert [x.id for x in repo.open_alerts()] == ["b2"]


def test_delivery_lifecycle_rows(repo):
    repo.upsert_alert(sample_alert())
    d = repo.add_delivery("a8f31", "created", "ntfy")
    assert repo.pending_deliveries()[0]["id"] == d
    repo.update_delivery(d, "PENDING", 1, 5.0, "boom")
    assert repo.deliveries_for("a8f31")[0]["attempt_count"] == 1
    repo.update_delivery(d, "DELIVERED", 2, 6.0, None)
    assert repo.pending_deliveries() == []


def test_checkpoint_roundtrip_overwrites(repo):
    assert repo.load_checkpoint("/data/app.log") is None
    repo.save_checkpoint("/data/app.log", 42, 100, 1.0)
    repo.save_checkpoint("/data/app.log", 42, 250, 2.0)
    assert repo.load_checkpoint("/data/app.log") == (42, 250)


def test_snapshot_history_and_pruning(repo):
    repo.save_snapshots([snap(ts=t) for t in (100.0, 200.0, 300.0)])
    assert [r["ts"] for r in repo.history("svc", 150.0)] == [200.0, 300.0]
    assert repo.services() == ["svc"]
    assert repo.prune_snapshots(250.0) == 2 and len(repo.history("svc", 0)) == 1


def test_baseline_samples_are_thinned_normal_only_and_skip_open_alert_periods(repo):
    rows = [snap(ts=float(t), rate=0.05) for t in range(0, 40)]                            # NORMAL every second
    rows += [snap(ts=float(t), sev="HIGH", z=4.0, ratio=4.0, rate=0.5) for t in range(40, 50)]   # ANOMALY rows
    rows += [snap(ts=float(t), rate=0.9) for t in range(50, 60)]                           # NORMAL but during an open alert
    repo.save_snapshots(rows)
    repo.upsert_alert(sample_alert(created_at=45.0, resolved_at=60.0, service="svc"))
    samples = repo.baseline_samples("svc", every=2.0, max_samples=30)
    assert samples and all(r == 0.05 for r in samples)                                     # nothing from the incident
    assert len(samples) == 20                                                              # thinned: 40 s / 2 s cadence
    assert len(repo.baseline_samples("svc", every=2.0, max_samples=5)) == 5


def test_a_dead_database_degrades_instead_of_raising(repo):
    repo.db.close()
    assert repo.save_snapshots([snap()]) is None and repo.history("svc", 0) == [] and repo.open_alerts() == []
    assert repo.status == "error" and repo.last_error


def test_external_id_is_stored_and_never_cleared_by_a_later_update(repo):
    repo.upsert_alert(sample_alert())
    d = repo.add_delivery("a8f31", "created", "sns")
    assert repo.deliveries_for("a8f31")[0]["external_id"] is None
    repo.update_delivery(d, "DELIVERED", 1, 5.0, None, "0123-message-id")
    repo.update_delivery(d, "DELIVERED", 2, 6.0, None)                        # e.g. a bookkeeping rewrite without an id
    row = repo.deliveries_for("a8f31")[0]
    assert row["external_id"] == "0123-message-id" and row["attempt_count"] == 2


def test_existing_database_without_external_id_is_migrated_in_place(tmp_path):
    import sqlite3
    path = str(tmp_path / "old.db")
    old = sqlite3.connect(path)
    old.executescript("""
        CREATE TABLE alert_deliveries (
          id INTEGER PRIMARY KEY AUTOINCREMENT, alert_id TEXT NOT NULL, event TEXT NOT NULL, channel TEXT NOT NULL,
          status TEXT NOT NULL, attempt_count INTEGER DEFAULT 0, last_attempt_at REAL, error_message TEXT);
        INSERT INTO alert_deliveries(alert_id,event,channel,status,attempt_count) VALUES ('old-alert','created','ntfy','DELIVERED',1);
    """)
    old.commit()
    old.close()

    repo = Repository(Database(path))                                          # opens the pre-AWS database
    cols = {r[1] for r in repo.db.conn.execute("PRAGMA table_info(alert_deliveries)")}
    assert "external_id" in cols
    kept = repo.deliveries_for("old-alert")
    assert len(kept) == 1 and kept[0]["status"] == "DELIVERED" and kept[0]["external_id"] is None   # data survives
    repo.update_delivery(kept[0]["id"], "DELIVERED", 1, 1.0, None, "msg-1")
    assert repo.deliveries_for("old-alert")[0]["external_id"] == "msg-1"
    repo.db.close()

    Repository(Database(path)).db.close()                                      # second open is a harmless no-op
