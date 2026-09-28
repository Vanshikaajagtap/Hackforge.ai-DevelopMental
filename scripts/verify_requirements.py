"""Run each of the problem statement's minimum requirements against the REAL code and print PASS / FAIL with evidence.

    python scripts/verify_requirements.py

Everything runs offline in a few seconds on a virtual clock. Requirement 8 (push to SNS / CloudWatch Logs) is verified against
`moto`, an in-process AWS mock (pip install -r requirements-dev.txt): it proves the sinks, retry path and payloads work, NOT that
your AWS account is configured - for that, follow the "AWS setup" checklist in README.md. Exit code 0 only if every check passes.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import tempfile
import warnings
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")           # moto only; never real credentials
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "ap-south-1")
warnings.filterwarnings("ignore")
logging.disable(logging.INFO)                                   # keep the report readable

from app.config import load_settings  # noqa: E402
from app.detection.detector import DetectionEngine  # noqa: E402
from app.detection.severity import classify  # noqa: E402
from app.detection.window import SlidingWindow  # noqa: E402
from app.ingestion.models import LogEvent  # noqa: E402
from app.ingestion.tailer import Tailer  # noqa: E402


class Clock:
    """A clock that only moves when told to, so every run is deterministic."""

    def __init__(self, start: float = 1_800_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def event(ts: float, err: bool, service: str = "payment-service") -> LogEvent:
    return LogEvent(ts=ts, service=service, level="ERROR" if err else "INFO", status=500 if err else 200,
                    message="m", request_id=None, is_error=err)


def line(ts: float, err: bool, service: str = "payment-service") -> str:
    return json.dumps({"timestamp": datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="milliseconds"),
                       "service": service, "level": "ERROR" if err else "INFO", "status": 500 if err else 200,
                       "message": "m", "request_id": "r"})


def feed(det, clock, seconds: int, per_sec: int, errors: int) -> list:
    snaps = []
    for _ in range(seconds):
        for i in range(per_sec):
            det.on_event(event(clock.now, i < errors))
        clock.advance(1)
        snaps.append(det.tick())
    return snaps


# ---- 1. monitor a continuously growing log file ----------------------------------------------------------
def r1_growing_file() -> str:
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "app.log"
        path.write_text("")
        tailer = Tailer(str(path), start_at="checkpoint")
        got: list[str] = []
        with open(path, "a", newline="\n") as f:
            f.write('{"partial": ')                                    # a write cut mid-line
            f.flush()
            assert tailer.read_available() == [], "a partial line must wait for its newline"
            f.write("true}\n")
            f.flush()
            got += tailer.read_available()
            for i in range(1000):                                      # the file keeps growing while we tail it
                f.write(json.dumps({"i": i}) + "\n")
                f.flush()
                if i % 100 == 99:
                    got += tailer.read_available()
        offset = tailer.offset
        tailer.close()
        assert len(got) == 1001 and got[0] == '{"partial": true}', len(got)
        resumed = Tailer(str(path), start_at="end", resume=__import__("app.ingestion.tailer", fromlist=["Checkpoint"]).Checkpoint(os.stat(path).st_ino, offset))
        assert resumed.read_available() == [], "resuming from the saved offset must not replay"
        resumed.close()
    return f"1,001 lines read while the file grew; partial line held until its newline; resume at offset {offset:,} replays nothing"


# ---- 2. rolling error rates over a sliding window ---------------------------------------------------------
def r2_sliding_window() -> str:
    w = SlidingWindow(10)
    for i in range(100):
        w.add(1000.0, i < 50, now=1000.0)
    assert w.error_rate == 0.5
    w.add(1004.0, False, now=1004.0)
    w.evict(now=1010.0)                                                # the t=1000 events are exactly 10 s old: still inside
    assert w.total == 101
    w.evict(now=1010.001)                                              # just past the window: gone
    assert (w.total, w.errors) == (1, 0) and w.error_rate == 0.0
    w.evict(now=1100.0)
    assert w.total == 0 and w.error_rate == 0.0                        # a silent service decays instead of freezing
    return "50/100 errors -> 50.0 %; events leave the window on the clock (boundary inclusive); silence decays to 0"


# ---- 3. establish a baseline for normal behaviour ---------------------------------------------------------
def r3_baseline() -> str:
    s = load_settings(env={})
    clock = Clock()
    eng = DetectionEngine(s, clock)
    det = eng.detector("payment-service")
    snaps = feed(det, clock, 30, 20, 1)                                # ~5 % errors, ~200 events per 10 s window
    assert snaps[0].state == "WARMUP" and snaps[-1].state == "NORMAL" and det.baseline.ready
    mean, n = det.baseline.mean, det.baseline.n
    assert abs(mean - 0.05) < 0.005
    feed(det, clock, 30, 20, 8)                                        # a 40 % incident must NOT teach the detector it is normal
    assert det.baseline.n == n and abs(det.baseline.mean - mean) < 0.02
    return f"WARMUP -> NORMAL after {n} samples; baseline {mean * 100:.1f} % +/- {det.baseline.std * 100:.1f} pp; a 40 % incident left it at {det.baseline.mean * 100:.1f} % (contamination guard)"


# ---- 4. detect deviations from the baseline ---------------------------------------------------------------
def r4_deviation() -> str:
    s = load_settings(env={})
    clock = Clock()
    det = DetectionEngine(s, clock).detector("payment-service")
    feed(det, clock, 30, 20, 1)
    surge = feed(det, clock, 15, 100, 5)                               # 5x volume, same 5 % error rate
    assert {x.state for x in surge} == {"NORMAL"}, "a traffic-only surge must not be an anomaly"
    feed(det, clock, 15, 20, 1)
    incident = feed(det, clock, 12, 20, 8)                             # error rate 5 % -> 40 %
    last = incident[-1]
    assert last.state == "ANOMALY" and last.z > 4 and last.error_rate > 0.3
    return f"5x traffic surge at 5 % errors: NORMAL; 5 % -> 40 % errors: ANOMALY (rate {last.error_rate * 100:.0f} %, z={last.z:.1f}, x{last.ratio:.1f} baseline)"


# ---- 5. assign severity levels ----------------------------------------------------------------------------
def r5_severity() -> str:
    s = load_settings(env={})
    got = {name: classify(z, rate, ratio, errors, s.severity) for name, (z, rate, ratio, errors) in {
        "z2.1": (2.1, 0.12, 2.0, 8), "z3.1": (3.1, 0.18, 3.5, 12), "z4.1": (4.1, 0.30, 6.0, 20),
        "tiny-sigma": (9.0, 0.04, 1.4, 20), "few-errors": (2.1, 0.12, 2.0, 4)}.items()}
    assert got == {"z2.1": "MEDIUM", "z3.1": "HIGH", "z4.1": "CRITICAL", "tiny-sigma": "NONE", "few-errors": "NONE"}, got
    clock = Clock()
    det = DetectionEngine(s, clock).detector("payment-service")
    feed(det, clock, 30, 100, 5)
    seen = []
    for errors in (12, 20, 35, 45):                                    # ramp: 12 % -> 20 % -> 35 % -> 45 %
        seen += [x.severity for x in feed(det, clock, 12, 100, errors)]
    order = [v for v in dict.fromkeys(seen) if v != "NONE"]
    assert order[-1] == "CRITICAL" and {"MEDIUM", "HIGH"} <= set(order), order        # it must climb through every level
    return f"gate table: z 2.1/3.1/4.1 -> MEDIUM/HIGH/CRITICAL, tiny-sigma wobble and too-few-errors -> NONE; live ramp escalated {' -> '.join(order)}"


# ---- 6 + 7. real-time WebSocket frontend that shows alerts as they are generated --------------------------
async def _live_pipeline() -> tuple[int, float, dict]:
    """Append lines to a real file; the real tailer/parser/detector/manager/hub run on a virtual clock."""
    from app.main import Runtime

    class Socket:                                                      # stands in for a browser on /ws
        def __init__(self) -> None:
            self.msgs: list[tuple[float, dict]] = []

        async def send_text(self, text: str) -> None:
            self.msgs.append((clock.now, json.loads(text)))

    clock = Clock()
    with tempfile.TemporaryDirectory() as d:
        s = replace(load_settings(env={}), log_path=str(Path(d) / "app.log"), db_path=":memory:", alerts_jsonl=str(Path(d) / "a.jsonl"),
                    ingestion=replace(load_settings(env={}).ingestion, start_at="checkpoint"))
        rt = Runtime(s, clock=clock, sinks={})
        sock = Socket()
        rt.hub._clients.add(sock)
        first_anomaly = None
        for phase, (secs, errs) in enumerate(((30, 1), (15, 8))):      # 5 % then 40 % errors
            for _ in range(secs):
                with open(s.log_path, "a", newline="\n") as f:
                    for i in range(20):
                        f.write(line(clock.now, i < errs) + "\n")
                for raw in rt.tailer.read_available():
                    await rt.ingest_line(raw)
                while not rt.queue.empty():
                    rt.engine.on_event(rt.queue.get_nowait())
                clock.advance(1)
                await rt._tick_once()
                if phase == 1 and first_anomaly is None and rt.latest["payment-service"].state == "ANOMALY":
                    first_anomaly = clock.now
        types = {}
        for _, m in sock.msgs:
            types[m["type"]] = types.get(m["type"], 0) + 1
        created = [t for t, m in sock.msgs if m["type"] == "alert.created"]
        assert created and first_anomaly is not None
        latency = created[0] - first_anomaly
        rt.tailer.close()
        rt.repo.db.close()
        return len(sock.msgs), latency, types


def r6_websocket() -> str:
    from fastapi.testclient import TestClient

    from app.main import create_app
    with tempfile.TemporaryDirectory() as d:
        base = load_settings(env={})
        s = replace(base, demo_mode=True, log_path=str(Path(d) / "app.log"), db_path=str(Path(d) / "lp.db"),
                    alerts_jsonl=str(Path(d) / "a.jsonl"))
        with TestClient(create_app(s, sinks={})) as client, client.websocket_connect("/ws") as ws:
            hello = ws.receive_json()
            assert hello["type"] == "hello" and {"config", "snapshots", "history", "alerts", "health"} <= set(hello["data"])
            seen = set()
            for _ in range(40):                                        # the real generator -> file -> tailer -> detector, pushed live
                m = ws.receive_json()
                seen.add(m["type"])
                if {"metric.update", "health.update"} <= seen:
                    break
            assert {"metric.update", "health.update"} <= seen, seen
            page = client.get("/").text
    assert "new WebSocket(" in page and "metric.update" in page and "alert.created" in page
    return f"dashboard connects to /ws: `hello` on connect, then live {', '.join(sorted(seen))}; the page's JS handles metric.update + alert.* messages"


def r7_alerts_displayed() -> str:
    n, latency, types = asyncio.run(_live_pipeline())
    assert latency <= 5, latency
    return (f"file -> tailer -> detector -> WebSocket: alert.created arrived {latency:.0f} simulated s after the first ANOMALY snapshot "
            f"(confirm_ticks=2); {n} messages pushed ({', '.join(f'{k}:{v}' for k, v in sorted(types.items()))})")


# ---- 8. push alerts to AWS CloudWatch Logs or SNS (against moto, an in-process AWS mock) -------------------
async def _push_to_aws() -> str:
    import boto3
    from moto import mock_aws

    from app.alerts.cloudwatch import CloudWatchSink
    from app.alerts.dispatcher import Dispatcher
    from app.alerts.manager import AlertManager
    from app.alerts.sns import SnsSink
    from app.detection.models import Snapshot
    from app.detection.state import AlertStateMachine
    from app.storage.db import Database
    from app.storage.repository import Repository

    region, group = "ap-south-1", "/logpulse/alerts"
    with mock_aws():
        arn = boto3.client("sns", region_name=region).create_topic(Name="logpulse-alerts")["TopicArn"]
        logs = boto3.client("logs", region_name=region)
        logs.create_log_group(logGroupName=group)
        s = load_settings(env={})
        repo = Repository(Database(":memory:"))
        dispatcher = Dispatcher({"sns": SnsSink(arn, region, s.profile.window_seconds),
                                 "cloudwatch": CloudWatchSink(group, "alerts", region)}, repo, retry_attempts=2, backoff_seconds=0)

        async def broadcast(_t, _d):
            return None
        manager = AlertManager(AlertStateMachine(s.detector, s.profile), repo, dispatcher, broadcast, s.profile.window_seconds)
        task = asyncio.create_task(dispatcher.run())
        snap = Snapshot(ts=1_800_000_000.0, service="payment-service", total=284, errors=109, error_rate=0.384, baseline_mean=0.051,
                        baseline_std=0.012, z=5.18, ratio=7.5, state="ANOMALY", severity="CRITICAL")
        await manager.process([snap])
        await asyncio.wait_for(dispatcher.drain(), 10)
        task.cancel()
        alert_id = repo.list_alerts()[0].id
        rows = {r["channel"]: r for r in repo.deliveries_for(alert_id)}
        assert rows["sns"]["status"] == rows["cloudwatch"]["status"] == "DELIVERED", rows
        stream = rows["cloudwatch"]["external_id"].split(":", 1)[1]
        events = logs.get_log_events(logGroupName=group, logStreamName=stream)["events"]
        body = json.loads(events[0]["message"])
        assert body["severity"] == "CRITICAL" and body["service"] == "payment-service" and "z_score" in body
        repo.db.close()
        return f"CRITICAL alert -> SNS MessageId {rows['sns']['external_id'][:8]}... and CloudWatch event in {rows['cloudwatch']['external_id']} (JSON keys: {', '.join(sorted(body)[:5])}...)"


def r8_push_to_aws() -> str:
    return asyncio.run(_push_to_aws()) + "  [moto mock - not your AWS account]"


CHECKS = [
    ("1", "Monitor a continuously growing log file", r1_growing_file),
    ("2", "Rolling error rates over a sliding window", r2_sliding_window),
    ("3", "Establish a baseline for normal behaviour", r3_baseline),
    ("4", "Detect deviations from the baseline", r4_deviation),
    ("5", "Assign severity levels to anomalies", r5_severity),
    ("6", "Real-time frontend (WebSockets)", r6_websocket),
    ("7", "Display alerts as they are generated", r7_alerts_displayed),
    ("8", "Push alerts to AWS CloudWatch Logs / SNS", r8_push_to_aws),
]


def main() -> int:
    """Run every check, print a report, return the process exit code."""
    failed = 0
    for num, name, fn in CHECKS:
        try:
            evidence = fn()
            print(f"[PASS] R{num} {name}\n         {evidence}")
        except Exception as e:  # noqa: BLE001 - report every requirement, not just the first failure
            failed += 1
            print(f"[FAIL] R{num} {name}\n         {type(e).__name__}: {e}")
    print(f"\n{len(CHECKS) - failed}/{len(CHECKS)} requirements verified" + ("" if not failed else " - FAILURES ABOVE"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
