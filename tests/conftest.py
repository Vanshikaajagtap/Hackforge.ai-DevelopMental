from __future__ import annotations

import pytest

from app.config import Settings, load_settings
from app.detection.models import Snapshot
from app.ingestion.models import LogEvent


class FakeClock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture(autouse=True)
def _never_touch_real_aws(monkeypatch):
    for k in ("AWS_PROFILE", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def settings() -> Settings:
    """The real config.yaml, demo profile, no environment - tests exercise the shipped thresholds."""
    return load_settings(env={})


def sample_alert(**over):
    """A populated Alert (the §35 example incident) for sink / dispatcher / storage tests."""
    from app.detection.models import Alert
    base = dict(id="a8f31", dedup_key="payment-service:error_rate", service="payment-service", severity="CRITICAL",
                peak_severity="CRITICAL", status="OPEN", created_at=1_790_000_000.0, resolved_at=None,
                current_rate=0.384, baseline_rate=0.051, z=5.18, ratio=7.5, sample_size=284,
                reason="Error rate 7.5× baseline with z=5.18 over 284 events")
    base.update(over)
    return Alert(**base)


class RecordingSink:
    """Fake sink: remembers (alert_id, event); can be told to fail."""

    def __init__(self, name: str = "fake", fail: bool = False) -> None:
        self.name, self.fail, self.sent = name, fail, []

    async def send(self, alert, event: str) -> None:
        if self.fail:
            raise RuntimeError("sink down")
        self.sent.append((alert.id, event))


class FakeWS:
    """Stands in for a browser connected to the hub."""

    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def send_text(self, text: str) -> None:
        import json
        self.messages.append(json.loads(text))

    def of(self, type_: str) -> list[dict]:
        return [m["data"] for m in self.messages if m["type"] == type_]


@pytest.fixture
def make_runtime(tmp_path, settings, clock):
    """Build a Runtime on a fake clock with fake sinks, a temp log file and a temp (or shared) DB - no real tasks."""
    from dataclasses import replace
    from app.main import Runtime

    def factory(sinks=None, db_path=None, log_path=None, queue_max=None, **env_overrides):
        s = replace(
            settings,
            log_path=str(log_path or tmp_path / "app.log"),
            db_path=str(db_path or ":memory:"),
            alerts_jsonl=str(tmp_path / "alerts.jsonl"),
            ingestion=replace(settings.ingestion, start_at="checkpoint",
                              queue_max=queue_max or settings.ingestion.queue_max),
            **env_overrides,
        )
        rt = Runtime(s, clock=clock, sinks={"fake": RecordingSink()} if sinks is None else sinks)
        rt.ws = FakeWS()
        rt.hub._clients.add(rt.ws)
        return rt

    return factory


def make_event(ts: float, is_error: bool = False, service: str = "svc") -> LogEvent:
    return LogEvent(ts=ts, service=service, level="ERROR" if is_error else "INFO",
                    status=500 if is_error else 200, message="m", request_id=None, is_error=is_error)


def run_seconds(det, clock: FakeClock, seconds: int, events_per_sec: int, errors_per_sec: int, on_tick=None) -> list:
    """Feed a steady stream for N simulated seconds, ticking once per second. Returns the snapshots."""
    snaps = []
    for _ in range(seconds):
        for i in range(events_per_sec):
            det.on_event(make_event(clock.now, is_error=i < errors_per_sec, service=det.service))
        clock.advance(1)
        snap = det.tick()
        snaps.append(snap)
        if on_tick:
            on_tick(snap)
    return snaps


def snap(ts: float = 0.0, service: str = "svc", sev: str = "NONE", z: float | None = 0.0, ratio: float | None = 1.0,
         rate: float = 0.05, total: int = 200, errors: int | None = None, mean: float = 0.05,
         std: float = 0.01) -> Snapshot:
    """Hand-built snapshot for state-machine tests."""
    state = "LOW_DATA" if z is None else ("ANOMALY" if sev != "NONE" else "NORMAL")
    return Snapshot(ts=ts, service=service, total=total, errors=int(rate * total) if errors is None else errors,
                    error_rate=rate, baseline_mean=mean, baseline_std=std, z=z, ratio=ratio, state=state, severity=sev)
