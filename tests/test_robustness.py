"""Failure-path and edge-case tests: the branches that only run when something goes wrong (rotation, cancelled sends,
dead WebSocket clients, a crashing tick, level shift, parser edge cases). Written from the coverage report."""
import asyncio
import importlib.util
import os
from dataclasses import replace
from pathlib import Path

import pytest

from app.alerts import build_sinks
from app.alerts.dispatcher import Delivery, Dispatcher
from app.alerts.manager import AlertManager
from app.api.websocket import Hub
from app.config import load_settings
from app.detection.state import AlertStateMachine
from app.generator import LogGenerator, ScenarioController
from app.ingestion import tailer as tailer_module
from app.ingestion.parser import ParseError, parse_line
from app.ingestion.tailer import Tailer
from app.main import Runtime
from app.replay import Rec, ReplaySpec, Replayer, dataset_bounds, find_offset, simulate
from app.ingestion.clf import ClfParser
from app.storage.db import Database
from app.storage.repository import Repository

from conftest import RecordingSink, sample_alert, snap
from test_replay import T0, clf, fake_time, write_dataset

ROOT = Path(__file__).resolve().parent.parent


# ---- NDJSON timestamps ---------------------------------------------------------------------------------
def test_ndjson_accepts_numeric_and_naive_timestamps_and_rejects_the_rest():
    base = '{"service":"a","level":"info","timestamp":%s}'
    assert parse_line(base % "1790000000.5").ts == 1790000000.5                       # epoch number
    assert parse_line(base % '"2026-01-01T00:00:00"').ts == 1767225600.0             # naive ISO -> UTC
    assert parse_line(base % '"2026-01-01T05:30:00+05:30"').ts == 1767225600.0       # explicit zone honoured
    for bad in ("true", "[1]", '"not a time"', "{}"):
        with pytest.raises(ParseError):
            parse_line(base % bad)


# ---- tailer: rotation branches (the real rename test cannot run on Windows) ----------------------------
def test_tailer_rejects_an_unknown_start_mode(tmp_path):
    with pytest.raises(ValueError):
        Tailer(str(tmp_path / "x"), start_at="middle")


def test_a_changed_inode_is_treated_as_rotation_and_read_from_zero(tmp_path, monkeypatch):
    p = tmp_path / "app.log"
    p.write_text("old1\nold2\n")
    t = Tailer(str(p), start_at="checkpoint")
    assert t.read_available() == ["old1", "old2"]
    real_stat, fired = os.stat, []

    class Rotated:                                            # the path now points at a different file
        st_ino, st_size = t.inode + 7, 100

    def fake_stat(path, *a, **kw):
        if str(path) == str(p) and not fired:
            fired.append(1)
            return Rotated
        return real_stat(path, *a, **kw)
    monkeypatch.setattr(tailer_module.os, "stat", fake_stat)
    assert t.read_available() == ["old1", "old2"]            # reopened at byte 0 (here: the same bytes stand in for the new file)
    assert t.rotations == 1 and t.offset == p.stat().st_size
    t.close()


def test_a_missing_replacement_keeps_the_old_handle_until_it_appears(tmp_path, monkeypatch):
    p = tmp_path / "app.log"
    p.write_text("a\n")
    t = Tailer(str(p), start_at="checkpoint")
    assert t.read_available() == ["a"]
    real_stat = os.stat

    def gone(path, *a, **kw):
        if str(path) == str(p):
            raise FileNotFoundError(path)                     # rotated away, new file not created yet
        return real_stat(path, *a, **kw)
    monkeypatch.setattr(tailer_module.os, "stat", gone)
    assert t.read_available() == [] and t.rotations == 0
    with open(p, "a") as f:
        f.write("b\n")                                        # the old handle is still readable
    assert t.read_available() == ["b"]
    t.close()


async def test_tailer_run_survives_a_transient_os_error_and_yields_to_the_loop(tmp_path):
    p = tmp_path / "app.log"
    p.write_text("".join(f"line{i}\n" for i in range(1200)))
    t = Tailer(str(p), start_at="checkpoint", poll_seconds=0.001)
    real, calls = t.read_available, []

    def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise OSError("share violation")                  # e.g. the writer briefly holds the file on Windows
        return real()
    t.read_available = flaky
    got = []

    async def sink(line):
        got.append(line)
    task = asyncio.create_task(t.run(sink))
    for _ in range(100):
        await asyncio.sleep(0.01)
        if len(got) == 1200:
            break
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert len(got) == 1200 and got[0] == "line0" and t.inode is None      # retried, read all (500-line yields), closed on cancel


# ---- dispatcher ----------------------------------------------------------------------------------------
@pytest.fixture
def repo():
    r = Repository(Database(":memory:"))
    yield r
    r.db.close()


def seeded(repo, channel):
    alert = sample_alert()
    repo.upsert_alert(alert)
    return Delivery(repo.add_delivery(alert.id, "created", channel), alert, "created", channel)


async def test_a_failing_dashboard_push_never_breaks_delivery_bookkeeping(repo):
    async def broken_push(alert_id):
        raise RuntimeError("websocket exploded")
    sink = RecordingSink("s")
    d = Dispatcher({"s": sink}, repo, retry_attempts=1, backoff_seconds=0, on_update=broken_push)
    task = asyncio.create_task(d.run())
    d.enqueue(seeded(repo, "s"))
    await asyncio.wait_for(d.drain(), 5)
    task.cancel()
    assert repo.deliveries_for("a8f31")[0]["status"] == "DELIVERED" and sink.sent


async def test_cancelling_the_worker_cancels_in_flight_deliveries(repo):
    started, cancelled = asyncio.Event(), []

    class Hang:
        name = "hang"

        async def send(self, alert, event):
            started.set()
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.append(1)
                raise
    d = Dispatcher({"hang": Hang()}, repo, retry_attempts=3, backoff_seconds=0)
    task = asyncio.create_task(d.run())
    d.enqueue(seeded(repo, "hang"))
    await asyncio.wait_for(started.wait(), 5)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await asyncio.sleep(0.05)
    assert cancelled == [1]                                   # shutdown does not leave sends running (or retrying)


# ---- alert manager -------------------------------------------------------------------------------------
async def test_level_shift_calls_back_so_the_baseline_can_relearn(settings, repo):
    called = []

    async def broadcast(t, d):
        pass
    dispatcher = Dispatcher({}, repo)
    machine = AlertStateMachine(settings.detector, settings.profile)
    manager = AlertManager(machine, repo, dispatcher, broadcast, settings.profile.window_seconds,
                           on_level_shift=called.append)
    crit = dict(sev="CRITICAL", z=9, ratio=9, rate=0.4)
    await manager.process([snap(ts=0, service="svc", **crit)])
    await manager.process([snap(ts=settings.profile.level_shift_seconds + 1, service="svc", **crit)])
    assert called == ["svc"] and not machine.is_open("svc")


def test_restore_skips_pending_deliveries_whose_alert_is_gone(settings, repo):
    repo.add_delivery("ghost-alert", "created", "ntfy")       # e.g. the alert row was pruned
    dispatcher = Dispatcher({"ntfy": RecordingSink("ntfy")}, repo)

    async def broadcast(t, d):
        pass
    AlertManager(AlertStateMachine(settings.detector, settings.profile), repo, dispatcher, broadcast, 10).restore()
    assert dispatcher.backlog == 0


# ---- WebSocket hub -------------------------------------------------------------------------------------
class FakeSocket:
    def __init__(self, fail_send=False, fail_receive=None):
        self.fail_send, self.fail_receive, self.sent = fail_send, fail_receive, []

    async def accept(self):
        pass

    async def send_text(self, text):
        if self.fail_send:
            raise RuntimeError("client went away")
        self.sent.append(text)

    async def receive_text(self):
        if self.fail_receive:
            raise self.fail_receive
        await asyncio.sleep(3600)


async def test_a_dead_dashboard_is_dropped_and_the_others_keep_receiving():
    hub = Hub(lambda: {})
    good, dead = FakeSocket(), FakeSocket(fail_send=True)
    hub._clients.update({good, dead})
    await hub.broadcast("metric.update", {"x": 1})
    assert hub.client_count == 1 and good.sent == ['{"type": "metric.update", "data": {"x": 1}}']
    await Hub(lambda: {}).broadcast("x", {})                  # no clients: a no-op


async def test_an_unexpected_socket_error_removes_the_client_quietly():
    hub = Hub(lambda: {"hello": True})
    ws = FakeSocket(fail_receive=RuntimeError("protocol error"))
    await hub.handle(ws)                                      # returns instead of raising
    assert hub.client_count == 0 and '"type": "hello"' in ws.sent[0]


# ---- runtime loops -------------------------------------------------------------------------------------
async def test_the_consumer_survives_a_detection_error_on_one_event(make_runtime, clock):
    rt = make_runtime()
    from conftest import make_event
    seen = []
    real = rt.engine.on_event

    def flaky(ev):
        if not seen:
            seen.append("boom")
            raise RuntimeError("bad event")
        return real(ev)
    rt.engine.on_event = flaky
    for i in range(2):
        rt.queue.put_nowait(make_event(clock.now + i, service="svc"))
    task = asyncio.create_task(rt._consume())
    for _ in range(50):
        await asyncio.sleep(0.01)
        if rt.queue.empty() and "svc" in rt.engine.services:
            break
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert seen == ["boom"] and rt.engine.services["svc"].window.total == 1        # first dropped, second processed


async def test_the_tick_loop_survives_a_crashing_tick_and_prunes_old_snapshots(make_runtime, settings):
    fast = replace(settings, profile=replace(settings.profile, tick_seconds=0.01),
                   storage=replace(settings.storage, prune_every_seconds=0.0))
    rt = make_runtime(base=fast)
    ticks, pruned = [], []

    async def tick_once():
        ticks.append(1)
        if len(ticks) == 1:
            raise RuntimeError("tick blew up")
    rt._tick_once = tick_once
    rt.repo.prune_snapshots = lambda before: pruned.append(before)
    task = asyncio.create_task(rt._tick_loop())
    for _ in range(100):
        await asyncio.sleep(0.01)
        if len(ticks) >= 3 and pruned:
            break
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert len(ticks) >= 3 and pruned                          # kept ticking after the exception, and pruned


async def test_runtime_builds_its_own_sinks_and_launches_the_aws_startup_check(tmp_path, settings, monkeypatch):
    s = replace(settings, log_path=str(tmp_path / "app.log"), db_path=str(tmp_path / "lp.db"), alerts_jsonl=str(tmp_path / "a.jsonl"),
                ingestion=replace(settings.ingestion, start_at="checkpoint"))
    rt = Runtime(s)                                            # sinks=None -> build_sinks(settings)
    assert set(rt.dispatcher.sinks) == {"console", "jsonl"}
    rt.repo.db.close()

    checked = []

    async def fake_check(sinks, record):
        checked.append(sorted(sinks))
    monkeypatch.setattr("app.main.aws_startup_check", fake_check)
    rt2 = Runtime(replace(s, db_path=str(tmp_path / "lp2.db")), sinks={"sns": RecordingSink("sns"), "ntfy": RecordingSink("ntfy")})
    rt2.start()
    await asyncio.sleep(0.05)
    await rt2.stop()
    assert checked == [["ntfy", "sns"]]                        # background, fail-soft AWS check is wired in


# ---- sink registry -------------------------------------------------------------------------------------
def test_build_sinks_reports_every_kind_of_skip(tmp_path, monkeypatch):
    text = Path("config.yaml").read_text(encoding="utf-8").replace(
        "sinks: [console, jsonl, sns, cloudwatch, ntfy, telegram]", "sinks: [console, jsonl, webhook, cloudwatch, bogus]")
    cfg = tmp_path / "c.yaml"
    cfg.write_text(text, encoding="utf-8")
    env = {"ALERTS_JSONL": str(tmp_path / "a.jsonl"), "AWS_ENABLED": "true", "CW_LOG_GROUP": ""}
    sinks, skipped = build_sinks(load_settings(cfg, env=env))
    assert set(sinks) == {"console", "jsonl"}
    assert "WEBHOOK_URL" in skipped["webhook"] and "CW_LOG_GROUP" in skipped["cloudwatch"] and skipped["bogus"] == "unknown sink"

    def boom(self, *a, **kw):
        raise RuntimeError("cannot start")
    monkeypatch.setattr("app.alerts.console.ConsoleSink.__init__", boom)
    sinks, skipped = build_sinks(load_settings(cfg, env=env))
    assert "console" not in sinks and "failed to start" in skipped["console"]      # one bad sink never stops the app


# ---- generator -----------------------------------------------------------------------------------------
async def test_the_generator_stays_silent_while_paused_and_resumes(tmp_path, settings):
    ctl = ScenarioController(settings.generator)
    assert ctl.timeline_end == float(settings.generator.mixed_timeline[-1][0])
    gen, path, stop = LogGenerator(settings.generator, ctl, seed=1), tmp_path / "gen.log", asyncio.Event()
    ctl.paused = True
    task = asyncio.create_task(gen.run(str(path), step=0.02, stop=stop))
    await asyncio.sleep(0.25)
    assert not path.exists() or path.read_text() == ""       # a replay owns the file: nothing is appended
    ctl.paused = False
    await asyncio.sleep(0.25)
    stop.set()
    await task
    assert path.read_text().count("\n") > 0


# ---- replay: file edge cases and level shift -----------------------------------------------------------
def test_files_without_usable_timestamps_are_handled(tmp_path):
    junk = tmp_path / "junk.log"
    junk.write_text("\n".join(f"garbage {i}" for i in range(120)) + "\n")     # more than the 50-line probe
    assert find_offset(junk, T0) == junk.stat().st_size
    with pytest.raises(ValueError, match="no timestamps"):
        dataset_bounds(junk)


async def test_a_large_segment_is_written_in_batches(tmp_path):
    ds, out = tmp_path / "ds", tmp_path / "app.log"
    write_dataset(ds, n=1300)
    clock, sleep = fake_time()
    rep = Replayer(ds, ReplaySpec(speed=1e6, seed=1), ClfParser(load_settings(env={}).mapping), clock, sleep)
    await rep.run(out)
    assert rep.progress.lines_sent == 1300 and len(out.read_bytes().split(b"\n")) == 1301


def test_simulate_resolves_a_sustained_change_as_a_level_shift_and_counts_bad_lines(nasa_settings):
    """40% errors that never go away must not leave an alert open forever: it is closed as a level shift, the baseline
    relearns the new level, and no second alert fires."""
    s = replace(nasa_settings, profile=replace(nasa_settings.profile, level_shift_seconds=30))
    recs = []
    for sec in range(0, 220):
        bad = sec >= 60                                           # calm for a minute, then 40 % 404s for good
        for i in range(6):
            recs.append(Rec(T0 + sec, clf(T0 + sec, "/history/x.html", 404 if bad and i < 3 else 200, host=f"h{sec}-{i}")))
    recs.insert(100, Rec(T0 + 40, "not a log line"))
    res = simulate(recs, s, speed=1, max_gap=15, seed=1)
    assert res.parse_errors == 1 and len(res.alerts) == 1
    a = res.alerts[0]
    assert a.service == "history" and a.level_shift and a.virt_end - a.virt_start >= 30 and a.rate >= 0.3


# ---- scripts -------------------------------------------------------------------------------------------
def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_benchmark_script_reports_measured_numbers(monkeypatch, capsys):
    mod = load_script("benchmark")
    monkeypatch.setattr("sys.argv", ["benchmark.py", "--sizes", "400"])
    mod.main()
    out = capsys.readouterr().out
    assert "core ev/s" in out and "pipeline ev/s" in out and "     400 |" in out


def test_generate_logs_script_appends_ndjson(monkeypatch, tmp_path, capsys):
    mod = load_script("generate_logs")
    out = tmp_path / "gen.log"
    monkeypatch.setattr("sys.argv", ["generate_logs.py", "--scenario", "normal", "--duration", "0.6", "--seed", "1", "--path", str(out)])
    mod.main()
    lines = [ln for ln in out.read_text().split("\n") if ln]
    assert lines and all(parse_line(ln).service in {"payment-service", "auth-service"} for ln in lines)
    assert "[generator] normal" in capsys.readouterr().out


def test_the_requirements_verification_script_passes_all_eight(capsys):
    """scripts/verify_requirements.py runs every problem-statement requirement against the real code (AWS via moto)."""
    mod = load_script("verify_requirements")
    assert mod.main() == 0
    out = capsys.readouterr().out
    assert out.count("[PASS]") == 8 and "[FAIL]" not in out and "8/8 requirements verified" in out
