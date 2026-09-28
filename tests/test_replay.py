"""Replayer: timestamp rewrite, gap scaling and capping, seeking, AWS guard, controller/API, and the nasa profile config.
Uses synthetic files and the small real fixtures - never the 200 MB dataset, never real AWS."""
import asyncio
import json
import time
from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from app.alerts.manager import AlertManager
from app.alerts.dispatcher import Dispatcher
from app.config import ReplayCfg, load_settings
from app.detection.detector import DetectionEngine
from app.detection.state import AlertStateMachine
from app.ingestion.clf import ClfParser, format_clf_timestamp, parse_clf_timestamp
from app.ingestion.parser import parse_line
from app.main import create_app
from app.replay import (
    AWS_CHANNELS, Rec, ReplayGuard, ReplaySpec, Replayer, TimelineStats, dataset_bounds, extract_ts, find_offset,
    iter_segment, parse_when, render_clf, render_ndjson, resolve_dataset_path, timeline, write_state_file)
from app.storage.db import Database
from app.storage.repository import Repository

from conftest import FIXTURES, FakeClock, RecordingSink, make_event, snap

T0 = parse_clf_timestamp("01/Jul/1995:00:00:00 -0400")


def clf(ts: float, url="/shuttle/x.html", status=200, host="h1") -> str:
    return f'{host} - - [{format_clf_timestamp(ts, -240)}] "GET {url} HTTP/1.0" {status} 100'


def write_dataset(path, n=200, step=1, junk_at=(), start=T0) -> list[str]:
    lines = []
    for i in range(n):
        if i in junk_at:
            lines.append("garbage \x05\x01 line without timestamp")
        lines.append(clf(start + i * step, host=f"h{i}"))
    path.write_bytes(("\n".join(lines) + "\n").encode("latin-1"))
    return lines


def recs(*ts):
    return [Rec(t, f"line{i}") for i, t in enumerate(ts)]


# =================================================================================================
# timeline: scaled gaps, seeded spread, gap cap
# =================================================================================================
def test_timeline_preserves_inter_arrival_gaps_scaled_by_speed():
    out = list(timeline(recs(100, 110, 130), speed=10, max_gap=99, seed=1))
    offs = [v for v, _ in out]
    assert offs == sorted(offs)
    assert offs[1] - offs[0] == pytest.approx(1.0, abs=0.11)          # 10 original s / 10 = 1 s (+ <0.1 s spread)
    assert offs[2] - offs[1] == pytest.approx(2.0, abs=0.11)          # 20 original s / 10 = 2 s


def test_records_sharing_a_second_are_spread_across_it_and_reproducible():
    same = recs(*([500] * 60))
    a = [v for v, _ in timeline(same, 1, 99, seed=7)]
    b = [v for v, _ in timeline(same, 1, 99, seed=7)]
    c = [v for v, _ in timeline(same, 1, 99, seed=8)]
    assert a == b and a != c                                           # same seed -> identical replay
    assert a == sorted(a) and 0 <= a[0] and a[-1] < 1.0 and len(set(a)) > 50   # spread inside the 1-second slot
    nxt = [v for v, _ in timeline(recs(500, 500, 501), 1, 99, seed=1)]
    assert nxt[2] >= 1.0                                               # the next second starts after the slot


def test_long_silent_gaps_are_capped_and_counted():
    st = TimelineStats()
    out = list(timeline(recs(0, 3600, 3601), speed=60, max_gap=15, seed=1, stats=st))
    offs = [v for v, _ in out]
    assert offs[1] - offs[0] == pytest.approx(15.0, abs=0.05)          # 3600 s / 60 = 60 s, capped to 15
    assert st.gaps_capped == 1 and st.longest_gap_orig == 3600
    uncapped = [v for v, _ in timeline(recs(0, 100), speed=10, max_gap=15, seed=1)]
    assert uncapped[1] - uncapped[0] == pytest.approx(10.0, abs=0.11)  # 10 s < cap: untouched


def test_out_of_order_and_timestampless_lines_never_move_time_backwards():
    items = [Rec(100, "a"), Rec(105, "b"), Rec(103, "late"), Rec(None, "junk"), Rec(106, "c")]
    out = list(timeline(items, speed=1, max_gap=99, seed=1))
    offs = [v for v, _ in out]
    assert offs == sorted(offs) and {r.line for _, r in out} == {"a", "b", "late", "junk", "c"}
    lead = list(timeline([Rec(None, "junk"), Rec(50, "x")], 1, 99, 1))          # junk first: still fine
    assert [r.line for _, r in lead] == ["junk", "x"]


def test_timeline_rejects_a_non_positive_speed():
    with pytest.raises(ValueError):
        list(timeline(recs(1), 0, 1))


def test_a_capped_silence_still_shows_low_data_briefly(nasa_settings):
    """Original: a 1-hour hole. Scaled+capped: 15 s > the 5 s window, so the window empties (LOW_DATA), then traffic resumes."""
    s = replace(nasa_settings, profile=replace(nasa_settings.profile, window_seconds=5, min_events=10, min_baseline_samples=3))
    clock = FakeClock(0.0)
    eng = DetectionEngine(s, clock)
    eng.detector("svc").restore_samples([0.0] * 3)                      # a ready baseline
    items = [Rec(float(t), "x") for t in range(0, 100)] + [Rec(float(t), "x") for t in range(3700, 3800)]
    states, nxt = [], 1.0
    for v, _ in timeline(items, speed=10, max_gap=15, seed=1):
        while nxt <= v:
            clock.now = nxt
            states.append((nxt, eng.tick()[0].state))
            nxt += 1
        clock.now = v
        eng.on_event(make_event(v, service="svc"))
    assert "NORMAL" in {st for _, st in states[:8]}
    assert "LOW_DATA" in {st for t, st in states if 12 < t < 25}       # silence is visible during the (capped) gap
    assert states[-1][1] == "NORMAL"                                    # and traffic recovers afterwards


# =================================================================================================
# seeking a time-sorted file
# =================================================================================================
def test_find_offset_and_iter_segment_on_a_big_synthetic_log(tmp_path):
    path = tmp_path / "log"
    lines = write_dataset(path, n=4000, junk_at={1500, 2500})           # > 64 KB so the binary search really runs
    assert path.stat().st_size > 1 << 17
    ts = lambda i: T0 + i                                                # noqa: E731 - line i has timestamp T0 + i (junk excluded)
    for want in (0, 1, 123, 1999, 3999):
        off = find_offset(path, ts(want))
        with open(path, "rb") as fh:
            fh.seek(off)
            assert extract_ts(fh.readline().decode("latin-1")) == ts(want)
    assert find_offset(path, T0 - 1000) == 0                            # before the file
    assert find_offset(path, T0 + 10**6) == path.stat().st_size         # after it
    seg = list(iter_segment(path, ts(1000), ts(1100)))
    stamps = [r.ts for r in seg if r.ts is not None]
    assert stamps[0] == ts(1000) and stamps[-1] == ts(1099) and len(stamps) == 100      # [start, end)
    spanning = list(iter_segment(path, ts(1490), ts(1510)))
    assert any(r.ts is None for r in spanning)                          # a timestamp-less line inside the window rides along
    assert len(list(iter_segment(path))) == len(lines)                  # whole file


def test_dataset_bounds_and_the_real_fixture():
    lo, hi = dataset_bounds(FIXTURES / "nasa_sample.log")
    assert 0 <= lo - parse_when("1995-07-24 02:40") < 30 and hi - lo == pytest.approx(40 * 60, abs=60)     # first real request ~02:40:03
    seg = list(iter_segment(FIXTURES / "nasa_sample.log", parse_when("1995-07-24 03:00"), parse_when("1995-07-24 03:10")))
    assert seg and all(parse_when("1995-07-24 03:00") <= r.ts < parse_when("1995-07-24 03:10") for r in seg)


def test_dataset_path_falls_back_to_the_common_file_name(tmp_path):
    (tmp_path / "access_log_Jul95").write_text("x")
    assert resolve_dataset_path(tmp_path / "NASA_access_log_Jul95").name == "access_log_Jul95"
    (tmp_path / "NASA_access_log_Jul95").write_text("x")
    assert resolve_dataset_path(tmp_path / "NASA_access_log_Jul95").name == "NASA_access_log_Jul95"   # exact name wins
    with pytest.raises(FileNotFoundError, match="README"):
        resolve_dataset_path(tmp_path / "nope" / "NASA_access_log_Jul95")


@pytest.mark.parametrize("text,expected", [
    ("13/Jul/1995:08:00:00 -0400", parse_clf_timestamp("13/Jul/1995:08:00:00 -0400")),
    ("1995-07-13 08:00", parse_clf_timestamp("13/Jul/1995:08:00:00 -0400")),           # naive = the log's zone
    ("1995-07-13T08:00:30", parse_clf_timestamp("13/Jul/1995:08:00:30 -0400")),
    ("1995-07-13T12:00:00+00:00", parse_clf_timestamp("13/Jul/1995:08:00:00 -0400")),  # explicit zone wins
    ("1995-07-13T12:00:00Z", parse_clf_timestamp("13/Jul/1995:08:00:00 -0400")),
    (804571201, 804571201.0),
])
def test_parse_when(text, expected):
    assert parse_when(text, -240) == expected


def test_parse_when_rejects_nonsense():
    with pytest.raises(ValueError, match="cannot read time"):
        parse_when("next tuesday")


# =================================================================================================
# rendering: timestamp rewritten to "now", original kept
# =================================================================================================
def test_render_clf_rewrites_the_timestamp_and_keeps_the_original():
    line = clf(T0 + 5, "/history/apollo/", 404)
    out = render_clf(line, 1_790_000_000.0)
    assert out.endswith(f' orig_ts="{format_clf_timestamp(T0 + 5, -240)}"')
    ev = ClfParser(load_settings(env={}).mapping)(out)
    assert ev.ts == 1_790_000_000.0 and ev.status == 404 and ev.message == "GET /history/apollo/"   # parses, same request
    assert format_clf_timestamp(1_790_000_000.0, 0) in out and "1995" not in out.split(" orig_ts=")[0]
    assert render_clf("alyssa.p", 5.0) == "alyssa.p"                    # malformed lines pass through (they get counted)


def test_render_ndjson_carries_the_original_timestamp():
    parser = ClfParser(load_settings(env={"LOGPULSE_PROFILE": "nasa"}).mapping)
    out = render_ndjson(clf(T0 + 9, "/images/a.gif", 200), 1_790_000_000.0, parser)
    obj = json.loads(out)
    assert obj["orig_timestamp"] == format_clf_timestamp(T0 + 9, -240) and obj["service"] == "images"
    assert parse_line(out).ts == pytest.approx(1_790_000_000.0)
    assert render_ndjson("alyssa.p", 1.0, parser) == "alyssa.p"


# =================================================================================================
# Replayer (fake clock + fake sleep)
# =================================================================================================
def fake_time():
    clock = FakeClock(1_790_000_000.0)

    async def sleep(dt):
        clock.advance(dt)
    return clock, sleep


def read_out(path) -> list[str]:
    return [ln for ln in path.read_bytes().decode("latin-1").split("\n") if ln]


async def test_replayer_appends_the_segment_with_rescaled_timestamps(tmp_path):
    ds, out = tmp_path / "ds", tmp_path / "app.log"
    write_dataset(ds, n=200, step=1)                                    # 200 original seconds
    clock, sleep = fake_time()
    rep = Replayer(ds, ReplaySpec(speed=100, seed=1, max_gap_seconds=5), ClfParser(load_settings(env={}).mapping), clock, sleep)
    t_start = clock()
    await rep.run(out)
    lines = read_out(out)
    assert len(lines) == 200 and rep.progress.lines_sent == 200 and not rep.progress.running and rep.progress.progress == 1.0
    stamps = [parse_clf_timestamp(ln.split("[")[1].split("]")[0]) for ln in lines]
    assert stamps == sorted(stamps)
    assert stamps[-1] - stamps[0] == pytest.approx(2.0, abs=1.1)        # 200 s at 100x = ~2 s of wall time
    assert t_start - 1 <= stamps[0] <= t_start + 1.1
    origs = [ln.split('orig_ts="')[1].rstrip('"') for ln in lines]
    assert origs[0] == format_clf_timestamp(T0, -240) and origs == sorted(origs, key=parse_clf_timestamp)


async def test_replayer_start_end_window_and_ndjson_format(tmp_path):
    ds, out = tmp_path / "ds", tmp_path / "app.log"
    write_dataset(ds, n=300)
    clock, sleep = fake_time()
    spec = ReplaySpec(start_ts=T0 + 100, end_ts=T0 + 150, speed=1000, seed=3, fmt="ndjson")
    await Replayer(ds, spec, ClfParser(load_settings(env={"LOGPULSE_PROFILE": "nasa"}).mapping), clock, sleep).run(out)
    rows = [json.loads(x) for x in read_out(out)]
    assert len(rows) == 50 and rows[0]["request_id"] == "h100" and rows[-1]["request_id"] == "h149"
    assert all("orig_timestamp" in r and r["level"] == "INFO" for r in rows)


async def test_replayer_loop_and_stop(tmp_path):
    ds, out = tmp_path / "ds", tmp_path / "app.log"
    write_dataset(ds, n=100)
    clock, base_sleep = fake_time()
    stop = asyncio.Event()
    t0 = clock()

    async def sleep(dt):
        await base_sleep(dt)
        if clock() - t0 > 4.5:                                          # 1 pass = 1 s at 100x
            stop.set()
    rep = Replayer(ds, ReplaySpec(speed=100, seed=1, loop=True), ClfParser(load_settings(env={}).mapping), clock, sleep)
    await rep.run(out, stop)
    assert rep.progress.loops >= 3 and rep.progress.lines_sent >= 300 and not rep.progress.running


async def test_replayer_stops_promptly_and_keeps_what_it_wrote(tmp_path):
    ds, out = tmp_path / "ds", tmp_path / "app.log"
    write_dataset(ds, n=500)
    clock, base_sleep = fake_time()
    stop = asyncio.Event()
    t0 = clock()

    async def sleep(dt):
        await base_sleep(dt)
        if clock() - t0 > 2.0:                                          # the whole segment would take ~50 s at 10x
            stop.set()
    rep = Replayer(ds, ReplaySpec(speed=10, seed=1), ClfParser(load_settings(env={}).mapping), clock, sleep)
    await rep.run(out, stop)
    assert 0 < len(read_out(out)) < 500 and rep.progress.lines_sent == len(read_out(out)) and not rep.progress.running


async def test_replayer_passes_malformed_lines_through_untouched(tmp_path):
    ds, out = tmp_path / "ds", tmp_path / "app.log"
    write_dataset(ds, n=10, junk_at={5})
    clock, sleep = fake_time()
    await Replayer(ds, ReplaySpec(speed=1000, seed=1), ClfParser(load_settings(env={}).mapping), clock, sleep).run(out)
    lines = read_out(out)
    assert len(lines) == 11 and any(ln.startswith("garbage") and "orig_ts" not in ln for ln in lines)


def test_replay_spec_validates_the_format(tmp_path):
    write_dataset(tmp_path / "ds", n=3)
    with pytest.raises(ValueError):
        Replayer(tmp_path / "ds", ReplaySpec(fmt="xml"), ClfParser(load_settings(env={}).mapping))


# =================================================================================================
# AWS guard: SNS + CloudWatch stay OFF during a replay
# =================================================================================================
@pytest.fixture
def cfg(tmp_path) -> ReplayCfg:
    return replace(load_settings(env={}).replay, aws_preset="spike-x", aws_max_sends_per_run=3,
                   state_file=str(tmp_path / "replay.state.json"), stale_seconds=5.0, send_to_aws=False)


def test_outside_a_replay_everything_is_permitted(cfg):
    g = ReplayGuard(cfg)
    assert all(g.permit(c) for c in ("sns", "cloudwatch", "ntfy", "telegram", "jsonl", "console"))
    assert g.aws_suppressed == 0 and not g.active


def test_aws_is_off_by_default_during_a_replay_but_other_channels_still_deliver(cfg):
    g = ReplayGuard(cfg)
    g.begin("r1", "normal-jul16", requested_aws=True)                   # asking is not enough for a non-aws preset
    assert [g.permit(c) for c in ("sns", "cloudwatch")] == [False, False]
    assert all(g.permit(c) for c in ("ntfy", "telegram", "jsonl", "console"))
    assert g.status()["aws_suppressed"] == 2 and g.status()["aws_allowed"] is False
    g.end()
    assert g.permit("sns") and not g.active                             # back to normal afterwards


def test_only_the_configured_preset_may_opt_in_and_it_must_ask(cfg):
    g = ReplayGuard(cfg)
    g.begin("r1", "spike-x", requested_aws=False)
    assert g.permit("sns") is False                                     # allowed preset, but the run did not ask
    g.begin("r2", "spike-x", requested_aws=True)
    assert g.permit("sns") is True and g.aws_allowed
    g.begin("r3", "spike-y", requested_aws=True)
    assert g.permit("sns") is False


def test_hard_cap_on_aws_sends_per_run_and_counters_reset_per_run(cfg):
    g = ReplayGuard(cfg)
    g.begin("r1", "spike-x", True)
    got = [g.permit("sns" if i % 2 else "cloudwatch") for i in range(6)]
    assert got == [True, True, True, False, False, False]               # cap of 3 is shared by SNS and CloudWatch
    assert g.status()["aws_sends"] == 3 and g.status()["aws_suppressed"] == 3
    assert g.permit("ntfy") is True                                     # the cap never touches other channels
    g.begin("r2", "spike-x", True)
    assert g.aws_sends == 0 and g.permit("sns") is True                 # a new run gets a fresh budget


def test_global_send_to_aws_flag_opens_the_gate_but_the_cap_still_applies(cfg):
    g = ReplayGuard(replace(cfg, send_to_aws=True, aws_max_sends_per_run=1))
    g.begin("r1", "anything", False)
    assert [g.permit("sns"), g.permit("sns")] == [True, False]


def test_aws_channel_set_is_exactly_sns_and_cloudwatch():
    assert AWS_CHANNELS == {"sns", "cloudwatch"}


def test_an_external_replay_is_adopted_from_its_heartbeat_file_and_released_when_stale(cfg):
    clock = FakeClock(1000.0)
    g = ReplayGuard(cfg, clock)
    g.poll_file()
    assert not g.active                                                  # no file
    write_state_file(cfg.state_file, {"active": True, "run_id": "cli-1", "preset": "normal-jul16", "aws": True, "updated_at": clock()})
    g.poll_file()
    assert g.active and g.source == "external" and g.permit("sns") is False   # 'aws': True is ignored for a non-aws preset
    clock.advance(10)                                                    # heartbeat went stale (the script died)
    g.poll_file()
    assert not g.active and g.permit("sns") is True
    write_state_file(cfg.state_file, {"active": True, "run_id": "cli-2", "preset": "spike-x", "aws": True, "updated_at": clock()})
    g.poll_file()
    assert g.active and g.permit("sns") is True                          # the one allowed preset that asked
    write_state_file(cfg.state_file, {"active": False, "run_id": "cli-2", "preset": "spike-x", "aws": False, "updated_at": clock()})
    g.poll_file()
    assert not g.active


def test_corrupt_state_file_means_no_replay_and_api_runs_are_not_overridden(cfg):
    g = ReplayGuard(cfg)
    with open(cfg.state_file, "w") as fh:
        fh.write("{not json")
    g.poll_file()
    assert not g.active
    g.begin("api-run", "normal-jul16", False, source="api")
    write_state_file(cfg.state_file, {"active": False, "updated_at": time.time()})
    g.poll_file()
    assert g.active and g.run_id == "api-run"                            # the external file cannot end an in-process run


# ---- the guard inside the alert manager --------------------------------------------------------------
async def test_manager_creates_no_aws_delivery_rows_while_replaying_but_still_notifies_the_rest(settings, cfg):
    repo = Repository(Database(":memory:"))
    sinks = {n: RecordingSink(n) for n in ("sns", "cloudwatch", "ntfy", "jsonl")}
    guard = ReplayGuard(cfg)

    async def broadcast(t, d):
        pass
    dispatcher = Dispatcher(sinks, repo, retry_attempts=1, backoff_seconds=0)
    manager = AlertManager(AlertStateMachine(settings.detector, settings.profile), repo, dispatcher, broadcast,
                           settings.profile.window_seconds, channel_permit=guard.permit)
    task = asyncio.create_task(dispatcher.run())
    crit = dict(sev="CRITICAL", z=9, ratio=9, rate=0.4)
    guard.begin("r1", "normal-jul16", False)
    await manager.process([snap(ts=0, service="a", **crit)])
    guard.end()
    await manager.process([snap(ts=1, service="b", **crit)])
    await asyncio.wait_for(dispatcher.drain(), 5)
    task.cancel()
    rows = {}
    for a in repo.list_alerts():
        rows[a.service] = sorted(r["channel"] for r in repo.deliveries_for(a.id))
    assert rows["a"] == ["jsonl", "ntfy"]                                # replaying: no sns / cloudwatch rows at all
    assert rows["b"] == ["cloudwatch", "jsonl", "ntfy", "sns"]           # not replaying: everything
    assert len(sinks["sns"].sent) == 1 and len(sinks["ntfy"].sent) == 2  # AWS sinks were never even called for alert "a"
    repo.db.close()


# =================================================================================================
# in-process controller through the API
# =================================================================================================
PRESETS = {
    "p-fast": {"description": "fast", "start": "1995-07-24 02:40", "end": "1995-07-24 02:50", "speed": 600, "seed": 1},
    "p-aws": {"description": "aws-capable", "start": "1995-07-24 02:40", "end": "1995-07-24 02:50", "speed": 600, "seed": 1},
    "p-long": {"description": "long", "start": "1995-07-24 02:40", "end": "1995-07-24 03:20", "speed": 5, "seed": 1},
}


@pytest.fixture
def replay_client(tmp_path, nasa_settings):
    def factory(demo=True, dataset=None, sinks=None):
        s = replace(
            nasa_settings, demo_mode=demo, log_path=str(tmp_path / "app.log"), db_path=str(tmp_path / "lp.db"),
            alerts_jsonl=str(tmp_path / "alerts.jsonl"),
            ingestion=replace(nasa_settings.ingestion, start_at="checkpoint"),
            replay=replace(nasa_settings.replay, dataset_path=str(dataset or FIXTURES / "nasa_sample.log"), presets=PRESETS,
                           aws_preset="p-aws", state_file=str(tmp_path / "replay.state.json"), max_gap_seconds=15))
        return TestClient(create_app(s, sinks={} if sinks is None else sinks))
    return factory


def wait_finished(c, timeout=20.0):
    end = time.time() + timeout
    while time.time() < end:
        st = c.get("/api/demo/replay").json()
        if not st["running"]:
            return st
        time.sleep(0.1)
    raise AssertionError("replay did not finish")


def test_replay_api_lists_presets_and_validates_requests(replay_client):
    with replay_client() as c:
        st = c.get("/api/demo/replay").json()
        assert st["running"] is False and {p["name"] for p in st["presets"]} == set(PRESETS)
        assert [p["aws"] for p in st["presets"] if p["name"] == "p-aws"] == [True]
        assert c.post("/api/demo/replay", json={"action": "start", "preset": "nope"}).status_code == 422
        assert c.post("/api/demo/replay", json={"action": "start"}).status_code == 422          # neither preset nor start
        assert c.post("/api/demo/replay", json={"action": "start", "preset": "p-fast", "speed": -5}).status_code == 422
        assert c.post("/api/demo/replay", json={"action": "start", "start": "next tuesday"}).status_code == 422
        assert c.post("/api/demo/replay", json={"action": "explode"}).status_code == 422
        assert c.post("/api/demo/replay", json={"action": "stop"}).json()["running"] is False    # stopping an idle replay is fine


def test_replay_api_is_absent_without_demo_mode(replay_client):
    with replay_client(demo=False) as c:
        assert c.get("/api/demo/replay").status_code == 404
        assert c.post("/api/demo/replay", json={"action": "start", "preset": "p-fast"}).status_code == 404


def test_a_missing_dataset_gives_a_clear_409(replay_client, tmp_path):
    with replay_client(dataset=tmp_path / "missing" / "NASA_access_log_Jul95") as c:
        r = c.post("/api/demo/replay", json={"action": "start", "preset": "p-fast"})
        assert r.status_code == 409 and "dataset not found" in r.json()["detail"]


def test_replay_runs_the_real_pipeline_end_to_end_through_the_api(replay_client, tmp_path):
    with replay_client() as c:
        r = c.post("/api/demo/replay", json={"action": "start", "preset": "p-fast"})
        assert r.status_code == 200 and r.json()["running"] is True and r.json()["preset"] == "p-fast"
        rt = c.app.state.rt
        assert rt.scenario.paused is True                                # the synthetic generator yields the file
        st = wait_finished(c)
        assert st["lines_sent"] > 100 and st["progress"] == 1.0 and st["error"] is None
        time.sleep(1.5)                                                  # let the tailer + a few ticks catch up
        assert rt.scenario.paused is False
        log = (tmp_path / "app.log").read_bytes().decode("latin-1")
        assert ' orig_ts="24/Jul/1995:02:4' in log and "1995:02:4" not in log.split(" orig_ts=")[0]
        metrics = {m["service"] for m in c.get("/api/metrics/current").json()}
        assert {"shuttle", "images", "history"} <= metrics               # CLF parsed and mapped to services by the app
        status = c.get("/api/system/status").json()
        assert status["parse_errors"] == 0 and status["replay"]["guard"]["active"] is False


def test_the_api_cannot_talk_its_way_past_the_aws_guard(replay_client):
    with replay_client() as c:
        for preset, ask, expect in (("p-fast", True, False), ("p-fast", False, False), ("p-aws", False, False), ("p-aws", True, True)):
            r = c.post("/api/demo/replay", json={"action": "start", "preset": preset, "aws": ask}).json()
            assert r["guard"]["aws_allowed"] is expect and r["guard"]["aws_cap"] == 20, (preset, ask)
            wait_finished(c)


def test_replay_can_be_stopped_and_restarted_and_resets_detection(replay_client):
    with replay_client() as c:
        rt = c.app.state.rt
        c.post("/api/demo/replay", json={"action": "start", "preset": "p-long"})
        time.sleep(0.6)
        assert c.get("/api/demo/replay").json()["running"] is True
        stopped = c.post("/api/demo/replay", json={"action": "stop"}).json()
        assert stopped["running"] is False and stopped["lines_sent"] > 0
        assert rt.scenario.paused is False                               # generator released on stop
        sent_after_stop = c.get("/api/demo/replay").json()["lines_sent"]
        time.sleep(0.5)
        assert c.get("/api/demo/replay").json()["lines_sent"] == sent_after_stop      # really stopped
        rt.engine.detector("stale-service")                              # leftover state from "before"
        c.post("/api/demo/replay", json={"action": "start", "preset": "p-fast"})
        assert "stale-service" not in rt.engine.services                 # each run starts from a clean detector
        wait_finished(c)


def test_hello_and_health_expose_the_replay_panel_data(replay_client):
    with replay_client() as c:
        with c.websocket_connect("/ws") as ws:
            hello = ws.receive_json()["data"]
        assert {p["name"] for p in hello["config"]["replay_presets"]} == set(PRESETS)
        assert hello["config"]["generator_active"] is False and hello["config"]["error_definition"] == "4xx+5xx"
        assert hello["health"]["replay"]["running"] is False
        page = c.get("/").text
    assert 'id="rp-start"' in page and "Dataset replay" in page


def test_reset_detection_closes_open_alerts_silently(make_runtime, clock):
    rt = make_runtime()
    from app.detection.models import Alert
    a = Alert(id="a1", dedup_key="svc:error_rate", service="svc", severity="HIGH", peak_severity="HIGH", status="OPEN",
              created_at=1.0, resolved_at=None, current_rate=0.3, baseline_rate=0.01, z=9, ratio=9, sample_size=100, reason="r")
    rt.repo.upsert_alert(a)
    rt.machine.restore([a])
    rt.reset_detection()
    got = rt.repo.get_alert("a1")
    assert got.status == "RESOLVED" and "closed silently" in got.reason and not rt.machine.open
    assert rt.repo.deliveries_for("a1") == []                            # no notification was created


# =================================================================================================
# config: the nasa profile, overrides, paths
# =================================================================================================
def test_nasa_profile_overrides_apply_only_to_that_profile():
    n = load_settings(env={"LOGPULSE_PROFILE": "nasa"})
    assert (n.ingestion.format, n.detector.error_definition, n.generator.autostart) == ("clf", "4xx+5xx", False)
    assert "shuttle" in n.mapping.service_prefixes and n.mapping.top_n == 8 and len(n.mapping.service_prefixes) == 8
    assert n.profile.min_events == 40 and n.detector.warmup_ceiling == 0.05 and n.replay.speed == 180
    assert n.severity.medium.rate == 0.12 and n.severity.high.rate == 0.20 and n.severity.critical.rate == 0.30
    d = load_settings(env={})
    assert (d.ingestion.format, d.detector.error_definition, d.generator.autostart) == ("auto", "5xx", True)
    assert d.severity.medium.rate == 0.10 and d.mapping.service_prefixes == [] and d.detector.warmup_ceiling == 0.25   # untouched
    assert load_settings(env={"LOGPULSE_PROFILE": "prod"}).detector.error_definition == "5xx"       # no leakage


def test_unknown_profile_is_a_clear_error():
    with pytest.raises(ValueError, match="unknown profile"):
        load_settings(env={"LOGPULSE_PROFILE": "nope"})


def test_replay_defaults_are_safe():
    r = load_settings(env={}).replay
    assert r.send_to_aws is False and r.aws_max_sends_per_run > 0 and r.aws_preset in r.presets
    assert r.max_gap_seconds > load_settings(env={"LOGPULSE_PROFILE": "nasa"}).profile.window_seconds    # LOW_DATA can show


def test_dataset_and_state_file_paths_follow_the_environment():
    r = load_settings(env={"LOG_PATH": "/data/app.log", "DATASET_PATH": "/data/datasets/NASA_access_log_Jul95"}).replay
    assert r.dataset_path == "/data/datasets/NASA_access_log_Jul95"
    assert r.state_file.replace("\\", "/") == "/data/replay.state.json"


def test_every_preset_in_the_shipped_config_is_well_formed():
    r = load_settings(env={"LOGPULSE_PROFILE": "nasa"}).replay
    assert len(r.presets) >= 4
    for name, p in r.presets.items():
        lo, hi = parse_when(p["start"], r.dataset_tz_minutes), parse_when(p["end"], r.dataset_tz_minutes)
        assert lo < hi and p["speed"] > 0 and p["description"], name
        assert parse_when("1995-07-01 00:00", -240) <= lo and hi <= parse_when("1995-07-28 13:32", -240), name   # inside the file


def test_starting_a_second_replay_stops_the_first(replay_client):
    with replay_client() as c:
        first = c.post("/api/demo/replay", json={"action": "start", "preset": "p-long"}).json()
        assert first["running"] and first["preset"] == "p-long"
        second = c.post("/api/demo/replay", json={"action": "start", "preset": "p-fast"}).json()
        assert second["preset"] == "p-fast" and second["run_id"] != first["run_id"]      # never two writers on one log
        done = wait_finished(c)
        assert done["preset"] == "p-fast" and done["error"] is None and done["progress"] == 1.0


def test_a_crashing_replay_is_reported_and_releases_the_guard_and_generator(replay_client, monkeypatch):
    async def boom(self, out_path, stop=None):
        raise RuntimeError("disk full")
    monkeypatch.setattr("app.replay.Replayer.run", boom)
    with replay_client() as c:
        assert c.post("/api/demo/replay", json={"action": "start", "preset": "p-fast"}).status_code == 200
        st = wait_finished(c)
        assert "disk full" in st["error"] and st["guard"]["active"] is False       # AWS suppression must not stick
        assert c.app.state.rt.scenario.paused is False                              # nor the paused generator
        assert c.get("/health").json() == {"status": "ok"}                          # the app itself is unaffected
