"""End-to-end on REAL lines of the NASA log (tests/fixtures/, a few hundred lines - not the 200 MB file):

  replayer -> app.log -> tailer -> CLF parser -> service mapping -> detector -> alert -> WebSocket / sinks

on a fake clock, plus the two scripts. Any attempt to build a boto3 client fails these tests: nothing here can reach AWS."""
import asyncio
import importlib.util
import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from app.ingestion.clf import ClfParser
from app.replay import ReplaySpec, Replayer, iter_segment, parse_when, simulate

from conftest import FIXTURES, RecordingSink

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = FIXTURES / "nasa_sample.log"


@pytest.fixture(autouse=True)
def no_real_aws(monkeypatch):
    import boto3

    def boom(*a, **kw):
        raise AssertionError("a test tried to create a real boto3 client / session")
    monkeypatch.setattr(boto3, "client", boom)
    monkeypatch.setattr(boto3, "Session", boom)


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


async def replay_into(rt, clock, dataset, spec, tail_seconds=40):
    """Drive a Replayer on the runtime's fake clock, running the app's ingest/tick steps whenever time advances."""
    nxt = [clock.now + 1.0]

    async def step():
        for raw in rt.tailer.read_available():
            await rt.ingest_line(raw)
        while not rt.queue.empty():                                     # what the consumer task does
            ev = rt.queue.get_nowait()
            rt.engine.on_event(ev)
            rt.health.record_event(ev.ts)
        while clock.now >= nxt[0]:
            await rt._tick_once()
            nxt[0] += 1.0

    async def sleep(dt):
        clock.advance(dt)
        await step()

    rep = Replayer(dataset, spec, ClfParser(rt.settings.mapping), clock, sleep, 0.1)
    dispatcher = asyncio.create_task(rt.dispatcher.run())
    await rep.run(rt.settings.log_path)
    for _ in range(int(tail_seconds)):
        await sleep(1.0)
    await asyncio.wait_for(rt.dispatcher.drain(), 5)
    dispatcher.cancel()
    return rep


def spec(**kw):
    return ReplaySpec(**{"speed": 60, "seed": 1, "max_gap_seconds": 15, **kw})


# ---- the whole pipeline ------------------------------------------------------------------------------
async def test_a_real_error_burst_replays_into_an_alert_and_a_websocket_message(make_runtime, clock, nasa_settings):
    rt = make_runtime(base=nasa_settings)
    rep = await replay_into(rt, clock, SAMPLE, spec())

    assert rep.progress.lines_sent == 781 and rt.stats.lines_read == 781
    assert rt.stats.parse_errors == 0 and rt.stats.events_parsed == 781        # real CLF, all lines understood
    services = {m["service"] for m in rt.ws.of("metric.update")}
    assert {"history", "shuttle", "images", "other", "root", "icons"} <= services   # mapped from URL prefixes

    created = rt.ws.of("alert.created")
    assert [a["service"] for a in created] == ["history"]                      # ONE alert, on the service that really broke
    a = created[0]
    assert a["status"] == "OPEN" and a["current_rate"] >= 0.12 and a["baseline_rate"] < 0.05 and a["z"] >= 3
    assert a["reason"] and a["evidence"]["window_seconds"] == 5
    assert rt.machine.is_open("history") or rt.ws.of("alert.resolved")


async def test_calm_real_traffic_before_the_burst_raises_no_alert(make_runtime, clock, nasa_settings):
    rt = make_runtime(base=nasa_settings)
    await replay_into(rt, clock, SAMPLE, spec(end_ts=parse_when("1995-07-24 03:00")))
    assert rt.stats.events_parsed > 400 and not rt.ws.of("alert.created")
    assert {m["state"] for m in rt.ws.of("metric.update")} >= {"NORMAL"}       # warmed up and judged normal


async def test_real_oddities_flow_through_the_pipeline_without_crashing(make_runtime, clock, nasa_settings, tmp_path):
    odd = (FIXTURES / "nasa_oddities.log").read_bytes().decode("latin-1").split("\n")
    odd = [ln for ln in odd if ln]
    calm = [ln for ln in SAMPLE.read_bytes().decode("latin-1").split("\n") if ln][:300]
    ds = tmp_path / "mixed.log"
    ds.write_bytes(("\n".join(calm + odd) + "\n").encode("latin-1"))           # junk requests, spaces in URLs, cut last line
    rt = make_runtime(base=nasa_settings)
    rep = await replay_into(rt, clock, ds, spec())
    assert rep.progress.lines_sent == len(calm) + len(odd)
    assert rt.stats.parse_errors == 1                                          # only the truncated "alyssa.p" is unusable
    assert rt.stats.parse_error_samples[0]["line"] == "alyssa.p"
    assert rt.stats.events_parsed == len(calm) + len(odd) - 1
    assert rt.health.report()["parse_errors"] == 1


async def test_a_second_run_starts_from_a_clean_detector(make_runtime, clock, nasa_settings):
    rt = make_runtime(base=nasa_settings)
    await replay_into(rt, clock, SAMPLE, spec())
    assert rt.ws.of("alert.created")
    before = len(rt.ws.of("alert.created"))
    rt.reset_detection()                                                       # what the API does at the start of every run
    await replay_into(rt, clock, SAMPLE, spec())
    assert len(rt.ws.of("alert.created")) == before + 1                        # the same real burst fires again, reproducibly


# ---- AWS safety, end to end ---------------------------------------------------------------------------
async def test_aws_stays_off_during_replay_opts_in_for_one_preset_is_capped_and_returns_afterwards(make_runtime, clock, nasa_settings, tmp_path):
    sinks = {n: RecordingSink(n) for n in ("sns", "cloudwatch", "ntfy", "jsonl")}
    replay_cfg = replace(nasa_settings.replay, aws_preset="spike-x", aws_max_sends_per_run=1, send_to_aws=False,
                         state_file=str(tmp_path / "replay.state.json"))
    rt = make_runtime(sinks=sinks, base=nasa_settings, replay=replay_cfg)

    # 1) an ordinary replay: alerts fire, ntfy/JSONL deliver, SNS + CloudWatch are never called
    rt.guard.begin("run-1", "normal-jul16", requested_aws=True)                # asking does not help for a non-AWS preset
    await replay_into(rt, clock, SAMPLE, spec())
    rt.guard.end()
    assert sinks["ntfy"].sent and sinks["jsonl"].sent
    assert sinks["sns"].sent == [] and sinks["cloudwatch"].sent == []
    alert_id = rt.ws.of("alert.created")[0]["id"]
    assert {r["channel"] for r in rt.repo.deliveries_for(alert_id)} == {"ntfy", "jsonl"}
    assert rt.guard.aws_suppressed >= 2

    # 2) the one allowed preset that asks: AWS may send, but never more than the cap (1) in the run
    rt.reset_detection()
    rt.guard.begin("run-2", "spike-x", requested_aws=True)
    await replay_into(rt, clock, SAMPLE, spec())
    aws_sent = len(sinks["sns"].sent) + len(sinks["cloudwatch"].sent)
    assert aws_sent == 1 and rt.guard.aws_sends == 1 and rt.guard.aws_suppressed >= 1
    ntfy_after = len(sinks["ntfy"].sent)
    assert ntfy_after >= 2                                                     # the free channels were never limited
    rt.guard.end()

    # 3) no replay running: a normal alert reaches every channel again
    await rt.send_test_alert()
    task = asyncio.create_task(rt.dispatcher.run())
    await asyncio.wait_for(rt.dispatcher.drain(), 5)
    task.cancel()
    assert len(sinks["sns"].sent) == 2 and len(sinks["cloudwatch"].sent) == 1 and len(sinks["ntfy"].sent) == ntfy_after + 1


async def test_detection_is_identical_whether_or_not_aws_is_suppressed(make_runtime, clock, nasa_settings):
    """The guard only removes deliveries; it must never change what the detector concludes."""
    rt = make_runtime(base=nasa_settings, sinks={"sns": RecordingSink("sns"), "ntfy": RecordingSink("ntfy")})

    def summary(alerts):     # the conclusion, not tick-level numbers: the second run starts at a different sub-second phase
        return [(a["service"], a["peak_severity"] if a["peak_severity"] in {"HIGH", "CRITICAL"} else "MEDIUM+") for a in alerts]

    rt.guard.begin("r1", "normal-jul16", False)                                # AWS suppressed
    await replay_into(rt, clock, SAMPLE, spec())
    rt.guard.end()
    first = summary(rt.ws.of("alert.created"))
    rt.reset_detection()
    await replay_into(rt, clock, SAMPLE, spec())                               # AWS not suppressed
    second = summary(rt.ws.of("alert.created"))[len(first):]
    assert first and first == second


# ---- the simulator agrees with the pipeline -----------------------------------------------------------
def test_simulation_finds_the_same_burst_and_stays_quiet_on_calm_traffic(nasa_settings):
    res = simulate(iter_segment(SAMPLE), nasa_settings, speed=60, max_gap=15, seed=1)
    assert [a.service for a in res.alerts] == ["history"] and res.parse_errors == 0 and res.events == 781
    a = res.alerts[0]
    assert a.peak in {"HIGH", "CRITICAL"} and a.rate >= 0.12 and a.errors >= 6
    calm = simulate(iter_segment(SAMPLE, None, parse_when("1995-07-24 03:00")), nasa_settings, speed=60, max_gap=15, seed=1)
    assert calm.alerts == []


def test_simulation_is_reproducible_for_a_seed(nasa_settings):
    a = simulate(iter_segment(SAMPLE), nasa_settings, speed=60, max_gap=15, seed=5)
    b = simulate(iter_segment(SAMPLE), nasa_settings, speed=60, max_gap=15, seed=5)
    assert [(x.service, x.peak, x.virt_start, x.errors) for x in a.alerts] == [(x.service, x.peak, x.virt_start, x.errors) for x in b.alerts]


# ---- scripts ------------------------------------------------------------------------------------------
def run_main(mod, argv, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", [mod.__name__ + ".py", *argv])
    try:
        mod.main()
        code = 0
    except SystemExit as e:
        code = e.code
    return code, capsys.readouterr()


def test_analyze_script_streams_the_fixture_and_reports(monkeypatch, capsys, tmp_path):
    mod = load_script("analyze_dataset")
    out = tmp_path / "a.json"
    code, cap = run_main(mod, ["--path", str(SAMPLE), "--window-minutes", "5", "--top-prefixes", "5", "--spike-services", "1",
                               "--out", str(out)], monkeypatch, capsys)
    assert code == 0
    res = json.loads(out.read_text())
    assert res["lines"] == 781 and res["parse_errors"] == 0 and res["error_definition"] == "4xx+5xx"
    assert res["prefixes"][0]["prefix"] in {"images", "shuttle"} and sum(p["events"] for p in res["prefixes"]) <= 781
    for needle in ("census", "top URL prefixes", "per-minute distribution", "longest silences", "threshold hints"):
        assert needle in cap.out
    e5 = tmp_path / "b.json"
    run_main(mod, ["--path", str(SAMPLE), "--error-definition", "5xx", "--window-minutes", "5", "--out", str(e5)], monkeypatch, capsys)
    errs = lambda p: sum(x["errors"] for x in json.loads(p.read_text())["prefixes"])          # noqa: E731
    assert errs(out) > errs(e5) >= 0                                           # 4xx+5xx counts more than 5xx alone


def test_analyze_script_scan_matches_the_parser_counts():
    mod = load_script("analyze_dataset")
    res = mod.scan(SAMPLE, 400, None, None)
    assert res["lines"] == 781 and res["bad"] == 0
    assert sum(t for t, _ in res["pm"].values()) == 781
    assert sum(e for _, e in res["pm"].values()) == sum(1 for c in res["status"].elements() if c >= 400)


def test_analyze_script_simulate_mode_runs_the_real_detector(monkeypatch, capsys, tmp_path):
    mod = load_script("analyze_dataset")
    out = tmp_path / "s.json"
    code, cap = run_main(mod, ["--simulate", "--profile", "nasa", "--path", str(SAMPLE), "--speed", "180", "--out", str(out)],
                         monkeypatch, capsys)
    sim = json.loads(out.read_text())["simulation"]
    assert code == 0 and sim["lines"] == 781 and sim["profile"] == "nasa"
    assert "simulating the real detector" in cap.out and "severity gates" in cap.out


def test_replay_script_lists_presets_and_replays_into_a_log(monkeypatch, capsys, tmp_path):
    mod = load_script("replay_dataset")
    monkeypatch.setenv("LOGPULSE_PROFILE", "nasa")
    monkeypatch.setenv("LOG_PATH", str(tmp_path / "app.log"))
    code, cap = run_main(mod, ["--list-presets"], monkeypatch, capsys)
    assert code == 0 and "spike-history-jul24" in cap.out and "[may use AWS with --aws]" in cap.out and "volume-surge-jul13" in cap.out

    out = tmp_path / "app.log"
    code, cap = run_main(mod, ["--dataset", str(SAMPLE), "--start", "1995-07-24 02:40", "--end", "1995-07-24 02:50",
                               "--speed", "3000", "--seed", "1", "--out", str(out)], monkeypatch, capsys)
    assert code == 0 and "done:" in cap.out
    lines = [ln for ln in out.read_bytes().decode("latin-1").split("\n") if ln]
    assert len(lines) > 50 and all(' orig_ts="24/Jul/1995:02:4' in ln for ln in lines)
    state = json.loads((tmp_path / "replay.state.json").read_text())
    assert state["active"] is False and state["preset"] == "custom" and state["aws"] is False   # heartbeat cleared on exit


def test_replay_script_errors_are_clear(monkeypatch, capsys, tmp_path):
    mod = load_script("replay_dataset")
    monkeypatch.setenv("LOGPULSE_PROFILE", "nasa")
    monkeypatch.setenv("LOG_PATH", str(tmp_path / "app.log"))
    code, cap = run_main(mod, ["--preset", "nope"], monkeypatch, capsys)
    assert code == 2 and "unknown preset" in cap.err
    code, cap = run_main(mod, ["--dataset", str(tmp_path / "missing" / "NASA_access_log_Jul95"), "--start", "1995-07-01 00:00"],
                         monkeypatch, capsys)
    assert code == 2 and "dataset not found" in cap.err
    code, cap = run_main(mod, ["--dataset", str(SAMPLE), "--start", "1995-07-24 02:40", "--end", "1995-07-24 02:41",
                               "--speed", "6000", "--aws", "--out", str(tmp_path / "x.log")], monkeypatch, capsys)
    assert code == 0 and "--aws ignored by the app" in cap.err                # asking for AWS on a non-AWS preset is called out
    state = json.loads((tmp_path / "replay.state.json").read_text())
    assert state["active"] is False
