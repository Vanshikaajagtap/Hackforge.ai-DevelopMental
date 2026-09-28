"""Append lines to a temp file -> tailer -> parser -> detector -> alert -> WebSocket message, on a fake clock."""
import asyncio
import json
from datetime import datetime, timezone

from conftest import RecordingSink

SVC = "payment-service"


def ndjson(ts: float, is_error: bool, service: str = SVC) -> str:
    return json.dumps({
        "timestamp": datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="milliseconds"),
        "service": service, "level": "ERROR" if is_error else "INFO",
        "status": 500 if is_error else 200, "message": "m", "request_id": "r",
    })


async def pump(rt, clock, seconds, events_per_sec, errors_per_sec, junk_every_second=False):
    """One simulated second at a time: the app 'writes' the file, the real tailer/parser/queue/engine/manager react."""
    path = rt.settings.log_path
    for _ in range(seconds):
        with open(path, "a", newline="\n", encoding="utf-8") as f:
            for i in range(events_per_sec):
                f.write(ndjson(clock.now, i < errors_per_sec) + "\n")
            if junk_every_second:
                f.write("{this is not json\n")
        for raw in rt.tailer.read_available():
            await rt.ingest_line(raw)
        while not rt.queue.empty():                 # what the consumer task does, deterministically
            ev = rt.queue.get_nowait()
            rt.engine.on_event(ev)
            rt.health.record_event(ev.ts)
        clock.advance(1)
        await rt._tick_once()


async def with_dispatcher(rt, coro):
    task = asyncio.create_task(rt.dispatcher.run())
    try:
        await coro
        await asyncio.wait_for(rt.dispatcher.drain(), 5)
    finally:
        task.cancel()


async def test_full_incident_lifecycle_file_to_websocket(make_runtime, clock):
    sink = RecordingSink("fake")
    rt = make_runtime(sinks={"fake": sink})

    async def story():
        await pump(rt, clock, 30, 20, 1, junk_every_second=True)             # learn a 5 % baseline (+ poison lines)
        assert not rt.ws.of("alert.created")

        await pump(rt, clock, 15, 100, 5)                                     # traffic spike: 5x volume, same 5 %
        assert not rt.ws.of("alert.created"), "volume alone must not alert"
        assert max(m["total"] for m in rt.ws.of("metric.update")) > 800

        await pump(rt, clock, 12, 20, 8)                                      # error spike: 40 %
    await with_dispatcher(rt, story())

    created = rt.ws.of("alert.created")
    assert len(created) == 1
    alert = created[0]
    assert alert["service"] == SVC and alert["status"] == "OPEN"
    assert alert["baseline_rate"] < 0.08 and alert["current_rate"] > 0.10 and alert["z"] >= 2   # opened while the rate was still ramping
    assert alert["reason"] and alert["evidence"]["window_seconds"] == 10
    latest = rt.latest[SVC]
    assert latest.state == "ANOMALY" and latest.severity == "CRITICAL"       # escalated to CRITICAL
    assert [e for _, e in sink.sent][0] == "created"
    assert rt.repo.get_alert(alert["id"]).peak_severity == "CRITICAL"          # incident kept growing
    assert not [e for _, e in sink.sent if e == "created"][1:]                # ONE created despite many anomalous ticks

    async def recovery():
        await pump(rt, clock, 25, 20, 1)                                      # recover
    await with_dispatcher(rt, recovery())

    resolved = rt.ws.of("alert.resolved")
    assert len(resolved) == 1 and resolved[0]["id"] == alert["id"] and resolved[0]["status"] == "RESOLVED"
    assert [e for _, e in sink.sent][-1] == "resolved"
    assert rt.ws.of("health.update") and rt.stats.parse_errors == 30   # the 30 poison lines: counted, never fatal
    assert rt.repo.get_alert(alert["id"]).status == "RESOLVED"
    assert {d["status"] for d in rt.repo.deliveries_for(alert["id"])} == {"DELIVERED"}


async def test_baseline_stays_flat_through_the_incident(make_runtime, clock):
    rt = make_runtime()
    await pump(rt, clock, 30, 20, 1)
    before = rt.latest[SVC].baseline_mean
    await pump(rt, clock, 40, 20, 8)                                          # long 40 % incident
    after = rt.latest[SVC]
    assert after.state == "ANOMALY" and abs(after.baseline_mean - before) < 0.015


async def test_restart_resumes_from_checkpoint_without_replay_or_duplicate_alert(make_runtime, clock, tmp_path):
    db, log = tmp_path / "logpulse.db", tmp_path / "app.log"
    sink1 = RecordingSink("fake")
    rt1 = make_runtime(sinks={"fake": sink1}, db_path=db, log_path=log)
    async def phase1():
        await pump(rt1, clock, 30, 20, 1)
        await pump(rt1, clock, 10, 20, 8)                                     # incident opens
    await with_dispatcher(rt1, phase1())
    assert len(rt1.ws.of("alert.created")) == 1 and sink1.sent
    lines_before = rt1.stats.lines_read
    rt1.save_checkpoint()                                                     # what the shutdown hook does
    rt1.tailer.close()
    rt1.repo.db.close()

    with open(log, "a", newline="\n") as f:                                   # written while LogPulse was down
        for _ in range(5):
            f.write(ndjson(clock.now, True) + "\n")

    sink2 = RecordingSink("fake")
    rt2 = make_runtime(sinks={"fake": sink2}, db_path=db, log_path=log)
    assert rt2.machine.is_open(SVC)                                           # open alert reloaded
    assert rt2.engine.detector(SVC).baseline.ready                            # baseline rebuilt from SQLite
    assert rt2.engine.detector(SVC).baseline.mean < 0.08                      # ...and it excludes the incident
    async def phase2():
        await pump(rt2, clock, 8, 20, 8)                                      # incident continues after restart
    await with_dispatcher(rt2, phase2())
    assert rt2.tailer.resumed_from_checkpoint
    assert rt2.stats.lines_read == 5 + 8 * 20                                 # only NEW lines; nothing replayed
    assert lines_before == 30 * 20 + 10 * 20
    assert not rt2.ws.of("alert.created") and "created" not in [e for _, e in sink2.sent]   # no duplicate notification
