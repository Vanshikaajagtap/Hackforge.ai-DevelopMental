from conftest import RecordingSink
from test_e2e import ndjson, pump


async def test_healthy_when_everything_is_fine(make_runtime, clock):
    rt = make_runtime()
    await pump(rt, clock, 10, 20, 1)
    h = rt.health.report()
    assert h["status"] == "HEALTHY" and h["reasons"] == []
    assert h["events_processed"] == 200 and h["events_per_second"] == 20.0
    assert h["db_status"] == "ok" and h["file_status"] == "ok" and h["last_event_age_seconds"] == 1.0
    assert h["queue_depth"] == 0 and h["dropped_events"] == 0


async def test_down_when_the_log_file_is_missing(make_runtime):
    rt = make_runtime()
    h = rt.health.report()
    assert h["status"] == "DOWN" and h["file_status"] == "missing" and "log file missing" in h["reasons"][0]


async def test_down_when_the_database_is_unwritable(make_runtime, clock):
    rt = make_runtime()
    await pump(rt, clock, 2, 5, 0)
    rt.repo.db.close()
    rt.repo.ping()
    assert rt.health.report()["status"] == "DOWN" and rt.health.report()["db_status"] == "error"


async def test_degraded_when_a_sink_is_failing(make_runtime, clock):
    rt = make_runtime(sinks={"fake": RecordingSink("fake", fail=True)})
    await pump(rt, clock, 2, 5, 0)
    rt.dispatcher.stats["fake"].failure += 1
    rt.dispatcher.stats["fake"].last_ok = False
    h = rt.health.report()
    assert h["status"] == "DEGRADED" and h["sinks"]["fake"]["status"] == "failing" and "sink failing" in h["reasons"][0]


async def test_disabled_sinks_are_reported_not_hidden(make_runtime, clock):
    rt = make_runtime()
    rt.health._skipped = {"ntfy": "NTFY_TOPIC not set"}
    await pump(rt, clock, 1, 1, 0)
    assert rt.health.report()["sinks"]["ntfy"]["status"] == "disabled"


async def test_backpressure_drops_the_oldest_and_degrades_health(make_runtime, clock):
    rt = make_runtime(queue_max=10)
    for i in range(13):
        await rt.ingest_line(ndjson(clock.now + i, False))                    # nobody consumes: queue overflows
    h = rt.health.report()
    assert rt.queue.qsize() == 10 and h["dropped_events"] == 3
    assert rt.queue.get_nowait().ts == clock.now + 3                          # the OLDEST three were dropped


async def test_queue_at_80_percent_is_degraded(make_runtime, clock):
    rt = make_runtime(queue_max=10)
    open(rt.settings.log_path, "a").close()                                   # the file exists, so we're not DOWN
    for i in range(8):
        await rt.ingest_line(ndjson(clock.now, False))
    h = rt.health.report()
    assert h["status"] == "DEGRADED" and "queue at 8/10" in h["reasons"][0]


async def test_late_events_and_parse_errors_are_counted(make_runtime, clock):
    rt = make_runtime()
    await pump(rt, clock, 1, 1, 0)
    await rt.ingest_line("garbage")
    await rt.ingest_line(ndjson(clock.now - 600, False))                      # 10 minutes old
    while not rt.queue.empty():
        rt.engine.on_event(rt.queue.get_nowait())
    h = rt.health.report()
    assert h["parse_errors"] == 1 and h["late_events"] == 1


async def test_silent_service_is_surfaced_but_does_not_degrade(make_runtime, clock):
    rt = make_runtime()
    await pump(rt, clock, 30, 20, 1)
    await pump(rt, clock, 35, 0, 0)
    h = rt.health.report()
    assert h["silent_services"] == ["payment-service"] and h["status"] == "HEALTHY"


async def test_aws_panel_is_off_when_aws_is_disabled(make_runtime):
    rt = make_runtime()
    assert rt.health.report()["aws"] == {"enabled": False, "aws_identity": "off", "sns": "off", "cloudwatch": "off"}


async def test_aws_panel_shows_checking_then_the_startup_check_results(make_runtime, clock):
    rt = make_runtime(sinks={"sns": RecordingSink("sns"), "cloudwatch": RecordingSink("cloudwatch")}, aws_enabled=True)
    await pump(rt, clock, 2, 5, 0)
    assert rt.health.report()["aws"] == {"enabled": True, "aws_identity": "checking...", "sns": "checking...",
                                         "cloudwatch": "checking..."}
    for name in ("aws_identity", "sns", "cloudwatch"):
        rt.health.set_aws(name, "ok")
    h = rt.health.report()
    assert h["aws"]["sns"] == "ok" and h["status"] == "HEALTHY" and not h["reasons"]


async def test_a_failed_aws_check_is_shown_but_fail_soft(make_runtime, clock):
    rt = make_runtime(sinks={"sns": RecordingSink("sns"), "jsonl": RecordingSink("jsonl")}, aws_enabled=True)
    await pump(rt, clock, 2, 5, 0)
    rt.health.set_aws("aws_identity", "error: NoCredentialsError: Unable to locate credentials")
    rt.health.set_aws("sns", "unchecked: no valid credentials")
    h = rt.health.report()
    assert h["aws"]["aws_identity"].startswith("error") and h["aws"]["cloudwatch"] == "not configured"
    assert h["status"] == "HEALTHY"                                   # does not take the monitor down or degrade it
    assert any("aws check failed" in r for r in h["reasons"])         # ...but it is visible
