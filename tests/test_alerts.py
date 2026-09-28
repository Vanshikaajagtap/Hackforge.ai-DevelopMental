import asyncio
import json
from pathlib import Path

import httpx
import pytest
from moto import mock_aws

from app.alerts import build_sinks
from app.alerts.base import alert_payload, render
from app.alerts.dispatcher import Delivery, Dispatcher
from app.alerts.jsonl import JsonlSink
from app.alerts.manager import AlertManager
from app.alerts.ntfy import NtfySink
from app.alerts.telegram import TelegramSink
from app.alerts.webhook import WebhookSink
from app.config import load_settings
from app.detection.state import AlertStateMachine
from app.storage.db import Database
from app.storage.repository import Repository

from conftest import sample_alert, snap


@pytest.fixture
def repo():
    r = Repository(Database(":memory:"))
    yield r
    r.db.close()


# ---- rendering + payload ---------------------------------------------------------------------------
def test_render_answers_what_where_when_how_much_and_evidence():
    text = render(sample_alert(), "created", window_seconds=10)
    assert text.splitlines()[0] == "[CRITICAL] payment-service error rate 38.4%"
    for needle in ("Baseline:   5.1%", "+33.3 pp", "7.5×", "Z-score:    5.18", "284 in last 10 s", "UTC", "Reason:"):
        assert needle in text


def test_render_resolved_and_escalated_headlines():
    assert render(sample_alert(status="RESOLVED", resolved_at=1_790_000_090.0), "resolved").startswith("[RESOLVED] payment-service")
    assert "(escalated)" in render(sample_alert(), "escalated").splitlines()[0]


def test_alert_payload_schema_matches_the_cloudwatch_shape():
    p = alert_payload(sample_alert(), "created")
    assert {"alert_id", "event", "timestamp", "service", "severity", "current_error_rate", "baseline_error_rate",
            "z_score", "relative_change", "sample_size"} <= set(p)
    assert p["alert_id"] == "a8f31" and p["current_error_rate"] == 0.384 and p["relative_change"] == 7.5


# ---- free sinks (HTTP mocked) ----------------------------------------------------------------------
def mock_transport(status=200):
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status)
    return httpx.MockTransport(handler), seen


async def test_ntfy_sink_request_shape_and_priority():
    transport, seen = mock_transport()
    sink = NtfySink("logpulse-topic", window_seconds=10, transport=transport)
    await sink.send(sample_alert(), "created")
    req = seen[0]
    assert str(req.url) == "https://ntfy.sh/logpulse-topic" and req.method == "POST"
    assert req.headers["Priority"] == "urgent" and req.headers["Tags"] == "rotating_light"
    assert req.headers["Title"] == "[CRITICAL] payment-service error rate (created)"
    assert b"payment-service error rate 38.4%" in req.content

    await sink.send(sample_alert(severity="HIGH"), "created")
    assert seen[1].headers["Priority"] == "high"
    await sink.send(sample_alert(status="RESOLVED", severity="MEDIUM"), "resolved")
    assert seen[2].headers["Tags"] == "white_check_mark"
    assert seen[2].headers["Title"] == "[RESOLVED] payment-service error rate (resolved)"


async def test_ntfy_title_stays_ascii_for_odd_service_names():
    transport, seen = mock_transport()
    await NtfySink("t", transport=transport).send(sample_alert(service="paiement-échec"), "created")
    seen[0].headers["Title"].encode("ascii")            # would raise if not ASCII


async def test_http_sinks_raise_on_error_status_so_the_dispatcher_can_retry():
    transport, _ = mock_transport(500)
    for sink in (NtfySink("t", transport=transport), TelegramSink("tok", "1", transport=transport),
                 WebhookSink("https://hooks.example/x", transport=transport)):
        with pytest.raises(httpx.HTTPStatusError):
            await sink.send(sample_alert(), "created")


async def test_telegram_sink_request_shape():
    transport, seen = mock_transport()
    await TelegramSink("123:ABC", "555", window_seconds=10, transport=transport).send(sample_alert(), "created")
    req = seen[0]
    assert str(req.url) == "https://api.telegram.org/bot123:ABC/sendMessage"
    body = json.loads(req.content)
    assert body["chat_id"] == "555" and body["text"].startswith("[CRITICAL] payment-service")


async def test_webhook_sink_posts_discord_and_slack_keys():
    transport, seen = mock_transport()
    await WebhookSink("https://hooks.example/x", transport=transport).send(sample_alert(), "created")
    body = json.loads(seen[0].content)
    assert body["content"] == body["text"] and "payment-service" in body["text"]


async def test_jsonl_sink_appends_one_json_object_per_event(tmp_path):
    path = tmp_path / "sub" / "alerts.jsonl"
    sink = JsonlSink(str(path))
    await sink.send(sample_alert(), "created")
    await sink.send(sample_alert(status="RESOLVED", resolved_at=1_790_000_090.0), "resolved")
    rows = [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines()]
    assert [r["event"] for r in rows] == ["created", "resolved"] and rows[0]["service"] == "payment-service"


# (SNS / CloudWatch sink tests live in test_aws_sinks.py)

# ---- sink registry ---------------------------------------------------------------------------------
def test_build_sinks_skips_unconfigured_and_placeholder_sinks(tmp_path):
    env = {"ALERTS_JSONL": str(tmp_path / "a.jsonl"), "NTFY_TOPIC": "logpulse-CHANGE-ME-long-random"}
    sinks, skipped = build_sinks(load_settings(env=env))
    assert set(sinks) == {"console", "jsonl"}
    assert "placeholder" in skipped["ntfy"] and "TELEGRAM" in skipped["telegram"]


def test_build_sinks_enables_configured_free_sinks_and_gates_aws_behind_the_flag(tmp_path):
    cfg = tmp_path / "c.yaml"
    text = Path("config.yaml").read_text(encoding="utf-8").replace(
        "sinks: [console, jsonl, sns, cloudwatch, ntfy, telegram]",
        "sinks: [console, jsonl, sns, cloudwatch, ntfy, telegram, webhook]")
    assert "webhook" in text
    cfg.write_text(text, encoding="utf-8")
    env = {"ALERTS_JSONL": str(tmp_path / "a.jsonl"), "NTFY_TOPIC": "real-topic-xyz", "TELEGRAM_BOT_TOKEN": "t",
           "TELEGRAM_CHAT_ID": "1", "WEBHOOK_URL": "https://hooks.example/x",
           "SNS_TOPIC_ARN": "arn:aws:sns:ap-south-1:123456789012:logpulse-alerts"}
    sinks, skipped = build_sinks(load_settings(cfg, env=env))
    assert set(sinks) == {"console", "jsonl", "ntfy", "telegram", "webhook"}         # AWS off -> no AWS sinks at all
    assert "AWS_ENABLED" in skipped["sns"] and "AWS_ENABLED" in skipped["cloudwatch"]

    with mock_aws():
        sinks, skipped = build_sinks(load_settings(cfg, env={**env, "AWS_ENABLED": "true"}))
    assert {"sns", "cloudwatch"} <= set(sinks) and not skipped


# ---- dispatcher: retry, FAILED, isolation ----------------------------------------------------------
class FlakySink:
    def __init__(self, name="flaky", fail_times=0):
        self.name, self.fail_times, self.calls, self.sent = name, fail_times, 0, []

    async def send(self, alert, event):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("boom")
        self.sent.append((alert.id, event))


def make_dispatcher(repo, sinks, sleeps=None, **kw):
    async def fake_sleep(s):
        if sleeps is not None:
            sleeps.append(s)
    return Dispatcher(sinks, repo, retry_attempts=3, backoff_seconds=2, sleep=fake_sleep, **kw)


async def run_deliveries(dispatcher, deliveries):
    task = asyncio.create_task(dispatcher.run())
    for d in deliveries:
        dispatcher.enqueue(d)
    await asyncio.wait_for(dispatcher.drain(), 5)
    task.cancel()


def seed(repo, sink_name):
    alert = sample_alert()
    repo.upsert_alert(alert)
    return Delivery(repo.add_delivery(alert.id, "created", sink_name), alert, "created", sink_name)


async def test_sink_failure_retries_with_exponential_backoff_then_delivers(repo):
    sink, sleeps = FlakySink(fail_times=2), []
    d = make_dispatcher(repo, {"flaky": sink}, sleeps)
    delivery = seed(repo, "flaky")
    await run_deliveries(d, [delivery])
    row = repo.deliveries_for("a8f31")[0]
    assert (row["status"], row["attempt_count"], row["error_message"]) == ("DELIVERED", 3, None)
    assert sleeps == [2, 4] and sink.sent == [("a8f31", "created")]
    assert d.stats["flaky"].success == 1 and d.stats["flaky"].failure == 2


async def test_exhausted_retries_mark_the_delivery_failed_with_the_error(repo):
    sink, sleeps = FlakySink(fail_times=99), []
    d = make_dispatcher(repo, {"flaky": sink}, sleeps)
    await run_deliveries(d, [seed(repo, "flaky")])
    row = repo.deliveries_for("a8f31")[0]
    assert row["status"] == "FAILED" and row["attempt_count"] == 3 and "boom" in row["error_message"]
    assert sink.calls == 3 and sleeps == [2, 4]
    assert d.stats["flaky"].last_ok is False


async def test_a_dead_sink_does_not_block_the_others(repo):
    dead, live = FlakySink("dead", fail_times=99), FlakySink("live")
    d = make_dispatcher(repo, {"dead": dead, "live": live})
    alert = sample_alert()
    repo.upsert_alert(alert)
    deliveries = [Delivery(repo.add_delivery(alert.id, "created", n), alert, "created", n) for n in ("dead", "live")]
    await run_deliveries(d, deliveries)
    status = {r["channel"]: r["status"] for r in repo.deliveries_for(alert.id)}
    assert status == {"dead": "FAILED", "live": "DELIVERED"}


async def test_hung_sink_times_out_and_is_retried(repo):
    class Hang:
        name = "hang"
        async def send(self, alert, event):
            await asyncio.sleep(3600)
    d = make_dispatcher(repo, {"hang": Hang()}, send_timeout=0.01)
    await run_deliveries(d, [seed(repo, "hang")])
    assert repo.deliveries_for("a8f31")[0]["status"] == "FAILED"


async def test_unknown_channel_is_marked_failed(repo):
    d = make_dispatcher(repo, {})
    await run_deliveries(d, [seed(repo, "ghost")])
    assert repo.deliveries_for("a8f31")[0]["error_message"] == "sink not configured"


# ---- manager: persist first, detection unaffected -------------------------------------------------
def make_manager(settings, repo, sinks, sleeps=None):
    machine = AlertStateMachine(settings.detector, settings.profile)
    sent = []

    async def broadcast(type_, data):
        sent.append((type_, data))

    holder = {}
    dispatcher = make_dispatcher(repo, sinks, sleeps, on_update=lambda aid: holder["m"].publish_update(aid))
    manager = AlertManager(machine, repo, dispatcher, broadcast, settings.profile.window_seconds)
    holder["m"] = manager
    return manager, dispatcher, sent


async def test_alert_and_pending_delivery_rows_exist_before_the_sink_is_called(settings, repo):
    observed = {}

    class Spy:
        name = "spy"
        async def send(self, alert, event):
            observed["alert"] = repo.get_alert(alert.id)
            observed["rows"] = repo.deliveries_for(alert.id)

    manager, dispatcher, _ = make_manager(settings, repo, {"spy": Spy()})
    task = asyncio.create_task(dispatcher.run())
    await manager.process([snap(ts=0, sev="CRITICAL", z=5.2, ratio=7.5, rate=0.38)])
    await asyncio.wait_for(dispatcher.drain(), 5)
    task.cancel()
    assert observed["alert"] is not None and observed["alert"].status == "OPEN"
    assert [(r["status"], r["event"]) for r in observed["rows"]] == [("PENDING", "created")]


async def test_broken_or_hung_sinks_never_delay_detection(settings, repo):
    class Hang:
        name = "hang"
        async def send(self, alert, event):
            await asyncio.sleep(3600)

    manager, dispatcher, sent = make_manager(settings, repo, {"hang": Hang(), "flaky": FlakySink(fail_times=99)})
    task = asyncio.create_task(dispatcher.run())
    await asyncio.wait_for(manager.process([snap(ts=0, sev="CRITICAL", z=5.2, ratio=7.5, rate=0.38)]), 1)
    assert [t for t, _ in sent] == ["alert.created"]                 # UI already told, sinks still stuck
    task.cancel()


async def test_ws_messages_and_delivery_badges_follow_the_lifecycle(settings, repo):
    sink = FlakySink("ntfy")
    manager, dispatcher, sent = make_manager(settings, repo, {"ntfy": sink})
    task = asyncio.create_task(dispatcher.run())
    crit = dict(sev="CRITICAL", z=5.2, ratio=7.5, rate=0.38)
    await manager.process([snap(ts=0, **crit)])
    await manager.process([snap(ts=1, sev="HIGH", z=3.5, ratio=3.0, rate=0.17)])         # quiet current-severity change
    for i in range(2, 5):
        await manager.process([snap(ts=i, sev="NONE", z=0.2, ratio=1.0)])
    await asyncio.wait_for(dispatcher.drain(), 5)
    task.cancel()
    types = [t for t, _ in sent]
    assert types[0] == "alert.created" and "alert.resolved" in types and "alert.updated" in types
    assert [e for _, e in sink.sent] == ["created", "resolved"]                          # not one message per tick
    final = [d for t, d in sent if t == "alert.updated"][-1]
    assert {r["status"] for r in final["deliveries"]} == {"DELIVERED"}


async def test_restart_reenqueues_pending_deliveries_and_reloads_open_alerts(settings, repo):
    alert = sample_alert()
    repo.upsert_alert(alert)
    repo.add_delivery(alert.id, "created", "ntfy")                # crashed before it was sent: still PENDING
    sink = FlakySink("ntfy")
    manager, dispatcher, _ = make_manager(settings, repo, {"ntfy": sink})
    manager.restore()
    assert manager.machine.is_open("payment-service")
    task = asyncio.create_task(dispatcher.run())
    await asyncio.wait_for(dispatcher.drain(), 5)
    task.cancel()
    assert sink.sent == [("a8f31", "created")]
    assert repo.deliveries_for("a8f31")[0]["status"] == "DELIVERED"
