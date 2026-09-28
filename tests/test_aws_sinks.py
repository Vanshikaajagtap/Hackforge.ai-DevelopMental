"""AWS sinks against moto (no account, no network), plus the fail-soft guarantees: AWS being down, mis-set or
unreachable must never affect detection or the other channels."""
import asyncio
import json
from datetime import datetime, timezone

import boto3
import pytest
from botocore.exceptions import EndpointConnectionError
from moto import mock_aws

from app.alerts import build_sinks
from app.alerts.aws_common import aws_startup_check
from app.alerts.cloudwatch import CloudWatchSink
from app.alerts.dispatcher import Delivery, Dispatcher
from app.alerts.jsonl import JsonlSink
from app.alerts.manager import AlertManager
from app.alerts.sns import SnsSink, _subject
from app.config import load_settings
from app.detection.state import AlertStateMachine
from app.storage.db import Database
from app.storage.repository import Repository

from conftest import sample_alert, snap

REGION = "ap-south-1"
GROUP = "/logpulse/alerts"


@pytest.fixture
def repo():
    return Repository(Database(":memory:"))


def make_topic() -> str:
    return boto3.client("sns", region_name=REGION).create_topic(Name="logpulse-alerts")["TopicArn"]


def today() -> str:
    return f"{datetime.now(timezone.utc):%Y-%m-%d}"


# ---- SNS ---------------------------------------------------------------------------------------------
async def test_sns_publish_returns_the_message_id():
    with mock_aws():
        msg_id = await SnsSink(make_topic(), REGION, window_seconds=10).send(sample_alert(), "created")
    assert isinstance(msg_id, str) and len(msg_id) > 8


async def test_sns_message_carries_subject_body_and_filterable_attributes():
    with mock_aws():
        arn = make_topic()
        sqs = boto3.client("sqs", region_name=REGION)
        queue = sqs.create_queue(QueueName="probe")["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        boto3.client("sns", region_name=REGION).subscribe(TopicArn=arn, Protocol="sqs", Endpoint=queue_arn)
        await SnsSink(arn, REGION, window_seconds=10).send(sample_alert(), "created")
        body = json.loads(sqs.receive_message(QueueUrl=queue, MaxNumberOfMessages=1)["Messages"][0]["Body"])
    assert body["Subject"] == "[CRITICAL] payment-service error rate (created)"
    assert "Z-score:    5.18" in body["Message"] and "284 in last 10 s" in body["Message"]
    attrs = {k: v["Value"] for k, v in body["MessageAttributes"].items()}
    assert attrs == {"severity": "CRITICAL", "service": "payment-service", "event": "created"}


def test_sns_subject_is_ascii_single_line_and_short():
    a = sample_alert(service="payément-service\nline2" + "x" * 200)
    s = _subject(a, "created")
    assert s.isascii() and len(s) < 100 and "\n" not in s and s.startswith("[CRITICAL] payment-service")


async def test_sns_to_a_missing_topic_raises_so_the_dispatcher_can_retry():
    with mock_aws(), pytest.raises(Exception):
        await SnsSink(f"arn:aws:sns:{REGION}:123456789012:does-not-exist", REGION).send(sample_alert(), "created")


async def test_sns_healthcheck():
    with mock_aws():
        await SnsSink(make_topic(), REGION).healthcheck()
        with pytest.raises(Exception):
            await SnsSink(f"arn:aws:sns:{REGION}:123456789012:nope", REGION).healthcheck()


# ---- CloudWatch --------------------------------------------------------------------------------------
async def test_cloudwatch_writes_structured_json_into_a_per_day_stream():
    with mock_aws():
        logs = boto3.client("logs", region_name=REGION)
        logs.create_log_group(logGroupName=GROUP)
        sink = CloudWatchSink(GROUP, region=REGION)
        ext1 = await sink.send(sample_alert(), "created")
        ext2 = await sink.send(sample_alert(), "escalated")                 # stream already exists: tolerated
        streams = [s["logStreamName"] for s in logs.describe_log_streams(logGroupName=GROUP)["logStreams"]]
        events = logs.get_log_events(logGroupName=GROUP, logStreamName=f"alerts/{today()}")["events"]
    assert streams == [f"alerts/{today()}"] and ext1 == ext2 == f"{GROUP}:alerts/{today()}"
    bodies = [json.loads(e["message"]) for e in events]
    assert [b["event"] for b in bodies] == ["created", "escalated"]
    assert bodies[0]["severity"] == "CRITICAL" and "z_score" in bodies[0] and bodies[0]["service"] == "payment-service"


async def test_cloudwatch_custom_stream_prefix():
    with mock_aws():
        boto3.client("logs", region_name=REGION).create_log_group(logGroupName=GROUP)
        ext = await CloudWatchSink(GROUP, "logpulse-demo", REGION).send(sample_alert(), "created")
    assert ext == f"{GROUP}:logpulse-demo/{today()}"


async def test_cloudwatch_with_a_missing_log_group_raises():
    with mock_aws(), pytest.raises(Exception):
        await CloudWatchSink("/does/not/exist", region=REGION).send(sample_alert(), "created")


async def test_cloudwatch_recreates_a_stream_deleted_behind_its_back():
    with mock_aws():
        logs = boto3.client("logs", region_name=REGION)
        logs.create_log_group(logGroupName=GROUP)
        sink = CloudWatchSink(GROUP, region=REGION)
        await sink.send(sample_alert(), "created")
        logs.delete_log_stream(logGroupName=GROUP, logStreamName=f"alerts/{today()}")
        await sink.send(sample_alert(), "escalated")                        # cached "ready" flag must not wedge us
        events = logs.get_log_events(logGroupName=GROUP, logStreamName=f"alerts/{today()}")["events"]
    assert [json.loads(e["message"])["event"] for e in events] == ["escalated"]


async def test_cloudwatch_healthcheck_needs_only_the_write_permissions():
    with mock_aws():
        boto3.client("logs", region_name=REGION).create_log_group(logGroupName=GROUP)
        await CloudWatchSink(GROUP, region=REGION).healthcheck()
        with pytest.raises(Exception):
            await CloudWatchSink("/nope", region=REGION).healthcheck()


# ---- dispatcher stores the external id ---------------------------------------------------------------
async def test_dispatcher_records_external_ids_as_proof_of_delivery(repo):
    with mock_aws():
        boto3.client("logs", region_name=REGION).create_log_group(logGroupName=GROUP)
        sinks = {"sns": SnsSink(make_topic(), REGION), "cloudwatch": CloudWatchSink(GROUP, region=REGION)}
        d = Dispatcher(sinks, repo, retry_attempts=3, backoff_seconds=0)
        alert = sample_alert()
        repo.upsert_alert(alert)
        task = asyncio.create_task(d.run())
        for ch in sinks:
            d.enqueue(Delivery(repo.add_delivery(alert.id, "created", ch), alert, "created", ch))
        await asyncio.wait_for(d.drain(), 10)
        task.cancel()
    rows = {r["channel"]: r for r in repo.deliveries_for(alert.id)}
    assert rows["sns"]["status"] == rows["cloudwatch"]["status"] == "DELIVERED"
    assert len(rows["sns"]["external_id"]) > 8                               # SNS MessageId
    assert rows["cloudwatch"]["external_id"] == f"{GROUP}:alerts/{today()}"


# ---- AWS down: fail-soft, detection and other channels unaffected -------------------------------------
class UnreachableClient:
    """Raises what botocore raises when AWS cannot be reached (no route / Wi-Fi off / DNS failure). It borrows the
    real client's `exceptions` namespace so the sinks' except-clauses behave exactly as they do in production.
    (Real sockets to a closed port work too, but Windows takes ~2 s per refused connect, which makes the suite crawl.)"""

    def __init__(self, service: str) -> None:
        self.exceptions = boto3.client(service, region_name=REGION).exceptions

    def _down(self, *args, **kwargs):
        raise EndpointConnectionError(endpoint_url=f"https://{REGION}.amazonaws.com")

    publish = get_topic_attributes = create_log_stream = put_log_events = _down


def unreachable_aws_sinks():
    return (SnsSink(f"arn:aws:sns:{REGION}:123456789012:logpulse-alerts", REGION, client=UnreachableClient("sns")),
            CloudWatchSink(GROUP, region=REGION, client=UnreachableClient("logs")))


async def test_aws_outage_fails_the_aws_deliveries_but_not_detection_or_the_other_channels(settings, repo, tmp_path):
    sns, cw = unreachable_aws_sinks()
    jsonl = JsonlSink(str(tmp_path / "alerts.jsonl"))
    sent, sleeps = [], []

    async def broadcast(t, d):
        sent.append((t, d))

    async def fake_sleep(s):
        sleeps.append(s)

    holder = {}
    dispatcher = Dispatcher({"sns": sns, "cloudwatch": cw, "jsonl": jsonl}, repo, retry_attempts=3, backoff_seconds=2,
                            sleep=fake_sleep, on_update=lambda aid: holder["m"].publish_update(aid))
    manager = AlertManager(AlertStateMachine(settings.detector, settings.profile), repo, dispatcher, broadcast,
                           settings.profile.window_seconds)
    holder["m"] = manager
    task = asyncio.create_task(dispatcher.run())
    # detection/alerting returns immediately even though both AWS sinks are dead
    await asyncio.wait_for(manager.process([snap(ts=0, sev="CRITICAL", z=5.2, ratio=7.5, rate=0.38)]), 1)
    assert [t for t, _ in sent] == ["alert.created"]
    await asyncio.wait_for(dispatcher.drain(), 60)
    task.cancel()

    alert_id = sent[0][1]["id"]
    rows = {r["channel"]: r for r in repo.deliveries_for(alert_id)}
    assert rows["jsonl"]["status"] == "DELIVERED"                            # redundant channel still delivered
    for ch in ("sns", "cloudwatch"):
        assert rows[ch]["status"] == "FAILED" and rows[ch]["attempt_count"] == 3 and rows[ch]["error_message"]
    assert (tmp_path / "alerts.jsonl").read_text().count('"event": "created"') == 1
    assert repo.get_alert(alert_id).status == "OPEN"                         # persisted first, regardless of AWS
    assert dispatcher.stats["sns"].last_ok is False and dispatcher.stats["jsonl"].last_ok is True


# ---- fail-soft startup check --------------------------------------------------------------------------
async def test_startup_check_reports_ok_for_a_healthy_setup():
    seen = {}
    with mock_aws():
        boto3.client("logs", region_name=REGION).create_log_group(logGroupName=GROUP)
        sinks = {"sns": SnsSink(make_topic(), REGION), "cloudwatch": CloudWatchSink(GROUP, region=REGION)}
        await aws_startup_check(sinks, seen.__setitem__)
    assert seen == {"aws_identity": "ok", "sns": "ok", "cloudwatch": "ok"}


async def test_startup_check_reports_each_resource_independently():
    seen = {}
    with mock_aws():
        sinks = {"sns": SnsSink(f"arn:aws:sns:{REGION}:123456789012:missing", REGION),
                 "cloudwatch": CloudWatchSink("/missing/group", region=REGION)}
        await aws_startup_check(sinks, seen.__setitem__)
    assert seen["aws_identity"] == "ok"
    assert seen["sns"].startswith("error") and seen["cloudwatch"].startswith("error")


async def test_startup_check_never_raises_when_credentials_are_bad(monkeypatch):
    def boom(region=None):
        raise RuntimeError("Unable to locate credentials")
    monkeypatch.setattr("app.alerts.aws_common.aws_session", boom)
    with mock_aws():
        sinks = {"sns": SnsSink(make_topic(), REGION, client=object())}
        seen = {}
        await aws_startup_check(sinks, seen.__setitem__)                    # must not raise
    assert seen["aws_identity"].startswith("error: RuntimeError") and seen["sns"].startswith("unchecked")


async def test_startup_check_is_a_noop_without_aws_sinks():
    seen = {}
    await aws_startup_check({"jsonl": JsonlSink("data/_never_created.jsonl")}, seen.__setitem__)
    assert seen == {}


# ---- factory ----------------------------------------------------------------------------------------
def test_aws_sinks_are_built_only_when_enabled_and_configured(tmp_path):
    base = {"ALERTS_JSONL": str(tmp_path / "a.jsonl"),
            "SNS_TOPIC_ARN": f"arn:aws:sns:{REGION}:123456789012:logpulse-alerts", "CW_LOG_GROUP": GROUP}
    sinks, skipped = build_sinks(load_settings(env=base))                                       # AWS_ENABLED unset
    assert not {"sns", "cloudwatch"} & set(sinks) and "AWS_ENABLED" in skipped["sns"]

    with mock_aws():
        sinks, _ = build_sinks(load_settings(env={**base, "AWS_ENABLED": "true", "CW_LOG_STREAM_PREFIX": "demo"}))
        assert list(sinks)[:4] == ["console", "jsonl", "sns", "cloudwatch"]                     # config order
        assert sinks["cloudwatch"].prefix == "demo" and sinks["cloudwatch"].region == REGION

        placeholder = {**base, "AWS_ENABLED": "true", "SNS_TOPIC_ARN": "arn:aws:sns:ap-south-1:<ACCOUNT_ID>:logpulse-alerts"}
        sinks, skipped = build_sinks(load_settings(env=placeholder))
        assert "sns" not in sinks and "placeholder" in skipped["sns"] and "cloudwatch" in sinks

        sinks, skipped = build_sinks(load_settings(env={**base, "AWS_ENABLED": "true", "SNS_TOPIC_ARN": ""}))
        assert "sns" not in sinks and "SNS_TOPIC_ARN" in skipped["sns"]


def test_dotenv_aws_credentials_are_exported_for_boto3_but_empty_values_are_not(tmp_path, monkeypatch):
    import os
    import app.config as config
    original_config = config.ROOT / "config.yaml"
    (tmp_path / ".env").write_text("AWS_ACCESS_KEY_ID=test-key-not-real\nAWS_SECRET_ACCESS_KEY=\nAWS_SESSION_TOKEN=\n")
    monkeypatch.setattr(config, "ROOT", tmp_path)
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(k, "x")          # registers cleanup so nothing leaks into other tests...
        monkeypatch.delenv(k)               # ...then start from "unset"
    config.load_settings(path=original_config)              # env=None -> reads the (patched) ROOT/.env like a real start
    assert os.environ["AWS_ACCESS_KEY_ID"] == "test-key-not-real"
    assert "AWS_SECRET_ACCESS_KEY" not in os.environ and "AWS_SESSION_TOKEN" not in os.environ
