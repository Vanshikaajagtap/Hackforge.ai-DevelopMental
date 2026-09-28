"""REST + WebSocket + static UI through the real FastAPI app (real tasks, real generator, temp files)."""
from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture
def client_factory(tmp_path, settings):
    def factory(demo=True, sinks=None):
        s = replace(settings, demo_mode=demo, log_path=str(tmp_path / "app.log"), db_path=str(tmp_path / "lp.db"),
                    alerts_jsonl=str(tmp_path / "alerts.jsonl"))
        return TestClient(create_app(s, sinks={} if sinks is None else sinks))
    return factory


def test_health_and_index(client_factory):
    with client_factory() as c:
        assert c.get("/health").json() == {"status": "ok"}
        page = c.get("/")
        assert page.status_code == 200 and "LogPulse" in page.text
        assert c.get("/static/chart.umd.min.js").status_code == 200          # vendored, no CDN
        assert c.get("/docs").status_code == 200                              # FastAPI auto docs


def test_system_status_reports_health_metrics(client_factory):
    with client_factory() as c:
        s = c.get("/api/system/status").json()
    for key in ("status", "queue_depth", "parse_errors", "late_events", "dropped_events", "tail_lag_ms", "sinks",
                "db_status", "file_status", "last_event_age_seconds", "events_per_second", "profile", "demo_mode"):
        assert key in s
    assert s["profile"] == "demo" and s["db_status"] == "ok"


def test_alert_endpoints_empty_and_404(client_factory):
    with client_factory() as c:
        assert c.get("/api/alerts").json() == []
        assert c.get("/api/alerts/nope").status_code == 404
        assert c.get("/api/alerts?status=BOGUS").status_code == 422
        assert c.get("/api/metrics/current").json() == []
        assert c.get("/api/metrics/history", params={"service": "x", "minutes": 5}).json() == []


def test_demo_scenario_endpoint(client_factory):
    with client_factory() as c:
        for name in ("normal", "traffic_spike", "error_spike", "recover", "mixed", "malformed"):
            r = c.post("/api/demo/scenario", json={"name": name})
            assert r.status_code == 200 and r.json() == {"scenario": name}
        assert c.post("/api/demo/scenario", json={"name": "explode"}).status_code == 422


def test_demo_endpoint_is_absent_when_demo_mode_is_off(client_factory):
    with client_factory(demo=False) as c:
        assert c.post("/api/demo/scenario", json={"name": "normal"}).status_code == 404


def test_websocket_hello_then_live_metric_updates(client_factory):
    with client_factory() as c, c.websocket_connect("/ws") as ws:
        hello = ws.receive_json()
        assert hello["type"] == "hello"
        assert {"config", "services", "snapshots", "history", "alerts", "health"} <= set(hello["data"])
        assert hello["data"]["config"]["demo_mode"] is True
        ws.send_json({"type": "ping"})                                        # keep-alive is accepted
        seen, metric = set(), None
        for _ in range(40):                                                   # the real generator -> file -> tailer -> detector
            msg = ws.receive_json()
            seen.add(msg["type"])
            if msg["type"] == "metric.update":
                metric = metric or msg["data"]
            if metric and "health.update" in seen:
                break
        assert "health.update" in seen and metric is not None
        assert {"ts", "service", "total", "errors", "error_rate", "baseline_mean", "baseline_std", "z", "ratio",
                "state", "severity"} == set(metric)
        assert metric["state"] == "WARMUP"


def test_test_alert_goes_through_the_real_dispatcher_and_every_sink(client_factory):
    import time
    from conftest import RecordingSink
    fake, other = RecordingSink("fake"), RecordingSink("other")
    with client_factory(sinks={"fake": fake, "other": other}) as c:
        r = c.post("/api/demo/test-alert")
        assert r.status_code == 200
        body = r.json()
        assert body["queued"] is True and body["alert_id"].startswith("test-") and body["sinks"] == ["fake", "other"]
        for _ in range(50):                                                   # delivery is asynchronous
            view = c.get(f"/api/alerts/{body['alert_id']}").json()
            if len(view["deliveries"]) == 2 and all(d["status"] == "DELIVERED" for d in view["deliveries"]):
                break
            time.sleep(0.1)
        assert {d["channel"] for d in view["deliveries"]} == {"fake", "other"}
        assert view["service"] == "test-service" and "TEST ALERT" in view["reason"]
        assert c.get("/api/alerts?status=OPEN").json() == []                  # never an active incident
        assert c.get("/api/system/status").json()["active_alerts"] == 0
    assert fake.sent == [(body["alert_id"], "created")] == other.sent


def test_test_alert_is_absent_when_demo_mode_is_off(client_factory):
    with client_factory(demo=False) as c:
        assert c.post("/api/demo/test-alert").status_code == 404


def test_system_status_exposes_the_aws_panel(client_factory):
    with client_factory() as c:
        aws = c.get("/api/system/status").json()["aws"]
    assert aws["enabled"] is False and aws["sns"] == "off"


def test_dashboard_has_the_test_alert_button_and_aws_row(client_factory):
    with client_factory() as c:
        page = c.get("/").text
    assert 'id="test-alert"' in page and "Send test alert" in page and 'id="h-aws"' in page and "external_id" in page
