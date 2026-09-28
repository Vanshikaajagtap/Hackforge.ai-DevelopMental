import pytest

from app.detection.baseline import Baseline
from app.detection.detector import DetectionEngine, ServiceDetector

from conftest import make_event, run_seconds


def test_baseline_mean_and_std_on_known_samples():
    b = Baseline(max_samples=30, min_samples=3, std_floor=0.02, ratio_floor=0.005)
    for r in (0.04, 0.06, 0.04, 0.06):
        b.add(r)
    assert b.mean == pytest.approx(0.05)
    assert b.std == pytest.approx(0.01)
    assert b.z(0.09) == pytest.approx((0.09 - 0.05) / 0.02)   # sigma 0.01 is floored to 0.02
    assert b.ratio(0.10) == pytest.approx(2.0)


def test_std_floor_applies_to_a_perfectly_steady_baseline():
    b = Baseline(30, 3, std_floor=0.02, ratio_floor=0.005)
    for _ in range(10):
        b.add(0.05)
    assert b.std == 0.0
    assert b.z(0.07) == pytest.approx(1.0)                    # not infinity


def test_baseline_is_a_bounded_rolling_window():
    b = Baseline(max_samples=3, min_samples=1, std_floor=0.02, ratio_floor=0.005)
    for r in (0.5, 0.1, 0.1, 0.1):
        b.add(r)
    assert b.n == 3 and b.mean == pytest.approx(0.1)


def test_ratio_uses_floor_for_zero_baseline():
    b = Baseline(30, 1, 0.02, 0.005)
    b.add(0.0)
    assert b.ratio(0.05) == pytest.approx(10.0)


def healthy(settings, clock, service="svc"):
    det = ServiceDetector(service, settings, clock)
    run_seconds(det, clock, 30, events_per_sec=20, errors_per_sec=1)      # 5 % errors, ~200 ev / 10 s
    return det


def test_state_is_warmup_until_enough_baseline_samples(settings, clock):
    det = ServiceDetector("svc", settings, clock)
    first = run_seconds(det, clock, 6, 20, 1)
    assert {s.state for s in first} == {"WARMUP"}
    assert all(s.z is None and s.severity == "NONE" for s in first)   # alerts suppressed while learning
    assert healthy(settings, type(clock)()).baseline.ready


def test_low_data_when_below_min_events(settings, clock):
    det = healthy(settings, clock)
    snaps = run_seconds(det, clock, 15, events_per_sec=0, errors_per_sec=0)  # service goes silent
    assert snaps[-1].total == 0 and snaps[-1].state == "LOW_DATA"            # decays instead of freezing


def test_few_events_never_alert_even_at_100_percent_errors(settings, clock):
    det = healthy(settings, clock)
    run_seconds(det, clock, 15, 0, 0)
    snaps = run_seconds(det, clock, 3, events_per_sec=5, errors_per_sec=5)   # 15 events, all errors
    assert all(s.state == "LOW_DATA" and s.severity == "NONE" for s in snaps)


def test_low_data_windows_are_not_admitted_to_baseline(settings, clock):
    det = healthy(settings, clock)
    run_seconds(det, clock, 12, 0, 0)                                        # old healthy events age out of the window
    n = det.baseline.n
    run_seconds(det, clock, 20, events_per_sec=1, errors_per_sec=1)          # < min_events, 100 % errors
    assert det.baseline.n == n


def test_traffic_only_spike_is_not_an_anomaly(settings, clock):
    det = healthy(settings, clock)
    snaps = run_seconds(det, clock, 20, events_per_sec=100, errors_per_sec=5)   # 5x volume, same 5 %
    assert {s.state for s in snaps} == {"NORMAL"}
    assert max(s.total for s in snaps) > 900


def test_anomalous_windows_are_not_admitted_to_baseline(settings, clock):
    det = healthy(settings, clock)
    mean_before = det.baseline.mean
    snaps = run_seconds(det, clock, 30, events_per_sec=20, errors_per_sec=8)    # 40 % errors
    assert snaps[-1].state == "ANOMALY" and snaps[-1].severity == "CRITICAL"
    n_at_anomaly = det.baseline.n
    run_seconds(det, clock, 20, 20, 8)
    assert det.baseline.n == n_at_anomaly                                        # frozen during the incident
    assert det.baseline.mean < mean_before + 0.02                                # stayed flat


def test_warmup_ceiling_blocks_startup_incident(settings, clock):
    det = ServiceDetector("svc", settings, clock)
    snaps = run_seconds(det, clock, 40, events_per_sec=20, errors_per_sec=8)     # 40 % from the start
    assert det.baseline.n == 0
    assert {s.state for s in snaps} == {"WARMUP"}


def test_baseline_is_frozen_while_an_alert_is_open(settings, clock):
    det = healthy(settings, clock)
    n = det.baseline.n
    det.frozen = True                                                            # what the engine sets while OPEN
    run_seconds(det, clock, 20, 20, 1)                                           # perfectly healthy traffic
    assert det.baseline.n == n
    det.frozen = False
    run_seconds(det, clock, 6, 20, 1)
    assert det.baseline.n > n


def test_late_events_are_dropped_and_counted(settings, clock):
    det = ServiceDetector("svc", settings, clock)
    assert det.on_event(make_event(clock.now - 60)) is False
    assert det.on_event(make_event(clock.now - 1)) is True
    assert det.late_events == 1 and det.window.total == 1


def test_level_shift_reset_relearns_from_the_current_level_once(settings, clock):
    det = healthy(settings, clock)
    run_seconds(det, clock, 12, 20, 8)                                           # the window is now full of 40 %...
    det.reset_baseline()                                                         # ...when the level shift is adopted
    assert det.baseline.n == 0
    snaps = run_seconds(det, clock, 30, 20, 8)                                   # 40 % is now the new normal
    assert snaps[-1].state == "NORMAL" and det.baseline.mean == pytest.approx(0.4, abs=0.02)


def test_restored_samples_skip_warmup(settings, clock):
    det = ServiceDetector("svc", settings, clock)
    det.restore_samples([0.05] * settings.profile.min_baseline_samples)
    snaps = run_seconds(det, clock, 3, 20, 1)
    assert snaps[-1].state == "NORMAL"


def test_silent_service_is_flagged_after_n_windows(settings, clock):
    eng = DetectionEngine(settings, clock)
    det = eng.detector("svc")
    run_seconds(det, clock, 30, 20, 1)
    assert eng.silent_services() == []
    run_seconds(det, clock, 25, 0, 0)                                            # 25 s < 3 x 10 s window: not yet
    assert eng.silent_services() == []
    run_seconds(det, clock, 10, 0, 0)                                            # 35 s > 3 x 10 s window
    assert eng.silent_services() == ["svc"]


def test_engine_tracks_services_independently(settings, clock):
    eng = DetectionEngine(settings, clock)
    eng.on_event(make_event(clock.now, service="a"))
    eng.on_event(make_event(clock.now, True, service="b"))
    snaps = {s.service: s for s in eng.tick()}
    assert (snaps["a"].errors, snaps["b"].errors) == (0, 1)


def test_engine_freezes_detectors_via_callback(settings, clock):
    open_services = {"a"}
    eng = DetectionEngine(settings, clock, frozen_fn=lambda s: s in open_services)
    eng.on_event(make_event(clock.now, service="a"))
    eng.on_event(make_event(clock.now, service="b"))
    eng.tick()
    assert eng.services["a"].frozen and not eng.services["b"].frozen
