import pytest

from app.detection.detector import DetectionEngine
from app.detection.state import AlertStateMachine, build_evidence

from conftest import make_event, run_seconds, snap


@pytest.fixture
def machine(settings):
    ids = iter(f"a{i}" for i in range(100))
    return AlertStateMachine(settings.detector, settings.profile, id_factory=lambda: next(ids))


def feed(machine, snaps):
    """Evaluate a sequence; return only the transitions (with their tick index)."""
    return [(i, t) for i, s in enumerate(snaps) if (t := machine.evaluate(s)) is not None]


HIGH = dict(sev="HIGH", z=3.5, ratio=3.4, rate=0.17)
CRIT = dict(sev="CRITICAL", z=5.2, ratio=7.5, rate=0.38)
MED = dict(sev="MEDIUM", z=2.3, ratio=2.0, rate=0.11)
CALM = dict(sev="NONE", z=0.3, ratio=1.0, rate=0.05)


def test_open_needs_confirm_ticks_but_critical_opens_immediately(machine):
    assert feed(machine, [snap(ts=0, **HIGH)]) == []                        # 1 tick: pending
    out = feed(machine, [snap(ts=1, **HIGH)])
    assert [t.kind for _, t in out] == ["created"] and out[0][1].notify

    m2 = AlertStateMachine(machine._cfg, machine._profile)
    assert [t.kind for _, t in feed(m2, [snap(ts=0, **CRIT)])] == ["created"]


def test_pending_streak_is_broken_by_a_calm_tick(machine):
    assert feed(machine, [snap(ts=0, **HIGH), snap(ts=1, **CALM), snap(ts=2, **HIGH)]) == []
    assert not machine.open


def test_dedup_same_anomaly_notifies_once_and_escalation_notifies_again(machine):
    out = feed(machine, [snap(ts=i, **HIGH) for i in range(10)])
    assert [t.kind for _, t in out if t.notify] == ["created"]              # 10 anomalous ticks -> ONE notification
    out = feed(machine, [snap(ts=20, **CRIT)])
    assert [(t.kind, t.notify) for _, t in out] == [("escalated", True)]    # severity up -> second notification
    alert = machine.open["svc:error_rate"]
    assert alert.severity == alert.peak_severity == "CRITICAL" and alert.current_rate == 0.38


def test_peak_severity_is_kept_and_a_lower_severity_is_a_quiet_update(machine):
    feed(machine, [snap(ts=0, **CRIT)])
    out = feed(machine, [snap(ts=1, **MED)])
    assert [(t.kind, t.notify) for _, t in out] == [("updated", False)]
    alert = machine.open["svc:error_rate"]
    assert (alert.severity, alert.peak_severity) == ("MEDIUM", "CRITICAL")
    assert feed(machine, [snap(ts=2, **HIGH)])[0][1].notify is False        # HIGH < peak: still no notification


def test_alert_evidence_tracks_the_true_peak_of_the_incident(machine):
    feed(machine, [snap(ts=0, sev="CRITICAL", z=5.0, ratio=5.2, rate=0.27)])
    out = feed(machine, [snap(ts=1, sev="CRITICAL", z=8.0, ratio=7.5, rate=0.38),      # keeps climbing inside CRITICAL
                         snap(ts=2, sev="CRITICAL", z=6.0, ratio=6.0, rate=0.30)])     # falling back does not lower it
    assert [(t.kind, t.notify, t.new_peak) for _, t in out] == [("updated", False, True)]
    alert = machine.open["svc:error_rate"]
    assert (alert.current_rate, alert.z, alert.ratio) == (0.38, 8.0, 7.5)
    for i in range(3, 6):
        resolved = machine.evaluate(snap(ts=i, sev="NONE", z=0.2, ratio=1.0, rate=0.05))
    assert resolved.kind == "resolved" and resolved.alert.current_rate == 0.38          # the resolve message quotes the peak


def test_hysteresis_does_not_flap_around_the_threshold(machine):
    feed(machine, [snap(ts=0, **HIGH), snap(ts=1, **HIGH)])                 # open
    wobble = [snap(ts=2, sev="MEDIUM", z=2.99, ratio=2.9, rate=0.14), snap(ts=3, sev="HIGH", z=3.02, ratio=3.0, rate=0.15),
              snap(ts=4, sev="MEDIUM", z=2.99, ratio=2.9, rate=0.14)]
    assert [t.kind for _, t in feed(machine, wobble) if t.notify] == []     # no resolve, no second "created"
    assert len(machine.open) == 1


def test_resolves_only_after_resolve_ticks_in_a_row(machine):
    feed(machine, [snap(ts=0, **CRIT)])
    calm = lambda ts: snap(ts=ts, sev="NONE", z=1.4, ratio=1.3, rate=0.07)
    barely = snap(ts=3, sev="NONE", z=1.8, ratio=1.9, rate=0.09)            # below trigger, above resolve bar
    assert feed(machine, [calm(1), calm(2), barely]) == []                   # streak broken
    out = feed(machine, [calm(4), calm(5), calm(6)])
    assert [t.kind for _, t in out] == ["resolved"] and out[0][0] == 2       # exactly on the 3rd calm tick
    resolved = out[0][1].alert
    assert resolved.status == "RESOLVED" and resolved.resolved_at == 6 and not machine.open


def test_low_data_while_open_holds_state(machine):
    feed(machine, [snap(ts=0, **CRIT)])
    quiet = [snap(ts=i, sev="NONE", z=None, ratio=None, rate=0.0, total=3, errors=0) for i in range(1, 10)]
    assert feed(machine, quiet) == [] and machine.is_open("svc")             # silence is not recovery


def test_recurrence_after_resolve_is_a_new_alert(machine):
    feed(machine, [snap(ts=0, **CRIT)])
    feed(machine, [snap(ts=i, sev="NONE", z=0.2, ratio=1.0) for i in range(1, 4)])
    assert not machine.open
    out = feed(machine, [snap(ts=10, **CRIT)])
    assert out[0][1].kind == "created" and out[0][1].alert.id != "a0"


def test_services_have_independent_alerts(machine):
    feed(machine, [snap(ts=0, service="pay", **CRIT), snap(ts=0, service="auth", **CALM)])
    assert machine.is_open("pay") and not machine.is_open("auth")


def test_level_shift_resolves_a_stuck_alert_and_flags_relearning(machine, settings):
    feed(machine, [snap(ts=0, **CRIT)])
    assert feed(machine, [snap(ts=settings.profile.level_shift_seconds, **CRIT)]) == []   # not yet
    out = feed(machine, [snap(ts=settings.profile.level_shift_seconds + 1, **CRIT)])
    t = out[0][1]
    assert (t.kind, t.notify, t.level_shift) == ("resolved", True, True)
    assert "level shift" in t.alert.reason


def test_level_shift_end_to_end_baseline_relearns_and_alert_does_not_return(settings, clock):
    machine = AlertStateMachine(settings.detector, settings.profile)
    engine = DetectionEngine(settings, clock, frozen_fn=machine.is_open)
    det = engine.detector("svc")
    events = []

    def on_tick(s):
        t = machine.evaluate(s)
        if t:
            events.append(t)
            if t.level_shift:
                det.reset_baseline()

    run_seconds(det, clock, 30, 20, 1, on_tick=on_tick)              # learn 5 %
    for _ in range(settings.profile.level_shift_seconds + 60):       # traffic permanently moves to 40 % errors
        for i in range(20):
            engine.on_event(make_event(clock.now, is_error=i < 8))
        clock.advance(1)
        for s in engine.tick():
            on_tick(s)
    kinds = [t.kind for t in events if t.kind in {"created", "resolved"}]
    assert kinds == ["created", "resolved"] and events[-1].level_shift          # opened once, resolved once, no re-alert
    assert det.baseline.mean == pytest.approx(0.4, abs=0.03)          # relearned the new level
    assert not machine.open


def test_restore_reopens_alert_without_a_duplicate_created(machine):
    feed(machine, [snap(ts=0, **CRIT)])
    saved = machine.open_alerts()
    fresh = AlertStateMachine(machine._cfg, machine._profile)
    fresh.restore(saved)
    assert fresh.is_open("svc")
    assert feed(fresh, [snap(ts=5, **CRIT)]) == []                   # still the same incident: silent


def test_evidence_is_explainable(settings):
    ev = build_evidence(snap(**CRIT, total=284, errors=109), 10)
    assert ev["is_anomaly"] and ev["sample_size"] == 284 and ev["window_seconds"] == 10
    assert ev["reason"] == "Error rate 7.5× baseline with z=5.20 over 284 events"
