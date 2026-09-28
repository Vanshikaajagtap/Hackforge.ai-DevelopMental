import json
from collections import Counter

import pytest

from app.generator import SCENARIOS, LogGenerator, ScenarioController
from app.ingestion.parser import ParseError, parse_line

from conftest import FakeClock


def generate(settings, scenario, seconds, seed=42, clock=None):
    clock = clock or FakeClock()
    ctl = ScenarioController(settings.generator, clock)
    ctl.set(scenario)
    gen = LogGenerator(settings.generator, ctl, seed=seed, clock=clock)
    lines = []
    for _ in range(int(seconds * 10)):
        clock.advance(0.1)
        lines += gen.batch(clock.now, 0.1)
    return lines


def stats(lines, service):
    evs = []
    for line in lines:
        try:
            ev = parse_line(line)
        except ParseError:
            continue
        if ev.service == service:
            evs.append(ev)
    return len(evs), sum(e.is_error for e in evs)


def test_all_prd_scenarios_exist():
    assert {"normal", "traffic_spike", "error_spike", "recover", "mixed", "malformed"} <= set(SCENARIOS)


def test_normal_rate_and_error_probability(settings):
    lines = generate(settings, "normal", 60)
    n, errs = stats(lines, "payment-service")
    assert 15 * 60 * 0.95 < len(lines) < 15 * 60 * 1.05                     # ~15 ev/s across 2 services
    assert 0.04 <= errs / n <= 0.06                                          # 4-6 % errors


def test_traffic_spike_is_5x_volume_with_unchanged_error_rate(settings):
    normal = generate(settings, "normal", 30)
    spike = generate(settings, "traffic_spike", 30)
    assert len(spike) == pytest.approx(5 * len(normal), rel=0.05)
    n, errs = stats(spike, "payment-service")
    assert 0.04 <= errs / n <= 0.06


def test_error_spike_ramps_then_escalates_on_the_spike_service_only(settings):
    lines = generate(settings, "error_spike", 40)
    evs = [parse_line(line) for line in lines]
    def rate(lo, hi, svc):
        sel = [e.is_error for e in evs if e.service == svc and lo <= e.ts - 1_000_000 < hi]
        return sum(sel) / len(sel)
    assert 0.12 <= rate(4, 14, "payment-service") <= 0.18                    # ~15 %
    assert 0.34 <= rate(18, 40, "payment-service") <= 0.42                    # ~38 %
    assert rate(0, 40, "auth-service") < 0.07                                 # neighbour stays healthy


def test_recover_is_normal_traffic(settings):
    n, errs = stats(generate(settings, "recover", 30), "payment-service")
    assert errs / n < 0.07


def test_mixed_follows_the_scripted_timeline(settings):
    clock = FakeClock()
    ctl = ScenarioController(settings.generator, clock)
    ctl.set("mixed")
    seen = {}
    for t in (0, 24, 26, 34, 36, 69, 71, 109, 111):
        clock.now = 1_000_000.0 + t
        seen[t] = ctl.effective()[0]
    assert (seen[0], seen[24], seen[26], seen[34], seen[36], seen[69], seen[71], seen[111]) == \
        ("normal", "normal", "traffic_spike", "traffic_spike", "error_spike", "error_spike", "normal", "normal")


def test_malformed_scenario_sprinkles_broken_lines_but_keeps_valid_traffic(settings):
    lines = generate(settings, "malformed", 20)
    bad = 0
    for line in lines:
        try:
            parse_line(line)
        except ParseError:
            bad += 1
    assert bad >= len(lines) // (settings.generator.malformed_every + 2) and bad < len(lines) * 0.1


def test_seeded_runs_are_reproducible(settings):
    def strip(lines):   # request ids and everything else derive from the seeded RNG; timestamps from the fake clock
        return [json.loads(x) for x in lines]
    assert strip(generate(settings, "error_spike", 10, seed=7)) == strip(generate(settings, "error_spike", 10, seed=7))
    assert strip(generate(settings, "error_spike", 10, seed=7)) != strip(generate(settings, "error_spike", 10, seed=8))


def test_unknown_scenario_is_rejected(settings):
    with pytest.raises(ValueError):
        ScenarioController(settings.generator).set("nope")


def test_line_format_matches_the_input_contract(settings):
    obj = json.loads(generate(settings, "normal", 1)[0])
    assert set(obj) == {"timestamp", "service", "level", "status", "message", "request_id"}
    assert obj["timestamp"].endswith("Z") and Counter(["ERROR", "INFO"]).keys() >= {obj["level"]}
