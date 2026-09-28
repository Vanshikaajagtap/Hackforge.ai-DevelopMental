import pytest

from app.detection.severity import classify


def sev(settings, z, rate, ratio, errors):
    return classify(z, rate, ratio, errors, settings.severity)


@pytest.mark.parametrize("z,rate,ratio,errors,expected", [
    (2.1, 0.12, 2.0, 8, "MEDIUM"),
    (3.1, 0.18, 3.5, 12, "HIGH"),
    (4.1, 0.30, 6.0, 20, "CRITICAL"),
    (1.9, 0.30, 6.0, 20, "NONE"),           # z gate off
    (2.1, 0.12, 2.0, 4, "NONE"),            # min-errors gate off
    (9.0, 0.04, 1.4, 20, "NONE"),           # tiny sigma: huge z but a 2%->4% change is not an incident
    (2.1, 0.06, 1.6, 8, "MEDIUM"),          # ratio alone satisfies the rate-or-ratio gate
    (2.1, 0.12, 1.2, 8, "MEDIUM"),          # absolute rate alone satisfies it too
    (3.1, 0.12, 2.0, 8, "MEDIUM"),          # HIGH z but HIGH rate/ratio gates fail -> falls to MEDIUM
    (4.1, 0.30, 6.0, 9, "HIGH"),            # CRITICAL needs 10 errors -> falls to HIGH
    (4.1, 0.10, 6.0, 20, "CRITICAL"),       # CRITICAL via ratio although rate is only 10 %
])
def test_severity_table(settings, z, rate, ratio, errors, expected):
    assert sev(settings, z, rate, ratio, errors) == expected


def test_thresholds_come_from_config_not_code(settings):
    from dataclasses import replace
    strict = replace(settings.severity, medium=replace(settings.severity.medium, z=5.0))
    assert classify(2.1, 0.12, 2.0, 8, strict) == "NONE"
