"""Common Log Format parser, timestamps/time zones, error definitions and the field mapping - including the REAL
malformed / odd lines of the NASA Jul-95 log (tests/fixtures/nasa_oddities.log)."""
from dataclasses import replace

from pathlib import Path

import pytest

from app.config import MappingCfg, error_min_status, load_settings
from app.ingestion.clf import (
    ClfParser, ServiceMapper, format_clf_timestamp, level_for, parse_clf_timestamp, path_prefix, split_request)
from app.ingestion.factory import build_parser
from app.ingestion.parser import ParseError, parse_line
from app.replay import iter_file_lines

from conftest import FIXTURES

MAPPING = MappingCfg(service_prefixes=["shuttle", "images", "history", "ksc.html", "/"])
GOOD = '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245'
T0 = 804571201.0                          # 01/Jul/1995:00:00:01 -0400


def parser(min_status=500, mapping=MAPPING):
    return ClfParser(mapping, min_status)


# ---- a normal line -----------------------------------------------------------------------------------
def test_valid_line_maps_every_field():
    ev = parser()(GOOD)
    assert (ev.ts, ev.service, ev.level, ev.status, ev.message, ev.request_id, ev.is_error) == \
        (T0, "history", "INFO", 200, "GET /history/apollo/", "199.72.81.55", False)


@pytest.mark.parametrize("tail,ok", [("200 6245", True), ("304 0", True), ("404 -", True), ("200 -", True), ("302", True)])
def test_dash_and_missing_byte_counts_are_accepted(tail, ok):
    line = f'h - - [01/Jul/1995:00:00:01 -0400] "GET /a HTTP/1.0" {tail}'
    assert parser()(line).status == int(tail.split()[0])


def test_replayer_extension_is_ignored_and_the_line_parses_identically():
    ext = GOOD + ' orig_ts="01/Jul/1995:00:00:01 -0400"'
    assert parser()(ext) == parser()(GOOD)
    quoted = 'h - - [01/Jul/1995:00:00:01 -0400] "GET /x\\"y HTTP/1.0" 200 5 orig_ts="01/Jul/1995:00:00:01 -0400"'
    assert parser()(quoted).status == 200                         # a quote inside the request does not confuse it


# ---- timestamps and zones ----------------------------------------------------------------------------
@pytest.mark.parametrize("text,expected", [
    ("01/Jul/1995:00:00:01 -0400", 804571201.0),
    ("01/Jul/1995:00:00:01 +0000", 804556801.0),
    ("01/Jul/1995:00:00:01 +0530", 804556801.0 - 19800),
    ("01/Jul/1995:00:00:01 -0930", 804556801.0 + 9 * 3600 + 1800),
    ("01/Jul/1995:00:00:01", 804556801.0),                       # missing zone is read as UTC
    ("01/JUL/1995:00:00:01 -0400", 804571201.0),                 # month names are case-insensitive
    ("31/Jul/1995:23:59:59 -0400", parse_clf_timestamp("01/Aug/1995:03:59:59 +0000")),   # crosses midnight in UTC
])
def test_timestamps_and_time_zone_offsets(text, expected):
    assert parse_clf_timestamp(text) == expected


@pytest.mark.parametrize("bad", ["32/Jul/1995:00:00:01 -0400", "01/Foo/1995:00:00:01 -0400", "01/Jul/1995:25:00:00 -0400",
                                 "01/Jul/1995:00:60:00 -0400", "01/Jul/1995:00:00:01 -04:00", "01/Jul/1995:00:00:01 -040",
                                 "01/Jul/1995:00:00:01 +2500", "01/Jul/1995:00:00:01 +0099", "garbage", "", "01/Jul/1995"])
def test_bad_timestamps_raise(bad):
    with pytest.raises(ParseError):
        parse_clf_timestamp(bad)


def test_format_roundtrip_is_locale_independent():
    assert format_clf_timestamp(T0, -240) == "01/Jul/1995:00:00:01 -0400"
    assert format_clf_timestamp(804556801.0, 0) == "01/Jul/1995:00:00:01 +0000"
    for tz in (-240, 0, 330, -570):
        assert parse_clf_timestamp(format_clf_timestamp(T0, tz)) == T0


# ---- malformed lines ---------------------------------------------------------------------------------
@pytest.mark.parametrize("bad,why", [
    ("alyssa.p", "truncated"),                                                     # the real last line of the file
    ("", "empty"), ("   ", "empty"), ("garbage in, garbage out", "truncated"),
    ('h - - [01/Jul/1995:00:00:01 -0400] "GET /a HTTP/1.0', "not a common"),        # cut inside the request
    ('h - - [01/Jul/1995:00:00:01 -0400] GET /a HTTP/1.0" 200 5', "not a common"),  # missing opening quote
    ('h - - [01/Jul/1995:00:00:01 -0400] "GET /a HTTP/1.0" abc 5', "not a common"),
    ('h - - [01/Jul/1995:00:00:01 -0400] "GET /a HTTP/1.0" 000 5', "bad status"),
    ('h - - [01/Jul/1995:00:00:01 -0400] "GET /a HTTP/1.0" 999 5', "bad status"),
    ('h - - [32/Jul/1995:00:00:01 -0400] "GET /a HTTP/1.0" 200 5', "bad timestamp"),
    (' HTTP/1.0" 404 -', "truncated"),                                             # the tail half of a cut line
    ('{"json": true}', "truncated"),
])
def test_malformed_lines_raise_parse_error_never_anything_else(bad, why):
    with pytest.raises(ParseError) as e:
        parser()(bad)
    assert why in str(e.value)


# ---- odd-but-valid requests --------------------------------------------------------------------------
@pytest.mark.parametrize("req,method,url,proto", [
    ("GET /a/b.html HTTP/1.0", "GET", "/a/b.html", "HTTP/1.0"),
    ("get /a HTTP/1.0", "GET", "/a", "HTTP/1.0"),
    ("GET /a b c.html HTTP/1.0", "GET", "/a b c.html", "HTTP/1.0"),                # spaces inside the URL
    ("GET /a b c.html", "GET", "/a b c.html", ""),                                 # no protocol
    ("GET", "GET", "", ""), ("", "", "", ""), ("   ", "", "", ""),
    ("\x05\x01", "", "", ""),                                                      # binary junk (real)
    ("1/history/apollo/images/", "", "", ""),                                      # real: leading '1' where the method belongs
])
def test_split_request(req, method, url, proto):
    assert split_request(req) == (method, url, proto)


@pytest.mark.parametrize("url,prefix", [
    ("/shuttle/missions/x.html", "shuttle"), ("/SHUTTLE/x", "shuttle"), ("/ksc.html", "ksc.html"),
    ("/ksc.html?a=b#c", "ksc.html"), ("/", ""), ("/?x=1", ""), ("http://www.nasa.gov/history/x", "history"),
    ("http://www.nasa.gov", ""), ("", None), ("shuttle/x", None), ("/://spacelink.msfc.nasa.gov", ":"),
])
def test_path_prefix(url, prefix):
    assert path_prefix(url) == prefix


# ---- field mapping: service --------------------------------------------------------------------------
def test_service_mapping_top_prefixes_root_and_other():
    m = ServiceMapper(MAPPING)
    assert [m.service_for(u) for u in ("/shuttle/x", "/images/a.gif", "/history/", "/ksc.html", "/", "/software/winvn", "", "junk")] == \
        ["shuttle", "images", "history", "ksc.html", "root", "other", "other", "other"]


def test_root_and_prefix_lists_are_configurable():
    m = ServiceMapper(MappingCfg(service_prefixes=["/shuttle/", "Images"], other_service="misc", root_service="home"))
    assert m.service_for("/shuttle/x") == "shuttle" and m.service_for("/IMAGES/x") == "images"
    assert m.service_for("/") == "misc"                         # "/" is only its own service when listed
    m2 = ServiceMapper(MappingCfg(service_prefixes=["/"], root_service="home"))
    assert m2.service_for("/") == "home"


# ---- field mapping: level ----------------------------------------------------------------------------
@pytest.mark.parametrize("status,level", [(200, "INFO"), (206, "INFO"), (302, "INFO"), (304, "INFO"), (399, "INFO"),
                                          (400, "WARN"), (404, "WARN"), (499, "WARN"), (500, "ERROR"), (501, "ERROR"), (599, "ERROR")])
def test_level_mapping(status, level):
    assert level_for(status) == level
    line = f'h - - [01/Jul/1995:00:00:01 -0400] "GET /a HTTP/1.0" {status} 0'
    assert parser()(line).level == level


# ---- error definitions -------------------------------------------------------------------------------
@pytest.mark.parametrize("definition,expected", [("5xx", 500), ("4xx+5xx", 400), (" 4XX+5XX ", 400), ("404", 404), (404, 404), ("100", 100)])
def test_error_definition_values(definition, expected):
    assert error_min_status(definition) == expected


@pytest.mark.parametrize("bad", ["3xx", "abc", "", 99, "700", "4xx+3xx"])
def test_bad_error_definitions_are_rejected(bad):
    with pytest.raises(ValueError):
        error_min_status(bad)


@pytest.mark.parametrize("status", [200, 302, 304, 399, 400, 403, 404, 499, 500, 501, 503, 599])
def test_is_error_under_each_definition(status):
    line = f'h - - [01/Jul/1995:00:00:01 -0400] "GET /a HTTP/1.0" {status} 0'
    assert parser(error_min_status("5xx"))(line).is_error is (status >= 500)
    assert parser(error_min_status("4xx+5xx"))(line).is_error is (status >= 400)
    assert parser(error_min_status("404"))(line).is_error is (status >= 404)


def test_error_definition_also_applies_to_ndjson_status():
    js = '{"timestamp":"2026-01-01T00:00:00Z","service":"a","level":"info","status":404}'
    assert parse_line(js).is_error is False                              # default: only 5xx
    assert parse_line(js, error_min_status=400).is_error is True
    assert parse_line(js.replace('"info"', '"error"')).is_error is True   # level still counts


def test_a_bad_definition_in_config_fails_fast(tmp_path):
    text = Path("config.yaml").read_text(encoding="utf-8").replace('error_definition: "4xx+5xx"', 'error_definition: "3xx"')
    cfg = tmp_path / "c.yaml"
    cfg.write_text(text, encoding="utf-8")
    with pytest.raises(ValueError, match="error_definition"):
        load_settings(cfg, env={"LOGPULSE_PROFILE": "nasa"})


# ---- format selection --------------------------------------------------------------------------------
def test_format_selection():
    s = load_settings(env={})
    js = '{"timestamp":"2026-01-01T00:00:00Z","service":"a","level":"info","status":200}'
    auto = build_parser(s)
    assert auto(GOOD).status == 200 and auto(js).service == "a"           # auto: by first character
    nd = build_parser(replace(s, ingestion=replace(s.ingestion, format="ndjson")))
    with pytest.raises(ParseError):
        nd(GOOD)
    clf = build_parser(replace(s, ingestion=replace(s.ingestion, format="clf")))
    with pytest.raises(ParseError):
        clf(js)
    assert clf(GOOD).status == 200


def test_bad_format_in_config_is_rejected(tmp_path):
    text = Path("config.yaml").read_text(encoding="utf-8").replace("ingestion:\n  start_at", "ingestion:\n  format: xml\n  start_at", 1)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(text, encoding="utf-8")
    with pytest.raises(ValueError, match="ingestion.format"):
        load_settings(cfg, env={})


def test_auto_applies_the_error_definition_to_both_formats():
    s = load_settings(env={"LOGPULSE_PROFILE": "nasa"})                   # nasa: 4xx+5xx (and format clf) ...
    auto = build_parser(replace(s, ingestion=replace(s.ingestion, format="auto")))   # ... so ask for auto explicitly
    assert auto('h - - [01/Jul/1995:00:00:01 -0400] "GET /a HTTP/1.0" 404 -').is_error is True
    assert auto('{"timestamp":"2026-01-01T00:00:00Z","service":"a","level":"info","status":404}').is_error is True


# ---- the real file's lines ---------------------------------------------------------------------------
def test_real_oddities_parse_and_only_the_truncated_line_is_an_error():
    lines = list(iter_file_lines(FIXTURES / "nasa_oddities.log"))
    p, ok, bad = parser(400), [], []
    for line in lines:
        try:
            ok.append((line, p(line)))
        except ParseError:
            bad.append(line)
    assert bad == ["alyssa.p"] and len(ok) == len(lines) - 1
    junk = [ev for line, ev in ok if not split_request(line.split('"')[1])[0]]
    assert junk and all(ev.service == "other" and ev.status == 400 and ev.is_error for ev in junk)   # binary requests
    spaced = [ev for _, ev in ok if " " in ev.message.split(" ", 1)[1]]
    assert spaced                                                          # URLs containing spaces survive whole
    assert any(ev.status == 501 and ev.level == "ERROR" for _, ev in ok)
    assert any(ev.message.startswith("POST") for _, ev in ok) and any(ev.message.startswith("HEAD") for _, ev in ok)


def test_real_sample_segment_parses_without_errors_and_maps_services():
    p = build_parser(load_settings(env={"LOGPULSE_PROFILE": "nasa"}))
    events = [p(line) for line in iter_file_lines(FIXTURES / "nasa_sample.log")]
    assert len(events) == 781
    assert {e.service for e in events} >= {"history", "shuttle", "images", "other", "root"}
    assert all(events[i].ts <= events[i + 1].ts for i in range(len(events) - 1))     # the log is chronological
    errs = sum(e.is_error for e in events)
    assert errs > sum(1 for e in events if e.status >= 500)                # 4xx+5xx counts more than 5xx alone
