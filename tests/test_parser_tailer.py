import json
import os
import sys

import pytest

from app.ingestion.parser import IngestStats, ParseError, parse_line
from app.ingestion.tailer import Checkpoint, Tailer


def line(**over):
    base = {"timestamp": "2026-01-01T00:00:00.000Z", "service": "pay", "level": "info",
            "status": 200, "message": "ok", "request_id": "r1"}
    base.update(over)
    return json.dumps(base)


# ---- parser ----------------------------------------------------------------------------------------
def test_parse_valid_line():
    ev = parse_line(line())
    assert (ev.service, ev.level, ev.status, ev.is_error, ev.request_id) == ("pay", "INFO", 200, False, "r1")
    assert ev.ts == 1767225600.0


def test_optional_fields_may_be_absent():
    ev = parse_line('{"timestamp":"2026-01-01T00:00:00Z","service":"a","level":"warn"}')
    assert (ev.status, ev.message, ev.request_id) == (None, "", None)


@pytest.mark.parametrize("over,is_error", [
    ({"level": "error"}, True), ({"level": "critical"}, True), ({"level": "fatal"}, True),
    ({"level": "info", "status": 503}, True), ({"level": "warn", "status": 404}, False),
    ({"level": "info", "status": None}, False), ({"level": "info", "status": 499}, False),
    ({"level": "info", "status": 500}, True), ({"level": "info", "status": 599}, True),
    ({"level": "info", "status": 600}, False),
])
def test_is_error_rules(over, is_error):
    assert parse_line(line(**over)).is_error is is_error


@pytest.mark.parametrize("bad", [
    "not json", "[1,2]", "{}", line(service=""), '{"timestamp":"nope","service":"a","level":"info"}',
    line(status="abc"), '{"service":"a","level":"info"}', '{"timestamp":"2026-01-01T00:00:00Z","level":"info"}',
    '{"timestamp":"2026-01-01T00:00:00Z","service":"a"}',
])
def test_malformed_and_missing_field_lines_raise_and_are_counted(bad):
    stats = IngestStats()
    with pytest.raises(ParseError) as e:
        parse_line(bad)
    stats.record_error(bad, str(e.value))
    assert stats.parse_errors == 1 and stats.parse_error_samples[0]["reason"]


def test_error_samples_are_capped_at_20_and_truncated():
    stats = IngestStats()
    for i in range(30):
        stats.record_error("x" * 500 + str(i), "why")
    assert stats.parse_errors == 30 and len(stats.parse_error_samples) == 20
    assert len(stats.parse_error_samples[0]["line"]) == 200


# ---- tailer ----------------------------------------------------------------------------------------
def test_tailer_waits_for_newline_on_partial_writes(tmp_path):
    p = tmp_path / "app.log"
    p.write_text("")
    t = Tailer(str(p), start_at="checkpoint")
    with open(p, "a", newline="\n") as f:
        f.write('{"a": 1')
        f.flush()
        assert t.read_available() == []                 # no newline yet -> nothing emitted
        f.write('}\n{"b": 2}\n{"c"')
        f.flush()
        assert t.read_available() == ['{"a": 1}', '{"b": 2}']
        f.write(": 3}\n")
        f.flush()
        assert t.read_available() == ['{"c": 3}']
    assert t.offset == p.stat().st_size


def test_tailer_does_not_reread_the_file(tmp_path):
    p = tmp_path / "app.log"
    p.write_text("one\ntwo\n")
    t = Tailer(str(p), start_at="checkpoint")
    assert t.read_available() == ["one", "two"]
    assert t.read_available() == []
    with open(p, "a") as f:
        f.write("three\n")
    assert t.read_available() == ["three"]


def test_start_at_end_skips_history_but_a_file_created_later_is_read_fully(tmp_path):
    p = tmp_path / "app.log"
    p.write_text("old1\nold2\n")
    t = Tailer(str(p), start_at="end")
    assert t.read_available() == []
    with open(p, "a") as f:
        f.write("new\n")
    assert t.read_available() == ["new"]

    q = tmp_path / "late.log"
    t2 = Tailer(str(q), start_at="end")
    assert t2.read_available() == []                    # file does not exist yet
    q.write_text("first\n")
    assert t2.read_available() == ["first"]             # appeared after start -> read from byte 0


def test_tailer_handles_crlf_and_blank_lines(tmp_path):
    p = tmp_path / "app.log"
    p.write_bytes(b"a\r\n\r\nb\n")
    assert Tailer(str(p), start_at="checkpoint").read_available() == ["a", "b"]


def test_checkpoint_resume_does_not_replay(tmp_path):
    p = tmp_path / "app.log"
    p.write_text("l1\nl2\nl3\n")
    t1 = Tailer(str(p), start_at="checkpoint")
    assert t1.read_available() == ["l1", "l2", "l3"]
    cp = Checkpoint(t1.inode, t1.offset)
    t1.close()
    with open(p, "a") as f:
        f.write("l4\nl5\n")                             # written while "the app was down"
    t2 = Tailer(str(p), start_at="end", resume=cp)      # even start_at=end must honour a valid checkpoint
    assert t2.read_available() == ["l4", "l5"]
    assert t2.resumed_from_checkpoint


def test_stale_checkpoint_is_ignored(tmp_path):
    p = tmp_path / "app.log"
    p.write_text("l1\nl2\n")
    t = Tailer(str(p), start_at="checkpoint", resume=Checkpoint(inode=123456789, offset=3))   # wrong inode
    assert t.read_available() == ["l1", "l2"] and not t.resumed_from_checkpoint
    beyond = Tailer(str(p), start_at="checkpoint", resume=Checkpoint(os.stat(p).st_ino, 10_000))   # offset > size
    assert beyond.read_available() == ["l1", "l2"] and not beyond.resumed_from_checkpoint


def test_truncation_reopens_from_zero(tmp_path):
    p = tmp_path / "app.log"
    p.write_text("aaaa\nbbbb\ncccc\n")
    t = Tailer(str(p), start_at="checkpoint")
    assert len(t.read_available()) == 3
    p.write_text("x\n")                                 # copytruncate-style rotation: same inode, smaller size
    assert t.read_available() == ["x"]
    assert t.rotations == 1


@pytest.mark.skipif(sys.platform == "win32", reason="Windows cannot rename a file another handle has open")
def test_rotation_drains_the_old_file_then_follows_the_new_one(tmp_path):
    p = tmp_path / "app.log"
    p.write_text("old1\n")
    t = Tailer(str(p), start_at="checkpoint")
    assert t.read_available() == ["old1"]
    with open(p, "a") as f:
        f.write("old2\n")                               # last write before rotation
    os.rename(p, tmp_path / "app.log.1")
    p.write_text("new1\nnew2\n")                        # logrotate: new file, new inode
    assert t.read_available() == ["old2", "new1", "new2"]
    assert t.rotations == 1
    with open(p, "a") as f:
        f.write("new3\n")
    assert t.read_available() == ["new3"]


def test_missing_file_yields_nothing_until_it_exists(tmp_path):
    t = Tailer(str(tmp_path / "nope.log"), start_at="end")
    assert t.read_available() == [] and not t.file_ok
