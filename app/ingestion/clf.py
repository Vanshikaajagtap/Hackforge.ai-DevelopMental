"""Common Log Format (Apache/NCSA) -> LogEvent, plus the config-driven field mapping.

    host ident authuser [dd/Mon/yyyy:HH:MM:SS +zzzz] "METHOD /path HTTP/1.0" status bytes

Field mapping (all configurable, see config.yaml `mapping:` and `detector.error_definition`):
  service  first URL path segment when it is one of `mapping.service_prefixes`, else `mapping.other_service`
  level    ERROR for 5xx, WARN for 4xx, INFO otherwise
  is_error status >= the minimum implied by `detector.error_definition` (500 for "5xx", 400 for "4xx+5xx", or custom)

The real NASA Jul-95 file has traffic like: `-` byte counts, binary junk in the method position, URLs containing spaces,
non-ASCII bytes and a truncated final line. Anything that is structurally a CLF line is accepted (the server really
did log that request); a line without a usable structure raises ParseError and is counted, never fatal.
"""
from __future__ import annotations

import calendar
import re
from datetime import datetime, timezone

from app.config import MappingCfg

from .models import LogEvent
from .parser import ParseError

_MONTH_NAMES = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
_MONTH = {m.lower(): i for i, m in enumerate(_MONTH_NAMES, start=1)}

# host ident user [ts] "request" status [bytes]   - bytes is optional so a line cut after the status still parses
_CLF = re.compile(
    r'^(?P<host>\S+) (?P<ident>\S+) (?P<user>\S+) \[(?P<ts>[^\]]*)\] "(?P<req>.*)" (?P<status>\d{3})(?: (?P<bytes>\d+|-))?\s*$')
_ORIG = ' orig_ts="'          # trailing extension written by the replayer: the untouched original timestamp
_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")


def parse_clf_timestamp(text: str) -> float:
    """'01/Jul/1995:00:00:01 -0400' -> epoch seconds. A missing zone is read as UTC. Raises ParseError."""
    try:
        date, _, tz = text.strip().partition(" ")
        day, mon, rest = date.split("/")
        year, hh, mm, ss = rest.split(":")
        d, h, mi, s = int(day), int(hh), int(mm), int(ss)
        if not (1 <= d <= 31 and 0 <= h <= 23 and 0 <= mi <= 59 and 0 <= s <= 60):
            raise ValueError("field out of range")
        epoch = calendar.timegm((int(year), _MONTH[mon.lower()], d, h, mi, s))
        offset = 0
        if tz:
            sign = -1 if tz[0] == "-" else 1
            if tz[0] not in "+-" or len(tz) != 5 or not tz[1:].isdigit() or int(tz[3:]) > 59 or int(tz[1:3]) > 23:
                raise ValueError("bad zone")
            offset = sign * (int(tz[1:3]) * 3600 + int(tz[3:5]) * 60)
        return float(epoch - offset)
    except (ValueError, KeyError, OverflowError) as e:
        raise ParseError(f"bad timestamp: {text[:40]!r}") from e


def format_clf_timestamp(epoch: float, tz_offset_minutes: int = 0) -> str:
    """Epoch seconds -> 'dd/Mon/yyyy:HH:MM:SS +zzzz' in the given zone (locale-independent)."""
    dt = datetime.fromtimestamp(epoch + tz_offset_minutes * 60, timezone.utc)      # wall clock in the target zone
    sign = "-" if tz_offset_minutes < 0 else "+"
    tz = f"{sign}{abs(tz_offset_minutes) // 60:02d}{abs(tz_offset_minutes) % 60:02d}"
    return f"{dt.day:02d}/{_MONTH_NAMES[dt.month - 1]}/{dt.year}:{dt:%H:%M:%S} {tz}"      # never strftime('%b'): locale-dependent


def split_request(req: str) -> tuple[str, str, str]:
    """'GET /a b.html HTTP/1.0' -> ('GET', '/a b.html', 'HTTP/1.0'). Junk (binary, no method) -> ('', '', '')."""
    parts = req.split()
    if not parts:
        return "", "", ""
    method = parts[0]
    if not (method.isascii() and method.isalpha()):
        return "", "", ""
    rest = parts[1:]
    proto = ""
    if rest and rest[-1].upper().startswith("HTTP/"):
        proto = rest.pop()
    return method.upper(), " ".join(rest), proto


def path_prefix(url: str) -> str | None:
    """First path segment, lower-cased: '/shuttle/x.html' -> 'shuttle', '/' -> '' (root), non-paths -> None."""
    u = url
    if _SCHEME.match(u):                                  # absolute-URI request line ("http://host/path"): keep only the path
        u = "/" + u.split("://", 1)[1].partition("/")[2]     # (a real path like "/://host" starts with "/", so it is not one)
    u = u.split("?", 1)[0].split("#", 1)[0]
    if not u.startswith("/"):
        return None
    return u[1:].split("/", 1)[0].lower()


class ServiceMapper:
    """Maps a request URL to a service name using the configured prefixes."""
    def __init__(self, mapping: MappingCfg) -> None:
        self.listed = {p.strip().lower().strip("/") or "/" for p in mapping.service_prefixes}
        self.other = mapping.other_service
        self.root = mapping.root_service

    def service_for(self, url: str) -> str:
        """Service for `url`: its first path segment if listed, else the catch-all."""
        prefix = path_prefix(url)
        if prefix is None:
            return self.other
        if prefix == "":
            return self.root if "/" in self.listed else self.other
        return prefix if prefix in self.listed else self.other


def level_for(status: int) -> str:
    """ERROR for 5xx, WARN for 4xx, INFO otherwise."""
    return "ERROR" if status >= 500 else "WARN" if status >= 400 else "INFO"


class ClfParser:
    """Parses one Common Log Format line into a LogEvent; raises ParseError when it cannot."""
    def __init__(self, mapping: MappingCfg, error_min_status: int = 500) -> None:
        self.mapper = ServiceMapper(mapping)
        self.error_min_status = error_min_status

    def __call__(self, line: str) -> LogEvent:
        text = line.strip()
        if not text:
            raise ParseError("empty line")
        cut = text.rfind(_ORIG)
        if cut > 0 and text.endswith('"'):
            text = text[:cut]                             # drop the replayer's original-timestamp extension
        m = _CLF.match(text)
        if m is None:
            reason = "truncated line (no timestamp)" if "[" not in text else "not a common-log-format line"
            raise ParseError(reason)
        status = int(m["status"])
        if not 100 <= status <= 599:
            raise ParseError(f"bad status: {status}")
        ts = parse_clf_timestamp(m["ts"])
        method, url, _proto = split_request(m["req"])
        message = f"{method} {url}".strip() if method else "unparseable request: " + m["req"][:60].encode("ascii", "replace").decode()
        return LogEvent(
            ts=ts,
            service=self.mapper.service_for(url),
            level=level_for(status),
            status=status,
            message=message[:200],
            request_id=m["host"],                          # the client - the closest thing CLF has to a request id
            is_error=self.error_min_status <= status <= 599,
        )
