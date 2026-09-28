"""Pick the line parser from config: ingestion.format = ndjson | clf | auto."""
from __future__ import annotations

from functools import partial
from typing import Callable

from app.config import Settings, error_min_status

from .clf import ClfParser
from .models import LogEvent
from .parser import parse_line

LineParser = Callable[[str], LogEvent]


def build_parser(settings: Settings) -> LineParser:
    """Choose the line parser from ingestion.format and apply the configured error definition."""
    min_status = error_min_status(settings.detector.error_definition)
    ndjson = partial(parse_line, error_min_status=min_status)
    clf = ClfParser(settings.mapping, min_status)
    fmt = settings.ingestion.format
    if fmt == "ndjson":
        return ndjson
    if fmt == "clf":
        return clf

    def auto(line: str) -> LogEvent:                     # a JSON object starts with "{"; a CLF line starts with a host
        return ndjson(line) if line.lstrip()[:1] == "{" else clf(line)

    return auto
