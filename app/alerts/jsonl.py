"""JSONL sink: appends one structured JSON object per alert event (CloudWatch-compatible schema)."""
from __future__ import annotations

import json
import os

from app.detection.models import Alert

from .base import alert_payload


class JsonlSink:
    """Structured alert log - the CloudWatch-Logs stand-in (same schema as CloudWatchSink's message)."""
    name = "jsonl"

    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

    async def send(self, alert: Alert, event: str) -> None:
        """Append the alert event as one JSON line."""
        with open(self.path, "a", encoding="utf-8", newline="\n") as fh:
            fh.write(json.dumps(alert_payload(alert, event), ensure_ascii=False) + "\n")
