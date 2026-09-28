"""CloudWatch Logs sink: one JSON event per alert in a per-UTC-day log stream (the evidence trail)."""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone

from app.detection.models import Alert

from .aws_common import BOTO_CFG, aws_session
from .base import alert_payload


class CloudWatchSink:
    """AWS CloudWatch Logs PutLogEvents: the evidence trail. The message is the same JSON the JSONL sink writes.

    The log GROUP must already exist (created once at setup, with 7-day retention); this sink only creates streams,
    one per UTC day, so the IAM policy needs just logs:CreateLogStream + logs:PutLogEvents.
    """
    name = "cloudwatch"

    def __init__(self, group: str, stream_prefix: str = "alerts", region: str | None = None, client=None) -> None:
        self.group = group
        self.prefix = stream_prefix
        self.region = region
        self.client = client or aws_session(region).client("logs", config=BOTO_CFG)
        self._ready: set[str] = set()

    def _stream_name(self) -> str:
        return f"{self.prefix}/{datetime.now(timezone.utc):%Y-%m-%d}"

    def _ensure_stream(self, stream: str) -> None:
        if stream in self._ready:
            return
        try:
            self.client.create_log_stream(logGroupName=self.group, logStreamName=stream)
        except self.client.exceptions.ResourceAlreadyExistsException:
            pass
        self._ready.add(stream)

    async def send(self, alert: Alert, event: str) -> str:
        """Write the alert as a JSON log event; returns `group:stream` as proof of delivery."""
        def _put() -> str:
            stream = self._stream_name()
            event_row = [{"timestamp": int(time.time() * 1000), "message": json.dumps(alert_payload(alert, event))}]
            self._ensure_stream(stream)
            try:
                self.client.put_log_events(logGroupName=self.group, logStreamName=stream, logEvents=event_row)
            except self.client.exceptions.ResourceNotFoundException:
                self._ready.discard(stream)         # stream deleted behind our back: recreate once (raises if the group is gone)
                self._ensure_stream(stream)
                self.client.put_log_events(logGroupName=self.group, logStreamName=stream, logEvents=event_row)
            return f"{self.group}:{stream}"

        return await asyncio.to_thread(_put)

    async def healthcheck(self) -> None:
        """Uses only the two permissions the app already has: creating today's stream proves the group exists
        and that we may write to it."""
        await asyncio.to_thread(self._ensure_stream, self._stream_name())
