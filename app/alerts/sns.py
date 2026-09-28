"""AWS SNS sink: publishes the alert as an email notification."""
from __future__ import annotations

import asyncio
import re

from app.detection.models import Alert

from .aws_common import BOTO_CFG, aws_session
from .base import ascii_safe, render


def _subject(alert: Alert, event: str) -> str:
    """SNS email subjects must be single-line ASCII and under 100 characters."""
    s = f"[{alert.severity}] {alert.service} error rate ({event})"
    return re.sub(r"[^\x20-\x7E]", "", s)[:99]


class SnsSink:
    """AWS SNS publish. Built only when AWS_ENABLED=true. boto3 is synchronous, so it runs in a thread."""
    name = "sns"

    def __init__(self, topic_arn: str, region: str | None = None, window_seconds: float | None = None, client=None) -> None:
        self.arn = topic_arn
        self.region = region
        self.window_seconds = window_seconds
        self.client = client or aws_session(region).client("sns", config=BOTO_CFG)

    async def send(self, alert: Alert, event: str) -> str:
        """Publish the alert; returns the SNS MessageId as proof of delivery."""
        def _publish() -> str:
            resp = self.client.publish(
                TopicArn=self.arn,
                Subject=_subject(alert, event),
                Message=render(alert, event, self.window_seconds),
                MessageAttributes={   # lets you add SNS subscription filter policies later
                    "severity": {"DataType": "String", "StringValue": alert.severity},
                    "service": {"DataType": "String", "StringValue": ascii_safe(alert.service) or "unknown"},
                    "event": {"DataType": "String", "StringValue": event},
                },
            )
            return resp["MessageId"]

        return await asyncio.to_thread(_publish)

    async def healthcheck(self) -> None:
        """Verify the topic is reachable (sns:GetTopicAttributes)."""
        await asyncio.to_thread(self.client.get_topic_attributes, TopicArn=self.arn)
