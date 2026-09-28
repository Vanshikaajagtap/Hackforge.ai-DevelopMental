"""Sink registry: builds the sinks named in config.alerts.sinks, skipping any that are not configured."""
from __future__ import annotations

import logging

from app.config import Settings

from .base import AlertSink

log = logging.getLogger("logpulse.alerts")


def build_sinks(settings: Settings) -> tuple[dict[str, AlertSink], dict[str, str]]:
    """Returns (enabled sinks by name, {name: reason} for sinks that were requested but not enabled)."""
    w = settings.profile.window_seconds
    sinks: dict[str, AlertSink] = {}
    skipped: dict[str, str] = {}

    for name in settings.alerts.sinks:
        try:
            if name == "console":
                from .console import ConsoleSink
                sinks[name] = ConsoleSink(w)
            elif name == "jsonl":
                from .jsonl import JsonlSink
                sinks[name] = JsonlSink(settings.alerts_jsonl)
            elif name == "ntfy":
                topic = settings.ntfy_topic
                if not topic or "CHANGE-ME" in topic:
                    skipped[name] = "NTFY_TOPIC not set (empty or still the .env.example placeholder)"
                    continue
                from .ntfy import NtfySink
                sinks[name] = NtfySink(topic, base_url=settings.ntfy_base_url, window_seconds=w)
            elif name == "telegram":
                if not (settings.telegram_bot_token and settings.telegram_chat_id):
                    skipped[name] = "TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set"
                    continue
                from .telegram import TelegramSink
                sinks[name] = TelegramSink(settings.telegram_bot_token, settings.telegram_chat_id, window_seconds=w)
            elif name == "webhook":
                if not settings.webhook_url:
                    skipped[name] = "WEBHOOK_URL not set"
                    continue
                from .webhook import WebhookSink
                sinks[name] = WebhookSink(settings.webhook_url, window_seconds=w)
            elif name in {"sns", "cloudwatch"}:
                if not settings.aws_enabled:
                    skipped[name] = "AWS_ENABLED is not true"
                    continue
                if name == "sns":
                    arn = settings.sns_topic_arn
                    if not arn or "<ACCOUNT_ID>" in arn:
                        skipped[name] = "SNS_TOPIC_ARN not set (empty or still the .env.example placeholder)"
                        continue
                    from .sns import SnsSink
                    sinks[name] = SnsSink(arn, settings.aws_region, w)
                else:
                    if not settings.cw_log_group:
                        skipped[name] = "CW_LOG_GROUP not set"
                        continue
                    from .cloudwatch import CloudWatchSink
                    sinks[name] = CloudWatchSink(settings.cw_log_group, settings.cw_log_stream_prefix, settings.aws_region)
            else:
                skipped[name] = "unknown sink"
        except Exception as e:  # noqa: BLE001 - a misconfigured sink must not stop the app
            skipped[name] = f"failed to start: {e}"
    for name, why in skipped.items():
        log.warning("sink %s disabled: %s", name, why)
    return sinks, skipped
