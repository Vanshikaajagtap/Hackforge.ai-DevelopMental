"""Shared AWS plumbing. Imported only when AWS sinks are built, so the rest of the app never needs AWS to be reachable.

Credentials come from the standard boto3 chain (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in the environment or an
instance role). They are never read, logged or stored by LogPulse itself.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Callable, Mapping

import boto3
from botocore.config import Config

log = logging.getLogger("logpulse.aws")

# Keep boto's own retries low: the Dispatcher owns retry/backoff and delivery status.
BOTO_CFG = Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=3, read_timeout=5)

DEFAULT_REGION = "ap-south-1"


def aws_session(region: str | None = None) -> boto3.Session:
    """A boto3 session pinned to `region` (else AWS_REGION / AWS_DEFAULT_REGION / ap-south-1)."""
    return boto3.Session(region_name=region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or DEFAULT_REGION)


def _short(e: Exception) -> str:
    return f"{type(e).__name__}: {e}"[:200]


async def aws_startup_check(sinks: Mapping[str, object], record: Callable[[str, str], None]) -> None:
    """Fail-soft credential + resource check. Never raises, never blocks startup: run it as a background task.

    Reports through `record(name, status)` (feeds the dashboard health panel) with names
    `aws_identity`, `sns`, `cloudwatch` and statuses `ok` / `error: ...`.
    """
    aws_sinks = [s for s in sinks.values() if getattr(s, "name", "") in ("sns", "cloudwatch")]
    if not aws_sinks:
        return
    try:
        region = getattr(aws_sinks[0], "region", None)
        sts = aws_session(region).client("sts", config=BOTO_CFG)
        ident = await asyncio.to_thread(sts.get_caller_identity)
        log.info("AWS credentials OK: %s", ident.get("Arn"))
        record("aws_identity", "ok")
    except Exception as e:  # noqa: BLE001 - detection and the other channels carry on regardless
        log.error("AWS credential check failed: %s", _short(e))
        record("aws_identity", f"error: {_short(e)}")
        for s in aws_sinks:
            record(s.name, "unchecked: no valid credentials")
        return
    for s in aws_sinks:
        check = getattr(s, "healthcheck", None)
        if check is None:
            continue
        try:
            await check()
            record(s.name, "ok")
        except Exception as e:  # noqa: BLE001
            log.error("AWS %s check failed: %s", s.name, _short(e))
            record(s.name, f"error: {_short(e)}")
