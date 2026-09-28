"""Typed settings: thresholds from config.yaml, secrets/paths from the environment (.env)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class ProfileCfg:
    window_seconds: float
    tick_seconds: float
    baseline_sample_every: float
    baseline_max_samples: int
    min_baseline_samples: int
    min_events: int
    level_shift_seconds: float


@dataclass(frozen=True)
class DetectorCfg:
    std_floor: float
    ratio_floor: float
    warmup_ceiling: float
    confirm_ticks: int
    resolve_ticks: int
    resolve_z: float
    resolve_ratio: float


@dataclass(frozen=True)
class SeverityRow:
    z: float
    rate: float
    ratio: float
    errors: int


@dataclass(frozen=True)
class SeverityCfg:
    medium: SeverityRow
    high: SeverityRow
    critical: SeverityRow


@dataclass(frozen=True)
class AlertsCfg:
    retry_attempts: int
    retry_backoff_seconds: float
    sink_timeout_seconds: float
    sinks: list[str]


@dataclass(frozen=True)
class IngestionCfg:
    start_at: str
    poll_seconds: float
    queue_max: int
    checkpoint_every_seconds: float


@dataclass(frozen=True)
class StorageCfg:
    snapshot_retention_hours: float
    prune_every_seconds: float
    hello_history_minutes: float


@dataclass(frozen=True)
class HealthCfg:
    degraded_queue_fraction: float
    degraded_tail_lag_seconds: float
    events_per_second_span_seconds: float
    silence_windows: float


@dataclass(frozen=True)
class GeneratorCfg:
    services: dict[str, float]
    normal_error_rate: float
    normal_error_drift: float
    error_spike_start_rate: float
    error_spike_rate: float
    critical_spike_rate: float
    error_spike_ramp_seconds: float
    error_spike_escalate_after_seconds: float
    traffic_spike_multiplier: float
    spike_service: str
    malformed_every: int
    mixed_timeline: list[list[Any]]


@dataclass(frozen=True)
class Settings:
    profile_name: str
    profile: ProfileCfg
    detector: DetectorCfg
    severity: SeverityCfg
    alerts: AlertsCfg
    ingestion: IngestionCfg
    storage: StorageCfg
    health: HealthCfg
    generator: GeneratorCfg
    # environment (secrets / paths / switches)
    demo_mode: bool = False
    log_path: str = "data/app.log"
    db_path: str = "data/logpulse.db"
    alerts_jsonl: str = "data/alerts.jsonl"
    ntfy_topic: str = ""
    ntfy_base_url: str = "https://ntfy.sh"
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    webhook_url: str = ""
    aws_enabled: bool = False
    aws_region: str = "ap-south-1"
    sns_topic_arn: str = ""
    cw_log_group: str = "/logpulse/alerts"
    cw_log_stream_prefix: str = "alerts"
    extra: dict[str, Any] = field(default_factory=dict)


def _truthy(v: str | None, default: bool = False) -> bool:
    return default if v is None else v.strip().lower() in {"1", "true", "yes", "on"}


def _load_dotenv(path: Path, env: dict[str, str]) -> None:
    """Minimal .env reader (KEY=VALUE, # comments). Real environment variables win."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def load_settings(path: str | os.PathLike | None = None, env: Mapping[str, str] | None = None) -> Settings:
    if env is None:
        e = dict(os.environ)
        _load_dotenv(ROOT / ".env", e)
        # boto3 reads credentials from the process environment, not from our settings object, so a local `.env`
        # (docker compose already injects it) must be exported. Empty values are skipped; real env vars win.
        for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_DEFAULT_REGION"):
            if e.get(key):
                os.environ.setdefault(key, e[key])
    else:
        e = dict(env)
    cfg_path = Path(path or e.get("CONFIG_PATH") or ROOT / "config.yaml")
    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    profile_name = e.get("LOGPULSE_PROFILE") or raw["profile"]
    sev = raw["severity"]
    return Settings(
        profile_name=profile_name,
        profile=ProfileCfg(**raw["profiles"][profile_name]),
        detector=DetectorCfg(**raw["detector"]),
        severity=SeverityCfg(
            medium=SeverityRow(**sev["medium"]),
            high=SeverityRow(**sev["high"]),
            critical=SeverityRow(**sev["critical"]),
        ),
        alerts=AlertsCfg(**raw["alerts"]),
        ingestion=IngestionCfg(**raw["ingestion"]),
        storage=StorageCfg(**raw["storage"]),
        health=HealthCfg(**raw["health"]),
        generator=GeneratorCfg(**raw["generator"]),
        demo_mode=_truthy(e.get("DEMO_MODE")),
        log_path=e.get("LOG_PATH", "data/app.log"),
        db_path=e.get("DB_PATH", "data/logpulse.db"),
        alerts_jsonl=e.get("ALERTS_JSONL", "data/alerts.jsonl"),
        ntfy_topic=e.get("NTFY_TOPIC", ""),
        ntfy_base_url=e.get("NTFY_BASE_URL") or "https://ntfy.sh",
        telegram_bot_token=e.get("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=e.get("TELEGRAM_CHAT_ID", ""),
        webhook_url=e.get("WEBHOOK_URL", ""),
        aws_enabled=_truthy(e.get("AWS_ENABLED")),
        aws_region=e.get("AWS_REGION", "ap-south-1"),
        sns_topic_arn=e.get("SNS_TOPIC_ARN", ""),
        cw_log_group=e.get("CW_LOG_GROUP", "/logpulse/alerts"),
        cw_log_stream_prefix=e.get("CW_LOG_STREAM_PREFIX") or "alerts",
    )
