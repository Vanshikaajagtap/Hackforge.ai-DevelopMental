"""Typed settings: thresholds from config.yaml, secrets/paths from the environment (.env)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Mapping

import yaml

ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class ProfileCfg:
    """Window and baseline sizing for a profile (demo | prod | nasa)."""
    window_seconds: float
    tick_seconds: float
    baseline_sample_every: float
    baseline_max_samples: int
    min_baseline_samples: int
    min_events: int
    level_shift_seconds: float


@dataclass(frozen=True)
class DetectorCfg:
    """Statistical guard rails, hysteresis tick counts and the error definition."""
    std_floor: float
    ratio_floor: float
    warmup_ceiling: float
    confirm_ticks: int
    resolve_ticks: int
    resolve_z: float
    resolve_ratio: float
    # Which HTTP statuses count as errors when a line carries a status: "5xx" | "4xx+5xx" | a minimum status (e.g. 404).
    # Applied by the parsers (it decides LogEvent.is_error); the detector itself only sees error / not-error.
    error_definition: str = "5xx"


@dataclass(frozen=True)
class SeverityRow:
    """Gate values for one severity level."""
    z: float
    rate: float
    ratio: float
    errors: int


@dataclass(frozen=True)
class SeverityCfg:
    """The MEDIUM / HIGH / CRITICAL gate table."""
    medium: SeverityRow
    high: SeverityRow
    critical: SeverityRow


@dataclass(frozen=True)
class AlertsCfg:
    """Retry policy, send timeout and the list of sinks."""
    retry_attempts: int
    retry_backoff_seconds: float
    sink_timeout_seconds: float
    sinks: list[str]


@dataclass(frozen=True)
class IngestionCfg:
    """Tailer behaviour and the input format."""
    start_at: str
    poll_seconds: float
    queue_max: int
    checkpoint_every_seconds: float
    format: str = "auto"          # ndjson | clf | auto (auto: a line starting with "{" is NDJSON, anything else is CLF)


@dataclass(frozen=True)
class MappingCfg:
    """Common Log Format -> LogEvent mapping."""
    service_prefixes: list[str] = field(default_factory=list)   # first URL path segments that become their own service
    other_service: str = "other"                                 # everything else
    root_service: str = "root"                                   # requests for "/"
    top_n: int = 0                                               # documentation only: how many prefixes the analysis kept


@dataclass(frozen=True)
class ReplayCfg:
    """Dataset replayer (scripts/replay_dataset.py and the demo API)."""
    dataset_path: str = "data/datasets/NASA_access_log_Jul95"   # falls back to access_log_Jul95 in the same folder
    state_file: str = ""                                         # heartbeat for an external replay; "" = replay.state.json next to LOG_PATH
    speed: float = 60.0                                          # original seconds per wall-clock second
    max_gap_seconds: float = 15.0                                # cap for a silent gap, in wall-clock seconds after scaling
    chunk_seconds: float = 0.1
    stale_seconds: float = 5.0                                   # a state file older than this means "no replay"
    dataset_tz_minutes: int = -240                               # the NASA log's own zone (-0400); naive preset times use it
    send_to_aws: bool = False                                    # SNS/CloudWatch stay OFF during a replay unless true...
    aws_preset: str = ""                                         # ...or a run of exactly this preset asks for them
    aws_max_sends_per_run: int = 20                              # hard cap on AWS deliveries in one run
    presets: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StorageCfg:
    """SQLite retention and how much history a new dashboard receives."""
    snapshot_retention_hours: float
    prune_every_seconds: float
    hello_history_minutes: float


@dataclass(frozen=True)
class HealthCfg:
    """Thresholds behind the health status."""
    degraded_queue_fraction: float
    degraded_tail_lag_seconds: float
    events_per_second_span_seconds: float
    silence_windows: float


@dataclass(frozen=True)
class GeneratorCfg:
    """Parameters of the synthetic demo generator."""
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
    autostart: bool = True        # demo mode: start the synthetic generator at boot (off for dataset profiles)


@dataclass(frozen=True)
class Settings:
    """Everything the app needs: thresholds from config.yaml plus environment-supplied paths and secrets."""
    profile_name: str
    profile: ProfileCfg
    detector: DetectorCfg
    severity: SeverityCfg
    alerts: AlertsCfg
    ingestion: IngestionCfg
    storage: StorageCfg
    health: HealthCfg
    generator: GeneratorCfg
    mapping: MappingCfg
    replay: ReplayCfg
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


def error_min_status(definition: str | int) -> int:
    """'5xx' -> 500, '4xx+5xx' -> 400, or an explicit minimum status such as 404. Raises ValueError otherwise."""
    d = str(definition).strip().lower().replace(" ", "")
    if d in {"5xx", "5"}:
        return 500
    if d in {"4xx+5xx", "4xx", "4xx+", "400+"}:
        return 400
    if d.isdigit() and 100 <= int(d) <= 599:
        return int(d)
    raise ValueError(f"detector.error_definition must be '5xx', '4xx+5xx' or a status 100-599, got {definition!r}")


def _deep_merge(base: dict, over: dict) -> dict:
    out = dict(base)
    for k, v in over.items():
        out[k] = _deep_merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


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
    """Load config.yaml (with profile overrides) and the environment into a Settings object."""
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
    if profile_name not in raw["profiles"]:
        raise ValueError(f"unknown profile {profile_name!r}; config.yaml defines {sorted(raw['profiles'])}")
    prof = dict(raw["profiles"][profile_name])
    # A profile may carry `overrides:` - sections deep-merged over the top-level ones for this profile only
    # (detector / severity / ingestion / mapping / generator / replay ...). That is how the `nasa` profile changes
    # thresholds, the error definition and the parser without touching the defaults.
    raw = _deep_merge(raw, prof.pop("overrides", {}) or {})
    sev = raw["severity"]
    error_min_status(raw["detector"].get("error_definition", "5xx"))            # fail fast on a typo
    if raw["ingestion"].get("format", "auto") not in {"ndjson", "clf", "auto"}:
        raise ValueError("ingestion.format must be ndjson, clf or auto")
    log_path = e.get("LOG_PATH", "data/app.log")
    replay = ReplayCfg(**(raw.get("replay") or {}))
    if e.get("DATASET_PATH"):
        replay = replace(replay, dataset_path=e["DATASET_PATH"])                   # e.g. /data/datasets/... inside Docker
    if not replay.state_file:                                                      # shared with the app via the mounted data dir
        replay = replace(replay, state_file=os.path.join(os.path.dirname(log_path) or ".", "replay.state.json"))
    return Settings(
        profile_name=profile_name,
        profile=ProfileCfg(**prof),
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
        mapping=MappingCfg(**(raw.get("mapping") or {})),
        replay=replay,
        demo_mode=_truthy(e.get("DEMO_MODE")),
        log_path=log_path,
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
