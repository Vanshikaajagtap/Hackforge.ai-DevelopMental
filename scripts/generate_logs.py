"""Append demo NDJSON logs to the log file (the same generator the dashboard buttons drive).

  python scripts/generate_logs.py --scenario mixed --path data/app.log
  python scripts/generate_logs.py --scenario error_spike --duration 60 --seed 42
  docker compose exec app python scripts/generate_logs.py --scenario error_spike
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import load_settings  # noqa: E402
from app.generator import SCENARIOS, LogGenerator, ScenarioController  # noqa: E402


async def _play(gen: LogGenerator, path: str, scenario: str, duration: float) -> None:
    stop = asyncio.Event()
    gen.controller.set(scenario)
    print(f"[generator] {scenario} for {duration:g}s -> {path}", flush=True)
    task = asyncio.create_task(gen.run(path, stop=stop))
    try:
        await asyncio.sleep(duration)
    finally:
        stop.set()
        await task


def main() -> None:
    """CLI entry point."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenario", choices=SCENARIOS, default="normal")
    ap.add_argument("--duration", type=float, help="seconds (default: 30, or the whole timeline for `mixed`)")
    ap.add_argument("--path", help="log file (default: LOG_PATH or data/app.log)")
    ap.add_argument("--seed", type=int, help="seed the RNG for a reproducible run")
    args = ap.parse_args()

    settings = load_settings()
    path = args.path or settings.log_path
    ctl = ScenarioController(settings.generator)
    duration = args.duration or (ctl.timeline_end if args.scenario == "mixed" else 30)
    try:
        asyncio.run(_play(LogGenerator(settings.generator, ctl, seed=args.seed), path, args.scenario, duration))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
