"""Replay the real NASA HTTP access log into the file LogPulse's tailer watches (LOG_PATH), in accelerated real time.

Every line's timestamp is rewritten to "now" while the original inter-arrival gaps are preserved (divided by --speed);
the untouched original timestamp is kept next to it (CLF: a trailing `orig_ts="..."`, NDJSON: `orig_timestamp`).

  python scripts/replay_dataset.py --list-presets
  python scripts/replay_dataset.py --preset spike-history-jul24
  python scripts/replay_dataset.py --start "1995-07-13 06:30" --end "1995-07-13 11:00" --speed 180 --seed 1
  python scripts/replay_dataset.py --preset normal-jul16 --loop

--speed is original seconds per wall-clock second (180: half an original hour passes in 10 s). Silent gaps are capped at
--max-gap wall seconds (keep it above the detector window so LOW_DATA still shows). Times are the log's own zone (-0400).
Start the app with LOGPULSE_PROFILE=nasa so the tuned window/thresholds and the CLF parser are active.

AWS safety: while a replay runs, SNS and CloudWatch are OFF (replay.send_to_aws: false). Only the preset named in
replay.aws_preset may opt in with --aws, and even then the app enforces replay.aws_max_sends_per_run.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


async def _heartbeat(state_file: str, base: dict, stop: asyncio.Event, aws: bool) -> None:
    from app.replay import write_state_file
    while not stop.is_set():
        write_state_file(state_file, {**base, "active": True, "aws": aws, "updated_at": time.time()})
        try:
            await asyncio.wait_for(stop.wait(), 1.0)
        except asyncio.TimeoutError:
            pass


async def _progress(rep, stop: asyncio.Event, fmt_orig, tz: int) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), 5.0)
        except asyncio.TimeoutError:
            pr = rep.progress
            when = fmt_orig(pr.orig_time, tz) if pr.orig_time else "-"
            pct = f"{pr.progress * 100:5.1f}%" if pr.progress is not None else "  -  "
            print(f"[replay] {pct}  original time {when}  lines sent {pr.lines_sent:,}  gaps capped {pr.gaps_capped}", flush=True)


async def amain(args: argparse.Namespace) -> int:
    """Async entry: resolve the preset, announce the run via the heartbeat file, replay, clean up."""
    os.environ.setdefault("LOGPULSE_PROFILE", args.profile)
    from app.config import load_settings
    from app.ingestion.clf import ClfParser
    from app.replay import (Replayer, ReplaySpec, fmt_orig, parse_when, resolve_dataset_path, write_state_file)

    s = load_settings()
    r = s.replay
    if args.list_presets:
        for name, p in (r.presets or {}).items():
            aws = "  [may use AWS with --aws]" if name == r.aws_preset else ""
            print(f"{name:<22}{p.get('start')} -> {p.get('end')}  speed {p.get('speed', r.speed)}{aws}\n{'':<22}{p.get('description', '')}")
        return 0

    p = dict((r.presets or {}).get(args.preset, {})) if args.preset else {}
    if args.preset and not p:
        print(f"unknown preset {args.preset!r}; use --list-presets", file=sys.stderr)
        return 2
    tz = r.dataset_tz_minutes
    start_txt, end_txt = args.start or p.get("start"), args.end or p.get("end")
    spec = ReplaySpec(
        start_ts=parse_when(start_txt, tz) if start_txt else None,
        end_ts=parse_when(end_txt, tz) if end_txt else None,
        speed=args.speed or float(p.get("speed") or r.speed),
        seed=args.seed if args.seed is not None else p.get("seed", 1),
        loop=args.loop or bool(p.get("loop", False)),
        max_gap_seconds=args.max_gap if args.max_gap is not None else float(p.get("max_gap_seconds", r.max_gap_seconds)),
        fmt=args.format,
    )
    try:
        dataset = resolve_dataset_path(args.dataset or r.dataset_path)
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        return 2
    rep = Replayer(dataset, spec, ClfParser(s.mapping), chunk_seconds=r.chunk_seconds)
    out = args.out or s.log_path
    name = args.preset or "custom"
    if spec.max_gap_seconds < s.profile.window_seconds:
        print(f"[replay] note: --max-gap {spec.max_gap_seconds:g}s is below the detector window ({s.profile.window_seconds:g}s): "
              "a silent gap will not empty the window, so LOW_DATA will not show", file=sys.stderr)
    if args.aws and name != r.aws_preset and not r.send_to_aws:
        print(f"[replay] --aws ignored by the app: only preset {r.aws_preset!r} may use AWS (replay.aws_preset)", file=sys.stderr)
    print(f"[replay] {name}: {fmt_orig(rep._lo, tz)} -> {fmt_orig(rep._hi, tz)}  speed {spec.speed:g}x  "
          f"~{(rep._hi - rep._lo) / spec.speed:,.0f}s wall  -> {out}  (profile {s.profile_name}, AWS {'requested' if args.aws else 'OFF'})", flush=True)

    stop = asyncio.Event()
    run_id = f"cli-{int(time.time())}-{os.getpid()}"
    base = {"run_id": run_id, "preset": name, "pid": os.getpid()}
    write_state_file(r.state_file, {**base, "active": True, "aws": args.aws, "updated_at": time.time()})
    tasks = [asyncio.create_task(_heartbeat(r.state_file, base, stop, args.aws)),
             asyncio.create_task(_progress(rep, stop, fmt_orig, tz))]
    try:
        await rep.run(out, stop)
        print(f"[replay] done: {rep.progress.lines_sent:,} lines, {rep.progress.loops} pass(es), "
              f"{rep.progress.gaps_capped} silent gap(s) capped", flush=True)
        return 0
    except KeyboardInterrupt:
        return 130
    finally:
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        write_state_file(r.state_file, {**base, "active": False, "aws": False, "updated_at": time.time()})


def main() -> None:
    """CLI entry point."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--preset", help="a named preset from config.yaml (see --list-presets)")
    ap.add_argument("--list-presets", action="store_true")
    ap.add_argument("--start", help="original time, e.g. '1995-07-13 06:30' or '13/Jul/1995:06:30:00 -0400' (default: file start)")
    ap.add_argument("--end", help="original time (default: file end)")
    ap.add_argument("--speed", type=float, help="original seconds per wall second (default: the preset's / replay.speed)")
    ap.add_argument("--seed", type=int, help="seeds the sub-second spread of same-second requests: same seed, same replay")
    ap.add_argument("--loop", action="store_true", help="restart the segment when it ends")
    ap.add_argument("--max-gap", type=float, help="cap for a silent gap, in wall seconds (default: replay.max_gap_seconds)")
    ap.add_argument("--format", choices=["clf", "ndjson"], default="clf", help="what to append to the log (default clf)")
    ap.add_argument("--dataset", help="the raw log (default: replay.dataset_path / DATASET_PATH)")
    ap.add_argument("--out", help="log file to append to (default: LOG_PATH)")
    ap.add_argument("--profile", default="nasa", help="config profile to load (default nasa)")
    ap.add_argument("--aws", action="store_true", help="ask the app to allow SNS/CloudWatch for this run (only the aws_preset is honoured)")
    args = ap.parse_args()
    try:
        sys.exit(asyncio.run(amain(args)))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
