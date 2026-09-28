"""Replay synthetic log lines through the parser + detector and report MEASURED numbers.

  python scripts/benchmark.py                 # 10k / 50k / 100k
  python scripts/benchmark.py --sizes 200000

Two measurements per size:
  core      parse_line + DetectionEngine.on_event per event (p50/p95/p99 latency), plus the 1 Hz ticks
  pipeline  real file -> Tailer -> parser -> bounded queue -> engine, producer and consumer concurrent;
            reports end-to-end events/s and the maximum queue depth reached
Nothing here is an estimate; the numbers depend on the machine that runs it.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import load_settings  # noqa: E402
from app.detection.detector import DetectionEngine  # noqa: E402
from app.ingestion.parser import parse_line  # noqa: E402
from app.ingestion.tailer import Tailer  # noqa: E402

RATE = 1000.0            # simulated events/s of event-time (controls tick density, not replay speed)
SERVICES = ("payment-service", "auth-service", "orders-service", "search-service")


def synth(n: int, start: float = 1_800_000_000.0) -> list[str]:
    """Generate `n` synthetic NDJSON lines with steady 5% errors across four services."""
    lines = []
    for i in range(n):
        ts = start + i / RATE
        is_err = i % 20 == 0                                           # 5 % errors
        lines.append(json.dumps({
            "timestamp": datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="milliseconds"),
            "service": SERVICES[i % len(SERVICES)], "level": "ERROR" if is_err else "INFO",
            "status": 500 if is_err else 200, "message": "synthetic", "request_id": f"req-{i}",
        }))
    return lines


class ReplayClock:
    """A clock that only moves when the replay tells it to."""
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def bench_core(lines: list[str], settings) -> dict:
    """Measure parse + detect latency and throughput."""
    clock = ReplayClock()
    engine = DetectionEngine(settings, clock)
    lat: list[int] = []
    tick_ns = 0
    ticks = 0
    next_tick = None
    t_all = time.perf_counter()
    for line in lines:
        t0 = time.perf_counter_ns()
        ev = parse_line(line)
        clock.now = max(clock.now, ev.ts)
        engine.on_event(ev)
        lat.append(time.perf_counter_ns() - t0)
        if next_tick is None:
            next_tick = ev.ts + 1
        while clock.now >= next_tick:
            t1 = time.perf_counter_ns()
            engine.tick()
            tick_ns += time.perf_counter_ns() - t1
            ticks += 1
            next_tick += 1
    elapsed = time.perf_counter() - t_all
    lat.sort()
    q = lambda p: lat[min(len(lat) - 1, int(len(lat) * p))] / 1000.0   # noqa: E731  (us)
    return {"events_per_s": len(lines) / elapsed, "p50_us": q(0.50), "p95_us": q(0.95), "p99_us": q(0.99),
            "mean_us": statistics.fmean(lat) / 1000.0, "ticks": ticks, "tick_ms_avg": (tick_ns / max(ticks, 1)) / 1e6}


async def bench_pipeline(lines: list[str], settings) -> dict:
    """Measure file -> tailer -> queue -> engine throughput and queue depth."""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "app.log")
        Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")
        clock = ReplayClock()
        engine = DetectionEngine(settings, clock)
        queue: asyncio.Queue = asyncio.Queue(maxsize=settings.ingestion.queue_max)
        tailer = Tailer(path, start_at="checkpoint", poll_seconds=0.001)
        stats = {"max_depth": 0, "dropped": 0, "consumed": 0}

        async def produce(line: str) -> None:
            ev = parse_line(line)
            if queue.full():
                queue.get_nowait()
                stats["dropped"] += 1
            queue.put_nowait(ev)
            stats["max_depth"] = max(stats["max_depth"], queue.qsize())

        async def consume() -> None:
            next_tick = None
            while True:
                ev = await queue.get()
                clock.now = max(clock.now, ev.ts)
                engine.on_event(ev)
                stats["consumed"] += 1
                if next_tick is None:
                    next_tick = ev.ts + 1
                while clock.now >= next_tick:
                    engine.tick()
                    next_tick += 1                       # (same shape as Runtime._consume: yields only when the queue is empty)

        t0 = time.perf_counter()
        consumer = asyncio.create_task(consume())
        producer = asyncio.create_task(tailer.run(produce))
        while stats["consumed"] + stats["dropped"] < len(lines):
            await asyncio.sleep(0.005)
        elapsed = time.perf_counter() - t0
        producer.cancel()
        consumer.cancel()
        await asyncio.gather(producer, consumer, return_exceptions=True)   # let the tailer close the file
        return {"events_per_s": stats["consumed"] / elapsed, "max_queue_depth": stats["max_depth"],
                "dropped": stats["dropped"], "queue_capacity": queue.maxsize}


def main() -> None:
    """CLI entry point."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sizes", type=int, nargs="+", default=[10_000, 50_000, 100_000])
    args = ap.parse_args()
    settings = load_settings(env={})
    print(f"python {platform.python_version()} on {platform.platform()} ({os.cpu_count()} logical CPUs), "
          f"profile={settings.profile_name}, {len(SERVICES)} services, ~5% errors\n")
    print(f"{'lines':>8} | {'core ev/s':>10} {'p50 us':>8} {'p95 us':>8} {'p99 us':>8} {'tick ms':>8} | "
          f"{'pipeline ev/s':>13} {'max queue':>10} {'dropped':>8}")
    for n in args.sizes:
        lines = synth(n)
        c = bench_core(lines, settings)
        p = asyncio.run(bench_pipeline(lines, settings))
        print(f"{n:>8} | {c['events_per_s']:>10,.0f} {c['p50_us']:>8.1f} {c['p95_us']:>8.1f} {c['p99_us']:>8.1f} "
              f"{c['tick_ms_avg']:>8.3f} | {p['events_per_s']:>13,.0f} {p['max_queue_depth']:>10} {p['dropped']:>8}")


if __name__ == "__main__":
    main()
