"""Dataset replay: append a real access log to the file the tailer watches, with timestamps rewritten to "now".

Pieces (all pure / injectable so they are testable without a 200 MB file or real time):
  * seeking          find_offset() - binary search a time-sorted log by timestamp, iter_segment()
  * timeline()       original inter-arrival gaps scaled by `speed`, long silences capped, seeded sub-second spread
  * Replayer         writes the timeline to a file in real time (clock and sleep are injectable)
  * ReplayGuard      keeps SNS / CloudWatch OFF during a replay (opt-in for one preset, hard cap per run)
  * ReplayController in-process start/stop for the demo API and dashboard buttons
  * simulate()       runs the real detector + state machine over a timeline on a virtual clock (tuning, tests)

`speed` means original seconds per wall-clock second (speed 60: one original minute passes each second).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator

from app.config import ReplayCfg, Settings
from app.detection.detector import DetectionEngine
from app.detection.state import AlertStateMachine
from app.ingestion.clf import ClfParser, format_clf_timestamp, parse_clf_timestamp
from app.ingestion.factory import build_parser
from app.ingestion.parser import ParseError

log = logging.getLogger("logpulse.replay")

AWS_CHANNELS = frozenset({"sns", "cloudwatch"})
_TS = re.compile(r"\[(\d\d/[A-Za-z]{3}/\d{4}:\d\d:\d\d:\d\d [+-]\d{4})\]")
_FALLBACK_NAMES = ("access_log_Jul95", "NASA_access_log_Jul95")


# =================================================================================================
# Dataset access
# =================================================================================================
def resolve_dataset_path(path: str | os.PathLike) -> Path:
    """The configured path, or - since the file is often saved as `access_log_Jul95` - a known alternative name next to it."""
    p = Path(path)
    if p.is_file():
        return p
    for name in _FALLBACK_NAMES:
        if (p.parent / name).is_file():
            return p.parent / name
    raise FileNotFoundError(
        f"dataset not found: {p} (also tried {', '.join(_FALLBACK_NAMES)} in {p.parent}). "
        "Download the NASA HTTP Jul-95 log and place it in data/datasets/ - see the README.")


def extract_ts(line: str) -> float | None:
    """Original timestamp of a CLF line, or None if it has none."""
    m = _TS.search(line)
    if not m:
        return None
    try:
        return parse_clf_timestamp(m.group(1))
    except ParseError:
        return None


def _line_ts_after(fh, pos: int, size: int) -> tuple[float | None, int]:
    """Timestamp and start offset of the first COMPLETE line at/after `pos` that has a parseable timestamp."""
    if pos > 0:
        fh.seek(pos - 1)
        fh.readline()                                     # discard the (possibly partial) line we landed in
    else:
        fh.seek(0)
    for _ in range(50):
        start = fh.tell()
        raw = fh.readline()
        if not raw:
            return None, size
        ts = extract_ts(raw.decode("latin-1"))
        if ts is not None:
            return ts, start
    return None, size


def find_offset(path: str | os.PathLike, start_ts: float) -> int:
    """Byte offset of the first line whose timestamp is >= start_ts (the log is chronological)."""
    size = os.path.getsize(path)
    with open(path, "rb") as fh:
        lo, hi = 0, size
        while hi - lo > 1 << 16:
            mid = (lo + hi) // 2
            ts, _ = _line_ts_after(fh, mid, size)
            if ts is None or ts >= start_ts:
                hi = mid
            else:
                lo = mid
        pos = lo
        if pos > 0:
            fh.seek(pos - 1)
            fh.readline()
            pos = fh.tell()
        else:
            fh.seek(0)
        while True:
            here = fh.tell()
            raw = fh.readline()
            if not raw:
                return size
            ts = extract_ts(raw.decode("latin-1"))
            if ts is not None and ts >= start_ts:
                return here


@dataclass(frozen=True)
class Rec:
    """A dataset line with its original timestamp (None when unusable)."""
    ts: float | None          # original epoch seconds (None: a line without a usable timestamp)
    line: str


def iter_file_lines(path: str | os.PathLike, offset: int = 0) -> Iterator[str]:
    r"""Non-blank lines split on \n only (real lines contain \x0c and \x85)."""
    with open(path, "rb") as fh:
        fh.seek(offset)
        for raw in fh:
            line = raw.decode("latin-1").rstrip("\r\n")     # latin-1 keeps every byte of this 1995 file intact
            if line.strip():
                yield line


def iter_segment(path: str | os.PathLike, start_ts: float | None = None, end_ts: float | None = None) -> Iterator[Rec]:
    """Records whose timestamp is in [start_ts, end_ts); untimed lines ride along."""
    offset = find_offset(path, start_ts) if start_ts is not None else 0
    for line in iter_file_lines(path, offset):
        ts = extract_ts(line)
        if end_ts is not None and ts is not None and ts >= end_ts:
            return
        yield Rec(ts, line)


def dataset_bounds(path: str | os.PathLike) -> tuple[float, float]:
    """(first, last) timestamp of the file, read from its two ends only."""
    with open(path, "rb") as fh:
        first = next((t for t in (extract_ts(ln.decode("latin-1")) for ln in fh) if t is not None), None)
        size = fh.seek(0, os.SEEK_END)
        fh.seek(max(0, size - 65536))
        tail = fh.read().decode("latin-1").splitlines()
    last = next((t for t in (extract_ts(ln) for ln in reversed(tail)) if t is not None), first)
    if first is None:
        raise ValueError("no timestamps found in dataset")
    return first, last


def parse_when(text: str | int | float, default_tz_minutes: int = -240) -> float:
    """A CLF stamp ('13/Jul/1995:08:00:00 -0400'), ISO-8601 ('1995-07-13T08:00-04:00') or a naive 'YYYY-MM-DD HH:MM[:SS]'
    (read in the dataset's own zone). Numbers are epoch seconds."""
    if isinstance(text, (int, float)):
        return float(text)
    s = str(text).strip()
    try:
        return parse_clf_timestamp(s)
    except ParseError:
        pass
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError as e:
        raise ValueError(f"cannot read time {text!r}: use '13/Jul/1995:08:00:00 -0400' or '1995-07-13 08:00'") from e
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).timestamp() - default_tz_minutes * 60
    return dt.timestamp()


def fmt_orig(epoch: float, tz_minutes: int = -240) -> str:
    """Format an original-time epoch in the dataset's zone."""
    return format_clf_timestamp(epoch, tz_minutes)


# =================================================================================================
# Timeline: scaled inter-arrival gaps, capped silences, seeded sub-second spread
# =================================================================================================
@dataclass
class TimelineStats:
    """Counters gathered while scheduling: capped gaps and the longest silence."""
    gaps_capped: int = 0
    longest_gap_orig: float = 0.0     # seconds of original silence (before capping)


def timeline(records: Iterable[Rec], speed: float, max_gap: float, seed: int | None = None,
             stats: TimelineStats | None = None) -> Iterator[tuple[float, Rec]]:
    """Yield (virtual_offset_seconds, record) with offsets starting at 0.

    * Original gaps are divided by `speed`; a scaled gap larger than `max_gap` is capped (a 6-hour night becomes
      `max_gap` seconds), and the cap is normally larger than the detector window so LOW_DATA still shows briefly.
    * The log has 1-second resolution, so records sharing a second are spread across that second's scaled slot with
      a seeded random spread (same seed -> identical replay). Lines without a timestamp ride along with their neighbour.
    * Out-of-order records are kept in the current group instead of moving time backwards.
    """
    if speed <= 0:
        raise ValueError("speed must be > 0")
    rng = random.Random(seed)
    st = stats if stats is not None else TimelineStats()
    base = 0.0
    prev_sec: int | None = None
    group: list[Rec] = []

    def flush() -> Iterator[tuple[float, Rec]]:
        # sorted, so time never runs backwards inside the slot; the RNG is seeded, so the spread is reproducible
        offs = sorted(rng.random() for _ in group)
        for off, rec in zip(offs, group):
            yield base + off / speed, rec

    for rec in records:
        sec = None if rec.ts is None else int(rec.ts)
        if prev_sec is None:
            prev_sec = sec
        elif sec is not None and sec > prev_sec:
            yield from flush()
            group = []
            gap = sec - prev_sec
            st.longest_gap_orig = max(st.longest_gap_orig, float(gap))
            scaled = gap / speed
            if scaled > max_gap:
                st.gaps_capped += 1
                scaled = max_gap
            base += scaled
            prev_sec = sec
        group.append(rec)
    yield from flush()


# =================================================================================================
# Rendering: rewrite the timestamp to "now" and keep the original next to it
# =================================================================================================
def render_clf(line: str, wall_ts: float) -> str:
    """Same CLF line with the bracketed timestamp replaced by wall-clock time (UTC) and the original appended."""
    m = _TS.search(line)
    if not m:
        return line                                        # malformed lines are passed through untouched (they get counted)
    new = format_clf_timestamp(wall_ts, 0)
    return f'{line[:m.start(1)]}{new}{line[m.end(1):]} orig_ts="{m.group(1)}"'


def render_ndjson(line: str, wall_ts: float, parser: ClfParser) -> str:
    """NDJSON form of a dataset line, with the original timestamp kept."""
    m = _TS.search(line)
    try:
        ev = parser(line)
    except ParseError:
        return line
    return json.dumps({
        "timestamp": datetime.fromtimestamp(wall_ts, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "service": ev.service, "level": ev.level, "status": ev.status, "message": ev.message,
        "request_id": ev.request_id, "orig_timestamp": m.group(1) if m else None,
    })


# =================================================================================================
# Replayer
# =================================================================================================
@dataclass
class ReplaySpec:
    """What to replay (window, speed, seed, loop, gap cap, output format)."""
    start_ts: float | None = None       # original epoch seconds (None: from the start of the file)
    end_ts: float | None = None
    speed: float = 60.0
    seed: int | None = None
    loop: bool = False
    max_gap_seconds: float = 15.0
    fmt: str = "clf"                    # clf | ndjson (what gets appended to the log)


@dataclass
class ReplayProgress:
    """Live progress of a replay."""
    running: bool = False
    lines_sent: int = 0
    loops: int = 0
    orig_time: float | None = None      # original timestamp of the record most recently written
    gaps_capped: int = 0
    progress: float | None = None       # 0..1 through the current pass


class Replayer:
    """Writes the rescaled timeline to the log file in real time (clock and sleep are injectable)."""
    def __init__(
        self,
        path: str | os.PathLike,
        spec: ReplaySpec,
        parser: ClfParser,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], "asyncio.Future"] = asyncio.sleep,
        chunk_seconds: float = 0.1,
        bounds: tuple[float, float] | None = None,
    ) -> None:
        if spec.fmt not in {"clf", "ndjson"}:
            raise ValueError("replay format must be clf or ndjson")
        self.path, self.spec, self.parser = Path(path), spec, parser
        self._clock, self._sleep, self._chunk = clock, sleep, chunk_seconds
        self.progress = ReplayProgress()
        first, last = bounds or dataset_bounds(self.path)
        self._lo = spec.start_ts if spec.start_ts is not None else first
        self._hi = spec.end_ts if spec.end_ts is not None else last

    def render(self, rec: Rec, wall_ts: float) -> str:
        """The output line for a record at wall-clock time `wall_ts`."""
        return render_clf(rec.line, wall_ts) if self.spec.fmt == "clf" else render_ndjson(rec.line, wall_ts, self.parser)

    async def run(self, out_path: str | os.PathLike, stop: asyncio.Event | None = None) -> None:
        """Play the segment (repeatedly if `loop`) until finished or stopped."""
        sp = self.spec
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        self.progress.running = True
        try:
            while True:
                stats = TimelineStats()
                t0 = self._clock()
                seg = iter_segment(self.path, sp.start_ts, sp.end_ts)
                seed = None if sp.seed is None else sp.seed + self.progress.loops
                if not await self._play(timeline(seg, sp.speed, sp.max_gap_seconds, seed, stats), t0, out_path, stop, stats):
                    return                                      # stopped
                self.progress.loops += 1
                self.progress.progress = 1.0
                if not sp.loop:
                    return
        finally:
            self.progress.running = False

    async def _play(self, items, t0: float, out_path, stop, stats: TimelineStats) -> bool:
        batch: list[str] = []
        span = max(self._hi - self._lo, 1.0)

        def write() -> None:
            nonlocal batch
            if batch:
                with open(out_path, "ab") as fh:
                    fh.write(("\n".join(batch) + "\n").encode("latin-1", "replace"))
                self.progress.lines_sent += len(batch)
                batch = []

        for v, rec in items:
            due = t0 + v
            if due > self._clock() and batch:
                write()                                         # about to wait: get what is already due onto disk first
            while True:
                now = self._clock()
                if stop is not None and stop.is_set():
                    write()
                    return False
                if due <= now + 0.0005:
                    break
                await self._sleep(min(due - now, self._chunk))
            batch.append(self.render(rec, due))
            if rec.ts is not None:
                self.progress.orig_time = rec.ts
                self.progress.progress = min(1.0, max(0.0, (rec.ts - self._lo) / span))
            self.progress.gaps_capped = stats.gaps_capped
            if len(batch) >= 500:
                write()
                await self._sleep(0)
        write()
        return True


# =================================================================================================
# AWS guard: SNS / CloudWatch stay OFF while a replay runs
# =================================================================================================
def write_state_file(path: str | os.PathLike, state: dict) -> None:
    """Atomic write so the app never reads half a file."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(state), encoding="utf-8")
    os.replace(tmp, p)


class ReplayGuard:
    """Decides whether an AWS delivery may be created. Outside a replay it always says yes.

    During a replay: `send_to_aws: false` (default) suppresses every SNS/CloudWatch delivery; a run of the single preset named
    in `aws_preset` (or `send_to_aws: true`) may use AWS, but never more than `aws_max_sends_per_run` deliveries per run.
    The run can be in-process (dashboard/API) or an external `scripts/replay_dataset.py`, which announces itself through a
    heartbeat file. The server decides - a client cannot talk its way past this.
    """

    def __init__(self, cfg: ReplayCfg, clock: Callable[[], float] = time.time) -> None:
        self.cfg, self._clock = cfg, clock
        self.active = False
        self.source = ""                 # "api" | "external"
        self.run_id = ""
        self.preset = ""
        self.aws_allowed = False
        self.aws_sends = 0
        self.aws_suppressed = 0

    # ---- rules -------------------------------------------------------------------------------------
    def allowed_for(self, preset: str, requested: bool) -> bool:
        """Whether a run of `preset` that asked for AWS may use it."""
        return bool(self.cfg.send_to_aws or (requested and self.cfg.aws_preset and preset == self.cfg.aws_preset))

    # ---- lifecycle ---------------------------------------------------------------------------------
    def begin(self, run_id: str, preset: str, requested_aws: bool, source: str = "api") -> None:
        """Start a run: decide AWS eligibility and reset the per-run counters."""
        self.active, self.source, self.run_id, self.preset = True, source, run_id, preset
        self.aws_allowed = self.allowed_for(preset, requested_aws)
        self.aws_sends = self.aws_suppressed = 0
        log.info("replay run %s started (preset=%s, AWS %s, cap %d)", run_id, preset or "-",
                 "ALLOWED" if self.aws_allowed else "OFF", self.cfg.aws_max_sends_per_run)

    def end(self) -> None:
        """End the current run."""
        if self.active:
            log.info("replay run %s ended (AWS sends %d, suppressed %d)", self.run_id, self.aws_sends, self.aws_suppressed)
        self.active = False

    def poll_file(self) -> None:
        """Adopt / release an external replay from its heartbeat file. Cheap: one small read per tick."""
        if self.active and self.source == "api":
            return
        try:
            with open(self.cfg.state_file, encoding="utf-8") as fh:
                st = json.load(fh)
        except (OSError, ValueError):
            st = None
        fresh = bool(st and st.get("active") and self._clock() - float(st.get("updated_at", 0)) < self.cfg.stale_seconds)
        if fresh:
            if not self.active or self.run_id != st.get("run_id"):
                self.begin(str(st.get("run_id", "external")), str(st.get("preset", "")), bool(st.get("aws")), "external")
        elif self.active and self.source == "external":
            self.end()

    # ---- decisions ---------------------------------------------------------------------------------
    def permit(self, channel: str) -> bool:
        """May a delivery on `channel` be created? SNS/CloudWatch are refused during replays unless opted in."""
        if not self.active or channel not in AWS_CHANNELS:
            return True
        if not self.aws_allowed or self.aws_sends >= self.cfg.aws_max_sends_per_run:
            self.aws_suppressed += 1
            return False
        self.aws_sends += 1
        return True

    def status(self) -> dict:
        """Guard state for the dashboard."""
        return {"active": self.active, "source": self.source, "preset": self.preset, "aws_allowed": self.aws_allowed,
                "aws_sends": self.aws_sends, "aws_cap": self.cfg.aws_max_sends_per_run, "aws_suppressed": self.aws_suppressed}


# =================================================================================================
# In-process controller (demo API + dashboard)
# =================================================================================================
class ReplayError(Exception):
    """A request the controller rejects, carrying an HTTP status."""
    def __init__(self, message: str, status: int = 422) -> None:
        super().__init__(message)
        self.status = status


class ReplayController:
    """Start, stop and inspect an in-process replay (demo API and dashboard)."""
    def __init__(self, settings: Settings, guard: ReplayGuard, on_start: Callable[[], None] | None = None,
                 on_finish: Callable[[], None] | None = None, clock: Callable[[], float] = time.time) -> None:
        self.s, self.guard, self._clock = settings, guard, clock
        self._on_start, self._on_finish = on_start, on_finish
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._replayer: Replayer | None = None
        self._meta: dict = {}
        self.error: str | None = None
        self._runs = 0

    # ---- info --------------------------------------------------------------------------------------
    def presets(self) -> list[dict]:
        """The configured presets, with which one may use AWS."""
        out = []
        for name, p in (self.s.replay.presets or {}).items():
            out.append({"name": name, "description": p.get("description", ""), "start": str(p.get("start", "")),
                        "end": str(p.get("end", "")), "speed": p.get("speed", self.s.replay.speed),
                        "aws": name == self.s.replay.aws_preset})
        return out

    @property
    def running(self) -> bool:
        """Whether a replay task is active."""
        return self._task is not None and not self._task.done()

    def status(self) -> dict:
        """Replay progress plus the AWS-guard status."""
        st: dict = {"running": self.running, "error": self.error, "profile": self.s.profile_name,
                    "guard": self.guard.status(), **self._meta}
        if self._replayer is not None:
            pr = self._replayer.progress
            st.update(lines_sent=pr.lines_sent, loops=pr.loops, gaps_capped=pr.gaps_capped, progress=pr.progress,
                      orig_time=None if pr.orig_time is None else fmt_orig(pr.orig_time, self.s.replay.dataset_tz_minutes))
        return st

    # ---- control -----------------------------------------------------------------------------------
    async def start(self, preset: str | None = None, speed: float | None = None, aws: bool = False,
                    start: str | None = None, end: str | None = None, loop: bool = False,
                    seed: int | None = None) -> dict:
        """Validate the request, reset detection, and start replaying (stops any run in progress)."""
        r = self.s.replay
        if self.running:
            await self.stop()
        spec_src: dict = {}
        if preset:
            if preset not in (r.presets or {}):
                raise ReplayError(f"unknown preset {preset!r}; available: {sorted(r.presets or {})}")
            spec_src = dict(r.presets[preset])
        elif start is None:
            raise ReplayError("give a preset, or a start (and optionally end)")
        tz = r.dataset_tz_minutes
        start_txt = start if start is not None else spec_src.get("start")
        end_txt = end if end is not None else spec_src.get("end")
        try:
            spec = ReplaySpec(
                start_ts=parse_when(start_txt, tz) if start_txt else None,
                end_ts=parse_when(end_txt, tz) if end_txt else None,
                speed=float(speed or spec_src.get("speed") or r.speed),
                seed=seed if seed is not None else spec_src.get("seed", 1),
                loop=loop or bool(spec_src.get("loop", False)),
                max_gap_seconds=float(spec_src.get("max_gap_seconds", r.max_gap_seconds)),
                fmt=str(spec_src.get("format", "clf")),
            )
            path = resolve_dataset_path(r.dataset_path)
        except FileNotFoundError as e:
            raise ReplayError(str(e), status=409) from e
        except (ValueError, KeyError) as e:
            raise ReplayError(str(e)) from e
        if spec.speed <= 0:
            raise ReplayError("speed must be > 0")

        self._runs += 1
        run_id = f"replay-{int(self._clock())}-{self._runs}"
        self.guard.begin(run_id, preset or "", aws, "api")
        if self._on_start:
            self._on_start()                 # pause the synthetic generator, reset detection state
        self.error = None
        self._stop = asyncio.Event()
        self._replayer = Replayer(path, spec, ClfParser(self.s.mapping), self._clock, asyncio.sleep, r.chunk_seconds)
        self._meta = {"preset": preset or "custom", "speed": spec.speed, "run_id": run_id,
                      "start": fmt_orig(self._replayer._lo, tz), "end": fmt_orig(self._replayer._hi, tz),
                      "max_gap_seconds": spec.max_gap_seconds, "loop": spec.loop, "format": spec.fmt}
        self._task = asyncio.create_task(self._run(), name=run_id)
        return self.status()

    async def _run(self) -> None:
        try:
            await self._replayer.run(self.s.log_path, self._stop)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - a replay failure must never take the app down
            log.exception("replay failed")
            self.error = f"{type(e).__name__}: {e}"
        finally:
            self.guard.end()
            if self._on_finish:
                self._on_finish()

    async def stop(self) -> dict:
        """Stop the current replay, keeping what it already wrote."""
        if self._task is not None:
            self._stop.set()
            try:
                await asyncio.wait_for(self._task, 5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
        return self.status()


# =================================================================================================
# Simulation: the real detector on a virtual clock (tuning + tests; never touches sinks or AWS)
# =================================================================================================
class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


@dataclass
class SimAlert:
    """An alert found by simulate(), with its original-time bounds."""
    service: str
    orig_start: float | None
    orig_end: float | None = None
    peak: str = "MEDIUM"
    opened_as: str = "MEDIUM"
    rate: float = 0.0
    baseline: float = 0.0
    z: float = 0.0
    ratio: float = 0.0
    errors: int = 0
    total: int = 0
    virt_start: float = 0.0
    virt_end: float | None = None
    level_shift: bool = False


@dataclass
class SimResult:
    """Everything simulate() observed: alerts, counts and per-service volume."""
    alerts: list[SimAlert] = field(default_factory=list)
    lines: int = 0
    parse_errors: int = 0
    events: int = 0
    virtual_seconds: float = 0.0
    services: dict[str, int] = field(default_factory=dict)


def simulate(records: Iterable[Rec], settings: Settings, *, speed: float, max_gap: float, seed: int | None = 1,
             drain_seconds: float = 120.0) -> SimResult:
    """Feed records through parser -> detector -> alert state machine exactly as the app would during a replay
    (event time = the scheduled wall time), on a virtual clock. Returns every alert with its original-time span."""
    from app.detection.severity import rank

    parse = build_parser(settings)
    clock = _Clock()
    machine = AlertStateMachine(settings.detector, settings.profile)
    engine = DetectionEngine(settings, clock, frozen_fn=machine.is_open)
    res = SimResult()
    open_alerts: dict[str, SimAlert] = {}
    last_orig: float | None = None
    tick = settings.profile.tick_seconds

    def do_tick() -> None:
        for snap in engine.tick():
            t = machine.evaluate(snap)
            if t is None:
                continue
            a = t.alert
            if t.kind == "created":
                open_alerts[a.id] = SimAlert(a.service, last_orig, peak=a.severity, opened_as=a.severity,
                                             virt_start=clock.now)
            sa = open_alerts.get(a.id)
            if sa is None:
                continue
            if rank(a.peak_severity) >= rank(sa.peak):
                sa.peak = a.peak_severity
            if t.kind in {"created", "escalated"} or t.new_peak:
                sa.rate, sa.baseline, sa.z, sa.ratio = a.current_rate, a.baseline_rate, a.z, a.ratio
                sa.errors, sa.total = snap.errors, snap.total
            if t.kind == "resolved":
                sa.orig_end, sa.virt_end, sa.level_shift = last_orig, clock.now, t.level_shift
                res.alerts.append(open_alerts.pop(a.id))
                if t.level_shift:
                    engine.detector(a.service).reset_baseline()

    next_tick: float | None = None
    for v, rec in timeline(records, speed, max_gap, seed):
        res.lines += 1
        if next_tick is None:
            next_tick = v + tick
        while next_tick <= v:
            clock.now = next_tick
            do_tick()
            next_tick += tick
        clock.now = v
        if rec.ts is not None:
            last_orig = rec.ts
        try:
            ev = parse(rec.line)
        except ParseError:
            res.parse_errors += 1
            continue
        engine.on_event(replace(ev, ts=v))                 # the live app sees the rewritten (wall-clock) timestamp
        res.events += 1
        res.services[ev.service] = res.services.get(ev.service, 0) + 1
    if next_tick is not None:                               # let the window drain so trailing alerts can resolve
        end = clock.now + drain_seconds
        while next_tick <= end:
            clock.now = next_tick
            do_tick()
            next_tick += tick
    res.virtual_seconds = clock.now
    res.alerts.extend(open_alerts.values())                 # still open at the end
    res.alerts.sort(key=lambda a: a.virt_start)
    return res
