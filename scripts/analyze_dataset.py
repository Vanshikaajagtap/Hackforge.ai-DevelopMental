"""Stream the NASA HTTP access log (Common Log Format) and measure it - never loading the whole file.

  python scripts/analyze_dataset.py                           # census + spikes + threshold hints (4xx+5xx errors)
  python scripts/analyze_dataset.py --error-definition 5xx    # same, counting only 5xx
  python scripts/analyze_dataset.py --window-minutes 5 15     # window sizes (original minutes) for the spike search
  python scripts/analyze_dataset.py --simulate --profile nasa # run the REAL detector over the data and list its alerts
  python scripts/analyze_dataset.py --simulate --start "1995-07-13 08:00" --end "1995-07-13 13:00"

Reports: per-minute and per-service error rates, distribution stats, the top windows by error-rate spike and by volume
spike, the biggest silences, and robust threshold suggestions. The traffic is real; the error definition and any
thresholds derived here are chosen settings (see docs/DECISIONS.md). Results are also saved as JSON (--out, git-ignored).
"""
from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import error_min_status, load_settings  # noqa: E402
from app.ingestion.clf import parse_clf_timestamp, path_prefix, split_request  # noqa: E402
from app.replay import (  # noqa: E402
    dataset_bounds, fmt_orig, iter_segment, parse_when, resolve_dataset_path, simulate)

LINE = re.compile(r'^\S+ \S+ \S+ \[(\d\d/[A-Za-z]{3}/\d{4}:\d\d:\d\d:\d\d [+-]\d{4})\] "(.*)" (\d{3})')
TZ = -240


def et(epoch_minute: int | float) -> str:
    """Minute index (or epoch seconds) -> 'Jul 13 08:15' in the log's own zone."""
    secs = epoch_minute * 60 if epoch_minute < 10**8 else epoch_minute
    s = fmt_orig(secs, TZ)                                # 13/Jul/1995:08:15:00 -0400
    return f"{s[3:6]} {s[:2]} {s[12:17]}"


def pct(sorted_vals: list[float], p: float) -> float:
    """Value at percentile `p` of an ascending list."""
    if not sorted_vals:
        return float("nan")
    return sorted_vals[min(len(sorted_vals) - 1, int(p * len(sorted_vals)))]


# ------------------------------------------------------------------------------------------------------------------
def scan(path: Path, min_status: int, start_ts: float | None, end_ts: float | None) -> dict:
    """One streaming pass. Per (minute, prefix) counts of [total, errors]; never stores lines."""
    pm: dict[tuple[int, str], list[int]] = defaultdict(lambda: [0, 0])
    status_hist: Counter = Counter()
    bad_samples: list[str] = []
    bad = lines = 0
    last_ts_str, last_ts = "", 0.0
    seg = iter_segment(path, start_ts, end_ts)
    for rec in seg:
        lines += 1
        m = LINE.match(rec.line)
        if not m:
            bad += 1
            if len(bad_samples) < 5:
                bad_samples.append(rec.line[:100])
            continue
        if m[1] != last_ts_str:
            last_ts_str, last_ts = m[1], parse_clf_timestamp(m[1])
        status = int(m[3])
        _, url, _ = split_request(m[2])
        prefix = path_prefix(url)
        label = "(junk)" if prefix is None else ("/" if prefix == "" else prefix)
        cell = pm[(int(last_ts // 60), label)]
        cell[0] += 1
        if min_status <= status <= 599:
            cell[1] += 1
        status_hist[status] += 1
    return {"pm": pm, "status": status_hist, "bad": bad, "lines": lines, "bad_samples": bad_samples}


def series(pm: dict, m0: int, m1: int, label: str | None) -> tuple[list[int], list[int]]:
    """Per-minute totals and error counts for one prefix (or all traffic)."""
    n = m1 - m0 + 1
    tot, err = [0] * n, [0] * n
    for (minute, lab), (t, e) in pm.items():
        if label is None or lab == label:
            tot[minute - m0] += t
            err[minute - m0] += e
    return tot, err


def prefix_sums(a: list[int]) -> list[int]:
    """Running sums with a leading zero, for O(1) window sums."""
    out = [0]
    for x in a:
        out.append(out[-1] + x)
    return out


def robust(vals: list[float]) -> tuple[float, float]:
    """(median, robust sigma = 1.4826 * MAD)."""
    if not vals:
        return 0.0, 0.0
    med = statistics.median(vals)
    return med, 1.4826 * statistics.median(abs(v - med) for v in vals)


def window_table(tot: list[int], err: list[int], w: int, min_events: int, history_windows: int = 48) -> list[dict]:
    """Sliding windows of w minutes (step 1 min) with rate, volume and a robust z against the trailing windows
    (sampled every w minutes over the previous `history_windows` windows - a local 'what was normal just before')."""
    ct, ce = prefix_sums(tot), prefix_sums(err)
    n = len(tot)
    rows = []
    for i in range(w, n + 1):
        t, e = ct[i] - ct[i - w], ce[i] - ce[i - w]
        rows.append({"i": i - w, "tot": t, "err": e, "rate": e / t if t else 0.0})
    out = []
    for k, r in enumerate(rows):
        hist = [rows[k - w * j] for j in range(1, history_windows + 1) if k - w * j >= 0]
        good = [h for h in hist if h["tot"] >= min_events]
        if len(good) < 8 or r["tot"] < min_events:
            continue
        med_r, sig_r = robust([h["rate"] for h in good])
        med_v, _ = robust([float(h["tot"]) for h in good])
        r = dict(r)
        r["base_rate"], r["sig_rate"] = med_r, sig_r
        r["z"] = (r["rate"] - med_r) / max(sig_r, 0.002)
        r["ratio"] = r["rate"] / max(med_r, 0.001)
        r["base_tot"] = med_v
        r["vol_ratio"] = r["tot"] / max(med_v, 1.0)
        out.append(r)
    return out


def top_non_overlapping(rows: list[dict], key: str, w: int, k: int, where=lambda r: True) -> list[dict]:
    """The best `k` windows by `key`, keeping picks at least two windows apart."""
    picked: list[dict] = []
    for r in sorted((x for x in rows if where(x)), key=lambda x: -x[key]):
        if all(abs(r["i"] - p["i"]) >= w * 2 for p in picked):
            picked.append(r)
        if len(picked) >= k:
            break
    return sorted(picked, key=lambda x: x["i"])


def fmt_window(m0: int, r: dict, w: int) -> str:
    """One report line describing a window."""
    return (f"{et(m0 + r['i'])} -> {et(m0 + r['i'] + w)}  events={r['tot']:>6,}  errors={r['err']:>5,}  "
            f"rate={r['rate']*100:5.2f}%  (local baseline {r['base_rate']*100:4.2f}%, z={r['z']:.1f}, "
            f"x{r['ratio']:.1f} rate, x{r['vol_ratio']:.1f} volume)")


# ------------------------------------------------------------------------------------------------------------------
def analyze(args, path: Path, min_status: int) -> dict:
    """Census, service table, distributions, silences, spikes and threshold hints; returns the JSON payload."""
    lo, hi = dataset_bounds(path)
    start = parse_when(args.start, TZ) if args.start else None
    end = parse_when(args.end, TZ) if args.end else None
    print(f"dataset: {path}  ({path.stat().st_size / 1e6:,.1f} MB)  {fmt_orig(lo, TZ)} .. {fmt_orig(hi, TZ)}")
    print(f"error definition: {args.error_definition}  (status >= {min_status})")
    res = scan(path, min_status, start, end)
    pm = res["pm"]
    m0 = min(k[0] for k in pm)
    m1 = max(k[0] for k in pm)
    minutes = m1 - m0 + 1
    tot, err = series(pm, m0, m1, None)
    lines, bad = res["lines"], res["bad"]
    out: dict = {"path": str(path), "error_definition": args.error_definition, "lines": lines, "parse_errors": bad}

    print(f"\n== census ==\nlines={lines:,}  unparseable={bad:,} {res['bad_samples']!r}  minutes={minutes:,} "
          f"({minutes / 1440:.1f} days)  events/s avg={sum(tot) / (minutes * 60):.2f}")
    print("status codes:", dict(sorted(res["status"].items())))
    total_err = sum(err)
    print(f"errors ({args.error_definition}): {total_err:,} = {total_err / max(sum(tot), 1) * 100:.3f}% of all requests")

    # ---- services (first URL segment) --------------------------------------------------------------
    by_prefix: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for (_, lab), (t, e) in pm.items():
        by_prefix[lab][0] += t
        by_prefix[lab][1] += e
    ranked = sorted(by_prefix.items(), key=lambda kv: -kv[1][0])
    print(f"\n== top URL prefixes (candidate services; {len(by_prefix):,} distinct) ==")
    print(f"{'prefix':<22}{'events':>11}{'share':>8}{'errors':>9}{'err rate':>10}")
    cum = 0
    prefixes = []
    for lab, (t, e) in ranked[: args.top_prefixes]:
        cum += t
        prefixes.append({"prefix": lab, "events": t, "errors": e, "rate": e / t if t else 0.0})
        print(f"{lab:<22}{t:>11,}{t / sum(tot) * 100:>7.1f}%{e:>9,}{e / t * 100:>9.2f}%")
    print(f"top {args.top_prefixes} cover {cum / sum(tot) * 100:.1f}% of traffic; the rest fold into 'other'")
    out["prefixes"] = prefixes

    # ---- per-minute distribution -------------------------------------------------------------------
    mins = [(t, e) for t, e in zip(tot, err) if t >= 20]
    rates = sorted(e / t for t, e in mins)
    vols = sorted(t for t in tot)
    print(f"\n== per-minute distribution (minutes with >= 20 events: {len(mins):,}) ==")
    print("error rate  p50={:.2f}%  p90={:.2f}%  p99={:.2f}%  p99.9={:.2f}%  max={:.1f}%".format(
        *(pct(rates, p) * 100 for p in (0.5, 0.9, 0.99, 0.999)), rates[-1] * 100))
    print("events/min  p50={:,.0f}  p90={:,.0f}  p99={:,.0f}  max={:,.0f}   (zero-event minutes: {:,})".format(
        pct(vols, 0.5), pct(vols, 0.9), pct(vols, 0.99), vols[-1], sum(1 for t in tot if t == 0)))
    hour_tot, hour_err = [0] * 24, [0] * 24
    for i, (t, e) in enumerate(zip(tot, err)):
        hh = int(((m0 + i) * 60 + TZ * 60) // 3600 % 24)
        hour_tot[hh] += t
        hour_err[hh] += e
    print("hour-of-day (ET) volume share %:", " ".join(f"{h:02d}:{hour_tot[h] / sum(hour_tot) * 100:3.1f}" for h in range(0, 24, 3)))
    out["per_minute"] = {"err_rate_percentiles": {str(p): pct(rates, p) for p in (0.5, 0.9, 0.99, 0.999)},
                         "events_percentiles": {str(p): pct(vols, p) for p in (0.5, 0.9, 0.99)}}

    # ---- silences ----------------------------------------------------------------------------------
    gaps, run = [], 0
    for i, t in enumerate(tot):
        if t == 0:
            run += 1
        elif run:
            gaps.append((run, i - run))
            run = 0
    gaps.sort(reverse=True)
    print("\n== longest silences (zero-event minutes in a row) ==")
    for length, at in gaps[:5]:
        print(f"  {length:>5} min  starting {et(m0 + at)}")
    out["silences"] = [{"minutes": g, "start": et(m0 + a)} for g, a in gaps[:5]]

    # ---- spikes -------------------------------------------------------------------------------------
    out["spikes"] = {}
    wanted = [(None, "ALL TRAFFIC")] + [(p["prefix"], f"service '{p['prefix']}'") for p in prefixes[: args.spike_services]]
    for w in args.window_minutes:
        for label, title in wanted:
            t_s, e_s = (tot, err) if label is None else series(pm, m0, m1, label)
            min_events = max(30, int(w * 3)) if label is None else max(20, int(w * 1.5))
            rows = window_table(t_s, e_s, w, min_events)
            if not rows:
                continue
            print(f"\n== {title}: {w}-minute windows ==")
            err_top = top_non_overlapping(rows, "z", w, args.top, lambda r: r["err"] >= 8 and r["rate"] > r["base_rate"])
            vol_top = top_non_overlapping(rows, "vol_ratio", w, args.top)
            print("  top error-rate spikes (by robust z vs the previous ~%d windows):" % 48)
            for r in sorted(err_top, key=lambda x: -x["z"]):
                print("   ", fmt_window(m0, r, w))
            print("  top volume spikes (events vs local median):")
            for r in sorted(vol_top, key=lambda x: -x["vol_ratio"]):
                print("   ", fmt_window(m0, r, w))
            out["spikes"][f"{title}|{w}m"] = {
                "error": [dict(r, start=et(m0 + r["i"]), end=et(m0 + r["i"] + w)) for r in err_top],
                "volume": [dict(r, start=et(m0 + r["i"]), end=et(m0 + r["i"] + w)) for r in vol_top]}
        # threshold hints from the OVERALL series at this window
    # ---- threshold hints ---------------------------------------------------------------------------
    print("\n== threshold hints (robust: from the bulk of the data, not its extremes) ==")
    hints = {}
    for label in [None] + [p["prefix"] for p in prefixes[: args.spike_services]]:
        t_s, e_s = (tot, err) if label is None else series(pm, m0, m1, label)
        for w in args.window_minutes:
            ct, ce = prefix_sums(t_s), prefix_sums(e_s)
            step = max(1, w)
            rr = [((ce[i] - ce[i - w]) / (ct[i] - ct[i - w])) for i in range(w, len(t_s) + 1, step)
                  if ct[i] - ct[i - w] >= max(20, int(w * 1.5))]
            if len(rr) < 20:
                continue
            med, sig = robust(rr)
            srt = sorted(rr)
            hints[f"{label or 'ALL'}|{w}m"] = {"median": med, "robust_sigma": sig, "p50": pct(srt, .5), "p90": pct(srt, .9),
                                               "p99": pct(srt, .99), "p999": pct(srt, .999), "windows": len(rr)}
            print(f"  {label or 'ALL':<12}{w:>3}m  n={len(rr):>5}  median={med * 100:5.2f}%  robust sigma={sig * 100:5.2f}%  "
                  f"p90={pct(srt, .9) * 100:5.2f}%  p99={pct(srt, .99) * 100:5.2f}%  p99.9={pct(srt, .999) * 100:5.2f}%")
    out["threshold_hints"] = hints
    return out


# ------------------------------------------------------------------------------------------------------------------
def run_simulation(args, path: Path) -> dict:
    """Run the real detector over the (segment of the) dataset and list its alerts."""
    settings = load_settings(args.config, env={"LOGPULSE_PROFILE": args.profile})
    r = settings.replay
    speed = args.speed or r.speed
    max_gap = args.max_gap if args.max_gap is not None else r.max_gap_seconds
    start = parse_when(args.start, TZ) if args.start else None
    end = parse_when(args.end, TZ) if args.end else None
    prof = settings.profile
    print(f"simulating the real detector: profile={args.profile}  window={prof.window_seconds:g}s wall = "
          f"{prof.window_seconds * speed / 60:.1f} original min at speed {speed:g}  min_events={prof.min_events}  "
          f"error_definition={settings.detector.error_definition}  format={settings.ingestion.format}")
    sm = settings.severity
    print(f"severity gates: medium z>={sm.medium.z} rate>={sm.medium.rate} ratio>={sm.medium.ratio} err>={sm.medium.errors} | "
          f"high z>={sm.high.z} rate>={sm.high.rate} ratio>={sm.high.ratio} | critical z>={sm.critical.z} rate>={sm.critical.rate} ratio>={sm.critical.ratio} err>={sm.critical.errors}")
    res = simulate(iter_segment(path, start, end), settings, speed=speed, max_gap=max_gap, seed=args.seed)
    print(f"lines={res.lines:,} events={res.events:,} parse_errors={res.parse_errors:,} virtual duration={res.virtual_seconds / 60:.1f} wall-min")
    print(f"services seen: {dict(sorted(res.services.items(), key=lambda kv: -kv[1]))}")
    print(f"\n{len(res.alerts)} alert(s):")
    print(f"{'service':<14}{'opened (ET)':<14}{'closed (ET)':<14}{'orig dur':>9}  {'open->peak':<18}{'rate':>7}{'base':>7}{'z':>7}{'err/n':>11}")
    rows = []
    for a in res.alerts:
        dur = (a.orig_end - a.orig_start) / 60 if a.orig_end and a.orig_start else float("nan")
        print(f"{a.service:<14}{et(a.orig_start or 0):<14}{(et(a.orig_end) if a.orig_end else '(open)'):<14}{dur:>7.0f}m  "
              f"{a.opened_as + '->' + a.peak:<18}{a.rate * 100:>6.1f}%{a.baseline * 100:>6.1f}%{a.z:>7.1f}{a.errors:>6}/{a.total:<5}"
              + ("  [level-shift]" if a.level_shift else ""))
        rows.append({"service": a.service, "opened": et(a.orig_start or 0), "closed": et(a.orig_end) if a.orig_end else None,
                     "opened_as": a.opened_as, "peak": a.peak, "rate": a.rate, "baseline": a.baseline, "z": a.z,
                     "errors": a.errors, "total": a.total, "level_shift": a.level_shift,
                     "orig_start": a.orig_start, "orig_end": a.orig_end})
    return {"simulation": {"profile": args.profile, "speed": speed, "alerts": rows, "lines": res.lines}}


def main() -> None:
    """CLI entry point."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--path", help="dataset file (default: replay.dataset_path, falling back to access_log_Jul95)")
    ap.add_argument("--error-definition", default="4xx+5xx", help="5xx | 4xx+5xx | a minimum status such as 404")
    ap.add_argument("--window-minutes", type=int, nargs="+", default=[10, 30], help="original-minute window sizes for the spike search")
    ap.add_argument("--top", type=int, default=5, help="windows to list per ranking")
    ap.add_argument("--top-prefixes", type=int, default=12)
    ap.add_argument("--spike-services", type=int, default=3, help="also search spikes per service for the top N prefixes")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--out", default="data/analysis_nasa.json")
    ap.add_argument("--simulate", action="store_true", help="run the real detector + alert state machine over the data")
    ap.add_argument("--profile", default="nasa")
    ap.add_argument("--config", help="alternative config.yaml (for threshold experiments)")
    ap.add_argument("--speed", type=float)
    ap.add_argument("--max-gap", type=float)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()

    base = load_settings(env={})
    path = resolve_dataset_path(args.path or base.replay.dataset_path)
    result = run_simulation(args, path) if args.simulate else analyze(args, path, error_min_status(args.error_definition))
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(result, indent=1, default=str), encoding="utf-8")
        print(f"\n(saved {args.out})")


if __name__ == "__main__":
    main()
