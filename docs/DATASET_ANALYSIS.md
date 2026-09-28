# NASA HTTP log (Jul 1995) — what the data is, and how the `nasa` profile was tuned

Everything below was **measured** by streaming the real file with `scripts/analyze_dataset.py` (never loading it whole) and by running
LogPulse's own detector over it (`--simulate`). Reproduce with:

```bash
python scripts/analyze_dataset.py --window-minutes 10 30            # census, spikes, threshold hints  (~16 s)
python scripts/analyze_dataset.py --simulate --profile nasa          # the real detector over the whole month (~75 s)
python scripts/analyze_dataset.py --simulate --profile nasa --start "1995-07-24 01:00" --end "1995-07-24 05:30"
```

**Honesty note.** The traffic is real. The *error definition* (4xx + 5xx), the *service mapping* (first URL segment) and every
*threshold* are choices made by this project to turn an access log into an error-rate detection problem. Nothing in the log says
"incident"; the spikes below are statistical events in the data, not labelled outages.

## 1. The file

| | |
|---|---|
| Source | NASA Kennedy Space Center WWW server, July 1995 (Internet Traffic Archive `NASA_access_log_Jul95`). Shipped in the repo as `data/datasets/NASA_access_log_Jul95.gz` (19.5 MB, lossless) and unpacked on first use; a raw `access_log_Jul95` / `NASA_access_log_Jul95` is accepted too |
| Size / lines | 205 MB, 1,891,715 lines, 1 Jul 00:00:01 → 28 Jul 13:32:25 (27.6 days), all in zone `-0400` |
| Average rate | 0.79 requests/s (per minute: p50 41, p90 91, p99 145, max 405; 162 zero-request minutes) |
| Status codes | 200: 1,701,534 · 304: 132,627 · 302: 46,573 · **404: 10,845** · 403: 54 · 400: 5 · **500: 62** · 501: 14 |
| Unusable lines | **1** (the truncated final line `alyssa.p`) — plus real oddities that *are* parseable: binary junk in the method position, URLs containing spaces, `-` byte counts, 8 lines with non-ASCII bytes |

**Consequence for the error definition.** Only 76 lines (0.004 %) are 5xx, so "5xx = error" leaves the detector nothing to
detect. Counting all 4xx + 5xx gives 10,980 errors = **0.58 %** of requests (mostly 404s). That is the `nasa` profile's
`detector.error_definition: "4xx+5xx"` — a chosen setting, documented as such.

## 2. Services (first URL path segment)

442 distinct prefixes; the top 8 cover 98 % of traffic and become services, the rest fold into `other`:

| service | events | share | 4xx+5xx errors | error rate |
|---|---:|---:|---:|---:|
| shuttle | 657,057 | 34.7 % | 2,664 | 0.41 % |
| images | 611,010 | 32.3 % | 417 | 0.07 % |
| history | 284,679 | 15.0 % | 3,425 | **1.20 %** |
| icons | 64,088 | 3.4 % | 113 | 0.18 % |
| htbin | 46,744 | 2.5 % | 24 | 0.05 % |
| ksc.html | 40,315 | 2.1 % | 49 | 0.12 % |
| cgi-bin | 38,091 | 2.0 % | 71 | 0.19 % |
| `/` (→ `root`) | 33,095 | 1.7 % | 227 | 0.69 % |
| *(other 434 prefixes → `other`)* | 116,635 | 6.2 % | — | noisy |

## 3. What "normal" looks like (30-minute windows)

| series | median | robust σ | p90 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| all traffic | 0.47 % | 0.33 % | 1.16 % | 2.53 % | 3.89 % |
| shuttle | 0.26 % | 0.38 % | 0.97 % | 2.94 % | 6.48 % |
| images | 0.00 % | 0.00 % | 0.22 % | 1.02 % | 2.42 % |
| **history** | 0.42 % | 0.63 % | 2.86 % | **10.45 %** | **19.48 %** |

Calm windows sit at 0–0.5 %, but the services are **very unequally noisy**: `history` (and the catch-all `other`) routinely
produce 5–20 % windows on a handful of requests (crawlers hitting missing pages), while `shuttle`/`images` almost never exceed 3–6 %.
Per-minute over all traffic: p50 0.00 %, p90 2.17 %, p99 7.89 %, p99.9 18.6 %, max 70.8 %. Diurnal load: ~2 % of traffic per
3 h at night vs ~6.5 % at midday. Longest silences: 22 min (13 Jul 19:49), 8 min (21 Jul), 7 min (24 Jul).

## 4. Real events in the data

**Error-rate spikes** (top by robust z against the previous ~48 windows; ET):

| window | series | events | errors | rate | local baseline |
|---|---|---:|---:|---:|---:|
| 24 Jul 03:00–03:30 | all / **history** | 446 / 113 | 29 / 28 | 6.5 % / **24.8 %** | 0.44 % / 0.00 % |
| 23 Jul 03:20–03:50 | all / history | 398 / 136 | 25 / 22 | 6.3 % / 16.2 % | 0.46 % |
| 2 Jul 08:24–08:54 | history | 67 | 15 | 22.4 % | 0.00 % |
| 25 Jul 11:18–11:48 | history | 251 | 39 | 15.5 % | 0.00 % |
| 11 Jul 01:48–02:18 | all | 689 | 37 | 5.4 % | 0.50 % |
| 19 Jul 11:21–13:21 | shuttle (sustained 2 h) | 643 / 702 | 30 / 36 | 4.7 % / 5.1 % | 0.2 % |
| 3 Jul ~10:54 | cgi-bin | 118 | 53 | **44.9 %** | 0.0 % |
| 12 Jul ~10:26 | icons | 134 | 64 | **47.8 %** | 0.0 % |

**Volume-only surge** (the false-alarm test): **13 Jul 08:30–10:20** — up to 3,043 requests in 10 minutes (5.7× the local median,
3.6× over 30 minutes) with an error rate of 0.16–0.41 % — *unchanged* from the surrounding 0.2–0.4 %.

## 5. Tuning the `nasa` profile

**Time scale.** Replay at speed 180 (`replay.speed`): 1 wall second = 3 original minutes. Detector window 10 s = **30 original minutes**;
tick 1 s = 3 min; baseline sample every 2 s = 6 min; 30 samples = 3 h of history; warm-up (10 samples) = 20 s = 1 h;
`confirm_ticks` 2 = 6 min; `resolve_ticks` 3 = 9 min; level-shift timeout 120 s = 6 h. Per-service 30-minute windows hold ~30–500
requests, so `min_events: 40` keeps tiny windows from meaning anything.

**Why the defaults do not work here.** The default gates (medium ≥ 10 % / high ≥ 15 % / critical ≥ 25 %, z ≥ 2/3/4) were the
starting point, but the first `nasa` attempt used *lower* floors (3 % / 6 % / 12 %, err ≥ 8) because the overall baseline is only
0.4 %. Running the real detector over all 27 days gave **137 alerts** — mostly `history` and `other` noise. Two effects:

1. **The baseline σ is tiny by design.** Anomalous windows are never admitted to the baseline (contamination guard), so σ measures only
   calm periods (≈ 0–0.5 %). z-scores are therefore inflated (a routine 8 % `history` burst scores z ≈ 8–12), and z cannot separate
   a routine burst from an incident. **The absolute floors have to carry the discrimination.**
2. **The noisiest coherent service sets the floor.** `history`'s p99 is 10.5 % and p99.9 is 19.5 %, so any global floor below ~10 %
   fires on routine behaviour.

**Sweep** (each row = the real detector over the whole month; `min_events` 30–60; parallel runs, seed 1):

| variant (medium / high / critical rate floor) | min_events | alerts / 27 days | note |
|---|---:|---:|---|
| 3 % / 6 % / 12 % (first attempt) | 30 | 137 | routine `history` + `other` bursts |
| 8 % / 12 % / 20 % | 60 | 44 | still noise |
| 10 % / 15 % / 25 % (the defaults) | 30 / 50 / 60 | 32 / 29 / 29 | 15–18 of them `other` |
| 11 % / 18 % / 30 % | 40 | 14 | picks up 11–12 % routine bursts |
| 12 % / 18 % / 28 % | 40 | 11 | |
| 15 % / 22 % / 32 % | 40 | 9 | starts missing real spikes (23 Jul, 25 Jul) |
| **12 % / 20 % / 30 %** (chosen) | **40** | **11** | identical alert set at `min_events` 30 and 40 → robust |
| same, `std_floor` 0.02, z 3/5/8 | 40 | 25 | z no longer discriminates |

**Chosen thresholds** (`config.yaml → profiles.nasa`):

| setting | value | derived from |
|---|---|---|
| medium / high / critical **rate** floor | **12 % / 20 % / 30 %** | just above `history` p99 (10.5 %) / at its p99.9 (19.5 %) / above any routine burst |
| ratio floors | 24× / 40× / 60× | = the rate floors ÷ 0.5 % (`ratio_floor`: the baseline is < 0.5 %), so the OR-gate cannot bypass the floor |
| min errors | 12 / 16 / 24 | a 12 % window of 40 events is ~5 errors: require real volume behind the rate |
| z | 5 / 8 / 12 | far below the z of genuine spikes (9–48) — a sanity gate, not the discriminator |
| `std_floor` | 0.01 | calm windows are 0–0.5 %; σ must be floored at 1 pp |
| `warmup_ceiling` | 0.05 | p99.9 of all-traffic windows is 3.9 %: above 5 % it is a spike, not baseline |
| `resolve_ratio` | 3.0 | with a 0.3 % baseline, "1.5× baseline" (0.45 %) is unreachable in a 40-request window |
| `min_events` | 40 | 30-minute per-service windows |

**Result over the whole month: 11 alerts**, all real bursts (peak rate 15–48 % against baselines of 0–8 %):

| opened (ET) | service | peak | rate (errors/events) |
|---|---|---|---|
| 2 Jul 08:56 | history | MEDIUM | 22.4 % (15/67) |
| 3 Jul 10:54 | cgi-bin | **CRITICAL** | 44.9 % (53/118) |
| 3 Jul 17:33 | other | MEDIUM | 18.1 % (15/83) |
| 6 Jul 02:00 | other | MEDIUM | 32.3 % (21/65) |
| 11 Jul 14:12 | icons | HIGH | 20.3 % (16/79) |
| 12 Jul 10:26 | icons | **CRITICAL** | 47.8 % (64/134) |
| 20 Jul 18:14 | other | MEDIUM | 31.7 % (13/41) |
| 21 Jul 03:44 | history | MEDIUM | 23.7 % (14/59) |
| 23 Jul 03:23 | history | MEDIUM | 15.2 % (22/145) |
| 24 Jul 03:12 | history | **HIGH** | 23.9 % (28/117) |
| 25 Jul 11:41 | history | MEDIUM | 15.5 % (39/251) |

**Known limits (be honest about them).** (a) Global floors cannot be right for every service: moderate spikes in the well-behaved
services — `shuttle` at 5–8 % against a 0.2 % baseline (19 Jul 11:21–13:21, 23 Jul 04:24) — stay under the 12 % floor and are *not*
alerted; per-service floors would fix that and are not built. (b) `other` is a heterogeneous bucket, not a service; three of the
11 alerts are on it. (c) The ratio gate is deliberately neutralised by `ratio_floor`. (d) These are choices tuned on this one month of
this one site; they are not a claim of general accuracy.

## 6. Presets (real segments, reproducible with `--seed`)

Each preset starts ≥ 1.5 original hours before the event (baseline warm-up) and ends ≥ 1 h after (so alerts can resolve).

| preset | segment (ET) | expected | simulated with the real detector |
|---|---|---|---|
| `spike-history-jul24` | 24 Jul 01:00–05:30 | history HIGH | history 03:12→03:47 MEDIUM→**HIGH**, 23.9 %, z 19.5 |
| `spike-icons-jul12` | 12 Jul 08:00–13:00 | icons CRITICAL | icons 10:26→11:12 **CRITICAL**, 47.8 % |
| `spike-cgibin-jul03` | 3 Jul 08:30–13:00 | cgi-bin CRITICAL | cgi-bin 10:53→11:32 HIGH→**CRITICAL**, 44.9 % (the live run also raised a borderline history MEDIUM, see below) |
| `volume-surge-jul13` | 13 Jul 06:30–11:00 | **no alert** | 0 alerts (5–6× volume, unchanged error rate) |
| `normal-jul16` | 16 Jul 08:00–14:00 | **no alert** | 0 alerts |

`spike-history-jul24` is the single preset allowed to opt in to AWS (`replay.aws_preset`).

### Live end-to-end results (app on the `nasa` profile, AWS **off**, ntfy to a local stand-in)

Each preset was replayed into the running app at speed 180 (one via `scripts/replay_dataset.py`, the rest via `POST /api/demo/replay`); alert times
are mapped back to the original log time through the `orig_ts` left in the replayed lines. 108,304 events, 0 parse errors, 0 dropped, 0 late, health HEALTHY.

| preset | lines | alerts raised (original time, ET) | verdict |
|---|---:|---|---|
| `spike-icons-jul12` (external script) | 25,924 | **icons** 10:27 → 11:12, HIGH → **CRITICAL** | as expected |
| `spike-history-jul24` | 4,757 | **history** 03:12 → 03:48, MEDIUM → **HIGH** | as expected |
| `spike-cgibin-jul03` | 21,673 | **cgi-bin** 10:55 → 11:31, HIGH → **CRITICAL**; plus **history** 09:40 → 10:10, MEDIUM | expected alert fired; one extra (below) |
| `volume-surge-jul13` | 45,013 | **none** | as expected: a real 5-6x volume surge did not alert |
| `normal-jul16` | 10,937 | **none** | as expected |

*The extra alert.* The 3 Jul preset also raised a MEDIUM on `history` at 09:40 ET (12.3 % errors, 33 of 268 requests): a real burst, but only just above
the 12 % medium floor. The whole-month simulation did not flag it, because a preset starts from a fresh detector at 08:30 (a shorter, different baseline)
and the live run's tick phase differs slightly from the simulation's. Borderline events like this are exactly where a single global floor is
fragile; raising the medium floor to ~13 % would suppress it, and by peak rates (the month's lowest is 15.2 %) would probably keep the other 11-alert-list events - but that was **not re-run**, so it is a suggestion, not a verified change.
It was not tuned away to fit the presets.

Nine notifications reached the ntfy stand-in (created / escalated / resolved; CRITICAL sent with `urgent` priority) and the console + JSONL sinks
(9 delivered, 0 failed each). SNS and CloudWatch stayed disabled; the run's guard reported `aws_allowed: false` throughout.
