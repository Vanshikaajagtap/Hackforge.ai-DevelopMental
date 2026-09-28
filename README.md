# LogPulse

**Real-time, explainable error-rate anomaly detection for a growing log file**, with alert-fatigue control, a live WebSocket dashboard, and alert delivery to
**AWS SNS (email)** and **AWS CloudWatch Logs (structured evidence trail)** — plus ntfy / Telegram / a JSONL file as redundant channels, all behind one
pluggable `AlertSink` interface. Detection never depends on any of them.

It tails an append-only log → keeps a per-service sliding window → learns a **baseline an incident cannot contaminate** → flags deviations only when a
z-score **and** a ratio **and** absolute floors agree → assigns MEDIUM / HIGH / CRITICAL → runs an alert lifecycle (confirm, escalate, hysteresis, dedup) →
persists to SQLite **first**, then delivers with retries. Every alert states *what / where / when / current / normal / how much / z / sample size / severity /
where it was delivered*. It runs on synthetic demo traffic **and** on the real NASA HTTP access log (July 1995).

Statistical and deterministic by design — no ML. The detection algorithm, alert lifecycle and architecture were written for this project (see [ACKNOWLEDGEMENTS.md](ACKNOWLEDGEMENTS.md)).

**Contents:** [Architecture](#architecture) · [Quickstart](#quickstart-3-commands) · [Requirements traceability](#requirements-traceability) · [Demo](#the-demo) ·
[How detection works](#how-detection-works) · [Alert delivery](#alert-delivery) · [Configuration reference](#configuration-reference) · [API](#api) ·
[NASA dataset](#real-data-replaying-the-nasa-http-log) · [AWS setup](#aws-live-sns--cloudwatch) · [Testing](#testing) · [Reliability](#reliability-and-the-test-that-proves-it) ·
[Troubleshooting](#troubleshooting) · [Limitations](#known-limitations) · [Layout](#project-layout)

## Architecture

```
 generator / your app / replayer ──append──▶  app.log
                                                │
                                      ┌─────────▼──────────┐
                                      │ Tailer             │  byte offset, partial-line buffer,
                                      │ ingestion/tailer   │  (inode, offset) checkpoint, rotation
                                      └─────────┬──────────┘
                                                │ raw lines
                                      ┌─────────▼──────────┐
                                      │ Parser  ndjson|clf │──▶ parse_errors++ (counted, never fatal)
                                      └─────────┬──────────┘
                                                │ LogEvent   bounded queue, drop-oldest
                              ┌─────────────────▼──────────────────┐
                              │ DetectionEngine (per service)      │  window → baseline → gates → severity
                              │ detection/detector.py              │  emits one Snapshot per tick
                              └───────┬─────────────┬──────────────┘
                          Snapshot    │             │ Snapshot
                                      │             ▼
                                      │   AlertStateMachine  (detection/state.py)
                                      │   confirm · escalate · hysteresis · dedup · level shift
                                      ▼             │ Transition
                                WebSocket hub       ▼
                                      │       AlertManager ── 1. persist alert + PENDING deliveries (SQLite)
                                      ▼             │         2. enqueue
                                  Dashboard         ▼
                                            Dispatcher (retry · backoff · timeout · external id)
                                                    ├──▶ AWS SNS (email)        ┐
                                                    ├──▶ AWS CloudWatch Logs    ├ redundant, independent:
                                                    ├──▶ ntfy · Telegram · JSONL┘ one failing never blocks another
                                                    └──▶ console
```

A modular monolith: one Python process, one `asyncio` loop; `ingestion/ detection/ alerts/ storage/ api/` each own one responsibility. Detection never
imports sinks and sinks never block detection. Details: [docs/architecture.md](docs/architecture.md), rationale: [docs/DECISIONS.md](docs/DECISIONS.md), full spec: [docs/PRD.md](docs/PRD.md).

## Quickstart (3 commands)

**With Docker** (nothing else to install):

```bash
cp .env.example .env            # Windows: copy .env.example .env   (optional: set NTFY_TOPIC to a long random string)
docker compose up --build
# open http://localhost:8000  ->  click "Mixed (2-min story)" or "Error spike"
```

**Without Docker** (Python 3.12+; a virtualenv is recommended: `python -m venv .venv` and activate it):

```bash
pip install -r requirements.txt
cp .env.example .env            # Windows: copy .env.example .env
python -m uvicorn app.main:app --port 8000
# open http://localhost:8000
```

Sinks whose settings are missing or still placeholders are skipped and shown as `disabled` on the health panel: the dashboard, SQLite and `alerts.jsonl` need no network.
Everything else (AWS, ntfy, Telegram, the NASA replay) is opt-in below.

## Requirements traceability

The problem statement's minimum requirements, where each lives, how to watch it work, and which tests cover it.
**`python scripts/verify_requirements.py`** runs all eight against the real code (offline, ~3 s) and prints PASS/FAIL with evidence; `docs/TEST_REPORT.md` records the last run.

| # | Requirement | Implemented in | See it working | Tests |
|---|---|---|---|---|
| 1 | Monitor a continuously growing log file | [`app/ingestion/tailer.py`](app/ingestion/tailer.py) `Tailer.read_available`, `Tailer.run` (byte offset, partial-line buffer, checkpoint resume, rotation); wired by [`app/main.py`](app/main.py) `Runtime.ingest_line` | dashboard → *Mixed* (a generator appends to the real file; events/s moves); `verify_requirements.py` R1 | `test_parser_tailer` (partial writes, no re-read, checkpoint, truncation, rotation), `test_robustness`, `test_e2e` |
| 2 | Rolling error rates over a sliding window | [`app/detection/window.py`](app/detection/window.py) `SlidingWindow.add / evict / error_rate`; used per service by `ServiceDetector.tick` | *Error rate* KPI and chart; R2 | `test_window` |
| 3 | Establish a baseline for normal behaviour | [`app/detection/baseline.py`](app/detection/baseline.py) `Baseline`; sampling + contamination guard in `ServiceDetector._maybe_sample`; restart rebuild in `Runtime._restore` / `Repository.baseline_samples` | *Baseline ± σ* KPI, dashed line + band on the chart; state `WARMUP` → `NORMAL`; R3 | `test_baseline`, `test_e2e` (flat through an incident, rebuilt after restart), `test_storage` |
| 4 | Detect deviations from the baseline | [`app/detection/detector.py`](app/detection/detector.py) `ServiceDetector.tick` (z, ratio, gates → `ANOMALY`); [`app/detection/state.py`](app/detection/state.py) `AlertStateMachine.evaluate` (confirm, hysteresis, dedup) | *Traffic spike* → no alert; *Error spike* → alert; R4 | `test_baseline` (traffic-only spike), `test_state`, `test_nasa_e2e` |
| 5 | Assign severity levels | [`app/detection/severity.py`](app/detection/severity.py) `classify`; gate table in [`config.yaml`](config.yaml) `severity:` | alert cards show MEDIUM → HIGH → CRITICAL escalation; R5 | `test_severity`, `test_state` |
| 6 | Real-time frontend using WebSockets | [`app/api/websocket.py`](app/api/websocket.py) `Hub`; [`app/api/routes.py`](app/api/routes.py) `ws_endpoint`; client in [`frontend/index.html`](frontend/index.html) | open http://localhost:8000 (no refresh needed; reconnects with backoff); R6 | `test_api` (hello + live `metric.update`), `test_replay` |
| 7 | Display alerts as they are generated | [`app/alerts/manager.py`](app/alerts/manager.py) `AlertManager._handle` broadcasts `alert.created / updated / resolved`; alert feed in `frontend/index.html` | click *Error spike*: the card appears within ~2 ticks with delivery badges; R7 | `test_e2e` (file → WebSocket), `test_alerts` (lifecycle messages), `test_nasa_e2e` |
| 8 | Push alerts to AWS CloudWatch Logs or SNS | [`app/alerts/sns.py`](app/alerts/sns.py) `SnsSink`, [`app/alerts/cloudwatch.py`](app/alerts/cloudwatch.py) `CloudWatchSink`, [`app/alerts/dispatcher.py`](app/alerts/dispatcher.py) `Dispatcher`, [`app/alerts/__init__.py`](app/alerts/__init__.py) `build_sinks`, [`app/alerts/aws_common.py`](app/alerts/aws_common.py) `aws_startup_check` | **mock:** R8 (moto). **live (needs your AWS account):** the [AWS checklist](#verify-with-real-aws-manual-checklist) → *Send test alert* | `test_aws_sinks` (SNS + CloudWatch against moto, retry/FAILED, AWS-unreachable isolation, startup check) |

Honest scope of #8: the AWS sinks are verified against `moto`, an in-process mock; nothing in this repository has been run against a real AWS account. The live check is a manual step you do once.

## The demo

With `DEMO_MODE=true` (the default in `.env.example`) the buttons make a generator **append to the real log file**; the tailer, detector and alerting run the full pipeline. Nothing is faked in the browser.

| Button | What happens |
|---|---|
| **Mixed (2-min story)** | scripted: normal → traffic spike (t=25 s) → error spike (t=35 s) → recover (t=70 s) |
| Normal | ~15 ev/s across 2 services, 4–6 % errors. `WARMUP` for ~20 s, then `NORMAL` |
| Traffic spike | 5× volume, same error rate → **no alert** ("volume ≠ errors") |
| Error spike | payment-service 6 → 15 → 38 %: MEDIUM/HIGH opens, escalates to CRITICAL (each notifies once) |
| Recover | back to normal → hysteresis → RESOLVED (notifies once) |
| Malformed lines | sprinkles broken JSON; the parse-error counter rises, nothing crashes |
| **Send test alert** | a labelled fake alert through the real dispatcher to **every configured sink** — verifies SNS / CloudWatch / ntfy / Telegram wiring in seconds; stored as resolved so it never counts as an incident |

Headless: `python scripts/generate_logs.py --scenario mixed --seed 42` (in Docker: `docker compose exec app python scripts/generate_logs.py --scenario mixed` — run it inside the container to avoid bind-mount inode quirks).

## How detection works

| Stage | Rule |
|---|---|
| Window | per-service time-based deque of `(ts, is_error)`; evicted on every event **and** every tick, so a silent service decays to `LOW_DATA` instead of freezing |
| Baseline | sampled every `baseline_sample_every` s, **not per event** (overlapping windows are near-identical samples that would collapse σ); `μ = mean`, `σ = pstdev` |
| Score | `z = (rate − μ) / max(σ, std_floor)`, `ratio = rate / max(μ, ratio_floor)` |
| Gates | severity table: **z ≥ … and errors ≥ … and (rate ≥ … or ratio ≥ …)** — a 2 %→4 % wobble with a tiny σ cannot page anyone |
| States | `WARMUP` (too few baseline samples; alerts suppressed) · `LOW_DATA` (< `min_events`) · `NORMAL` · `ANOMALY` |
| Contamination guard | anomalous windows are never admitted to the baseline; warm-up samples must be ≤ `warmup_ceiling`; the baseline is frozen while an alert is open |
| Lifecycle | open after `confirm_ticks` (CRITICAL immediately) · notify only on open / **escalation past the alert's peak** / resolve · resolve needs `z < resolve_z` and `ratio < resolve_ratio` for `resolve_ticks` in a row (hysteresis) |
| Level shift | open longer than `level_shift_seconds` → resolved as a "sustained level shift"; the baseline relearns the new level so an alert cannot stay stuck |

The clock is injectable, so every behaviour above is unit-tested deterministically.

## Alert delivery

`AlertManager` writes the alert **and** `PENDING` delivery rows to SQLite first; the `Dispatcher` then sends each (alert, channel) as its own task with retry
(`retry_attempts`, exponential `retry_backoff_seconds`, a `sink_timeout_seconds` per send) → `DELIVERED` / `FAILED` + `last_error`. Detection never awaits any of it.
After a restart, `PENDING` rows are re-enqueued and `OPEN` alerts reload (no duplicate "created").

| Sink | Enabled when | Notes |
|---|---|---|
| console | always | dev visibility |
| jsonl | always | `ALERTS_JSONL`; the same schema as the CloudWatch message |
| ntfy | `NTFY_TOPIC` set (not the placeholder) | the topic is the password; `NTFY_BASE_URL` for self-hosted |
| telegram | `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` | @BotFather → `/newbot`; message the bot once; read `chat_id` from `getUpdates` |
| webhook | `WEBHOOK_URL` and `webhook` in `alerts.sinks` | Discord / Slack |
| sns | `AWS_ENABLED=true` + a real `SNS_TOPIC_ARN` | email via SNS; returns the `MessageId` (badge tooltip); ASCII-sanitised subject; severity/service/event message attributes |
| cloudwatch | `AWS_ENABLED=true` + `CW_LOG_GROUP` | one JSON event per alert into stream `<prefix>/YYYY-MM-DD`; the log group must already exist |

Every successful delivery stores an `external_id` (SNS `MessageId` / CloudWatch `group:stream`) as proof; hover a delivery badge to see it.

## Configuration reference

Thresholds live in [`config.yaml`](config.yaml); secrets and paths come from the environment (`.env` is read automatically; real environment variables win).
Select a profile with `LOGPULSE_PROFILE` (`demo` default · `prod` · `nasa`). A profile can carry `overrides:` that are deep-merged over the top-level sections for that profile only.

**Profiles** (`profiles.<name>`): `window_seconds` (sliding window; demo 10, prod 60) · `tick_seconds` (evaluation cadence, 1) · `baseline_sample_every` (baseline sampling cadence) ·
`baseline_max_samples` (rolling baseline length) · `min_baseline_samples` (warm-up length) · `min_events` (below this a window is `LOW_DATA`) · `level_shift_seconds` (open longer → level shift).

| Section | Key | Default | Meaning |
|---|---|---|---|
| `detector` | `std_floor` | 0.02 | σ floor (2 pp): stops a perfectly steady baseline making every wiggle "infinitely anomalous" |
| | `ratio_floor` | 0.005 | denominator floor for `rate / max(μ, ratio_floor)` |
| | `warmup_ceiling` | 0.25 | a warm-up sample above this rate is not admitted (an incident during startup must not become "normal") |
| | `confirm_ticks` / `resolve_ticks` | 2 / 3 | consecutive ticks to open (CRITICAL opens at once) / to resolve |
| | `resolve_z` / `resolve_ratio` | 1.5 / 1.5 | calm bar to resolve (stricter than the trigger: hysteresis) |
| | `error_definition` | `5xx` | which statuses count as errors: `5xx`, `4xx+5xx`, or a minimum status such as `404` (NDJSON `level` ERROR/CRITICAL/FATAL always counts) |
| `severity` | `medium / high / critical` | z 2/3/4 · rate 0.10/0.15/0.25 · ratio 1.5/3/5 · errors 5/5/10 | all of: `z ≥`, `errors ≥`, and (`rate ≥` **or** `ratio ≥`); the highest satisfied row wins |
| `alerts` | `retry_attempts`, `retry_backoff_seconds`, `sink_timeout_seconds` | 3, 2, 10 | dispatcher retry policy |
| | `sinks` | `[console, jsonl, sns, cloudwatch, ntfy, telegram]` | requested sinks; unconfigured ones are skipped and reported |
| `ingestion` | `format` | `auto` | `ndjson`, `clf` (Common Log Format) or `auto` (a line starting with `{` is NDJSON) |
| | `start_at` | `end` | first run without a checkpoint: `end` skips old lines, `checkpoint` reads from 0 (a valid checkpoint is always honoured) |
| | `poll_seconds`, `queue_max`, `checkpoint_every_seconds` | 0.2, 10000, 2 | tail poll, backpressure bound (drop-oldest, `DEGRADED` at 80 %), checkpoint cadence |
| `storage` | `snapshot_retention_hours`, `prune_every_seconds`, `hello_history_minutes` | 24, 600, 5 | SQLite retention and how much history a new dashboard gets |
| `health` | `degraded_queue_fraction`, `degraded_tail_lag_seconds`, `silence_windows` | 0.8, 5, 3 | health thresholds; a service silent for N windows is flagged |
| `generator` | rates, spike shapes, `mixed_timeline`, `autostart` | see file | the synthetic demo traffic (`autostart: false` in the `nasa` profile) |
| `mapping` | `service_prefixes`, `other_service`, `root_service` | `[]`, `other`, `root` | CLF → service: first URL segment if listed, else `other` |
| `replay` | `speed`, `max_gap_seconds`, `send_to_aws`, `aws_preset`, `aws_max_sends_per_run`, `presets`, … | see file | dataset replay and its AWS safety |

**Environment variables:** `DEMO_MODE` (enables the demo buttons/endpoints) · `LOGPULSE_PROFILE` · `CONFIG_PATH` · `LOG_PATH` · `DB_PATH` · `ALERTS_JSONL` · `DATASET_PATH` ·
`NTFY_TOPIC`, `NTFY_BASE_URL` · `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` · `WEBHOOK_URL` · `AWS_ENABLED`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `SNS_TOPIC_ARN`, `CW_LOG_GROUP`, `CW_LOG_STREAM_PREFIX`.
`.env.example` documents each; Docker overrides the paths with `/data/...` (see `docker-compose.yml`).

## API

`GET /health` · `GET /api/metrics/current` · `GET /api/metrics/history?service=&minutes=` · `GET /api/alerts[?status=&limit=]` · `GET /api/alerts/{id}` ·
`GET /api/system/status` (health, sinks, AWS startup-check panel, replay) · `POST /api/demo/scenario {"name": …}` · `POST /api/demo/test-alert` ·
`GET/POST /api/demo/replay` (the three demo endpoints return 404 unless `DEMO_MODE=true`) · `WS /ws` · interactive docs at `/docs`.

WebSocket messages are `{"type", "data"}`: `hello` (full state on connect) · `metric.update` (once per tick per service) ·
`alert.created` / `alert.updated` / `alert.resolved` (with delivery badges) · `health.update`. The client pings every 60 s and reconnects with backoff.

## Real data: replaying the NASA HTTP log

LogPulse also runs on a **real** access log: the public NASA Kennedy Space Center WWW server log for July 1995 (1.89 M requests, 27.6 days, Common Log Format).
The traffic is real; the **error definition (4xx + 5xx) and every threshold are chosen settings** — nothing in the log labels an incident.
Measurements, the tuning sweep and the reasoning: [docs/DATASET_ANALYSIS.md](docs/DATASET_ANALYSIS.md). Attribution: [ACKNOWLEDGEMENTS.md](ACKNOWLEDGEMENTS.md).

**1. Get the file.** Download `NASA_access_log_Jul95.gz` from the Internet Traffic Archive (<http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html>), gunzip it, and put it here
(git-ignored; never committed): `data/datasets/NASA_access_log_Jul95` (~205 MB; the name `access_log_Jul95` is accepted too).

**2. Analyse it** (streams the file, ~16 s, never loads it whole):

```bash
python scripts/analyze_dataset.py --window-minutes 10 30        # census, per-service rates, distribution, top spikes, threshold hints
python scripts/analyze_dataset.py --error-definition 5xx        # why 5xx alone is useless here (76 lines)
python scripts/analyze_dataset.py --simulate --profile nasa     # run LogPulse's real detector over the whole month (~75 s), list its alerts
```

**3. Replay it into the live system.** Start the app with the tuned `nasa` profile (CLF parser, 4xx+5xx errors, top-8 URL prefixes as services, thresholds derived from the data), then replay a preset:

```bash
# terminal 1     (PowerShell: $env:LOGPULSE_PROFILE='nasa'; $env:DEMO_MODE='true'; python -m uvicorn app.main:app --port 8000)
LOGPULSE_PROFILE=nasa DEMO_MODE=true python -m uvicorn app.main:app --port 8000
# terminal 2   (or use the "Dataset replay" panel on the dashboard, or POST /api/demo/replay)
python scripts/replay_dataset.py --list-presets
python scripts/replay_dataset.py --preset spike-history-jul24
python scripts/replay_dataset.py --start "1995-07-13 06:30" --end "1995-07-13 11:00" --speed 180 --seed 1 [--loop]
```

`--speed` is *original seconds per wall-clock second*: at 180, one wall second = 3 original minutes and the profile's 10 s detector window = 30 original minutes; a 4.5-hour
preset takes 90 s. Timestamps are rewritten to "now" with the original gaps scaled by `--speed` (same-second requests are spread with a seeded RNG: same `--seed`, same replay);
the original timestamp stays in the line (`orig_ts="…"`). Silent gaps are capped at `--max-gap` wall seconds (keep it above the window so `LOW_DATA` still shows briefly).
In Docker: set `LOGPULSE_PROFILE=nasa` in `.env`, put the file in `./data/datasets/` (compose mounts `./data` at `/data`), then
`docker compose exec app python scripts/replay_dataset.py --preset spike-history-jul24`.

**Presets** (real segments, ET; verified with the real detector and a live replay — results in [docs/DATASET_ANALYSIS.md](docs/DATASET_ANALYSIS.md)):

| preset | what it is | expected |
|---|---|---|
| `spike-history-jul24` | `/history` 404 storm, 24 Jul ~03:12 (24 % in 30 min) | history **HIGH** |
| `spike-icons-jul12` | `/icons` error burst ~48 %, 12 Jul ~10:26 | icons **CRITICAL** |
| `spike-cgibin-jul03` | `/cgi-bin` failures ~45 %, 3 Jul ~10:54 | cgi-bin **CRITICAL** (a live run also raised a borderline `history` MEDIUM at 09:40: 12.3 % vs the 12 % floor) |
| `volume-surge-jul13` | **5–6× traffic** 13 Jul 08:30–10:20, error rate unchanged | **no alert** |
| `normal-jul16` | a busy weekday, 16 Jul 08:00–14:00 | **no alert** |

**AWS is OFF during a replay** (`replay.send_to_aws: false`), so a long replay cannot flood email or CloudWatch. While a replay runs — from the dashboard/API *or* the external script
(it announces itself through a heartbeat file) — SNS and CloudWatch deliveries are not even created; ntfy, Telegram, console, JSONL and the dashboard keep working and detection is unaffected.
Only the one preset named in `replay.aws_preset` (`spike-history-jul24`) may opt in (`--aws`, or the dashboard checkbox), and even then the app enforces `replay.aws_max_sends_per_run`
(default 20, shared by both AWS sinks). The server decides; a client cannot override it. Tests and the analysis scripts never touch AWS.

**Known limits.** One global set of severity floors (12 % / 20 % / 30 %) cannot suit every service: `history` and the catch-all `other` are intrinsically bursty (routine p99 window error rate ≈ 10 %),
so moderate spikes in quiet services such as `shuttle` (5–8 % against 0.2 %) are *not* alerted. The thresholds were tuned on this one month of this one site.

## AWS (live SNS + CloudWatch)

**Strategy:** SNS is the *notification* (email to the team / judges); CloudWatch Logs is the *evidence trail* (queryable JSON). ntfy / Telegram / JSONL stay on as redundant channels, so a
bad key, wrong region or venue Wi-Fi cannot silence the alerts — and cannot touch detection (alerts are persisted first, the dispatcher retries, failures show as `sns ✗` badges).
Use **one region everywhere** (suggested `ap-south-1`).

### One-time setup (someone with an admin login, ~20 min — never use root access keys)

1. **SNS → Topics → Create topic** (Standard) `logpulse-alerts`; copy the Topic ARN.
2. Add an **Email** subscription per recipient. **Each person must click "Confirm subscription"** in their inbox (check spam) — unconfirmed subscriptions receive nothing.
3. **CloudWatch → Log groups → Create** `/logpulse/alerts` with **7-day retention** (the default never expires). LogPulse does *not* create the group.
4. **IAM → Policies → Create** from [`iam-policy.json`](iam-policy.json) (replace `<ACCOUNT_ID>`, and the region if not `ap-south-1`) → name `LogPulseAlertWriter`.
5. **IAM → Users → Create** `logpulse-app`, *no console access*, attach only that policy → **Create access key** ("application running outside AWS"); copy it once.
6. **Billing → Budgets** → create a small budget (e.g. $1) with your email.
7. Smoke test before running LogPulse: `aws sns publish --topic-arn <ARN> --region ap-south-1 --subject "LogPulse test" --message hello` → an email arrives.

### Configure (only on the machine that will talk to AWS)

```bash
# .env  (gitignored - never commit, never paste into chat)
AWS_ENABLED=true
AWS_REGION=ap-south-1
AWS_ACCESS_KEY_ID=...            # the logpulse-app key
AWS_SECRET_ACCESS_KEY=...
SNS_TOPIC_ARN=arn:aws:sns:ap-south-1:<your account id>:logpulse-alerts
CW_LOG_GROUP=/logpulse/alerts
CW_LOG_STREAM_PREFIX=alerts
```

`docker compose` forwards `.env` into the container; a local run exports the `AWS_*` values from `.env` for boto3. Everyone else keeps `AWS_ENABLED=false` (they still get console / JSONL /
ntfy and run the `moto` tests). A sink is skipped, with the reason on the health panel, when a variable is missing or still the `<ACCOUNT_ID>` placeholder.

**Startup check (fail-soft):** on start LogPulse checks the credentials (`sts:GetCallerIdentity`, no permission needed), the SNS topic (`sns:GetTopicAttributes`) and the log group (creating today's stream).
Results show in the **AWS row of the health panel** and `GET /api/system/status` → `aws`. A failure is reported and logged but never blocks startup or delivery on other channels.

### Verify with real AWS (manual checklist)

- [ ] `aws sns publish …` from the CLI → email received
- [ ] `.env` on the demo machine has keys, region, topic ARN and log group; `AWS_ENABLED=true`
- [ ] `docker compose up --build` → logs show `AWS credentials OK: arn:aws:iam::…:user/logpulse-app`
- [ ] Health panel: `AWS identity ✓ ok · SNS ✓ ok · CloudWatch ✓ ok`
- [ ] Dashboard → **Send test alert** → email arrives (SNS)
- [ ] CloudWatch console → `/logpulse/alerts` → stream `alerts/YYYY-MM-DD` → the JSON event is there
- [ ] The alert's `sns` / `cloudwatch` badges show `✓`; hovering shows the MessageId / `group:stream`
- [ ] Run **Mixed** → alerts at open, each escalation and resolve reach the mailbox **and** CloudWatch
- [ ] Turn Wi-Fi off (or set a wrong topic ARN) → `sns ✗ retrying` then `FAILED`; detection keeps running; ntfy / JSONL still deliver
- [ ] `pytest` is green

CloudWatch Logs Insights (events are JSON, so fields are auto-discovered):
`fields @timestamp, service, severity, current_error_rate, z_score | filter severity = "CRITICAL" | sort @timestamp desc`

**Security, cost, cleanup.** Dedicated IAM user, two actions on two resources ([`iam-policy.json`](iam-policy.json)), keys only in a gitignored `.env`, 7-day log retention. If a key leaks:
IAM → user → *Security credentials* → **Deactivate**, delete, create a new one. Volume is a handful of alerts per demo (dedup prevents storms); SNS and CloudWatch Logs have free allowances but terms
change — verify in the Billing console and keep the budget alert; use **email**, not SMS. Afterwards delete the SNS topic, the log group, the access key(s), the IAM user and the policy.

| AWS symptom | Likely cause | Fix |
|---|---|---|
| No email | subscription *Pending confirmation*; spam; wrong region/ARN | confirm it; check SNS → Subscriptions; re-check ARN and region |
| `AuthorizationError` / `AccessDenied` on publish | policy `Resource` ≠ the real topic ARN | compare them character by character |
| `InvalidClientTokenId` / `SignatureDoesNotMatch` | typo'd / whitespace / deactivated key, or a skewed system clock | re-copy the keys; sync the clock |
| `ResourceNotFoundException` (logs) | log group missing, or wrong name/region | create `/logpulse/alerts` in the same region |
| `AccessDenied` on `PutLogEvents` | policy log-group ARN missing the trailing `:*` | use the ARN from `iam-policy.json` |
| Works locally, nothing from Docker | env vars not reaching the container | check `env_file`; `docker compose exec app env \| grep AWS` |

## Testing

```bash
pip install -r requirements-dev.txt                  # pytest, pytest-asyncio, pytest-cov, moto, ruff (+ runtime deps)
python -m pytest -q                                  # the whole suite, offline (moto for AWS, httpx.MockTransport for ntfy/Telegram/webhook)
python -m pytest -q --cov=app --cov=scripts --cov-report=term-missing     # with coverage
python -m ruff check app scripts tests               # lint (pyflakes, syntax/runtime errors, bugbear)
python scripts/verify_requirements.py                # the 8 problem-statement requirements, run against the real code
python scripts/benchmark.py                          # measured throughput / latency on YOUR machine
```

Tests use a fake clock and small **real** fixtures (`tests/fixtures/`, a few hundred lines of the NASA log) — never the 200 MB file, never real AWS or network (the NASA end-to-end tests fail if a boto3 client is created).
The latest measured results (test count, coverage, lint, the live NASA replay, secrets scan) are recorded in [docs/TEST_REPORT.md](docs/TEST_REPORT.md).

## Reliability, and the test that proves it

| Edge case | Handling | Tests |
|---|---|---|
| partial writes | a line is parsed only after its `\n` | `test_parser_tailer` |
| malformed / missing-field lines | counted, last 20 sampled on the health panel, never fatal | `test_parser_tailer`, `test_e2e` |
| low volume | `LOW_DATA`, no evaluation, no baseline sample | `test_baseline` |
| traffic surge | rate-based + gates → no alert | `test_baseline`, `test_e2e`, `test_nasa_e2e` |
| incident teaching the detector | contamination guard, warm-up ceiling, frozen while open | `test_baseline`, `test_e2e` |
| flapping / alert storms | confirm ticks, hysteresis, dedup, notify-on-escalation only | `test_state` |
| sustained level shift | resolved as a level shift; baseline relearns | `test_state`, `test_robustness` |
| sink failure | retries → `FAILED`, detection unaffected, other sinks unaffected | `test_alerts` |
| AWS unreachable / mis-set | SNS + CloudWatch go `FAILED`; alert already persisted; other channels still deliver; startup check fail-soft | `test_aws_sinks`, `test_health` |
| old database | `external_id` column added in place, data kept | `test_storage` |
| real-world log oddities (binary junk, spaces in URLs, `-` bytes, truncated line) | tolerant CLF parser; only unusable lines raise `ParseError` and are counted | `test_clf`, `test_nasa_e2e` |
| replay flooding AWS | SNS/CloudWatch off during a replay, one opt-in preset, per-run cap, server-enforced | `test_replay`, `test_nasa_e2e` |
| crash / restart | checkpoint resume (no replay), baseline rebuilt from SQLite, open alerts + pending deliveries reloaded | `test_e2e`, `test_storage` |
| rotation / truncation | reopen from 0 after draining the old handle | `test_parser_tailer` (rename test skipped on Windows), `test_robustness` (simulated) |
| backpressure | bounded queue, drop-oldest, `dropped_events`, `DEGRADED` at ≥ 80 % | `test_health` |
| DB down | repository degrades, health goes `DOWN`, pipeline keeps running | `test_storage`, `test_health` |
| a crashing tick / event / replay | logged and survived; the AWS guard and generator are released | `test_robustness`, `test_replay` |

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `docker compose` says `.env` not found | not copied yet | `cp .env.example .env` |
| Port 8000 already in use | another process | `python -m uvicorn app.main:app --port 8001` (Docker: change `ports:` in `docker-compose.yml`) |
| Dashboard stays `WARMUP` / no data | nothing is being appended, or the writer and the app disagree on `LOG_PATH` | click *Mixed* (needs `DEMO_MODE=true`); check both use the same `LOG_PATH`; health panel shows "last event" |
| Health `DOWN`: "log file missing" | `LOG_PATH` points at a file that does not exist (demo mode creates it) | fix `LOG_PATH` or start the writer |
| `No such file or directory: '/data/...'` locally | a `.env` copied from an older example with Docker paths | use the current `.env.example` (relative `data/...` paths) |
| Buttons missing | `DEMO_MODE` is not `true` | set it in `.env` and restart |
| Health panel: `ntfy ⊘ disabled` / `sns ⊘ disabled` | that sink's variables are unset or placeholders (by design) | set them — see the sink table; the reason is shown on hover |
| `ModuleNotFoundError: moto` / `pytest_cov` | dev dependencies missing | `pip install -r requirements-dev.txt` |
| Replay: `409 dataset not found` | the raw log is not in `data/datasets/` | download it (see the NASA section); `access_log_Jul95` also works |
| Replay runs but nothing alerts / thresholds look wrong | app is on the `demo` profile (10 %/15 %/25 % floors, 5xx-only errors, no URL-to-service mapping) | start with `LOGPULSE_PROFILE=nasa` — the dashboard warns when the profile is not `nasa` |
| No AWS deliveries during a replay | intended: AWS is off during replays | only `replay.aws_preset` may opt in (`--aws`); see the NASA section |
| Windows: tail seems to miss a rotated file | Windows cannot rename an open file; inode semantics differ | develop/run in Docker or WSL for rotation scenarios |
| ntfy pushes stop arriving | free-tier daily cap, or a wrong topic | check the topic; dedup keeps volume low; JSONL/dashboard still record everything |

## Benchmark

`python scripts/benchmark.py` replays synthetic lines and prints what it **measured on your machine**: parse+detect events/s and p50/p95/p99 per-event latency, and end-to-end
file→tailer→queue→engine events/s with the max queue depth reached (excludes SQLite and WebSocket work). Numbers from the last run are in `docs/TEST_REPORT.md`.

## Known limitations

- No dashboard authentication (a non-goal): keep it on localhost or behind a tunnel. The ntfy topic is effectively a password — don't commit or screenshot it.
- One process, one node. The scale path is collector → Kafka/Kinesis → detector workers partitioned by service → aggregate store → alert service; none of that is built.
- The checkpoint stores the tailer's read position, so events sitting in the in-memory queue at the instant of a hard crash can be lost (seconds at most).
- One global set of severity floors per profile: quiet services with tiny baselines and a bursty catch-all bucket cannot both be served (see the NASA section). Per-service floors are not built.
- The AWS sinks are verified against `moto`; live delivery needs your account and the manual checklist. `docker compose up --build` has not been run in the environment this was developed in.
- Free-tier terms (ntfy daily cap, Render sleep behaviour, AWS free plan) change — re-check them the day you deploy.

Optional public URL: Render free web service (Docker) sleeps after 15 min without HTTP/WebSocket traffic (the 60 s client ping keeps an open dashboard awake), cold-starts in 30–60 s and has an
ephemeral disk (SQLite resets on redeploy). A laptop plus a Cloudflare Tunnel / ngrok link is the simpler alternative.

## Project layout

```
app/            ingestion/ (tailer, ndjson + clf parsers)  detection/ (window, baseline, severity, detector, alert state machine)
                alerts/ (sinks, dispatcher, manager, AWS plumbing)  storage/ (SQLite)  api/ (routes, websocket)
                main.py (wiring)  config.py  health.py  generator.py (demo traffic)  replay.py (dataset replay, AWS guard)
frontend/       index.html + vendored Chart.js (no CDN, no build step)
scripts/        generate_logs.py  replay_dataset.py  analyze_dataset.py  verify_requirements.py  benchmark.py
tests/          unit + integration + end-to-end, tests/fixtures/ (real NASA lines)
docs/           PRD.md  architecture.md  DECISIONS.md  DATASET_ANALYSIS.md  TEST_REPORT.md  WORKLOG.md
config.yaml  .env.example  iam-policy.json  Dockerfile  docker-compose.yml  requirements.txt  requirements-dev.txt  ruff.toml
```

License and attribution for the dataset and third-party libraries: [ACKNOWLEDGEMENTS.md](ACKNOWLEDGEMENTS.md).
