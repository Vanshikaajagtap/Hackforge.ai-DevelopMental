# LogPulse — Work Log

Everything done in this session, in order, with what was verified and what was not.
Project folder: `C:\Users\DELL\Desktop\LogPulse` · Stack: Python (3.13 locally, 3.12 in the Dockerfile), FastAPI, SQLite, vanilla JS + Chart.js.

## 1. Timeline

| # | What happened |
|---|---|
| 1 | You sent PRD v2 and asked for a solo build in tiers (P0 → P1 → P2). I scaffolded, wrote a first walking skeleton (generator → tailer → detector → WebSocket → chart) and began tests. |
| 2 | You interrupted and asked me to delete everything I had made and wait for a new PRD. I stopped the demo server, checked the folder (it had been empty when I started, so everything in it was mine) and deleted it all. |
| 3 | You sent the complete PRD and asked for the entire system in one go: all P0/P1/P2, contracts, config, schema, sinks, state machine, Docker, tests; then run it end to end. I rebuilt from scratch. |
| 4 | Ran the suite, fixed failures, ran the live demo, inspected the results, fixed what the live run exposed, screenshotted the dashboard, tested restart recovery on live data. |
| 5 | You asked me to run it on localhost. It is running at http://localhost:8000 (demo mode). |
| 6 | This work log. |

## 2. What was built

**Scaffold** — repo structure per PRD §58/§60 (minus the git commands), plus a few extra files: `app/generator.py`, `app/health.py`, `app/alerts/webhook.py`, `requirements-dev.txt`, `pytest.ini`, `.dockerignore`.

| Area | Files | What it does |
|---|---|---|
| Config | `config.yaml`, `app/config.py` | Every threshold in YAML (demo / prod profiles); secrets and paths from env or `.env`. |
| Ingestion | `app/ingestion/tailer.py`, `parser.py`, `models.py` | Poll-tail with byte offset, partial-line buffer, `(inode, offset)` checkpoint resume, rotation/truncation handling. NDJSON parser; bad lines are counted and sampled (last 20), never fatal. |
| Detection | `app/detection/window.py`, `baseline.py`, `severity.py`, `detector.py`, `state.py`, `models.py` | Time-based sliding window (also evicts on tick, so a silent service decays); baseline sampled on a fixed cadence; z-score + ratio + absolute gates; severity table; warm-up / low-data states; contamination guard (anomalies not admitted, warm-up ceiling, frozen while an alert is open); alert state machine (confirm ticks, escalation, hysteresis, dedup, level-shift timeout); silence detection; late-event counting. |
| Alerts | `app/alerts/*` | `AlertSink` protocol and message renderer; sinks: console, JSONL, ntfy, Telegram, webhook, SNS, CloudWatch; dispatcher with retry, exponential backoff, timeout and `PENDING/DELIVERED/FAILED` status; manager that persists the alert and delivery rows **before** sending; sink registry that skips unconfigured sinks. |
| Storage | `app/storage/db.py`, `repository.py` | SQLite WAL with the four PRD tables; all SQL in one place; repository degrades instead of raising. |
| API | `app/api/routes.py`, `websocket.py`, `app/main.py` | All REST routes from §41, WebSocket hub (`hello`, `metric.update`, `alert.*`, `health.update`), wiring, restart recovery. |
| Health | `app/health.py` | HEALTHY / DEGRADED / DOWN plus tail lag, queue depth, drops, late events, parse errors, per-sink status, DB and file status. |
| Demo | `app/generator.py`, `scripts/generate_logs.py` | Six scenarios: normal, traffic_spike, error_spike, recover, mixed (scripted 2-minute story), malformed. Seedable, reproducible. |
| Dashboard | `frontend/index.html`, `frontend/static/chart.umd.min.js` | Single page, dark theme: KPI strip, chart with baseline band and alert markers, live alert feed with delivery badges, health panel, demo buttons. Chart.js is vendored (no CDN). |
| Tooling | `scripts/benchmark.py` | Replays synthetic lines and prints measured numbers. |
| Deploy | `Dockerfile`, `docker-compose.yml`, `.env.example`, `.dockerignore` | Per PRD §52. |
| Docs | `README.md`, `docs/architecture.md`, `docs/DECISIONS.md` | Quickstart, method, reliability table, architecture, five decisions. |

Not built (per your instructions): anything in §6 Non-Goals or P3; team/PPT/Q&A/AWS side-quest parts.

## 3. Verification results

| Check | Result |
|---|---|
| Unit + integration tests | **139 passed, 1 skipped.** The skip is rotation-by-rename, which Windows can't do on an open file. |
| Live demo (real file → tailer → detector → WebSocket) | Traffic spike (window volume 90 → 441 events): **no alert**. Error spike: MEDIUM at 44 s, HIGH, then CRITICAL; resolved at 80.7 s. Baseline stayed 4.7–6.0 % throughout. All deliveries reached DELIVERED. |
| Sink path | ntfy request format verified against a local stand-in server (title, priority, tags, body); JSONL log matched the alerts. |
| Restart recovery on live data | Checkpoint resumed, 2 services restored, both `NORMAL` immediately (no re-warm-up). |
| Dashboard | Rendered in headless Edge during a CRITICAL incident: KPIs, chart, markers, alert cards with peak values, health panel, demo buttons all correct. |
| Packaging | Simulated the Docker image (only the Dockerfile's `COPY` files, runtime-only venv, container start command): `/health`, page and Chart.js respond; compose file parses. |
| Benchmark (this machine, Python 3.13, 20 logical CPUs) | Parse + detect ≈ 225–235k events/s, p95 ≈ 4.4 µs/event. File → tailer → queue → engine ≈ 185–189k events/s, max queue depth 500, 0 dropped. Excludes SQLite and WebSocket work. |

## 4. Bugs found and fixed during the build

1. **Tailer** decided "did the file exist at start" on every poll, so a log file created after startup was skipped to its end (this is exactly the demo case). Now decided once.
2. **Repository** — `open_alerts` was wrapped by the safe-decorator but called another wrapped method, so its own success reset the DB error flag. A dead DB could be reported healthy.
3. **Restart baseline** — only `NORMAL` snapshots were used to rebuild the baseline, so a restart in the first ~minute lost it. Eligible `WARMUP` snapshots (populated, under the ceiling) are now used too.
4. **Alert evidence** only refreshed on escalation, so the resolve message said "peak CRITICAL 26.7 %" when the real peak was 38 %. Evidence now tracks the true peak (quiet update, no extra notification).
5. **ntfy title** for a resolve read "[MEDIUM] … (resolved)"; now "[RESOLVED] …".
6. **Dashboard** opened on the healthy service instead of the one with the active alert, and plotted misleading spikes from sparse early WARMUP windows. Now opens on the service with an open alert; windows under `min_events` are left as gaps.
7. **Benchmark** — cancelled tailer wasn't awaited (Windows file lock); consumer yielded differently from the app. Both fixed so the numbers represent the app.

## 5. Not verified — be honest about these

- `docker compose up --build` — Docker isn't installed on this machine. Only the simulated-image check above was done.
- Real ntfy.sh, Telegram, and AWS SNS/CloudWatch delivery. AWS sinks are tested against `moto` only; ntfy against a local stand-in.
- Interactive browser use: button clicks, tab switching, WebSocket reconnect after a server restart, light mode, phone width. (Only a headless render was checked.)
- Rotation-by-rename on Windows (test skipped).

## 6. Known limitations

- No dashboard authentication (PRD non-goal).
- A hard crash can lose events still in the in-memory queue, because the checkpoint records the read position. Seconds at most.
- Single process / single node; the Kafka/Kinesis scale path is a slide, not code.
- Free-tier terms (ntfy limits, Render sleep, AWS plan) change; re-check on deploy day.

## 7. How to run

**Currently running:** http://localhost:8000 (demo mode, console + JSONL sinks; ntfy/Telegram disabled).
Logs: `data/server.out`, `data/server.err`. Alert log: `data/alerts.jsonl`. Docs: `/docs`.

```powershell
# start (from the project folder)
$env:DEMO_MODE='true'; $env:LOG_PATH='data/app.log'; $env:DB_PATH='data/logpulse.db'; $env:ALERTS_JSONL='data/alerts.jsonl'
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000

# stop the background server
Stop-Process -Id (Get-NetTCPConnection -LocalPort 8000 -State Listen).OwningProcess

# tests / benchmark
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe scripts\benchmark.py
```

Docker: `cp .env.example .env`, set `NTFY_TOPIC` (long random string), then `docker compose up --build`.
To enable phone alerts: set `NTFY_TOPIC` (subscribe to `https://ntfy.sh/<topic>`) and/or `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`.

## 8. Suggested next steps

1. Run `docker compose up --build` on a machine with Docker and confirm the first-run experience from a clean clone.
2. Set a real `NTFY_TOPIC`, click **Mixed**, and confirm the phone buzzes at open, escalation and resolve.
3. Rehearse the 2-minute demo three times; tune thresholds in `config.yaml` if the timing feels off on your machine.
4. Click through the dashboard yourself (buttons, tab switch, refresh mid-incident) and report anything odd.

---

## 9. Update — AWS integration (SNS + CloudWatch made live-ready)

Implemented from `LogPulse_AWS_Integration_Changes.md` (the file wasn't in the repo, so the attached copy was used). Full suite afterwards: **163 passed, 1 skipped**.

**Code:** `AlertSink.send()` returns an external id · new `alerts/aws_common.py` (boto config with low retries, session helper, fail-soft `aws_startup_check`) · `sns.py` (MessageId, ASCII-sanitised subject, message attributes, `healthcheck`) · `cloudwatch.py` (per-UTC-day streams `alerts/YYYY-MM-DD`, returns `group:stream`, recreates a deleted stream once, `healthcheck` using only the two granted permissions) · sink registry skips AWS sinks unless `AWS_ENABLED=true` and vars are real (the `<ACCOUNT_ID>` placeholder counts as unset) · dispatcher stores `external_id` · `alert_deliveries.external_id` column with an in-place, idempotent migration · `AlertManager.send_test` + `POST /api/demo/test-alert` (stored as RESOLVED, `test-` id, never an open incident) · health report gains an `aws` panel · `.env` AWS keys are exported for boto3 · config key renamed `sink_timeout_seconds`, sink list now `[console, jsonl, sns, cloudwatch, ntfy, telegram]`.

**Dashboard:** *Send test alert* button, delivery-badge tooltips showing `external_id`/error, AWS row in the health panel (icon + text), TEST label on test alerts.

**Files/docs:** `iam-policy.json` (placeholders only), `.env.example` AWS block, README AWS section (setup, manual checklist, troubleshooting, security/cost/cleanup), `DECISIONS.md` #4/#5, and **`docs/PRD.md` created** (the PRD wasn't in the repo) with the §8 changes plus the §7 demo/PPT/Q&A edits, §9 timeline notes and the §11 risk rows.

**Verified:** 24 new tests (moto SNS incl. SQS-delivered message inspection, CloudWatch JSON/stream/recreate, healthchecks, startup-check fail-soft cases, AWS-unreachable isolation, external_id storage, DB migration, test-alert endpoint, AWS panel, `.env` credential export). Live run with `AWS_ENABLED=false`: nothing changed (traffic spike → no alert; error spike → CRITICAL → resolved). Live run with `AWS_ENABLED=true` and **no credentials**: startup not blocked, panel shows the error, SNS/CloudWatch deliveries end FAILED after 3 attempts, console/JSONL still DELIVERED, detection kept running. An old pre-`external_id` database was migrated live.

**Not verified (needs real AWS — see the README checklist):** any real SNS email, CloudWatch event, credential check against a real account, the IAM policy itself, and Logs Insights.


---

## 10. Update - real NASA HTTP log (Common Log Format), replayer, tuned `nasa` profile

Dataset: `data/datasets/access_log_Jul95` (205 MB, 1,891,715 lines, 1-28 Jul 1995; also accepted as `NASA_access_log_Jul95`). Git-ignored; never committed.
Inspected by streaming only. Full measurements, the threshold sweep and the reasoning: `docs/DATASET_ANALYSIS.md`.

**Built:** CLF parser + config-driven mapping (`app/ingestion/clf.py`, `factory.py`; `ingestion.format: ndjson|clf|auto`; service from top-8 URL prefixes, level from
status, `detector.error_definition: 5xx | 4xx+5xx | N`) - profile `overrides:` deep-merge so `nasa` changes parser/thresholds/mapping without touching `demo`/`prod` -
replayer (`app/replay.py`, `scripts/replay_dataset.py`, dashboard panel, `GET/POST /api/demo/replay`): timestamps rewritten to now, gaps scaled by speed, seeded sub-second
spread, silence cap, time-seeking, presets, loop - `ReplayGuard` (SNS/CloudWatch off during a replay, one opt-in preset, per-run cap, in-process or external heartbeat) -
`scripts/analyze_dataset.py` (census, per-service/per-minute rates, spikes, threshold hints, `--simulate` runs the real detector) - 5 presets - demo mode now creates the log
file so health is not DOWN before the first replay.

**Measured:** 5xx is only 76 lines (0.004 %), 4xx+5xx is 0.58 %; `history` and `other` are intrinsically bursty (p99 30-min window ~10 %), so z is inflated (tiny baseline sigma)
and the absolute floors carry the discrimination. First attempt (3/6/12 % floors) gave 137 alerts over the month; chosen **12 % / 20 % / 30 %**, `min_events` 40, ratio 24/40/60x,
errors 12/16/24, z 5/8/12, `std_floor` 0.01, `warmup_ceiling` 0.05, `resolve_ratio` 3 gives 11 alerts, all real bursts.

**Bugs found on the way:** an absolute-URI heuristic that turned the real path `/://spacelink...` into the root service; `str.splitlines()` splitting real lines on `\x0c`/`\x85`
(tests use `\n` only); a global-env side effect avoided in tests.

**Verified:** 324 tests pass (1 skipped) incl. CLF parser (valid, `-` bytes, zones, malformed, real oddities), mapping/error definitions, replayer timestamp rewrite / gap scaling /
gap cap / seeking, AWS guard, API, and end-to-end on `tests/fixtures/` (a few hundred real lines) with boto3 blocked. **Live** (app on `nasa`, AWS off, 108,304 events, 0 parse errors):
icons HIGH->CRITICAL (12 Jul 10:27), history MEDIUM->HIGH (24 Jul 03:12), cgi-bin HIGH->CRITICAL (3 Jul 10:55) + one borderline history MEDIUM (3 Jul 09:40), **no alert** on the real
volume-only surge (13 Jul) or the normal segment (16 Jul); an external replay was recognised via its heartbeat and AWS stayed off.

**Not verified:** Docker (not installed); any real AWS; interactive browser use beyond a headless render; that thresholds generalise beyond this one month of this one site;
per-service floors (not built - moderate `shuttle` spikes are not alerted).

## Dataset shipped in the repo (feature/add-nasa-dataset)

- `data/datasets/NASA_access_log_Jul95.gz` (19.5 MB, lossless gzip of the 205 MB log) is now tracked; the unpacked raw file stays git-ignored (GitHub's per-file limit is 100 MB).
- `resolve_dataset_path()` unpacks the archive once, atomically, next to itself; a raw file already present always wins and is never overwritten.
- 8 new tests (unpack once, raw wins, alternative name, broken archive leaves nothing behind, replay from a gz-only folder, API start, pinned SHA-256 + line count). Suite: 358 passed, 1 skipped; ruff clean.
- README, ACKNOWLEDGEMENTS (redistribution terms still to be confirmed by a human), DATASET_ANALYSIS, PRD and DECISIONS (#13) updated.
