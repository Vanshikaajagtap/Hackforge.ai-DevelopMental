# LogPulse

Explainable, statistical, **poison-resistant error-rate detection** for a growing log file — with alert-fatigue
control, live WebSocket dashboard, and alert delivery to **AWS SNS (email) + AWS CloudWatch Logs (structured evidence trail)**, with
ntfy / Telegram / a JSONL file as redundant channels — all behind one pluggable `AlertSink` interface. Detection never depends on AWS.

Tails an append-only NDJSON log → per-service sliding window → past-only baseline that an incident cannot contaminate →
z-score **and** ratio **and** absolute gates → severity → alert state machine (confirm, escalate, hysteresis, dedup) →
SQLite first, then delivery with retries. Every alert says what / where / when / current / normal / how much / z / sample size / severity / delivered where.

## Quickstart

```bash
cp .env.example .env          # set NTFY_TOPIC (long random string) and, optionally, the Telegram vars
docker compose up --build
# open http://localhost:8000  -> subscribe to https://ntfy.sh/<your topic> on your phone -> click "Error spike"
```

Without Docker (Python 3.12+):

```bash
python -m venv .venv && . .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
cp .env.example .env    # then set LOG_PATH=data/app.log DB_PATH=data/logpulse.db ALERTS_JSONL=data/alerts.jsonl
uvicorn app.main:app --port 8000
pytest -q
```

Unset / placeholder sinks are skipped (and shown as `disabled` on the health panel) — the dashboard, SQLite and `alerts.jsonl` need no network.

## The demo (about 2 minutes, `DEMO_MODE=true`)

The buttons make a generator **append to the real log file**; the tailer, detector and alerting run the full pipeline. Nothing is faked in the browser.

| Button | What happens |
|---|---|
| **Mixed (2-min story)** | scripted timeline: normal → traffic spike (t=25 s) → error spike (t=35 s) → recover (t=70 s) |
| Normal | ~15 ev/s across 2 services, 4–6 % errors. `WARMUP` for ~20 s, then `NORMAL` |
| Traffic spike | 5× volume, same error rate → **no alert** ("volume ≠ errors") |
| Error spike | payment-service 6 → 15 % → 38 %: MEDIUM/HIGH opens, escalates to CRITICAL (each notifies once) |
| Recover | back to normal → hysteresis → RESOLVED (notifies once) |
| Malformed lines | sprinkles broken JSON; watch the parse-error counter, nothing crashes |
| **Send test alert** | pushes a labelled fake alert through the real dispatcher to **every configured sink** — verifies SNS / CloudWatch / ntfy / Telegram wiring in seconds. Stored as resolved, so it never counts as an incident |

Headless: `docker compose exec app python scripts/generate_logs.py --scenario mixed` (or `--scenario error_spike --seed 42`). Run the generator
**inside** the container to avoid bind-mount inode quirks on Windows/macOS.

## How detection works

| Stage | Rule |
|---|---|
| Window | per-service time-based deque of `(ts, is_error)`; evicted on every event **and** every tick, so a silent service decays to `LOW_DATA` |
| Baseline | sampled every `baseline_sample_every` s (not per event — overlapping windows would collapse σ); `μ = mean`, `σ = pstdev` |
| Score | `z = (rate − μ) / max(σ, std_floor)`, `ratio = rate / max(μ, 0.005)` |
| Gates | severity table in `config.yaml`: **z ≥ … and errors ≥ … and (rate ≥ … or ratio ≥ …)** — a 2 %→4 % wobble can't page anyone |
| States | `WARMUP` (too few baseline samples, alerts suppressed) · `LOW_DATA` (< `min_events`) · `NORMAL` · `ANOMALY` |
| Contamination | anomalous windows are never admitted; warm-up samples must be ≤ `warmup_ceiling`; the baseline is frozen while an alert is open |
| Lifecycle | open after `confirm_ticks` (CRITICAL immediately) · notify only on open / **escalation past the peak** / resolve · resolve needs `z < 1.5` and `ratio < 1.5` for `resolve_ticks` in a row |
| Level shift | open longer than `level_shift_seconds` → resolved as a "sustained level shift", baseline relearns the new level |

All thresholds live in [config.yaml](config.yaml) (`profile: demo` = 10 s window / ~20 s warm-up; `prod` = 60 s window).
Secrets and paths come from `.env`. The clock is injectable, so every behaviour above is unit-tested deterministically.

## Alert delivery

`AlertManager` writes the alert **and** `PENDING` delivery rows to SQLite first; the `Dispatcher` then sends each (alert, channel) as its own
task with retry (`retry_attempts`, exponential `retry_backoff_seconds`) → `DELIVERED` / `FAILED` + `last_error`. Detection never awaits any of it.
On restart, `PENDING` rows are re-enqueued and `OPEN` alerts reload (no duplicate "created").

| Sink | Enabled when | Notes |
|---|---|---|
| console | always | dev visibility |
| jsonl | always | `ALERTS_JSONL`; the CloudWatch `PutLogEvents` schema |
| ntfy | `NTFY_TOPIC` set (not the placeholder) | topic is the password; `NTFY_BASE_URL` for self-hosted |
| telegram | `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` | @BotFather → `/newbot`; message the bot; read `chat_id` from `getUpdates` |
| webhook | `WEBHOOK_URL` and `webhook` in `alerts.sinks` | Discord / Slack |
| sns | `AWS_ENABLED=true` and a real `SNS_TOPIC_ARN` | email via SNS; returns the `MessageId` (shown on the dashboard badge tooltip); ASCII-sanitised subject; severity/service/event message attributes for filter policies |
| cloudwatch | `AWS_ENABLED=true` and `CW_LOG_GROUP` | one JSON event per alert into stream `<prefix>/YYYY-MM-DD` (new stream per UTC day); same schema as `alerts.jsonl`; the log group must already exist |

Both AWS sinks are verified against `moto` in the test suite. **Live** delivery to a real AWS account is a manual step — see [AWS setup](#aws-live-sns--cloudwatch) below.
Every successful delivery stores an `external_id` (SNS `MessageId` / CloudWatch `group:stream`) as proof; hover a delivery badge on the dashboard to see it.

## API

`GET /health` · `GET /api/metrics/current` · `GET /api/metrics/history?service=&minutes=` · `GET /api/alerts[?status=&limit=]` · `GET /api/alerts/{id}` ·
`GET /api/system/status` (includes the `aws` startup-check panel) · `POST /api/demo/scenario {"name": …}` and `POST /api/demo/test-alert` (both 404 unless `DEMO_MODE=true`) · `WS /ws` · interactive docs at `/docs`.

WebSocket messages are `{"type", "data"}`: `hello` (full state on connect) · `metric.update` (once per tick per service) ·
`alert.created` / `alert.updated` / `alert.resolved` (with delivery badges) · `health.update`. The client pings every 60 s.

## AWS (live SNS + CloudWatch)

**Strategy:** SNS is the *notification* (email to the team / judges); CloudWatch Logs is the *evidence trail* (queryable JSON). ntfy / Telegram / JSONL stay on as
redundant channels, so a bad key, wrong region or venue Wi-Fi cannot silence the alerts — and cannot touch detection (alerts are persisted first, the dispatcher retries,
failures show as `sns ✗` badges). Use **one region everywhere** (suggested `ap-south-1`).

### One-time setup (someone with an admin login, ~20 min — never use root access keys)

1. **SNS → Topics → Create topic** (Standard) `logpulse-alerts`; copy the Topic ARN.
2. Add an **Email** subscription per recipient. **Each person must click "Confirm subscription"** in their inbox (check spam) — unconfirmed subscriptions receive nothing.
3. **CloudWatch → Log groups → Create** `/logpulse/alerts` with **7-day retention** (the default is never-expire). LogPulse does *not* create the group.
4. **IAM → Policies → Create** from [`iam-policy.json`](iam-policy.json) (replace `<ACCOUNT_ID>`, and the region if not `ap-south-1`) → name `LogPulseAlertWriter`.
5. **IAM → Users → Create** `logpulse-app`, *no console access*, attach only that policy → **Create access key** ("application running outside AWS") and copy it once.
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

`docker compose` forwards `.env` into the container; a local run also exports the `AWS_*` values from `.env` for boto3. Everyone else keeps `AWS_ENABLED=false`
(they still get console / JSONL / ntfy and run the `moto` tests). The sinks are skipped, with the reason shown on the health panel, when a variable is missing or is
still the `<ACCOUNT_ID>` placeholder.

### Startup check (fail-soft)

On start LogPulse checks the credentials (`sts:GetCallerIdentity`, no permission needed), the SNS topic (`sns:GetTopicAttributes`) and the log group (creating today's
stream). Results appear in the **AWS row of the health panel** and in `GET /api/system/status` → `aws`. A failure is reported and logged but never blocks startup or delivery on other channels.

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

### Security, cost, cleanup

- Dedicated IAM user, two actions on two resources ([`iam-policy.json`](iam-policy.json)), keys only in a gitignored `.env`, 7-day log retention. If a key leaks: IAM → user → *Security credentials* → **Deactivate**, delete, create a new one.
- Volume is a handful of alerts per demo (dedup prevents storms). SNS and CloudWatch Logs have free allowances, but terms change — verify in the Billing console and keep the budget alert. Use **email**, not SMS.
- After the event: delete the SNS topic, the log group, the access key(s), the IAM user and the policy (`aws sns delete-topic`, `aws logs delete-log-group`, `aws iam delete-access-key` / `detach-user-policy` / `delete-user` / `delete-policy`).

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| No email | subscription still *Pending confirmation*; spam; wrong region/ARN | confirm it; check SNS → Subscriptions; re-check ARN and region |
| `AuthorizationError` / `AccessDenied` on publish | policy `Resource` ≠ the real topic ARN | compare them character by character |
| `InvalidClientTokenId` / `SignatureDoesNotMatch` | typo'd / whitespace / deactivated key, or a skewed system clock | re-copy the keys; sync the clock |
| `ResourceNotFoundException` (logs) | log group missing, or wrong name/region | create `/logpulse/alerts` in the same region |
| `AccessDenied` on `PutLogEvents` | policy log-group ARN missing the trailing `:*` | use the ARN from `iam-policy.json` |
| Works locally, nothing from Docker | env vars not reaching the container | check `env_file`; `docker compose exec app env | grep AWS` |
| Works at home, fails at the venue | blocked / slow network | phone hotspot; rely on ntfy + JSONL; keep a backup video |

Optional stretch: a CloudWatch metric filter + alarm on `severity = CRITICAL` that publishes to the same SNS topic (sends a *second* email per CRITICAL; needs admin rights to create).

## Reliability, and the test that proves it

| Edge case | Handling | Tests |
|---|---|---|
| partial writes | a line is parsed only after its `\n` | `test_parser_tailer` |
| malformed / missing-field lines | counted, last 20 sampled on the health panel, never fatal | `test_parser_tailer`, `test_e2e` |
| low volume | `LOW_DATA`, no evaluation, no baseline sample | `test_baseline` |
| traffic surge | rate-based + gates → no alert | `test_baseline`, `test_e2e` |
| incident teaching the detector | contamination guard, warm-up ceiling, frozen while open | `test_baseline`, `test_e2e` |
| flapping / alert storms | confirm ticks, hysteresis, dedup, notify-on-escalation only | `test_state` |
| sink failure | retries → `FAILED`, detection unaffected, other sinks unaffected | `test_alerts` |
| AWS unreachable / mis-set | SNS + CloudWatch deliveries go `FAILED`; alert already persisted; JSONL (and ntfy / Telegram) still deliver; startup check fail-soft | `test_aws_sinks`, `test_health` |
| old database | `external_id` column added in place, data kept | `test_storage` |
| crash / restart | checkpoint resume (no replay), baseline rebuilt from SQLite, open alerts + pending deliveries reloaded | `test_e2e`, `test_storage` |
| rotation / truncation | reopen from 0 after draining the old handle | `test_parser_tailer` (rename-rotation test is skipped on Windows) |
| backpressure | bounded queue, drop-oldest, `dropped_events`, `DEGRADED` at ≥ 80 % | `test_health` |
| DB down | repository degrades, health goes `DOWN`, pipeline keeps running | `test_storage`, `test_health` |

`pytest -q` runs the whole suite offline (`moto` for AWS, `httpx.MockTransport` for ntfy/Telegram/webhook).

## Benchmark

`python scripts/benchmark.py` replays synthetic lines and prints what it **measured on your machine**: parse+detect events/s and p50/p95/p99
per-event latency, and end-to-end file→tailer→queue→engine events/s with the max queue depth reached. (Excludes SQLite and WebSocket work.)

## Known limitations

- No dashboard authentication (non-goal); keep it on localhost / behind a tunnel. The ntfy topic is effectively a password — don't commit or screenshot it.
- One process, one node. The scale path is collector → Kafka/Kinesis → detector workers partitioned by service → aggregate store → alert service; none of that is built.
- The checkpoint records the tailer's read position, so events sitting in the in-memory queue at the instant of a hard crash can be lost (seconds at most).
- Free-tier terms (ntfy daily cap, Render sleep behaviour, AWS free plan) change — re-check the day you deploy.

Optional public URL: Render free web service (Docker) sleeps after 15 min without HTTP/WebSocket traffic (the 60 s client ping keeps an open dashboard awake),
cold-starts in 30–60 s and has an ephemeral disk (SQLite resets on redeploy). A laptop plus a Cloudflare Tunnel / ngrok link is the simpler alternative.

See [docs/architecture.md](docs/architecture.md) and [docs/DECISIONS.md](docs/DECISIONS.md).
