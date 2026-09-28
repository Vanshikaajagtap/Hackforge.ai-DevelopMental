# Decisions

1. **Statistical detector over ML.** The requirement is a streaming rate-deviation problem. A sliding window + past-only baseline + z/ratio/absolute gates is
   deterministic, low-latency and explainable (every alert carries its evidence). ML can be layered on later for semantic anomalies behind the same interfaces.
2. **SQLite (WAL) over Postgres.** We persist compact aggregates — snapshots, alerts, deliveries, a checkpoint — not raw logs. That is durable, zero-ops and one
   container fewer. All SQL lives in `storage/repository.py`, so moving to Postgres is a driver swap. Schema additions are applied in place (`Database._migrate`).
3. **Poll-tail over inotify.** A 200 ms `readline` loop with a byte offset and a partial-line buffer is portable (Docker, WSL, macOS, Windows) and plenty at this scale.
   Checkpoint = `(inode, offset)`; truncation / rotation reopen from 0.
4. **Live AWS, redundant by design, behind one `AlertSink` interface.** SNS (email) is the notification and CloudWatch Logs (structured JSON, one stream per UTC day) is the
   evidence trail; both are live on the demo machine (`AWS_ENABLED=true`) and return an external id (SNS `MessageId`, CloudWatch `group:stream`) that is stored and shown as
   proof of delivery. ntfy / Telegram / JSONL stay on as redundant channels. AWS is **fail-soft**: alerts are persisted before delivery, the dispatcher retries with backoff
   and a per-send timeout, boto's own retries are kept low so the dispatcher owns retry/status, and the startup credential/resource check only reports — it never blocks.
   Detection has no import path to AWS. Credentials live in a dedicated IAM user limited to two actions on two resources (`iam-policy.json`), only in a gitignored `.env`.
   Everyone else runs `AWS_ENABLED=false` plus the `moto` tests.
5. **Local-first deployment.** One container via Docker Compose is the primary target; Render or a tunnel are optional. The core system (dashboard, SQLite, JSONL) has no
   cloud dependency; AWS and the push channels are additive.
