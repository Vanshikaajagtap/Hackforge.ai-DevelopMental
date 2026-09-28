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

## Real-dataset mode (NASA HTTP log, Jul 1995)

Full measurements and the tuning sweep are in [DATASET_ANALYSIS.md](DATASET_ANALYSIS.md). The traffic is real; the error definition,
service mapping and every threshold below are **choices**, not properties of the data.

6. **Common Log Format is a first-class input, selected by config.** `ingestion.format: ndjson | clf | auto` (auto: a line starting with `{` is
   NDJSON, anything else CLF). The CLF parser accepts anything structurally CLF - `-` byte counts, any `+hhmm` zone, URLs with spaces, binary
   junk in the method position, non-ASCII bytes - because the server really logged those requests; a line with no usable structure (the real
   truncated last line) raises `ParseError` and is counted in `parse_errors`, never fatal. Mapping is config-driven: **service** = first URL
   segment if it is one of the top-N prefixes in `mapping.service_prefixes` (chosen by the analysis script, N = 8, 98 % of traffic), else `other`;
   **level** = ERROR for 5xx, WARN for 4xx, INFO otherwise; **is_error** = status ≥ the minimum implied by `detector.error_definition`
   (`5xx` | `4xx+5xx` | a number). The NASA profile uses `4xx+5xx` because only 76 of 1.9 M lines are 5xx (0.004 %) whereas 4xx+5xx is 0.58 %.
   The definition is applied by the parser, so the detector stays format-agnostic.
7. **The `nasa` profile's thresholds are derived from the data, and the reasoning is documented, including what it cannot do.** The detector's
   baseline excludes anomalous windows, so its σ is tiny (calm windows sit at 0-0.5 %) and z-scores are inflated; z therefore cannot tell a routine
   burst from an incident, and the absolute floors must. The noisiest coherent service (`history`) has a 10.5 % p99 and a 19.5 % p99.9 over
   30-minute windows, so floors of **12 % / 20 % / 30 %** (medium / high / critical), `min_events 40`, ratio floors equal to the rate floors
   (÷ the 0.5 % `ratio_floor`), `warmup_ceiling 5 %`, `std_floor 1 pp` and `resolve_ratio 3` were chosen by sweeping candidates through the real
   detector over the whole month (137 alerts with floors of 3 %/6 %/12 %; 11 with the chosen ones, all genuine bursts; the same 11 at `min_events`
   30 and 40). Known limit: one global set of floors cannot suit every service - moderate spikes in `shuttle` (5-8 % against 0.2 %) are not alerted;
   per-service floors would fix this and are not built. Profiles carry `overrides:` that are deep-merged over the top-level sections, so `demo`
   and `prod` are untouched.
8. **Replay is a first-class, safety-checked feature, not a shell loop.** The replayer appends the real log to the file the tailer watches with
   timestamps rewritten to "now": original inter-arrival gaps are divided by `--speed` (original seconds per wall second), same-second requests
   are spread across their second with a *seeded* RNG (same seed → identical replay), and the original timestamp is kept in the line
   (`orig_ts="…"` for CLF, `orig_timestamp` for NDJSON). A silent gap longer than `max_gap_seconds` (wall, after scaling) is capped - and the cap
   must exceed the detector window so the window still empties and `LOW_DATA` shows briefly. The time-scale is explicit: at speed 180 a 10 s
   window is 30 original minutes. **AWS safety:** `replay.send_to_aws: false` - while any replay runs (in-process, or an external script announcing
   itself through a heartbeat file) SNS/CloudWatch deliveries are not even created; only the single preset named in `replay.aws_preset` may opt in,
   the run must ask, and the app enforces `aws_max_sends_per_run` (shared by both AWS sinks). The server, not the client, decides; ntfy, Telegram,
   console, JSONL and the dashboard are never limited; detection has no dependency on any of it. Tests and the analysis scripts cannot reach AWS
   (the end-to-end tests fail if a boto3 client is even created).

## Engineering and submission decisions

9. **Requirements are executable, not just documented.** `scripts/verify_requirements.py` runs the eight problem-statement requirements against the real code and prints
   PASS/FAIL with evidence; it is itself part of the test suite, so the README's traceability table cannot drift from the code. Requirement 8 is verified against `moto`
   (an in-process AWS mock) and the docs say so plainly: live AWS delivery is a manual checklist, never claimed as tested.
10. **Time and I/O are injected; failure paths are tested, not assumed.** A fake clock drives the detector and the whole pipeline, small real fixtures stand in for the 200 MB dataset,
    and the failure paths (rotation, cancelled sends, dead WebSocket clients, a crashing tick, level shift, a crashing replay) have their own tests. Coverage is measured, not asserted.
11. **One `.env.example` for both ways of running.** Local paths are relative (`data/...`); `docker-compose.yml` overrides them with `/data/...` in the container, so the same file
    gives a working 3-command start with or without Docker. Secrets are never in the repository: `.env` is git-ignored and only placeholders are committed.
12. **Provenance is documented.** `ACKNOWLEDGEMENTS.md` credits the NASA-HTTP dataset and every dependency (licenses read from package metadata) and flags the files that contain
    third-party or specification-derived material, so a reviewer can check them.
13. **Ship the dataset compressed, unpack on first use.** GitHub rejects any file over 100 MB and the raw NASA log is 205 MB; the archive's own gzip is 19.5 MB. Only that `.gz` is committed
    (the raw file stays git-ignored), the loader unpacks it once, atomically (temp file then rename, so a crash or two processes at once can never expose a half-written file), and a test pins the
    decompressed SHA-256 so a corrupted or altered copy is caught. Alternatives considered: Git LFS (extra tooling and a storage quota for every clone) and asking every user to download it
    (a manual step that the README steps would depend on). Redistribution terms of the archive still need a human check (ACKNOWLEDGEMENTS.md).
