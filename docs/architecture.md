# Architecture

A modular monolith: one Python process, one asyncio event loop, five packages with one responsibility each
(`ingestion`, `detection`, `alerts`, `storage`, `api`). The one-page overview and the requirement-to-code map are in the [README](../README.md).

```
 generator / real app ──append──▶  /data/app.log
                                        │
                              ┌─────────▼─────────┐
                              │ Tailer            │  byte offset, partial-line buffer,
                              │ ingestion/tailer  │  (inode, offset) checkpoint, rotation
                              └─────────┬─────────┘
                                        │ raw lines
                              ┌─────────▼─────────┐
                              │ Parser            │──▶ parse_errors++ (+ last 20 samples)
                              │ ndjson | clf | auto│  (ingestion/parser.py, clf.py, factory.py)
                              └─────────┬─────────┘
                                        │ LogEvent   asyncio.Queue(maxsize=queue_max), drop-oldest
                       ┌────────────────▼─────────────────┐
                       │ DetectionEngine  (per service)   │  detection/detector.py
                       │  SlidingWindow → Baseline →      │  window.py, baseline.py, severity.py
                       │  gates → severity → Snapshot     │
                       └───────┬───────────┬──────────────┘
                     Snapshot  │           │ Snapshot (each tick)
                               │           ▼
                               │     AlertStateMachine        detection/state.py  (pure logic)
                               │     confirm · escalate · hysteresis · dedup · level shift
                               │           │ Transition
                               ▼           ▼
                        WebSocket hub   AlertManager           alerts/manager.py
                        (api/websocket)    │ 1. persist alert + PENDING deliveries (SQLite)
                               │           │ 2. enqueue
                               ▼           ▼
                          Dashboard    Dispatcher (retry, backoff) ─▶ console | jsonl | ntfy | telegram | webhook | [sns | cloudwatch]
```

## Modules

| Package | Responsibility |
|---|---|
| `app/ingestion` | `Tailer` (read new complete lines, checkpoint, rotation), `parse_line` (NDJSON) and `ClfParser` (Common Log Format) → `LogEvent`, `build_parser` (`ndjson | clf | auto`), `IngestStats` |
| `app/detection` | `SlidingWindow`, `Baseline`, `classify` (severity table), `ServiceDetector` / `DetectionEngine` (emit `Snapshot` per tick), `AlertStateMachine` |
| `app/alerts` | `AlertSink` protocol + `render` / `alert_payload`, sinks (console, JSONL, ntfy, Telegram, webhook, SNS, CloudWatch), `Dispatcher` (retry, backoff, timeout, external id), `AlertManager` (persist first), `build_sinks` registry, `aws_common` (boto config + fail-soft startup check) |
| `app/storage` | SQLite schema (`db.py`) and every query (`repository.py`), degrading instead of raising |
| `app/api` | REST routes, WebSocket hub |
| `app/health.py` | "monitor the monitor": HEALTHY / DEGRADED / DOWN and the numbers behind it |
| `app/generator.py` | demo log generator (scenarios, seeded, error-diffusion so the demo is reproducible) |
| `app/replay.py` | dataset replayer (`timeline`, `Replayer`), `ReplayGuard` (AWS off during replays), `ReplayController`, `simulate()` |
| `app/config.py` | typed settings: `config.yaml` (with profile `overrides:`) + environment |
| `app/main.py` | `Runtime`: wiring, tasks, restart recovery, lifespan |
| `scripts/` | `generate_logs`, `replay_dataset`, `analyze_dataset`, `verify_requirements`, `benchmark` |

## Concurrency (one event loop)

`tailer.run` → `ingest_line` → bounded queue → `_consume` → engine. `_tick_loop` (1 Hz) evaluates every service, saves snapshots, runs the state machine
via the manager and broadcasts `metric.update` + `health.update`. `dispatcher.run` turns each queued delivery into its own task. `_checkpoint_loop` saves
`(inode, offset)` every 2 s and on shutdown. In demo mode a generator task appends to the log file.
Sinks that are synchronous (boto3) run in `asyncio.to_thread`; each send has a timeout so a hung sink cannot pile up.
When AWS sinks exist, a background task runs the fail-soft credential/resource check once. A dataset replay (`ReplayController`) is one more task; the
tick loop also polls the replay heartbeat file so an external replay script is recognised.

## Design rules

1. One module = one responsibility. 2. Detection never imports sinks; sinks never block detection.
3. Every threshold lives in `config.yaml`. 4. The clock is injectable (`Runtime(settings, clock=…)`, `ServiceDetector(…, clock)`).
5. Persist alerts before attempting delivery. 6. Every I/O boundary (file, HTTP sinks, DB) fails without crashing the pipeline.

## Failure isolation

| If this fails… | …this still works | How |
|---|---|---|
| a sink (SNS, CloudWatch, ntfy, Telegram, webhook) | detection, the dashboard, every other sink | alert and `PENDING` rows are persisted first; each delivery is its own task with retry, backoff and a timeout; status shows as `FAILED` on the dashboard |
| AWS entirely (bad key, wrong region, no network) | everything except the two AWS channels | startup check is fail-soft and only reports; boto's own retries are kept low so the dispatcher owns retry and status |
| the WebSocket client | the pipeline | dead clients are dropped on broadcast |
| the database | ingestion and detection | the repository degrades (logs, flips `healthy`) instead of raising; health goes `DOWN` |
| one event / one tick | the loops | the consumer and the tick loop catch and log, then continue |
| a bad log line | the pipeline | `ParseError` is counted and sampled (last 20), never raised further |
| a replay | the app | the replay task is isolated; on failure the AWS guard and the synthetic generator are released |
| a long replay | your inbox / CloudWatch bill | SNS and CloudWatch deliveries are not created during a replay unless one named preset opts in, with a hard per-run cap |

## Restart recovery

1. Open the DB, load the checkpoint → the tailer seeks to it if the inode matches and `offset ≤ size` (otherwise `start_at`: `end` skips old junk, `checkpoint` reads from 0).
2. Rebuild each service's baseline from stored snapshots the live detector would have admitted (NORMAL, or populated WARMUP under the ceiling), never from periods when an alert was open.
3. Reload `OPEN` alerts into the state machine (no duplicate "created") and re-enqueue `PENDING` deliveries.
4. Events older than the window cutoff are dropped and counted as `late_events`.

## Data lifecycle

`RAW` app.log (source of truth) → `NORMALIZED` LogEvent (transient) → `HOT` per-service deque → `AGGREGATED` SQLite (snapshots pruned after `snapshot_retention_hours`).
The database is not a second copy of every log line.

## Scale path (not built)

collector → Kafka / Kinesis → detector workers partitioned by service → aggregate store → alert service → SNS / ntfy / PagerDuty. The detector, state machine and
sink interface are unchanged; only the source of `LogEvent`s and the store move.

## Real-dataset mode (NASA HTTP log)

```
data/datasets/NASA_access_log_Jul95 (205 MB, never loaded whole, git-ignored)
        │  find_offset() binary search by time  →  iter_segment(start, end)
        ▼
  timeline():  original gaps ÷ speed · long silences capped · seeded sub-second spread
        ▼
  Replayer  (scripts/replay_dataset.py  ·  ReplayController via POST /api/demo/replay)
        │  rewrites [timestamp] to "now", keeps orig_ts="…"   ── heartbeat file ──▶ ReplayGuard.poll_file()
        ▼                                                                                │
   data/app.log ──▶ Tailer ──▶ build_parser(): ndjson | clf | auto ──▶ queue ──▶ engine ──▶ state machine ──▶ AlertManager
                                  └ ClfParser + ServiceMapper (config: mapping, detector.error_definition)       │ channel_permit()
                                                                                                                  ▼
                                                                         ReplayGuard: SNS / CloudWatch OFF during a replay
                                                                         (one opt-in preset, per-run cap); other sinks unaffected
```

`simulate()` (in `app/replay.py`) runs the same parser → engine → state machine on a virtual clock; `scripts/analyze_dataset.py --simulate`
and the end-to-end tests use it, so tuning and tests exercise the real detector, not a re-implementation.
Profiles can carry `overrides:` (deep-merged over the top-level config), which is how `LOGPULSE_PROFILE=nasa` switches parser, error
definition, service mapping, thresholds and generator without touching `demo` / `prod`.

## Quality gates

`python -m pytest` (fake clock, small real fixtures, no network; AWS via `moto`), `python -m ruff check app scripts tests`, and
`python scripts/verify_requirements.py`, which runs the eight problem-statement requirements against the real code. Measured results are recorded in
[TEST_REPORT.md](TEST_REPORT.md).
