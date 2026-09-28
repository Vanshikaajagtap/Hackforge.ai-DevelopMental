# Architecture

A modular monolith: one Python process, one asyncio event loop, five packages with one responsibility each.

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
                              │ ingestion/parser  │
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
| `app/ingestion` | `Tailer` (read new complete lines, checkpoint, rotation), `parse_line` → `LogEvent`, `IngestStats` |
| `app/detection` | `SlidingWindow`, `Baseline`, `classify` (severity table), `ServiceDetector` / `DetectionEngine` (emit `Snapshot` per tick), `AlertStateMachine` |
| `app/alerts` | `AlertSink` protocol + `render`, sinks, `Dispatcher` (retry / delivery status), `AlertManager` (persist first), `build_sinks` registry |
| `app/storage` | SQLite schema (`db.py`) and every query (`repository.py`), degrading instead of raising |
| `app/api` | REST routes, WebSocket hub |
| `app/health.py` | "monitor the monitor": HEALTHY / DEGRADED / DOWN and the numbers behind it |
| `app/generator.py` | demo log generator (scenarios, seeded, error-diffusion so the demo is reproducible) |
| `app/main.py` | `Runtime`: wiring, tasks, restart recovery, lifespan |

## Concurrency (one event loop)

`tailer.run` → `ingest_line` → bounded queue → `_consume` → engine. `_tick_loop` (1 Hz) evaluates every service, saves snapshots, runs the state machine
via the manager and broadcasts `metric.update` + `health.update`. `dispatcher.run` turns each queued delivery into its own task. `_checkpoint_loop` saves
`(inode, offset)` every 2 s and on shutdown. In demo mode a generator task appends to the log file.
Sinks that are synchronous (boto3) run in `asyncio.to_thread`; each send has a timeout so a hung sink cannot pile up.

## Design rules

1. One module = one responsibility. 2. Detection never imports sinks; sinks never block detection.
3. Every threshold lives in `config.yaml`. 4. The clock is injectable (`Runtime(settings, clock=…)`, `ServiceDetector(…, clock)`).
5. Persist alerts before attempting delivery. 6. Every I/O boundary (file, HTTP sinks, DB) fails without crashing the pipeline.

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
