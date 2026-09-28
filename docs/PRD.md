# PRD — Real-Time Log Anomaly Detector with Alert Feed

**Working name:** LogPulse
**Problem:** Python — Real-Time Log Anomaly Detector with Alert Feed
**Difficulty:** Hard
**Team:** 5
**Build window:** ≤ 4 hours
**Budget:** ₹0 — an AWS account is now available; usage is tiny and stays inside the free allowances (verify in the Billing console and keep a budget alert)
**Primary judging signal:** Engineering mindset, architecture, implementation quality, scalability, deployment/data handling
**Repo:** https://github.com/Vanshikaajagtap/Hackforge.ai-DevelopMental.git (empty — scaffold in §60)

> **Revision note — AWS integration.** The team now has an AWS account, so the last minimum requirement ("push alerts to AWS CloudWatch Logs or SNS") is made **live**
> instead of mock-verified. Sections changed: §4, §4.1, §9, §26, §31–§34, §36, §37–§38, §41, §45, §49–§51, §57.3, §62, §64–§67, §69–§71, §73 and the demo timeline (§44).
> The free channels (ntfy / Telegram / JSONL) stay as **redundant** channels. Detection never depends on AWS.

---

## 1. Executive Product Definition

### 1.1 Product statement

LogPulse is a lightweight, real-time observability system that continuously watches an append-only application log file, converts incoming records into normalized events, maintains a per-service sliding window of recent error behavior, learns a **poison-resistant** baseline of normal behavior, detects statistically and operationally significant deviations, assigns severity, manages the incident lifecycle (open → escalate → resolve), and delivers explainable alerts through a live WebSocket dashboard, **AWS SNS (email) and AWS CloudWatch Logs (structured evidence trail)**, with ntfy / Telegram / a structured alert log as redundant channels — all behind a pluggable sink interface.

The system is intentionally **statistical and explainable**, not ML-heavy.

### 1.2 The two questions it answers

Continuously:

> "Is the error behavior of this service abnormal compared with its recent normal behavior?"

And when it is:

> "How abnormal is it, why did we flag it, how severe is it, and did the alert actually reach someone?"

---

## 2. Why This Problem Is Harder Than It Looks

At first glance:

```
read file → count errors → compare threshold → show red alert
```

That works — and looks like a beginner project.

The engineering is in the chain of edge cases:

```
growing file
    ↓ partial writes
    ↓ malformed lines
    ↓ event normalization
    ↓ rolling state (time-based eviction, silence)
    ↓ baseline sampling (overlap → false confidence)
    ↓ baseline contamination (incident becomes "normal")
    ↓ low-volume false positives (1 of 1 = 100 %)
    ↓ noisy statistics (tiny σ → huge z)
    ↓ severity policy
    ↓ alert flapping / alert storms
    ↓ notification failure (including cloud outages)
    ↓ restart / recovery
    ↓ deployment without a paid cloud
```

That chain is where LogPulse differentiates itself.

---

## 3. Research Findings — Architectural Patterns

(Carried from v1; these justify the design.)

| Pattern | What it is | Lesson for us |
|---|---|---|
| **A — Simple real-time detector** | Sidecar Python daemon tails logs, per-key `deque` sliding windows, rolling baseline, z-score / rate multiplier, small dashboard | A deque window + statistical baseline is defensible when designed carefully |
| **B — Kafka + ML + PostgreSQL + dashboard** | Producer → Kafka → ML consumer → Postgres → dashboard, Isolation Forest, Docker Compose, health checks | Decoupling and persistence are production patterns — but reproducing the stack in 4 h is a bad trade |
| **C — Large distributed** | Loki → Kafka → Flink → inference cluster; or ingestion → Kafka → processor → Elasticsearch → React on Kubernetes | Solves throughput, multi-source, replay, horizontal scale. Show you understand it; don't build it |
| **D — Statistical engineering** | Compares fixed thresholds, rolling z-scores, SPC; past-only baselines; anomalies not admitted into the baseline | The most relevant finding: **baseline contamination** |

Contamination example:

```
Normal:         5 % error rate
Incident:      35 %
Bad baseline:   5 → 8 → 12 → 17 → 22 → 28 %
```

The detector slowly teaches itself the outage is normal. Serious design flaw; LogPulse prevents it (§21).

---

## 4. Constraint Research — AWS Access and the Free Channels

The problem statement says "Push alerts to AWS CloudWatch Logs **or** SNS". **Status: resolved — the team now has an AWS account, so both are implemented live** (SNS = the notification, CloudWatch Logs = the evidence trail). The earlier no-account research is kept below because those channels remain valuable as **redundant fallbacks** (venue Wi-Fi, expired keys, wrong region).

| Option | Finding | Decision now |
|---|---|---|
| **AWS SNS + CloudWatch Logs** | New accounts get a credit-based free plan (up to $100–$200 for up to 6 months), and SNS and CloudWatch Logs both have always-free allowances. Signup needs a credit card. Terms change — verify in the Billing console | **Live, primary.** Dedicated least-privilege IAM user; budget alert; 7-day log retention (§36) |
| **LocalStack** | Since March 2026 the image requires an auth token from a free LocalStack account | Skip |
| **moto** (Python AWS mock) | Free, offline, unit-test friendly | **Use it** for automated sink tests (no credentials needed by anyone) |
| **ntfy.sh** | HTTP pub-sub push service; publish with a plain POST; no signup; topics are public-by-name, so the topic acts as a password. Community reports cite ~250 messages/day on the free hosted plan (verify) | **Redundant channel #1** |
| **Telegram Bot API** | Free bot via BotFather. Bots are throttled at roughly 30 msg/s overall and ~20 msg/min per group | **Redundant channel #2** |
| **Discord / Slack webhook** | Free incoming webhooks | Optional |
| **Render free web service** | Spins down after 15 min with no incoming HTTP request or incoming WebSocket message; 30–60 s cold start; ephemeral filesystem | **Optional public URL**; client sends WS ping every 60 s |
| **Laptop + Docker Compose** | Free, no cold starts, no dependency on venue network for the core system | **Primary demo environment** |

### 4.1 The honest framing

Alerts are delivered **live to AWS SNS (email) and AWS CloudWatch Logs**; ntfy, Telegram and a JSONL file are redundant channels behind the same `AlertSink` interface. If AWS is unreachable or mis-configured, alerts are still persisted first, retried with backoff, shown as failed on the dashboard, and delivered on the other channels — detection is never affected. Every successful AWS delivery stores an external id (SNS `MessageId`, CloudWatch `group:stream`) as proof.

---

## 5. Product Goal

### Primary goal

Detect abnormal changes in error behavior from a continuously growing log file in near real time, and expose those anomalies as actionable, explainable alerts.

### Secondary goals

- Clean separation of ingestion, detection, persistence, notification
- Fault tolerance (bad input, slow/failed sinks, restarts)
- Alert-fatigue control (dedup, hysteresis)
- Baseline contamination protection
- Restart recovery
- Reproducible deployment; free-tier friendly
- Observable system health ("monitor the monitor")
- Deterministic, testable behavior

---

## 6. Non-Goals

| Feature | Decision |
|---|---|
| Deep learning / LLM analysis | No |
| Kafka / Zookeeper / Redis | No |
| Kubernetes / ECS / EC2 | No |
| PostgreSQL / Elasticsearch | No (SQLite) |
| Log search engine | No |
| Multi-tenant auth / user management | No |
| Microservices | No |
| React / build toolchain | No |
| SMS delivery | No (email is the safe SNS channel) |

Knowing what not to build is part of the signal.

---

## 7. Core User

**Application / SRE / DevOps engineer.**

> "Is this application behaving abnormally right now, and do I need to investigate?"

So the dashboard is an **incident detection console**, not a log viewer.

---

## 8. Core User Journey

```
Application writes logs
        ↓
file grows
        ↓
LogPulse tails new complete lines
        ↓
parse + normalize
        ↓
update per-service sliding window
        ↓
every tick: compute error rate
        ↓
compare with past-only baseline (z, ratio, absolute gates)
        ↓
anomaly?
   ┌────┴────┐
   NO       YES
   │          │
sample into   severity → alert state machine
baseline         ↓
             persist alert (SQLite)
                ↓
        ┌───────┴────────┐
        ↓                ↓
    WebSocket       dispatcher → SNS / CloudWatch / ntfy / Telegram / JSONL
        ↓
    dashboard
```

---

## 9. Requirement Traceability

| Problem-statement requirement | LogPulse feature | Implementation | Acceptance test |
|---|---|---|---|
| Monitor a continuously growing log file | Poll-tailer with offset + partial-line buffer + checkpoint | `asyncio` readline loop | Append lines while running → events seen without restart |
| Rolling error rates using a sliding window | Time-based deque per service | `collections.deque` | Old events leave window; rate = errors/total |
| Establish a baseline for normal behavior | Past-only, fixed-cadence sampled, contamination-guarded baseline | `statistics.mean/pstdev` | Anomalous windows never enter baseline |
| Detect deviations from the baseline | z-score + ratio + absolute gates | Pure Python | Spike detected; traffic-only spike not |
| Assign severity levels | Policy table MEDIUM/HIGH/CRITICAL in config | `config.yaml` | Known inputs → expected severities |
| Real-time frontend (WebSockets or polling) | WebSocket push dashboard | FastAPI WS + one HTML page | Alert appears without refresh |
| Display alerts as they are generated | Live alert feed with evidence + delivery badges | Chart.js (vendored) | Alert visible < 2 s after threshold crossing |
| Push alerts to AWS CloudWatch Logs or SNS | `AlertSink` interface: **SNS and CloudWatch sinks live**, plus redundant ntfy/Telegram/JSONL | boto3 + moto tests; httpx | **Email received (SNS) and a JSON event present in the CloudWatch log group**; sink tests pass |

---

## 10. Architecture

### 10.1 MVP architecture

```
                     ┌─────────────────────┐
                     │   Append-only Log   │
                     │   /data/app.log     │
                     └──────────┬──────────┘
                                │
                                ▼
                     ┌─────────────────────┐
                     │     File Tailer     │
                     │ offset + checkpoint │
                     └──────────┬──────────┘
                                │ raw lines
                                ▼
                     ┌─────────────────────┐
                     │ Parser / Normalizer │──▶ parse_errors++
                     └──────────┬──────────┘
                                │ LogEvent
                                ▼
                        [ bounded queue ]
                                │
                                ▼
              ┌─────────────────────────────────┐
              │   Detection Engine (per service)│
              │                                 │
              │  Sliding Window (deque)         │
              │       ↓                         │
              │  Error Rate                     │
              │       ↓                         │
              │  Baseline Engine (past-only)    │
              │       ↓                         │
              │  Deviation: z, ratio, gates     │
              │       ↓                         │
              │  Severity Engine                │
              └───────────┬─────────┬───────────┘
                          │         │
                metric.update       │ anomaly evidence
                          │         ▼
                          │   ┌───────────────────┐
                          │   │   Alert Manager   │
                          │   │ dedup / hysteresis│
                          │   └─────────┬─────────┘
                          │             │ persist first
                          │             ▼
                          │      ┌─────────────┐
                          │      │   SQLite    │
                          │      └──────┬──────┘
                          │             │
                          │             ▼
                          │      ┌─────────────┐
                          │      │ Dispatcher  │ retry + timeout + delivery status + external id
                          │      └──┬──┬──┬──┬─┘
                          │         │  │  │  │
                          │       SNS  CW ntfy Telegram JSONL
                          ▼
                 ┌───────────────────┐
                 │ WebSocket Manager │
                 └─────────┬─────────┘
                           ▼
                 ┌───────────────────┐
                 │  Live Dashboard   │
                 └───────────────────┘
```

### 10.2 Key design rules

1. One module = one responsibility.
2. Detection never imports sinks; sinks never block detection.
3. Every threshold lives in `config.yaml`.
4. The clock is injectable (deterministic tests, replay).
5. Persist alerts **before** attempting delivery.
6. All I/O boundaries (file, HTTP sinks, AWS, DB) fail without crashing the pipeline.

---

## 11. Critical Architectural Decision: Modular Monolith

One Python application, one container:

```
ingestion/   detection/   alerts/   storage/   api/
```

Benefits: development speed, clean boundaries, trivial deployment, obvious scale path (each module can become a worker later).

Five containers are not "more production" — they are more failure surface in four hours.

---

## 12. Concurrency Model

Everything runs in one `asyncio` event loop:

```
Tailer task ──▶ asyncio.Queue(maxsize=10_000) ──▶ Detection task
                                                    │
Tick task (every 1 s) ──────────────────────────────┤
                                                    ▼
                                         AlertManager → DB write
                                                    │
                                         Dispatcher task (queue) → sinks (httpx async / boto3 in thread)
WebSocket hub: broadcast on every snapshot / alert
AWS startup check: background task, fail-soft
```

- File reading is decoupled from processing by the queue — a slow DB/sink never stops reading.
- boto3 is synchronous → call via `asyncio.to_thread`.
- SQLite writes are small; use one connection with WAL + a lock, or run writes via `to_thread`.

### 12.1 Backpressure

If `queue_depth ≥ 80 %` of max → health = `DEGRADED`. If full → drop oldest, count `dropped_events`, surface in health panel. (At scale this queue becomes Kafka/Kinesis.)

---

## 13. Data Model & Lifecycle

```
RAW          /data/app.log              append-only, source of truth
NORMALIZED   LogEvent objects           transient (queue / window)
HOT          per-service deque          in-memory rolling window
AGGREGATED   SQLite                     durable: snapshots, alerts, deliveries, checkpoint
```

The database is **not** a second copy of every log line. It stores compact aggregates only.

---

## 14. Input Data Contract

Newline-delimited JSON (one object per line):

```json
{"timestamp":"2026-09-28T12:10:33.127Z","service":"payment-service","level":"ERROR","status":500,"message":"Payment gateway timeout","request_id":"req-83ad9"}
{"timestamp":"2026-09-28T12:10:34.127Z","service":"payment-service","level":"INFO","status":200,"message":"Payment completed","request_id":"req-83ae0"}
```

Required fields: `timestamp`, `service`, `level`. Optional: `status`, `message`, `request_id`. Missing required field → parse error.

---

## 15. Normalized Event Schema

```python
@dataclass(frozen=True)
class LogEvent:
    ts: float                 # epoch seconds
    service: str
    level: str                # upper-cased
    status: int | None
    message: str
    request_id: str | None
    is_error: bool            # derived
```

`is_error` is derived, not trusted from one field:

- `level ∈ {ERROR, CRITICAL, FATAL}` → error
- `500 ≤ status ≤ 599` → error

Timestamps: ISO-8601 (`datetime.fromisoformat` handles `Z` in Python 3.12).

---

## 16. File Tailer Design

Poll-based (no inotify/watchdog — simpler, portable, sufficient):

```python
while running:
    chunk = f.readline()
    if not chunk:
        await asyncio.sleep(0.2)
        continue
    buffer += chunk
    if buffer.endswith("\n"):
        emit(buffer); buffer = ""
```

### 16.1 Read only new lines
Track `inode` and `offset`; never reread the file.

### 16.2 Partial writes
Do not parse a line until its `\n` arrives; keep the fragment in a buffer.

### 16.3 Checkpoint
Every 2 s and on shutdown, write `(source, inode, offset, updated_at)` to SQLite. On start:

- inode matches and `offset ≤ size` → seek to offset
- otherwise → start at 0

Config `start_at: checkpoint | end` controls first-run behavior (demo default `end` so old junk isn't replayed).

### 16.4 Rotation / truncation (P2)
If `st_ino` changes or `size < offset` → reopen and read from 0. Finish reading the old handle first if still open.

### 16.5 Environment note
Develop and run inside Docker/WSL — inode semantics differ on Windows.

---

## 17. Parser Design

```
line → json.loads → validate required fields → parse timestamp → LogEvent
```

Any failure:

```
parse failure → parse_errors++ → keep last 20 samples for health panel → skip → continue
```

The pipeline never crashes on bad input. Late/out-of-order events older than the window cutoff are dropped and counted as `late_events`.

---

## 18. Sliding Window Algorithm

Per service: `deque[(ts, is_error)]` with running `total` and `errors`.

```python
class SlidingWindow:
    def __init__(self, seconds):
        self.w = seconds
        self.q = deque()
        self.total = 0
        self.errors = 0

    def add(self, ts, is_error):
        self.q.append((ts, is_error))
        self.total += 1
        self.errors += int(is_error)

    def evict(self, now):
        cutoff = now - self.w
        while self.q and self.q[0][0] < cutoff:
            _, e = self.q.popleft()
            self.total -= 1
            self.errors -= int(e)

    @property
    def error_rate(self):
        return self.errors / self.total if self.total else 0.0
```

Each event is inserted and evicted once → O(1) amortized. `evict` also runs on every tick with the clock, so a silent service decays to `LOW_DATA` rather than freezing on stale numbers.

### 18.1 Why not fixed one-minute buckets?

A burst at 12:00:58–12:01:01 gets split across two buckets (2 + 2 errors) and looks mild; a true sliding window sees 4 errors in 4 seconds. Time-based sliding windows track real behavior.

---

## 19. Baseline Design

Use a **past-only** rolling baseline, sampled on a **fixed cadence** — not per event.

### 19.1 Why fixed cadence matters (correction to v1)

If you sample the baseline every time the window changes, consecutive samples come from almost the same set of events. They are nearly identical, so σ collapses toward zero and a small wobble looks like a huge z-score. Sampling every `baseline_sample_every` seconds (≥ a fraction of the window) gives more independent samples.

### 19.2 Procedure

Every `baseline_sample_every` seconds, per service:

```
if window.total ≥ min_events
   and current window is NOT anomalous
   and (baseline is mature OR rate ≤ warmup_ceiling):
       samples.append(error_rate)          # deque(maxlen=baseline_max_samples)
```

Then:

```
μ = mean(samples)
σ = pstdev(samples)
z = (current_rate − μ) / max(σ, std_floor)
ratio = current_rate / max(μ, 0.005)
```

`std_floor` (default 0.02 = 2 percentage points) prevents a perfectly steady baseline from making every wiggle "infinitely anomalous".

---

## 20. Baseline Contamination Protection

```
current window
      ↓
   evaluate
      ↓
 normal ─────────────► admitted to baseline
      │
 anomaly ────────────► NOT admitted
```

Also:

- **Warm-up ceiling:** during warm-up, a sample above `warmup_ceiling` (0.25) is not admitted, so an incident during startup can't define "normal".
- **Frozen while OPEN:** no samples are admitted while an alert for that service is open.

Headline line for judges:

> "The anomalous windows were excluded from the baseline so the detector cannot learn an incident as normal."

### 20.1 The other side: level shift (P2)
If traffic genuinely changes permanently, the baseline would never adapt and the alert would never resolve. After `level_shift_seconds` OPEN, LogPulse emits a "sustained level shift" note, resolves the alert, clears the sample buffer, and relearns from the current level (warm-up ceiling waived once for this explicit adoption).

---

## 21. Warm-Up State

```
samples < min_baseline_samples  →  WARMUP
```

During warm-up: current rate shown, baseline shown as "learning", **alerts suppressed**. Prevents garbage alerts immediately after startup.

Demo profile: 10 samples × 2 s = ~20 s warm-up. Prod profile: 10 samples × 10 s ≈ 100 s.

---

## 22. Low-Volume Protection

```
window.total < min_events  →  LOW_DATA
```

1 request / 1 error = 100 % is not an outage. In `LOW_DATA` no anomaly evaluation happens and no baseline sample is admitted.

---

## 23. Deviation Logic — Three Signals + Gates

| Signal | Purpose |
|---|---|
| **z-score** | Statistical surprise vs. history |
| **ratio** (`current / baseline`) | Relative change; robust when σ is tiny |
| **absolute rate** and **error count** | Operational relevance: 6 % is not 42 % |

An anomaly requires the severity table's gates (§25) to hold **together**. This prevents "z = 9 because σ was tiny and rate went 2 % → 4 %."

---

## 24. Anomaly Evidence Output

```json
{
  "is_anomaly": true,
  "service": "payment-service",
  "current_error_rate": 0.287,
  "baseline_error_rate": 0.051,
  "baseline_std": 0.012,
  "relative_change": 5.62,
  "z_score": 4.87,
  "sample_size": 184,
  "errors": 53,
  "window_seconds": 10,
  "reason": "Error rate 5.6× baseline with z=4.87 over 184 events"
}
```

Explainable evidence, not `{"anomaly": true}`.

---

## 25. Severity Engine

Policy-driven table; the **highest** row whose conditions are met wins.

| Severity | z ≥ | and (rate ≥ **or** ratio ≥) | and errors ≥ |
|---|---|---|---|
| NORMAL | — | — | — |
| MEDIUM | 2 | 0.10 or 1.5× | 5 |
| HIGH | 3 | 0.15 or 3× | 5 |
| CRITICAL | 4 | 0.25 or 5× | 10 |

All numbers live in `config.yaml`.

---

## 26. Configuration

```yaml
profile: demo                      # demo | prod

profiles:
  demo:
    window_seconds: 10
    tick_seconds: 1
    baseline_sample_every: 2
    baseline_max_samples: 30
    min_baseline_samples: 10
    min_events: 30
    level_shift_seconds: 120
  prod:
    window_seconds: 60
    tick_seconds: 1
    baseline_sample_every: 10
    baseline_max_samples: 30
    min_baseline_samples: 10
    min_events: 20
    level_shift_seconds: 900

detector:
  std_floor: 0.02
  warmup_ceiling: 0.25
  confirm_ticks: 2
  resolve_ticks: 3
  resolve_z: 1.5
  resolve_ratio: 1.5

severity:
  medium:   {z: 2.0, rate: 0.10, ratio: 1.5, errors: 5}
  high:     {z: 3.0, rate: 0.15, ratio: 3.0, errors: 5}
  critical: {z: 4.0, rate: 0.25, ratio: 5.0, errors: 10}

alerts:
  retry_attempts: 3
  retry_backoff_seconds: 2
  sink_timeout_seconds: 10
  # sns / cloudwatch are only built when AWS_ENABLED=true AND their env vars are set
  sinks: [console, jsonl, sns, cloudwatch, ntfy, telegram]

ingestion:
  start_at: end                               # end | checkpoint
  queue_max: 10000
  checkpoint_every_seconds: 2
```

Environment variables (secrets/paths) come from `.env`; behavior thresholds from YAML. Bad:

```python
if z > 3:   # scattered everywhere
```

Good: one config object, tunable per environment.

---

## 27. Alert State Machine

```
NORMAL
  │  severity ≥ MEDIUM for confirm_ticks (or CRITICAL immediately)
  ▼
OPEN  ──(severity rises)──▶ OPEN (escalated, re-notify)
  │  resolve conditions hold for resolve_ticks
  ▼
RESOLVED ──▶ NORMAL
```

Per `dedup_key = service + ":error_rate"`. The alert keeps its **peak severity** and shows the **current** severity separately.

Pseudocode:

```python
def evaluate(self, snap):
    key = f"{snap.service}:error_rate"
    a = self.open.get(key)
    if a is None:
        if snap.severity != "NONE":
            self.pending[key] += 1
            if snap.severity == "CRITICAL" or self.pending[key] >= cfg.confirm_ticks:
                return self._open(snap)          # notify
        else:
            self.pending[key] = 0
    else:
        if rank(snap.severity) > rank(a.peak_severity):
            return self._escalate(a, snap)       # notify
        if snap.z < cfg.resolve_z and snap.ratio < cfg.resolve_ratio:
            self.calm[key] += 1
            if self.calm[key] >= cfg.resolve_ticks:
                return self._resolve(a, snap)    # notify
        else:
            self.calm[key] = 0
```

---

## 28. Hysteresis

Different thresholds to trigger vs. resolve:

```
trigger:  severity ≥ MEDIUM (z ≥ 2 with gates)
resolve:  z < 1.5 AND ratio < 1.5 for 3 consecutive ticks
```

Without hysteresis: 3.01 → alert, 2.99 → resolve, 3.02 → alert… (flapping). With it: stays OPEN until sustained recovery.

---

## 29. Alert Deduplication

While the error rate stays at 40 %, we do **not** send a notification every tick. Notify only when:

- a new alert opens, **or**
- severity increases (escalation), **or**
- the alert resolves.

Anomaly recurring after resolve = new alert (new notification).

---

## 30. Log-Silence Detection (P2, cheap and impressive)

If a service that was active goes silent (`total == 0` for N × window while baseline volume was healthy), emit a `LOW_DATA`/"silence" health warning on the dashboard (not a paged alert). A dead service producing no errors is the classic blind spot of error-rate detectors — mention it in Q&A even if only shown as a dashboard state.

---

## 31. Alert Delivery Architecture

```python
class AlertSink(Protocol):
    name: str
    async def send(self, alert: Alert, event: Literal["created","escalated","resolved"]) -> Optional[str]:
        """Deliver the alert. Return an external id if the channel has one
        (SNS MessageId, CloudWatch group:stream). Raise on failure — the dispatcher
        handles retries and delivery status."""
```

Implementations:

```
ConsoleSink      always on (dev visibility)
JsonlSink        structured alert log (same schema as the CloudWatch message)
SnsSink          AWS — the notification (email)            ← LIVE
CloudWatchSink   AWS — the evidence trail (structured JSON) ← LIVE
NtfySink         redundant push, no signup
TelegramSink     redundant push, bot
WebhookSink      Discord/Slack (optional)
```

Sinks are chosen from `config.alerts.sinks`; AWS sinks are constructed only when `AWS_ENABLED=true` and their env vars are set (a placeholder value such as `<ACCOUNT_ID>` counts as unset). Adding a sink never touches detection.

**One payload schema** (`alert_payload`) is shared by the JSONL and CloudWatch sinks; SNS uses the human-readable `render()` text.

---

## 32. Redundant Channels — Implementation Notes

These stay on even with AWS live: if AWS credentials, region or Wi-Fi misbehave on demo day, alerts still arrive.

### 32.1 ntfy

```python
class NtfySink:
    name = "ntfy"
    def __init__(self, topic): self.url = f"https://ntfy.sh/{topic}"

    async def send(self, alert, event):
        prio = {"CRITICAL": "urgent", "HIGH": "high"}.get(alert.severity, "default")
        headers = {
            "Title": f"[{alert.severity}] {alert.service} error rate ({event})",  # ASCII only
            "Priority": prio,
            "Tags": "rotating_light" if event != "resolved" else "white_check_mark",
        }
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.post(self.url, content=render(alert, event).encode(), headers=headers)
            r.raise_for_status()
```

Use a long random topic (`logpulse-<random>`). Anyone who knows the topic can read it. Judges can open `https://ntfy.sh/<topic>` in a browser or install the app.

### 32.2 Telegram

Setup: message **@BotFather** → `/newbot` → token. Send any message to your bot, then open `https://api.telegram.org/bot<TOKEN>/getUpdates` to read your `chat_id`.

```python
class TelegramSink:
    name = "telegram"
    async def send(self, alert, event):
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.post(f"https://api.telegram.org/bot{self.token}/sendMessage",
                             json={"chat_id": self.chat_id, "text": render(alert, event)})
            r.raise_for_status()
```

### 32.3 JSONL alert log

One JSON object per alert event appended to `/data/alerts.jsonl`. The same schema is sent to CloudWatch `PutLogEvents`:

```json
{"alert_id":"a8f31","event":"created","timestamp":"2026-09-28T12:41:09Z","service":"payment-service","severity":"CRITICAL","current_error_rate":0.384,"baseline_error_rate":0.051,"z_score":5.18,"relative_change":7.5,"sample_size":284}
```

---

## 33. AWS Sinks (Live)

Constructed only when `AWS_ENABLED=true`. Shared plumbing lives in `alerts/aws_common.py`.

### 33.1 `aws_common.py`

```python
BOTO_CFG = Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=3, read_timeout=5)

def aws_session(region=None) -> boto3.Session:
    return boto3.Session(region_name=region or os.getenv("AWS_REGION") or "ap-south-1")
```

boto's own retries are kept low: **the Dispatcher owns retry/backoff and delivery status.**

### 33.2 SNS — the notification

```python
def _subject(alert, event):          # SNS email subject: single-line ASCII, < 100 chars
    s = f"[{alert.severity}] {alert.service} error rate ({event})"
    return re.sub(r"[^\x20-\x7E]", "", s)[:99]

class SnsSink:
    name = "sns"
    async def send(self, alert, event) -> str:
        def _publish():
            resp = self.client.publish(
                TopicArn=self.arn, Subject=_subject(alert, event), Message=render(alert, event),
                MessageAttributes={"severity": ..., "service": ..., "event": ...})   # for filter policies later
            return resp["MessageId"]
        return await asyncio.to_thread(_publish)

    async def healthcheck(self):     # sns:GetTopicAttributes
        await asyncio.to_thread(self.client.get_topic_attributes, TopicArn=self.arn)
```

### 33.3 CloudWatch Logs — the evidence trail

```python
class CloudWatchSink:
    name = "cloudwatch"
    def _stream_name(self):          # one stream per UTC day
        return f"{self.prefix}/{datetime.now(timezone.utc):%Y-%m-%d}"

    async def send(self, alert, event) -> str:
        def _put():
            stream = self._stream_name()
            self._ensure_stream(stream)                       # create_log_stream, AlreadyExists tolerated
            self.client.put_log_events(logGroupName=self.group, logStreamName=stream,
                logEvents=[{"timestamp": int(time.time()*1000),
                            "message": json.dumps(alert_payload(alert, event))}])
            return f"{self.group}:{stream}"
        return await asyncio.to_thread(_put)
```

The log **group** must already exist (created once at setup, §36); if it doesn't, the dispatcher retries and the delivery ends `FAILED` with a clear error. `healthcheck()` creates today's stream, which proves the group exists and is writable using only the two permissions the app holds.

### 33.4 Fail-soft startup check

```
sts.get_caller_identity  ->  "AWS credentials OK: arn:aws:iam::…:user/logpulse-app"   (no IAM permission needed)
sns.healthcheck()        ->  topic reachable
cloudwatch.healthcheck() ->  log group exists and is writable
```

Runs as a **background task**; results feed the health panel (`aws_identity`, `sns`, `cloudwatch`) and `/api/system/status`. It never blocks or crashes startup, and a failure does not affect detection or the other channels.

### 33.5 Tests with moto (no account)

```python
with mock_aws():
    arn = boto3.client("sns", region_name="ap-south-1").create_topic(Name="logpulse-alerts")["TopicArn"]
    msg_id = await SnsSink(arn, "ap-south-1").send(sample_alert(), "created")     # returns a MessageId
```

Also covered: CloudWatch JSON body and per-day stream, missing log group raises, sanitised SNS subject, healthchecks, startup-check fail-soft behaviour, and an "AWS unreachable" test proving detection and the other channels are unaffected. Live verification is the checklist in §57.3.

### 33.6 Credentials and IAM

Never commit keys. A dedicated IAM user (`logpulse-app`, no console access) with only the policy in §36.2. Keys live only in the gitignored `.env` on the demo laptop and on the person implementing the sinks; everyone else runs `AWS_ENABLED=false`. Never create access keys for the root user.

---

## 34. Notification Reliability (Outbox-Lite)

Detection must never depend on a sink:

```
detector → AlertManager → persist alert + delivery rows (PENDING) → dispatcher → sink
```

Dispatcher: for each pending delivery, `send()` under `asyncio.wait_for(…, sink_timeout_seconds)`; on exception retry with backoff (`retry_backoff_seconds × 2^attempt`) up to `retry_attempts`; then mark `FAILED` with `last_error`. On success mark `DELIVERED` and store the sink's **external id** (`external_id`). Each (alert, channel) delivery runs as its own task, so a dead SNS cannot delay ntfy. Delivery state is visible on the dashboard as badges (`sns ✓ · cloudwatch ✓ · ntfy ✗ retrying`); hovering a badge shows the external id or the last error.

For the 4-hour build: in-memory dispatcher queue + DB rows for status. On restart, `PENDING` rows are re-enqueued.

---

## 35. Alert Message Template

```
[CRITICAL] payment-service error rate 38.4%

Baseline:   5.1%   (+33.3 pp, 7.5×)
Z-score:    5.18
Events:     284 in last 10 s
Opened:     12:41:09 UTC
Reason:     Error rate exceeded baseline 7.5× with sufficient traffic
```

Every alert answers **what, where, when, current, normal, how much, evidence, sample size, severity, delivered-where**.

---

## 36. AWS Setup (Mandatory, Done Before the Build)

*(Replaces the former optional "AWS side quest".)* One person with an admin login, about 20 minutes. Pick **one region** (suggested `ap-south-1`) and use it everywhere: SNS topic, log group, IAM policy ARNs and `.env` must match. Never create access keys for the root user.

### 36.1 Console click-path

1. **SNS → Topics → Create topic** → Standard → `logpulse-alerts`. Copy the **Topic ARN**.
2. **Create subscription** → Email → one per recipient. **Each recipient must click "Confirm subscription"** (check spam) — unconfirmed subscriptions receive nothing.
3. **CloudWatch → Log groups → Create** `/logpulse/alerts` with **7-day retention** (the default never expires).
4. **IAM → Policies → Create** from `iam-policy.json` (below) → `LogPulseAlertWriter`.
5. **IAM → Users → Create** `logpulse-app`, no console access, attach only that policy → **Create access key** ("application running outside AWS"); copy once.
6. **Billing → Budgets** → small budget with your email.
7. Smoke test: `aws sns publish --topic-arn <ARN> --region ap-south-1 --subject "LogPulse test" --message hello` → email arrives.

### 36.2 Least-privilege policy (`iam-policy.json`, committed with placeholders)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "PublishAlerts", "Effect": "Allow",
      "Action": ["sns:Publish", "sns:GetTopicAttributes"],
      "Resource": "arn:aws:sns:ap-south-1:<ACCOUNT_ID>:logpulse-alerts" },
    { "Sid": "WriteAlertLogs", "Effect": "Allow",
      "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:ap-south-1:<ACCOUNT_ID>:log-group:/logpulse/alerts:*" }
  ]
}
```

The app does **not** create the log group or set retention. `sts:GetCallerIdentity` (startup check) needs no permission.

### 36.3 Configuration (`.env` on the demo laptop only; gitignored)

```
AWS_ENABLED=true
AWS_REGION=ap-south-1
AWS_ACCESS_KEY_ID=…
AWS_SECRET_ACCESS_KEY=…
SNS_TOPIC_ARN=arn:aws:sns:ap-south-1:<ACCOUNT_ID>:logpulse-alerts
CW_LOG_GROUP=/logpulse/alerts
CW_LOG_STREAM_PREFIX=alerts
```

`docker compose` forwards these through `env_file`; a local run exports the `AWS_*` values from `.env` for boto3. Never bake keys into the image.

### 36.4 Cleanup after the event

Delete the SNS topic, the log group, the access key(s), the IAM user and the policy.

---

## 37. Frontend Product Design

Single static page served by FastAPI. Monitoring command center:

```
┌─────────────────────────────────────────────────────────────┐
│ LOGPULSE                     ● HEALTHY   profile: demo      │
├────────────┬────────────┬────────────┬──────────────────────┤
│ Error Rate │ Baseline   │ Active     │ Events/s             │
│   28.7%    │ 5.1% ±1.2  │ Alerts: 2  │   15.2               │
├─────────────────────────────────────────────────────────────┤
│   Error Rate vs Baseline (last 5 min)                       │
│   current ─────────────╮                                    │
│   baseline ~ ~ ~ ~ ~ ~ │╲    [band = μ ± σ]  ▲ alert marks  │
├─────────────────────────────────────────────────────────────┤
│ ALERTS                                                      │
│ 🔴 CRITICAL payment-service 38.4% 7.5× z=5.18               │
│    sns ✓ cloudwatch ✓ ntfy ✓ tg ✓   (hover: MessageId)      │
├─────────────────────────────────────────────────────────────┤
│ Detector health: tail lag 0.2 s · parse errors 2 · queue 3  │
│ sinks: sns ✓ cloudwatch ✓ ntfy ✓ jsonl ✓ · DB ✓ · file ✓   │
│ AWS: identity ✓ · SNS ✓ · CloudWatch ✓                      │
├─────────────────────────────────────────────────────────────┤
│ DEMO: [Normal] [Error spike] [Traffic spike] [Recover]      │
│       [Send test alert]                                     │
└─────────────────────────────────────────────────────────────┘
```

**Vendor Chart.js** into `frontend/static/` — do not depend on a CDN at the venue.

---

## 38. What the Dashboard Must Show

- **Top metrics:** current error rate, baseline (± σ), active alerts, events/s, state chip (`WARMUP / LOW_DATA / NORMAL / ANOMALY`)
- **Chart:** current rate, baseline line, shaded band, alert markers; service selector
- **Alerts:** severity, service, opened/resolved times, current rate, baseline, z, ratio, sample size, reason, **delivery badges with an `external_id` tooltip** (SNS MessageId is good proof in a screenshot)
- **Detector health:** tail lag, events/s, queue depth, parse errors, dropped/late events, DB state, per-sink status, file status, **AWS status (`aws_identity`, `sns`, `cloudwatch`: green = ok, red = the error text)**
- **Demo panel** (only when `DEMO_MODE=true`): scenario buttons plus **Send test alert** — a labelled fake alert through the real dispatcher to every configured sink, so the AWS wiring can be verified in seconds

You are not only monitoring the application — you're monitoring the monitor.

---

## 39. Real-Time Transport

WebSocket at `/ws`; one connection, server pushes. Polling would send requests even when nothing changed.

- Client reconnects with exponential backoff.
- On connect the server sends a `hello` with current state (so a refresh isn't blank).
- Client sends `{"type":"ping"}` every 60 s (also keeps Render's free tier awake).

---

## 40. WebSocket Message Contract

```json
{"type":"hello","data":{"services":[...],"snapshots":[...],"alerts":[...],"health":{...}}}
{"type":"metric.update","data":{"ts":1790000000.1,"service":"payment-service","total":150,"errors":8,"error_rate":0.053,"baseline_mean":0.051,"baseline_std":0.012,"z":0.2,"ratio":1.04,"state":"NORMAL","severity":"NONE"}}
{"type":"alert.created","data":{...Alert + deliveries...}}
{"type":"alert.updated","data":{...Alert + deliveries...}}
{"type":"alert.resolved","data":{...Alert + deliveries...}}
{"type":"health.update","data":{...}}
```

Metric updates are emitted once per tick per service (not per event). Alert messages carry the alert's delivery rows (channel, event, status, attempts, error, `external_id`).

---

## 41. Backend API

```
GET  /health
GET  /api/metrics/current
GET  /api/metrics/history?service=&minutes=
GET  /api/alerts
GET  /api/alerts/{id}
GET  /api/system/status        # includes the "aws" startup-check panel
WS   /ws
POST /api/demo/scenario        # {"name":"normal|traffic_spike|error_spike|recover|mixed|malformed"} — only if DEMO_MODE=true
POST /api/demo/test-alert      # only if DEMO_MODE=true: labelled fake alert through the real dispatcher + all sinks
```

FastAPI's auto docs at `/docs` double as API documentation for the repo.

---

## 42. Demo Mode

`DEMO_MODE=true` enables the demo endpoints and buttons. The scenario endpoint starts/stops a generator task that **appends to the real log file** — the tailer, detector and alerts still run the full pipeline. Nothing is faked in the frontend. The test-alert endpoint stores its alert as `RESOLVED` with a `test-` id so it can never be reloaded as an open incident.

---

## 43. Demo Generator

`scripts/generate_logs.py` (also importable by the app for the buttons):

| Scenario | Rate | Error probability | Notes |
|---|---|---|---|
| `normal` | ~15 ev/s across 2 services | 4–6 % | Small jitter |
| `traffic_spike` | ~75 ev/s | 4–6 % (unchanged) | Must **not** alert |
| `error_spike` | ~15 ev/s | ramp 6 → 15 → 38 % on `payment-service` | Triggers MEDIUM/HIGH → CRITICAL |
| `recover` | ~15 ev/s | back to 4–6 % | Triggers RESOLVED |
| `mixed` | scripted timeline (§44) | — | Full story, hands-off |
| `malformed` | sprinkle broken lines | — | Shows parse-error counter |

Usage:

```
python scripts/generate_logs.py --scenario mixed --path /data/app.log
docker compose exec app python scripts/generate_logs.py --scenario error_spike
```

Run generators **inside** the container to avoid bind-mount inode quirks. Seed the RNG (`--seed 42`) for reproducibility. Flush after each write.

---

## 44. Deterministic Demo Timeline (2 minutes)

Demo profile (10 s window, 20 s warm-up):

| Time | Generator | What the judge sees |
|---|---|---|
| 0–25 s | normal | State `WARMUP` → `NORMAL`; baseline appears |
| 25–35 s | **traffic_spike** | Events/s jumps 5×; error rate flat; **no alert** ("volume ≠ errors") |
| 35–50 s | error_spike → ~15 % | MEDIUM/HIGH opens; **email lands (SNS)**; phone buzzes |
| 50–70 s | → ~38 % | Escalates to CRITICAL; second notification |
| 70–100 s | recover | Hysteresis → RESOLVED; notification in **both** channels |
| 100–110 s | (point at chart) | Baseline stayed flat through the incident |

Then say: **"The anomalous windows were excluded from the baseline to prevent incident contamination."**

### 44.1 AWS additions to the demo

| Moment | Add |
|---|---|
| CRITICAL fires | Show the **email** landing (SNS) on a second screen/phone; point at the dashboard badge `sns ✓` and hover it for the **MessageId** |
| Right after | Open CloudWatch → `/logpulse/alerts` → the new JSON event (same schema as `alerts.jsonl`) |
| Flourish (30 s) | **Logs Insights** on the log group: `fields @timestamp, service, severity, current_error_rate, z_score \| filter severity = "CRITICAL" \| sort @timestamp desc` — the events are JSON, so fields are auto-discovered |
| Recovery | The resolve event appears in both channels |

**Pre-open before presenting:** SNS topic, CloudWatch log stream, Logs Insights, the mailbox, the dashboard. Click **Send test alert** once beforehand to confirm the wiring.

Rehearse three times (once with Wi-Fi off/on to show failure isolation). Record a backup video.

---

## 45. Persistence (SQLite, WAL)

```sql
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS metric_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL NOT NULL, service TEXT NOT NULL,
  total INTEGER, errors INTEGER, error_rate REAL,
  baseline_mean REAL, baseline_std REAL, z REAL, ratio REAL,
  state TEXT, severity TEXT
);
CREATE INDEX IF NOT EXISTS ix_snap_service_ts ON metric_snapshots(service, ts);

CREATE TABLE IF NOT EXISTS alerts (
  id TEXT PRIMARY KEY, dedup_key TEXT NOT NULL, service TEXT NOT NULL,
  anomaly_type TEXT NOT NULL DEFAULT 'error_rate',
  severity TEXT, peak_severity TEXT, status TEXT NOT NULL,   -- OPEN | RESOLVED
  created_at REAL NOT NULL, resolved_at REAL,
  current_rate REAL, baseline_rate REAL, z REAL, ratio REAL,
  sample_size INTEGER, reason TEXT
);

CREATE TABLE IF NOT EXISTS alert_deliveries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL, event TEXT NOT NULL, channel TEXT NOT NULL,
  status TEXT NOT NULL,                                     -- PENDING | DELIVERED | FAILED
  attempt_count INTEGER DEFAULT 0, last_attempt_at REAL, error_message TEXT,
  external_id TEXT                                          -- SNS MessageId / CloudWatch group:stream
);

CREATE TABLE IF NOT EXISTS checkpoints (
  source TEXT PRIMARY KEY, inode INTEGER, offset INTEGER, updated_at REAL
);
```

**Migration:** an existing database created before `external_id` is upgraded in place on startup (`ALTER TABLE alert_deliveries ADD COLUMN external_id TEXT`, idempotent, data kept).

Snapshot retention: store one row per tick per service, prune rows older than 24 h (or store every 5th tick). For 2 services at 1 Hz that is small either way.

---

## 46. Restart Recovery

On startup:

```
1. Open DB (migrating if needed), load checkpoint
2. Reopen log file; if inode matches and offset ≤ size → seek(offset)
3. Rebuild each baseline from stored snapshots the live detector would have admitted
   (NORMAL, or populated WARMUP under the ceiling; never periods when an alert was open)
4. Load OPEN alerts back into AlertManager; re-enqueue PENDING deliveries
5. Resume tailing and detection
```

Result: crash → restart → no full-file replay, no lost baseline, no duplicate "created" notification for an alert that is already open.

---

## 47. File Rotation

`app.log → app.log.1`, new `app.log` created. Tailer compares `st_ino` / size; on change it reopens from offset 0 (after draining the old handle). Without this, ingestion silently stops after rotation.

---

## 48. Malformed Records

```
{"timestamp":"bad value...   →   parse_errors++ → sample stored → skip → continue
```

Health panel shows the count and last few bad lines (truncated to 200 chars).

---

## 49. Detector Health Metrics

```
events_processed        events_per_second       queue_depth
parse_errors            late_events             dropped_events
tail_lag_ms             detection_latency_ms    active_alerts
sink_success/failure    db_status               file_status
last_event_age_seconds  aws_identity            sns / cloudwatch (startup-check status)
```

Health status:

- `HEALTHY` — all fine
- `DEGRADED` — queue ≥ 80 %, or a sink failing, or tail lag > 5 s
- `DOWN` — file missing or DB unwritable

A failed AWS startup check is shown (red, with the error text) as an informational reason; it does not by itself degrade or take down the monitor (fail-soft) — an actual failing delivery does turn the sink's status to `failing`.

---

## 50. Security & Secrets

- `.env` is gitignored; commit only `.env.example` (placeholders only) and `iam-policy.json` (with `<ACCOUNT_ID>`).
- **AWS keys:** dedicated IAM user (`logpulse-app`), no console access, only the two-action / two-resource policy in §36.2 — never `AdministratorAccess`, never root keys. Keys exist only in the gitignored `.env` on the demo laptop and on the person implementing the sinks. Never paste them into chat, Slack or the repo. **If a key leaks:** IAM → user → Security credentials → Deactivate, delete, create a new one.
- Log retention on the CloudWatch group: 7 days.
- The ntfy topic is effectively a password — keep it out of the repo and slides; crop screenshots. SNS topic ARNs are not secrets, but crop account ids from public slides.
- Rotate the Telegram token (BotFather `/revoke`) if it ever lands in git.
- Demo endpoints exist only when `DEMO_MODE=true`.
- No auth on the dashboard (non-goal); mention it as a known limitation.

---

## 51. Deployment

### 51.1 Level 1 — Local (primary demo)

```
docker compose up --build   →   http://localhost:8000
```

### 51.2 Level 2 — Public URL (optional)

- **Render free web service** (Docker): deploy from the repo. Notes: sleeps after 15 min without an HTTP request or an incoming WebSocket message from a connected client; 30–60 s cold start; ephemeral disk (SQLite resets on redeploy — fine). Open the URL ~2 min before presenting.
- **Temporary tunnel** (Cloudflare Tunnel or ngrok) from your laptop for a shareable link during judging.

### 51.3 Level 3 — AWS (default for the demo machine)

Same container with `AWS_ENABLED=true` and the §36.3 variables; SNS and CloudWatch sinks activate. Everyone else runs `AWS_ENABLED=false`. Confirm the container has outbound internet.

### 51.4 Production evolution (slides only)

```
Host/file → Collector → Kafka/Kinesis → detector workers (partition by service)
          → aggregate store → alert service → SNS / ntfy / PagerDuty
```

---

## 52. Docker

`Dockerfile`:

```dockerfile
FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY frontend ./frontend
COPY scripts ./scripts
COPY config.yaml .
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health')"
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
```

`docker-compose.yml`:

```yaml
services:
  app:
    build: .
    ports:
      - "8000:8000"
    env_file: .env
    volumes:
      - ./data:/data
    restart: unless-stopped
```

One service. Do not add Kafka, Redis, Prometheus, Grafana, Nginx, or Postgres. `env_file: .env` already forwards the AWS variables — no compose change is needed for AWS.

---

## 53. Recommended Tech Stack

| Layer | Choice | Why |
|---|---|---|
| Language | Python 3.12 | Speed of implementation |
| API / WS | FastAPI + Uvicorn | REST + WebSocket in one |
| Detection | stdlib (`collections`, `statistics`) | Deterministic, no heavy deps |
| Storage | SQLite (WAL) | Free, durable, zero-ops |
| Frontend | HTML/CSS/JS + vendored Chart.js | One static page |
| Real-time | WebSocket | Push-based feed |
| HTTP client | httpx (async) | ntfy/Telegram sinks |
| Cloud sinks | boto3: SNS + CloudWatch Logs (live) | The PS requirement, live |
| Redundant push | ntfy.sh, Telegram | Fallback when AWS/venue misbehaves |
| Config | YAML + `.env` | Tunable, secret-safe |
| Container | Docker Compose | Reproducible |
| Testing | pytest, pytest-asyncio, moto | Fast, offline |

`requirements.txt`:
```
fastapi
uvicorn[standard]
pyyaml
httpx
boto3
```
Dev: `pytest pytest-asyncio moto[sns,logs]`

---

## 54. Why Not React

Four hours. One static page with native WebSocket and Chart.js removes build config, dependency hell, and deploy friction. Judges score the system.

## 55. Why Not Isolation Forest (or any ML)

The requirement is a rolling error-rate + baseline + deviation problem. ML adds training, feature design, serialization, cold start, calibration, opaque explanations and drift — for little benefit. A statistical detector is deterministic, low-latency, explainable. ML can be added later for semantic/structural anomalies behind the same interfaces.

## 56. Why SQLite Instead of PostgreSQL

We store compact aggregates, alerts, deliveries and a checkpoint — not raw logs. That fits SQLite comfortably, costs nothing, and removes a container. The repository layer isolates SQL, so moving to Postgres is a driver swap.

---

## 57. Testing Strategy

Test the math, not just "API returns 200."

### 57.1 Unit tests (must-have)

| # | Test |
|---|---|
| 1 | Window: old event evicted; boundary event handled; silent service decays |
| 2 | Error rate: 50/100 → 0.5; 0 events → 0.0 |
| 3 | Baseline: known samples → expected μ, σ; `std_floor` applied |
| 4 | Contamination: anomalous sample not admitted; warm-up ceiling enforced; frozen while OPEN |
| 5 | States: `WARMUP` (too few samples), `LOW_DATA` (too few events) |
| 6 | Traffic-only spike → no anomaly |
| 7 | Severity table: z 2.1 / 3.1 / 4.1 with gates on and off |
| 8 | Dedup: same anomaly twice → one notification; severity up → second |
| 9 | Hysteresis: 3.01 → 2.99 → 3.02 does not flap; resolves after `resolve_ticks` |
| 10 | Parser: partial line waits for `\n`; malformed counted; missing field counted |
| 11 | Sink failure → retries → `FAILED`; detection unaffected |
| 12 | Checkpoint: restart resumes at offset; no replay |
| 13 | Level shift: OPEN > timeout → resolved as level shift, baseline relearns |

### 57.2 Integration test

```
append lines to temp file → tailer → parser → detector → alert → WebSocket message
```

One end-to-end test with a fake clock and a fake sink.

### 57.3 Cloud/sink tests

CI never needs real AWS or real ntfy/Telegram: `moto` for AWS (`mock_aws`) and `httpx.MockTransport` for HTTP sinks. Automated AWS tests:

- SNS publish returns a `MessageId`; message has the sanitised ASCII subject and severity/service/event attributes
- CloudWatch writes structured JSON into the per-day stream and returns `group:stream`; a missing log group raises; a deleted stream is recreated once
- `healthcheck()` for both sinks; the fail-soft startup check (healthy, per-resource errors, bad credentials, no AWS sinks)
- **AWS unreachable:** SNS + CloudWatch deliveries end `FAILED`, the alert is already persisted, detection returns immediately, JSONL still delivers
- `external_id` is stored on the delivery row and never cleared; an old database is migrated in place
- `POST /api/demo/test-alert` goes through the real dispatcher to every sink and never creates an open alert

**Live verification checklist (manual, real AWS — tick each):**

- [ ] `aws sns publish` from the CLI → email received (§36.1 step 7)
- [ ] `.env` on the demo laptop has keys, region, topic ARN, log group; `AWS_ENABLED=true`
- [ ] `docker compose up --build` → logs show **"AWS credentials OK: arn:aws:iam::…:user/logpulse-app"**
- [ ] Health panel shows `AWS identity ✓ · SNS ✓ · CloudWatch ✓`
- [ ] Dashboard **Send test alert** → email arrives (SNS)
- [ ] CloudWatch console → `/logpulse/alerts` → stream `alerts/YYYY-MM-DD` → JSON event visible
- [ ] Dashboard badges show `sns ✓` / `cloudwatch ✓`; hovering shows the MessageId / `group:stream`
- [ ] **Run the full demo scenario** → alerts at open, escalate, resolve reach email + CloudWatch
- [ ] Wi-Fi off / wrong topic ARN → dashboard shows `sns ✗ retrying/FAILED`, detection keeps running, ntfy/JSONL still deliver
- [ ] `pytest` green

### 57.4 Performance test

Replay 10 k / 50 k / 100 k synthetic lines through parser + detector; report events/s, p95 per-event latency, and queue growth. **Report what you measured.** Nothing unmeasured goes on a slide.

---

## 58. Repository Structure

```
Hackforge.ai-DevelopMental/
│
├── app/
│   ├── main.py                    # wiring, startup/shutdown, tasks
│   ├── config.py                  # YAML + env loader, profile selection
│   ├── generator.py               # demo log generator
│   ├── health.py                  # HEALTHY / DEGRADED / DOWN + AWS panel
│   ├── api/
│   │   ├── routes.py
│   │   └── websocket.py
│   ├── ingestion/
│   │   ├── tailer.py
│   │   ├── parser.py
│   │   └── models.py              # LogEvent
│   ├── detection/
│   │   ├── window.py
│   │   ├── baseline.py
│   │   ├── detector.py            # per-service engine, tick loop
│   │   ├── severity.py
│   │   ├── state.py               # alert state machine
│   │   └── models.py              # Snapshot, Alert
│   ├── alerts/
│   │   ├── base.py                # AlertSink protocol, render(), alert_payload()
│   │   ├── aws_common.py          # boto session/config, fail-soft startup check
│   │   ├── manager.py
│   │   ├── dispatcher.py
│   │   ├── console.py
│   │   ├── jsonl.py
│   │   ├── ntfy.py
│   │   ├── telegram.py
│   │   ├── webhook.py
│   │   ├── sns.py
│   │   └── cloudwatch.py
│   └── storage/
│       ├── db.py
│       └── repository.py
│
├── frontend/
│   ├── index.html
│   └── static/chart.umd.min.js    # vendored
│
├── scripts/
│   ├── generate_logs.py
│   └── benchmark.py
│
├── tests/
│   ├── test_window.py
│   ├── test_baseline.py
│   ├── test_severity.py
│   ├── test_state.py
│   ├── test_parser_tailer.py
│   ├── test_alerts.py
│   ├── test_aws_sinks.py
│   ├── test_storage.py  test_generator.py  test_health.py  test_api.py
│   └── test_e2e.py
│
├── docs/
│   ├── PRD.md
│   ├── architecture.md
│   ├── DECISIONS.md
│   └── WORKLOG.md
│
├── data/.gitkeep
├── config.yaml
├── iam-policy.json                # template, placeholders only
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

### 58.1 DECISIONS.md (one page, 5 bullets)

1. Statistical detector over ML
2. SQLite over Postgres
3. Poll-tail over inotify
4. Live AWS (SNS + CloudWatch) behind the `AlertSink` abstraction, with redundant free channels and fail-soft behaviour
5. Local-first deployment (Compose), optional Render/tunnel

---

## 59. Git Workflow

```
main
├── feature/ingestion
├── feature/detection
├── feature/api
├── feature/frontend
└── feature/alerts
```

Merge to `main` at least hourly; small PRs; everyone pulls before starting the next block.

Commit style:

```
feat: add file tailer with checkpoint recovery
feat: implement rolling baseline with contamination guard
feat: add websocket alert feed
feat: add SNS and CloudWatch sinks with startup check
feat: add ntfy and telegram sinks
test: add hysteresis and dedup tests
docs: add architecture and decisions
```

---

## 60. Scaffold (run once, from repo root — Git Bash / WSL / macOS / Linux)

```bash
mkdir -p app/{api,ingestion,detection,alerts,storage} frontend/static scripts tests data docs
touch app/__init__.py app/{api,ingestion,detection,alerts,storage}/__init__.py
touch app/main.py app/config.py app/api/{routes.py,websocket.py}
touch app/ingestion/{tailer.py,parser.py,models.py}
touch app/detection/{window.py,baseline.py,detector.py,severity.py,state.py,models.py}
touch app/alerts/{base.py,aws_common.py,manager.py,dispatcher.py,console.py,jsonl.py,ntfy.py,telegram.py,sns.py,cloudwatch.py}
touch app/storage/{db.py,repository.py}
touch frontend/index.html scripts/{generate_logs.py,benchmark.py}
touch tests/{test_window.py,test_baseline.py,test_severity.py,test_state.py,test_parser_tailer.py,test_alerts.py,test_aws_sinks.py,test_e2e.py}
touch docs/{architecture.md,DECISIONS.md} config.yaml iam-policy.json requirements.txt Dockerfile docker-compose.yml README.md data/.gitkeep

cat > .gitignore <<'EOF'
__pycache__/
*.pyc
.env
data/*
!data/.gitkeep
.venv/
.pytest_cache/
EOF

cat > .env.example <<'EOF'
DEMO_MODE=true
LOG_PATH=/data/app.log
DB_PATH=/data/logpulse.db
ALERTS_JSONL=/data/alerts.jsonl
NTFY_TOPIC=logpulse-CHANGE-ME-long-random
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# --- AWS (leave AWS_ENABLED=false on machines without credentials) ---
AWS_ENABLED=false
AWS_REGION=ap-south-1
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
SNS_TOPIC_ARN=arn:aws:sns:ap-south-1:<ACCOUNT_ID>:logpulse-alerts
CW_LOG_GROUP=/logpulse/alerts
CW_LOG_STREAM_PREFIX=alerts
EOF

git add -A && git commit -m "chore: scaffold LogPulse structure" && git push origin main
```

---

## 61. Interface Contracts (lock in the first 15 minutes)

These four shapes let five people build in parallel. Put them in code (`models.py` files) in the first commit.

```python
# app/ingestion/models.py
@dataclass(frozen=True)
class LogEvent: ts: float; service: str; level: str; status: int | None; message: str; request_id: str | None; is_error: bool

# app/detection/models.py
@dataclass
class Snapshot:
    ts: float; service: str; total: int; errors: int; error_rate: float
    baseline_mean: float | None; baseline_std: float | None
    z: float | None; ratio: float | None
    state: Literal["WARMUP","LOW_DATA","NORMAL","ANOMALY"]
    severity: Literal["NONE","MEDIUM","HIGH","CRITICAL"]

@dataclass
class Alert:
    id: str; dedup_key: str; service: str
    severity: str; peak_severity: str; status: Literal["OPEN","RESOLVED"]
    created_at: float; resolved_at: float | None
    current_rate: float; baseline_rate: float; z: float; ratio: float
    sample_size: int; reason: str

# app/alerts/base.py
class AlertSink(Protocol):
    name: str
    async def send(self, alert: Alert, event: str) -> Optional[str]: ...   # returns an external id if the channel has one
```

Plus the WebSocket message shapes in §40 and the REST list in §41.

---

## 62. Four-Hour Execution Plan

### 62.1 Ownership

| Person | Owns | Files |
|---|---|---|
| **A** | Ingestion + generator | `ingestion/*`, `scripts/generate_logs.py` |
| **B** | Detection core + its tests | `detection/*`, unit tests 1–9, 13 |
| **C** | FastAPI, WebSocket hub, SQLite (incl. `external_id`), config, health (incl. AWS fields), `/api/demo/test-alert`, wiring | `api/*`, `storage/*`, `config.py`, `main.py` |
| **D** | Dashboard (badges with `external_id` tooltip, **Send test alert** button, AWS row in the health panel), then PPT lead | `frontend/*`, slides |
| **E** | Alerts (manager, dispatcher, sinks incl. SNS/CloudWatch/startup check), Docker, README | `alerts/*`, `Dockerfile`, compose |

Everyone owns a technical surface; PPT is shared (D leads, B writes the math slide, E the deployment/AWS slide, A the reliability slide, C the architecture slide).

### 62.2 AWS-related adjustments

- **Before 0:00 (or in the first 20 min):** whoever owns the AWS account does §36 (topic, email confirmations, log group + retention, IAM user, access key, budget). Post the **Topic ARN, region and log-group name** in the team chat — **not the keys**.
- **Credentials handling:** only **E** and the **demo-laptop owner** hold an access key (an IAM user can hold two, so each gets their own; rotate/delete afterwards). Everyone else runs `AWS_ENABLED=false` with the moto tests and console/JSONL/ntfy.
- **E, hours 1–2:** ntfy/JSONL first (fallback), then `SnsSink`, then `CloudWatchSink`, then the startup check + test-alert endpoint. Targets: **live SNS email by 1:30**, CloudWatch by 2:00.
- **2:15–3:00:** full-scenario AWS run (§57.3 checklist) instead of a mock-only run; capture the PPT screenshots here (email, CloudWatch event, Logs Insights, dashboard badge).
- **3:00 rehearsal:** run once with Wi-Fi off/on to show failure isolation.
- **Freeze at 3:30:** no IAM or config changes after that; rehearse from a fresh `docker compose up`.

### 62.3 Timeline

**0:00–0:15 — Foundation (everyone together)**
- Lock contracts (§61) and `config.yaml`
- One person runs the scaffold (§60) and pushes; everyone pulls, creates a branch
- E: create ntfy topic, Telegram bot; verify a manual `curl` message works; confirm the SNS smoke test (§36.1 step 7)

**0:15–1:15 — Build against contracts**
- A: tailer (offset, partial lines) + parser + queue + generator `normal`/`error_spike`
- B: `SlidingWindow`, `Baseline`, severity, `Detector.tick()` with fake clock; tests 1–7
- C: FastAPI app, `/ws` hub broadcasting **fake** `metric.update`, SQLite schema, config loader
- D: HTML skeleton, Chart.js (vendored), WS client consuming C's fake feed
- E: `AlertSink` base, Console + JSONL sinks, ntfy sink, `render()`

**1:15 — CHECKPOINT: Walking skeleton.** Generator → file → tailer → detector → WS → chart moving on screen. If this isn't true by 1:30, everyone helps integration.

**1:15–2:15 — Make it real**
- B: gates, warm-up, low-volume, contamination; tests 4–8
- C: persist snapshots/alerts, health metrics, `/api/*`
- A: checkpoint, malformed lines, `traffic_spike`/`recover` scenarios, demo endpoint hooks
- D: alert feed, KPI strip, band, health panel, demo buttons
- E: AlertManager (dedup, state machine with B), dispatcher + retries, **SNS + CloudWatch sinks (live)**, Telegram sink

**2:15–3:00 — Reliability + polish**
- State machine, hysteresis, delivery badges end-to-end
- Restart recovery (checkpoint + baseline reload + open alerts)
- Rotation/truncation if time
- B/E finish tests 8–13 and the AWS tests; A writes the e2e test
- D starts the PPT skeleton

**3:00–3:30 — Deploy + rehearse**
- `docker compose up --build` **from a fresh clone** (with the demo laptop's `.env`)
- Full demo rehearsal ×2 (§44); tune thresholds so the timeline works every time
- Optional: Render / tunnel URL

**3:30–3:50 — Code freeze**
- Screenshots, backup screen recording, architecture image, README, finish PPT
- No new features

**3:50–4:00 — Final rehearsal.** Laptop charged; phone hotspot ready; mailbox, ntfy topic and AWS console tabs open.

### 62.4 Rules
- If a P1 item is not done by 2:45, cut it.
- Never cut the demo path.
- Nobody merges a broken `main` in the last hour.

---

## 63. Definition of Done per Person

| Person | Done when |
|---|---|
| A | Appending lines during a run produces events; partial line waits; bad line counted; restart resumes at offset; all 6 scenarios work |
| B | Tests 1–9, 13 green; a synthetic spike produces MEDIUM→CRITICAL and a traffic-only spike produces none |
| C | Dashboard receives `hello` + live `metric.update`; alerts/snapshots survive restart; `/health` and `/api/system/status` accurate (incl. AWS fields); `external_id` stored |
| D | Chart, KPIs, alert feed with badges (tooltip shows external id), health panel with AWS row, demo buttons and **Send test alert** all update live; reconnects after server restart |
| E | **SNS email and CloudWatch event arrive from the demo laptop**; ntfy + Telegram messages arrive on a phone; killing the network shows `FAILED` badges while detection continues; `docker compose up` works from a clean clone; README steps verified |

---

## 64. PPT Structure (9 slides, ≤ 6 minutes)

| # | Slide | Content | Speaker line |
|---|---|---|---|
| 1 | Problem & insight | Error spikes are easy to see and easy to get wrong: traffic surges cause false alarms; incidents teach detectors they're normal | "Two failure modes kill anomaly detectors — we designed against both." |
| 2 | Architecture | Diagram from §10.1 | "One modular app; ingestion, detection, alerting, delivery are separate." |
| 3 | Detection method | Sliding window, past-only baseline, z + ratio + absolute gates, severity table; small chart showing flat baseline during an incident | "We alert only when it's statistically *and* operationally significant." |
| 4 | Alert lifecycle | State machine, hysteresis, dedup, persist-then-deliver | "Persistent incidents don't create alert storms." |
| 5 | Live demo | The §44 timeline plus **screenshots: the email, the CloudWatch event, the Logs Insights result, and the dashboard badge with its MessageId** | "Normal, traffic spike — no alert, error spike — email and CloudWatch event, recovery." |
| 6 | Reliability | Table: partial writes, malformed lines, low volume, backpressure, restart, sink failure → the test that proves each | "Every edge case has a test." |
| 7 | **Live on AWS, redundant by design** | `SNS (email) + CloudWatch Logs` live; `ntfy/Telegram/JSONL` as backups; the `AlertSink` interface; short excerpt of the least-privilege IAM policy; 7-day retention; Compose deploy | "Alerts land in AWS — and if AWS is unreachable, they still land somewhere else." |
| 8 | Evidence | Test count, benchmark numbers **as measured**, "moto-tested AWS sinks + live integration verified", "an AWS outage can't block detection (test)", what we deliberately didn't build | "We chose depth over technology count." |
| 9 | Scale path & roadmap | Collector → Kafka/Kinesis → partitioned workers → aggregate store → alert service; seasonality-aware baseline, ML for semantic anomalies | "Same detector, horizontally partitioned by service." |

**Security bullet (slide 7 or 8):** "Dedicated IAM user, two actions on two resources, keys never in git, 7-day log retention."

---

## 65. The 90-Second Judge Story

> "Applications continuously append logs to a file. Our ingestion layer tails only new, complete records and normalizes them. The detection engine keeps a true sliding window per service and computes the current error rate. We compare that against a past-only baseline using a z-score, a relative multiplier and absolute floors, and we deliberately exclude anomalous windows from the baseline so an incident can't teach the detector that it's normal. A policy engine assigns severity; the alert manager applies confirmation, hysteresis and deduplication so persistent incidents don't spam. Alerts are persisted before asynchronous delivery — live to AWS SNS as an email and to CloudWatch Logs as a structured JSON event, with ntfy, Telegram and a JSONL log as redundant channels; if any channel is down, detection is unaffected and the failure shows on the dashboard. WebSockets push everything to a live dashboard. It runs as one Docker Compose app; at scale the same detector sits behind Kafka or Kinesis, partitioned by service."

---

## 66. Judge Q&A

| Question | Answer |
|---|---|
| **Why no ML?** | The requirement is a streaming rate-deviation problem. Statistical detection is deterministic, low-latency, explainable; ML can be layered on for semantic anomalies behind the same interfaces |
| **How do you avoid false positives?** | Min-event guard + warm-up + z **and** ratio **and** absolute gates + confirmation ticks + hysteresis. Live proof: the traffic-spike scenario doesn't alert |
| **Natural traffic growth?** | Baseline adapts from accepted history; we use rates, not counts |
| **30-minute incident?** | Anomalous windows aren't admitted; after a timeout it's treated as a level shift and relearned |
| **What if σ is tiny?** | `std_floor` plus absolute/ratio gates prevent z-score explosions |
| **Why sample the baseline on a cadence?** | Overlapping windows are near-identical; per-event sampling collapses σ |
| **Crash?** | Checkpointed offset + persisted history; open alerts reload; no full replay |
| **What if AWS is unreachable?** | Detection is independent: alerts are persisted first, the dispatcher retries with backoff and a timeout, failures show on the dashboard (`sns ✗ retrying/FAILED`), and ntfy/Telegram/JSONL still deliver. There is a test for exactly this |
| **How is AWS secured?** | Dedicated IAM user with two actions on two resources, keys only in a gitignored `.env`, 7-day log retention, budget alert |
| **Why SQLite?** | We store aggregates, not raw logs; SQLite is durable and zero-ops; repository layer makes Postgres a swap |
| **Why poll-tail, not inotify?** | Portable, simple, sufficient at this scale; 200 ms latency |
| **What if the service goes silent?** | Surfaced as silence/`LOW_DATA` on the dashboard (P2) — a known blind spot of error-rate detectors |
| **Backpressure?** | Bounded queue, depth monitored, `DEGRADED` health, dropped counter; at scale becomes Kafka/Kinesis |
| **How would you scale?** | Collector → stream → partition by service → detector workers → aggregate store → alert service |

---

## 67. What Not to Say in the PPT

Avoid: "AI-powered next-generation system", "scales to millions of events per second", "production-ready enterprise-grade", and any AWS claim you did not verify live (state exactly what you ran).

Say: "Modular monolith with a clear migration path to partitioned stream processing." "Measured X events/s on a laptop." "Live SNS and CloudWatch delivery, with redundant channels."

Remove the wording "AWS-ready, mock-verified" everywhere.

---

## 68. What Makes the Repo Look Strong

```
✓ clean modular structure       ✓ README with 3-command quickstart
✓ architecture diagram          ✓ .env.example + iam-policy.json template
✓ Dockerfile + compose          ✓ demo generator + demo buttons
✓ config.yaml with profiles     ✓ tests that verify the math
✓ DECISIONS.md                  ✓ screenshots + backup demo video
✓ /docs API page (FastAPI)      ✓ conventional commit history
```

A judge should understand the system in under one minute.

README quickstart:

```
cp .env.example .env     # set NTFY_TOPIC (and optionally Telegram / AWS)
docker compose up --build
open http://localhost:8000   # click "Send test alert", then "Error spike"
```

---

## 69. Risk Register

| Risk | Impact | Mitigation |
|---|---|---|
| Thresholds misfire in demo | Embarrassing live | Tune during 3:00 rehearsal; demo profile; gates + floors; seeded generator |
| Venue Wi-Fi / ntfy / AWS down | No phone/email alert | Hotspot; dashboard + JSONL work offline; backup video; screenshot slide |
| Integration crunch | Late/broken build | Contracts at 0:15; walking skeleton at 1:15; fake feeds unblock UI |
| Windows file semantics | Tailer bugs | Develop in Docker/WSL; generator runs in container |
| Scope creep | Shallow everything | Priority tiers (§70); cut P2 first |
| Secret leak | Public ntfy topic / bot token / AWS key | `.env` gitignored; rotate/deactivate if leaked |
| Free-tier sleep (Render) | Dead URL at pitch | Wake 2 min early; laptop is primary |
| ntfy free limits | Missed alerts in rehearsal | Dedup; use JSONL + Telegram as backup; don't spam-test |
| **AWS credentials mis-set on demo day** | SNS/CloudWatch silent or failing | Startup check + health panel; **Send test alert** button before presenting; fallback channels |
| **AWS key leaked to git** | Account abuse / cost | `.env` gitignored; least-privilege user; secret scanning; deactivate → delete → rotate |
| **SNS email confirmation forgotten** | No email arrives | Verify in the §57.3 checklist (CLI smoke test + subscription status "Confirmed") |

---

## 70. Feature Priority

**P0 — must work**
```
✓ growing log file      ✓ severity
✓ parser                ✓ live frontend (WebSocket)
✓ sliding window        ✓ alert feed
✓ error rate            ✓ SNS sink (live) + JSONL log
✓ baseline              ✓ generator + demo buttons
✓ anomaly detection
```

**P1 — very high value**
```
✓ CloudWatch sink (live)        ✓ checkpoint recovery
✓ SQLite persistence            ✓ retry dispatcher + delivery badges (+ external id)
✓ dedup + hysteresis            ✓ health panel (+ AWS status)
✓ contamination protection      ✓ Docker Compose
✓ warm-up + low-volume guard    ✓ ntfy + Telegram redundant channels
✓ malformed-line handling       ✓ Send test alert + AWS startup check
✓ 13 unit tests + AWS moto tests
```

**P2 — differentiators**
```
✓ file rotation / truncation    ✓ level-shift timeout
✓ silence indicator             ✓ benchmark (measured)
✓ Render/tunnel public URL      ✓ CloudWatch metric-filter alarm (stretch, §72)
```

**P3 — don't touch**
```
ML · Kafka · Redis · Kubernetes · LLM RCA · multi-tenancy · React · SMS
```

---

## 71. Final Acceptance Criteria

| Requirement | Acceptance condition |
|---|---|
| Growing file | New appended records detected without restart |
| Sliding window | Old events leave the window; silent service decays |
| Error rate | Rolling rate computed correctly (unit-tested) |
| Baseline | Built only from prior accepted, sufficiently-sampled observations |
| Warm-up | Alerts suppressed until enough baseline samples exist |
| Low volume | `LOW_DATA` state below `min_events`; no alert |
| Anomaly detection | Significant deviation produces an anomaly with evidence |
| No false positive on traffic | Traffic-only spike produces no alert |
| Severity | Configurable MEDIUM/HIGH/CRITICAL from policy table |
| Explainability | Alert shows current vs baseline, z, ratio, sample size, reason |
| Live feed | Frontend receives alerts without refresh; reconnects after restart |
| Dedup / hysteresis | Persistent incident → one open + escalations + one resolve; no flapping |
| **Push delivery** | **An SNS email is received and a JSON event is present in the CloudWatch log group**; the same alert also reaches ≥ 1 redundant channel (ntfy/Telegram) and the JSONL log |
| **Cloud sinks** | **SNS + CloudWatch sinks pass the moto tests and the live checklist (§57.3); the dashboard shows `sns ✓` / `cloudwatch ✓` with the external id** |
| Persistence | Alert history and open alerts survive process restart; an older database is migrated in place |
| Checkpoint | Restart does not replay the whole file |
| Rotation | File replacement doesn't permanently stop ingestion (P2) |
| Malformed data | Bad records counted, never crash the pipeline |
| Sink failure | Detection continues; delivery shown as retrying/FAILED; other channels still deliver |
| Docker | Fresh clone → `docker compose up --build` works |
| Demo | Normal → traffic spike (no alert) → error spike → critical → recovery, reproducibly |

---

## 72. Optional Stretch — CloudWatch-Native Alarm Loop

Only if everything above is green. Turn CloudWatch into an independent second detector of your alerts: a metric filter on the alert log group plus an alarm that publishes to the same SNS topic. Creating these needs admin permissions, not the app user's policy.

```bash
aws logs put-metric-filter --region ap-south-1 --log-group-name /logpulse/alerts \
  --filter-name critical-created \
  --filter-pattern '{ $.severity = "CRITICAL" && $.event = "created" }' \
  --metric-transformations metricName=CriticalAlerts,metricNamespace=LogPulse,metricValue=1

aws cloudwatch put-metric-alarm --region ap-south-1 --alarm-name logpulse-critical \
  --namespace LogPulse --metric-name CriticalAlerts --statistic Sum --period 60 \
  --evaluation-periods 1 --threshold 1 --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching --alarm-actions <TOPIC_ARN>
```

This sends a **second** email per CRITICAL — skip it for a clean single-email demo.

---

## 73. Final Product Concept and Architecture (Locked for the Build)

**LogPulse — Explainable Real-Time Error Anomaly Detection**

A streaming observability engine that watches append-only application logs, learns a past-only, contamination-resistant baseline, detects statistically and operationally significant error-rate deviations using a true sliding window, manages incident state and alert fatigue, and delivers live, explainable alerts through WebSockets, **AWS SNS and CloudWatch Logs**, redundant free push channels and a structured alert log — all behind one sink interface, with detection independent of any of them.

Not "a Python ML anomaly detector."

```
                    ┌─────────────────────┐
                    │  Demo Log Generator │
                    │  / real application │
                    └──────────┬──────────┘
                               │ append
                               ▼
                    ┌─────────────────────┐
                    │  /data/app.log      │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │ Tailer (offset,     │
                    │ partial-line, ckpt) │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │ Parser + Normalize  │
                    └──────────┬──────────┘
                               ▼
          ┌────────────────────────────────────────┐
          │          STREAMING DETECTOR            │
          │ sliding deque (per service)            │
          │   ↓                                    │
          │ rolling error rate                     │
          │   ↓                                    │
          │ past-only baseline (contamination-safe)│
          │   ↓                                    │
          │ z-score + ratio + absolute gates       │
          │   ↓                                    │
          │ severity + alert state machine         │
          └───────────────┬────────────────────────┘
                          │
               ┌──────────┼───────────┐
               ▼          ▼           ▼
            SQLite     WebSocket   Dispatcher (retry, timeout, external id)
               │          │           │
               │          ▼           ├──▶ AWS SNS (email)            LIVE
               │      Dashboard       ├──▶ AWS CloudWatch Logs (JSON) LIVE
               │                      ├──▶ ntfy        (redundant)
               │                      ├──▶ Telegram    (redundant)
               │                      └──▶ alerts.jsonl (redundant)
               │
        (history, alerts, deliveries + external_id, checkpoint)

Deploy:  Docker Compose on laptop (primary; AWS enabled on the demo machine) · Render / tunnel (optional)
```

---

## Appendix — Sources Consulted for Free-Stack Decisions

- Render changelog: free web services stay active while receiving WebSocket messages (spin-down after 15 min without HTTP request or incoming WebSocket message) — https://render-www.onrender.com/changelog/free-web-services-now-remain-active-while-receiving-websocket-messages
- ntfy project (HTTP pub-sub, no signup, topic as password) — https://github.com/binwiederhier/ntfy
- LocalStack image consolidation requiring an auth token (March 2026) — https://github.com/elgohr/go-localstack/issues/1021
- AWS Free Tier update (credits, 6-month free plan, always-free services) — https://aws.amazon.com/about-aws/whats-new/2025/07/aws-free-tier-credits-month-free-plan/
- Telegram bot flood-limit guidance (python-telegram-bot wiki) — https://github.com/python-telegram-bot/python-telegram-bot/wiki/Avoiding-flood-limits

Free-tier terms change; re-verify limits (ntfy daily cap, Render behavior, AWS plan) the day you build.
