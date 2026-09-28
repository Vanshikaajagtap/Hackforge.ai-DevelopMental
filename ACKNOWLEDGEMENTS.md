# Acknowledgements and attribution

## Dataset

**NASA-HTTP — July 1995 access log** (`NASA_access_log_Jul95`), distributed by the **Internet Traffic Archive**:
<http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html>

> The logs were collected by **Jim Dumoulin of the NASA Kennedy Space Center** and contributed by **Martin Arlitt** and **Carey Williamson** of the
> **University of Saskatchewan**.

How this project uses it:

- The dataset is included **in compressed form**: `data/datasets/NASA_access_log_Jul95.gz` (19.5 MB) is the archive's data gzip-compressed losslessly and otherwise unmodified
  (SHA-256 of the decompressed 205,242,368 bytes: `96551161b5bdcaacbc3c17fa108191c478fb35dfe87895c16e34a8f6552bf29a`, pinned by a test). GitHub rejects files over 100 MB, so the unpacked
  file (`data/datasets/NASA_access_log_Jul95`) is git-ignored and is created locally, once, on first use. The credit above applies to that copy.
  **Redistribution:** the archive's terms of use were not verified while preparing this repository - confirm they allow redistribution before submitting (see the review table below).
- `tests/fixtures/nasa_sample.log` (781 consecutive lines, 24 Jul 02:40-03:20 ET) and `tests/fixtures/nasa_oddities.log` (28 selected lines, including the truncated final line)
  are small excerpts kept so the tests run without the full file. They are unmodified lines of the dataset.
- `scripts/analyze_dataset.py` and `scripts/replay_dataset.py` read the file for analysis and replay; `docs/DATASET_ANALYSIS.md` reports measurements computed from it.
- The traffic is real; the error definition (4xx + 5xx), the service mapping and every threshold are choices made in this project.
- Please consult the archive page for its terms of use and cite the archive and the contributors above in any publication.

## What was written for this project

The **detection algorithm** (sliding window, fixed-cadence past-only baseline with the contamination guard, z-score + ratio + absolute-floor gates, severity table),
the **alert lifecycle** (confirm ticks, escalation past the peak, hysteresis, dedup, level shift, persist-then-deliver with retry), the **architecture** (modular monolith,
`AlertSink` abstraction, fail-soft AWS handling, replay AWS guard), the CLF parser and field mapping, the dataset replayer and analysis tooling, the dashboard and the tests
were implemented for this project. The z-score, sliding-window and error-diffusion techniques used are standard, textbook methods.

## Third-party software

Licenses below were read from the installed package metadata (`importlib.metadata`) of the environment used to build and test this project.

**Runtime — direct dependencies** (`requirements.txt`)

| Package | Version tested | License | Use |
|---|---|---|---|
| FastAPI | 0.141.1 | MIT | HTTP + WebSocket API |
| Uvicorn (`[standard]`) | 0.54.0 | BSD-3-Clause | ASGI server |
| PyYAML | 6.0.3 | MIT | `config.yaml` |
| HTTPX | 0.28.1 | BSD-3-Clause | ntfy / Telegram / webhook sinks |
| boto3 | 1.43.103 | Apache-2.0 | AWS SNS + CloudWatch Logs sinks |

**Runtime — notable transitive dependencies** (installed automatically): Starlette 1.7.0 (BSD-3-Clause), Pydantic 2.13.5 / pydantic-core 2.46.5 (MIT), websockets 17.1 (BSD-3-Clause),
httptools 0.8.0 (MIT), watchfiles 1.3.0 (MIT), python-dotenv 1.2.3 (BSD-3-Clause), Click 8.5.0 (BSD-3-Clause), h11 0.16.0 (MIT), AnyIO 4.15.1 (MIT), HTTPCore 1.0.9 (BSD-3-Clause),
idna 3.20 (BSD-3-Clause), certifi 2026.7.22 (**MPL-2.0**), botocore 1.43.103 (Apache-2.0), s3transfer 0.19.2 (Apache-2.0), jmespath 1.1.0 (MIT), urllib3 2.8.0 (MIT),
python-dateutil 2.9.0 (dual-licensed Apache-2.0 / BSD-3-Clause upstream; its metadata says only "Dual License"), typing-extensions 4.16.0 (PSF-2.0), annotated-types 0.8.0 (MIT).

**Development and test only** (`requirements-dev.txt`)

| Package | Version tested | License | Use |
|---|---|---|---|
| pytest | 9.1.1 | MIT | test runner |
| pytest-asyncio | 1.4.0 | Apache-2.0 | async tests |
| pytest-cov (+ coverage.py) | 7.1.0 | MIT (coverage.py: Apache-2.0) | coverage |
| moto | 5.2.3 | Apache-2.0 | in-process AWS mock (SNS, CloudWatch Logs, SQS) |
| Ruff | 0.16.9 | MIT | lint |

**Vendored front-end library**

| Library | Version | License | Where |
|---|---|---|---|
| Chart.js | 4.4.7 | MIT (header: "(c) 2024 Chart.js Contributors, Released under the MIT License") | `frontend/static/chart.umd.min.js`, an **unmodified copy** from cdn.jsdelivr.net, vendored so the dashboard works without internet |

**External services** (used through their public APIs, no code included): AWS SNS and CloudWatch Logs, ntfy.sh, Telegram Bot API.

## Files to review before submission (code that did not originate as original code)

| File | What it is | Why flagged |
|---|---|---|
| `frontend/static/chart.umd.min.js` | Chart.js v4.4.7, minified | **Copied third-party code** (MIT). Keep the license header; it is intact |
| `data/datasets/NASA_access_log_Jul95.gz` | the full NASA-HTTP July 1995 log, gzip-compressed (19.5 MB) | **Copied third-party data**, redistributed unmodified; confirm the archive's terms allow it |
| `tests/fixtures/nasa_sample.log`, `tests/fixtures/nasa_oddities.log` | unmodified lines of the NASA dataset | **Copied third-party data**; covered by the dataset's terms (see above) |
| `app/alerts/sns.py`, `app/alerts/cloudwatch.py`, `app/alerts/aws_common.py`, `iam-policy.json`, `tests/test_aws_sinks.py` | AWS sinks, shared boto config, startup check, least-privilege policy, moto tests | Written from the specification and code snippets in **the team's `AWS_Integration_Changes` document** (adapted to this codebase: names, error handling, healthchecks, tests). The structure closely follows those snippets, so confirm you are comfortable with their origin |
| `app/alerts/ntfy.py`, `app/alerts/telegram.py`, `app/alerts/jsonl.py`, `app/alerts/webhook.py` | free-channel sinks | Follow the snippets in the team's **PRD** (§32); the ntfy/Telegram HTTP calls are the services' documented public APIs |
| `app/detection/window.py`, `app/detection/state.py` | sliding window; alert state machine | Implement the algorithm and pseudocode given in the team's **PRD** (§18, §27); code is original but the design is specified there |
| `docs/PRD.md` | product requirements | The team's document, with the AWS and real-dataset revisions applied |

No code in this repository was knowingly copied from Stack Overflow, GitHub repositories, blog posts or other external sources beyond what is listed above.
This was checked by review of the file list, not by an automated plagiarism scan; the repository history contains a single commit, so provenance cannot be reconstructed from git.
