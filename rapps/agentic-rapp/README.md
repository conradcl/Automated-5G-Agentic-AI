# Read-only Health Agent rApp MVP

The rApp subscribes to live Health xApp evidence through the lab R1/DME
interface, performs deterministic checks for the RIC/E2 KPM telemetry
monitoring path with LangGraph, and can ask DeepSeek to interpret the evidence
in concise advisory prose. It consumes telemetry JSON over the network; it does
not scrape xApp output files, logs, or terminal text. Accepted observations and
conversation context are retained as structured records in a local SQLite
database so the rApp can reason over recent history and survive restarts.

```text
E2 node -> Health xApp -> Evidence API -> R1/DME job -> rApp -> LangGraph
```

`app.py` is the lab R1/DME-lite broker, not the controller. `main.py`,
`consumer.py`, `memory.py`, `health_checks.py`, and `graph.py` form the
read-only rApp.

## Install

Python 3.10 or newer is required.

Use a full checkout of this repository because the rApp and Evidence Producer
share the repository-root `oran_telemetry` wire-contract package.

```bash
cd rapps/agentic-rapp
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The defaults work when all three Python services share one VM. To override the
rApp settings, copy `.env.example` to `.env` and load it in the rApp terminal:

```bash
cp .env.example .env
set -a
source .env
set +a
```

If that file contains `DEEPSEEK_API_KEY`, do not load it in the DME or Evidence
API terminal. Configure those processes separately if their defaults need to
change.

To enable DeepSeek explanations, export `DEEPSEEK_API_KEY` in the rApp terminal.
For least privilege, do not expose that key to the DME process. The default
model is `deepseek-v4-flash`. If the key is absent or the API is unavailable,
the rApp continues with a local deterministic prose fallback.
The default `DEEPSEEK_TEMPERATURE=0.1` reduces response variation.

```bash
export DEEPSEEK_API_KEY=your_key_here
python main.py
```

## Run on the FlexRIC VM

Start components in this order:

1. R1/DME-lite, or the lab SMO's real R1/ICS implementation.
2. Health Evidence Producer.
3. Health Agent rApp.
4. FlexRIC, E2 node/gNB, and the Health xApp.

For the local DME-lite setup:

```bash
# terminal 1
cd rapps/agentic-rapp
source .venv/bin/activate
python app.py

# terminal 2
cd services/evidence-api
source .venv/bin/activate
python main.py

# terminal 3
cd rapps/agentic-rapp
source .venv/bin/activate
python main.py
```

Build and run the C Health xApp as described in
`xapps/health-xapp/README.md`. Once it begins publishing, ask the rApp:

```text
Is the RIC/E2 telemetry monitoring path healthy?
```

The terminal prints the locally verified monitoring-path summary followed by a
clearly labeled advisory DeepSeek interpretation instead of dumping the
complete report JSON.

## Stateful telemetry and conversation memory

The Health xApp already publishes one structured latest-value observation about
every second. The rApp stores each accepted R1 delivery directly in SQLite; it
does not create a text log or poll the latest snapshot into a duplicate file.
The default database is `data/rapp_memory.sqlite3` under the rApp directory and
SQLite WAL mode keeps the one-process receiver and memory worker safe to use
concurrently. The callback hands accepted snapshots to a bounded background
writer, so a SQLite lock cannot stall the R1 HTTP response. Queue drops and
write failures are counted and reported at shutdown.

The memory flow is:

```text
each accepted R1 observation -> SQLite telemetry_samples
60 seconds from first pending sample -> hidden DeepSeek digest call
                                   -> persisted telemetry_window digest

user question -> last completed digest
              + samples received since that digest (the partial window)
              + prior turns for this thread
              + frozen latest snapshot
              -> advisory DeepSeek explanation

frozen latest snapshot -> deterministic health checks -> authoritative status
```

The configured DeepSeek Chat Completions API is stateless. Sending data and
discarding the response would not make a later request remember it. Therefore,
the minute call returns a short advisory digest that the rApp stores locally and
explicitly supplies on later questions. The digest is not printed and cannot
alter a deterministic health result.

A window is 60 seconds, not exactly 60 records. Network delay, an xApp restart,
or missed R1 delivery can produce fewer samples. A one-second close grace lets
the asynchronous writer finish observations stamped near the boundary. If a
payload would exceed its configured size, raw samples are evenly compacted and
the included/omitted counts are sent explicitly. Deterministic
`all_sample_facts` still cover every received row, keep source instances and
units separate, and describe actual sequence gaps so sampled-row jumps are not
mistaken for lost delivery. When DeepSeek is disabled or a minute call fails, a
bounded factual digest is produced locally so ingestion and questions continue.
Failed minute calls are not retried automatically in this MVP; their local
factual digest remains available to questions.

Conversation state is isolated by `thread_id` and survives rApp restarts. The
terminal uses `RAPP_DEFAULT_THREAD_ID` (default `health-agent-cli`). Python
callers can select another conversation while keeping the existing API:

```python
from graph import ask, ask_structured

text = ask("What changed since the previous minute?", thread_id="operator-a")
result = ask_structured("And what about PRBs?", thread_id="operator-a")
```

For the existing verdict-free model boundary, future LLM calls receive prior
user questions and prior *DeepSeek advisory explanations*. Deterministic status,
the `HealthReport`, check results, and fallback prose are never replayed to the
model as conversation memory.

## DeepSeek evidence boundary

For a normal stateful question, the model receives one JSON user message with
this shape:

```json
{
  "question": "Is the RIC/E2 telemetry monitoring path healthy?",
  "evidence": {
    "evidence_schema_version": "1.1",
    "collected_at": "...",
    "delivery": {},
    "telemetry": {},
    "calculated_timing_ms": {},
    "metric_semantics": {},
    "interpretation_context": {}
  },
  "memory_context": {
    "memory_schema_version": "1.0",
    "thread_id": "health-agent-cli",
    "conversation": [],
    "completed_windows": [],
    "partial_window": {},
    "context_limits": {}
  }
}
```

Only allowlisted wire-contract telemetry, R1 delivery metadata, calculated
timing facts, static metric semantics, and configured freshness/required-metric
context cross the LLM boundary. Historical context contains only allowlisted
telemetry windows and advisory prose. The request never contains
`overall_status`, the deterministic `HealthReport`, check statuses, or another
application verdict. Telemetry, earlier questions, and saved model prose are
all treated as untrusted data rather than instructions.

Calculated timing values are in milliseconds and are derived in Python from
the frozen snapshot. DeepSeek is told to use those values instead of doing
timestamp arithmetic. `KPM.IndicationLatency` is also normalized from
microseconds when its unit and validity permit it. Cross-process wall-clock
deltas are labeled as such and are not treated as proven elapsed durations
unless the participating clocks are synchronized.

Metric semantics explicitly state that PRB measurements are not percentages,
no capacity denominators or performance thresholds are supplied, and zero
throughput/volume/delay is not a failure without independent traffic-demand
evidence. The prompt also prohibits treating independently flattened metric
values as necessarily correlated. Repository semantics are withheld when the
source or reported unit does not match the known Health xApp contract.

The freshness threshold is explicitly bound only to latest-KPM age, and the
future-skew allowance only to timestamp consistency. Neither may be reused as a
performance threshold for KPM indication latency or another metric.

DeepSeek is restricted to advisory evidence interpretation. Its prose remains
display-only and cannot change the deterministic status or report. Control
characters are removed before prose reaches the terminal. Questions and
serialized evidence also have configurable size limits to bound accidental API
cost.

External question and minute-digest requests can include the xApp source and
instance identifiers, R1 delivery timestamps, measurement names/values/units,
missing metric names, connection/count fields, recent advisory conversation,
and freshness context. With continuous evidence, minute digestion can add up to
1,440 model calls per day. Do not enable DeepSeek if this data, call volume, or
cost is unacceptable outside the lab environment.

The R1 callback has a configurable request-body limit but no application-layer
authentication in this lab MVP. Keep its default loopback bind or protect a
non-loopback deployment with the SMO/service mesh, firewall policy, and TLS.
The default memory directory is created owner-only and the SQLite database is
mode `0600`; operators using a custom existing parent directory must secure that
directory themselves.

The local graph deliberately calls DeepSeek before it creates the deterministic
monitoring-path report. It evaluates the same frozen snapshot afterward,
preserves the report in the structured application result, and uses it for
fallback if the interpretation call fails. Phase 1 is explanation-only and
has no tools: DeepSeek cannot execute commands or change the testbed. State is
owned and bounded by the local rApp database rather than by the model provider.

## Health evaluation

Freshness comes from the xApp's `last_kpm_indication_at` timestamp, not HTTP
delivery time. These labels apply only to the
`ric_e2_kpm_telemetry_monitoring_path` scope; they do not assess the 5G core,
UE/PDU-session state, end-to-end reachability, offered traffic, or subscriber
performance. The rApp reports:

- `healthy` when all critical checks pass;
- `degraded` when evidence is fresh but metrics are missing/invalid;
- `unhealthy` when RIC/E2/KPM/freshness checks fail;
- `unknown` before any valid evidence is delivered.

Each stored row still contains the xApp's flat latest-value metric map. History
does not add per-UE/per-node identity, synchronized aggregation windows,
capacity denominators, or configured performance thresholds that the source did
not provide.

## Configuration

`config.py` reads environment variables directly. Bind addresses control where
a service listens; advertised callback URLs must be reachable by the other VM
processes. Never advertise `0.0.0.0` as a callback address. See `.env.example`
for memory cadence, context limits, raw retention, retained digest count, and
database-path settings. Raw samples default to 24 hours of retention; the
latest 1,440 completed digests and 100 turns per conversation are retained.
Retention pruning also runs periodically when no new window completes.

Run the rApp regression suite with:

```bash
cd rapps/agentic-rapp
source .venv/bin/activate
python -m pytest -q
```
