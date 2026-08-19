# Automated Health Agent rApp MVP

The rApp subscribes to live Health xApp evidence through the lab R1/DME
interface, performs deterministic checks for the RIC/E2 KPM telemetry
monitoring path with LangGraph, and can ask DeepSeek to interpret the evidence
in concise advisory prose. When a question needs more evidence, DeepSeek can
select from a fixed catalog of bounded, read-only collectors before answering.
The rApp consumes telemetry JSON over the network; it does not scrape xApp
output files, logs, or terminal text. Accepted observations and conversation
context are retained as structured records in a local SQLite database so the
rApp can reason over recent history and survive restarts.

```text
E2 node -> Health xApp -> Evidence API -> R1/DME job -> rApp -> LangGraph
```

`app.py` is the lab R1/DME-lite broker, not the controller. `main.py`,
`consumer.py`, `memory.py`, `read_tools.py`, `health_checks.py`, and `graph.py`
form the evidence, memory, and diagnostic LangGraph path.

`automation.py` adds a separate policy-controlled background loop for automatic
incident notice, diagnosis, remediation, and verification. Read tools and the
LLM remain advisory and read-only; only the local deterministic controller can
invoke the fixed remediation adapters.

## Prompt-free incident automation

No user question is required for monitoring or recovery. When
`RAPP_AUTOMATION_ENABLED=true`, the rApp:

1. evaluates the latest frozen telemetry every five seconds;
2. runs the memory-aware advisory LangGraph on a separate thread every 60
   seconds using the isolated `health-agent-automation` conversation, so model
   or read-tool latency cannot block health polling; automation and the CLI own
   separate compiled graph instances, so a manual question cannot pause the
   scheduled graph;
3. opens an incident after three consecutive identical critical failures;
4. automatically runs an incident-specific LangGraph diagnosis before any
   write action;
5. applies a deterministic policy gate: delivery/freshness faults can reconcile
   the configured R1 Information Job and xApp/KPM faults can additionally use
   one configured xApp restart; unsupported faults such as an isolated E2-node
   failure escalate without a blind write;
6. verifies recovery only when deterministic health passes and telemetry either
   advances on the same xApp source instance or arrives from a valid new source
   instance after its expected sequence reset;
7. records a durable action intent before execution, followed by the bounded
   result, verification, failure, or escalation in the `automation_events`
   SQLite table; audit failure therefore prevents a write action; and
8. runs as the existing interactive CLI on a TTY or remains alive as a headless
   service when stdin is closed, until SIGTERM or Ctrl-C.

The default second-stage restart backend is `disabled`. To enable it, configure
exactly one operator-owned target, for example:

```bash
export RAPP_AUTOMATION_XAPP_RESTART_BACKEND=docker
export RAPP_AUTOMATION_XAPP_RESTART_TARGET=health-xapp
```

Supported backends are `docker` and `systemd`. They use fixed absolute
executables, `shell=False`, a validated literal target, a bounded timeout, one
second-stage attempt, and a cooldown. DeepSeek never receives a write-tool
catalog and cannot provide a command or target. Docker daemon access remains
host-privileged and should only be granted where this recovery policy is
acceptable.

DeepSeek does make agentic diagnostic decisions: within LangGraph it decides
whether more evidence is needed, selects one bounded read tool at a time, sees
validated results and durable local context, and decides when it can answer.
The resulting diagnosis gates incident handling, but production-safe write
authorization remains deterministic. One scheduled graph assessment is not
necessarily one provider HTTP request: bounded tool selection can require
multiple model turns, and telemetry-window digest generation is a separate
memory workflow.

Every scheduled assessment is printed, including a healthy result. Manual CLI
questions do not reset or suppress the automation cadence; both graph instances
share the thread-safe telemetry store while retaining separate conversation
threads.

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
Visible explanations normally use three to seven short sentences or bullets and
stay within 250 words; an explicitly detailed request may use up to 350 words.
The 1,200-token setting is a completion safety ceiling, not a target. Hidden
minute summaries allow 800 tokens by default. If DeepSeek reports
`finish_reason=length`, the client retries once with a bounded larger output
budget and a stricter brevity instruction. This retry changes only response
length handling; the verdict-free evidence boundary and deterministic health
authority remain unchanged.

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

## Model-selected read-only evidence tools

When `RAPP_READ_TOOLS_ENABLED=true` and DeepSeek is configured, the graph lets
the model request additional evidence only when the question needs it. The
catalog contains seven fixed, zero-argument tools:

| Tool | Evidence returned |
| --- | --- |
| `get_dme_job_status` | DME liveness/counts and the configured Information Job's registration, state, producer IDs, owner, information type, and callback matches |
| `get_evidence_pipeline_status` | Evidence API liveness/readiness, repository and DME-registration flags, producer liveness, evidence-received flag, and active-job count |
| `get_evidence_history` | A bounded, normalized set of canonical Evidence API events; raw bodies and full metric values are omitted |
| `get_recent_telemetry_windows` | Deterministic facts for recent SQLite windows; saved model digests and arbitrary SQL are not exposed |
| `get_sequence_advancement` | Per-source-instance sequence advancement, observed gaps, and non-increasing observations over a bounded lookback |
| `ping_ue_path` | One bounded uplink ICMP sample from the configured UE interface to the configured literal IP, returning packet loss and RTT statistics |
| `get_oai_container_status` | Runtime and health state for only the configured OAI container allowlist |

The model supplies no arguments. HTTP origins, job identity, ping destination,
interface, count, and container names all come from operator configuration.
HTTP collectors use fixed GET paths, reject redirects, cap response bodies, and
ignore environment proxy variables. Process collectors use fixed absolute
executables with `shell=False`; no model-provided shell, SQL, HTTP, SSH, Docker,
interface, destination, or container argument is accepted. A tool name is used
at most once per question, and the complete loop is capped by
`RAPP_READ_TOOL_MAX_CALLS`. An explicit positive instruction such as
`Use ping_ue_path and get_oai_container_status` deterministically queues those
allowlisted reads in the named order; mere mentions and negative instructions
do not execute tools. Semantic requests without exact names remain
model-selected.

These observations have deliberately narrow meanings:

- DME `ENABLED` does not prove that R1 delivery succeeded.
- An Evidence API active-job count does not prove that the configured job is active.
- A sequence gap means that this rApp did not retain an observation; it is not proof of IP packet loss. Duplicate deliveries are not observable after SQLite uniqueness filtering.
- The ping is a bounded active probe, not a complete subscriber-performance assessment.
- Container runtime/health state does not prove that the application or 5G service is correct.

`ask_structured(...)` exposes execution metadata under `answer["read_tools"]`,
including tools used, per-tool outcome and elapsed time, total tool time,
bounded results, stop reason, and any planning error. The interactive CLI emits
one compact receipt before the answer, for example
`[TOOLS] get_dme_job_status completed in 0.4s.` The follow-up DeepSeek request
is explicitly told that supplied results came from completed rApp tool calls,
so it uses those results instead of disclaiming tool access. Existing `ask(...)`
behavior and the deterministic health report remain intact. If DeepSeek, a
collector, or result validation fails, the graph safely uses the existing
explanation/fallback path.

## DeepSeek evidence boundary

For a normal stateful question, the model receives one JSON user message with
this shape (the native function catalog is carried separately):

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
  "read_tool_results": [],
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

The first planning request contains an empty result array. After each selected
tool, the graph sends the same frozen evidence and bounded memory context plus
the validated results collected so far. Every advertised function has an
empty-object parameter schema.

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
and freshness context. Tool-enabled questions can additionally include
DME/Evidence API status and normalized history, deterministic SQLite
window/sequence facts, the configured ping interface and target plus loss/RTT,
and allowlisted container names and runtime states. With continuous evidence,
minute digestion can add up to 1,440 model calls per day, before any extra
question-time planning calls. Do not enable DeepSeek if this data, call volume,
or cost is unacceptable outside the lab environment.

The R1 callback has a configurable request-body limit but no application-layer
authentication in this lab MVP. Keep its default loopback bind or protect a
non-loopback deployment with the SMO/service mesh, firewall policy, and TLS.
The default memory directory is created owner-only and the SQLite database is
mode `0600`; operators using a custom existing parent directory must secure that
directory themselves.

The local graph freezes the latest snapshot before any model call. DeepSeek may
then select up to `RAPP_READ_TOOL_MAX_CALLS` distinct tools from the fixed
catalog, one at a time. Each result is validated, bounded, and returned as
untrusted evidence for the next planning step. After the model returns advisory
prose, or tool planning safely falls back, the graph evaluates the original
frozen snapshot with deterministic health checks.

Read-tool results can inform only the advisory explanation. They are never
passed to `evaluate_health`, cannot change `overall_status`, and cannot execute
repairs or other mutations. State remains owned and bounded by the local rApp
database rather than by the model provider.

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

Read-tool limits are validated when the registry starts. The supported ranges
are:

| Setting | Supported range |
| --- | --- |
| `RAPP_READ_TOOL_MAX_CALLS` | 1–7 distinct calls per question |
| `RAPP_READ_TOOL_HTTP_TIMEOUT_S` | greater than 0 through 30 seconds |
| `RAPP_READ_TOOL_HTTP_MAX_BODY_BYTES` | 1,024–2,000,000 bytes per response |
| `RAPP_READ_TOOL_MAX_RESULT_CHARS` | 1,000–200,000 serialized characters |
| `RAPP_READ_TOOL_HISTORY_LIMIT` | 1–120 events |
| `RAPP_READ_TOOL_WINDOW_LIMIT` | 1–10 completed windows |
| `RAPP_READ_TOOL_WINDOW_MAX_SAMPLES` | 1–10,000 raw samples shared across requested windows |
| `RAPP_READ_TOOL_WINDOW_MAX_BYTES` | 65,536–50,000,000 stored JSON bytes shared across requested windows |
| `RAPP_READ_TOOL_SEQUENCE_LOOKBACK_S` | 5–300 seconds |
| `RAPP_READ_TOOL_SEQUENCE_MAX_SAMPLES` | 2–2,000 samples |
| `RAPP_READ_TOOL_PING_COUNT` | 1–10 requests |
| `RAPP_READ_TOOL_PING_REPLY_TIMEOUT_S` | 1–10 seconds per reply |
| `RAPP_READ_TOOL_COMMAND_TIMEOUT_S` | 1–30 seconds |
| `RAPP_READ_TOOL_OAI_CONTAINERS` | 1–16 unique, safely named containers |

The window sample and stored-byte budgets are shared across all requested
windows. If retained raw rows were pruned, incomplete, or would exceed either
budget, the tool reports facts as unavailable for the affected window instead
of exposing a stored model digest or silently returning partial facts.

### Read-tool operational requirements

`DME_BASE_URL` and `EVIDENCE_API_BASE_URL` must be directly reachable from the
rApp when their tools are selected. For tool use, both values must be HTTP(S)
origins without credentials, paths, queries, or fragments. Tool HTTP requests
do not follow redirects or use environment-configured proxies. Keep the APIs on
loopback or protect non-loopback deployments with network policy and TLS.

`/usr/bin/ping` and `/usr/bin/docker` are required only when their tools are
selected. The ping tool may require the host's normal ICMP capability, and the
configured UE interface must exist. It sends up to the configured number of
ICMP requests, so it is non-mutating but is not a passive observation.

Container inspection requires access to the Docker daemon. Docker socket or
Docker-group access is effectively host-level privilege even though this rApp
invokes only a fixed, read-only inspect operation. Do not grant Docker access
solely for this feature without accepting that risk. Set
`RAPP_READ_TOOLS_ENABLED=false` when the model-selected collectors are not
appropriate; the existing explanation and deterministic-fallback behavior
continues without them.

Run the rApp regression suite with:

```bash
cd rapps/agentic-rapp
source .venv/bin/activate
python -m pytest -q
```
