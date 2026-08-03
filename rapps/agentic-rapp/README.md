# Read-only Health Agent rApp MVP

The rApp subscribes to live Health xApp evidence through the lab R1/DME
interface, performs deterministic checks for the RIC/E2 KPM telemetry
monitoring path with LangGraph, and can ask DeepSeek to interpret the evidence
in concise advisory prose. It consumes telemetry JSON over the network; it does
not scrape xApp output files, logs, or terminal text.

```text
E2 node -> Health xApp -> Evidence API -> R1/DME job -> rApp -> LangGraph
```

`app.py` is the lab R1/DME-lite broker, not the controller. `main.py`,
`consumer.py`, `health_checks.py`, and `graph.py` form the read-only rApp.

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

## DeepSeek evidence boundary

The model receives one JSON user message with this shape:

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
  }
}
```

Only allowlisted wire-contract telemetry, R1 delivery metadata, calculated
timing facts, static metric semantics, and configured freshness/required-metric
context cross the LLM boundary. The request never contains `overall_status`,
the deterministic `HealthReport`, check statuses, a summary, or another
application verdict. Telemetry strings are treated as untrusted data rather
than instructions.

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

The external request includes the xApp source and instance identifiers, R1 job
identity and delivery timestamps, measurement names/values/units, missing metric
names, connection/count fields, and freshness context. Do not enable DeepSeek if
those fields are not permitted to leave the lab environment.

The local graph deliberately calls DeepSeek before it creates the deterministic
monitoring-path report. It evaluates the same frozen snapshot afterward,
preserves the report in the structured application result, and uses it for
fallback if the interpretation call fails. Phase 1 is explanation-only and
stateless: DeepSeek has no tools, receives no conversation history, and cannot
execute commands or change the testbed.

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

The flat metric map represents the latest value reported in an indication. It
does not yet retain per-UE or per-node metric identity, a historical baseline,
aggregation-window details, or a capacity denominator.

## Configuration

`config.py` reads environment variables directly. Bind addresses control where
a service listens; advertised callback URLs must be reachable by the other VM
processes. Never advertise `0.0.0.0` as a callback address.
