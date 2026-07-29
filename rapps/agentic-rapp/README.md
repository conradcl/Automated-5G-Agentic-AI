# Read-only Health Agent rApp MVP

The rApp subscribes to live Health xApp evidence through the lab R1/DME
interface and performs deterministic health checks with LangGraph. It does not
read xApp JSON or log files.

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

The defaults work when all three Python services share one VM. To override
them, copy `.env.example` to `.env` and export it in every rApp/DME terminal:

```bash
cp .env.example .env
set -a
source .env
set +a
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
Is the system healthy?
```

## Health evaluation

Freshness comes from the xApp's `last_kpm_indication_at` timestamp, not HTTP
delivery time. The rApp reports:

- `healthy` when all critical checks pass;
- `degraded` when evidence is fresh but metrics are missing/invalid;
- `unhealthy` when RIC/E2/KPM/freshness checks fail;
- `unknown` before any valid evidence is delivered.

The flat metric map represents the latest value reported in an indication. It
does not yet retain per-UE or per-node metric identity.

## Configuration

`config.py` reads environment variables directly. Bind addresses control where
a service listens; advertised callback URLs must be reachable by the other VM
processes. Never advertise `0.0.0.0` as a callback address.
