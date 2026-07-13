# Automated-5G-Agentic-AI
Agentic AI for a live 5G/O-RAN testbed that can monitor system health, run traffic experiments, collect core/RAN/RIC/xApp metrics, answer user questions, diagnose unhealthy states, and safely execute approved recovery or experiment commands.

The agent collects live system state from the testbed, converts the results into structured JSON, generates deterministic diagnostics, and sends the structured evidence to an LLM for a human-readable explanation.

## Current Testbed

The current development environment uses:

* OAI 5G Core
* OAI gNB
* OAI nrUE
* FlexRIC near-RT RIC
* Existing xApps such as KPIMON for future KPM metric integration
* DeepSeek API for LLM-based explanation

Current data collection is local to the VM, but the project is being designed so the same agent framework can later be adapted to a real lab 5G system through SSH, APIs, log files, monitoring tools, or O-RAN/xApp interfaces.

## Current Features
### Live System-State Collection

The agent currently collects:

* OAI core Docker container status
* Required core container health
* UE tunnel/interface status
* UE IP address
* Uplink ping test from UE tunnel to external data network
* Downlink ping test from external data network to UE
* Packet loss and average RTT from ping output
* Overall testbed health status

### Deterministic Diagnostics

The project includes a deterministic diagnostics layer in `diagnostics.py`.

This layer converts raw system-state data into structured findings with:

* component
* status
* severity
* evidence
* suggested safe action

Example diagnostic finding:

```
{
  "component": "UE",
  "status": "not connected",
  "severity": "critical",
  "evidence": "The oaitun_ue1 interface does not exist.",
  "suggested_action": "Start the gNB and nrUE, then verify with: ip addr show oaitun_ue1"
}
```

This makes the LLM an explanation layer on top of verified tool output instead of relying on the LLM to interpret raw terminal output by itself.

### Interactive Terminal Agent

The project includes a terminal chatbot interface. A user can ask questions such as:

* Is the testbed healthy right now?
* Is the UE connected?
* Are uplink and downlink traffic working?
* What evidence supports your answer?
* What should I check first if the system is unhealthy?

For each question, the agent:

1. Collects fresh live system state
2. Builds deterministic diagnostics
3. Sends the structured state and diagnostics to the LLM
4. Prints a natural-language answer
5. Saves the question, state, diagnostics, and answer as a session record

## Setup

Create and activate a virtual environment:

`python3 -m venv .venv`

`source .venv/bin/activate`

Install dependencies:

`pip install -r requirements.txt`

Create a .env file in the repo root with the following line of code:

`DEEPSEEK_API_KEY=your_key_here`

The .env file should not be committed to GitHub.

## Running

One shot health check:

`source .venv/bin/activate`

`PYTHONPATH=src python -m oran_agent.`

This collects live system state once, prints the JSON output, and sends the result to the LLM.

Running the Interactive Terminal Agent:

`source .venv/bin/activate`

`PYTHONPATH=src python -m oran_agent.chat`

OR use the helper script: `./scripts/run_chat.sh`


Fault Injection Tests:
- Core down: docker compose down
- UE missing: core running without gNB/UE
- External DN failure: docker stop oai-ext-dn
- UPF failure: docker stop oai-upf
- AMF failure: docker stop oai-amf
- gNB/UE process failure: Ctrl+C corresponding process

## Agentic Core (LangGraph Orchestration Layer)

The `agentic_core/` directory contains the agent architecture, built on LangGraph. Instead of calling an LLM directly for each user question, `agentic_core/` introduces a graph-based orchestration layer intended to support multi-step reasoning, tool use, and persistent state across turns, serving as the foundation for Dual-Brain architecture.

Note: `agentic_core/` uses its own `requirements.txt` and virtual environment, separate from `legacy/python-chatbot/`.

### Current Status

- LangGraph is installed and confirmed working end-to-end: a single-node graph (`brain1`) builds, compiles, and executes correctly against a local LLM served via  Ollama.
- Currently running `llama3.2:3b` for development/testing purposes.
- A Postgres-backed checkpointer (`config/db.py`) has been written to persist graph state across turns and sessions, but is not yet wired into the active graph or tested against a running Postgres instance.
- No tool nodes are implemented yet. Brain 1 currently has no access to live testbed data — wiring the existing `diagnostics.py` / `collectors/kpm.py` logic (or the xApp's E2 summary output) in as LangGraph tools is the next planned step.
- Brain 2 (ONNX classifiers) and the PAOR (Perceive–Act–Observe–Reflect) loop structure have not been started.

### Setup

Requires Python 3.10+. On Ubuntu 20.04 (focal), the deadsnakes PPA no longer supports focal following its April 2025 EOL, so Python is installed via [`uv`](https://github.com/astral-sh/uv) instead of apt:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env
uv python install 3.11
```

Create the virtual environment and install dependencies:

```bash
cd agentic_core
uv venv --python 3.11 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in real values:

```bash
cp .env.example .env
```

Ollama must be installed and running locally (or reachable over LAN) with the target model pulled:

```bash
curl -fsSL https://ollama.com/install.sh | sh
ollama pull llama3.2:3b
```

### Running the Smoke Test

```bash
cd agentic_core
source .venv/bin/activate
python run_test.py
```

This builds the graph, prints its structure as a Mermaid diagram, sends a test message through Brain 1, and prints the model's response.
