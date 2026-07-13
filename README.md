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