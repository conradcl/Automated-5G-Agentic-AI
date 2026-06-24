# Automated-5G-Agentic-AI
Agentic AI for a live 5G/O-RAN testbed that can monitor system health, run traffic experiments, collect core/RAN/RIC/xApp metrics, answer user questions, diagnose unhealthy states, and safely execute approved recovery or experiment commands.

To run use: `./scripts/run_chat.sh`


Fault Injection Tests:
- Core down: docker compose down
- UE missing: core running without gNB/UE
- External DN failure: docker stop oai-ext-dn
- UPF failure: docker stop oai-upf
- AMF failure: docker stop oai-amf
- gNB/UE process failure: Ctrl+C corresponding process