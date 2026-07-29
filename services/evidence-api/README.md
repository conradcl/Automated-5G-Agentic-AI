# O-RAN Health Evidence Producer

This service is the network and R1 boundary between the Health xApp and
read-only rApps.

```text
Health xApp -> POST /v1/evidence -> evidence repository
                                      |
                                      +-> R1 Information Job deliveries
```

The service stores evidence but does not score network health. Deterministic
health interpretation remains in the Health Agent rApp.

## Run

Start DME first, then run this service from its directory:

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

The default API is at `http://127.0.0.1:9991`; OpenAPI documentation is at
`/docs`.

## Interfaces

```text
POST   /v1/evidence
GET    /v1/evidence/latest
GET    /v1/evidence/history?limit=20
GET    /healthz
GET    /readyz

GET    /producer/health-check
POST   /producer/info-job
DELETE /producer/info-job/{job_id}
```

The first group is used by xApp publishers and operators. The second group is
the producer callback interface used by R1/DME.

## Storage

`EVIDENCE_BACKEND=memory` is bounded and appropriate only for tests or the
single-process simulator. For deployment, set:

```text
EVIDENCE_BACKEND=postgres
EVIDENCE_DATABASE_URL=postgresql://...
```

The PostgreSQL repository creates its schema on startup and preserves ordering
by `(source, source_instance_id, sequence_number)` across service restarts.

There is no xApp JSON/log-file fallback. Operational application logs should be
collected from stdout by the deployment platform; health evidence is always a
structured network message.
