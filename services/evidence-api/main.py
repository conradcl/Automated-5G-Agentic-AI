"""Network-native Health Evidence Producer for the read-only rApp.

The service accepts the canonical Health xApp telemetry contract over HTTP,
stores accepted observations, registers itself as an R1/DME data producer, and
delivers new evidence to every active Information Job. It never reads xApp
output files and never decides whether the network is healthy; that decision
belongs to the rApp's deterministic health checks.
"""
from __future__ import annotations

import copy
import json
import logging
import os
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Protocol

import requests
from dotenv import load_dotenv
from fastapi import Body, FastAPI, HTTPException, Query, Response
from fastapi.responses import JSONResponse


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from oran_telemetry import (  # noqa: E402
    isoformat_utc,
    parse_timestamp,
    validate_telemetry_payload,
)


load_dotenv(Path(__file__).with_name(".env"))
logger = logging.getLogger("health-evidence-producer")

DME_BASE_URL = os.getenv("DME_BASE_URL", "http://127.0.0.1:9990")
INFO_TYPE_ID = os.getenv("INFO_TYPE_ID", "oran-health-monitor-kpm-v1")
EVIDENCE_PRODUCER_ID = os.getenv(
    "EVIDENCE_PRODUCER_ID", "health-evidence-producer-1"
)
EVIDENCE_API_BIND_HOST = os.getenv("EVIDENCE_API_BIND_HOST", "127.0.0.1")
EVIDENCE_API_PORT = int(os.getenv("EVIDENCE_API_PORT", "9991"))
EVIDENCE_API_BASE_URL = os.getenv(
    "EVIDENCE_API_BASE_URL", f"http://127.0.0.1:{EVIDENCE_API_PORT}"
)
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "5"))
REGISTRATION_RETRY_S = float(os.getenv("REGISTRATION_RETRY_S", "5"))
REGISTRATION_RECONCILE_S = float(os.getenv("REGISTRATION_RECONCILE_S", "30"))
AUTO_REGISTER_WITH_DME = os.getenv("AUTO_REGISTER_WITH_DME", "true").lower() in {
    "1",
    "true",
    "yes",
}
EVIDENCE_BACKEND = os.getenv("EVIDENCE_BACKEND", "memory").lower()
EVIDENCE_DATABASE_URL = os.getenv("EVIDENCE_DATABASE_URL", "")
EVIDENCE_MEMORY_LIMIT = int(os.getenv("EVIDENCE_MEMORY_LIMIT", "10000"))


INFO_TYPE_SCHEMA = {
    "info_job_data_schema": {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "title": "oran-health-monitor-kpm-v1-subscription",
        "description": "Subscription parameters for Health xApp KPM evidence",
        "type": "object",
        "properties": {"callback_url": {"type": "string", "format": "uri"}},
    }
}


class EvidenceRepository(Protocol):
    def accept(self, payload: dict) -> str: ...

    def latest(self) -> Optional[dict]: ...

    def history(self, limit: int) -> list[dict]: ...

    def healthcheck(self) -> bool: ...


class InMemoryEvidenceRepository:
    """Bounded repository for tests and short, single-process lab runs."""

    def __init__(self, limit: int = EVIDENCE_MEMORY_LIMIT) -> None:
        self._limit = max(1, limit)
        self._lock = threading.RLock()
        self._events: list[dict] = []
        self._last_sequences: dict[tuple[str, str], int] = {}

    def accept(self, payload: dict) -> str:
        key = (payload["source"], payload["source_instance_id"])
        sequence = payload["sequence_number"]
        with self._lock:
            previous = self._last_sequences.get(key)
            if previous is not None:
                if sequence == previous:
                    return "duplicate"
                if sequence < previous:
                    return "out_of_order"
            self._last_sequences[key] = sequence
            self._events.append(copy.deepcopy(payload))
            if len(self._events) > self._limit:
                del self._events[: len(self._events) - self._limit]
            return "accepted"

    def latest(self) -> Optional[dict]:
        with self._lock:
            return copy.deepcopy(self._events[-1]) if self._events else None

    def history(self, limit: int) -> list[dict]:
        with self._lock:
            return copy.deepcopy(self._events[-limit:])

    def healthcheck(self) -> bool:
        return True


class PostgresEvidenceRepository:
    """Durable repository for a deployed, multi-process Evidence API."""

    def __init__(self, database_url: str) -> None:
        if not database_url:
            raise ValueError("EVIDENCE_DATABASE_URL is required for postgres")
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - deployment dependency check
            raise RuntimeError("Install psycopg[binary] for the postgres backend") from exc
        self._psycopg = psycopg
        self._database_url = database_url
        self._ensure_schema()

    def _connect(self):
        return self._psycopg.connect(self._database_url)

    def _ensure_schema(self) -> None:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS health_evidence (
                    id BIGSERIAL PRIMARY KEY,
                    source TEXT NOT NULL,
                    source_instance_id TEXT NOT NULL,
                    sequence_number BIGINT NOT NULL,
                    observed_at TIMESTAMPTZ NOT NULL,
                    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    payload JSONB NOT NULL,
                    UNIQUE (source, source_instance_id, sequence_number)
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS health_evidence_received_idx
                ON health_evidence (received_at DESC, id DESC)
                """
            )

    def accept(self, payload: dict) -> str:
        source = payload["source"]
        instance = payload["source_instance_id"]
        sequence = payload["sequence_number"]
        observed_at = parse_timestamp(payload["observed_at"])
        with self._connect() as connection, connection.cursor() as cursor:
            # Serialize ordering decisions for this source instance across API
            # replicas before checking and inserting the new sequence.
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"{source}\0{instance}",),
            )
            cursor.execute(
                """
                SELECT sequence_number
                FROM health_evidence
                WHERE source = %s AND source_instance_id = %s
                ORDER BY sequence_number DESC
                LIMIT 1
                """,
                (source, instance),
            )
            row = cursor.fetchone()
            if row is not None:
                previous = row[0]
                if sequence == previous:
                    return "duplicate"
                if sequence < previous:
                    return "out_of_order"
            cursor.execute(
                """
                INSERT INTO health_evidence
                    (source, source_instance_id, sequence_number, observed_at, payload)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                """,
                (source, instance, sequence, observed_at, json.dumps(payload)),
            )
        return "accepted"

    def latest(self) -> Optional[dict]:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT payload FROM health_evidence
                ORDER BY received_at DESC, id DESC LIMIT 1
                """
            )
            row = cursor.fetchone()
            return copy.deepcopy(row[0]) if row else None

    def history(self, limit: int) -> list[dict]:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT payload FROM health_evidence
                ORDER BY received_at DESC, id DESC LIMIT %s
                """,
                (limit,),
            )
            # API history is chronological even though the efficient query is
            # newest-first.
            return [copy.deepcopy(row[0]) for row in reversed(cursor.fetchall())]

    def healthcheck(self) -> bool:
        try:
            with self._connect() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                return cursor.fetchone() == (1,)
        except Exception:
            return False


def _create_repository() -> EvidenceRepository:
    if EVIDENCE_BACKEND == "memory":
        return InMemoryEvidenceRepository()
    if EVIDENCE_BACKEND == "postgres":
        return PostgresEvidenceRepository(EVIDENCE_DATABASE_URL)
    raise ValueError(f"Unsupported EVIDENCE_BACKEND: {EVIDENCE_BACKEND!r}")


_repository: EvidenceRepository = _create_repository()
_jobs_lock = threading.RLock()
_active_jobs: dict[str, dict] = {}
_registration_lock = threading.RLock()
_dme_registered = False
_registration_stop = threading.Event()
_registration_thread: Optional[threading.Thread] = None


def _set_registered(value: bool) -> None:
    global _dme_registered
    with _registration_lock:
        _dme_registered = value


def register_with_dme() -> None:
    response = requests.put(
        f"{DME_BASE_URL}/data-producer/v1/info-types/{INFO_TYPE_ID}",
        json=INFO_TYPE_SCHEMA,
        timeout=HTTP_TIMEOUT_S,
    )
    response.raise_for_status()
    registration = {
        "info_producer_supervision_callback_url": (
            f"{EVIDENCE_API_BASE_URL}/producer/health-check"
        ),
        "info_job_callback_url": f"{EVIDENCE_API_BASE_URL}/producer/info-job",
        "supported_info_types": [INFO_TYPE_ID],
    }
    response = requests.put(
        f"{DME_BASE_URL}/data-producer/v1/info-producers/{EVIDENCE_PRODUCER_ID}",
        json=registration,
        timeout=HTTP_TIMEOUT_S,
    )
    response.raise_for_status()
    _set_registered(True)


def _registration_worker() -> None:
    # Wait until this service's advertised callback URL is reachable. DME may
    # immediately replay existing jobs during registration.
    while not _registration_stop.is_set():
        try:
            requests.get(
                f"{EVIDENCE_API_BASE_URL}/healthz", timeout=HTTP_TIMEOUT_S
            ).raise_for_status()
            break
        except requests.RequestException:
            _registration_stop.wait(REGISTRATION_RETRY_S)

    # Register on every producer process start, even when DME still has the
    # previous registration. Its idempotent PUT replays existing Information
    # Jobs so this process can rebuild its in-memory active-job set.
    while not _registration_stop.is_set():
        try:
            register_with_dme()
            break
        except requests.RequestException as exc:
            _set_registered(False)
            logger.warning("Initial DME registration failed: %s", exc)
            _registration_stop.wait(REGISTRATION_RETRY_S)

    while not _registration_stop.is_set():
        try:
            response = requests.get(
                f"{DME_BASE_URL}/data-producer/v1/info-producers/{EVIDENCE_PRODUCER_ID}",
                timeout=HTTP_TIMEOUT_S,
            )
            if response.status_code == 404:
                register_with_dme()
            else:
                response.raise_for_status()
                _set_registered(True)
            _registration_stop.wait(REGISTRATION_RECONCILE_S)
        except requests.RequestException as exc:
            _set_registered(False)
            logger.warning("DME registration/reconciliation failed: %s", exc)
            _registration_stop.wait(REGISTRATION_RETRY_S)


def start_registration_worker() -> None:
    global _registration_thread
    if _registration_thread is not None and _registration_thread.is_alive():
        return
    _registration_stop.clear()
    _registration_thread = threading.Thread(
        target=_registration_worker,
        name="dme-registration",
        daemon=True,
    )
    _registration_thread.start()


def stop_registration_worker() -> None:
    _registration_stop.set()
    if _registration_thread is not None:
        _registration_thread.join(timeout=HTTP_TIMEOUT_S + 1)


def _delivery_envelope(job_id: str, telemetry: dict) -> dict:
    return {
        "info_job_identity": job_id,
        "producer_pushed_at": isoformat_utc(datetime.now(timezone.utc)),
        "telemetry": telemetry,
    }


def _deliver_to_job(job_id: str, target_uri: str, telemetry: dict) -> dict:
    try:
        response = requests.post(
            target_uri,
            json=_delivery_envelope(job_id, telemetry),
            timeout=HTTP_TIMEOUT_S,
        )
        response.raise_for_status()
        return {"job_id": job_id, "status": "delivered"}
    except requests.RequestException as exc:
        logger.warning("Evidence delivery failed for job %s: %s", job_id, exc)
        return {"job_id": job_id, "status": "failed", "detail": str(exc)}


def _deliver_to_active_jobs(telemetry: dict) -> list[dict]:
    with _jobs_lock:
        jobs = {
            job_id: registration["target_uri"]
            for job_id, registration in _active_jobs.items()
        }
    return [
        _deliver_to_job(job_id, target_uri, telemetry)
        for job_id, target_uri in jobs.items()
    ]


@asynccontextmanager
async def lifespan(_: FastAPI):
    if AUTO_REGISTER_WITH_DME:
        start_registration_worker()
    yield
    stop_registration_worker()


app = FastAPI(
    title="O-RAN Health Evidence Producer",
    description="Network ingestion, durable evidence, and R1/DME data production",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
def root() -> dict:
    return {
        "service": "oran-health-evidence-producer",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "living"}


@app.get("/readyz")
def readyz(response: Response) -> dict:
    repository_ready = _repository.healthcheck()
    with _registration_lock:
        dme_ready = _dme_registered
    ready = repository_ready and dme_ready
    if not ready:
        response.status_code = 503
    return {
        "status": "ready" if ready else "not-ready",
        "repository": repository_ready,
        "dme_registered": dme_ready,
    }


@app.post("/v1/evidence")
def ingest_evidence(payload: Any = Body(...)) -> JSONResponse:
    errors = validate_telemetry_payload(payload)
    if errors:
        return JSONResponse(
            status_code=400,
            content={"status": "rejected", "errors": errors},
        )

    outcome = _repository.accept(payload)
    if outcome == "duplicate":
        return JSONResponse(
            status_code=200,
            content={"status": "duplicate", "deliveries": []},
        )
    if outcome == "out_of_order":
        return JSONResponse(
            status_code=409,
            content={
                "status": "rejected",
                "errors": ["sequence_number is older than the last accepted message"],
            },
        )

    deliveries = _deliver_to_active_jobs(payload)
    return JSONResponse(
        status_code=202,
        content={"status": "accepted", "deliveries": deliveries},
    )


@app.get("/v1/evidence/latest")
def latest_evidence() -> dict:
    latest = _repository.latest()
    if latest is None:
        raise HTTPException(status_code=404, detail="No evidence received yet")
    return latest


@app.get("/v1/evidence/history")
def evidence_history(limit: int = Query(default=20, ge=1, le=1000)) -> dict:
    events = _repository.history(limit)
    return {"count": len(events), "events": events}


# R1/DME producer callbacks.
@app.get("/producer/health-check")
def producer_health_check() -> dict:
    return {
        "status": "living",
        "repository": _repository.healthcheck(),
        "evidence_received": _repository.latest() is not None,
        "active_jobs": len(_active_jobs),
    }


@app.post("/producer/info-job")
def info_job_created(body: Any = Body(...)) -> JSONResponse:
    if not isinstance(body, dict) or not isinstance(
        body.get("info_job_identity"), str
    ):
        return JSONResponse(
            status_code=400,
            content={"status": 400, "detail": "invalid Information Job"},
        )

    job_id = body["info_job_identity"]
    target_uri = body.get("target_uri")
    if not target_uri and isinstance(body.get("info_job_data"), dict):
        target_uri = body["info_job_data"].get("callback_url")
    if not isinstance(target_uri, str) or not target_uri:
        return JSONResponse(
            status_code=400,
            content={"status": 400, "detail": "no callback target provided"},
        )

    with _jobs_lock:
        _active_jobs[job_id] = {"target_uri": target_uri}
    latest = _repository.latest()
    delivery = _deliver_to_job(job_id, target_uri, latest) if latest else None
    return JSONResponse(
        status_code=200,
        content={"status": "enabled", "initial_delivery": delivery},
    )


@app.delete("/producer/info-job/{job_id}")
def info_job_deleted(job_id: str) -> dict:
    with _jobs_lock:
        _active_jobs.pop(job_id, None)
    return {"status": "disabled"}


def reset_state(repository: Optional[EvidenceRepository] = None) -> None:
    """Reset process state for isolated tests."""
    global _repository
    stop_registration_worker()
    _repository = repository or InMemoryEvidenceRepository()
    with _jobs_lock:
        _active_jobs.clear()
    _set_registered(False)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=EVIDENCE_API_BIND_HOST,
        port=EVIDENCE_API_PORT,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )
