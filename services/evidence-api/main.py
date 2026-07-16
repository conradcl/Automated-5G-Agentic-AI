import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

load_dotenv()

app = FastAPI(
    title="O-RAN Evidence API",
    description="Stores and exposes normalized xApp evidence for the LangGraph rApp.",
    version="0.1.0",
)

# Point this to your current xApp JSON file.
# Example:
# XAPP_LATEST_JSON_PATH=/home/conrad/Automated-5G-Agentic-AI/runs/latest_e2_observation.json
XAPP_LATEST_JSON_PATH = Path(
    os.getenv("XAPP_LATEST_JSON_PATH", "runs/latest_e2_observation.json")
)

# For the first version, keep events in memory.
# Later we replace this with Postgres/TimescaleDB.
KPM_EVENTS: List[Dict[str, Any]] = []


class KpmMetricEvent(BaseModel):
    schema_version: str = "1.0"
    event_type: str = "kpm_metric_event"
    source_app: str = "health-monitor-xapp"

    ric_id: Optional[str] = None
    e2_node_id: Optional[str] = None
    cell_id: Optional[str] = None
    ue_id: Optional[str] = None

    timestamp_unix_ms: int = Field(
        default_factory=lambda: int(time.time() * 1000)
    )
    collection_period_ms: Optional[int] = None

    metrics: Dict[str, float] = Field(default_factory=dict)
    status: Dict[str, Any] = Field(default_factory=dict)


@app.get("/")
def root():
    return {
        "service": "oran-evidence-api",
        "status": "running",
        "docs": "/docs",
    }


@app.get("/v1/health/latest")
def get_latest_health():
    """
    This endpoint returns the clean health object that the LangGraph rApp should call.
    For now, it can use either:
    1. The most recent normalized KPM event POSTed to this API.
    2. The current xApp JSON summary file.
    """

    if KPM_EVENTS:
        return build_health_from_latest_event(KPM_EVENTS[-1])

    if XAPP_LATEST_JSON_PATH.exists():
        return build_health_from_xapp_file(XAPP_LATEST_JSON_PATH)

    return {
        "overall_health": "unknown",
        "source": "evidence-api",
        "timestamp_unix_ms": int(time.time() * 1000),
        "reason": "No KPM events received and no xApp JSON file found.",
        "expected_xapp_json_path": str(XAPP_LATEST_JSON_PATH),
    }


@app.post("/v1/kpm/events")
def ingest_kpm_event(event: KpmMetricEvent):
    """
    This is the endpoint the xApp will eventually POST to.

    The xApp should send normalized MetricEvent JSON here.
    """
    event_dict = event.model_dump()
    KPM_EVENTS.append(event_dict)

    return {
        "accepted": True,
        "stored_events": len(KPM_EVENTS),
        "event": event_dict,
    }


@app.get("/v1/kpm/latest")
def get_latest_kpm():
    if not KPM_EVENTS:
        raise HTTPException(status_code=404, detail="No KPM events received yet.")

    return KPM_EVENTS[-1]


@app.get("/v1/kpm/history")
def get_kpm_history(limit: int = 20):
    return {
        "count": min(limit, len(KPM_EVENTS)),
        "events": KPM_EVENTS[-limit:],
    }


@app.get("/v1/logs/recent")
def get_recent_logs(limit: int = 50):
    """
    For now, this returns the raw evidence path from the xApp summary file if available.
    Later, this should read from Loki, files, or a database.
    """
    if not XAPP_LATEST_JSON_PATH.exists():
        return {
            "logs_available": False,
            "message": "No xApp summary file found.",
        }

    with open(XAPP_LATEST_JSON_PATH, "r") as f:
        raw = json.load(f)

    raw_log_path = raw.get("raw_evidence_path")

    return {
        "logs_available": bool(raw_log_path),
        "raw_evidence_path": raw_log_path,
        "message": "Raw logs are referenced but not parsed yet.",
        "limit": limit,
    }


def build_health_from_latest_event(event: Dict[str, Any]) -> Dict[str, Any]:
    metrics = event.get("metrics", {})
    status = event.get("status", {})

    dl_packet_loss = metrics.get("dl_packet_loss_percent")
    latency_ms = metrics.get("latency_ms")

    active_anomalies = []

    if dl_packet_loss is not None and dl_packet_loss > 5.0:
        active_anomalies.append({
            "type": "packet_loss",
            "severity": "medium",
            "metric": "dl_packet_loss_percent",
            "value": dl_packet_loss,
            "threshold": 5.0,
        })

    if latency_ms is not None and latency_ms > 100.0:
        active_anomalies.append({
            "type": "latency",
            "severity": "medium",
            "metric": "latency_ms",
            "value": latency_ms,
            "threshold": 100.0,
        })

    if active_anomalies:
        overall_health = "degraded"
    else:
        overall_health = "healthy"

    return {
        "overall_health": overall_health,
        "source": event.get("source_app", "unknown"),
        "timestamp_unix_ms": int(time.time() * 1000),
        "e2_node_id": event.get("e2_node_id"),
        "cell_id": event.get("cell_id"),
        "kpm_status": "fresh",
        "metrics": metrics,
        "status": status,
        "active_anomalies": active_anomalies,
        "raw_event": event,
    }


def build_health_from_xapp_file(path: Path) -> Dict[str, Any]:
    with open(path, "r") as f:
        raw = json.load(f)

    e2_nodes_connected = raw.get("e2_nodes_connected", 0)
    kpm_indications_received = raw.get("kpm_indications_received", 0)

    known_issues = []

    # Based on your earlier output, latency may currently be wrong because
    # seconds and microseconds are mixed.
    latency_value = raw.get("last_kpm_latency_us")
    if latency_value is not None and latency_value > 10_000_000:
        known_issues.append({
            "type": "invalid_latency",
            "message": "Latency value appears invalid and is ignored for health scoring.",
            "value": latency_value,
        })

    if e2_nodes_connected >= 1 and kpm_indications_received > 0:
        overall_health = "healthy"
    elif e2_nodes_connected >= 1 and kpm_indications_received == 0:
        overall_health = "degraded"
    else:
        overall_health = "unhealthy"

    return {
        "overall_health": overall_health,
        "source": "xapp_summary_file",
        "timestamp_unix_ms": int(time.time() * 1000),
        "ric_connection": "connected" if e2_nodes_connected >= 1 else "disconnected",
        "e2_nodes_connected": e2_nodes_connected,
        "kpm_indications_received": kpm_indications_received,
        "latency_available": raw.get("latency_available", False),
        "kpm_status": "present" if kpm_indications_received > 0 else "missing",
        "known_issues": known_issues,
        "raw_evidence_path": raw.get("raw_evidence_path"),
        "raw_xapp_summary": raw,
    }