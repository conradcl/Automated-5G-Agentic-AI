"""Environment-driven configuration shared by the lab R1 services.

The MVP runs the DME broker, telemetry producer adapter, and rApp consumer as
separate processes.  Bind addresses control where a process listens; public
base URLs are the addresses advertised to the other processes.  Keeping those
separate also makes the same code usable in containers later.
"""
from __future__ import annotations

import os


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


def _env_csv(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return tuple(item.strip() for item in raw.split(",") if item.strip())


# R1/DME-lite broker
DME_BIND_HOST = os.environ.get("DME_BIND_HOST", "127.0.0.1")
DME_PORT = _env_int("DME_PORT", 9990)
DME_BASE_URL = os.environ.get("DME_BASE_URL", f"http://127.0.0.1:{DME_PORT}")

INFO_TYPE_ID = os.environ.get("INFO_TYPE_ID", "oran-health-monitor-kpm-v1")
HTTP_TIMEOUT_S = _env_float("HTTP_TIMEOUT_S", 5.0)

# Read-only health rApp consumer
JOB_ID = os.environ.get("JOB_ID", "health-agent-rapp-job-1")
JOB_OWNER = os.environ.get("JOB_OWNER", "health-agent-rapp")
CONSUMER_BIND_HOST = os.environ.get("CONSUMER_BIND_HOST", "127.0.0.1")
CONSUMER_PORT = _env_int("CONSUMER_PORT", 9992)
CONSUMER_BASE_URL = os.environ.get(
    "CONSUMER_BASE_URL", f"http://127.0.0.1:{CONSUMER_PORT}"
)
CONSUMER_CALLBACK_PATH = os.environ.get(
    "CONSUMER_CALLBACK_PATH", "/consumer/health-data"
)

# Source freshness is based on the xApp's KPM observation timestamp, never on
# the time at which an adapter happens to deliver or redeliver the message.
STALE_AFTER_S = _env_float("STALE_AFTER_S", 30.0)
MAX_FUTURE_SKEW_S = _env_float("MAX_FUTURE_SKEW_S", 5.0)

# The Health xApp requests these metrics. Override with a comma-separated list
# if the lab E2 node exposes a different KPM measurement set.
REQUIRED_KPM_METRICS = _env_csv(
    "REQUIRED_KPM_METRICS",
    (
        "RRU.PrbTotDl",
        "RRU.PrbTotUl",
        "DRB.UEThpDl",
        "DRB.UEThpUl",
        "DRB.RlcSduDelayDl",
    ),
)
