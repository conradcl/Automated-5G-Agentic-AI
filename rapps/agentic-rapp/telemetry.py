"""Compatibility import for the repository-wide telemetry contract."""
from __future__ import annotations

import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from oran_telemetry import (  # noqa: E402,F401
    REQUIRED_FIELDS,
    SCHEMA_VERSION,
    TelemetryCache,
    isoformat_utc,
    parse_timestamp,
    utc_now,
    validate_telemetry_payload,
)

__all__ = [
    "REQUIRED_FIELDS",
    "SCHEMA_VERSION",
    "TelemetryCache",
    "isoformat_utc",
    "parse_timestamp",
    "utc_now",
    "validate_telemetry_payload",
]
