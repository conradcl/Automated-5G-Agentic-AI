"""Canonical wire contract for Health xApp telemetry.

This package intentionally uses only the Python standard library so the
Evidence API and rApp can share validation without coupling their web
frameworks or deployment lifecycles.
"""
from __future__ import annotations

import copy
import math
import threading
from datetime import datetime, timezone
from typing import Any, Optional


SCHEMA_VERSION = "1.0"

REQUIRED_FIELDS = (
    "schema_version",
    "source",
    "source_instance_id",
    "sequence_number",
    "observed_at",
    "ric_connected",
    "e2_nodes_connected",
    "kpm_indications_received",
    "last_kpm_indication_at",
    "metrics",
    "missing_metrics",
    "incomplete",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_timestamp(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def validate_telemetry_payload(payload: Any) -> list[str]:
    """Return contract violations; an empty list means the payload is valid.

    Negative measurements are accepted here so the rApp can report them as bad
    evidence. NaN and infinity are rejected because they are not interoperable
    JSON numbers.
    """
    if not isinstance(payload, dict):
        return ["payload must be a JSON object"]

    errors: list[str] = []
    for field in REQUIRED_FIELDS:
        if field not in payload:
            errors.append(f"missing required field: {field}")
    if errors:
        return errors

    if payload["schema_version"] != SCHEMA_VERSION:
        errors.append(f"unsupported schema_version: expected {SCHEMA_VERSION!r}")
    if not isinstance(payload["source"], str) or not payload["source"].strip():
        errors.append("source must be a non-empty string")
    if (
        not isinstance(payload["source_instance_id"], str)
        or not payload["source_instance_id"].strip()
    ):
        errors.append("source_instance_id must be a non-empty string")
    if not _is_int(payload["sequence_number"]) or payload["sequence_number"] < 0:
        errors.append("sequence_number must be a non-negative integer")

    if parse_timestamp(payload["observed_at"]) is None:
        errors.append("observed_at must be a timezone-aware ISO-8601 timestamp")
    last_kpm = payload["last_kpm_indication_at"]
    if last_kpm is not None and parse_timestamp(last_kpm) is None:
        errors.append(
            "last_kpm_indication_at must be null or a timezone-aware ISO-8601 timestamp"
        )

    if not isinstance(payload["ric_connected"], bool):
        errors.append("ric_connected must be a boolean")
    for field in ("e2_nodes_connected", "kpm_indications_received"):
        if not _is_int(payload[field]):
            errors.append(f"{field} must be an integer")

    metrics = payload["metrics"]
    if not isinstance(metrics, dict):
        errors.append("metrics must be an object")
    else:
        for name, measurement in metrics.items():
            if not isinstance(name, str) or not name:
                errors.append("metric names must be non-empty strings")
                continue
            if not isinstance(measurement, dict):
                errors.append(f"metric {name!r} must be an object")
                continue
            if "value" not in measurement or "unit" not in measurement:
                errors.append(f"metric {name!r} requires value and unit")
                continue
            value = measurement["value"]
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                errors.append(f"metric {name!r} value must be numeric")
            elif not math.isfinite(value):
                errors.append(f"metric {name!r} value must be finite")
            if not isinstance(measurement["unit"], str):
                errors.append(f"metric {name!r} unit must be a string")
            if "valid" in measurement and not isinstance(measurement["valid"], bool):
                errors.append(f"metric {name!r} valid must be a boolean")

    missing_metrics = payload["missing_metrics"]
    if not isinstance(missing_metrics, list) or not all(
        isinstance(name, str) and name for name in missing_metrics
    ):
        errors.append("missing_metrics must be an array of non-empty strings")
    if not isinstance(payload["incomplete"], bool):
        errors.append("incomplete must be a boolean")
    return errors


class TelemetryCache:
    """Thread-safe latest-value cache with per-source sequence protection."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._latest: Optional[dict] = None
        self._latest_received_at: Optional[datetime] = None
        self._last_sequences: dict[tuple[str, str], int] = {}

    def accept(
        self, payload: dict, received_at: Optional[datetime] = None
    ) -> tuple[str, Optional[dict]]:
        key = (payload["source"], payload["source_instance_id"])
        sequence = payload["sequence_number"]
        with self._lock:
            previous = self._last_sequences.get(key)
            if previous is not None:
                if sequence == previous:
                    return "duplicate", copy.deepcopy(self._latest)
                if sequence < previous:
                    return "out_of_order", copy.deepcopy(self._latest)
            self._last_sequences[key] = sequence
            self._latest = copy.deepcopy(payload)
            self._latest_received_at = received_at or utc_now()
            return "accepted", copy.deepcopy(self._latest)

    def latest(self) -> tuple[Optional[dict], Optional[datetime]]:
        with self._lock:
            return copy.deepcopy(self._latest), self._latest_received_at

    def reset(self) -> None:
        with self._lock:
            self._latest = None
            self._latest_received_at = None
            self._last_sequences.clear()
