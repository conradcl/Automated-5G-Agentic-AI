"""Deterministic health evaluation for the read-only MVP.

The LLM is intentionally not responsible for these conclusions. Every status
is derived from the versioned telemetry payload and can be unit tested.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional, TypedDict

import config
from consumer import HealthSnapshot
from telemetry import isoformat_utc, parse_timestamp


class HealthCheck(TypedDict):
    name: str
    status: str
    severity: str
    value: Any
    detail: str


class HealthReport(TypedDict):
    assessment_scope: str
    overall_status: str
    generated_at: str
    source_observed_at: Optional[str]
    source_instance_id: Optional[str]
    sequence_number: Optional[int]
    checks: list[HealthCheck]


def _check(
    name: str, status: str, severity: str, value: Any, detail: str
) -> HealthCheck:
    return HealthCheck(
        name=name,
        status=status,
        severity=severity,
        value=value,
        detail=detail,
    )


def evaluate_health(
    snapshot: HealthSnapshot, now: Optional[datetime] = None
) -> HealthReport:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)

    telemetry = snapshot.get("telemetry")
    if not snapshot.get("received") or not isinstance(telemetry, dict):
        return HealthReport(
            assessment_scope=config.ASSESSMENT_SCOPE,
            overall_status="unknown",
            generated_at=isoformat_utc(now),
            source_observed_at=None,
            source_instance_id=None,
            sequence_number=None,
            checks=[
                _check(
                    "telemetry_received",
                    "fail",
                    "critical",
                    False,
                    "No Health xApp telemetry has been delivered through the R1 Information Job.",
                )
            ],
        )

    checks: list[HealthCheck] = [
        _check(
            "telemetry_received",
            "pass",
            "critical",
            True,
            "A telemetry snapshot was delivered through the R1 Information Job.",
        )
    ]

    ric_connected = telemetry.get("ric_connected") is True
    checks.append(
        _check(
            "xapp_connected_to_ric",
            "pass" if ric_connected else "fail",
            "critical",
            telemetry.get("ric_connected"),
            "Health xApp reports a Near-RT RIC connection."
            if ric_connected
            else "Health xApp does not report a Near-RT RIC connection.",
        )
    )

    node_count = telemetry.get("e2_nodes_connected")
    nodes_ok = isinstance(node_count, int) and not isinstance(node_count, bool) and node_count > 0
    checks.append(
        _check(
            "e2_nodes_connected",
            "pass" if nodes_ok else "fail",
            "critical",
            node_count,
            "At least one E2 node is connected."
            if nodes_ok
            else "No connected E2 node is reported.",
        )
    )

    indication_count = telemetry.get("kpm_indications_received")
    indications_ok = (
        isinstance(indication_count, int)
        and not isinstance(indication_count, bool)
        and indication_count > 0
    )
    checks.append(
        _check(
            "kpm_indications_received",
            "pass" if indications_ok else "fail",
            "critical",
            indication_count,
            "At least one KPM indication has been received."
            if indications_ok
            else "No KPM indication has been received.",
        )
    )

    observed_at = parse_timestamp(telemetry.get("observed_at"))
    last_kpm_at = parse_timestamp(telemetry.get("last_kpm_indication_at"))
    timestamp_errors: list[str] = []
    if observed_at is None:
        timestamp_errors.append("observed_at is invalid")
    elif (observed_at - now).total_seconds() > config.MAX_FUTURE_SKEW_S:
        timestamp_errors.append("observed_at is too far in the future")
    if indications_ok and last_kpm_at is None:
        timestamp_errors.append("last_kpm_indication_at is missing or invalid")
    if observed_at is not None and last_kpm_at is not None:
        if (last_kpm_at - observed_at).total_seconds() > config.MAX_FUTURE_SKEW_S:
            timestamp_errors.append("last_kpm_indication_at is after observed_at")
        if (last_kpm_at - now).total_seconds() > config.MAX_FUTURE_SKEW_S:
            timestamp_errors.append("last_kpm_indication_at is too far in the future")

    checks.append(
        _check(
            "timestamps_valid",
            "pass" if not timestamp_errors else "fail",
            "critical",
            {
                "observed_at": telemetry.get("observed_at"),
                "last_kpm_indication_at": telemetry.get("last_kpm_indication_at"),
            },
            "Source timestamps are valid and internally consistent."
            if not timestamp_errors
            else "; ".join(timestamp_errors),
        )
    )

    if last_kpm_at is None:
        fresh = False
        age_s: Optional[float] = None
    else:
        age_s = (now - last_kpm_at).total_seconds()
        fresh = -config.MAX_FUTURE_SKEW_S <= age_s <= config.STALE_AFTER_S
    checks.append(
        _check(
            "kpm_data_fresh",
            "pass" if fresh else "fail",
            "critical",
            {"age_seconds": age_s, "maximum_seconds": config.STALE_AFTER_S},
            "The latest KPM indication is fresh."
            if fresh
            else "The latest KPM indication is missing, stale, or future-dated.",
        )
    )

    metrics = telemetry.get("metrics")
    metrics = metrics if isinstance(metrics, dict) else {}
    explicitly_missing = telemetry.get("missing_metrics")
    missing = set(explicitly_missing if isinstance(explicitly_missing, list) else [])
    for required_name in config.REQUIRED_KPM_METRICS:
        measurement = metrics.get(required_name)
        if not isinstance(measurement, dict) or measurement.get("valid", True) is not True:
            missing.add(required_name)
    missing_list = sorted(missing)
    checks.append(
        _check(
            "required_metrics_present",
            "pass" if not missing_list else "warning",
            "warning",
            {"missing": missing_list, "required": list(config.REQUIRED_KPM_METRICS)},
            "All configured KPM metrics are present."
            if not missing_list
            else "One or more configured KPM metrics are missing or invalid.",
        )
    )

    bad_values: list[dict] = []
    for field in ("e2_nodes_connected", "kpm_indications_received"):
        value = telemetry.get(field)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value < 0:
            bad_values.append({"field": field, "value": value, "reason": "negative"})
    for name, measurement in metrics.items():
        if not isinstance(measurement, dict):
            continue
        value = measurement.get("value")
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value < 0:
            bad_values.append({"field": name, "value": value, "reason": "negative"})
        if measurement.get("valid", True) is not True:
            bad_values.append({"field": name, "value": value, "reason": "marked invalid"})
    checks.append(
        _check(
            "metric_values_plausible",
            "pass" if not bad_values else "warning",
            "warning",
            {"bad_values": bad_values},
            "No obviously impossible negative or invalid metric values were found."
            if not bad_values
            else "One or more counters or measurements are obviously invalid.",
        )
    )

    incomplete = telemetry.get("incomplete") is True
    checks.append(
        _check(
            "measurement_complete",
            "warning" if incomplete else "pass",
            "warning",
            not incomplete,
            "The xApp marked this measurement set incomplete."
            if incomplete
            else "The xApp did not mark this measurement set incomplete.",
        )
    )

    if any(c["status"] == "fail" and c["severity"] == "critical" for c in checks):
        overall = "unhealthy"
    elif any(c["status"] == "warning" for c in checks):
        overall = "degraded"
    else:
        overall = "healthy"

    return HealthReport(
        assessment_scope=config.ASSESSMENT_SCOPE,
        overall_status=overall,
        generated_at=isoformat_utc(now),
        source_observed_at=telemetry.get("observed_at"),
        source_instance_id=telemetry.get("source_instance_id"),
        sequence_number=telemetry.get("sequence_number"),
        checks=checks,
    )
