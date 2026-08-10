"""Fixed-purpose, read-only evidence tools for the live Health Agent rApp.

The model can select only a catalog name. URLs, SQL, process arguments, network
targets, interfaces, and container names come exclusively from operator
configuration and are never accepted from model output.
"""
from __future__ import annotations

import copy
import ipaddress
import json
import math
import re
import subprocess
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional
from urllib.parse import urlsplit, urlunsplit

import requests

import config
from memory import NullRAppMemory, RAppMemory
from telemetry import isoformat_utc, parse_timestamp, validate_telemetry_payload


READ_TOOL_SCHEMA_VERSION = "1.0"

GET_DME_JOB_STATUS = "get_dme_job_status"
GET_EVIDENCE_PIPELINE_STATUS = "get_evidence_pipeline_status"
GET_EVIDENCE_HISTORY = "get_evidence_history"
GET_RECENT_TELEMETRY_WINDOWS = "get_recent_telemetry_windows"
GET_SEQUENCE_ADVANCEMENT = "get_sequence_advancement"
PING_UE_PATH = "ping_ue_path"
GET_OAI_CONTAINER_STATUS = "get_oai_container_status"

READ_TOOL_NAMES = (
    GET_DME_JOB_STATUS,
    GET_EVIDENCE_PIPELINE_STATUS,
    GET_EVIDENCE_HISTORY,
    GET_RECENT_TELEMETRY_WINDOWS,
    GET_SEQUENCE_ADVANCEMENT,
    PING_UE_PATH,
    GET_OAI_CONTAINER_STATUS,
)

_SCOPES = {
    GET_DME_JOB_STATUS: "r1_control_plane",
    GET_EVIDENCE_PIPELINE_STATUS: "evidence_pipeline",
    GET_EVIDENCE_HISTORY: "evidence_history",
    GET_RECENT_TELEMETRY_WINDOWS: "telemetry_history",
    GET_SEQUENCE_ADVANCEMENT: "telemetry_delivery",
    PING_UE_PATH: "ue_user_plane",
    GET_OAI_CONTAINER_STATUS: "oai_core_runtime",
}

_DESCRIPTIONS = {
    GET_DME_JOB_STATUS: (
        "Read the configured DME service and configured R1 Information Job. "
        "Use for subscription, producer, callback, or DME questions."
    ),
    GET_EVIDENCE_PIPELINE_STATUS: (
        "Read the configured Evidence API liveness, readiness, repository, DME "
        "registration, evidence-received flag, and active-job count."
    ),
    GET_EVIDENCE_HISTORY: (
        "Read a fixed bounded set of recent canonical Evidence API observations. "
        "Use for source history or upstream sequence questions."
    ),
    GET_RECENT_TELEMETRY_WINDOWS: (
        "Read deterministic facts for a fixed bounded number of recent local "
        "SQLite telemetry windows; no SQL or advisory digest is exposed."
    ),
    GET_SEQUENCE_ADVANCEMENT: (
        "Check whether rApp-received sequence numbers advanced during a fixed "
        "lookback, separately for each source instance."
    ),
    PING_UE_PATH: (
        "Run one bounded uplink ping using the operator-configured UE interface "
        "and external-data-network target; return packet loss and RTT only."
    ),
    GET_OAI_CONTAINER_STATUS: (
        "Inspect runtime state only for the operator-configured OAI container "
        "allowlist; no Docker command or container argument is model-controlled."
    ),
}

_LIMITATIONS = {
    GET_DME_JOB_STATUS: [
        "DME ENABLED means a compatible producer is registered, not that R1 delivery succeeded."
    ],
    GET_EVIDENCE_PIPELINE_STATUS: [
        "The active-job value is a count and does not prove the configured job is active."
    ],
    GET_EVIDENCE_HISTORY: [
        "History is bounded and normalized; raw HTTP bodies and full metric values are omitted."
    ],
    GET_RECENT_TELEMETRY_WINDOWS: [
        "Facts may be unavailable after raw-sample retention pruning; stored model digests are omitted."
    ],
    GET_SEQUENCE_ADVANCEMENT: [
        "A sequence gap means not observed by this rApp and is not proof of IP packet loss.",
        "Database uniqueness means duplicate deliveries are not observable in retained rows.",
    ],
    PING_UE_PATH: [
        "This is one bounded ICMP sample and is not a complete subscriber-performance assessment."
    ],
    GET_OAI_CONTAINER_STATUS: [
        "Container runtime state does not prove application or 5G service correctness."
    ],
}

_RESULT_FIELDS = {
    "read_tool_schema_version",
    "tool_name",
    "scope",
    "observed_at",
    "ok",
    "data",
    "error",
    "limitations",
}
_ERROR_FIELDS = {"code", "message", "retryable"}
_ERROR_CODES = {
    "timeout",
    "unreachable",
    "http_error",
    "invalid_response",
    "response_too_large",
    "permission_denied",
    "command_failed",
    "unavailable",
    "invalid_result",
}
_FORBIDDEN_RESULT_KEYS = {
    "overall_status",
    "health_report",
    "checks",
    "report",
    "summary",
    "suggested_action",
    "command",
}
_EXPECTED_DATA_FIELDS = {
    GET_DME_JOB_STATUS: {
        "dme_living",
        "registered_info_type_count",
        "registered_producer_count",
        "registered_job_count",
        "configured_job_id",
        "job_registered",
        "job_operational_state",
        "producer_ids",
        "info_type_matches",
        "job_owner_matches",
        "callback_matches_config",
    },
    GET_EVIDENCE_PIPELINE_STATUS: {
        "living",
        "ready",
        "repository_ready",
        "dme_registered",
        "producer_living",
        "evidence_received",
        "active_job_count",
    },
    GET_EVIDENCE_HISTORY: {
        "requested_limit",
        "returned_count",
        "invalid_events_omitted",
        "events_omitted_for_size",
        "events",
    },
    GET_RECENT_TELEMETRY_WINDOWS: {
        "requested_limit",
        "returned_count",
        "windows",
    },
    GET_SEQUENCE_ADVANCEMENT: {
        "lookback_seconds",
        "sample_count",
        "sample_cap_reached",
        "retained_stream_count",
        "returned_stream_count",
        "stream_cap_reached",
        "duplicates_observable",
        "streams",
    },
    PING_UE_PATH: {
        "direction",
        "interface",
        "target",
        "transmitted",
        "received",
        "packet_loss_percent",
        "rtt_min_ms",
        "rtt_avg_ms",
        "rtt_max_ms",
        "rtt_mdev_ms",
        "return_code",
        "timed_out",
    },
    GET_OAI_CONTAINER_STATUS: {
        "requested_count",
        "present_count",
        "running_count",
        "containers",
    },
}

_INTERFACE_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,15}$")
_CONTAINER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_PATH_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_PING_STATS = re.compile(
    r"(?P<transmitted>\d+)\s+packets transmitted,\s*"
    r"(?P<received>\d+)\s+(?:packets\s+)?received,.*?"
    r"(?P<loss>\d+(?:\.\d+)?)%\s+packet loss",
    re.IGNORECASE | re.DOTALL,
)
_PING_RTT = re.compile(
    r"(?:rtt|round-trip) min/avg/max/(?:mdev|stddev)\s*=\s*"
    r"(?P<minimum>\d+(?:\.\d+)?)/(?P<average>\d+(?:\.\d+)?)/"
    r"(?P<maximum>\d+(?:\.\d+)?)/(?P<mdev>\d+(?:\.\d+)?)\s*ms",
    re.IGNORECASE,
)


class ReadToolConfigurationError(ValueError):
    """Raised when an operator-owned read-tool target is unsafe or invalid."""


class _ToolCollectionError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable


def read_tool_catalog() -> list[dict[str, Any]]:
    """Return immutable zero-argument tool definitions for model selection."""
    return [
        {
            "name": name,
            "description": _DESCRIPTIONS[name],
            "parameters": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        }
        for name in READ_TOOL_NAMES
    ]


def validate_read_tool_catalog(catalog: Any) -> list[str]:
    if not isinstance(catalog, list):
        return ["read-tool catalog must be an array"]
    errors: list[str] = []
    seen: set[str] = set()
    for item in catalog:
        if not isinstance(item, dict) or set(item) != {
            "name",
            "description",
            "parameters",
        }:
            errors.append("read-tool catalog entry is malformed")
            continue
        name = item.get("name")
        if name not in READ_TOOL_NAMES or name in seen:
            errors.append("read-tool catalog name is unsupported or duplicated")
        else:
            seen.add(name)
        if not isinstance(item.get("description"), str) or not item["description"]:
            errors.append("read-tool description is malformed")
        if item.get("parameters") != {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        }:
            errors.append("read-tool parameters must be an empty object schema")
    return errors


def _walk_json(value: Any, *, depth: int = 0) -> tuple[bool, Optional[str]]:
    if depth > 10:
        return False, "read-tool result is nested too deeply"
    if value is None or isinstance(value, (str, bool, int)):
        if isinstance(value, str) and len(value) > 4096:
            return False, "read-tool result contains an oversized string"
        if isinstance(value, int) and not isinstance(value, bool) and not _is_int64(value):
            return False, "read-tool result contains an integer outside the storage range"
        return True, None
    if isinstance(value, float):
        if not math.isfinite(value):
            return False, "read-tool result contains a non-finite number"
        return True, None
    if isinstance(value, list):
        if len(value) > 2000:
            return False, "read-tool result contains an oversized array"
        for item in value:
            valid, error = _walk_json(item, depth=depth + 1)
            if not valid:
                return valid, error
        return True, None
    if isinstance(value, dict):
        if len(value) > 512:
            return False, "read-tool result contains an oversized object"
        for key, item in value.items():
            if not isinstance(key, str) or not key or len(key) > 128:
                return False, "read-tool result contains an invalid object key"
            if key in _FORBIDDEN_RESULT_KEYS:
                return False, f"read-tool result contains forbidden key {key!r}"
            valid, error = _walk_json(item, depth=depth + 1)
            if not valid:
                return valid, error
        return True, None
    return False, "read-tool result contains a non-JSON value"


def validate_read_tool_result(
    result: Any,
    *,
    max_chars: Optional[int] = None,
) -> list[str]:
    if not isinstance(result, dict) or set(result) != _RESULT_FIELDS:
        return ["read-tool result does not match the allowed envelope"]
    errors: list[str] = []
    name = result.get("tool_name")
    if name not in READ_TOOL_NAMES:
        errors.append("read-tool result name is unsupported")
    if result.get("read_tool_schema_version") != READ_TOOL_SCHEMA_VERSION:
        errors.append("read-tool result schema version is unsupported")
    if name in _SCOPES and result.get("scope") != _SCOPES[name]:
        errors.append("read-tool result scope is inconsistent")
    if parse_timestamp(result.get("observed_at")) is None:
        errors.append("read-tool observation timestamp is malformed")
    if not isinstance(result.get("ok"), bool):
        errors.append("read-tool collection flag must be boolean")
    data = result.get("data")
    if not isinstance(data, dict):
        errors.append("read-tool result data must be an object")
    elif result.get("ok") is True and name in _EXPECTED_DATA_FIELDS:
        if set(data) != _EXPECTED_DATA_FIELDS[name]:
            errors.append("read-tool success data does not match its typed schema")
        else:
            errors.extend(_validate_success_data(name, data))
    elif result.get("ok") is False and data:
        errors.append("failed read-tool result data must be empty")
    error = result.get("error")
    if result.get("ok") is True and error is not None:
        errors.append("successful read-tool result cannot contain an error")
    if result.get("ok") is False:
        if not isinstance(error, dict) or set(error) != _ERROR_FIELDS:
            errors.append("failed read-tool result error is malformed")
        elif (
            not isinstance(error.get("code"), str)
            or not isinstance(error.get("message"), str)
            or not isinstance(error.get("retryable"), bool)
        ):
            errors.append("failed read-tool result error types are malformed")
        elif (
            error["code"] not in _ERROR_CODES
            or not error["message"]
            or len(error["message"]) > 256
        ):
            errors.append("failed read-tool result error values are malformed")
    limitations = result.get("limitations")
    if not isinstance(limitations, list) or not all(
        isinstance(item, str) and 0 < len(item) <= 512 for item in limitations
    ):
        errors.append("read-tool limitations are malformed")
    valid_json, json_error = _walk_json(result)
    if not valid_json and json_error:
        errors.append(json_error)
    bound = config.RAPP_READ_TOOL_MAX_RESULT_CHARS if max_chars is None else max_chars
    if isinstance(bound, bool) or not isinstance(bound, int) or bound < 1:
        errors.append("read-tool result size limit is invalid")
        bound = 1
    try:
        serialized = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        errors.append("read-tool result cannot be serialized")
    else:
        if len(serialized) > bound:
            errors.append("read-tool result exceeds the configured size limit")
    return errors


def validate_read_tool_results(results: Any) -> list[str]:
    if not isinstance(results, list):
        return ["read-tool results must be an array"]
    if len(results) > config.RAPP_READ_TOOL_MAX_CALLS:
        return ["read-tool result count exceeds the configured call limit"]
    errors: list[str] = []
    seen: set[str] = set()
    for result in results:
        item_errors = validate_read_tool_result(result)
        errors.extend(item_errors)
        if isinstance(result, dict):
            name = result.get("tool_name")
            if isinstance(name, str):
                if name in seen:
                    errors.append("read-tool result names must be unique")
                seen.add(name)
    return errors


def _validated_base_url(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReadToolConfigurationError(f"{label} must be a non-empty URL")
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise ReadToolConfigurationError(f"{label} must be an HTTP(S) origin")
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", "")).rstrip("/")


def _nonnegative_int(value: Any) -> Optional[int]:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _is_int64(value: Any, *, nonnegative: bool = False) -> bool:
    if isinstance(value, bool) or not isinstance(value, int):
        return False
    if not -(2**63) <= value <= 2**63 - 1:
        return False
    return not nonnegative or value >= 0


def _bounded_string(value: Any, limit: int = 128) -> Optional[str]:
    if not isinstance(value, str):
        return None
    return value[:limit]


def _is_finite_number(value: Any, *, nonnegative: bool = False) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        finite = math.isfinite(value)
    except (OverflowError, TypeError, ValueError):
        return False
    return finite and (not nonnegative or value >= 0)


def _timestamp_or_none(value: Any) -> bool:
    return value is None or parse_timestamp(value) is not None


def _validate_success_data(tool_name: str, data: dict[str, Any]) -> list[str]:
    """Validate the per-tool result contract, including nested value types."""
    errors: list[str] = []

    def malformed(detail: str) -> None:
        errors.append(f"{tool_name} data is malformed: {detail}")

    if tool_name == GET_DME_JOB_STATUS:
        if not isinstance(data["dme_living"], bool):
            malformed("dme_living must be boolean")
        for field in (
            "registered_info_type_count",
            "registered_producer_count",
            "registered_job_count",
        ):
            if data[field] is not None and not _is_int64(
                data[field], nonnegative=True
            ):
                malformed(f"{field} must be a non-negative integer or null")
        if (
            not isinstance(data["configured_job_id"], str)
            or not _PATH_SEGMENT_PATTERN.fullmatch(data["configured_job_id"])
        ):
            malformed("configured_job_id is invalid")
        if not isinstance(data["job_registered"], bool):
            malformed("job_registered must be boolean")
        if (
            not isinstance(data["job_operational_state"], str)
            or data["job_operational_state"]
            not in {"ENABLED", "DISABLED", "UNKNOWN"}
        ):
            malformed("job_operational_state is unsupported")
        producers = data["producer_ids"]
        if (
            not isinstance(producers, list)
            or len(producers) > 32
            or not all(
                isinstance(item, str) and 0 < len(item) <= 128
                for item in producers
            )
        ):
            malformed("producer_ids is invalid")
        for field in (
            "info_type_matches",
            "job_owner_matches",
            "callback_matches_config",
        ):
            if data[field] is not None and not isinstance(data[field], bool):
                malformed(f"{field} must be boolean or null")

    elif tool_name == GET_EVIDENCE_PIPELINE_STATUS:
        for field in ("living", "ready", "producer_living"):
            if not isinstance(data[field], bool):
                malformed(f"{field} must be boolean")
        for field in ("repository_ready", "dme_registered", "evidence_received"):
            if data[field] is not None and not isinstance(data[field], bool):
                malformed(f"{field} must be boolean or null")
        if data["active_job_count"] is not None and not _is_int64(
            data["active_job_count"], nonnegative=True
        ):
            malformed("active_job_count must be a non-negative integer or null")

    elif tool_name == GET_EVIDENCE_HISTORY:
        for field in (
            "requested_limit",
            "returned_count",
            "invalid_events_omitted",
            "events_omitted_for_size",
        ):
            if not _is_int64(data[field], nonnegative=True):
                malformed(f"{field} must be a non-negative integer")
        events = data["events"]
        event_fields = {
            "source",
            "source_instance_id",
            "sequence_number",
            "observed_at",
            "last_kpm_indication_at",
            "ric_connected",
            "e2_nodes_connected",
            "kpm_indications_received",
            "incomplete",
            "missing_metric_count",
            "metric_names",
            "metric_names_omitted",
        }
        if not isinstance(events, list):
            malformed("events must be an array")
        else:
            if data["returned_count"] != len(events):
                malformed("returned_count does not match events")
            if _is_int64(data["requested_limit"], nonnegative=True) and len(
                events
            ) > data["requested_limit"]:
                malformed("events exceed requested_limit")
            for event in events:
                if not isinstance(event, dict) or set(event) != event_fields:
                    malformed("event schema is invalid")
                    continue
                if not all(
                    isinstance(event[field], str) and 0 < len(event[field]) <= 64
                    for field in ("source", "source_instance_id")
                ):
                    malformed("event source identity is invalid")
                if not _is_int64(event["sequence_number"], nonnegative=True):
                    malformed("event sequence_number is invalid")
                if parse_timestamp(event["observed_at"]) is None or not _timestamp_or_none(
                    event["last_kpm_indication_at"]
                ):
                    malformed("event timestamp is invalid")
                if not isinstance(event["ric_connected"], bool) or not isinstance(
                    event["incomplete"], bool
                ):
                    malformed("event boolean field is invalid")
                for field in (
                    "e2_nodes_connected",
                    "kpm_indications_received",
                ):
                    if not _is_int64(event[field]):
                        malformed(f"event {field} is invalid")
                for field in ("missing_metric_count", "metric_names_omitted"):
                    if not _is_int64(event[field], nonnegative=True):
                        malformed(f"event {field} is invalid")
                names = event["metric_names"]
                if (
                    not isinstance(names, list)
                    or len(names) > 32
                    or not all(
                        isinstance(item, str) and 0 < len(item) <= 128
                        for item in names
                    )
                ):
                    malformed("event metric_names is invalid")

    elif tool_name == GET_RECENT_TELEMETRY_WINDOWS:
        if not _is_int64(data["requested_limit"], nonnegative=True) or not 1 <= data[
            "requested_limit"
        ] <= 10:
            malformed("requested_limit is invalid")
        windows = data["windows"]
        window_fields = {
            "window_start",
            "window_end",
            "received_sample_count",
            "digest_source",
            "digest_error_present",
            "facts_available",
            "facts_unavailable_reason",
            "all_sample_facts",
        }
        if not isinstance(windows, list):
            malformed("windows must be an array")
        else:
            count_mismatch = data["returned_count"] != len(windows)
            limit_exceeded = _is_int64(
                data["requested_limit"], nonnegative=True
            ) and len(windows) > data["requested_limit"]
            if count_mismatch or limit_exceeded:
                malformed("window counts are inconsistent")
            for window in windows:
                if not isinstance(window, dict) or set(window) != window_fields:
                    malformed("window schema is invalid")
                    continue
                start = parse_timestamp(window["window_start"])
                end = parse_timestamp(window["window_end"])
                if start is None or end is None or start > end:
                    malformed("window timestamps are invalid")
                if not _is_int64(window["received_sample_count"], nonnegative=True):
                    malformed("window sample count is invalid")
                if (
                    not isinstance(window["digest_source"], str)
                    or not 0 < len(window["digest_source"]) <= 64
                    or not isinstance(window["digest_error_present"], bool)
                    or not isinstance(window["facts_available"], bool)
                ):
                    malformed("window metadata is invalid")
                reason = window["facts_unavailable_reason"]
                facts = window["all_sample_facts"]
                if window["facts_available"]:
                    if reason is not None or not isinstance(facts, dict):
                        malformed("available window facts are inconsistent")
                elif (
                    not isinstance(reason, str)
                    or reason
                    not in {
                        "sample_budget_exceeded",
                        "byte_budget_exceeded",
                        "raw_samples_pruned_or_incomplete",
                    }
                    or facts is not None
                ):
                    malformed("unavailable window facts are inconsistent")

    elif tool_name == GET_SEQUENCE_ADVANCEMENT:
        if not _is_finite_number(data["lookback_seconds"], nonnegative=True):
            malformed("lookback_seconds is invalid")
        for field in (
            "sample_count",
            "retained_stream_count",
            "returned_stream_count",
        ):
            if not _is_int64(data[field], nonnegative=True):
                malformed(f"{field} is invalid")
        for field in ("sample_cap_reached", "stream_cap_reached"):
            if not isinstance(data[field], bool):
                malformed(f"{field} must be boolean")
        if data["duplicates_observable"] is not False:
            malformed("duplicates_observable must be false")
        streams = data["streams"]
        stream_fields = {
            "source",
            "source_instance_id",
            "observations",
            "first_sequence",
            "last_sequence",
            "observed_sequence_gaps",
            "non_increasing_steps",
            "advancement_state",
            "first_received_at",
            "last_received_at",
            "last_known_sequence",
            "last_known_received_at",
            "last_known_age_seconds",
        }
        if not isinstance(streams, list):
            malformed("streams must be an array")
        else:
            if data["returned_stream_count"] != len(streams):
                malformed("returned_stream_count does not match streams")
            for stream in streams:
                if not isinstance(stream, dict) or set(stream) != stream_fields:
                    malformed("stream schema is invalid")
                    continue
                if not all(
                    isinstance(stream[field], str) and 0 < len(stream[field]) <= 64
                    for field in ("source", "source_instance_id")
                ):
                    malformed("stream identity is invalid")
                observations = stream["observations"]
                if not _is_int64(observations, nonnegative=True):
                    malformed("stream observations is invalid")
                    continue
                for field in ("observed_sequence_gaps", "non_increasing_steps"):
                    if not _is_int64(stream[field], nonnegative=True):
                        malformed(f"stream {field} is invalid")
                if not _is_int64(stream["last_known_sequence"], nonnegative=True):
                    malformed("stream last-known sequence is invalid")
                if parse_timestamp(stream["last_known_received_at"]) is None or not _is_finite_number(
                    stream["last_known_age_seconds"], nonnegative=True
                ):
                    malformed("stream last-known timing is invalid")
                if observations == 0:
                    if any(
                        stream[field] is not None
                        for field in (
                            "first_sequence",
                            "last_sequence",
                            "first_received_at",
                            "last_received_at",
                        )
                    ) or stream["advancement_state"] != "no_recent_observations":
                        malformed("stream with no observations is inconsistent")
                else:
                    if not all(
                        _is_int64(stream[field], nonnegative=True)
                        for field in ("first_sequence", "last_sequence")
                    ) or any(
                        parse_timestamp(stream[field]) is None
                        for field in ("first_received_at", "last_received_at")
                    ):
                        malformed("stream observed range is invalid")
                    allowed_states = (
                        {"insufficient_samples"}
                        if observations == 1
                        else {"advancing", "non_monotonic"}
                    )
                    if (
                        not isinstance(stream["advancement_state"], str)
                        or stream["advancement_state"] not in allowed_states
                    ):
                        malformed("stream advancement_state is inconsistent")

    elif tool_name == PING_UE_PATH:
        if data["direction"] != "ue_to_external_data_network":
            malformed("ping direction is invalid")
        if data["interface"] != config.RAPP_READ_TOOL_PING_INTERFACE or data[
            "target"
        ] != config.RAPP_READ_TOOL_PING_TARGET:
            malformed("ping target or interface is inconsistent")
        transmitted = data["transmitted"]
        received = data["received"]
        loss = data["packet_loss_percent"]
        if (
            not _is_int64(transmitted, nonnegative=True)
            or transmitted != config.RAPP_READ_TOOL_PING_COUNT
            or not _is_int64(received, nonnegative=True)
            or received > transmitted
            or not _is_finite_number(loss, nonnegative=True)
            or loss > 100
        ):
            malformed("ping packet counts or loss are invalid")
        elif abs(loss - (100.0 * (transmitted - received) / transmitted)) > 0.2:
            malformed("ping packet loss is inconsistent with counts")
        rtt_fields = (
            "rtt_min_ms",
            "rtt_avg_ms",
            "rtt_max_ms",
            "rtt_mdev_ms",
        )
        if received == 0:
            if any(data[field] is not None for field in rtt_fields):
                malformed("ping RTT must be null when no reply was received")
        elif not all(
            _is_finite_number(data[field], nonnegative=True) for field in rtt_fields
        ):
            malformed("ping RTT is invalid")
        elif not data["rtt_min_ms"] <= data["rtt_avg_ms"] <= data["rtt_max_ms"]:
            malformed("ping RTT range is inconsistent")
        if (
            not _is_int64(data["return_code"])
            or data["return_code"] not in {0, 1}
            or data["timed_out"] is not False
        ):
            malformed("ping completion state is invalid")

    elif tool_name == GET_OAI_CONTAINER_STATUS:
        for field in ("requested_count", "present_count", "running_count"):
            if not _is_int64(data[field], nonnegative=True):
                malformed(f"{field} is invalid")
        containers = data["containers"]
        container_fields = {
            "name",
            "present",
            "running",
            "runtime_state",
            "health_state",
        }
        if not isinstance(containers, list):
            malformed("containers must be an array")
        else:
            if data["requested_count"] != len(containers):
                malformed("requested_count does not match containers")
            if [item.get("name") for item in containers if isinstance(item, dict)] != list(
                config.RAPP_READ_TOOL_OAI_CONTAINERS
            ):
                malformed("container names do not match the allowlist")
            present_count = 0
            running_count = 0
            for item in containers:
                if not isinstance(item, dict) or set(item) != container_fields:
                    malformed("container schema is invalid")
                    continue
                if not isinstance(item["present"], bool) or not isinstance(
                    item["running"], bool
                ):
                    malformed("container flags are invalid")
                    continue
                present_count += int(item["present"])
                running_count += int(item["running"])
                if not item["present"] and (
                    item["running"]
                    or item["runtime_state"] != "missing"
                    or item["health_state"] != "unknown"
                ):
                    malformed("missing container state is inconsistent")
                if (
                    not isinstance(item["runtime_state"], str)
                    or not 0 < len(item["runtime_state"]) <= 32
                    or not isinstance(item["health_state"], str)
                    or item["health_state"]
                    not in {
                        "no_healthcheck",
                        "starting",
                        "healthy",
                        "unhealthy",
                        "unknown",
                    }
                ):
                    malformed("container runtime state is invalid")
            if data["present_count"] != present_count or data[
                "running_count"
            ] != running_count:
                malformed("container counts are inconsistent")

    return errors


class _FixedJsonClient:
    def __init__(
        self,
        base_url: str,
        *,
        session: Any,
        timeout_s: float,
        max_body_bytes: int,
    ) -> None:
        self.base_url = base_url
        self.session = session
        self.timeout_s = timeout_s
        self.max_body_bytes = max_body_bytes

    def get(self, path: str, *, accepted_statuses: set[int]) -> tuple[int, Any]:
        try:
            response = self.session.get(
                f"{self.base_url}{path}",
                headers={"Accept": "application/json"},
                timeout=self.timeout_s,
                allow_redirects=False,
                stream=True,
            )
        except requests.Timeout as exc:
            raise _ToolCollectionError(
                "timeout", "configured service request timed out", retryable=True
            ) from exc
        except requests.RequestException as exc:
            raise _ToolCollectionError(
                "unreachable", "configured service could not be reached", retryable=True
            ) from exc

        try:
            status_code = getattr(response, "status_code", None)
            if not isinstance(status_code, int) or status_code not in accepted_statuses:
                raise _ToolCollectionError(
                    "http_error",
                    "configured service returned an unexpected HTTP status",
                    retryable=status_code is None or status_code >= 500,
                )
            chunks: list[bytes] = []
            total = 0
            if callable(getattr(response, "iter_content", None)):
                iterator = response.iter_content(chunk_size=4096)
            else:
                iterator = [getattr(response, "content", b"")]
            for chunk in iterator:
                if not chunk:
                    continue
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                total += len(chunk)
                if total > self.max_body_bytes:
                    raise _ToolCollectionError(
                        "response_too_large",
                        "configured service response exceeded the size limit",
                        retryable=False,
                    )
                chunks.append(chunk)
            try:
                body = json.loads(b"".join(chunks).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise _ToolCollectionError(
                    "invalid_response",
                    "configured service returned malformed JSON",
                    retryable=False,
                ) from exc
            return status_code, body
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()


class ReadToolRegistry:
    """Execute only fixed, zero-argument, read-only evidence collectors."""

    def __init__(
        self,
        memory_store: RAppMemory | NullRAppMemory,
        *,
        dme_base_url: Optional[str] = None,
        evidence_api_base_url: Optional[str] = None,
        http_session: Any = None,
        command_runner: Callable[..., Any] = subprocess.run,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.memory_store = memory_store
        self.dme_base_url = _validated_base_url(
            config.DME_BASE_URL if dme_base_url is None else dme_base_url,
            "DME_BASE_URL",
        )
        self.evidence_api_base_url = _validated_base_url(
            config.EVIDENCE_API_BASE_URL
            if evidence_api_base_url is None
            else evidence_api_base_url,
            "EVIDENCE_API_BASE_URL",
        )
        if (
            not math.isfinite(config.RAPP_READ_TOOL_HTTP_TIMEOUT_S)
            or not 0 < config.RAPP_READ_TOOL_HTTP_TIMEOUT_S <= 30
        ):
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_HTTP_TIMEOUT_S must be between 0 and 30"
            )
        if not 1024 <= config.RAPP_READ_TOOL_HTTP_MAX_BODY_BYTES <= 2_000_000:
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_HTTP_MAX_BODY_BYTES must be between 1024 and 2000000"
            )
        if not 1000 <= config.RAPP_READ_TOOL_MAX_RESULT_CHARS <= 200_000:
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_MAX_RESULT_CHARS must be between 1000 and 200000"
            )
        if not 1 <= config.RAPP_READ_TOOL_HISTORY_LIMIT <= 120:
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_HISTORY_LIMIT must be between 1 and 120"
            )
        if not 1 <= config.RAPP_READ_TOOL_WINDOW_LIMIT <= 10:
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_WINDOW_LIMIT must be between 1 and 10"
            )
        if not 1 <= config.RAPP_READ_TOOL_WINDOW_MAX_SAMPLES <= 10_000:
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_WINDOW_MAX_SAMPLES must be between 1 and 10000"
            )
        if not 65_536 <= config.RAPP_READ_TOOL_WINDOW_MAX_BYTES <= 50_000_000:
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_WINDOW_MAX_BYTES must be between 65536 and 50000000"
            )
        if (
            not math.isfinite(config.RAPP_READ_TOOL_SEQUENCE_LOOKBACK_S)
            or not 5 <= config.RAPP_READ_TOOL_SEQUENCE_LOOKBACK_S <= 300
        ):
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_SEQUENCE_LOOKBACK_S must be between 5 and 300"
            )
        if not 2 <= config.RAPP_READ_TOOL_SEQUENCE_MAX_SAMPLES <= 2000:
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_SEQUENCE_MAX_SAMPLES must be between 2 and 2000"
            )
        try:
            ipaddress.ip_address(config.RAPP_READ_TOOL_PING_TARGET)
        except ValueError as exc:
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_PING_TARGET must be a literal IP address"
            ) from exc
        if not _INTERFACE_PATTERN.fullmatch(config.RAPP_READ_TOOL_PING_INTERFACE):
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_PING_INTERFACE is not a safe interface name"
            )
        if not 1 <= config.RAPP_READ_TOOL_PING_COUNT <= 10:
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_PING_COUNT must be between 1 and 10"
            )
        if not 1 <= config.RAPP_READ_TOOL_PING_REPLY_TIMEOUT_S <= 10:
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_PING_REPLY_TIMEOUT_S must be between 1 and 10"
            )
        if (
            not math.isfinite(config.RAPP_READ_TOOL_COMMAND_TIMEOUT_S)
            or not 1 <= config.RAPP_READ_TOOL_COMMAND_TIMEOUT_S <= 30
        ):
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_COMMAND_TIMEOUT_S must be between 1 and 30"
            )
        containers = tuple(config.RAPP_READ_TOOL_OAI_CONTAINERS)
        if not containers or len(containers) > 16 or len(set(containers)) != len(containers):
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_OAI_CONTAINERS must contain 1 to 16 unique names"
            )
        if not all(_CONTAINER_PATTERN.fullmatch(name) for name in containers):
            raise ReadToolConfigurationError(
                "RAPP_READ_TOOL_OAI_CONTAINERS contains an unsafe name"
            )
        if not _PATH_SEGMENT_PATTERN.fullmatch(config.JOB_ID):
            raise ReadToolConfigurationError("JOB_ID must be one safe URL path segment")
        self.containers = containers
        if http_session is None:
            http_session = requests.Session()
            http_session.trust_env = False
        self.http_session = http_session
        self.command_runner = command_runner
        self.now = now
        self._dme = _FixedJsonClient(
            self.dme_base_url,
            session=http_session,
            timeout_s=config.RAPP_READ_TOOL_HTTP_TIMEOUT_S,
            max_body_bytes=config.RAPP_READ_TOOL_HTTP_MAX_BODY_BYTES,
        )
        self._evidence = _FixedJsonClient(
            self.evidence_api_base_url,
            session=http_session,
            timeout_s=config.RAPP_READ_TOOL_HTTP_TIMEOUT_S,
            max_body_bytes=config.RAPP_READ_TOOL_HTTP_MAX_BODY_BYTES,
        )

    def catalog(self) -> list[dict[str, Any]]:
        return read_tool_catalog()

    def execute(self, tool_name: str) -> dict[str, Any]:
        methods = {
            GET_DME_JOB_STATUS: self._get_dme_job_status,
            GET_EVIDENCE_PIPELINE_STATUS: self._get_evidence_pipeline_status,
            GET_EVIDENCE_HISTORY: self._get_evidence_history,
            GET_RECENT_TELEMETRY_WINDOWS: self._get_recent_telemetry_windows,
            GET_SEQUENCE_ADVANCEMENT: self._get_sequence_advancement,
            PING_UE_PATH: self._ping_ue_path,
            GET_OAI_CONTAINER_STATUS: self._get_oai_container_status,
        }
        if tool_name not in methods:
            raise ValueError("read-tool name is not allowlisted")
        try:
            data = methods[tool_name]()
            result = self._result(tool_name, ok=True, data=data, error=None)
            errors = validate_read_tool_result(result)
            if errors:
                return self._result(
                    tool_name,
                    ok=False,
                    data={},
                    error={
                        "code": "invalid_result",
                        "message": "read-only collector produced invalid evidence",
                        "retryable": False,
                    },
                )
            return result
        except _ToolCollectionError as exc:
            return self._result(
                tool_name,
                ok=False,
                data={},
                error={
                    "code": exc.code,
                    "message": exc.message,
                    "retryable": exc.retryable,
                },
            )
        except Exception:
            return self._result(
                tool_name,
                ok=False,
                data={},
                error={
                    "code": "unavailable",
                    "message": "read-only evidence collector is unavailable",
                    "retryable": True,
                },
            )

    def _result(
        self,
        tool_name: str,
        *,
        ok: bool,
        data: dict[str, Any],
        error: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "read_tool_schema_version": READ_TOOL_SCHEMA_VERSION,
            "tool_name": tool_name,
            "scope": _SCOPES[tool_name],
            "observed_at": isoformat_utc(self.now()),
            "ok": ok,
            "data": copy.deepcopy(data),
            "error": copy.deepcopy(error),
            "limitations": list(_LIMITATIONS[tool_name]),
        }

    def _get_dme_job_status(self) -> dict[str, Any]:
        _, service = self._dme.get("/status", accepted_statuses={200})
        if not isinstance(service, dict):
            raise _ToolCollectionError(
                "invalid_response", "DME status response was malformed", retryable=False
            )
        job_code, job = self._dme.get(
            f"/data-consumer/v1/info-jobs/{config.JOB_ID}",
            accepted_statuses={200, 404},
        )
        if job_code == 200 and not isinstance(job, dict):
            raise _ToolCollectionError(
                "invalid_response", "DME job response was malformed", retryable=False
            )
        job_registered = job_code == 200 and isinstance(job, dict)
        operational_state = "UNKNOWN"
        producer_ids: list[str] = []
        if job_registered:
            _, job_state = self._dme.get(
                f"/data-consumer/v1/info-jobs/{config.JOB_ID}/status",
                accepted_statuses={200},
            )
            if not isinstance(job_state, dict):
                raise _ToolCollectionError(
                    "invalid_response",
                    "DME job-state response was malformed",
                    retryable=False,
                )
            raw_state = job_state.get("info_job_status")
            if raw_state in {"ENABLED", "DISABLED"}:
                operational_state = raw_state
            raw_producers = job_state.get("producers")
            if isinstance(raw_producers, list):
                producer_ids = [
                    bounded
                    for value in raw_producers[:32]
                    if (bounded := _bounded_string(value, 128)) is not None
                ]
        configured_callback = (
            f"{config.CONSUMER_BASE_URL}{config.CONSUMER_CALLBACK_PATH}"
        )
        definition = job.get("job_definition") if job_registered else None
        definition_callback = (
            definition.get("callback_url") if isinstance(definition, dict) else None
        )
        return {
            "dme_living": service.get("status") == "living",
            "registered_info_type_count": _nonnegative_int(service.get("no_of_types")),
            "registered_producer_count": _nonnegative_int(
                service.get("no_of_producers")
            ),
            "registered_job_count": _nonnegative_int(service.get("no_of_jobs")),
            "configured_job_id": config.JOB_ID,
            "job_registered": job_registered,
            "job_operational_state": operational_state,
            "producer_ids": producer_ids,
            "info_type_matches": (
                job.get("info_type_id") == config.INFO_TYPE_ID
                if job_registered
                else None
            ),
            "job_owner_matches": (
                job.get("job_owner") == config.JOB_OWNER if job_registered else None
            ),
            "callback_matches_config": (
                job.get("job_result_uri") == configured_callback
                and definition_callback == configured_callback
                if job_registered
                else None
            ),
        }

    def _get_evidence_pipeline_status(self) -> dict[str, Any]:
        _, health = self._evidence.get("/healthz", accepted_statuses={200})
        readiness_code, readiness = self._evidence.get(
            "/readyz", accepted_statuses={200, 503}
        )
        _, producer = self._evidence.get(
            "/producer/health-check", accepted_statuses={200}
        )
        if not all(isinstance(item, dict) for item in (health, readiness, producer)):
            raise _ToolCollectionError(
                "invalid_response",
                "Evidence API status response was malformed",
                retryable=False,
            )
        ready = readiness.get("status") == "ready"
        if ready != (readiness_code == 200):
            raise _ToolCollectionError(
                "invalid_response",
                "Evidence API readiness status contradicted its HTTP status",
                retryable=False,
            )
        return {
            "living": health.get("status") == "living",
            "ready": ready,
            "repository_ready": (
                readiness.get("repository")
                if isinstance(readiness.get("repository"), bool)
                else None
            ),
            "dme_registered": (
                readiness.get("dme_registered")
                if isinstance(readiness.get("dme_registered"), bool)
                else None
            ),
            "producer_living": producer.get("status") == "living",
            "evidence_received": (
                producer.get("evidence_received")
                if isinstance(producer.get("evidence_received"), bool)
                else None
            ),
            "active_job_count": _nonnegative_int(producer.get("active_jobs")),
        }

    def _get_evidence_history(self) -> dict[str, Any]:
        limit = config.RAPP_READ_TOOL_HISTORY_LIMIT
        _, body = self._evidence.get(
            f"/v1/evidence/history?limit={limit}", accepted_statuses={200}
        )
        if not isinstance(body, dict) or not isinstance(body.get("events"), list):
            raise _ToolCollectionError(
                "invalid_response",
                "Evidence API history response was malformed",
                retryable=False,
            )
        normalized = []
        invalid = 0
        omitted_for_size = 0
        for event in body["events"][:limit]:
            try:
                errors = validate_telemetry_payload(event)
            except Exception:
                errors = ["validator failure"]
            if errors:
                invalid += 1
                continue
            if not all(
                _is_int64(event[field], nonnegative=(field == "sequence_number"))
                for field in (
                    "sequence_number",
                    "e2_nodes_connected",
                    "kpm_indications_received",
                )
            ):
                invalid += 1
                continue
            metric_names = sorted(
                bounded
                for name in event["metrics"]
                if isinstance(name, str)
                and name
                and (bounded := _bounded_string(name, 128)) is not None
            )
            candidate = {
                "source": _bounded_string(event["source"], 64),
                "source_instance_id": _bounded_string(
                    event["source_instance_id"], 64
                ),
                "sequence_number": event["sequence_number"],
                "observed_at": event["observed_at"],
                "last_kpm_indication_at": event["last_kpm_indication_at"],
                "ric_connected": event["ric_connected"],
                "e2_nodes_connected": event["e2_nodes_connected"],
                "kpm_indications_received": event["kpm_indications_received"],
                "incomplete": event["incomplete"],
                "missing_metric_count": len(event["missing_metrics"]),
                "metric_names": metric_names[:32],
                "metric_names_omitted": max(0, len(metric_names) - 32),
            }
            projected = normalized + [candidate]
            projected_data = {
                "requested_limit": limit,
                "returned_count": len(projected),
                "invalid_events_omitted": invalid,
                "events_omitted_for_size": omitted_for_size,
                "events": projected,
            }
            if len(
                json.dumps(projected_data, ensure_ascii=False, separators=(",", ":"))
            ) > max(1000, config.RAPP_READ_TOOL_MAX_RESULT_CHARS - 2000):
                omitted_for_size += 1
                continue
            normalized.append(candidate)
        return {
            "requested_limit": limit,
            "returned_count": len(normalized),
            "invalid_events_omitted": invalid,
            "events_omitted_for_size": omitted_for_size,
            "events": normalized,
        }

    def _get_recent_telemetry_windows(self) -> dict[str, Any]:
        windows = self.memory_store.read_recent_window_facts(
            limit=config.RAPP_READ_TOOL_WINDOW_LIMIT,
            max_samples=config.RAPP_READ_TOOL_WINDOW_MAX_SAMPLES,
            max_bytes=config.RAPP_READ_TOOL_WINDOW_MAX_BYTES,
        )
        return {
            "requested_limit": config.RAPP_READ_TOOL_WINDOW_LIMIT,
            "returned_count": len(windows),
            "windows": windows,
        }

    def _get_sequence_advancement(self) -> dict[str, Any]:
        return self.memory_store.read_sequence_advancement(
            lookback_seconds=config.RAPP_READ_TOOL_SEQUENCE_LOOKBACK_S,
            max_samples=config.RAPP_READ_TOOL_SEQUENCE_MAX_SAMPLES,
            now=self.now(),
        )

    def _run_process(self, arguments: list[str], *, timeout: float) -> Any:
        try:
            return self.command_runner(
                arguments,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
                shell=False,
                stdin=subprocess.DEVNULL,
                env={"PATH": "/usr/bin:/bin", "LC_ALL": "C", "LANG": "C"},
            )
        except subprocess.TimeoutExpired as exc:
            raise _ToolCollectionError(
                "timeout", "read-only process timed out", retryable=True
            ) from exc
        except PermissionError as exc:
            raise _ToolCollectionError(
                "permission_denied",
                "read-only process permission was denied",
                retryable=False,
            ) from exc
        except FileNotFoundError as exc:
            raise _ToolCollectionError(
                "unavailable", "required read-only executable is unavailable", retryable=False
            ) from exc
        except OSError as exc:
            raise _ToolCollectionError(
                "unavailable", "read-only process could not be started", retryable=True
            ) from exc

    def _ping_ue_path(self) -> dict[str, Any]:
        arguments = [
            "/usr/bin/ping",
            "-n",
            "-c",
            str(config.RAPP_READ_TOOL_PING_COUNT),
            "-W",
            str(config.RAPP_READ_TOOL_PING_REPLY_TIMEOUT_S),
            "-I",
            config.RAPP_READ_TOOL_PING_INTERFACE,
            config.RAPP_READ_TOOL_PING_TARGET,
        ]
        completed = self._run_process(
            arguments, timeout=config.RAPP_READ_TOOL_COMMAND_TIMEOUT_S
        )
        stdout = str(getattr(completed, "stdout", ""))[:16384]
        stderr = str(getattr(completed, "stderr", ""))[:4096]
        return_code = getattr(completed, "returncode", None)
        match = _PING_STATS.search(stdout)
        if match is None:
            code = "permission_denied" if "permission" in stderr.lower() else "command_failed"
            raise _ToolCollectionError(
                code,
                "bounded UE ping did not return parseable statistics",
                retryable=code == "command_failed",
            )
        rtt = _PING_RTT.search(stdout)
        return {
            "direction": "ue_to_external_data_network",
            "interface": config.RAPP_READ_TOOL_PING_INTERFACE,
            "target": config.RAPP_READ_TOOL_PING_TARGET,
            "transmitted": int(match.group("transmitted")),
            "received": int(match.group("received")),
            "packet_loss_percent": float(match.group("loss")),
            "rtt_min_ms": float(rtt.group("minimum")) if rtt else None,
            "rtt_avg_ms": float(rtt.group("average")) if rtt else None,
            "rtt_max_ms": float(rtt.group("maximum")) if rtt else None,
            "rtt_mdev_ms": float(rtt.group("mdev")) if rtt else None,
            "return_code": return_code if isinstance(return_code, int) else None,
            "timed_out": False,
        }

    def _get_oai_container_status(self) -> dict[str, Any]:
        deadline = time.monotonic() + config.RAPP_READ_TOOL_COMMAND_TIMEOUT_S
        containers = []
        for name in self.containers:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise _ToolCollectionError(
                    "timeout", "OAI container inspection timed out", retryable=True
                )
            completed = self._run_process(
                [
                    "/usr/bin/docker",
                    "container",
                    "inspect",
                    "--format",
                    (
                        "{{.State.Status}}\t{{.State.Running}}\t"
                        "{{if .State.Health}}{{.State.Health.Status}}"
                        "{{else}}no_healthcheck{{end}}"
                    ),
                    name,
                ],
                timeout=min(2.0, remaining),
            )
            return_code = getattr(completed, "returncode", None)
            stdout = str(getattr(completed, "stdout", ""))[:16384].strip()
            stderr = str(getattr(completed, "stderr", ""))[:4096].lower()
            if return_code != 0:
                if "no such object" in stderr or "no such container" in stderr:
                    containers.append(
                        {
                            "name": name,
                            "present": False,
                            "running": False,
                            "runtime_state": "missing",
                            "health_state": "unknown",
                        }
                    )
                    continue
                code = "permission_denied" if "permission denied" in stderr else "command_failed"
                raise _ToolCollectionError(
                    code,
                    "Docker could not inspect the allowlisted containers",
                    retryable=code == "command_failed",
                )
            state_fields = stdout.split("\t")
            if len(state_fields) != 3 or state_fields[1] not in {"true", "false"}:
                raise _ToolCollectionError(
                    "invalid_response",
                    "Docker returned malformed container state",
                    retryable=False,
                )
            runtime_state, running_text, health_state = state_fields
            if (
                runtime_state
                not in {
                    "created",
                    "restarting",
                    "running",
                    "removing",
                    "paused",
                    "exited",
                    "dead",
                }
                or health_state
                not in {"no_healthcheck", "starting", "healthy", "unhealthy"}
            ):
                raise _ToolCollectionError(
                    "invalid_response",
                    "Docker returned malformed container state",
                    retryable=False,
                )
            containers.append(
                {
                    "name": name,
                    "present": True,
                    "running": running_text == "true",
                    "runtime_state": runtime_state,
                    "health_state": health_state,
                }
            )
        return {
            "requested_count": len(self.containers),
            "present_count": sum(item["present"] for item in containers),
            "running_count": sum(item["running"] for item in containers),
            "containers": containers,
        }
