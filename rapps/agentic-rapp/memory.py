"""Durable conversational and rolling-telemetry memory for the Health rApp.

DeepSeek's chat-completions endpoint is stateless.  This module therefore keeps
the source of truth locally in SQLite, rolls received telemetry into bounded
time windows, and stores the advisory digest returned by a background model
call.  A later question explicitly receives that digest, the current partial
window, and prior advisory conversation turns.

No deterministic HealthReport is stored in, or reconstructed by, this module.
The latest-snapshot evaluator in ``health_checks.py`` remains authoritative.
"""
from __future__ import annotations

import copy
import json
import logging
import math
import os
import queue
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Protocol

import config
from telemetry import isoformat_utc, parse_timestamp


logger = logging.getLogger(__name__)

MEMORY_SCHEMA_VERSION = "1.0"
WINDOW_SCHEMA_VERSION = "1.0"

_WINDOW_FIELDS = {
    "window_schema_version",
    "window_kind",
    "window_start",
    "window_end",
    "received_sample_count",
    "included_sample_count",
    "omitted_sample_count",
    "compaction",
    "facts_compaction",
    "all_sample_facts",
    "samples",
}
_SAMPLE_FIELDS = {
    "received_at",
    "producer_pushed_at",
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
}
_MEMORY_CONTEXT_FIELDS = {
    "memory_schema_version",
    "thread_id",
    "conversation",
    "completed_windows",
    "partial_window",
    "context_limits",
}
_CONVERSATION_FIELDS = {"asked_at", "question", "advisory_explanation"}
_COMPLETED_WINDOW_FIELDS = {
    "window_start",
    "window_end",
    "received_sample_count",
    "advisory_digest",
    "digest_source",
}
_CONTEXT_LIMIT_FIELDS = {
    "max_context_chars",
    "conversation_turns_omitted",
    "completed_windows_omitted",
}
_WINDOW_FACT_FIELDS = {
    "sample_count",
    "ric_connected_true_observations",
    "ric_connected_false_observations",
    "incomplete_observations",
    "e2_nodes_connected_range",
    "kpm_indication_counters",
    "kpm_counter_streams_omitted",
    "reported_missing_metrics",
    "missing_metric_names_omitted",
    "sequence_streams",
    "sequence_streams_omitted",
    "metric_series",
    "metric_series_omitted",
    "unit_conflict_metric_names",
    "unit_conflict_metric_names_omitted",
}


def _utc(value: Optional[datetime] = None) -> datetime:
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def normalize_thread_id(thread_id: Optional[str]) -> str:
    """Resolve the stable default CLI session and reject unsafe identifiers."""
    resolved = config.RAPP_DEFAULT_THREAD_ID if thread_id is None else thread_id
    if not isinstance(resolved, str) or not resolved.strip():
        raise ValueError("thread_id must be a non-empty string")
    resolved = resolved.strip()
    if len(resolved) > config.RAPP_MAX_THREAD_ID_CHARS:
        raise ValueError(
            f"thread_id exceeds {config.RAPP_MAX_THREAD_ID_CHARS} characters"
        )
    return resolved


@dataclass(frozen=True)
class StoredSample:
    id: int
    recorded_at: str
    recorded_epoch: float
    snapshot: dict[str, Any]


@dataclass(frozen=True)
class PendingTelemetryWindow:
    first_sample_id: int
    last_sample_id: int
    window_start: str
    window_end: str
    samples: tuple[StoredSample, ...]


class WindowSummarizer(Protocol):
    def summarize_window(self, window: dict[str, Any]) -> str: ...


def _sample_for_llm(sample: StoredSample) -> dict[str, Any]:
    snapshot = sample.snapshot
    telemetry = snapshot.get("telemetry")
    telemetry = telemetry if isinstance(telemetry, dict) else {}
    source_metrics = telemetry.get("metrics")
    metrics: dict[str, Any] = {}
    if isinstance(source_metrics, dict):
        for name, measurement in source_metrics.items():
            if isinstance(name, str) and isinstance(measurement, dict):
                copied = {
                    field: copy.deepcopy(measurement[field])
                    for field in ("value", "unit", "valid")
                    if field in measurement
                }
                metrics[name] = copied

    pushed_at = parse_timestamp(snapshot.get("producer_pushed_at"))
    return {
        "received_at": sample.recorded_at,
        "producer_pushed_at": isoformat_utc(pushed_at) if pushed_at else None,
        "source": copy.deepcopy(telemetry.get("source")),
        "source_instance_id": copy.deepcopy(
            telemetry.get("source_instance_id")
        ),
        "sequence_number": copy.deepcopy(telemetry.get("sequence_number")),
        "observed_at": copy.deepcopy(telemetry.get("observed_at")),
        "ric_connected": copy.deepcopy(telemetry.get("ric_connected")),
        "e2_nodes_connected": copy.deepcopy(
            telemetry.get("e2_nodes_connected")
        ),
        "kpm_indications_received": copy.deepcopy(
            telemetry.get("kpm_indications_received")
        ),
        "last_kpm_indication_at": copy.deepcopy(
            telemetry.get("last_kpm_indication_at")
        ),
        "metrics": metrics,
        "missing_metrics": copy.deepcopy(telemetry.get("missing_metrics")),
        "incomplete": copy.deepcopy(telemetry.get("incomplete")),
    }


def _bounded_label(value: Any, limit: int) -> Optional[str]:
    if not isinstance(value, str):
        return None
    return value if len(value) <= limit else value[:limit]


def _bounded_advisory_text(value: str, limit: int) -> str:
    safe = "".join(
        character
        for character in value
        if character in {"\n", "\t"}
        or (ord(character) >= 32 and not 127 <= ord(character) <= 159)
    ).strip()
    if len(safe) <= limit:
        return safe
    marker = " [truncated by local memory limit]"
    return safe[: max(0, limit - len(marker))].rstrip() + marker


def _window_facts(
    samples: tuple[StoredSample, ...] | list[StoredSample],
    *,
    max_metric_series: int = 32,
    max_sequence_streams: int = 16,
    max_missing_names: int = 64,
) -> dict[str, Any]:
    """Calculate bounded facts over every row, including omitted raw samples."""
    ric_true = 0
    ric_false = 0
    incomplete = 0
    e2_values: list[int] = []
    indication_counters: dict[tuple[str, str], list[int]] = {}
    missing: set[str] = set()
    metric_values: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    metric_names_to_units: dict[tuple[str, str, str], set[str]] = {}
    sequences: dict[tuple[str, str], list[int]] = {}

    for sample in samples:
        telemetry = sample.snapshot.get("telemetry")
        if not isinstance(telemetry, dict):
            continue
        if telemetry.get("ric_connected") is True:
            ric_true += 1
        elif telemetry.get("ric_connected") is False:
            ric_false += 1
        if telemetry.get("incomplete") is True:
            incomplete += 1
        e2_count = telemetry.get("e2_nodes_connected")
        if isinstance(e2_count, int) and not isinstance(e2_count, bool):
            e2_values.append(e2_count)
        for name in telemetry.get("missing_metrics", []):
            if isinstance(name, str) and name:
                missing.add(name)

        source = telemetry.get("source")
        instance = telemetry.get("source_instance_id")
        sequence = telemetry.get("sequence_number")
        if (
            isinstance(source, str)
            and isinstance(instance, str)
            and isinstance(sequence, int)
            and not isinstance(sequence, bool)
        ):
            sequences.setdefault((source, instance), []).append(sequence)
            indication_count = telemetry.get("kpm_indications_received")
            if isinstance(indication_count, int) and not isinstance(
                indication_count, bool
            ):
                indication_counters.setdefault((source, instance), []).append(
                    indication_count
                )

        source_metrics = telemetry.get("metrics")
        if not isinstance(source_metrics, dict):
            continue
        for name, measurement in source_metrics.items():
            if (
                not isinstance(source, str)
                or not isinstance(instance, str)
                or not isinstance(name, str)
                or not isinstance(measurement, dict)
            ):
                continue
            unit = measurement.get("unit")
            if not isinstance(unit, str):
                continue
            metric_names_to_units.setdefault((source, instance, name), set()).add(
                unit
            )
            key = (source, instance, name, unit)
            entry = metric_values.setdefault(
                key,
                {
                    "valid_values": [],
                    "invalid_observation_count": 0,
                },
            )
            value = measurement.get("value")
            valid = measurement.get("valid", True) is True
            if (
                valid
                and isinstance(value, (int, float))
                and not isinstance(value, bool)
                and math.isfinite(value)
            ):
                entry["valid_values"].append(float(value))
            else:
                entry["invalid_observation_count"] += 1

    sequence_streams = []
    for (source, instance), values in list(sequences.items())[:max_sequence_streams]:
        gaps = 0
        non_increasing = 0
        for previous, current in zip(values, values[1:]):
            if current > previous:
                gaps += max(0, current - previous - 1)
            else:
                non_increasing += 1
        sequence_streams.append(
            {
                "source": _bounded_label(source, 64),
                "source_instance_id": _bounded_label(instance, 64),
                "observations": len(values),
                "first": values[0],
                "last": values[-1],
                "minimum": min(values),
                "maximum": max(values),
                "observed_sequence_gaps": gaps,
                "non_increasing_steps": non_increasing,
            }
        )

    metric_series = []
    for (source, instance, name, unit), entry in list(metric_values.items())[
        :max_metric_series
    ]:
        valid_values = entry["valid_values"]
        metric_series.append(
            {
                "source": _bounded_label(source, 64),
                "source_instance_id": _bounded_label(instance, 64),
                "name": _bounded_label(name, 128),
                "unit": _bounded_label(unit, 64),
                "valid_observation_count": len(valid_values),
                "invalid_observation_count": entry[
                    "invalid_observation_count"
                ],
                "minimum": min(valid_values) if valid_values else None,
                "maximum": max(valid_values) if valid_values else None,
                "latest": valid_values[-1] if valid_values else None,
            }
        )

    missing_names = sorted(missing)
    unit_conflicts = sorted(
        (source, instance, name)
        for (source, instance, name), units in metric_names_to_units.items()
        if len(units) > 1
    )
    kpm_counters = [
        {
            "source": _bounded_label(source, 64),
            "source_instance_id": _bounded_label(instance, 64),
            "observations": len(values),
            "first": values[0],
            "last": values[-1],
            "minimum": min(values),
            "maximum": max(values),
        }
        for (source, instance), values in list(indication_counters.items())[
            :max_sequence_streams
        ]
    ]
    return {
        "sample_count": len(samples),
        "ric_connected_true_observations": ric_true,
        "ric_connected_false_observations": ric_false,
        "incomplete_observations": incomplete,
        "e2_nodes_connected_range": (
            [min(e2_values), max(e2_values)] if e2_values else None
        ),
        "kpm_indication_counters": kpm_counters,
        "kpm_counter_streams_omitted": max(
            0, len(indication_counters) - max_sequence_streams
        ),
        "reported_missing_metrics": [
            _bounded_label(name, 128)
            for name in missing_names[:max_missing_names]
        ],
        "missing_metric_names_omitted": max(
            0, len(missing_names) - max_missing_names
        ),
        "sequence_streams": sequence_streams,
        "sequence_streams_omitted": max(
            0, len(sequences) - max_sequence_streams
        ),
        "metric_series": metric_series,
        "metric_series_omitted": max(
            0, len(metric_values) - max_metric_series
        ),
        "unit_conflict_metric_names": [
            _bounded_label(f"{source}/{instance}:{name}", 192)
            for source, instance, name in unit_conflicts[:32]
        ],
        "unit_conflict_metric_names_omitted": max(
            0, len(unit_conflicts) - 32
        ),
    }


def _counts_only_window_facts(facts: dict[str, Any]) -> dict[str, Any]:
    compacted = copy.deepcopy(facts)
    compacted["missing_metric_names_omitted"] += len(
        compacted["reported_missing_metrics"]
    )
    compacted["reported_missing_metrics"] = []
    compacted["sequence_streams_omitted"] += len(
        compacted["sequence_streams"]
    )
    compacted["sequence_streams"] = []
    compacted["kpm_counter_streams_omitted"] += len(
        compacted["kpm_indication_counters"]
    )
    compacted["kpm_indication_counters"] = []
    compacted["metric_series_omitted"] += len(compacted["metric_series"])
    compacted["metric_series"] = []
    compacted["unit_conflict_metric_names_omitted"] += len(
        compacted["unit_conflict_metric_names"]
    )
    compacted["unit_conflict_metric_names"] = []
    return compacted


def compact_window_to_metadata(window: dict[str, Any]) -> dict[str, Any]:
    """Remove raw samples while retaining explicit all-sample count facts."""
    compacted = copy.deepcopy(window)
    compacted["omitted_sample_count"] = compacted["received_sample_count"]
    compacted["included_sample_count"] = 0
    compacted["compaction"] = "metadata_only_for_size"
    compacted["facts_compaction"] = "counts_only_for_size"
    compacted["all_sample_facts"] = _counts_only_window_facts(
        compacted["all_sample_facts"]
    )
    compacted["samples"] = []
    return compacted


def _evenly_spaced(items: list[StoredSample], limit: int) -> list[StoredSample]:
    if len(items) <= limit:
        return items
    if limit <= 1:
        return [items[-1]]
    indexes = {
        round(position * (len(items) - 1) / (limit - 1))
        for position in range(limit)
    }
    return [items[index] for index in sorted(indexes)]


def build_window_payload(
    samples: tuple[StoredSample, ...] | list[StoredSample],
    *,
    window_start: str,
    window_end: str,
    window_kind: str,
    max_samples: Optional[int] = None,
    max_chars: Optional[int] = None,
) -> dict[str, Any]:
    """Create an allowlisted, explicitly compacted model payload."""
    all_samples = list(samples)
    max_samples = (
        config.TELEMETRY_MEMORY_MAX_SAMPLES_PER_PAYLOAD
        if max_samples is None
        else max_samples
    )
    max_chars = (
        config.TELEMETRY_MEMORY_MAX_PAYLOAD_CHARS
        if max_chars is None
        else max_chars
    )
    if max_samples < 1 or max_chars < 1000:
        raise ValueError("telemetry memory payload limits are too small")

    eligible = _evenly_spaced(all_samples, max_samples)
    full_facts = _window_facts(all_samples)

    def make_payload(
        selected: list[StoredSample],
        compaction: str,
        *,
        facts: Optional[dict[str, Any]] = None,
        facts_compaction: str = "none",
    ) -> dict[str, Any]:
        return {
            "window_schema_version": WINDOW_SCHEMA_VERSION,
            "window_kind": window_kind,
            "window_start": window_start,
            "window_end": window_end,
            "received_sample_count": len(all_samples),
            "included_sample_count": len(selected),
            "omitted_sample_count": len(all_samples) - len(selected),
            "compaction": compaction,
            "facts_compaction": facts_compaction,
            "all_sample_facts": facts if facts is not None else full_facts,
            "samples": [_sample_for_llm(sample) for sample in selected],
        }

    compaction = "none" if len(eligible) == len(all_samples) else "evenly_sampled"
    payload = make_payload(eligible, compaction)
    if len(json.dumps(payload, ensure_ascii=False, separators=(",", ":"))) > max_chars:
        best_payload: Optional[dict[str, Any]] = None
        low = 1
        high = len(eligible)
        while low <= high:
            candidate_count = (low + high) // 2
            candidate = _evenly_spaced(eligible, candidate_count)
            candidate_payload = make_payload(
                candidate,
                "evenly_sampled_for_size",
            )
            candidate_size = len(
                json.dumps(
                    candidate_payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
            if candidate_size <= max_chars:
                best_payload = candidate_payload
                low = candidate_count + 1
            else:
                high = candidate_count - 1
        if best_payload is not None:
            payload = best_payload

    if len(json.dumps(payload, ensure_ascii=False, separators=(",", ":"))) > max_chars:
        # A producer can legally use long metric/source strings.  Never slice a
        # structured value into misleading evidence; expose that every sample
        # was omitted and retain only bounded window metadata instead.
        payload = compact_window_to_metadata(payload)
    if len(json.dumps(payload, ensure_ascii=False, separators=(",", ":"))) > max_chars:
        raise ValueError("telemetry memory metadata exceeds the payload limit")
    return payload


def _validate_window_facts(facts: Any, sample_count: Any) -> list[str]:
    if not isinstance(facts, dict) or set(facts) != _WINDOW_FACT_FIELDS:
        return ["all-sample window facts do not match the allowed schema"]
    errors: list[str] = []
    integer_fields = (
        "sample_count",
        "ric_connected_true_observations",
        "ric_connected_false_observations",
        "incomplete_observations",
        "missing_metric_names_omitted",
        "sequence_streams_omitted",
        "kpm_counter_streams_omitted",
        "metric_series_omitted",
        "unit_conflict_metric_names_omitted",
    )
    for field in integer_fields:
        value = facts.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            errors.append(f"window fact {field} must be a non-negative integer")
    if facts.get("sample_count") != sample_count:
        errors.append("window fact sample_count is inconsistent")
    if isinstance(sample_count, int) and not isinstance(sample_count, bool):
        for field in (
            "ric_connected_true_observations",
            "ric_connected_false_observations",
            "incomplete_observations",
        ):
            if isinstance(facts.get(field), int) and facts[field] > sample_count:
                errors.append(f"window fact {field} exceeds sample_count")
        if (
            isinstance(facts.get("ric_connected_true_observations"), int)
            and isinstance(facts.get("ric_connected_false_observations"), int)
            and facts["ric_connected_true_observations"]
            + facts["ric_connected_false_observations"]
            > sample_count
        ):
            errors.append("window RIC observation counts exceed sample_count")

    e2_range = facts.get("e2_nodes_connected_range")
    if e2_range is not None and (
        not isinstance(e2_range, list)
        or len(e2_range) != 2
        or any(isinstance(value, bool) or not isinstance(value, int) for value in e2_range)
    ):
        errors.append("window E2-node range is malformed")
    elif e2_range is not None and e2_range[0] > e2_range[1]:
        errors.append("window E2-node range is reversed")
    counters = facts.get("kpm_indication_counters")
    counter_fields = {
        "source",
        "source_instance_id",
        "observations",
        "first",
        "last",
        "minimum",
        "maximum",
    }
    if not isinstance(counters, list):
        errors.append("window KPM indication counters must be an array")
    else:
        for counter in counters:
            if not isinstance(counter, dict) or set(counter) != counter_fields:
                errors.append("window KPM indication counter is malformed")
                continue
            if not isinstance(counter.get("source"), str) or not isinstance(
                counter.get("source_instance_id"), str
            ):
                errors.append("window KPM indication counter identity is malformed")
            for field in counter_fields - {"source", "source_instance_id"}:
                value = counter.get(field)
                if (
                    isinstance(value, bool)
                    or not isinstance(value, int)
                    or value < 0
                ):
                    errors.append("window KPM indication counter value is malformed")
                    break
            counter_value_fields = counter_fields - {
                "source",
                "source_instance_id",
            }
            if all(
                isinstance(counter.get(field), int)
                for field in counter_value_fields
            ):
                if not (
                    counter["minimum"]
                    <= counter["first"]
                    <= counter["maximum"]
                    and counter["minimum"]
                    <= counter["last"]
                    <= counter["maximum"]
                ):
                    errors.append("window KPM indication counter range is inconsistent")
                if isinstance(sample_count, int) and counter["observations"] > sample_count:
                    errors.append("window KPM indication observations exceed sample_count")

    for field in ("reported_missing_metrics", "unit_conflict_metric_names"):
        values = facts.get(field)
        if not isinstance(values, list) or not all(
            isinstance(value, str) for value in values
        ):
            errors.append(f"window fact {field} must be an array of strings")

    streams = facts.get("sequence_streams")
    stream_fields = {
        "source",
        "source_instance_id",
        "observations",
        "first",
        "last",
        "minimum",
        "maximum",
        "observed_sequence_gaps",
        "non_increasing_steps",
    }
    if not isinstance(streams, list):
        errors.append("window sequence streams must be an array")
    else:
        for stream in streams:
            if not isinstance(stream, dict) or set(stream) != stream_fields:
                errors.append("window sequence stream is malformed")
                continue
            if not isinstance(stream.get("source"), str) or not isinstance(
                stream.get("source_instance_id"), str
            ):
                errors.append("window sequence stream identity is malformed")
            for field in stream_fields - {"source", "source_instance_id"}:
                value = stream.get(field)
                if (
                    isinstance(value, bool)
                    or not isinstance(value, int)
                    or value < 0
                ):
                    errors.append("window sequence stream value is malformed")
                    break
            stream_value_fields = stream_fields - {
                "source",
                "source_instance_id",
            }
            if all(
                isinstance(stream.get(field), int)
                for field in stream_value_fields
            ):
                if not (
                    stream["minimum"] <= stream["first"] <= stream["maximum"]
                    and stream["minimum"] <= stream["last"] <= stream["maximum"]
                ):
                    errors.append("window sequence stream range is inconsistent")
                if isinstance(sample_count, int) and stream["observations"] > sample_count:
                    errors.append("window sequence observations exceed sample_count")

    series = facts.get("metric_series")
    series_fields = {
        "source",
        "source_instance_id",
        "name",
        "unit",
        "valid_observation_count",
        "invalid_observation_count",
        "minimum",
        "maximum",
        "latest",
    }
    if not isinstance(series, list):
        errors.append("window metric series must be an array")
    else:
        for item in series:
            if not isinstance(item, dict) or set(item) != series_fields:
                errors.append("window metric series item is malformed")
                continue
            if any(
                not isinstance(item.get(field), str)
                for field in ("source", "source_instance_id", "name", "unit")
            ):
                errors.append("window metric series identity is malformed")
            for field in ("valid_observation_count", "invalid_observation_count"):
                value = item.get(field)
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    errors.append("window metric observation count is malformed")
            for field in ("minimum", "maximum", "latest"):
                value = item.get(field)
                if value is not None and (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                ):
                    errors.append("window metric aggregate is malformed")
            minimum = item.get("minimum")
            maximum = item.get("maximum")
            latest = item.get("latest")
            if minimum is not None and maximum is not None and minimum > maximum:
                errors.append("window metric aggregate range is reversed")
            if (
                latest is not None
                and minimum is not None
                and maximum is not None
                and not minimum <= latest <= maximum
            ):
                errors.append("window metric latest value is outside its range")
            valid_count = item.get("valid_observation_count")
            invalid_count = item.get("invalid_observation_count")
            if (
                isinstance(sample_count, int)
                and isinstance(valid_count, int)
                and isinstance(invalid_count, int)
                and valid_count + invalid_count > sample_count
            ):
                errors.append("window metric observation counts exceed sample_count")
    return errors


def validate_window_payload(window: Any) -> list[str]:
    """Validate the exact telemetry-window schema accepted by the LLM client."""
    if not isinstance(window, dict) or set(window) != _WINDOW_FIELDS:
        return ["telemetry window does not match the allowed schema"]
    errors: list[str] = []
    if window.get("window_schema_version") != WINDOW_SCHEMA_VERSION:
        errors.append("unsupported telemetry window schema version")
    if window.get("window_kind") not in {"completed", "partial"}:
        errors.append("invalid telemetry window kind")
    parsed_window_times: dict[str, Optional[datetime]] = {}
    for field in ("window_start", "window_end"):
        parsed_window_times[field] = parse_timestamp(window.get(field))
        if parse_timestamp(window.get(field)) is None:
            errors.append(f"{field} must be a timezone-aware timestamp")
    if (
        parsed_window_times.get("window_start") is not None
        and parsed_window_times.get("window_end") is not None
        and parsed_window_times["window_start"] > parsed_window_times["window_end"]
    ):
        errors.append("telemetry window timestamps are reversed")
    for field in (
        "received_sample_count",
        "included_sample_count",
        "omitted_sample_count",
    ):
        value = window.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            errors.append(f"{field} must be a non-negative integer")
    if (
        isinstance(window.get("received_sample_count"), int)
        and isinstance(window.get("included_sample_count"), int)
        and isinstance(window.get("omitted_sample_count"), int)
        and window["received_sample_count"]
        != window["included_sample_count"] + window["omitted_sample_count"]
    ):
        errors.append("telemetry window sample counts are inconsistent")
    compaction = window.get("compaction")
    if compaction not in {
        "none",
        "evenly_sampled",
        "evenly_sampled_for_size",
        "metadata_only_for_size",
    }:
        errors.append("invalid telemetry window compaction label")
    received_count = window.get("received_sample_count")
    included_count = window.get("included_sample_count")
    omitted_count = window.get("omitted_sample_count")
    if received_count == 0:
        errors.append("telemetry windows must contain at least one received sample")
    if compaction == "none" and (
        omitted_count != 0 or included_count != received_count
    ):
        errors.append("uncompacted telemetry window has omitted samples")
    if compaction in {"evenly_sampled", "evenly_sampled_for_size"} and (
        not isinstance(omitted_count, int)
        or omitted_count <= 0
        or not isinstance(included_count, int)
        or included_count <= 0
    ):
        errors.append("sampled telemetry window has inconsistent counts")
    if compaction == "metadata_only_for_size" and (
        included_count != 0 or omitted_count != received_count
    ):
        errors.append("metadata-only telemetry window has inconsistent counts")
    if window.get("facts_compaction") not in {
        "none",
        "counts_only_for_size",
    }:
        errors.append("invalid telemetry window facts compaction label")
    facts = window.get("all_sample_facts")
    if (
        window.get("facts_compaction") == "counts_only_for_size"
        and isinstance(facts, dict)
        and any(
            facts.get(field)
            for field in (
                "reported_missing_metrics",
                "sequence_streams",
                "kpm_indication_counters",
                "metric_series",
                "unit_conflict_metric_names",
            )
        )
    ):
        errors.append("counts-only window facts still contain detailed series")
    errors.extend(
        _validate_window_facts(
            facts,
            window.get("received_sample_count"),
        )
    )
    samples = window.get("samples")
    if not isinstance(samples, list):
        errors.append("telemetry window samples must be an array")
        return errors
    if window.get("included_sample_count") != len(samples):
        errors.append("included sample count does not match samples")
    for sample in samples:
        if not isinstance(sample, dict) or set(sample) != _SAMPLE_FIELDS:
            errors.append("telemetry sample does not match the allowed schema")
            continue
        if parse_timestamp(sample.get("received_at")) is None:
            errors.append("telemetry sample received_at is malformed")
        else:
            sample_received_at = parse_timestamp(sample["received_at"])
            start = parsed_window_times.get("window_start")
            end = parsed_window_times.get("window_end")
            if (
                start is not None
                and end is not None
                and sample_received_at is not None
                and not start <= sample_received_at <= end
            ):
                errors.append("telemetry sample falls outside its window")
        producer_pushed_at = sample.get("producer_pushed_at")
        if producer_pushed_at is not None and parse_timestamp(producer_pushed_at) is None:
            errors.append("telemetry sample producer_pushed_at is malformed")
        for field in ("source", "source_instance_id"):
            if not isinstance(sample.get(field), str) or not sample[field]:
                errors.append(f"telemetry sample {field} must be a non-empty string")
        sequence = sample.get("sequence_number")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            errors.append("telemetry sample sequence_number is malformed")
        if parse_timestamp(sample.get("observed_at")) is None:
            errors.append("telemetry sample observed_at is malformed")
        last_kpm = sample.get("last_kpm_indication_at")
        if last_kpm is not None and parse_timestamp(last_kpm) is None:
            errors.append("telemetry sample last_kpm_indication_at is malformed")
        if not isinstance(sample.get("ric_connected"), bool):
            errors.append("telemetry sample ric_connected must be a boolean")
        for field in ("e2_nodes_connected", "kpm_indications_received"):
            value = sample.get(field)
            if isinstance(value, bool) or not isinstance(value, int):
                errors.append(f"telemetry sample {field} must be an integer")
        missing = sample.get("missing_metrics")
        if not isinstance(missing, list) or not all(
            isinstance(name, str) and name for name in missing
        ):
            errors.append("telemetry sample missing_metrics is malformed")
        if not isinstance(sample.get("incomplete"), bool):
            errors.append("telemetry sample incomplete must be a boolean")
        metrics = sample.get("metrics")
        if not isinstance(metrics, dict):
            errors.append("telemetry sample metrics must be an object")
        else:
            for name, measurement in metrics.items():
                if (
                    not isinstance(name, str)
                    or not name
                    or not isinstance(measurement, dict)
                    or not set(measurement).issubset({"value", "unit", "valid"})
                    or "value" not in measurement
                    or "unit" not in measurement
                ):
                    errors.append("telemetry sample metric is malformed")
                    continue
                value = measurement["value"]
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                    or not isinstance(measurement["unit"], str)
                    or (
                        "valid" in measurement
                        and not isinstance(measurement["valid"], bool)
                    )
                ):
                    errors.append("telemetry sample metric is malformed")
    return errors


def validate_memory_context(context: Any) -> list[str]:
    """Validate conversation/history context before it crosses the LLM boundary."""
    if not isinstance(context, dict) or set(context) != _MEMORY_CONTEXT_FIELDS:
        return ["memory context does not match the allowed schema"]
    errors: list[str] = []
    if context.get("memory_schema_version") != MEMORY_SCHEMA_VERSION:
        errors.append("unsupported memory schema version")
    if not isinstance(context.get("thread_id"), str):
        errors.append("memory thread_id must be a string")
    else:
        try:
            normalize_thread_id(context["thread_id"])
        except ValueError as exc:
            errors.append(str(exc))

    limits = context.get("context_limits")
    if not isinstance(limits, dict) or set(limits) != _CONTEXT_LIMIT_FIELDS:
        errors.append("memory context limits are malformed")
    else:
        for field in _CONTEXT_LIMIT_FIELDS:
            value = limits.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                errors.append(f"memory context limit {field} is malformed")

    conversation = context.get("conversation")
    if not isinstance(conversation, list):
        errors.append("conversation memory must be an array")
    else:
        for turn in conversation:
            if not isinstance(turn, dict) or set(turn) != _CONVERSATION_FIELDS:
                errors.append("conversation turn does not match the allowed schema")
                continue
            if parse_timestamp(turn.get("asked_at")) is None:
                errors.append("conversation timestamp is malformed")
            if not isinstance(turn.get("question"), str):
                errors.append("conversation question must be a string")
            advisory = turn.get("advisory_explanation")
            if advisory is not None and not isinstance(advisory, str):
                errors.append("advisory conversation text must be a string or null")

    completed = context.get("completed_windows")
    if not isinstance(completed, list):
        errors.append("completed telemetry windows must be an array")
    else:
        for window in completed:
            if not isinstance(window, dict) or set(window) != _COMPLETED_WINDOW_FIELDS:
                errors.append("completed window does not match the allowed schema")
                continue
            if parse_timestamp(window.get("window_start")) is None or parse_timestamp(
                window.get("window_end")
            ) is None:
                errors.append("completed window timestamps are malformed")
            count = window.get("received_sample_count")
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                errors.append("completed window sample count is malformed")
            if not isinstance(window.get("advisory_digest"), str):
                errors.append("completed window advisory digest must be a string")
            if window.get("digest_source") not in {
                "deepseek",
                "deterministic-fallback",
            }:
                errors.append("completed window digest source is invalid")

    partial = context.get("partial_window")
    if partial is not None:
        errors.extend(validate_window_payload(partial))
        if isinstance(partial, dict) and partial.get("window_kind") != "partial":
            errors.append("partial telemetry window has the wrong kind")
    return errors


def deterministic_window_digest(samples: tuple[StoredSample, ...]) -> str:
    """Produce bounded factual context when the optional model call fails."""
    facts = _window_facts(samples, max_metric_series=16)
    digest = {
        "digest_kind": "deterministic_all_sample_facts",
        "all_sample_facts": facts,
    }
    serialized = json.dumps(digest, ensure_ascii=False, separators=(",", ":"))
    if len(serialized) <= config.TELEMETRY_MEMORY_MAX_DIGEST_CHARS:
        return serialized
    digest["all_sample_facts"] = _counts_only_window_facts(facts)
    serialized = json.dumps(digest, ensure_ascii=False, separators=(",", ":"))
    if len(serialized) > config.TELEMETRY_MEMORY_MAX_DIGEST_CHARS:
        raise ValueError("deterministic telemetry digest exceeds its size limit")
    return serialized


class RAppMemory:
    """Single-process SQLite repository with thread-safe access and WAL mode."""

    def __init__(
        self,
        database_path: str,
        *,
        window_seconds: Optional[float] = None,
        window_close_grace_seconds: Optional[float] = None,
        raw_retention_hours: Optional[float] = None,
        retained_windows: Optional[int] = None,
        conversation_context_turns: Optional[int] = None,
        conversation_retained_turns: Optional[int] = None,
    ) -> None:
        self.database_path = database_path
        self.window_seconds = (
            config.TELEMETRY_MEMORY_WINDOW_S
            if window_seconds is None
            else window_seconds
        )
        self.raw_retention_hours = (
            config.TELEMETRY_MEMORY_RAW_RETENTION_HOURS
            if raw_retention_hours is None
            else raw_retention_hours
        )
        self.window_close_grace_seconds = (
            config.TELEMETRY_MEMORY_WINDOW_CLOSE_GRACE_S
            if window_close_grace_seconds is None
            else window_close_grace_seconds
        )
        self.retained_windows = (
            config.TELEMETRY_MEMORY_RETAINED_WINDOWS
            if retained_windows is None
            else retained_windows
        )
        self.conversation_context_turns = (
            config.RAPP_CONVERSATION_CONTEXT_TURNS
            if conversation_context_turns is None
            else conversation_context_turns
        )
        self.conversation_retained_turns = (
            config.RAPP_CONVERSATION_RETAINED_TURNS
            if conversation_retained_turns is None
            else conversation_retained_turns
        )
        if not math.isfinite(self.window_seconds) or self.window_seconds <= 0:
            raise ValueError("telemetry memory window must be positive")
        if (
            not math.isfinite(self.window_close_grace_seconds)
            or self.window_close_grace_seconds < 0
        ):
            raise ValueError("telemetry memory close grace cannot be negative")
        if (
            not math.isfinite(self.raw_retention_hours)
            or self.raw_retention_hours <= 0
        ):
            raise ValueError("raw telemetry retention must be positive")
        if self.retained_windows < 1:
            raise ValueError("at least one telemetry window must be retained")
        if self.conversation_context_turns < 0:
            raise ValueError("conversation context turn count cannot be negative")
        if self.conversation_retained_turns < max(
            1, self.conversation_context_turns
        ):
            raise ValueError(
                "retained conversation turns must cover the context turn count"
            )
        for name, value in (
            ("RAPP_MEMORY_MAX_CONTEXT_CHARS", config.RAPP_MEMORY_MAX_CONTEXT_CHARS),
            ("RAPP_MEMORY_MAX_ADVISORY_CHARS", config.RAPP_MEMORY_MAX_ADVISORY_CHARS),
            (
                "TELEMETRY_MEMORY_MAX_DIGEST_CHARS",
                config.TELEMETRY_MEMORY_MAX_DIGEST_CHARS,
            ),
        ):
            if value < 1000:
                raise ValueError(f"{name} must be at least 1000")

        connection_target = database_path
        if database_path != ":memory:":
            database_file = Path(database_path).expanduser().resolve()
            parent_existed = database_file.parent.exists()
            database_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if not parent_existed:
                os.chmod(database_file.parent, 0o700)
            database_file.touch(mode=0o600, exist_ok=True)
            os.chmod(database_file, 0o600)
            connection_target = str(database_file)
            self.database_path = connection_target
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            connection_target,
            timeout=5.0,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        with self._lock:
            self._connection.execute("PRAGMA foreign_keys = ON")
            if connection_target != ":memory:":
                self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = NORMAL")
            self._ensure_schema()
            if connection_target != ":memory:":
                for suffix in ("", "-wal", "-shm"):
                    auxiliary = Path(f"{connection_target}{suffix}")
                    if auxiliary.exists():
                        os.chmod(auxiliary, 0o600)

    def _ensure_schema(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS telemetry_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at TEXT NOT NULL,
                recorded_epoch REAL NOT NULL,
                source TEXT NOT NULL,
                source_instance_id TEXT NOT NULL,
                sequence_number INTEGER NOT NULL,
                snapshot_json TEXT NOT NULL,
                UNIQUE(source, source_instance_id, sequence_number)
            );
            CREATE INDEX IF NOT EXISTS telemetry_samples_time_idx
                ON telemetry_samples(recorded_epoch, id);
            CREATE INDEX IF NOT EXISTS telemetry_samples_stream_time_idx
                ON telemetry_samples(
                    source, source_instance_id, recorded_epoch, id
                );

            CREATE TABLE IF NOT EXISTS telemetry_windows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_sample_id INTEGER NOT NULL,
                last_sample_id INTEGER NOT NULL UNIQUE,
                last_sample_epoch REAL NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                received_sample_count INTEGER NOT NULL,
                advisory_digest TEXT NOT NULL,
                digest_source TEXT NOT NULL,
                llm_error TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS conversation_turns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL,
                asked_at TEXT NOT NULL,
                question TEXT NOT NULL,
                advisory_explanation TEXT,
                explanation_source TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS conversation_thread_idx
                ON conversation_turns(thread_id, id DESC);

            CREATE TABLE IF NOT EXISTS memory_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        self._connection.commit()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def record_snapshot(
        self,
        snapshot: dict[str, Any],
        *,
        recorded_at: Optional[datetime] = None,
    ) -> bool:
        telemetry = snapshot.get("telemetry")
        if snapshot.get("received") is not True or not isinstance(telemetry, dict):
            return False
        source = telemetry.get("source")
        instance = telemetry.get("source_instance_id")
        sequence = telemetry.get("sequence_number")
        if (
            not isinstance(source, str)
            or not source
            or not isinstance(instance, str)
            or not instance
            or isinstance(sequence, bool)
            or not isinstance(sequence, int)
        ):
            return False

        captured = recorded_at
        if captured is None:
            captured = parse_timestamp(snapshot.get("received_at"))
        captured = _utc(captured)
        captured_text = isoformat_utc(captured)
        serialized = json.dumps(
            copy.deepcopy(snapshot), ensure_ascii=False, separators=(",", ":")
        )
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO telemetry_samples
                    (recorded_at, recorded_epoch, source, source_instance_id,
                     sequence_number, snapshot_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    captured_text,
                    captured.timestamp(),
                    source,
                    instance,
                    sequence,
                    serialized,
                ),
            )
            return cursor.rowcount == 1

    def _watermark(self) -> int:
        row = self._connection.execute(
            "SELECT value FROM memory_metadata WHERE key = 'last_window_sample_id'"
        ).fetchone()
        return int(row["value"]) if row else 0

    @staticmethod
    def _row_to_sample(row: sqlite3.Row) -> StoredSample:
        return StoredSample(
            id=int(row["id"]),
            recorded_at=row["recorded_at"],
            recorded_epoch=float(row["recorded_epoch"]),
            snapshot=json.loads(row["snapshot_json"]),
        )

    def next_ready_window(
        self, *, now: Optional[datetime] = None
    ) -> Optional[PendingTelemetryWindow]:
        current = _utc(now)
        with self._lock:
            watermark = self._watermark()
            first = self._connection.execute(
                """
                SELECT id, recorded_at, recorded_epoch, snapshot_json
                FROM telemetry_samples WHERE id > ? ORDER BY id LIMIT 1
                """,
                (watermark,),
            ).fetchone()
            if first is None:
                return None
            window_end_epoch = float(first["recorded_epoch"]) + self.window_seconds
            if (
                current.timestamp()
                < window_end_epoch + self.window_close_grace_seconds
            ):
                return None
            candidate_rows = self._connection.execute(
                """
                SELECT id, recorded_at, recorded_epoch, snapshot_json
                FROM telemetry_samples
                WHERE id > ?
                ORDER BY id
                """,
                (watermark,),
            ).fetchall()
            # Advance the durable watermark only through one contiguous ID
            # prefix. If the wall clock moves backwards, filtering all rows by
            # timestamp could otherwise include a newer ID after an excluded
            # one and permanently skip that excluded sample.
            rows = []
            for row in candidate_rows:
                if float(row["recorded_epoch"]) >= window_end_epoch:
                    break
                rows.append(row)
            if not rows:
                return None
            samples = tuple(self._row_to_sample(row) for row in rows)
            return PendingTelemetryWindow(
                first_sample_id=samples[0].id,
                last_sample_id=samples[-1].id,
                window_start=samples[0].recorded_at,
                window_end=isoformat_utc(
                    datetime.fromtimestamp(window_end_epoch, tz=timezone.utc)
                ),
                samples=samples,
            )

    def complete_window(
        self,
        window: PendingTelemetryWindow,
        *,
        advisory_digest: str,
        digest_source: str,
        llm_error: Optional[str] = None,
        completed_at: Optional[datetime] = None,
    ) -> None:
        if digest_source not in {"deepseek", "deterministic-fallback"}:
            raise ValueError("invalid telemetry digest source")
        if not isinstance(advisory_digest, str) or not advisory_digest.strip():
            raise ValueError("telemetry advisory digest must be non-empty")
        bounded_digest = _bounded_advisory_text(
            advisory_digest,
            config.TELEMETRY_MEMORY_MAX_DIGEST_CHARS,
        )
        if not bounded_digest:
            raise ValueError("telemetry advisory digest has no displayable text")
        completed_text = isoformat_utc(_utc(completed_at))
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT OR IGNORE INTO telemetry_windows
                    (first_sample_id, last_sample_id, last_sample_epoch,
                     window_start, window_end, received_sample_count,
                     advisory_digest, digest_source, llm_error, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    window.first_sample_id,
                    window.last_sample_id,
                    max(sample.recorded_epoch for sample in window.samples),
                    window.window_start,
                    window.window_end,
                    len(window.samples),
                    bounded_digest,
                    digest_source,
                    llm_error,
                    completed_text,
                ),
            )
            existing = self._watermark()
            self._connection.execute(
                """
                INSERT INTO memory_metadata(key, value)
                VALUES ('last_window_sample_id', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(max(existing, window.last_sample_id)),),
            )
        self.prune(now=completed_at)

    def record_turn(
        self,
        thread_id: Optional[str],
        *,
        question: str,
        advisory_explanation: Optional[str],
        explanation_source: str,
        asked_at: Optional[datetime] = None,
    ) -> None:
        resolved = normalize_thread_id(thread_id)
        if not isinstance(question, str):
            raise ValueError("conversation question must be a string")
        advisory = (
            _bounded_advisory_text(
                advisory_explanation,
                config.RAPP_MEMORY_MAX_ADVISORY_CHARS,
            )
            if explanation_source == "deepseek"
            and isinstance(advisory_explanation, str)
            else None
        )
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO conversation_turns
                    (thread_id, asked_at, question, advisory_explanation,
                     explanation_source)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    resolved,
                    isoformat_utc(_utc(asked_at)),
                    question[: config.DEEPSEEK_MAX_QUESTION_CHARS],
                    advisory,
                    explanation_source,
                ),
            )
            self._connection.execute(
                """
                DELETE FROM conversation_turns
                WHERE thread_id = ? AND id NOT IN (
                    SELECT id FROM conversation_turns
                    WHERE thread_id = ? ORDER BY id DESC LIMIT ?
                )
                """,
                (
                    resolved,
                    resolved,
                    self.conversation_retained_turns,
                ),
            )

    def build_query_context(
        self,
        thread_id: Optional[str],
        *,
        through_snapshot: Optional[dict[str, Any]] = None,
        context_windows: Optional[int] = None,
    ) -> dict[str, Any]:
        resolved = normalize_thread_id(thread_id)
        context_windows = (
            config.TELEMETRY_MEMORY_CONTEXT_WINDOWS
            if context_windows is None
            else context_windows
        )
        if context_windows < 0:
            raise ValueError("telemetry context window count cannot be negative")
        cutoff = datetime.max.replace(tzinfo=timezone.utc).timestamp()
        if isinstance(through_snapshot, dict):
            received_at = parse_timestamp(through_snapshot.get("received_at"))
            if received_at is not None:
                cutoff = received_at.timestamp()

        with self._lock:
            turn_count = int(
                self._connection.execute(
                    """
                    SELECT COUNT(*) AS count FROM conversation_turns
                    WHERE thread_id = ?
                    """,
                    (resolved,),
                ).fetchone()["count"]
            )
            turn_rows = self._connection.execute(
                """
                SELECT asked_at, question, advisory_explanation
                FROM conversation_turns
                WHERE thread_id = ? ORDER BY id DESC LIMIT ?
                """,
                (resolved, self.conversation_context_turns),
            ).fetchall()
            window_count = int(
                self._connection.execute(
                    """
                    SELECT COUNT(*) AS count FROM telemetry_windows
                    WHERE last_sample_epoch <= ?
                    """,
                    (cutoff,),
                ).fetchone()["count"]
            )
            window_rows = self._connection.execute(
                """
                SELECT last_sample_id, window_start, window_end,
                       received_sample_count, advisory_digest, digest_source
                FROM telemetry_windows
                WHERE last_sample_epoch <= ?
                ORDER BY id DESC LIMIT ?
                """,
                (cutoff, context_windows),
            ).fetchall()
            chronological_windows = list(reversed(window_rows))
            watermark_row = self._connection.execute(
                """
                SELECT last_sample_id FROM telemetry_windows
                WHERE last_sample_epoch <= ?
                ORDER BY id DESC LIMIT 1
                """,
                (cutoff,),
            ).fetchone()
            # The raw-data watermark is independent of how many completed
            # digests the caller elects to display. In particular, requesting
            # zero digests must not replay already-completed samples as partial.
            watermark = int(watermark_row["last_sample_id"]) if watermark_row else 0
            partial_rows = self._connection.execute(
                """
                SELECT id, recorded_at, recorded_epoch, snapshot_json
                FROM telemetry_samples
                WHERE id > ? AND recorded_epoch <= ?
                ORDER BY id
                """,
                (watermark, cutoff),
            ).fetchall()

        conversation = [
            {
                "asked_at": row["asked_at"],
                "question": row["question"],
                "advisory_explanation": row["advisory_explanation"],
            }
            for row in reversed(turn_rows)
        ]
        completed_windows = [
            {
                "window_start": row["window_start"],
                "window_end": row["window_end"],
                "received_sample_count": int(row["received_sample_count"]),
                "advisory_digest": row["advisory_digest"],
                "digest_source": row["digest_source"],
            }
            for row in chronological_windows
        ]
        partial_samples = tuple(
            self._row_to_sample(row) for row in partial_rows
        )
        partial_window = None
        if partial_samples:
            partial_window = build_window_payload(
                partial_samples,
                window_start=partial_samples[0].recorded_at,
                window_end=partial_samples[-1].recorded_at,
                window_kind="partial",
                max_chars=max(1000, config.RAPP_MEMORY_MAX_CONTEXT_CHARS // 2),
            )

        context = {
            "memory_schema_version": MEMORY_SCHEMA_VERSION,
            "thread_id": resolved,
            "conversation": conversation,
            "completed_windows": completed_windows,
            "partial_window": partial_window,
            "context_limits": {
                "max_context_chars": config.RAPP_MEMORY_MAX_CONTEXT_CHARS,
                "conversation_turns_omitted": max(
                    0, turn_count - len(conversation)
                ),
                "completed_windows_omitted": max(
                    0, window_count - len(completed_windows)
                ),
            },
        }
        def serialized_size() -> int:
            return len(
                json.dumps(context, ensure_ascii=False, separators=(",", ":"))
            )

        while (
            serialized_size() > config.RAPP_MEMORY_MAX_CONTEXT_CHARS
            and context["conversation"]
        ):
            context["conversation"].pop(0)
            context["context_limits"]["conversation_turns_omitted"] += 1
        while (
            serialized_size() > config.RAPP_MEMORY_MAX_CONTEXT_CHARS
            and context["completed_windows"]
        ):
            context["completed_windows"].pop(0)
            context["context_limits"]["completed_windows_omitted"] += 1
        if (
            serialized_size() > config.RAPP_MEMORY_MAX_CONTEXT_CHARS
            and partial_samples
        ):
            context["partial_window"] = build_window_payload(
                partial_samples,
                window_start=partial_samples[0].recorded_at,
                window_end=partial_samples[-1].recorded_at,
                window_kind="partial",
                max_samples=1,
                max_chars=1000,
            )
        return context

    def read_recent_window_facts(
        self,
        *,
        limit: int,
        max_samples: int,
        max_bytes: int,
    ) -> list[dict[str, Any]]:
        """Return bounded deterministic facts without exposing SQL or digests."""
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10:
            raise ValueError("recent telemetry window limit must be between 1 and 10")
        if (
            isinstance(max_samples, bool)
            or not isinstance(max_samples, int)
            or not 1 <= max_samples <= 10_000
        ):
            raise ValueError("recent telemetry window sample cap must be between 1 and 10000")
        if (
            isinstance(max_bytes, bool)
            or not isinstance(max_bytes, int)
            or not 65_536 <= max_bytes <= 50_000_000
        ):
            raise ValueError(
                "recent telemetry window byte cap must be between 65536 and 50000000"
            )
        with self._lock:
            window_rows = self._connection.execute(
                """
                SELECT first_sample_id, last_sample_id, window_start, window_end,
                       received_sample_count, digest_source, llm_error
                FROM telemetry_windows
                ORDER BY id DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
            resolved: list[
                tuple[sqlite3.Row, list[sqlite3.Row], Optional[str]]
            ] = []
            remaining_samples = max_samples
            remaining_bytes = max_bytes
            for window_row in window_rows:
                expected_count = int(window_row["received_sample_count"])
                if expected_count < 1:
                    resolved.append(
                        (window_row, [], "raw_samples_pruned_or_incomplete")
                    )
                    continue
                if expected_count > remaining_samples:
                    resolved.append(
                        (window_row, [], "sample_budget_exceeded")
                    )
                    continue
                storage = self._connection.execute(
                    """
                    SELECT COUNT(*) AS count,
                           COALESCE(
                               SUM(LENGTH(CAST(snapshot_json AS BLOB))), 0
                           ) AS stored_bytes,
                           MIN(id) AS first_id,
                           MAX(id) AS last_id
                    FROM telemetry_samples
                    WHERE id BETWEEN ? AND ?
                    """,
                    (
                        int(window_row["first_sample_id"]),
                        int(window_row["last_sample_id"]),
                    ),
                ).fetchone()
                raw_rows_complete = (
                    int(storage["count"]) == expected_count
                    and storage["first_id"] is not None
                    and storage["last_id"] is not None
                    and int(storage["first_id"])
                    == int(window_row["first_sample_id"])
                    and int(storage["last_id"])
                    == int(window_row["last_sample_id"])
                )
                if not raw_rows_complete:
                    resolved.append(
                        (window_row, [], "raw_samples_pruned_or_incomplete")
                    )
                    continue
                stored_bytes = int(storage["stored_bytes"])
                if stored_bytes > remaining_bytes:
                    resolved.append((window_row, [], "byte_budget_exceeded"))
                    continue
                sample_rows = self._connection.execute(
                    """
                    SELECT id, recorded_at, recorded_epoch, snapshot_json
                    FROM telemetry_samples
                    WHERE id BETWEEN ? AND ?
                    ORDER BY id
                    LIMIT ?
                    """,
                    (
                        int(window_row["first_sample_id"]),
                        int(window_row["last_sample_id"]),
                        expected_count,
                    ),
                ).fetchall()
                remaining_samples -= len(sample_rows)
                remaining_bytes -= stored_bytes
                resolved.append((window_row, sample_rows, None))

        windows: list[dict[str, Any]] = []
        for window_row, sample_rows, unavailable_reason in resolved:
            expected_count = int(window_row["received_sample_count"])
            raw_complete = (
                len(sample_rows) == expected_count
                and bool(sample_rows)
                and int(sample_rows[0]["id"])
                == int(window_row["first_sample_id"])
                and int(sample_rows[-1]["id"])
                == int(window_row["last_sample_id"])
            )
            facts = None
            if raw_complete:
                samples = tuple(self._row_to_sample(row) for row in sample_rows)
                facts = _window_facts(
                    samples,
                    max_metric_series=16,
                    max_sequence_streams=8,
                    max_missing_names=32,
                )
            elif unavailable_reason is None:
                unavailable_reason = "raw_samples_pruned_or_incomplete"
            windows.append(
                {
                    "window_start": window_row["window_start"],
                    "window_end": window_row["window_end"],
                    "received_sample_count": expected_count,
                    "digest_source": window_row["digest_source"],
                    "digest_error_present": window_row["llm_error"] is not None,
                    "facts_available": facts is not None,
                    "facts_unavailable_reason": unavailable_reason,
                    "all_sample_facts": facts,
                }
            )
        return windows

    def read_sequence_advancement(
        self,
        *,
        lookback_seconds: float,
        max_samples: int,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        """Summarize received sequence progress per source instance."""
        if (
            isinstance(lookback_seconds, bool)
            or not isinstance(lookback_seconds, (int, float))
            or not math.isfinite(lookback_seconds)
            or not 1 <= lookback_seconds <= 3600
        ):
            raise ValueError("sequence lookback must be between 1 and 3600 seconds")
        if (
            isinstance(max_samples, bool)
            or not isinstance(max_samples, int)
            or not 2 <= max_samples <= 5000
        ):
            raise ValueError("sequence sample cap must be between 2 and 5000")
        observed_now = _utc(now)
        cutoff = observed_now.timestamp() - float(lookback_seconds)
        with self._lock:
            stream_limit = min(32, max_samples)
            known_stream_count = int(
                self._connection.execute(
                    """
                    SELECT COUNT(*) AS count FROM (
                        SELECT 1 FROM telemetry_samples
                        GROUP BY source, source_instance_id
                    )
                    """
                ).fetchone()["count"]
            )
            stream_rows = self._connection.execute(
                """
                SELECT source, source_instance_id, MAX(id) AS last_id
                FROM telemetry_samples
                GROUP BY source, source_instance_id
                ORDER BY last_id DESC
                LIMIT ?
                """,
                (stream_limit,),
            ).fetchall()
            stream_cap_reached = known_stream_count > stream_limit
            selected_streams = stream_rows
            stream_count = len(selected_streams)
            base_budget = max_samples // stream_count if stream_count else 0
            extra_budget = max_samples % stream_count if stream_count else 0
            resolved_streams: list[
                tuple[sqlite3.Row, list[sqlite3.Row], sqlite3.Row, bool]
            ] = []
            for index, stream in enumerate(selected_streams):
                budget = base_budget + (1 if index < extra_budget else 0)
                recent_descending = self._connection.execute(
                    """
                    SELECT recorded_at, source, source_instance_id,
                           sequence_number
                    FROM telemetry_samples
                    WHERE source = ? AND source_instance_id = ?
                      AND recorded_epoch >= ?
                    ORDER BY id DESC LIMIT ?
                    """,
                    (
                        stream["source"],
                        stream["source_instance_id"],
                        cutoff,
                        budget + 1,
                    ),
                ).fetchall()
                latest = self._connection.execute(
                    """
                    SELECT recorded_at, sequence_number
                    FROM telemetry_samples WHERE id = ?
                    """,
                    (int(stream["last_id"]),),
                ).fetchone()
                if latest is None:
                    continue
                per_stream_cap = len(recent_descending) > budget
                recent = list(reversed(recent_descending[:budget]))
                resolved_streams.append(
                    (stream, recent, latest, per_stream_cap)
                )

        streams = []
        sample_count = 0
        sample_cap_reached = False
        for stream, recent_rows, latest, per_stream_cap in resolved_streams:
            source = str(stream["source"])
            instance = str(stream["source_instance_id"])
            sequences = [int(row["sequence_number"]) for row in recent_rows]
            sample_count += len(sequences)
            sample_cap_reached = sample_cap_reached or per_stream_cap
            gaps = 0
            non_increasing = 0
            for previous, current in zip(sequences, sequences[1:]):
                if current > previous:
                    gaps += max(0, current - previous - 1)
                else:
                    non_increasing += 1
            if not sequences:
                advancement_state = "no_recent_observations"
            elif len(sequences) < 2:
                advancement_state = "insufficient_samples"
            elif non_increasing:
                advancement_state = "non_monotonic"
            else:
                advancement_state = "advancing"
            latest_at = parse_timestamp(latest["recorded_at"])
            latest_age = None
            if latest_at is not None:
                latest_age = max(
                    0.0,
                    (observed_now - latest_at).total_seconds(),
                )
            streams.append(
                {
                    "source": _bounded_label(source, 64),
                    "source_instance_id": _bounded_label(instance, 64),
                    "observations": len(sequences),
                    "first_sequence": sequences[0] if sequences else None,
                    "last_sequence": sequences[-1] if sequences else None,
                    "observed_sequence_gaps": gaps,
                    "non_increasing_steps": non_increasing,
                    "advancement_state": advancement_state,
                    "first_received_at": (
                        recent_rows[0]["recorded_at"] if recent_rows else None
                    ),
                    "last_received_at": (
                        recent_rows[-1]["recorded_at"] if recent_rows else None
                    ),
                    "last_known_sequence": int(latest["sequence_number"]),
                    "last_known_received_at": latest["recorded_at"],
                    "last_known_age_seconds": latest_age,
                }
            )
        return {
            "lookback_seconds": float(lookback_seconds),
            "sample_count": sample_count,
            "sample_cap_reached": sample_cap_reached,
            "retained_stream_count": known_stream_count,
            "returned_stream_count": len(streams),
            "stream_cap_reached": stream_cap_reached,
            "duplicates_observable": False,
            "streams": streams,
        }

    def prune(self, *, now: Optional[datetime] = None) -> None:
        cutoff = _utc(now).timestamp() - self.raw_retention_hours * 3600.0
        with self._lock, self._connection:
            watermark = self._watermark()
            self._connection.execute(
                """
                DELETE FROM telemetry_samples
                WHERE id <= ? AND recorded_epoch < ?
                """,
                (watermark, cutoff),
            )
            self._connection.execute(
                """
                DELETE FROM telemetry_windows WHERE id NOT IN (
                    SELECT id FROM telemetry_windows
                    ORDER BY id DESC LIMIT ?
                )
                """,
                (self.retained_windows,),
            )

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "telemetry_samples": int(
                    self._connection.execute(
                        "SELECT COUNT(*) AS count FROM telemetry_samples"
                    ).fetchone()["count"]
                ),
                "telemetry_windows": int(
                    self._connection.execute(
                        "SELECT COUNT(*) AS count FROM telemetry_windows"
                    ).fetchone()["count"]
                ),
                "conversation_turns": int(
                    self._connection.execute(
                        "SELECT COUNT(*) AS count FROM conversation_turns"
                    ).fetchone()["count"]
                ),
                "last_window_sample_id": self._watermark(),
            }

    def reset(self) -> None:
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM telemetry_samples")
            self._connection.execute("DELETE FROM telemetry_windows")
            self._connection.execute("DELETE FROM conversation_turns")
            self._connection.execute("DELETE FROM memory_metadata")


class NullRAppMemory:
    """No-op implementation used for tests and explicitly disabled memory."""

    def __init__(self, initialization_error: Optional[str] = None) -> None:
        self.initialization_error = initialization_error

    def record_snapshot(self, *_args: Any, **_kwargs: Any) -> bool:
        return False

    def record_turn(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def build_query_context(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def read_recent_window_facts(self, **_kwargs: Any) -> list[dict[str, Any]]:
        raise RuntimeError("durable telemetry memory is unavailable")

    def read_sequence_advancement(self, **_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("durable telemetry memory is unavailable")


class SnapshotMemoryWriter:
    """Bounded non-blocking handoff from the Flask callback to SQLite."""

    def __init__(
        self,
        memory: RAppMemory,
        *,
        queue_size: Optional[int] = None,
    ) -> None:
        queue_size = (
            config.TELEMETRY_MEMORY_INGEST_QUEUE_SIZE
            if queue_size is None
            else queue_size
        )
        if queue_size < 1:
            raise ValueError("telemetry memory ingest queue must have capacity")
        self.memory = memory
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(queue_size)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lifecycle_lock = threading.Lock()
        self._stats_lock = threading.Lock()
        self._accepted = 0
        self._dropped = 0
        self._failures = 0

    def start(self) -> None:
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="telemetry-memory-writer",
                daemon=True,
            )
            self._thread.start()

    def submit(self, snapshot: dict[str, Any]) -> bool:
        with self._lifecycle_lock:
            if (
                self._thread is None
                or not self._thread.is_alive()
                or self._stop.is_set()
            ):
                with self._stats_lock:
                    self._dropped += 1
                return False
            try:
                self._queue.put_nowait(copy.deepcopy(snapshot))
                return True
            except queue.Full:
                with self._stats_lock:
                    self._dropped += 1
                    dropped = self._dropped
                logger.warning(
                    "Telemetry memory ingest queue is full; dropped sample count=%d",
                    dropped,
                )
                return False

    def stats(self) -> dict[str, int]:
        with self._stats_lock:
            return {
                "accepted": self._accepted,
                "dropped": self._dropped,
                "failures": self._failures,
                "queued": self._queue.qsize(),
            }

    def stop(self, timeout: Optional[float] = None) -> bool:
        with self._lifecycle_lock:
            self._stop.set()
            thread = self._thread
        if thread is not None:
            thread.join(timeout=5.0 if timeout is None else timeout)
        return thread is None or not thread.is_alive()

    def _run(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            try:
                snapshot = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                stored = self.memory.record_snapshot(snapshot)
                with self._stats_lock:
                    if stored:
                        self._accepted += 1
            except Exception:
                with self._stats_lock:
                    self._failures += 1
                logger.exception("Could not persist queued telemetry sample")
            finally:
                self._queue.task_done()


class TelemetryMemoryWorker:
    """Flush completed telemetry windows without blocking the HTTP receiver."""

    def __init__(
        self,
        memory: RAppMemory,
        summarizer: Optional[WindowSummarizer],
        *,
        poll_seconds: Optional[float] = None,
    ) -> None:
        self.memory = memory
        self.summarizer = summarizer
        self.poll_seconds = (
            config.TELEMETRY_MEMORY_POLL_S
            if poll_seconds is None
            else poll_seconds
        )
        if not math.isfinite(self.poll_seconds) or self.poll_seconds <= 0:
            raise ValueError("telemetry memory poll interval must be positive")
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_prune_at: Optional[datetime] = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="telemetry-memory",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: Optional[float] = None) -> bool:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(
                timeout=(config.DEEPSEEK_TIMEOUT_S + 1.0)
                if timeout is None
                else timeout
            )
        return thread is None or not thread.is_alive()

    def flush_ready(
        self,
        *,
        now: Optional[datetime] = None,
        max_windows: int = 1,
    ) -> int:
        processed = 0
        for _ in range(max_windows):
            window = self.memory.next_ready_window(now=now)
            if window is None:
                break
            payload = build_window_payload(
                window.samples,
                window_start=window.window_start,
                window_end=window.window_end,
                window_kind="completed",
            )
            digest_source = "deepseek"
            llm_error: Optional[str] = None
            try:
                if self.summarizer is None:
                    raise RuntimeError("telemetry window summarizer is disabled")
                advisory_digest = self.summarizer.summarize_window(payload)
                if not isinstance(advisory_digest, str) or not advisory_digest.strip():
                    raise RuntimeError("telemetry window summarizer returned no text")
            except Exception as exc:  # Model availability cannot stop telemetry.
                digest_source = "deterministic-fallback"
                llm_error = f"{type(exc).__name__}: {str(exc)[:400]}"
                advisory_digest = deterministic_window_digest(window.samples)
                logger.info(
                    "Telemetry window used deterministic memory fallback (%s)",
                    type(exc).__name__,
                )
            self.memory.complete_window(
                window,
                advisory_digest=advisory_digest,
                digest_source=digest_source,
                llm_error=llm_error,
                completed_at=now,
            )
            processed += 1
        return processed

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.flush_ready()
                now = _utc()
                if (
                    self._last_prune_at is None
                    or (now - self._last_prune_at).total_seconds() >= 60
                ):
                    self.memory.prune(now=now)
                    self._last_prune_at = now
            except Exception:
                logger.exception("Telemetry memory worker iteration failed")
            self._stop.wait(self.poll_seconds)


_runtime_memory_lock = threading.Lock()
_runtime_memory: Optional[RAppMemory | NullRAppMemory] = None


def get_runtime_memory() -> RAppMemory | NullRAppMemory:
    global _runtime_memory
    with _runtime_memory_lock:
        if _runtime_memory is None:
            if not config.RAPP_MEMORY_ENABLED:
                _runtime_memory = NullRAppMemory()
            else:
                try:
                    _runtime_memory = RAppMemory(config.RAPP_MEMORY_DB_PATH)
                except Exception as exc:
                    logger.exception(
                        "Durable rApp memory could not start; continuing without memory"
                    )
                    _runtime_memory = NullRAppMemory(
                        f"{type(exc).__name__}: {str(exc)[:400]}"
                    )
        return _runtime_memory


def close_runtime_memory() -> None:
    """Close the process-wide SQLite connection during application shutdown."""
    global _runtime_memory
    with _runtime_memory_lock:
        active = _runtime_memory
        _runtime_memory = None
    if isinstance(active, RAppMemory):
        active.close()
