from __future__ import annotations

import copy
import json
import stat
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import consumer
import config
import deepseek_client
import graph
import memory as memory_module
from memory import (
    RAppMemory,
    SnapshotMemoryWriter,
    StoredSample,
    TelemetryMemoryWorker,
    build_window_payload,
    deterministic_window_digest,
    validate_memory_context,
    validate_window_payload,
)


BASE = datetime(2026, 8, 7, 12, 0, 0, tzinfo=timezone.utc)


def _text(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _telemetry(sequence: int, at: datetime, **extra: Any) -> dict[str, Any]:
    telemetry = {
        "schema_version": "1.0",
        "source": "health-xapp",
        "source_instance_id": "test-instance",
        "sequence_number": sequence,
        "observed_at": _text(at),
        "ric_connected": True,
        "e2_nodes_connected": 1,
        "kpm_indications_received": sequence + 1,
        "last_kpm_indication_at": _text(at),
        "metrics": {
            "RRU.PrbTotDl": {"value": sequence, "unit": "PRB", "valid": True}
        },
        "missing_metrics": [],
        "incomplete": False,
    }
    telemetry.update(extra)
    return telemetry


def _snapshot(sequence: int, at: datetime, **extra: Any) -> dict[str, Any]:
    return {
        "received": True,
        "telemetry": _telemetry(sequence, at, **extra),
        "received_at": _text(at),
        "producer_pushed_at": _text(at),
        "info_job_identity": "health-agent-rapp-job-1",
    }


def _all_keys(value: Any) -> set[str]:
    if isinstance(value, dict):
        keys = set(value)
        for child in value.values():
            keys.update(_all_keys(child))
        return keys
    if isinstance(value, list):
        keys: set[str] = set()
        for child in value:
            keys.update(_all_keys(child))
        return keys
    return set()


class FakeResponse:
    status_code = 200

    def __init__(self, text: str) -> None:
        self.text = text

    def json(self) -> dict[str, Any]:
        return {
            "choices": [
                {
                    "message": {"content": self.text},
                    "finish_reason": "stop",
                }
            ]
        }


def test_sixty_second_window_and_query_time_partial_context(tmp_path) -> None:
    store = RAppMemory(
        str(tmp_path / "memory.sqlite3"),
        window_seconds=60,
        window_close_grace_seconds=0,
        raw_retention_hours=24,
    )
    for offset in (0, 1, 59, 60, 84):
        at = BASE + timedelta(seconds=offset)
        assert store.record_snapshot(_snapshot(offset, at), recorded_at=at)

    assert store.next_ready_window(now=BASE + timedelta(seconds=59.999)) is None
    ready = store.next_ready_window(now=BASE + timedelta(seconds=60))
    assert ready is not None
    assert [sample.snapshot["telemetry"]["sequence_number"] for sample in ready.samples] == [
        0,
        1,
        59,
    ]

    captured: list[dict[str, Any]] = []

    class Summarizer:
        def summarize_window(self, window: dict[str, Any]) -> str:
            captured.append(window)
            return "Three observations were received in this time window."

    worker = TelemetryMemoryWorker(store, Summarizer(), poll_seconds=0.01)
    assert worker.flush_ready(now=BASE + timedelta(seconds=60)) == 1
    assert len(captured) == 1
    assert captured[0]["window_kind"] == "completed"
    assert captured[0]["received_sample_count"] == 3

    context = store.build_query_context(
        "thread-a",
        through_snapshot=_snapshot(84, BASE + timedelta(seconds=84)),
    )
    assert validate_memory_context(context) == []
    assert context["completed_windows"][0]["received_sample_count"] == 3
    assert context["completed_windows"][0]["digest_source"] == "deepseek"
    partial = context["partial_window"]
    assert partial["received_sample_count"] == 2
    assert [sample["sequence_number"] for sample in partial["samples"]] == [60, 84]
    no_digest_context = store.build_query_context(
        "thread-a",
        through_snapshot=_snapshot(84, BASE + timedelta(seconds=84)),
        context_windows=0,
    )
    assert no_digest_context["completed_windows"] == []
    assert [
        sample["sequence_number"]
        for sample in no_digest_context["partial_window"]["samples"]
    ] == [60, 84]
    store.close()


def test_window_watermark_never_skips_out_of_order_receive_times(tmp_path) -> None:
    store = RAppMemory(
        str(tmp_path / "memory.sqlite3"),
        window_seconds=60,
        window_close_grace_seconds=0,
    )
    # IDs are authoritative for no-gap processing. Simulate a wall-clock jump
    # forward followed by a correction backwards.
    for sequence, offset in ((1, 0), (2, 120), (3, 10)):
        at = BASE + timedelta(seconds=offset)
        assert store.record_snapshot(
            _snapshot(sequence, at), recorded_at=at
        )

    first = store.next_ready_window(now=BASE + timedelta(seconds=60))
    assert first is not None
    assert [item.snapshot["telemetry"]["sequence_number"] for item in first.samples] == [1]
    store.complete_window(
        first,
        advisory_digest="First contiguous window.",
        digest_source="deepseek",
        completed_at=BASE + timedelta(seconds=60),
    )

    second = store.next_ready_window(now=BASE + timedelta(seconds=180))
    assert second is not None
    assert [item.snapshot["telemetry"]["sequence_number"] for item in second.samples] == [
        2,
        3,
    ]
    store.close()


def test_window_close_grace_allows_async_writer_to_catch_up(tmp_path) -> None:
    store = RAppMemory(
        str(tmp_path / "memory.sqlite3"),
        window_seconds=60,
        window_close_grace_seconds=1,
    )
    store.record_snapshot(_snapshot(1, BASE), recorded_at=BASE)
    assert store.next_ready_window(now=BASE + timedelta(seconds=60)) is None
    assert store.next_ready_window(now=BASE + timedelta(seconds=61)) is not None
    store.close()


def test_conversation_memory_is_durable_bounded_and_thread_isolated(tmp_path) -> None:
    database = str(tmp_path / "memory.sqlite3")
    store = RAppMemory(
        database,
        conversation_context_turns=2,
        conversation_retained_turns=3,
    )
    for index in range(4):
        store.record_turn(
            "alpha",
            question=f"question-{index}",
            advisory_explanation=f"advisory-{index}",
            explanation_source="deepseek",
            asked_at=BASE + timedelta(seconds=index),
        )
    store.record_turn(
        "beta",
        question="private-to-beta",
        advisory_explanation="beta-advisory",
        explanation_source="deepseek",
        asked_at=BASE,
    )
    store.close()

    reopened = RAppMemory(
        database,
        conversation_context_turns=2,
        conversation_retained_turns=3,
    )
    alpha = reopened.build_query_context("alpha")
    beta = reopened.build_query_context("beta")
    assert [turn["question"] for turn in alpha["conversation"]] == [
        "question-2",
        "question-3",
    ]
    assert [turn["question"] for turn in beta["conversation"]] == [
        "private-to-beta"
    ]
    assert "private-to-beta" not in json.dumps(alpha)
    reopened.close()


def test_combined_memory_context_is_bounded_with_explicit_omissions(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(config, "RAPP_MEMORY_MAX_CONTEXT_CHARS", 1500)
    store = RAppMemory(
        str(tmp_path / "memory.sqlite3"),
        conversation_context_turns=8,
        conversation_retained_turns=8,
    )
    for index in range(5):
        store.record_turn(
            "alpha",
            question=f"question-{index}-" + ("q" * 400),
            advisory_explanation="a" * 500,
            explanation_source="deepseek",
            asked_at=BASE + timedelta(seconds=index),
        )
    for index in range(3):
        at = BASE + timedelta(seconds=index)
        store.record_snapshot(_snapshot(index, at), recorded_at=at)

    context = store.build_query_context(
        "alpha", through_snapshot=_snapshot(2, BASE + timedelta(seconds=2))
    )
    assert len(json.dumps(context, separators=(",", ":"))) <= 1500
    assert context["context_limits"]["conversation_turns_omitted"] > 0
    assert validate_memory_context(context) == []
    store.close()


def test_deterministic_fallback_turn_is_not_resent_to_the_model(tmp_path) -> None:
    store = RAppMemory(str(tmp_path / "memory.sqlite3"))
    store.record_turn(
        "alpha",
        question="What failed?",
        advisory_explanation=(
            "Deterministic evidence notes containing a local check result."
        ),
        explanation_source="deterministic-fallback",
        asked_at=BASE,
    )

    context = store.build_query_context("alpha")
    assert context["conversation"] == [
        {
            "asked_at": _text(BASE),
            "question": "What failed?",
            "advisory_explanation": None,
        }
    ]
    store.close()


def test_graph_uses_real_thread_memory_without_breaking_old_explainer_api(
    tmp_path, monkeypatch
) -> None:
    store = RAppMemory(str(tmp_path / "memory.sqlite3"))
    store.record_turn(
        "alpha",
        question="What did the PRB value do?",
        advisory_explanation="It was reported as 5 PRB.",
        explanation_source="deepseek",
        asked_at=BASE,
    )
    snapshot = _snapshot(9, BASE + timedelta(seconds=9))
    monkeypatch.setattr(graph, "get_health_snapshot", lambda: snapshot)
    seen_contexts: list[dict[str, Any]] = []

    class ContextExplainer:
        def explain(self, _question: str, _evidence: dict[str, Any]) -> str:
            raise AssertionError("the contextual method should be selected")

        def explain_with_context(
            self,
            _question: str,
            _evidence: dict[str, Any],
            context: dict[str, Any],
        ) -> str:
            seen_contexts.append(context)
            return "The prior advisory observation is available as context."

    compiled = graph.build_graph(
        explainer=ContextExplainer(),
        memory_store=store,
    )
    answer = graph.ask_structured(
        "And what about now?",
        thread_id="alpha",
        compiled_graph=compiled,
    )

    assert seen_contexts[0]["conversation"][0]["question"] == (
        "What did the PRB value do?"
    )
    assert answer["memory"]["thread_id"] == "alpha"
    assert answer["memory"]["conversation_turns_available"] == 1
    assert answer["memory"]["context_used_for_explanation"] is True
    after = store.build_query_context("alpha")
    assert [turn["question"] for turn in after["conversation"]][-1] == (
        "And what about now?"
    )
    assert answer["summary"]["status"] in {
        "healthy",
        "degraded",
        "unhealthy",
        "unknown",
    }
    store.close()


def test_original_two_argument_explainer_remains_compatible(tmp_path, monkeypatch) -> None:
    store = RAppMemory(str(tmp_path / "memory.sqlite3"))
    monkeypatch.setattr(graph, "get_health_snapshot", lambda: _snapshot(1, BASE))

    class LegacyExplainer:
        def explain(self, question: str, evidence: dict[str, Any]) -> str:
            assert question == "Current state?"
            assert evidence["telemetry"]["sequence_number"] == 1
            return "Current evidence was received."

    answer = graph.ask_structured(
        "Current state?",
        thread_id="legacy",
        compiled_graph=graph.build_graph(
            explainer=LegacyExplainer(), memory_store=store
        ),
    )
    assert answer["explanation"] == "Current evidence was received."
    store.close()


def test_contextual_deepseek_request_contains_only_advisory_memory(tmp_path) -> None:
    store = RAppMemory(str(tmp_path / "memory.sqlite3"))
    store.record_turn(
        "alpha",
        question="Earlier question",
        advisory_explanation="Earlier advisory explanation",
        explanation_source="deepseek",
        asked_at=BASE,
    )
    context = store.build_query_context("alpha")
    evidence = deepseek_client.build_evidence_payload(
        _snapshot(2, BASE), collected_at=BASE + timedelta(seconds=1)
    )
    captured: dict[str, Any] = {}

    def post(_url: str, **kwargs: Any) -> FakeResponse:
        captured.update(kwargs)
        return FakeResponse("Context-aware evidence explanation.")

    client = deepseek_client.DeepSeekExplainer(
        api_key="secret", http_post=post
    )
    assert client.explain_with_context("Follow up", evidence, context) == (
        "Context-aware evidence explanation."
    )
    model_input = json.loads(captured["json"]["messages"][1]["content"])
    assert set(model_input) == {"question", "evidence", "memory_context"}
    serialized = json.dumps(model_input)
    assert "Earlier question" in serialized
    assert not {"health_report", "overall_status", "checks"} & _all_keys(
        model_input
    )
    assert "secret" not in json.dumps(captured["json"])
    store.close()


def test_contextual_client_fits_total_request_by_dropping_oldest_turns(
    tmp_path,
) -> None:
    store = RAppMemory(
        str(tmp_path / "memory.sqlite3"),
        conversation_context_turns=8,
        conversation_retained_turns=8,
    )
    for index in range(4):
        store.record_turn(
            "alpha",
            question=f"old-{index}-" + ("q" * 500),
            advisory_explanation="a" * 800,
            explanation_source="deepseek",
            asked_at=BASE + timedelta(seconds=index),
        )
    context = store.build_query_context("alpha")
    evidence = deepseek_client.build_evidence_payload(
        _snapshot(2, BASE), collected_at=BASE + timedelta(seconds=1)
    )
    empty_context = copy.deepcopy(context)
    empty_context["context_limits"]["conversation_turns_omitted"] += len(
        empty_context["conversation"]
    )
    empty_context["conversation"] = []
    baseline = len(
        json.dumps(
            {
                "question": "Follow up",
                "evidence": evidence,
                "memory_context": empty_context,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    captured: dict[str, Any] = {}

    def post(_url: str, **kwargs: Any) -> FakeResponse:
        captured.update(kwargs)
        return FakeResponse("Bounded contextual response.")

    client = deepseek_client.DeepSeekExplainer(
        api_key="secret",
        max_input_chars=baseline + 20,
        http_post=post,
    )
    client.explain_with_context("Follow up", evidence, context)
    sent = json.loads(captured["json"]["messages"][1]["content"])
    assert sent["memory_context"]["conversation"] == []
    assert sent["memory_context"]["context_limits"][
        "conversation_turns_omitted"
    ] >= 4
    assert len(captured["json"]["messages"][1]["content"]) <= baseline + 20
    store.close()


def test_graph_retries_latest_snapshot_when_memory_cannot_fit(monkeypatch) -> None:
    monkeypatch.setattr(graph, "get_health_snapshot", lambda: _snapshot(1, BASE))
    calls: list[str] = []

    class ContextFailureExplainer:
        def explain_with_context(self, *_args: Any) -> str:
            calls.append("context")
            raise deepseek_client.MemoryContextUnavailable("test context overflow")

        def explain(self, _question: str, _evidence: dict[str, Any]) -> str:
            calls.append("latest")
            return "Latest snapshot explanation."

    class Memory:
        def build_query_context(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {
                "memory_schema_version": "1.0",
                "thread_id": "alpha",
                "conversation": [],
                "completed_windows": [],
                "partial_window": None,
                "context_limits": {
                    "max_context_chars": 30000,
                    "conversation_turns_omitted": 0,
                    "completed_windows_omitted": 0,
                },
            }

        def record_turn(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    answer = graph.ask_structured(
        "Question",
        thread_id="alpha",
        compiled_graph=graph.build_graph(
            explainer=ContextFailureExplainer(), memory_store=Memory()
        ),
    )
    assert calls == ["context", "latest"]
    assert answer["explanation"] == "Latest snapshot explanation."
    assert answer["memory"]["context_used_for_explanation"] is False


def test_memory_load_failure_is_visible_but_does_not_break_answer(monkeypatch) -> None:
    monkeypatch.setattr(graph, "get_health_snapshot", lambda: _snapshot(1, BASE))

    class FailingMemory:
        def build_query_context(self, *_args: Any, **_kwargs: Any) -> None:
            raise OSError("test database outage")

        def record_turn(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    class Explainer:
        def explain(self, _question: str, _evidence: dict[str, Any]) -> str:
            return "Latest snapshot remains available."

    answer = graph.ask_structured(
        "Question",
        thread_id="alpha",
        compiled_graph=graph.build_graph(
            explainer=Explainer(), memory_store=FailingMemory()
        ),
    )
    assert answer["explanation"] == "Latest snapshot remains available."
    assert "OSError" in answer["memory"]["load_error"]
    assert answer["memory"]["context_used_for_explanation"] is False


def test_window_payload_compaction_is_explicit_and_bounded() -> None:
    samples = []
    for index in range(3):
        at = BASE + timedelta(seconds=index)
        snapshot = _snapshot(
            index,
            at,
            metrics={
                "metric-" + ("x" * 5000): {
                    "value": index,
                    "unit": "unit",
                    "valid": True,
                }
            },
        )
        samples.append(
            StoredSample(index + 1, _text(at), at.timestamp(), snapshot)
        )
    payload = build_window_payload(
        samples,
        window_start=_text(BASE),
        window_end=_text(BASE + timedelta(seconds=60)),
        window_kind="completed",
        max_chars=1000,
    )

    assert payload["compaction"] == "metadata_only_for_size"
    assert payload["received_sample_count"] == 3
    assert payload["included_sample_count"] == 0
    assert payload["omitted_sample_count"] == 3
    assert validate_window_payload(payload) == []
    assert len(json.dumps(payload)) < 1000


def test_all_sample_facts_keep_source_instances_units_and_invalid_values_separate() -> None:
    definitions = [
        (
            1,
            "instance-a",
            10,
            {"value": 1, "unit": "PRB", "valid": True},
        ),
        (
            3,
            "instance-a",
            12,
            {"value": 999, "unit": "PRB", "valid": False},
        ),
        (
            4,
            "instance-a",
            13,
            {"value": 5, "unit": "%", "valid": True},
        ),
        (
            0,
            "instance-b",
            1,
            {"value": 7, "unit": "PRB", "valid": True},
        ),
    ]
    samples = []
    for index, (sequence, instance, counter, measurement) in enumerate(definitions):
        at = BASE + timedelta(seconds=index)
        snapshot = _snapshot(
            sequence,
            at,
            source_instance_id=instance,
            kpm_indications_received=counter,
            metrics={"RRU.PrbTotDl": measurement},
        )
        if index == 3:
            snapshot["producer_pushed_at"] = {"unexpected": "nested-data"}
        samples.append(
            StoredSample(index + 1, _text(at), at.timestamp(), snapshot)
        )

    payload = build_window_payload(
        samples,
        window_start=_text(BASE),
        window_end=_text(BASE + timedelta(seconds=60)),
        window_kind="completed",
        max_samples=1,
    )
    assert validate_window_payload(payload) == []
    assert payload["samples"][0]["producer_pushed_at"] is None
    facts = payload["all_sample_facts"]
    assert len(facts["sequence_streams"]) == 2
    instance_a_sequence = next(
        item
        for item in facts["sequence_streams"]
        if item["source_instance_id"] == "instance-a"
    )
    assert instance_a_sequence["observed_sequence_gaps"] == 1
    assert len(facts["kpm_indication_counters"]) == 2
    assert len(facts["metric_series"]) == 3
    instance_a_prb = next(
        item
        for item in facts["metric_series"]
        if item["source_instance_id"] == "instance-a"
        and item["unit"] == "PRB"
    )
    assert instance_a_prb["minimum"] == 1
    assert instance_a_prb["maximum"] == 1
    assert instance_a_prb["invalid_observation_count"] == 1
    assert facts["unit_conflict_metric_names"]


def test_window_validator_rejects_nested_values_and_contradictory_metadata() -> None:
    sample = StoredSample(1, _text(BASE), BASE.timestamp(), _snapshot(1, BASE))
    valid = build_window_payload(
        [sample],
        window_start=_text(BASE),
        window_end=_text(BASE + timedelta(seconds=60)),
        window_kind="completed",
    )

    nested = copy.deepcopy(valid)
    nested["samples"][0]["metrics"]["RRU.PrbTotDl"]["value"] = {
        "overall_status": "healthy"
    }
    assert validate_window_payload(nested)

    def fail_network(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("invalid window must not reach the network")

    client = deepseek_client.DeepSeekExplainer(
        api_key="secret",
        http_post=fail_network,
    )
    with pytest.raises(deepseek_client.ExplanationUnavailable):
        client.summarize_window(nested)

    reversed_window = copy.deepcopy(valid)
    reversed_window["window_start"], reversed_window["window_end"] = (
        reversed_window["window_end"],
        reversed_window["window_start"],
    )
    assert validate_window_payload(reversed_window)

    contradictory = copy.deepcopy(valid)
    contradictory["compaction"] = "metadata_only_for_size"
    assert validate_window_payload(contradictory)

    reversed_range = copy.deepcopy(valid)
    series = reversed_range["all_sample_facts"]["metric_series"][0]
    series["minimum"] = 10
    series["maximum"] = 1
    assert validate_window_payload(reversed_range)


def test_deterministic_digest_bounds_legal_long_units() -> None:
    at = BASE
    snapshot = _snapshot(
        1,
        at,
        metrics={
            "vendor.metric": {
                "value": 1,
                "unit": "u" * 100_000,
                "valid": True,
            }
        },
    )
    sample = StoredSample(1, _text(at), at.timestamp(), snapshot)
    digest = deterministic_window_digest((sample,))
    assert len(digest) <= config.TELEMETRY_MEMORY_MAX_DIGEST_CHARS
    assert "u" * 1000 not in digest


def test_snapshot_writer_is_nonblocking_bounded_and_drains() -> None:
    started = threading.Event()
    release = threading.Event()
    recorded: list[int] = []

    class BlockingStore:
        def record_snapshot(self, snapshot: dict[str, Any]) -> bool:
            started.set()
            assert release.wait(2)
            recorded.append(snapshot["telemetry"]["sequence_number"])
            return True

    writer = SnapshotMemoryWriter(BlockingStore(), queue_size=1)  # type: ignore[arg-type]
    writer.start()
    assert writer.submit(_snapshot(1, BASE)) is True
    assert started.wait(1)
    assert writer.submit(_snapshot(2, BASE + timedelta(seconds=1))) is True
    assert writer.submit(_snapshot(3, BASE + timedelta(seconds=2))) is False
    release.set()
    assert writer.stop(timeout=2) is True
    assert recorded == [1, 2]
    assert writer.stats() == {
        "accepted": 2,
        "dropped": 1,
        "failures": 0,
        "queued": 0,
    }
    assert writer.submit(_snapshot(4, BASE + timedelta(seconds=3))) is False


def test_sqlite_permissions_pruning_and_watermark_survive_restart(tmp_path) -> None:
    database = tmp_path / "private" / "memory.sqlite3"
    store = RAppMemory(
        str(database),
        window_seconds=60,
        window_close_grace_seconds=0,
        raw_retention_hours=1,
        retained_windows=2,
    )
    assert stat.S_IMODE(database.stat().st_mode) == 0o600
    assert stat.S_IMODE(database.parent.stat().st_mode) == 0o700

    for index, offset in enumerate((0, 120, 240)):
        at = BASE + timedelta(seconds=offset)
        store.record_snapshot(_snapshot(index, at), recorded_at=at)
        window = store.next_ready_window(now=at + timedelta(seconds=60))
        assert window is not None
        store.complete_window(
            window,
            advisory_digest=f"window-{index}",
            digest_source="deepseek",
            completed_at=at + timedelta(seconds=60),
        )
    unprocessed_at = BASE + timedelta(seconds=300)
    store.record_snapshot(_snapshot(99, unprocessed_at), recorded_at=unprocessed_at)
    watermark = store.stats()["last_window_sample_id"]
    store.prune(now=BASE + timedelta(hours=2))
    assert store.stats()["telemetry_samples"] == 1
    assert store.stats()["telemetry_windows"] == 2
    store.close()

    reopened = RAppMemory(
        str(database),
        window_seconds=60,
        window_close_grace_seconds=0,
        raw_retention_hours=1,
        retained_windows=2,
    )
    assert reopened.stats()["last_window_sample_id"] == watermark
    assert reopened.stats()["telemetry_samples"] == 1
    reopened.close()


def test_runtime_memory_initialization_fails_open(monkeypatch) -> None:
    memory_module.close_runtime_memory()

    class BrokenMemory:
        def __init__(self, _path: str) -> None:
            raise OSError("read-only filesystem")

    monkeypatch.setattr(config, "RAPP_MEMORY_ENABLED", True)
    monkeypatch.setattr(memory_module, "RAppMemory", BrokenMemory)
    active = memory_module.get_runtime_memory()
    assert isinstance(active, memory_module.NullRAppMemory)
    assert "OSError" in active.initialization_error
    memory_module.close_runtime_memory()


def test_default_ask_uses_stable_durable_cli_thread(tmp_path, monkeypatch) -> None:
    database = tmp_path / "runtime.sqlite3"
    graph.reset_runtime_graph()
    memory_module.close_runtime_memory()
    monkeypatch.setattr(config, "RAPP_MEMORY_ENABLED", True)
    monkeypatch.setattr(config, "RAPP_MEMORY_DB_PATH", str(database))
    monkeypatch.setattr(config, "DEEPSEEK_API_KEY", "")
    monkeypatch.setattr(graph, "get_health_snapshot", lambda: _snapshot(1, BASE))

    try:
        first = graph.ask("First question")
        second = graph.ask_structured("Second question")
        assert isinstance(first, str)
        assert second["memory"]["thread_id"] == config.RAPP_DEFAULT_THREAD_ID
        assert second["memory"]["conversation_turns_available"] == 1
    finally:
        graph.reset_runtime_graph()
        memory_module.close_runtime_memory()

    reopened = RAppMemory(str(database))
    context = reopened.build_query_context(config.RAPP_DEFAULT_THREAD_ID)
    assert [turn["question"] for turn in context["conversation"]] == [
        "First question",
        "Second question",
    ]
    reopened.close()


def test_consumer_appends_only_accepted_r1_deliveries() -> None:
    accepted: list[dict[str, Any]] = []
    consumer.reset_state()
    consumer.set_snapshot_sink(accepted.append)
    client = consumer.receiver_app.test_client()
    envelope = {
        "info_job_identity": "health-agent-rapp-job-1",
        "producer_pushed_at": _text(BASE),
        "telemetry": _telemetry(1, BASE),
    }

    try:
        response = client.post("/consumer/health-data", json=envelope)
        assert response.status_code == 200
        assert len(accepted) == 1
        assert consumer.get_health_snapshot()["telemetry"]["sequence_number"] == 1

        wrong_job = dict(envelope)
        wrong_job["info_job_identity"] = "another-job"
        response = client.post("/consumer/health-data", json=wrong_job)
        assert response.status_code == 409
        assert len(accepted) == 1
    finally:
        consumer.set_snapshot_sink(None)
        consumer.reset_state()


def test_consumer_rejects_oversized_callback_body() -> None:
    previous = consumer.receiver_app.config["MAX_CONTENT_LENGTH"]
    consumer.receiver_app.config["MAX_CONTENT_LENGTH"] = 100
    try:
        response = consumer.receiver_app.test_client().post(
            "/consumer/health-data",
            json={
                "info_job_identity": "health-agent-rapp-job-1",
                "telemetry": _telemetry(1, BASE),
            },
        )
        assert response.status_code == 413
    finally:
        consumer.receiver_app.config["MAX_CONTENT_LENGTH"] = previous


def test_window_summary_call_is_internal_structured_and_verdict_free() -> None:
    at = BASE
    sample = StoredSample(1, _text(at), at.timestamp(), _snapshot(1, at))
    window = build_window_payload(
        [sample],
        window_start=_text(at),
        window_end=_text(at + timedelta(seconds=60)),
        window_kind="completed",
    )
    captured: dict[str, Any] = {}

    def post(_url: str, **kwargs: Any) -> FakeResponse:
        captured.update(kwargs)
        return FakeResponse("One structured observation was received.")

    client = deepseek_client.DeepSeekExplainer(
        api_key="secret", http_post=post
    )
    assert client.summarize_window(window) == (
        "One structured observation was received."
    )
    request = captured["json"]
    model_input = json.loads(request["messages"][1]["content"])
    assert model_input == {"telemetry_window": window}
    serialized = json.dumps(model_input)
    assert "overall_status" not in serialized
    assert "health_report" not in serialized
    assert request["max_tokens"] == 300
    assert "not shown directly" in request["messages"][0]["content"]
