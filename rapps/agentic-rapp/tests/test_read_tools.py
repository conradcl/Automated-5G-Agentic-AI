from __future__ import annotations

import copy
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import config
import deepseek_client
import graph
from deepseek_client import DeepSeekExplainer, ExplanationUnavailable
from memory import NullRAppMemory, RAppMemory
from read_tools import (
    GET_DME_JOB_STATUS,
    GET_EVIDENCE_HISTORY,
    GET_EVIDENCE_PIPELINE_STATUS,
    GET_OAI_CONTAINER_STATUS,
    GET_RECENT_TELEMETRY_WINDOWS,
    GET_SEQUENCE_ADVANCEMENT,
    PING_UE_PATH,
    READ_TOOL_NAMES,
    ReadToolConfigurationError,
    ReadToolRegistry,
    read_tool_catalog,
    validate_read_tool_catalog,
    validate_read_tool_result,
)


BASE = datetime(2026, 8, 9, 12, 0, 0, tzinfo=timezone.utc)


def _text(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _telemetry(
    sequence: int,
    at: datetime,
    *,
    source: str = "health-xapp",
    instance: str = "test-instance",
    metric_name: str = "RRU.PrbTotDl",
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "source": source,
        "source_instance_id": instance,
        "sequence_number": sequence,
        "observed_at": _text(at),
        "ric_connected": True,
        "e2_nodes_connected": 1,
        "kpm_indications_received": sequence,
        "last_kpm_indication_at": _text(at),
        "metrics": {
            metric_name: {"value": float(sequence), "unit": "PRB", "valid": True}
        },
        "missing_metrics": [],
        "incomplete": False,
    }


def _snapshot(
    sequence: int,
    at: datetime,
    *,
    source: str = "health-xapp",
    instance: str = "test-instance",
) -> dict[str, Any]:
    return {
        "received": True,
        "telemetry": _telemetry(
            sequence,
            at,
            source=source,
            instance=instance,
        ),
        "received_at": _text(at),
        "producer_pushed_at": _text(at),
        "info_job_identity": config.JOB_ID,
    }


def _evidence() -> dict[str, Any]:
    return deepseek_client.build_evidence_payload(
        _snapshot(7, BASE),
        collected_at=BASE + timedelta(seconds=1),
    )


class JsonResponse:
    def __init__(self, body: Any, status_code: int = 200) -> None:
        self.status_code = status_code
        self._content = json.dumps(body).encode("utf-8")
        self.closed = False

    def iter_content(self, chunk_size: int = 4096):
        for offset in range(0, len(self._content), chunk_size):
            yield self._content[offset : offset + chunk_size]

    def close(self) -> None:
        self.closed = True


class FakeSession:
    def __init__(self, responses: dict[str, JsonResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, **kwargs: Any) -> JsonResponse:
        self.calls.append((url, kwargs))
        return self.responses[url]


@dataclass
class Completed:
    stdout: str = ""
    stderr: str = ""
    returncode: int = 0


def _registry(
    *,
    session: Any | None = None,
    memory_store: Any | None = None,
    command_runner: Any = subprocess.run,
) -> ReadToolRegistry:
    return ReadToolRegistry(
        memory_store or NullRAppMemory(),
        dme_base_url="http://dme.test",
        evidence_api_base_url="http://evidence.test",
        http_session=session or FakeSession({}),
        command_runner=command_runner,
        now=lambda: BASE,
    )


def test_catalog_is_fixed_zero_argument_and_unknown_tools_are_rejected() -> None:
    catalog = read_tool_catalog()
    assert tuple(item["name"] for item in catalog) == READ_TOOL_NAMES
    assert validate_read_tool_catalog(catalog) == []
    assert all(
        item["parameters"]
        == {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        }
        for item in catalog
    )

    with pytest.raises(ValueError, match="allowlisted"):
        _registry().execute("http_get")


def test_dme_tool_uses_only_fixed_get_paths_and_normalizes_job_status() -> None:
    callback = f"{config.CONSUMER_BASE_URL}{config.CONSUMER_CALLBACK_PATH}"
    session = FakeSession(
        {
            "http://dme.test/status": JsonResponse(
                {
                    "status": "living",
                    "no_of_types": 1,
                    "no_of_producers": 1,
                    "no_of_jobs": 1,
                }
            ),
            f"http://dme.test/data-consumer/v1/info-jobs/{config.JOB_ID}": JsonResponse(
                {
                    "info_type_id": config.INFO_TYPE_ID,
                    "job_result_uri": callback,
                    "job_owner": config.JOB_OWNER,
                    "job_definition": {"callback_url": callback},
                }
            ),
            f"http://dme.test/data-consumer/v1/info-jobs/{config.JOB_ID}/status": JsonResponse(
                {"info_job_status": "ENABLED", "producers": ["producer-1"]}
            ),
        }
    )

    result = _registry(session=session).execute(GET_DME_JOB_STATUS)

    assert result["ok"] is True
    assert validate_read_tool_result(result) == []
    assert result["data"]["job_registered"] is True
    assert result["data"]["job_operational_state"] == "ENABLED"
    assert result["data"]["callback_matches_config"] is True
    assert [call[0] for call in session.calls] == list(session.responses)
    assert all(call[1]["allow_redirects"] is False for call in session.calls)
    assert all(call[1]["stream"] is True for call in session.calls)
    assert all(call[1]["headers"] == {"Accept": "application/json"} for call in session.calls)


def test_dme_malformed_success_and_unsafe_job_id_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = FakeSession(
        {
            "http://dme.test/status": JsonResponse(
                {
                    "status": "living",
                    "no_of_types": 1,
                    "no_of_producers": 1,
                    "no_of_jobs": 1,
                }
            ),
            f"http://dme.test/data-consumer/v1/info-jobs/{config.JOB_ID}": JsonResponse(
                ["not", "a", "job"]
            ),
        }
    )
    result = _registry(session=session).execute(GET_DME_JOB_STATUS)
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_response"

    monkeypatch.setattr(config, "JOB_ID", "../unexpected")
    with pytest.raises(ReadToolConfigurationError, match="path segment"):
        _registry()


def test_evidence_readiness_uses_http_status_and_history_is_normalized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "RAPP_READ_TOOL_HISTORY_LIMIT", 2)
    long_metric_name = "M" * 5000
    valid = _telemetry(9, BASE, metric_name=long_metric_name)
    session = FakeSession(
        {
            "http://evidence.test/healthz": JsonResponse({"status": "living"}),
            "http://evidence.test/readyz": JsonResponse(
                {
                    "status": "not-ready",
                    "repository": True,
                    "dme_registered": False,
                },
                status_code=503,
            ),
            "http://evidence.test/producer/health-check": JsonResponse(
                {
                    "status": "living",
                    "repository": True,
                    "evidence_received": True,
                    "active_jobs": 1,
                }
            ),
            "http://evidence.test/v1/evidence/history?limit=2": JsonResponse(
                {"count": 2, "events": [valid, {"bad": "event"}]}
            ),
        }
    )
    registry = _registry(session=session)

    status = registry.execute(GET_EVIDENCE_PIPELINE_STATUS)
    history = registry.execute(GET_EVIDENCE_HISTORY)

    assert status["ok"] is True
    assert status["data"]["ready"] is False
    assert status["data"]["repository_ready"] is True
    assert history["ok"] is True
    assert history["data"]["returned_count"] == 1
    assert history["data"]["invalid_events_omitted"] == 1
    event = history["data"]["events"][0]
    assert len(event["metric_names"][0]) == 128
    assert "metrics" not in event
    assert "value" not in json.dumps(event)
    assert validate_read_tool_result(history) == []


def test_contradictory_evidence_readiness_is_rejected() -> None:
    session = FakeSession(
        {
            "http://evidence.test/healthz": JsonResponse({"status": "living"}),
            "http://evidence.test/readyz": JsonResponse(
                {
                    "status": "ready",
                    "repository": True,
                    "dme_registered": True,
                },
                status_code=503,
            ),
            "http://evidence.test/producer/health-check": JsonResponse(
                {
                    "status": "living",
                    "repository": True,
                    "evidence_received": True,
                    "active_jobs": 1,
                }
            ),
        }
    )
    result = _registry(session=session).execute(GET_EVIDENCE_PIPELINE_STATUS)
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_response"


def test_recent_window_facts_enforce_a_database_sample_budget(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = RAppMemory(
        str(tmp_path / "memory.sqlite3"),
        window_seconds=60,
        window_close_grace_seconds=0,
    )
    for offset in (0, 1):
        at = BASE + timedelta(seconds=offset)
        assert store.record_snapshot(_snapshot(offset, at), recorded_at=at)
    first = store.next_ready_window(now=BASE + timedelta(seconds=60))
    assert first is not None
    store.complete_window(
        first,
        advisory_digest="first",
        digest_source="deepseek",
        completed_at=BASE + timedelta(seconds=60),
    )
    for offset in (60, 61):
        at = BASE + timedelta(seconds=offset)
        assert store.record_snapshot(_snapshot(offset, at), recorded_at=at)
    second = store.next_ready_window(now=BASE + timedelta(seconds=120))
    assert second is not None
    store.complete_window(
        second,
        advisory_digest="second",
        digest_source="deepseek",
        completed_at=BASE + timedelta(seconds=120),
    )

    windows = store.read_recent_window_facts(
        limit=2,
        max_samples=2,
        max_bytes=4_000_000,
    )

    assert windows[0]["facts_available"] is True
    assert windows[0]["facts_unavailable_reason"] is None
    assert windows[1]["facts_available"] is False
    assert windows[1]["facts_unavailable_reason"] == "sample_budget_exceeded"
    monkeypatch.setattr(config, "RAPP_READ_TOOL_WINDOW_LIMIT", 2)
    monkeypatch.setattr(config, "RAPP_READ_TOOL_WINDOW_MAX_SAMPLES", 2)
    tool_result = _registry(memory_store=store).execute(
        GET_RECENT_TELEMETRY_WINDOWS
    )
    assert tool_result["ok"] is True
    assert validate_read_tool_result(tool_result) == []
    store.close()


def test_sequence_tool_reports_advancing_and_stalled_streams(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = RAppMemory(str(tmp_path / "memory.sqlite3"))
    old = BASE - timedelta(seconds=120)
    assert store.record_snapshot(
        _snapshot(5, old, instance="stalled"), recorded_at=old
    )
    for sequence, seconds_ago in ((1, 10), (3, 1)):
        at = BASE - timedelta(seconds=seconds_ago)
        assert store.record_snapshot(
            _snapshot(sequence, at, instance="advancing"), recorded_at=at
        )

    result = store.read_sequence_advancement(
        lookback_seconds=60,
        max_samples=10,
        now=BASE,
    )
    streams = {item["source_instance_id"]: item for item in result["streams"]}

    assert streams["advancing"]["advancement_state"] == "advancing"
    assert streams["advancing"]["observed_sequence_gaps"] == 1
    assert streams["stalled"]["advancement_state"] == "no_recent_observations"
    assert streams["stalled"]["last_known_sequence"] == 5
    assert streams["stalled"]["last_known_age_seconds"] == 120
    assert result["returned_stream_count"] == 2
    monkeypatch.setattr(config, "RAPP_READ_TOOL_SEQUENCE_LOOKBACK_S", 60.0)
    monkeypatch.setattr(config, "RAPP_READ_TOOL_SEQUENCE_MAX_SAMPLES", 10)
    tool_result = _registry(memory_store=store).execute(GET_SEQUENCE_ADVANCEMENT)
    assert tool_result["ok"] is True
    assert validate_read_tool_result(tool_result) == []
    store.close()


def test_ping_tool_uses_fixed_argv_and_returns_only_parsed_statistics() -> None:
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def runner(arguments: list[str], **kwargs: Any) -> Completed:
        calls.append((arguments, kwargs))
        return Completed(
            stdout=(
                "3 packets transmitted, 2 received, 33.3333% packet loss\n"
                "rtt min/avg/max/mdev = 10.000/12.000/14.000/2.000 ms\n"
            ),
            returncode=1,
        )

    result = _registry(command_runner=runner).execute(PING_UE_PATH)

    assert result["ok"] is True
    assert result["data"]["packet_loss_percent"] == pytest.approx(33.3333)
    assert result["data"]["rtt_avg_ms"] == 12.0
    arguments, kwargs = calls[0]
    assert arguments == [
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
    assert kwargs["shell"] is False
    assert kwargs["stdin"] is subprocess.DEVNULL
    assert kwargs["env"] == {"PATH": "/usr/bin:/bin", "LC_ALL": "C", "LANG": "C"}


def test_ping_tool_rejects_impossible_statistics() -> None:
    def runner(_arguments: list[str], **_kwargs: Any) -> Completed:
        return Completed(
            stdout="3 packets transmitted, 4 received, 0% packet loss\n",
            returncode=2,
        )

    result = _registry(command_runner=runner).execute(PING_UE_PATH)
    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_result"


def test_container_tool_inspects_only_the_configured_allowlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        config,
        "RAPP_READ_TOOL_OAI_CONTAINERS",
        ("oai-amf", "oai-upf"),
    )
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def runner(arguments: list[str], **kwargs: Any) -> Completed:
        calls.append((arguments, kwargs))
        if arguments[-1] == "oai-upf":
            return Completed(stderr="Error: No such object", returncode=1)
        return Completed(stdout="running\ttrue\thealthy\n", returncode=0)

    result = _registry(command_runner=runner).execute(GET_OAI_CONTAINER_STATUS)

    assert result["ok"] is True
    assert result["data"]["requested_count"] == 2
    assert result["data"]["running_count"] == 1
    assert result["data"]["containers"][1]["runtime_state"] == "missing"
    assert [call[0][-1] for call in calls] == ["oai-amf", "oai-upf"]
    assert all(call[0][:3] == ["/usr/bin/docker", "container", "inspect"] for call in calls)
    assert all(call[1]["shell"] is False for call in calls)


def test_result_validator_rejects_wrong_types_ranges_and_verdict_fields() -> None:
    result = _registry(
        command_runner=lambda *_args, **_kwargs: Completed(
            stdout="3 packets transmitted, 3 received, 0% packet loss\n"
            "rtt min/avg/max/mdev = 1/2/3/1 ms\n",
            returncode=0,
        )
    ).execute(PING_UE_PATH)
    assert result["ok"] is True

    wrong_type = copy.deepcopy(result)
    wrong_type["data"]["transmitted"] = "three"
    assert any("transmitted" in item or "packet counts" in item for item in validate_read_tool_result(wrong_type))

    impossible_loss = copy.deepcopy(result)
    impossible_loss["data"]["packet_loss_percent"] = 120.0
    assert validate_read_tool_result(impossible_loss)

    injected_verdict = copy.deepcopy(result)
    injected_verdict["data"]["nested"] = {"overall_status": "healthy"}
    assert validate_read_tool_result(injected_verdict)


class PlannerResponse:
    def __init__(self, body: dict[str, Any]) -> None:
        self.status_code = 200
        self._body = body

    def json(self) -> dict[str, Any]:
        return self._body


def test_deepseek_selects_only_one_fixed_empty_argument_tool() -> None:
    captured: dict[str, Any] = {}

    def post(_url: str, **kwargs: Any) -> PlannerResponse:
        captured.update(kwargs)
        return PlannerResponse(
            {
                "choices": [
                    {
                        "finish_reason": "tool_calls",
                        "message": {
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {
                                        "name": GET_DME_JOB_STATUS,
                                        "arguments": "{}",
                                    },
                                }
                            ],
                        },
                    }
                ]
            }
        )

    client = DeepSeekExplainer(api_key="test-key", http_post=post)
    decision = client.decide_read_tool(
        "Is the R1 job registered?",
        _evidence(),
        None,
        [],
        [read_tool_catalog()[0]],
    )

    assert decision == {"decision": "call_tool", "tool_name": GET_DME_JOB_STATUS}
    function = captured["json"]["tools"][0]["function"]
    assert function["parameters"] == {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    }
    assert captured["json"]["tool_choice"] == "auto"


def test_deepseek_rejects_model_supplied_tool_arguments() -> None:
    def post(_url: str, **_kwargs: Any) -> PlannerResponse:
        return PlannerResponse(
            {
                "choices": [
                    {
                        "finish_reason": "tool_calls",
                        "message": {
                            "tool_calls": [
                                {
                                    "function": {
                                        "name": GET_DME_JOB_STATUS,
                                        "arguments": '{"url":"http://attacker.test"}',
                                    }
                                }
                            ]
                        },
                    }
                ]
            }
        )

    with pytest.raises(ExplanationUnavailable, match="unsupported"):
        DeepSeekExplainer(api_key="test-key", http_post=post).decide_read_tool(
            "Check it",
            _evidence(),
            None,
            [],
            [read_tool_catalog()[0]],
        )


def _failed_dme_result() -> dict[str, Any]:
    return {
        "read_tool_schema_version": "1.0",
        "tool_name": GET_DME_JOB_STATUS,
        "scope": "r1_control_plane",
        "observed_at": _text(BASE),
        "ok": False,
        "data": {},
        "error": {
            "code": "unreachable",
            "message": "configured service could not be reached",
            "retryable": True,
        },
        "limitations": [
            "DME ENABLED means a compatible producer is registered, not that R1 delivery succeeded."
        ],
    }


def _failed_result(tool_name: str) -> dict[str, Any]:
    scopes = {
        GET_DME_JOB_STATUS: "r1_control_plane",
        GET_EVIDENCE_PIPELINE_STATUS: "evidence_pipeline",
        GET_EVIDENCE_HISTORY: "evidence_history",
    }
    result = _failed_dme_result()
    result["tool_name"] = tool_name
    result["scope"] = scopes[tool_name]
    result["limitations"] = ["Collection failed without changing the testbed."]
    return result


def test_graph_runs_model_selected_tool_then_evaluates_the_frozen_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    planner_inputs: list[dict[str, Any]] = []

    class Explainer:
        def decide_read_tool(
            self,
            question: str,
            evidence: dict[str, Any],
            memory_context: Any,
            results: list[dict[str, Any]],
            catalog: list[dict[str, Any]],
        ) -> dict[str, Any]:
            events.append("plan")
            planner_inputs.append(
                {
                    "question": question,
                    "evidence": copy.deepcopy(evidence),
                    "results": copy.deepcopy(results),
                    "catalog": copy.deepcopy(catalog),
                }
            )
            if not results:
                return {"decision": "call_tool", "tool_name": GET_DME_JOB_STATUS}
            return {
                "decision": "answer",
                "answer": "The configured DME endpoint was unavailable during this read.",
            }

        def explain(self, _question: str, _evidence: dict[str, Any]) -> str:
            raise AssertionError("the completed tool loop should not call explain")

    class Registry:
        def catalog(self) -> list[dict[str, Any]]:
            return [read_tool_catalog()[0]]

        def execute(self, tool_name: str) -> dict[str, Any]:
            events.append("tool")
            assert tool_name == GET_DME_JOB_STATUS
            return _failed_dme_result()

    report = {
        "assessment_scope": config.ASSESSMENT_SCOPE,
        "overall_status": "healthy",
        "generated_at": _text(BASE),
        "source_observed_at": _text(BASE),
        "source_instance_id": "test-instance",
        "sequence_number": 7,
        "checks": [],
    }

    def evaluate(snapshot: dict[str, Any], *, now: datetime) -> dict[str, Any]:
        events.append("evaluate")
        assert snapshot == _snapshot(7, BASE)
        assert now.tzinfo is not None
        return report

    monkeypatch.setattr(graph, "get_health_snapshot", lambda: _snapshot(7, BASE))
    monkeypatch.setattr(graph, "evaluate_health", evaluate)
    compiled = graph.build_graph(
        explainer=Explainer(),
        read_tool_registry=Registry(),
    )

    answer = graph.ask_structured("Is the R1 job working?", compiled_graph=compiled)

    assert events == ["plan", "tool", "plan", "evaluate"]
    assert planner_inputs[0]["results"] == []
    assert planner_inputs[1]["results"] == [_failed_dme_result()]
    assert "overall_status" not in json.dumps(planner_inputs)
    assert answer["health_report"] == report
    assert answer["read_tools"]["calls_made"] == 1
    assert answer["read_tools"]["tools_used"] == [GET_DME_JOB_STATUS]
    assert answer["read_tools"]["stop_reason"] == "model_answer"


def test_graph_catalog_failure_falls_back_without_crashing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Explainer:
        def decide_read_tool(self, *_args: Any) -> dict[str, Any]:
            raise AssertionError("planner must not receive a broken catalog")

        def explain(self, _question: str, _evidence: dict[str, Any]) -> str:
            return "Current evidence remained available."

    class BrokenRegistry:
        def catalog(self) -> list[dict[str, Any]]:
            raise RuntimeError("catalog down")

        def execute(self, _tool_name: str) -> dict[str, Any]:
            raise AssertionError("no tool may execute")

    monkeypatch.setattr(graph, "get_health_snapshot", lambda: _snapshot(7, BASE))
    answer = graph.ask_structured(
        "Current state?",
        compiled_graph=graph.build_graph(
            explainer=Explainer(),
            read_tool_registry=BrokenRegistry(),
        ),
    )

    assert answer["explanation"] == "Current evidence remained available."
    assert answer["read_tools"]["calls_made"] == 0
    assert answer["read_tools"]["stop_reason"] == "planner_unavailable"
    assert "catalog down" not in json.dumps(answer)


def test_graph_enforces_distinct_tool_and_total_call_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected: list[str] = []

    class Explainer:
        def decide_read_tool(
            self,
            _question: str,
            _evidence: dict[str, Any],
            _memory: Any,
            _results: list[dict[str, Any]],
            catalog: list[dict[str, Any]],
        ) -> dict[str, Any]:
            if catalog:
                return {"decision": "call_tool", "tool_name": catalog[0]["name"]}
            return {"decision": "answer", "answer": "The bounded reads are complete."}

        def explain(self, _question: str, _evidence: dict[str, Any]) -> str:
            raise AssertionError("the planner should return its final answer")

    class Registry:
        def catalog(self) -> list[dict[str, Any]]:
            return read_tool_catalog()[:3]

        def execute(self, tool_name: str) -> dict[str, Any]:
            selected.append(tool_name)
            return _failed_result(tool_name)

    monkeypatch.setattr(config, "RAPP_READ_TOOL_MAX_CALLS", 2)
    monkeypatch.setattr(graph, "get_health_snapshot", lambda: _snapshot(7, BASE))
    answer = graph.ask_structured(
        "Check the control-plane evidence",
        compiled_graph=graph.build_graph(
            explainer=Explainer(),
            read_tool_registry=Registry(),
        ),
    )

    assert selected == [GET_DME_JOB_STATUS, GET_EVIDENCE_PIPELINE_STATUS]
    assert len(set(selected)) == 2
    assert answer["read_tools"]["calls_made"] == 2
    assert answer["read_tools"]["stop_reason"] == "max_calls"


def test_legacy_explainer_response_shape_remains_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class LegacyExplainer:
        def explain(self, _question: str, _evidence: dict[str, Any]) -> str:
            return "Current evidence was explained."

    monkeypatch.setattr(graph, "get_health_snapshot", lambda: _snapshot(7, BASE))
    answer = graph.ask_structured(
        "Current state?",
        compiled_graph=graph.build_graph(explainer=LegacyExplainer()),
    )
    assert "read_tools" not in answer
