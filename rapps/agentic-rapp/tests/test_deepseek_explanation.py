from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pytest

import deepseek_client
import graph
from deepseek_client import DeepSeekExplainer, ExplanationUnavailable


FORBIDDEN_LLM_KEYS = {
    "checks",
    "health_report",
    "healthy",
    "overall_status",
    "report",
    "status",
    "summary",
    "verdict",
}


def _telemetry(**extra: Any) -> dict[str, Any]:
    telemetry = {
        "schema_version": "1.0",
        "source": "health-xapp",
        "source_instance_id": "test-instance",
        "sequence_number": 7,
        "observed_at": "2026-07-31T14:00:00Z",
        "ric_connected": True,
        "e2_nodes_connected": 1,
        "kpm_indications_received": 12,
        "last_kpm_indication_at": "2026-07-31T13:59:59Z",
        "metrics": {
            "RRU.PrbTotDl": {"value": 10, "unit": "PRB", "valid": True}
        },
        "missing_metrics": ["DRB.UEThpDl"],
        "incomplete": True,
    }
    telemetry.update(extra)
    return telemetry


def _snapshot(**telemetry_extra: Any) -> dict[str, Any]:
    return {
        "received": True,
        "telemetry": _telemetry(**telemetry_extra),
        "received_at": "2026-07-31T14:00:01Z",
        "producer_pushed_at": "2026-07-31T14:00:00.500000Z",
        "info_job_identity": "health-agent-rapp-job-1",
    }


def _evidence() -> dict[str, Any]:
    return deepseek_client.build_evidence_payload(
        _snapshot(),
        collected_at=datetime(2026, 7, 31, 14, 0, 2, tzinfo=timezone.utc),
    )


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
    def __init__(self, body: Any, status_code: int = 200) -> None:
        self._body = body
        self.status_code = status_code

    def json(self) -> Any:
        return self._body


def test_request_is_structured_evidence_without_a_verdict() -> None:
    captured: dict[str, Any] = {}

    def fake_post(url: str, **kwargs: Any) -> FakeResponse:
        captured.update({"url": url, **kwargs})
        return FakeResponse(
            {"choices": [{"message": {"content": "Observed evidence explained."}}]}
        )

    snapshot = _snapshot(
        overall_status="DO_NOT_SEND_THIS_VERDICT",
        checks=[{"status": "DO_NOT_SEND_THIS_CHECK"}],
    )
    evidence = deepseek_client.build_evidence_payload(
        snapshot,
        collected_at=datetime(2026, 7, 31, 14, 0, 2, tzinfo=timezone.utc),
    )
    client = DeepSeekExplainer(
        api_key="test-secret",
        base_url="https://api.deepseek.example/",
        model="deepseek-v4-flash",
        temperature=0.1,
        timeout_s=9,
        max_tokens=321,
        http_post=fake_post,
    )

    assert client.explain("What does the evidence show?", evidence) == (
        "Observed evidence explained."
    )

    request_body = captured["json"]
    user_message = json.loads(request_body["messages"][1]["content"])
    assert user_message == {
        "question": "What does the evidence show?",
        "evidence": evidence,
    }
    assert not (FORBIDDEN_LLM_KEYS & _all_keys(user_message))
    assert "DO_NOT_SEND_THIS_VERDICT" not in json.dumps(user_message)
    assert captured["url"] == "https://api.deepseek.example/chat/completions"
    assert captured["headers"]["Authorization"] == "Bearer test-secret"
    assert captured["timeout"] == 9
    assert request_body["model"] == "deepseek-v4-flash"
    assert request_body["thinking"] == {"type": "disabled"}
    assert request_body["temperature"] == 0.1
    assert request_body["max_tokens"] == 321
    system_prompt = request_body["messages"][0]["content"]
    assert "Zero throughput, volume, or delay is" in system_prompt
    assert "Never append a percent" in system_prompt
    assert "without an explicit supplied threshold" in system_prompt
    assert "evidence.calculated_timing_ms" in system_prompt
    assert "test-secret" not in json.dumps(request_body)


def test_evidence_contains_metric_semantics_and_calculated_timing() -> None:
    snapshot = _snapshot(
        metrics={
            "RRU.PrbTotUl": {"value": 115, "unit": "PRB", "valid": True},
            "DRB.PdcpSduVolumeDL": {
                "value": 0,
                "unit": "kb",
                "valid": True,
            },
            "DRB.RlcSduDelayDl": {
                "value": 0,
                "unit": "us",
                "valid": True,
            },
            "DRB.UEThpDl": {"value": 0, "unit": "kbps", "valid": True},
            "KPM.IndicationLatency": {
                "value": 72567,
                "unit": "us",
                "valid": True,
            },
        },
        missing_metrics=[],
        incomplete=False,
    )
    evidence = deepseek_client.build_evidence_payload(
        snapshot,
        collected_at=datetime(2026, 7, 31, 14, 0, 2, tzinfo=timezone.utc),
    )

    assert evidence["evidence_schema_version"] == "1.1"
    assert evidence["calculated_timing_ms"] == {
        "latest_kpm_to_source_snapshot": 1000.0,
        "latest_kpm_age_at_assessment": 3000.0,
        "source_snapshot_age_at_assessment": 2000.0,
        "source_snapshot_to_producer_push": 500.0,
        "source_snapshot_to_rapp_receive": 1000.0,
        "producer_push_to_rapp_receive": 500.0,
        "rapp_receive_to_assessment": 1000.0,
        "kpm_collect_to_xapp_receive": 72.567,
    }

    context = evidence["interpretation_context"]
    assert context["assessment_scope"] == (
        "ric_e2_kpm_telemetry_monitoring_path"
    )
    assert context["snapshot_mode"] == "single_latest_snapshot"
    assert context["traffic_demand_evidence_available"] is False
    assert context["configured_performance_thresholds"] == {}
    assert "latest_kpm_age_at_assessment" in context[
        "threshold_applications"
    ]["stale_after_seconds"]
    assert "not a metric or performance threshold" in context[
        "threshold_applications"
    ]["max_future_skew_seconds"]
    assert any(
        "clocks are synchronized" in limitation
        for limitation in context["evidence_limitations"]
    )

    semantics = evidence["metric_semantics"]
    prb = semantics["RRU.PrbTotUl"]
    assert prb["is_percentage"] is False
    assert prb["reported_unit"] == "PRB"
    assert prb["expected_unit"] == "PRB"
    assert prb["unit_matches_expected"] is True
    assert prb["capacity_denominator_available"] is False
    assert prb["configured_health_threshold"] is None
    for name in (
        "DRB.PdcpSduVolumeDL",
        "DRB.RlcSduDelayDl",
        "DRB.UEThpDl",
    ):
        assert (
            semantics[name][
                "zero_value_is_failure_without_independent_context"
            ]
            is False
        )
    latency = semantics["KPM.IndicationLatency"]
    assert latency["value_kind"] == "xapp_calculated_duration"
    assert latency["configured_health_threshold"] is None
    assert "not R1 delivery latency" in latency["meaning"]


def test_timestamp_math_is_supplied_in_milliseconds() -> None:
    snapshot = _snapshot(
        observed_at="2026-07-31T19:10:29.717622Z",
        last_kpm_indication_at="2026-07-31T19:10:29.716976Z",
    )
    snapshot["producer_pushed_at"] = "2026-07-31T19:10:29.798000Z"
    snapshot["received_at"] = "2026-07-31T19:10:29.798727Z"
    evidence = deepseek_client.build_evidence_payload(
        snapshot,
        collected_at=datetime(
            2026, 7, 31, 19, 10, 30, 91414, tzinfo=timezone.utc
        ),
    )

    timing = evidence["calculated_timing_ms"]
    assert timing["latest_kpm_to_source_snapshot"] == 0.646
    assert timing["source_snapshot_to_rapp_receive"] == 81.105
    assert timing["producer_push_to_rapp_receive"] == 0.727


def test_missing_timestamps_and_unknown_metrics_do_not_invite_guesses() -> None:
    snapshot = _snapshot(
        observed_at="2026-07-31T14:00:00Z",
        last_kpm_indication_at=None,
        metrics={
            "vendor.metric": {"value": 3, "unit": "widgets", "valid": True}
        },
    )
    snapshot["producer_pushed_at"] = "malformed"
    snapshot["received_at"] = None
    evidence = deepseek_client.build_evidence_payload(
        snapshot,
        collected_at=datetime(2026, 7, 31, 14, 0, 2, tzinfo=timezone.utc),
    )

    timing = evidence["calculated_timing_ms"]
    assert timing["latest_kpm_age_at_assessment"] is None
    assert timing["producer_push_to_rapp_receive"] is None
    assert timing["rapp_receive_to_assessment"] is None
    assert evidence["delivery"]["producer_pushed_at"] is None
    assert evidence["delivery"]["producer_pushed_at_validity"] == "invalid"
    assert evidence["delivery"]["received_at_validity"] == "missing"
    unknown = evidence["metric_semantics"]["vendor.metric"]
    assert unknown["is_percentage"] is None
    assert unknown["capacity_denominator_available"] is None
    assert unknown["configured_health_threshold"] is None
    assert (
        unknown["zero_value_is_failure_without_independent_context"] is None
    )
    assert unknown["value_kind"] == "unknown_raw_measurement"


@pytest.mark.parametrize(
    ("source", "unit", "expected_kind"),
    [
        ("health-xapp", "%", "unit_mismatch"),
        ("vendor-xapp", "PRB", "unknown_raw_measurement"),
    ],
)
def test_semantics_are_withheld_for_wrong_units_or_sources(
    source: str,
    unit: str,
    expected_kind: str,
) -> None:
    evidence = deepseek_client.build_evidence_payload(
        _snapshot(
            source=source,
            metrics={
                "RRU.PrbTotUl": {"value": 115, "unit": unit, "valid": True}
            },
        ),
        collected_at=datetime(2026, 7, 31, 14, 0, 2, tzinfo=timezone.utc),
    )

    semantics = evidence["metric_semantics"]["RRU.PrbTotUl"]
    assert semantics["value_kind"] == expected_kind
    assert semantics["is_percentage"] is None
    assert semantics["capacity_denominator_available"] is None
    assert semantics["zero_value_is_failure_without_independent_context"] is None


def test_future_timestamp_delta_is_preserved_instead_of_clamped() -> None:
    evidence = deepseek_client.build_evidence_payload(
        _snapshot(
            observed_at="2026-07-31T10:00:03-04:00",
            last_kpm_indication_at="2026-07-31T14:00:02Z",
        ),
        collected_at=datetime(2026, 7, 31, 14, 0, 2, tzinfo=timezone.utc),
    )

    assert evidence["calculated_timing_ms"][
        "source_snapshot_age_at_assessment"
    ] == -1000.0


def test_no_telemetry_produces_explicitly_empty_grounding() -> None:
    evidence = deepseek_client.build_evidence_payload(
        {
            "received": False,
            "telemetry": None,
            "received_at": None,
            "producer_pushed_at": None,
            "info_job_identity": None,
        },
        collected_at=datetime(2026, 7, 31, 14, 0, 2, tzinfo=timezone.utc),
    )

    assert evidence["telemetry"] is None
    assert evidence["metric_semantics"] == {}
    assert set(evidence["calculated_timing_ms"].values()) == {None}
    assert evidence["delivery"]["received_at_validity"] == "missing"
    assert evidence["delivery"]["producer_pushed_at_validity"] == "missing"


def test_missing_api_key_never_calls_the_network() -> None:
    def fail_if_called(*_: Any, **__: Any) -> None:
        raise AssertionError("network transport should not be called")

    client = DeepSeekExplainer(api_key="", http_post=fail_if_called)
    with pytest.raises(ExplanationUnavailable, match="DEEPSEEK_API_KEY"):
        client.explain("question", _evidence())


def test_invalid_temperature_never_calls_the_network() -> None:
    def fail_if_called(*_: Any, **__: Any) -> None:
        raise AssertionError("network transport should not be called")

    client = DeepSeekExplainer(
        api_key="test-secret",
        temperature=2.1,
        http_post=fail_if_called,
    )
    with pytest.raises(ExplanationUnavailable, match="temperature"):
        client.explain("question", _evidence())


def test_http_boundary_rejects_a_report_instead_of_evidence() -> None:
    def fail_if_called(*_: Any, **__: Any) -> None:
        raise AssertionError("network transport should not be called")

    client = DeepSeekExplainer(api_key="test-secret", http_post=fail_if_called)
    with pytest.raises(ExplanationUnavailable, match="allowed schema"):
        client.explain(
            "question",
            {"overall_status": "unhealthy", "checks": []},
        )


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"choices": []},
        {"choices": [{"message": {}}]},
        {"choices": [{"message": {"content": "   "}}]},
    ],
)
def test_malformed_or_empty_response_is_rejected(body: Any) -> None:
    client = DeepSeekExplainer(
        api_key="test-secret",
        http_post=lambda *_args, **_kwargs: FakeResponse(body),
    )
    with pytest.raises(ExplanationUnavailable):
        client.explain("question", _evidence())


@pytest.mark.parametrize(
    ("status_code", "message"),
    [
        (401, "rejected the API key"),
        (402, "insufficient balance"),
        (429, "rate limit"),
        (500, "HTTP 500"),
    ],
)
def test_http_failure_is_safely_classified(status_code: int, message: str) -> None:
    client = DeepSeekExplainer(
        api_key="test-secret",
        http_post=lambda *_args, **_kwargs: FakeResponse({}, status_code),
    )
    with pytest.raises(ExplanationUnavailable, match=message):
        client.explain("question", _evidence())


def test_status_words_in_explanatory_prose_are_not_false_positives() -> None:
    client = DeepSeekExplainer(
        api_key="test-secret",
        http_post=lambda *_args, **_kwargs: FakeResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": (
                                "The terms healthy, unhealthy, and degraded are "
                                "local verdict labels; this explanation only "
                                "describes the supplied measurements."
                            )
                        },
                        "finish_reason": "stop",
                    }
                ]
            }
        ),
    )
    explanation = client.explain("question", _evidence())

    assert "this explanation only describes" in explanation


def test_model_text_is_safe_for_terminal_output() -> None:
    unsafe = "Observed evidence.\x1b]52;c;payload\x07\rOverwritten"
    safe = deepseek_client.normalize_explanation(unsafe)

    assert "Observed evidence." in safe
    assert "Overwritten" in safe
    assert all(
        character in {"\n", "\t"}
        or (ord(character) >= 32 and not 127 <= ord(character) <= 159)
        for character in safe
    )


@pytest.mark.parametrize(
    ("overall_status", "message_fragment"),
    [
        ("healthy", "monitoring path is healthy"),
        ("degraded", "monitoring path is degraded"),
        ("unhealthy", "monitoring path is unhealthy"),
        ("unknown", "monitoring path is unknown"),
    ],
)
def test_deterministic_status_is_explicitly_scoped(
    overall_status: str,
    message_fragment: str,
) -> None:
    state = {
        "report": {
            "assessment_scope": "ric_e2_kpm_telemetry_monitoring_path",
            "overall_status": overall_status,
            "generated_at": "2026-07-31T14:00:02Z",
            "source_observed_at": "2026-07-31T14:00:00Z",
            "source_instance_id": "test-instance",
            "sequence_number": 7,
            "checks": [],
        },
        "explanation": "Observed evidence explained.",
        "explanation_source": "deepseek",
    }

    answer = graph.build_structured_answer(state)["answer"]

    assert answer["summary"]["scope"] == (
        "ric_e2_kpm_telemetry_monitoring_path"
    )
    assert message_fragment in answer["summary"]["message"]


def test_graph_explains_before_evaluating_and_preserves_local_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    captured_evidence: dict[str, Any] = {}
    sentinel = "LOCAL_REPORT_SENTINEL_NOT_FOR_LLM"

    class CapturingExplainer:
        def explain(self, question: str, evidence: dict[str, Any]) -> str:
            events.append("explain")
            captured_evidence.update(evidence)
            assert question == "Is the system healthy?"
            return "The evidence shows one connected E2 node and recent KPM data."

    report = {
        "assessment_scope": "ric_e2_kpm_telemetry_monitoring_path",
        "overall_status": "unhealthy",
        "generated_at": "2026-07-31T14:00:02Z",
        "source_observed_at": "2026-07-31T14:00:00Z",
        "source_instance_id": "test-instance",
        "sequence_number": 7,
        "checks": [
            {
                "name": "sentinel_check",
                "status": "fail",
                "severity": "critical",
                "value": False,
                "detail": sentinel,
            }
        ],
    }

    def fake_evaluate(_snapshot: Any, now: datetime) -> dict[str, Any]:
        events.append("evaluate")
        assert captured_evidence["collected_at"] == now.isoformat().replace(
            "+00:00", "Z"
        )
        return report

    monkeypatch.setattr(graph, "get_health_snapshot", _snapshot)
    monkeypatch.setattr(graph, "evaluate_health", fake_evaluate)
    compiled = graph.build_graph(explainer=CapturingExplainer())

    answer = graph.ask_structured(
        "Is the system healthy?", compiled_graph=compiled
    )

    assert events == ["explain", "evaluate"]
    assert sentinel not in json.dumps(captured_evidence)
    assert not (FORBIDDEN_LLM_KEYS & _all_keys(captured_evidence))
    assert answer["summary"]["status"] == "unhealthy"
    assert answer["summary"]["scope"] == (
        "ric_e2_kpm_telemetry_monitoring_path"
    )
    assert answer["health_report"] == report
    assert answer["explanation"] == (
        "The evidence shows one connected E2 node and recent KPM data."
    )
    assert answer["explanation_source"] == "deepseek"
    assert answer["explanation_label"] == (
        "Advisory evidence interpretation (DeepSeek):"
    )
    assert answer["display"].startswith(
        "The RIC/E2 KPM telemetry monitoring path is unhealthy."
    )
    assert "\n\nAdvisory evidence interpretation (DeepSeek):\n" in answer[
        "display"
    ]


def test_model_prose_cannot_override_the_local_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class VerdictExplainer:
        def explain(self, _question: str, _evidence: dict[str, Any]) -> str:
            return "The system is healthy."

    monkeypatch.setattr(graph, "get_health_snapshot", _snapshot)

    local_report = {
        "assessment_scope": "ric_e2_kpm_telemetry_monitoring_path",
        "overall_status": "unhealthy",
        "generated_at": "2026-07-31T14:00:02Z",
        "source_observed_at": "2026-07-31T14:00:00Z",
        "source_instance_id": "test-instance",
        "sequence_number": 7,
        "checks": [],
    }
    monkeypatch.setattr(
        graph,
        "evaluate_health",
        lambda _snapshot, now: local_report,
    )
    compiled = graph.build_graph(explainer=VerdictExplainer())
    answer = graph.ask_structured("Assess it", compiled_graph=compiled)

    assert answer["summary"]["status"] == "unhealthy"
    assert answer["health_report"] == local_report
    assert answer["explanation_source"] == "deepseek"
    assert answer["display"].startswith(
        "The RIC/E2 KPM telemetry monitoring path is unhealthy."
    )


def test_graph_uses_deterministic_prose_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingExplainer:
        def explain(self, _question: str, _evidence: dict[str, Any]) -> str:
            raise ExplanationUnavailable("test outage")

    monkeypatch.setattr(graph, "get_health_snapshot", _snapshot)
    compiled = graph.build_graph(explainer=FailingExplainer())
    answer = graph.ask_structured("Explain the state", compiled_graph=compiled)

    assert answer["explanation_source"] == "deterministic-fallback"
    assert "DeepSeek explanation is unavailable: test outage." in answer["display"]
    assert "Deterministic evidence notes:" in answer["display"]
    assert "Advisory evidence interpretation (DeepSeek):" not in answer["display"]
    assert answer["summary"]["status"] in {
        "healthy",
        "degraded",
        "unhealthy",
        "unknown",
    }
