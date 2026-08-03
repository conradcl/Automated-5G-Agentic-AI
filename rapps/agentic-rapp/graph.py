"""LangGraph workflow for deterministic health plus DeepSeek explanation.

The LLM boundary is deliberately reached before the deterministic health report
is created. DeepSeek receives a verdict-free evidence envelope, while the local
health evaluator remains authoritative and available when the model fails.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional, Protocol, TypedDict

from langgraph.graph import END, START, StateGraph

import config
from consumer import HealthSnapshot, get_health_snapshot
from deepseek_client import (
    ASSESSMENT_SCOPE,
    DeepSeekExplainer,
    ExplanationUnavailable,
    build_evidence_payload,
    normalize_explanation,
)
from health_checks import HealthReport, evaluate_health


logger = logging.getLogger(__name__)
DEFAULT_QUERY = f"Is the {config.ASSESSMENT_SCOPE_LABEL} healthy?"


class EvidenceExplainer(Protocol):
    def explain(self, question: str, evidence: dict[str, Any]) -> str: ...


class AgentState(TypedDict, total=False):
    query: str
    snapshot: HealthSnapshot
    assessment_at: datetime
    evidence: dict[str, Any]
    explanation: Optional[str]
    explanation_source: str
    llm_error: Optional[str]
    report: HealthReport
    answer: dict


def fetch_health_data(_: AgentState) -> dict:
    return {
        "snapshot": get_health_snapshot(),
        "assessment_at": datetime.now(timezone.utc),
    }


def prepare_llm_evidence(state: AgentState) -> dict:
    return {
        "evidence": build_evidence_payload(
            state["snapshot"], collected_at=state["assessment_at"]
        )
    }


def make_explain_evidence_node(explainer: EvidenceExplainer):
    def explain_evidence(state: AgentState) -> dict:
        try:
            explanation = normalize_explanation(
                explainer.explain(
                    state.get("query", DEFAULT_QUERY),
                    state["evidence"],
                )
            )
            return {
                "explanation": explanation,
                "explanation_source": "deepseek",
                "llm_error": None,
            }
        except ExplanationUnavailable as exc:
            return {
                "explanation": None,
                "explanation_source": "deterministic-fallback",
                "llm_error": str(exc),
            }
        except Exception as exc:  # Keep model/library failures out of the CLI loop.
            logger.warning(
                "Unexpected DeepSeek explanation failure (%s)",
                type(exc).__name__,
            )
            return {
                "explanation": None,
                "explanation_source": "deterministic-fallback",
                "llm_error": "unexpected DeepSeek client failure",
            }

    return explain_evidence


def run_health_checks(state: AgentState) -> dict:
    return {
        "report": evaluate_health(
            state["snapshot"],
            now=state["assessment_at"],
        )
    }


def _fallback_explanation(report: HealthReport, reason: Optional[str]) -> str:
    unavailable = "DeepSeek explanation is unavailable"
    if reason:
        unavailable += f": {reason}"
    unavailable += "."

    findings = [
        check["detail"] for check in report["checks"] if check["status"] != "pass"
    ]
    if findings:
        return f"{unavailable} Evidence requiring attention: {' '.join(findings)}"
    return unavailable


def build_structured_answer(state: AgentState) -> dict:
    report = state["report"]
    summary = {
        "healthy": report["overall_status"] == "healthy",
        "status": report["overall_status"],
        "scope": report.get("assessment_scope", ASSESSMENT_SCOPE),
        "message": {
            "healthy": f"The {config.ASSESSMENT_SCOPE_LABEL} is healthy.",
            "degraded": (
                f"The {config.ASSESSMENT_SCOPE_LABEL} is degraded because one "
                "or more telemetry warnings are present."
            ),
            "unhealthy": f"The {config.ASSESSMENT_SCOPE_LABEL} is unhealthy.",
            "unknown": (
                f"The health of the {config.ASSESSMENT_SCOPE_LABEL} is unknown "
                "because no valid telemetry is available."
            ),
        }[report["overall_status"]],
    }
    explanation = state.get("explanation") or _fallback_explanation(
        report, state.get("llm_error")
    )
    explanation_source = state.get(
        "explanation_source", "deterministic-fallback"
    )
    explanation_label = (
        "Advisory evidence interpretation (DeepSeek):"
        if explanation_source == "deepseek"
        else "Deterministic evidence notes:"
    )
    display = f"{summary['message']}\n\n{explanation_label}\n{explanation}"
    return {
        "answer": {
            "question": state.get("query", DEFAULT_QUERY),
            "summary": summary,
            "explanation": explanation,
            "explanation_label": explanation_label,
            "explanation_source": explanation_source,
            "llm": {
                "provider": "deepseek",
                "model": config.DEEPSEEK_MODEL,
                "temperature": config.DEEPSEEK_TEMPERATURE,
                "error": state.get("llm_error"),
            },
            "health_report": report,
            "display": display,
        }
    }


def build_graph(explainer: Optional[EvidenceExplainer] = None):
    explainer = explainer or DeepSeekExplainer()
    graph = StateGraph(AgentState)
    graph.add_node("fetch_health_data", fetch_health_data)
    graph.add_node("prepare_llm_evidence", prepare_llm_evidence)
    graph.add_node("explain_evidence", make_explain_evidence_node(explainer))
    graph.add_node("run_health_checks", run_health_checks)
    graph.add_node("build_structured_answer", build_structured_answer)
    graph.add_edge(START, "fetch_health_data")
    graph.add_edge("fetch_health_data", "prepare_llm_evidence")
    graph.add_edge("prepare_llm_evidence", "explain_evidence")
    graph.add_edge("explain_evidence", "run_health_checks")
    graph.add_edge("run_health_checks", "build_structured_answer")
    graph.add_edge("build_structured_answer", END)
    return graph.compile()


_compiled_graph = None


def ask_structured(
    user_query: str = DEFAULT_QUERY, *, compiled_graph=None
) -> dict:
    global _compiled_graph
    active_graph = compiled_graph
    if active_graph is None:
        if _compiled_graph is None:
            _compiled_graph = build_graph()
        active_graph = _compiled_graph
    result = active_graph.invoke({"query": user_query})
    return result["answer"]


def ask(
    user_query: str = DEFAULT_QUERY,
    thread_id: Optional[str] = None,
    *,
    compiled_graph=None,
) -> str:
    """Return prose while preserving the original CLI entry point.

    ``thread_id`` is accepted for compatibility with the earlier chat prototype;
    this stateless read-only graph does not need conversational checkpoints.
    """
    del thread_id
    return ask_structured(user_query, compiled_graph=compiled_graph)["display"]
