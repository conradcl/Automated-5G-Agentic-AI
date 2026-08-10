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
    MemoryContextUnavailable,
    build_evidence_payload,
    normalize_explanation,
)
from health_checks import HealthReport, evaluate_health
from memory import get_runtime_memory, normalize_thread_id


logger = logging.getLogger(__name__)
DEFAULT_QUERY = f"Is the {config.ASSESSMENT_SCOPE_LABEL} healthy?"


class EvidenceExplainer(Protocol):
    def explain(self, question: str, evidence: dict[str, Any]) -> str: ...


class AgentState(TypedDict, total=False):
    query: str
    thread_id: str
    snapshot: HealthSnapshot
    assessment_at: datetime
    evidence: dict[str, Any]
    memory_context: Optional[dict[str, Any]]
    memory_error: Optional[str]
    memory_context_used: bool
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


def make_load_memory_node(memory_store):
    def load_memory(state: AgentState) -> dict:
        if memory_store is None:
            return {"memory_context": None, "memory_error": None}
        try:
            return {
                "memory_context": memory_store.build_query_context(
                    state["thread_id"],
                    through_snapshot=state["snapshot"],
                ),
                "memory_error": None,
            }
        except Exception as exc:
            logger.warning("Could not load rApp memory (%s)", type(exc).__name__)
            return {
                "memory_context": None,
                "memory_error": f"{type(exc).__name__}: {str(exc)[:300]}",
            }

    return load_memory


def make_explain_evidence_node(explainer: EvidenceExplainer):
    def explain_evidence(state: AgentState) -> dict:
        try:
            memory_context_used = False
            contextual_explain = getattr(explainer, "explain_with_context", None)
            if (
                callable(contextual_explain)
                and state.get("memory_context") is not None
            ):
                try:
                    model_text = contextual_explain(
                        state.get("query", DEFAULT_QUERY),
                        state["evidence"],
                        state["memory_context"],
                    )
                    memory_context_used = True
                except MemoryContextUnavailable as exc:
                    logger.warning(
                        "Local memory context was omitted (%s)", str(exc)
                    )
                    model_text = explainer.explain(
                        state.get("query", DEFAULT_QUERY),
                        state["evidence"],
                    )
            else:
                # Preserve compatibility with existing two-argument explainer
                # adapters and injected test doubles.
                model_text = explainer.explain(
                    state.get("query", DEFAULT_QUERY),
                    state["evidence"],
                )
            explanation = normalize_explanation(model_text)
            return {
                "explanation": explanation,
                "explanation_source": "deepseek",
                "llm_error": None,
                "memory_context_used": memory_context_used,
            }
        except ExplanationUnavailable as exc:
            return {
                "explanation": None,
                "explanation_source": "deterministic-fallback",
                "llm_error": str(exc),
                "memory_context_used": False,
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
                "memory_context_used": False,
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
    answer = {
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
    memory_context = state.get("memory_context")
    if isinstance(memory_context, dict) or state.get("memory_error"):
        if not isinstance(memory_context, dict):
            memory_context = {}
        partial = memory_context.get("partial_window")
        answer["memory"] = {
            "thread_id": state.get("thread_id"),
            "conversation_turns_available": len(
                memory_context.get("conversation", [])
            ),
            "completed_windows_available": len(
                memory_context.get("completed_windows", [])
            ),
            "partial_samples_available": (
                partial.get("included_sample_count", 0)
                if isinstance(partial, dict)
                else 0
            ),
            "context_used_for_explanation": state.get(
                "memory_context_used", False
            ),
            "load_error": state.get("memory_error"),
        }
    return {"answer": answer}


def make_persist_conversation_node(memory_store):
    def persist_conversation(state: AgentState) -> dict:
        if memory_store is None:
            return {}
        try:
            answer = state["answer"]
            memory_store.record_turn(
                state["thread_id"],
                question=state.get("query", DEFAULT_QUERY),
                # Only the pre-verdict advisory explanation is retained for
                # future model context. The deterministic report/display never
                # crosses back into the LLM boundary.
                advisory_explanation=answer.get("explanation"),
                explanation_source=answer.get(
                    "explanation_source", "deterministic-fallback"
                ),
                asked_at=state.get("assessment_at"),
            )
        except Exception as exc:
            logger.warning(
                "Could not persist rApp conversation memory (%s)",
                type(exc).__name__,
            )
        return {}

    return persist_conversation


def build_graph(
    explainer: Optional[EvidenceExplainer] = None,
    *,
    memory_store=None,
):
    explainer = explainer or DeepSeekExplainer()
    graph = StateGraph(AgentState)
    graph.add_node("fetch_health_data", fetch_health_data)
    graph.add_node("load_memory", make_load_memory_node(memory_store))
    graph.add_node("prepare_llm_evidence", prepare_llm_evidence)
    graph.add_node("explain_evidence", make_explain_evidence_node(explainer))
    graph.add_node("run_health_checks", run_health_checks)
    graph.add_node("build_structured_answer", build_structured_answer)
    graph.add_node(
        "persist_conversation",
        make_persist_conversation_node(memory_store),
    )
    graph.add_edge(START, "fetch_health_data")
    graph.add_edge("fetch_health_data", "load_memory")
    graph.add_edge("load_memory", "prepare_llm_evidence")
    graph.add_edge("prepare_llm_evidence", "explain_evidence")
    graph.add_edge("explain_evidence", "run_health_checks")
    graph.add_edge("run_health_checks", "build_structured_answer")
    graph.add_edge("build_structured_answer", "persist_conversation")
    graph.add_edge("persist_conversation", END)
    return graph.compile()


_compiled_graph = None


def reset_runtime_graph() -> None:
    """Drop the compiled graph when its process-wide memory store is closed."""
    global _compiled_graph
    _compiled_graph = None


def ask_structured(
    user_query: str = DEFAULT_QUERY,
    thread_id: Optional[str] = None,
    *,
    compiled_graph=None,
) -> dict:
    global _compiled_graph
    active_graph = compiled_graph
    if active_graph is None:
        if _compiled_graph is None:
            _compiled_graph = build_graph(memory_store=get_runtime_memory())
        active_graph = _compiled_graph
    result = active_graph.invoke(
        {
            "query": user_query,
            "thread_id": normalize_thread_id(thread_id),
        }
    )
    return result["answer"]


def ask(
    user_query: str = DEFAULT_QUERY,
    thread_id: Optional[str] = None,
    *,
    compiled_graph=None,
) -> str:
    """Return prose while preserving the original CLI entry point."""
    return ask_structured(
        user_query,
        thread_id=thread_id,
        compiled_graph=compiled_graph,
    )["display"]
