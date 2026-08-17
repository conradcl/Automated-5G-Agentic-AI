"""LangGraph workflow for deterministic health plus DeepSeek explanation.

The LLM boundary is deliberately reached before the deterministic health report
is created. DeepSeek receives a verdict-free evidence envelope, while the local
health evaluator remains authoritative and available when the model fails.
"""
from __future__ import annotations

import copy
import logging
import threading
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
from read_tools import (
    READ_TOOL_NAMES,
    ReadToolConfigurationError,
    ReadToolRegistry,
    validate_read_tool_catalog,
    validate_read_tool_result,
)


logger = logging.getLogger(__name__)
DEFAULT_QUERY = f"Is the {config.ASSESSMENT_SCOPE_LABEL} healthy?"


class EvidenceExplainer(Protocol):
    def explain(self, question: str, evidence: dict[str, Any]) -> str: ...


class ReadToolRunner(Protocol):
    def catalog(self) -> list[dict[str, Any]]: ...

    def execute(self, tool_name: str) -> dict[str, Any]: ...


class AgentState(TypedDict, total=False):
    query: str
    thread_id: str
    snapshot: HealthSnapshot
    assessment_at: datetime
    evidence: dict[str, Any]
    memory_context: Optional[dict[str, Any]]
    memory_error: Optional[str]
    memory_context_used: bool
    read_tools_enabled: bool
    read_tool_results: list[dict[str, Any]]
    read_tool_request: Optional[str]
    read_tool_calls_made: int
    read_tool_stop_reason: Optional[str]
    read_tool_planning_error: Optional[str]
    read_tool_execution_error: Optional[str]
    read_tool_route: str
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


def make_plan_read_tools_node(
    explainer: EvidenceExplainer,
    read_tool_runner: Optional[ReadToolRunner],
    *,
    max_calls: int,
):
    def plan_read_tools(state: AgentState) -> dict:
        decide = getattr(explainer, "decide_read_tool", None)
        if read_tool_runner is None:
            return {
                "read_tools_enabled": False,
                "read_tool_results": list(state.get("read_tool_results", [])),
                "read_tool_calls_made": state.get("read_tool_calls_made", 0),
                "read_tool_stop_reason": "disabled",
                "read_tool_route": "fallback",
            }
        if not callable(decide):
            return {
                "read_tools_enabled": False,
                "read_tool_results": list(state.get("read_tool_results", [])),
                "read_tool_calls_made": state.get("read_tool_calls_made", 0),
                "read_tool_stop_reason": "explainer_without_tool_capability",
                "read_tool_route": "fallback",
            }
        execution_error = state.get("read_tool_execution_error")
        if execution_error:
            return {
                "read_tools_enabled": True,
                "read_tool_planning_error": execution_error,
                "read_tool_stop_reason": "invalid_tool_result",
                "read_tool_route": "fallback",
            }

        results = list(state.get("read_tool_results", []))
        calls_made = state.get("read_tool_calls_made", len(results))
        used_names = {
            result.get("tool_name")
            for result in results
            if isinstance(result, dict)
            and isinstance(result.get("tool_name"), str)
        }
        memory_used = state.get("memory_context") is not None
        try:
            full_catalog = read_tool_runner.catalog()
            catalog_errors = validate_read_tool_catalog(full_catalog)
            if catalog_errors:
                raise ExplanationUnavailable(
                    f"read-tool catalog is malformed: {catalog_errors[0]}"
                )
            catalog = []
            if calls_made < max_calls:
                catalog = [
                    tool
                    for tool in full_catalog
                    if tool["name"] not in used_names
                ]
            try:
                decision = decide(
                    state.get("query", DEFAULT_QUERY),
                    state["evidence"],
                    state.get("memory_context"),
                    results,
                    catalog,
                )
            except MemoryContextUnavailable as exc:
                logger.warning(
                    "Local memory was omitted from read-tool planning (%s)", str(exc)
                )
                memory_used = False
                decision = decide(
                    state.get("query", DEFAULT_QUERY),
                    state["evidence"],
                    None,
                    results,
                    catalog,
                )
            if not isinstance(decision, dict):
                raise ExplanationUnavailable(
                    "read-tool planner returned a malformed decision"
                )
            if decision.get("decision") == "answer" and set(decision) == {
                "decision",
                "answer",
            }:
                explanation = normalize_explanation(decision.get("answer"))
                return {
                    "read_tools_enabled": True,
                    "read_tool_request": None,
                    "read_tool_stop_reason": (
                        "max_calls" if calls_made >= max_calls else "model_answer"
                    ),
                    "read_tool_planning_error": None,
                    "read_tool_route": "answer",
                    "explanation": explanation,
                    "explanation_source": "deepseek",
                    "llm_error": None,
                    "memory_context_used": memory_used,
                }
            if decision.get("decision") == "call_tool" and set(decision) == {
                "decision",
                "tool_name",
            }:
                requested = decision.get("tool_name")
                available_names = {
                    tool.get("name")
                    for tool in catalog
                    if isinstance(tool, dict)
                }
                if requested not in available_names or requested in used_names:
                    raise ExplanationUnavailable(
                        "read-tool planner requested an unavailable tool"
                    )
                return {
                    "read_tools_enabled": True,
                    "read_tool_request": requested,
                    "read_tool_stop_reason": None,
                    "read_tool_planning_error": None,
                    "read_tool_route": "execute",
                }
            raise ExplanationUnavailable(
                "read-tool planner returned a malformed decision"
            )
        except ExplanationUnavailable as exc:
            return {
                "read_tools_enabled": True,
                "read_tool_request": None,
                "read_tool_stop_reason": "planner_unavailable",
                "read_tool_planning_error": str(exc)[:300],
                "read_tool_route": "fallback",
            }
        except Exception as exc:
            logger.warning(
                "Unexpected read-tool planning failure (%s)", type(exc).__name__
            )
            return {
                "read_tools_enabled": True,
                "read_tool_request": None,
                "read_tool_stop_reason": "planner_unavailable",
                "read_tool_planning_error": "unexpected read-tool planning failure",
                "read_tool_route": "fallback",
            }

    return plan_read_tools


def make_execute_read_tool_node(read_tool_runner: ReadToolRunner):
    def execute_read_tool(state: AgentState) -> dict:
        requested = state.get("read_tool_request")
        results = list(state.get("read_tool_results", []))
        calls_made = state.get("read_tool_calls_made", len(results))
        if requested not in READ_TOOL_NAMES:
            return {
                "read_tool_request": None,
                "read_tool_calls_made": calls_made + 1,
                "read_tool_execution_error": "read-tool request was not allowlisted",
            }
        try:
            result = read_tool_runner.execute(requested)
        except Exception:
            logger.warning("Read-tool runner failed for %s", requested)
            return {
                "read_tool_request": None,
                "read_tool_calls_made": calls_made + 1,
                "read_tool_execution_error": "read-tool runner failed safely",
            }
        result_errors = validate_read_tool_result(result)
        if result_errors:
            return {
                "read_tool_request": None,
                "read_tool_calls_made": calls_made + 1,
                "read_tool_execution_error": (
                    f"read-tool result was rejected: {result_errors[0]}"
                )[:300],
            }
        results.append(copy.deepcopy(result))
        return {
            "read_tool_results": results,
            "read_tool_request": None,
            "read_tool_calls_made": calls_made + 1,
            "read_tool_execution_error": None,
        }

    return execute_read_tool


def route_after_read_tool_plan(state: AgentState) -> str:
    route = state.get("read_tool_route")
    if route in {"execute", "answer", "fallback"}:
        return route
    return "fallback"


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
    if state.get("read_tools_enabled") is True:
        answer["read_tools"] = {
            "enabled": state.get("read_tools_enabled", False),
            "calls_made": state.get("read_tool_calls_made", 0),
            "tools_used": [
                result.get("tool_name")
                for result in state.get("read_tool_results", [])
                if isinstance(result, dict)
            ],
            "stop_reason": state.get("read_tool_stop_reason"),
            "planning_error": state.get("read_tool_planning_error"),
            "results": copy.deepcopy(state.get("read_tool_results", [])),
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
    read_tool_registry: Optional[ReadToolRunner] = None,
):
    using_default_explainer = explainer is None
    explainer = explainer or DeepSeekExplainer()
    if (
        read_tool_registry is None
        and (using_default_explainer or isinstance(explainer, DeepSeekExplainer))
        and config.RAPP_READ_TOOLS_ENABLED
    ):
        try:
            read_tool_registry = ReadToolRegistry(
                memory_store if memory_store is not None else get_runtime_memory()
            )
        except ReadToolConfigurationError as exc:
            logger.warning("Read-only evidence tools are disabled (%s)", str(exc))
            read_tool_registry = None
    max_read_tool_calls = config.RAPP_READ_TOOL_MAX_CALLS
    if (
        isinstance(max_read_tool_calls, bool)
        or not isinstance(max_read_tool_calls, int)
        or not 1 <= max_read_tool_calls <= len(READ_TOOL_NAMES)
    ):
        logger.warning("Read-only evidence tools have an invalid call limit")
        read_tool_registry = None
        max_read_tool_calls = 1
    graph = StateGraph(AgentState)
    graph.add_node("fetch_health_data", fetch_health_data)
    graph.add_node("load_memory", make_load_memory_node(memory_store))
    graph.add_node("prepare_llm_evidence", prepare_llm_evidence)
    graph.add_node(
        "plan_read_tools",
        make_plan_read_tools_node(
            explainer,
            read_tool_registry,
            max_calls=max_read_tool_calls,
        ),
    )
    if read_tool_registry is not None:
        graph.add_node(
            "execute_read_tool",
            make_execute_read_tool_node(read_tool_registry),
        )
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
    graph.add_edge("prepare_llm_evidence", "plan_read_tools")
    route_map = {
        "answer": "run_health_checks",
        "fallback": "explain_evidence",
    }
    if read_tool_registry is not None:
        route_map["execute"] = "execute_read_tool"
    else:
        route_map["execute"] = "explain_evidence"
    graph.add_conditional_edges(
        "plan_read_tools",
        route_after_read_tool_plan,
        route_map,
    )
    if read_tool_registry is not None:
        graph.add_edge("execute_read_tool", "plan_read_tools")
    graph.add_edge("explain_evidence", "run_health_checks")
    graph.add_edge("run_health_checks", "build_structured_answer")
    graph.add_edge("build_structured_answer", "persist_conversation")
    graph.add_edge("persist_conversation", END)
    return graph.compile()


_compiled_graph = None
_runtime_graph_lock = threading.RLock()


def reset_runtime_graph() -> None:
    """Drop the compiled graph when its process-wide memory store is closed."""
    global _compiled_graph
    with _runtime_graph_lock:
        _compiled_graph = None


def ask_structured(
    user_query: str = DEFAULT_QUERY,
    thread_id: Optional[str] = None,
    *,
    compiled_graph=None,
) -> dict:
    global _compiled_graph
    if compiled_graph is not None:
        result = compiled_graph.invoke(
            {
                "query": user_query,
                "thread_id": normalize_thread_id(thread_id),
            }
        )
        return result["answer"]
    # The default compiled graph owns one shared read-tool registry/session.
    # Serialize its invocations so the scheduled diagnosis and interactive CLI
    # cannot race its initialization or concurrently mutate client state.
    with _runtime_graph_lock:
        if _compiled_graph is None:
            _compiled_graph = build_graph(memory_store=get_runtime_memory())
        result = _compiled_graph.invoke(
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
