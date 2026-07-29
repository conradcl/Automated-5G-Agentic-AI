"""LangGraph workflow for the read-only Health Agent rApp MVP.

The first MVP does not need an LLM to decide network health. LangGraph
orchestrates three deterministic, testable nodes:

    fetch R1 snapshot -> evaluate health -> build structured answer

An explanation model can be added later *after* ``evaluate_health`` without
allowing model wording to alter the verified findings.
"""
from __future__ import annotations

import json
from typing import Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from consumer import HealthSnapshot, get_health_snapshot
from health_checks import HealthReport, evaluate_health


class AgentState(TypedDict, total=False):
    query: str
    snapshot: HealthSnapshot
    report: HealthReport
    answer: dict


def fetch_health_data(_: AgentState) -> dict:
    return {"snapshot": get_health_snapshot()}


def run_health_checks(state: AgentState) -> dict:
    return {"report": evaluate_health(state["snapshot"])}


def build_structured_answer(state: AgentState) -> dict:
    report = state["report"]
    summary = {
        "healthy": report["overall_status"] == "healthy",
        "status": report["overall_status"],
        "message": {
            "healthy": "The monitored 5G/O-RAN system is healthy.",
            "degraded": "The system is reachable, but one or more health warnings are present.",
            "unhealthy": "The monitored 5G/O-RAN system is unhealthy.",
            "unknown": "System health is unknown because no valid telemetry is available.",
        }[report["overall_status"]],
    }
    return {
        "answer": {
            "question": state.get("query", "Is the system healthy?"),
            "summary": summary,
            "health_report": report,
        }
    }


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("fetch_health_data", fetch_health_data)
    graph.add_node("run_health_checks", run_health_checks)
    graph.add_node("build_structured_answer", build_structured_answer)
    graph.add_edge(START, "fetch_health_data")
    graph.add_edge("fetch_health_data", "run_health_checks")
    graph.add_edge("run_health_checks", "build_structured_answer")
    graph.add_edge("build_structured_answer", END)
    return graph.compile()


_compiled_graph = None


def ask_structured(user_query: str = "Is the system healthy?") -> dict:
    global _compiled_graph
    if _compiled_graph is None:
        _compiled_graph = build_graph()
    result = _compiled_graph.invoke({"query": user_query})
    return result["answer"]


def ask(user_query: str = "Is the system healthy?", thread_id: Optional[str] = None) -> str:
    """Return a human-readable JSON form while preserving the original CLI API.

    ``thread_id`` is accepted for compatibility with the earlier chat prototype;
    this stateless read-only graph does not need conversational checkpoints.
    """
    del thread_id
    return json.dumps(ask_structured(user_query), indent=2)
