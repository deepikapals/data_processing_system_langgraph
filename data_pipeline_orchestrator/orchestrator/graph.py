"""Builds the LangGraph StateGraph that orchestrates the sequential pipeline.

The graph is a straight line: START -> step1 -> step2 -> step3 -> step4 -> END.
Add branching, retries, or parallel fan-out later by changing the edges here.
"""
from __future__ import annotations

from typing import Callable, Optional

from langgraph.graph import END, START, StateGraph

from orchestrator.nodes import (
    api_invoker_agent,
    api_status_validator_agent,
    data_pipeline_invoker_agent,
    data_pipeline_monitor_agent,
    hil_agent,
    step1_ingest,
    step2_process,
    step3_validate,
    step4_finalize,
)
from orchestrator.state import PipelineState


def _route_after_api_invoker(state: PipelineState) -> str:
    """Route to status validator only when submitJob returns 200 OK."""
    if state.get("api_submit_ok"):
        return "api_status_validator_agent"
    return "step3_validate"


def _route_after_status_validator(state: PipelineState) -> str:
    """Route to downstream invoker only when status validator reported SUCCESS."""
    if state.get("api_status_ok"):
        return "data_pipeline_invoker_agent"
    return "step3_validate"


def _route_after_data_pipeline_invoker(state: PipelineState) -> str:
    """Route to DAG monitor when invocation returns HTTP 200, otherwise HIL."""
    if state.get("dag_invocation_ok"):
        return "data_pipeline_monitor_agent"
    return "hil_agent"


def _route_after_data_pipeline_monitor(state: PipelineState) -> str:
    """Continue pipeline after monitor success; otherwise route to HIL."""
    if state.get("dag_monitor_ok"):
        return "step3_validate"
    return "hil_agent"


def build_graph():
    """Construct and compile the pipeline graph."""
    graph = StateGraph(PipelineState)

    graph.add_node("step1_ingest", step1_ingest)
    graph.add_node("step2_process", step2_process)
    graph.add_node("api_invoker_agent", api_invoker_agent)
    graph.add_node("api_status_validator_agent", api_status_validator_agent)
    graph.add_node("data_pipeline_invoker_agent", data_pipeline_invoker_agent)
    graph.add_node("data_pipeline_monitor_agent", data_pipeline_monitor_agent)
    graph.add_node("hil_agent", hil_agent)
    graph.add_node("step3_validate", step3_validate)
    graph.add_node("step4_finalize", step4_finalize)

    graph.add_edge(START, "step1_ingest")
    graph.add_edge("step1_ingest", "step2_process")
    graph.add_edge("step2_process", "api_invoker_agent")
    graph.add_conditional_edges(
        "api_invoker_agent",
        _route_after_api_invoker,
        {
            "api_status_validator_agent": "api_status_validator_agent",
            "step3_validate": "step3_validate",
        },
    )
    graph.add_conditional_edges(
        "api_status_validator_agent",
        _route_after_status_validator,
        {
            "data_pipeline_invoker_agent": "data_pipeline_invoker_agent",
            "step3_validate": "step3_validate",
        },
    )
    graph.add_conditional_edges(
        "data_pipeline_invoker_agent",
        _route_after_data_pipeline_invoker,
        {
            "data_pipeline_monitor_agent": "data_pipeline_monitor_agent",
            "hil_agent": "hil_agent",
        },
    )
    graph.add_conditional_edges(
        "data_pipeline_monitor_agent",
        _route_after_data_pipeline_monitor,
        {
            "step3_validate": "step3_validate",
            "hil_agent": "hil_agent",
        },
    )
    graph.add_edge("hil_agent", "step3_validate")
    graph.add_edge("step3_validate", "step4_finalize")
    graph.add_edge("step4_finalize", END)

    return graph.compile()


def run_pipeline(input_data, status_callback: Optional[Callable[[str], None]] = None):
    """Build the graph and run it once with optional live status streaming."""
    app = build_graph()
    initial_state = {"input": input_data, "log": []}
    if status_callback is not None:
        initial_state["status_callback"] = status_callback
    return app.invoke(initial_state)
