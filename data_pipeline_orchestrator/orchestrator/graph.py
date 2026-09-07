"""Builds the LangGraph StateGraph that orchestrates the sequential pipeline.

Flow: START -> api_invoker_agent -> (status/DAG agents, HIL on failure) ->
run_summarizer_memorizer_agent -> END.
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
    run_summarizer_memorizer_agent,
)
from orchestrator.state import PipelineState


def _route_after_api_invoker(state: PipelineState) -> str:
    """Route to status validator only when submitJob returns 200 OK."""
    if state.get("api_submit_ok"):
        return "api_status_validator_agent"
    return "run_summarizer_memorizer_agent"


def _route_after_status_validator(state: PipelineState) -> str:
    """Route to downstream invoker only when status validator reported SUCCESS."""
    if state.get("api_status_ok"):
        return "data_pipeline_invoker_agent"
    return "run_summarizer_memorizer_agent"


def _route_after_data_pipeline_invoker(state: PipelineState) -> str:
    """Route to DAG monitor when invocation returns HTTP 200, otherwise HIL."""
    if state.get("dag_invocation_ok"):
        return "data_pipeline_monitor_agent"
    return "hil_agent"


def _route_after_data_pipeline_monitor(state: PipelineState) -> str:
    """Continue pipeline after monitor success; otherwise route to HIL."""
    if state.get("dag_monitor_ok"):
        return "run_summarizer_memorizer_agent"
    return "hil_agent"


def build_graph():
    """Construct and compile the pipeline graph."""
    graph = StateGraph(PipelineState)

    graph.add_node("api_invoker_agent", api_invoker_agent)
    graph.add_node("api_status_validator_agent", api_status_validator_agent)
    graph.add_node("data_pipeline_invoker_agent", data_pipeline_invoker_agent)
    graph.add_node("data_pipeline_monitor_agent", data_pipeline_monitor_agent)
    graph.add_node("hil_agent", hil_agent)
    graph.add_node("run_summarizer_memorizer_agent", run_summarizer_memorizer_agent)

    graph.add_edge(START, "api_invoker_agent")
    graph.add_conditional_edges(
        "api_invoker_agent",
        _route_after_api_invoker,
        {
            "api_status_validator_agent": "api_status_validator_agent",
            "run_summarizer_memorizer_agent": "run_summarizer_memorizer_agent",
        },
    )
    graph.add_conditional_edges(
        "api_status_validator_agent",
        _route_after_status_validator,
        {
            "data_pipeline_invoker_agent": "data_pipeline_invoker_agent",
            "run_summarizer_memorizer_agent": "run_summarizer_memorizer_agent",
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
            "run_summarizer_memorizer_agent": "run_summarizer_memorizer_agent",
            "hil_agent": "hil_agent",
        },
    )
    graph.add_edge("hil_agent", "run_summarizer_memorizer_agent")
    graph.add_edge("run_summarizer_memorizer_agent", END)

    return graph.compile()


def run_pipeline(input_data, status_callback: Optional[Callable[[str], None]] = None):
    """Build the graph and run it once with optional live status streaming."""
    app = build_graph()
    initial_state = {"input": input_data, "log": []}
    if status_callback is not None:
        initial_state["status_callback"] = status_callback
    return app.invoke(initial_state)


def stream_pipeline(input_data, status_callback: Optional[Callable[[str], None]] = None):
    """Run the graph with LangGraph streaming.

    Yields one ``{node_name: partial_state_update}`` dict per node as it
    finishes, so a UI can show the state graph advancing in real time.
    """
    app = build_graph()
    initial_state = {"input": input_data, "log": []}
    if status_callback is not None:
        initial_state["status_callback"] = status_callback
    yield from app.stream(initial_state, stream_mode="updates")
