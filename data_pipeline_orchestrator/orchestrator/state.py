"""Shared state schema passed between nodes in the pipeline graph."""
from __future__ import annotations

from typing import Any, Callable, Optional, TypedDict


class PipelineState(TypedDict, total=False):
    """State that flows through the sequential pipeline.

    Each node reads what it needs from this dict and returns a partial
    dict of updates; LangGraph merges those updates into the running state.
    Extend this schema as your real pipeline steps take shape.
    """

    # Original input handed to the pipeline when it starts.
    input: Any

    # Output produced by each step, kept around for debugging/inspection.
    step1_output: Optional[Any]
    step2_output: Optional[Any]
    step3_output: Optional[Any]

    # API orchestration details.
    api_submit_ok: Optional[bool]
    api_submit_status_code: Optional[int]
    job_id: Optional[str]
    job_submit_status: Optional[str]
    api_status_ok: Optional[bool]
    api_status_code: Optional[int]
    api_job_status: Optional[str]
    data_pipeline_invoked: Optional[bool]
    dag_name: Optional[str]
    dag_run_id: Optional[str]
    airflow_bearer_token: Optional[str]
    dag_trigger_last_url: Optional[str]
    dag_trigger_attempt_urls: Optional[list[str]]
    dag_invocation_ok: Optional[bool]
    dag_invocation_status_code: Optional[int]
    dag_monitor_ok: Optional[bool]
    dag_monitor_status: Optional[str]
    dag_monitor_status_code: Optional[int]
    hil_required: Optional[bool]
    hil_message: Optional[str]

    # Final result once the pipeline has finished.
    final_output: Optional[Any]

    # Free-form list of human-readable log lines, appended to by each node.
    log: list[str]

    # Populated if a node hits an error it wants downstream nodes to see.
    error: Optional[str]

    # Optional hook for streaming live status messages to callers (e.g., CLI).
    status_callback: Callable[[str], None]
