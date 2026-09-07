"""Streamlit UI for the data pipeline orchestrator.

Web front end for the `Run datapipeline job` CLI command. The LangGraph
state graph is drawn live: nodes light up as they run and turn green when
done. Anything involving the human-in-the-loop (HIL) step is highlighted.

Run:
    streamlit run app.py
"""
from __future__ import annotations

import html
import queue
import threading
import time

import streamlit as st

from orchestrator.graph import stream_pipeline
from datetime import datetime

from orchestrator.nodes import (
    _append_long_term_memory,
    _build_execution_summary,
    _load_long_term_memory,
)

st.set_page_config(page_title="Data Pipeline Orchestrator", page_icon="🛠️")

# --- static graph structure, mirrors orchestrator/graph.py --------------------
NODES = [
    "api_invoker_agent",
    "api_status_validator_agent",
    "data_pipeline_invoker_agent",
    "data_pipeline_monitor_agent",
    "hil_agent",
    "run_summarizer_memorizer_agent",
]
EDGES = [  # (from, to, is_conditional)
    ("START", "api_invoker_agent", False),
    ("api_invoker_agent", "api_status_validator_agent", True),
    ("api_invoker_agent", "run_summarizer_memorizer_agent", True),
    ("api_status_validator_agent", "data_pipeline_invoker_agent", True),
    ("api_status_validator_agent", "run_summarizer_memorizer_agent", True),
    ("data_pipeline_invoker_agent", "data_pipeline_monitor_agent", True),
    ("data_pipeline_invoker_agent", "hil_agent", True),
    ("data_pipeline_monitor_agent", "run_summarizer_memorizer_agent", True),
    ("data_pipeline_monitor_agent", "hil_agent", True),
    ("hil_agent", "run_summarizer_memorizer_agent", False),
    ("run_summarizer_memorizer_agent", "END", False),
]
_FILL = {"pending": "#e9ecef", "running": "#ffe08a", "done": "#b7e4c7", "error": "#ff9d9d"}

_HIL_STYLE = "background:#ffdf7e;color:#3a2c00;padding:1px 5px;border-radius:3px;font-weight:600"
_HIL_WORDS = ("hil", "human in the loop", "human action", "manual review", "manual intervention")


def _is_hil(text: str) -> bool:
    low = text.lower()
    return any(word in low for word in _HIL_WORDS)


def _lines_html(lines: list[str]) -> str:
    out = []
    for line in lines:
        safe = html.escape(str(line))
        if _is_hil(line):
            safe = f'<span style="{_HIL_STYLE}">{safe}</span>'
        out.append(f'<div style="margin:2px 0;font-family:monospace;font-size:0.85rem">{safe}</div>')
    return "".join(out)


def _dot(statuses: dict) -> str:
    out = [
        "digraph pipeline {",
        "rankdir=TB; bgcolor=transparent;",
        'node [style=filled, shape=box, fontname="monospace", fontsize=10];',
        'START [shape=oval, fillcolor="#cfe8ff"];',
        'END [shape=oval, fillcolor="#cfe8ff"];',
    ]
    for node in NODES:
        state = statuses.get(node, "pending")
        fill = _FILL[state]
        if node == "hil_agent" and state not in ("pending", "error"):
            fill = "#ff9d9d"
        pen = 3 if state in ("running", "error") else 1
        out.append(f'"{node}" [fillcolor="{fill}", penwidth={pen}];')
    for src, dst, cond in EDGES:
        out.append(f'"{src}" -> "{dst}" [style={"dashed" if cond else "solid"}];')
    out.append("}")
    return "\n".join(out)


# --- UI ----------------------------------------------------------------------
st.title("Data Pipeline Orchestrator")
st.caption(
    "Front end for `Run datapipeline job`. The LangGraph state graph updates live; "
    "HIL steps are highlighted."
)

_ICONS = {"pending": "⬜", "running": "🟨", "done": "🟩", "error": "🟥"}


class _StopRequested(Exception):
    """Raised inside the status callback to abort a run when Stop is clicked."""


def _run_graph_thread(text: str, q: "queue.Queue", stop_event: threading.Event) -> None:
    """Drive the LangGraph stream on a worker thread, pushing updates onto `q`.

    On Stop (or a crash) the graph loop ends early and
    run_summarizer_memorizer_agent is run here so the user still gets a summary.
    """
    result: dict = {"input": text, "log": []}

    def cb(message: str) -> None:
        q.put(("status", message))
        if stop_event.is_set():
            raise _StopRequested()

    stopped = False
    try:
        for chunk in stream_pipeline(text, status_callback=cb):
            for _node, upd in chunk.items():
                if isinstance(upd, dict):
                    result.update(upd)
            q.put(("chunk", chunk))
            if stop_event.is_set():
                stopped = True
                break
    except _StopRequested:
        stopped = True
    except Exception as exc:  # noqa: BLE001 - surface any crash as an errored run
        result.setdefault("error", f"Pipeline crashed: {exc}")

    if stopped:
        result.setdefault("error", "Run stopped by user before completion.")

    if "summary" not in result:  # summarizer node did not get to run — run it here
        memory = list(result.get("log", []))
        summary = _build_execution_summary(result, memory)
        outcome = "FAILED" if result.get("error") else "SUCCESS"
        ltm = _append_long_term_memory(
            {
                "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
                "input": result.get("input"),
                "outcome": outcome,
                "summary": f"{outcome}: {str(result.get('error') or result.get('input'))[:200]}",
            }
        )
        result.update(
            {
                "final_output": result.get("input"),
                "execution_memory": memory,
                "summary": summary,
                "long_term_memory": ltm,
                "log": memory + ["run_summarizer_memorizer_agent: summary generated (forced)"],
            }
        )
        q.put(("chunk", {"run_summarizer_memorizer_agent": {"summary": summary}}))

    q.put(("final", result))


def _draw_graph(statuses: dict, note: str) -> None:
    st.subheader("State graph")
    st.caption(
        "⬜ pending  🟨 running  🟩 done  🟥 error / HIL / stopped"
        "   ·   dashed = conditional edge"
    )
    st.graphviz_chart(_dot(statuses), use_container_width=True)
    seen = sum(1 for s in statuses.values() if s != "pending")
    st.progress(seen / len(NODES), text=note)
    st.markdown(
        "\n".join(f"{_ICONS[statuses[n]]} `{n}`" for n in NODES if statuses[n] != "pending")
        or "_waiting…_"
    )


def _render_history() -> None:
    """Historical Job Run Summary — long-term memory, newest first."""
    history = st.session_state.get("result", {}).get("long_term_memory") if st.session_state.get("result") else None
    history = history or _load_long_term_memory()
    st.sidebar.header("Historical Job Run Summary")
    if not history:
        st.sidebar.caption("No past runs yet.")
        return
    st.sidebar.caption(f"Last {len(history)} runs")
    for e in reversed(history):
        badge = "🟩" if e.get("outcome") == "SUCCESS" else "🟥"
        st.sidebar.markdown(
            f"{badge} **{e.get('timestamp')}**  \n"
            f"input: `{e.get('input')}`  \n"
            f"{html.escape(str(e.get('summary', '')))}"
        )
        st.sidebar.divider()


_render_history()

input_data = st.text_input("Pipeline input", placeholder="Enter input value…")
run = st.button("Run datapipeline job", type="primary")

if run or st.session_state.pop("_retry", False):
    q: "queue.Queue" = queue.Queue()
    stop_event = threading.Event()
    st.session_state["job"] = {
        "q": q,
        "stop": stop_event,
        "statuses": {n: "pending" for n in NODES},
        "messages": [],
        "running": True,
        "stopped": False,
    }
    st.session_state["result"] = None
    threading.Thread(
        target=_run_graph_thread, args=(input_data, q, stop_event), daemon=True
    ).start()

job = st.session_state.get("job")
if job and job["running"]:
    statuses, messages = job["statuses"], job["messages"]

    if st.button("⏹ Stop", type="secondary"):
        job["stop"].set()
        job["stopped"] = True

    final_result = None
    try:
        while True:
            kind, payload = job["q"].get_nowait()
            if kind == "status":
                messages.append(payload)
                head = payload.split(" ", 1)[0]
                if statuses.get(head) == "pending":
                    statuses[head] = "running"
            elif kind == "chunk":
                for node, upd in payload.items():
                    errored = isinstance(upd, dict) and upd.get("error") and not upd.get("hil_required")
                    if node in statuses:
                        statuses[node] = "error" if errored else "done"
            elif kind == "final":
                final_result = payload
    except queue.Empty:
        pass

    if job["stopped"]:
        for node, state in statuses.items():
            if state == "running":
                statuses[node] = "error"

    _draw_graph(statuses, "stopped — summarizing" if job["stopped"] and not final_result else ("done" if final_result else "running…"))
    st.subheader("Live status")
    st.markdown(_lines_html(messages), unsafe_allow_html=True)

    if final_result is not None:
        st.session_state["result"] = final_result
        st.session_state["final_statuses"] = statuses
        job["running"] = False
        st.rerun()
    else:
        time.sleep(0.4)
        st.rerun()

result = st.session_state.get("result")
if result:
    if st.session_state.get("final_statuses"):
        _draw_graph(st.session_state["final_statuses"], "stopped" if result.get("error") == "Run stopped by user before completion." else "finished")

    if result.get("summary"):
        st.subheader("Summary")
        highlighted = result["summary"]
        if result.get("hil_required"):
            highlighted = "\n".join(
                f'<span style="{_HIL_STYLE}">{html.escape(ln)}</span>' if _is_hil(ln) else html.escape(ln)
                for ln in highlighted.splitlines()
            )
            st.markdown(f"<div>{highlighted.replace(chr(10), '<br>')}</div>", unsafe_allow_html=True)
        else:
            st.markdown(highlighted)

    with st.expander("Execution memory (step snapshot held by run_summarizer_memorizer_agent)"):
        st.code("\n".join(result.get("execution_memory", [])) or "(empty)")

    st.subheader("Pipeline log")
    st.markdown(_lines_html(result.get("log", [])), unsafe_allow_html=True)

    if result.get("hil_required"):
        st.subheader("⚠️ Human-in-the-loop required")
        st.markdown(
            f'<div style="{_HIL_STYLE};display:block;padding:10px;margin-bottom:8px">'
            f'{html.escape(result.get("hil_message") or "Manual intervention required before continuing.")}'
            "</div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            "**Next actions**\n"
            "1. Check Airflow REST API at http://airflow.localhost:6563/api/v2/dags/\n"
            "2. Verify the DAG `random_number_check_dag` exists and is deployable\n"
            "3. Retry with the button below"
        )
        with st.expander("HIL diagnostics"):
            st.json(
                {
                    "job_id": result.get("job_id"),
                    "dag_name": result.get("dag_name"),
                    "dag_invocation_status_code": result.get("dag_invocation_status_code"),
                    "dag_trigger_last_url": result.get("dag_trigger_last_url"),
                    "dag_trigger_attempt_urls": result.get("dag_trigger_attempt_urls"),
                    "error": result.get("error"),
                }
            )
        if st.button("Retry pipeline"):
            st.session_state["_retry"] = True
            st.rerun()

    if result.get("error"):
        st.error(result["error"])
    elif result.get("final_output") is not None:
        st.success(f"Final output: {result['final_output']}")
