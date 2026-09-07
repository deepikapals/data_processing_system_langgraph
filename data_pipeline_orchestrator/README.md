# data_pipeline_orchestrator

A [LangGraph](https://github.com/langchain-ai/langgraph) orchestrator that drives a
job through an external **submitJob** API and an **Apache Airflow** DAG, polling
each for completion, escalating to a human when something goes wrong, and
finishing with a written summary that is remembered across runs.

It ships with three front ends:

| Front end | Command | Use |
|-----------|---------|-----|
| One‑shot CLI | `python main.py "input"` | scripted / single run |
| Interactive CLI | `python main.py --interactive` | REPL with a human‑in‑the‑loop prompt |
| Streamlit UI | `streamlit run app.py` | live state‑graph view, Stop button, run history |

---

## What it does

Given an input string, the orchestrator runs this sequence:

1. **`api_invoker_agent`** – `POST` the **submitJob** API, retrying until it gets
   `HTTP 200`. On success it captures a `job_id`.
2. **`api_status_validator_agent`** – poll **jobStatus** every 60 s until the API
   reports `SUCCESS`.
3. **`data_pipeline_invoker_agent`** – fetch an Airflow bearer token, then trigger
   the DAG **`random_number_check_dag`**. Captures the `dag_run_id` from the
   response.
4. **`data_pipeline_monitor_agent`** – poll that one DAG run
   (`GET .../dagRuns/<dag_run_id>`) every 60 s until it reports `SUCCESS`.
5. **`hil_agent`** – reached whenever step 3 or 4 fails; records a
   human‑in‑the‑loop (HIL) message and marks the run as needing manual review.
6. **`run_summarizer_memorizer_agent`** – always the last node. Writes a
   human‑readable summary, keeps a short‑term memory snapshot of the run, and
   appends a dated one‑line summary to long‑term memory (last 10 runs, on disk).

Any earlier failure (submitJob never returns 200, status never reaches SUCCESS)
short‑circuits straight to the summarizer; DAG‑stage failures go through HIL
first. The pipeline never raises — every path ends at the summarizer with a
`summary` and, on failure, an `error`.

### Graph

```
                START
                  │
                  ▼
          api_invoker_agent ─────────────(submit ≠ 200)────────────┐
                  │ (200)                                          │
                  ▼                                                │
      api_status_validator_agent ───────(status ≠ SUCCESS)─────────┤
                  │ (SUCCESS)                                      │
                  ▼                                                │
     data_pipeline_invoker_agent ───────(trigger ≠ 200)──► hil_agent
                  │ (200/201)                                      │
                  ▼                                                │
     data_pipeline_monitor_agent ───────(FAILED / gave up)──► hil_agent
                  │ (SUCCESS)                                      │
                  ▼                                                │
     run_summarizer_memorizer_agent ◄──────────────────────────────┘
                  │
                  ▼
                 END
```

Solid edges are unconditional; the branch points above are LangGraph
`add_conditional_edges` wired in `orchestrator/graph.py`.

---

## Repository layout

```
.
├── main.py                     # CLI entry point (one-shot + interactive)
├── app.py                      # Streamlit UI
├── orchestrator/
│   ├── state.py                # PipelineState (TypedDict) – shared state schema
│   ├── nodes.py                # all agent node functions + HTTP / memory helpers
│   └── graph.py                # build_graph(), run_pipeline(), stream_pipeline()
├── tests/
│   ├── test_graph.py           # graph / node behaviour (mostly mocked HTTP)
│   └── test_cli.py             # CLI command + HIL-choice parsing
├── requirements.txt
├── .env.example
├── .gitignore
└── long_term_memory.json       # created at runtime, git-ignored
```

### Key modules

- **`orchestrator/state.py`** – `PipelineState`, a `TypedDict(total=False)`. Each
  node returns a partial dict; LangGraph merges it. Notable fields:
  `input`, `job_id`, `api_submit_ok`, `api_status_ok`, `dag_name`, `dag_run_id`,
  `dag_invocation_ok`, `dag_monitor_ok`, `dag_monitor_status`, `hil_required`,
  `hil_message`, `error`, `final_output`, `log` (list of human‑readable lines),
  `execution_memory`, `summary`, `long_term_memory`, and `status_callback`
  (optional live‑status hook).
- **`orchestrator/nodes.py`** – the six agent nodes plus helpers:
  `_http_json_request` (5 s‑timeout JSON HTTP), URL builders, Airflow token
  resolver, `_build_execution_summary`, and the long‑term‑memory
  load/append helpers.
- **`orchestrator/graph.py`**
  - `build_graph()` → compiled `StateGraph`.
  - `run_pipeline(input, status_callback=None)` → runs once, returns the final
    state dict.
  - `stream_pipeline(input, status_callback=None)` → generator yielding
    `{node_name: partial_update}` per node, for live UIs.

---

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # only needed if you wire up an LLM node
```

Python 3.9+. The default flow needs **no API keys**; it only needs the external
services below to actually succeed (without them the run still completes, it just
ends in an error / HIL state).

### External services expected

| Service | Default location | Used by |
|---------|------------------|---------|
| submitJob API | `http://localhost:8080/api/submitJob/` | `api_invoker_agent` |
| jobStatus API | `http://localhost:8080/api/jobStatus/<job_id>` | `api_status_validator_agent` |
| Airflow auth  | `http://airflow.localhost:6563/auth/token` | `data_pipeline_invoker_agent` |
| Airflow REST  | `http://airflow.localhost:6563/api/v2/dags/…` | invoker + monitor |

All of these are overridable via environment variables (see
[Configuration](#configuration)).

---

## Running

### One‑shot CLI

```bash
python main.py "customer orders"
```

Prints the step‑by‑step log, then either the final output or an error / HIL
block. Exit code is `1` on error, `0` otherwise. Live status lines are printed
with a `[status]` prefix as the run progresses.

### Interactive CLI

```bash
python main.py --interactive     # or: python main.py -i   (also the default with no args)
```

Commands:

| Command | Effect |
|---------|--------|
| `Run datapipeline job` | run with the default input `"example input"` |
| `Run datapipeline job <text>` | run with `<text>` |
| `run pipeline with input <text>` | run with `<text>` |
| `run <text>` | run with `<text>` |
| `help` / `?` | list commands |
| `exit` / `quit` | leave |

If a run ends in HIL, the CLI enters a loop offering:

```
1. Retry now
2. Show diagnostics      (job_id, dag_name, status codes, attempted URLs, error)
3. Exit
```

### Streamlit UI

```bash
streamlit run app.py
```

Opens `http://localhost:8501`. Features:

- **Live state graph** (Graphviz), redrawn as the run streams:
  `⬜ pending  🟨 running  🟩 done  🟥 error / HIL / stopped`; dashed edges are
  conditional. A progress bar and a per‑node checklist track position.
- **Live status log**, with any HIL‑related line highlighted amber.
- **Stop button** – the graph runs on a worker thread; Stop signals it, every
  node still `running` turns 🟥, and `run_summarizer_memorizer_agent` is still
  executed so you get a summary of the partial run.
- **Summary**, **Execution memory** (step snapshot), and full **Pipeline log**.
- **HIL panel** on failure: message, suggested next actions, a diagnostics
  expander, and a **Retry** button.
- **Sidebar → "Historical Job Run Summary"** – long‑term memory, newest first,
  each entry showing `🟩`/`🟥`, timestamp, input, and one‑line summary.

---

## Memory

| Kind | Where | Lifetime | Produced by |
|------|-------|----------|-------------|
| **Log** | `state["log"]` | one run | every node appends |
| **Short‑term memory** | `state["execution_memory"]` | one run | snapshot of `log` at finalize |
| **Summary** | `state["summary"]` | one run | `_build_execution_summary` (deterministic, no LLM) |
| **Long‑term memory** | `long_term_memory.json` + `state["long_term_memory"]` | last **10** runs, on disk | `run_summarizer_memorizer_agent` |

A long‑term entry looks like:

```json
{
  "timestamp": "2026-09-07T15:17:04-04:00",
  "input": "customer orders",
  "outcome": "SUCCESS",
  "summary": "SUCCESS: final output 'customer orders'"
}
```

Writes are best‑effort — a run never fails because memory could not be written.
Path is overridable with `LONG_TERM_MEMORY_PATH`.

---

## Failure handling, retries & timeouts

| Node | Retry / poll behaviour | On give‑up |
|------|------------------------|------------|
| `api_invoker_agent` | retries the whole submitJob call **5×**, **20 s** apart, until `HTTP 200` | `api_submit_ok=False` → summarizer |
| `api_status_validator_agent` | polls jobStatus every **60 s** until `SUCCESS`; `404` keeps retrying; other HTTP error stops | `api_status_ok=False` → summarizer |
| `data_pipeline_invoker_agent` | tries several trigger URLs / payload shapes; `422` → next payload, `404/405` → legacy experimental endpoint | `dag_invocation_ok=False` → `hil_agent` |
| `data_pipeline_monitor_agent` | polls the single DAG‑run URL every **60 s**, up to **5** attempts. Terminal `FAILED` / `UPSTREAM_FAILED` / `ERROR`, or a missing `dag_run_id`, stop immediately | `dag_monitor_ok=False` → `hil_agent` |

`_http_json_request` uses a **5 s** connect/read timeout per call.

---

## Configuration

All optional. Set in `.env` (loaded by `main.py`) or the process environment.

### Retries / limits

| Variable | Default | Meaning |
|----------|---------|---------|
| `API_INVOKER_MAX_RETRIES` | `5` | submitJob attempts before giving up |
| `API_INVOKER_RETRY_DELAY` | `20` | seconds between submitJob attempts |
| `DAG_MONITOR_MAX_RETRIES` | `5` | DAG‑status polls before routing to HIL |
| `LONG_TERM_MEMORY_PATH` | `./long_term_memory.json` | long‑term memory file location |

### submitJob / jobStatus API

| Variable | Default | Meaning |
|----------|---------|---------|
| `SUBMIT_JOB_URL` | *(4 localhost:8080 candidates)* | exact submitJob URL |
| `JOB_STATUS_URL_TEMPLATE` | *(4 localhost:8080 candidates)* | template, `{job_id}` placeholder |

### Airflow

| Variable | Default | Meaning |
|----------|---------|---------|
| `AIRFLOW_API_BASE_URL` | derived from `DAGS_API_BASE_URL` | REST base, e.g. `http://host:8080/api/v1` (Airflow 2.x) or `/api/v2` (3.x) |
| `DAGS_API_BASE_URL` | `http://airflow.localhost:6563/dags/` | Airflow UI base; origin is reused to derive the REST base |
| `DAGS_TRIGGER_URL_TEMPLATE` | *(derived)* | exact trigger URL, `{dag_name}` placeholder |
| `DAGS_STATUS_URL_TEMPLATE` | *(derived)* | exact status URL, `{dag_name}` / `{dag_run_id}` placeholders |
| `AIRFLOW_AUTH_TOKEN_URL` | `http://airflow.localhost:6563/auth/token` | token endpoint |
| `AIRFLOW_TOKEN_USERNAME` / `AIRFLOW_TOKEN_PASSWORD` | `admin` / `admin` | credentials posted to the token endpoint |
| `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` | *(unset)* | if **both** set, every request also carries HTTP Basic auth |
| `AIRFLOW_DAG_CONF` | *(unset)* | JSON object merged into the DAG trigger `conf` |

> The default REST base is `…/api/v2`, which is Airflow **3.x**. On Airflow 2.x
> set `AIRFLOW_API_BASE_URL=http://<host>:<port>/api/v1`.

### LLM (optional, unused by default)

| Variable | Default | Meaning |
|----------|---------|---------|
| `OPENAI_API_KEY` | *(unset)* | needed only if a node calls `get_llm()` |
| `OPENAI_MODEL` | `gpt-4o-mini` | model for `get_llm()` |

---

## Testing

```bash
pytest
```

- `tests/test_cli.py` – CLI command and HIL‑choice parsing. Fast, no network.
- `tests/test_graph.py` – graph / node behaviour with `_http_json_request`
  monkey‑patched (retry caps, HIL routing, terminal DAG failure, long‑term
  memory trimming, live status streaming, …).

Note: `test_pipeline_runs_end_to_end` makes **real** HTTP calls (no mock) and is
therefore slow and environment‑dependent; run the rest with
`pytest --deselect tests/test_graph.py::test_pipeline_runs_end_to_end` if the
services are not up.

---

## Extending

- **New cross‑step data** → add a field to `PipelineState` in
  `orchestrator/state.py` first.
- **New / changed flow** → edit nodes and edges in `orchestrator/graph.py`. Keep
  the wiring explicit; nodes must return *partial* state updates only.
- **Real processing** → replace the HTTP calls in `orchestrator/nodes.py`. If a
  node needs an LLM, use the `get_llm()` helper (OpenAI via `langchain-openai`)
  and keep a no‑network path for tests.
- **UI** → `app.py`'s `NODES` / `EDGES` lists mirror `graph.py`; update them when
  the graph changes so the live diagram stays correct.
