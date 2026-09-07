# CLAUDE.md

This file gives Claude Code the minimum project context needed to be effective in this repository.

## Project Summary

- Name: data_pipeline_orchestrator
- Purpose: Minimal LangGraph scaffold for a 4-step sequential data pipeline.
- Entry point: `main.py`
- Core package: `orchestrator/`
- Tests: `tests/test_graph.py`

## Architecture

- State schema lives in `orchestrator/state.py` as `PipelineState` (TypedDict).
- Pipeline nodes live in `orchestrator/nodes.py`:
  - `step1_ingest`
  - `step2_process`
  - `step3_validate`
  - `step4_finalize`
- Graph wiring lives in `orchestrator/graph.py`.
- Current flow is linear:
  - START -> step1_ingest -> step2_process -> step3_validate -> step4_finalize -> END

## Local Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Environment notes:
- Default scaffold works without API keys.
- Optional LLM usage in nodes relies on:
  - `OPENAI_API_KEY`
  - optional `OPENAI_MODEL` (defaults to `gpt-4o-mini`)

## Run and Test

Run pipeline:

```bash
python main.py "some input value"
```

Run tests:

```bash
pytest
```

## Editing Guidelines For This Repo

- Keep the graph wiring in `orchestrator/graph.py` simple and explicit.
- Extend `PipelineState` first when adding new cross-step data.
- Node functions should return partial state updates only.
- Preserve deterministic behavior in tests (avoid network calls in default tests).
- If introducing LLM calls, keep a no-network test path and/or mock responses.

## Typical Extension Points

- Replace placeholder logic in `orchestrator/nodes.py` with real ingest/process/validate/finalize steps.
- Add branching/retries/parallelism by changing edges in `orchestrator/graph.py`.
- Add additional state fields in `orchestrator/state.py` as the pipeline evolves.

## Quick Sanity Checks After Changes

- `pytest` passes.
- Running `python main.py "hello"` still prints 4 log lines and a final output.
- No accidental dependency on API keys for baseline flow.
