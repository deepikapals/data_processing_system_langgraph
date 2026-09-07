# data_pipeline_orchestrator

A minimal LangGraph project: an orchestrator that sequentially executes a
multi-step process. This is a generic scaffold — the four pipeline steps
are placeholders for you to replace with real logic.

## Structure

```
.
├── main.py                  # entry point: builds the graph, runs it, prints results
├── orchestrator/
│   ├── state.py              # PipelineState: the shared state passed between nodes
│   ├── nodes.py               # agent node functions (api invoke/status, DAG invoke/monitor, HIL, summarizer)
│   └── graph.py                # wires the nodes into a StateGraph: api_invoker_agent -> ... -> run_summarizer_memorizer_agent
├── tests/
│   └── test_graph.py           # smoke test that runs the graph end-to-end
├── requirements.txt
├── .env.example
└── .gitignore
```

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then fill in OPENAI_API_KEY if you wire up an LLM step
```

## Run

```bash
python main.py "some input value"
```

Interactive CLI mode:

```bash
python main.py --interactive
```

Then type commands such as:

```text
Run datapipeline job

help
exit
```

The prompt "Run datapipeline job" sequentially triggers agents configured
in the LangGraph state graph.
No additional text is required after "Run datapipeline job".

This prints the log of each step and the final output. No API key is
required for the default scaffold — the steps just pass data through.

## Test

```bash
pytest
```

## Extending

- Add real logic to the step functions in `orchestrator/nodes.py`.
- To make a step call an LLM, use the `get_llm()` helper in `nodes.py`
  (already wired for OpenAI via `langchain-openai`) — see the commented
  example in `step2_process`.
- To change the flow (branching, retries, parallel steps), edit the edges
  in `orchestrator/graph.py`.
- Extend `PipelineState` in `orchestrator/state.py` as your real data
  shape takes form.
