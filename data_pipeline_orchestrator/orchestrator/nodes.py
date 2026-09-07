"""Node functions for the sequential pipeline.

Each function takes the current PipelineState and returns a dict of the
fields it wants to update. This is a generic scaffold: the logic in each
step is a placeholder — replace it with your real processing.

An optional OpenAI-backed helper (get_llm) is included and wired into
step2 as a commented-out example, so you can turn any step into an LLM
call once you set OPENAI_API_KEY in .env.
"""
from __future__ import annotations

import json
import os
import time
import base64
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from orchestrator.state import PipelineState

# Long-term memory: a rolling JSON list of the last N run summaries with
# timestamps, persisted across process restarts.
LONG_TERM_MEMORY_LIMIT = 10

# Terminal DAG-run states that mean "stop polling, hand to a human".
_DAG_TERMINAL_FAILURE_STATES = {"FAILED", "UPSTREAM_FAILED", "ERROR"}


def _long_term_memory_path() -> Path:
    configured = os.environ.get("LONG_TERM_MEMORY_PATH")
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parent.parent / "long_term_memory.json"


def _load_long_term_memory() -> list[dict[str, Any]]:
    try:
        data = json.loads(_long_term_memory_path().read_text("utf-8"))
        return data if isinstance(data, list) else []
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return []


def _append_long_term_memory(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """Append `entry`, keep only the last LONG_TERM_MEMORY_LIMIT, persist, return the list."""
    history = _load_long_term_memory()
    history.append(entry)
    history = history[-LONG_TERM_MEMORY_LIMIT:]
    try:
        _long_term_memory_path().write_text(json.dumps(history, indent=2), "utf-8")
    except OSError:
        pass  # best-effort; a run must not fail because memory could not be written
    return history


def _emit_status(state: PipelineState, message: str) -> None:
    """Send a live status update when a callback is provided by the caller."""
    callback = state.get("status_callback")
    if callback is None:
        return
    callback(message)


@lru_cache(maxsize=1)
def get_llm():
    """Lazily build a ChatOpenAI client, only when a node actually needs one.

    Kept separate from the node functions so nodes that don't need an LLM
    never pay the import/credential cost, and so tests can run without an
    OPENAI_API_KEY set.
    """
    from langchain_openai import ChatOpenAI

    return ChatOpenAI(
        model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        temperature=0,
    )


def _http_json_request(
    method: str,
    url: str,
    payload_obj: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
) -> tuple[int, Any]:
    """Issue an HTTP request and parse a JSON response body."""
    payload = None
    headers: dict[str, str] = {}

    username = os.environ.get("AIRFLOW_USERNAME")
    password = os.environ.get("AIRFLOW_PASSWORD")
    if username is not None and password is not None:
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        headers["Authorization"] = f"Basic {token}"

    if method.upper() in {"POST", "PUT", "PATCH"}:
        # Airflow REST endpoints expect a JSON payload for mutating operations.
        payload = json.dumps(payload_obj or {}).encode("utf-8")
        headers["Content-Type"] = "application/json"

    if extra_headers:
        headers.update(extra_headers)

    request = Request(url=url, method=method, data=payload, headers=headers)
    with urlopen(request, timeout=5) as response:
        status_code = response.getcode()
        raw_body = response.read().decode("utf-8").strip()
        body = json.loads(raw_body) if raw_body else {}
        return status_code, body


def _submit_job_urls() -> list[str]:
    """Build submitJob URL candidates, honoring SUBMIT_JOB_URL when set."""
    configured = os.environ.get("SUBMIT_JOB_URL")
    if configured:
        return [configured]

    return [
        "http://localhost:8080/api/submitJob/",
        "http://localhost:8080/api/submitJob",
        "http://localhost:8080/submitJob/",
        "http://localhost:8080/submitJob",
    ]


def _job_status_urls(job_id: str) -> list[str]:
    """Build jobStatus URL candidates, honoring JOB_STATUS_URL_TEMPLATE when set."""
    configured = os.environ.get("JOB_STATUS_URL_TEMPLATE")
    if configured:
        return [configured.format(job_id=job_id)]

    return [
        f"http://localhost:8080/api/jobStatus/{job_id}",
        f"http://localhost:8080/api/jobStatus/{job_id}/",
        f"http://localhost:8080/jobStatus/{job_id}",
        f"http://localhost:8080/jobStatus/{job_id}/",
    ]


def _airflow_api_base_url() -> str:
    """Resolve Airflow REST API base URL.

    If AIRFLOW_API_BASE_URL is not set, derive it from the Airflow UI base
    URL so users can configure only the /dags/ address.
    """
    configured_api_base = os.environ.get("AIRFLOW_API_BASE_URL")
    if configured_api_base:
        return configured_api_base.rstrip("/")

    dags_ui_base = os.environ.get("DAGS_API_BASE_URL", "http://airflow.localhost:6563/dags/")
    split = urlsplit(dags_ui_base)
    origin = f"{split.scheme}://{split.netloc}"
    return f"{origin}/api/v2"


def _dag_trigger_urls(dag_name: str) -> list[str]:
    """Build DAG trigger URL candidates for current and legacy Airflow APIs."""
    configured = os.environ.get("DAGS_TRIGGER_URL_TEMPLATE")
    if configured:
        return [configured.format(dag_name=dag_name)]

    base = _airflow_api_base_url()
    split = urlsplit(base)
    origin = f"{split.scheme}://{split.netloc}"

    urls = [
        f"{base}/dags/{dag_name}/dagRuns",
        f"{base}/dags/{dag_name}/dagRuns/",
        # Legacy Airflow experimental endpoint fallback.
        f"{origin}/api/experimental/dags/{dag_name}/dag_runs",
    ]

    # Dedupe while preserving order.
    seen: set[str] = set()
    deduped: list[str] = []
    for item in urls:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _dag_status_urls(dag_name: str, dag_run_id: str | None) -> list[str]:
    """Single DAG status URL: GET {base}/dags/{dag_name}/dagRuns/{dag_run_id}.

    `dag_run_id` comes from data_pipeline_invoker_agent. Returns an empty list
    when it is missing so the monitor fails fast instead of guessing.
    """
    configured = os.environ.get("DAGS_STATUS_URL_TEMPLATE")
    if configured:
        return [configured.format(dag_name=dag_name, dag_run_id=dag_run_id or "")]

    if not dag_run_id:
        return []

    base = _airflow_api_base_url()  # default http://airflow.localhost:6563/api/v2
    return [f"{base}/dags/{dag_name}/dagRuns/{dag_run_id}"]


def _airflow_token_url() -> str:
    """Resolve Airflow auth token endpoint URL."""
    return os.environ.get("AIRFLOW_AUTH_TOKEN_URL", "http://airflow.localhost:6563/auth/token")


def _dag_trigger_payload_candidates(job_id: str | None) -> list[dict[str, Any]]:
    """Return compatible payload candidates for Airflow DAG trigger APIs."""
    conf_payload: dict[str, Any] = {}
    dag_conf_raw = os.environ.get("AIRFLOW_DAG_CONF")
    if dag_conf_raw:
        try:
            parsed = json.loads(dag_conf_raw)
            if isinstance(parsed, dict):
                conf_payload = parsed
        except json.JSONDecodeError:
            pass

    run_id_seed = (job_id or str(int(time.time()))).replace("-", "_")
    generated_run_id = f"manual__{run_id_seed}"
    logical_date = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    candidates: list[dict[str, Any]] = [
        {},
        {"conf": conf_payload},
        {"dag_run_id": generated_run_id, "conf": conf_payload},
        {"dag_run_id": generated_run_id, "logical_date": logical_date, "conf": conf_payload},
    ]

    # Dedupe while preserving order.
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for item in candidates:
        marker = json.dumps(item, sort_keys=True)
        if marker not in seen:
            seen.add(marker)
            deduped.append(item)
    return deduped


def _resolve_airflow_bearer_token(state: PipelineState, log: list[str]) -> str | None:
    """Return bearer token from state or fetch a new one from Airflow auth endpoint."""
    existing_token = state.get("airflow_bearer_token")
    if existing_token:
        return existing_token

    token_url = _airflow_token_url()
    username = os.environ.get("AIRFLOW_TOKEN_USERNAME", "admin")
    password = os.environ.get("AIRFLOW_TOKEN_PASSWORD", "admin")

    log.append(f"data_pipeline_invoker_agent: requesting bearer token via POST {token_url}")
    try:
        status_code, payload = _http_json_request(
            "POST",
            token_url,
            payload_obj={"username": username, "password": password},
        )
        if status_code not in {200, 201}:
            log.append(f"data_pipeline_invoker_agent: token endpoint returned HTTP {status_code}")
            return None
        if not isinstance(payload, dict):
            log.append("data_pipeline_invoker_agent: token response payload is not a JSON object")
            return None
        token = payload.get("access_token") or payload.get("token") or payload.get("bearer_token")
        if not token:
            log.append("data_pipeline_invoker_agent: token response missing access_token")
            return None
        return str(token)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        log.append(f"data_pipeline_invoker_agent: token request failed: {exc}")
        return None


def api_invoker_agent(state: PipelineState) -> dict[str, Any]:
    """Call submitJob API and persist status/job metadata in state.

    Retries the whole call up to API_INVOKER_MAX_RETRIES times (default 5),
    waiting API_INVOKER_RETRY_DELAY seconds (default 20) between attempts,
    until submitJob returns HTTP 200.
    """
    submit_urls = _submit_job_urls()
    log = list(state.get("log", []))
    max_retries = int(os.environ.get("API_INVOKER_MAX_RETRIES", "5"))
    retry_delay = int(os.environ.get("API_INVOKER_RETRY_DELAY", "20"))

    for attempt in range(1, max_retries + 1):
        last_http_error: HTTPError | None = None
        last_other_error: Exception | None = None

        for submit_url in submit_urls:
            log.append(f"api_invoker_agent: calling POST {submit_url} (attempt {attempt}/{max_retries})")
            _emit_status(state, f"api_invoker_agent calling POST {submit_url} (attempt {attempt}/{max_retries})")
            try:
                status_code, payload = _http_json_request("POST", submit_url)
                job_id = payload.get("jobId")
                job_submit_status = payload.get("status")

                log.append(f"api_invoker_agent: status_code={status_code}, job_id={job_id!r}")
                _emit_status(state, f"api_invoker_agent received HTTP {status_code}; job_id={job_id}")
                if status_code == 200:
                    return {
                        "api_submit_ok": True,
                        "api_submit_status_code": status_code,
                        "job_id": job_id,
                        "job_submit_status": job_submit_status,
                        "log": log,
                    }
                last_http_error = None
                last_other_error = RuntimeError(f"submitJob returned HTTP {status_code}")
                break
            except HTTPError as exc:
                last_http_error = exc
                log.append(f"api_invoker_agent: HTTPError status_code={exc.code} at {submit_url}")
                _emit_status(state, f"api_invoker_agent received HTTP {exc.code} at {submit_url}")
                if exc.code not in (404, 405):
                    break
            except (URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_other_error = exc
                log.append(f"api_invoker_agent: request failed at {submit_url}: {exc}")
                _emit_status(state, f"api_invoker_agent request failed at {submit_url}: {exc}")
                break

        if attempt < max_retries:
            log.append(f"api_invoker_agent: submitJob failed, retrying in {retry_delay} seconds")
            _emit_status(state, f"api_invoker_agent submitJob failed, retrying in {retry_delay} seconds")
            time.sleep(retry_delay)

    if last_http_error is not None:
        return {
            "api_submit_ok": False,
            "api_submit_status_code": last_http_error.code,
            "error": f"submitJob API failed with HTTP {last_http_error.code} after {max_retries} attempts",
            "log": log,
        }

    return {
        "api_submit_ok": False,
        "error": f"submitJob API request failed after {max_retries} attempts: {last_other_error}",
        "log": log,
    }


def api_status_validator_agent(state: PipelineState) -> dict[str, Any]:
    """Poll job status every minute until the API reports SUCCESS."""
    job_id = state.get("job_id")
    log = list(state.get("log", []))

    if not job_id:
        log.append("api_status_validator_agent: skipped (missing job_id)")
        _emit_status(state, "api_status_validator_agent skipped: missing job_id")
        return {
            "api_status_ok": False,
            "error": state.get("error") or "Missing job_id for status validation",
            "log": log,
        }

    status_urls = _job_status_urls(job_id)
    while True:
        last_http_error: HTTPError | None = None
        pending_status: str | None = None

        for status_url in status_urls:
            log.append(f"api_status_validator_agent: calling GET {status_url}")
            _emit_status(state, f"api_status_validator_agent calling GET {status_url}")
            try:
                status_code, payload = _http_json_request("GET", status_url)
                api_job_status = payload.get("status")
                pending_status = api_job_status

                log.append(
                    "api_status_validator_agent: "
                    f"status_code={status_code}, api_job_status={api_job_status!r}"
                )
                _emit_status(
                    state,
                    f"api_status_validator_agent received HTTP {status_code}; status={api_job_status}",
                )

                if status_code == 200 and api_job_status == "SUCCESS":
                    _emit_status(state, "api_status_validator_agent completed with SUCCESS")
                    return {
                        "api_status_ok": True,
                        "api_status_code": status_code,
                        "api_job_status": api_job_status,
                        "log": log,
                    }

                break
            except HTTPError as exc:
                last_http_error = exc
                log.append(f"api_status_validator_agent: HTTPError status_code={exc.code} at {status_url}")
                _emit_status(state, f"api_status_validator_agent received HTTP {exc.code} at {status_url}")
                if exc.code != 404:
                    return {
                        "api_status_ok": False,
                        "api_status_code": exc.code,
                        "error": f"jobStatus API failed with HTTP {exc.code}",
                        "log": log,
                    }
            except (URLError, TimeoutError, json.JSONDecodeError) as exc:
                log.append(f"api_status_validator_agent: request failed at {status_url}: {exc}")
                _emit_status(state, f"api_status_validator_agent request failed at {status_url}: {exc}")
                return {
                    "api_status_ok": False,
                    "error": f"jobStatus API request failed: {exc}",
                    "log": log,
                }

        if last_http_error is not None and pending_status is None:
            log.append("api_status_validator_agent: all status URLs returned 404, retrying in 60 seconds")
            _emit_status(state, "api_status_validator_agent all status URLs returned 404, retrying")

        log.append("api_status_validator_agent: status not SUCCESS, waiting 60 seconds before retry")
        _emit_status(state, "api_status_validator_agent status not SUCCESS, waiting 60 seconds")
        time.sleep(60)


def data_pipeline_invoker_agent(state: PipelineState) -> dict[str, Any]:
    """Invoke downstream DAG API after status validation is SUCCESS."""
    log = list(state.get("log", []))
    job_id = state.get("job_id")
    dag_name = "random_number_check_dag"
    dag_urls = _dag_trigger_urls(dag_name)
    payload_candidates = _dag_trigger_payload_candidates(job_id)
    last_http_error: HTTPError | None = None
    last_other_error: Exception | None = None
    airflow_bearer_token = _resolve_airflow_bearer_token(state, log)

    if not airflow_bearer_token:
        _emit_status(state, "data_pipeline_invoker_agent failed to acquire bearer token")
        return {
            "data_pipeline_invoked": True,
            "dag_name": dag_name,
            "dag_trigger_last_url": dag_urls[-1] if dag_urls else None,
            "dag_trigger_attempt_urls": dag_urls,
            "dag_invocation_ok": False,
            "error": "Failed to acquire Airflow bearer token from /auth/token",
            "log": log,
        }

    auth_headers = {"Authorization": f"Bearer {airflow_bearer_token}"}

    for dag_url in dag_urls:
        for payload_obj in payload_candidates:
            log.append(
                f"data_pipeline_invoker_agent: invoking dag={dag_name!r} for job_id={job_id!r} via POST {dag_url}"
            )
            _emit_status(state, f"data_pipeline_invoker_agent calling POST {dag_url} for dag={dag_name}")
            try:
                status_code, payload = _http_json_request(
                    "POST",
                    dag_url,
                    payload_obj=payload_obj,
                    extra_headers=auth_headers,
                )
                ok = status_code in {200, 201}
                dag_run_id = payload.get("dag_run_id") or payload.get("run_id")
                log.append(f"data_pipeline_invoker_agent: status_code={status_code}")
                _emit_status(
                    state,
                    f"data_pipeline_invoker_agent received HTTP {status_code}; dag_run_id={dag_run_id}",
                )
                return {
                    "data_pipeline_invoked": True,
                    "dag_name": dag_name,
                    "dag_run_id": dag_run_id,
                    "airflow_bearer_token": airflow_bearer_token,
                    "dag_trigger_last_url": dag_url,
                    "dag_trigger_attempt_urls": dag_urls,
                    "dag_invocation_ok": ok,
                    "dag_invocation_status_code": status_code,
                    "log": log,
                }
            except HTTPError as exc:
                last_http_error = exc
                log.append(f"data_pipeline_invoker_agent: HTTPError status_code={exc.code} at {dag_url}")
                _emit_status(state, f"data_pipeline_invoker_agent received HTTP {exc.code} at {dag_url}")
                # For 422, retry with another valid payload shape before failing.
                if exc.code == 422:
                    continue
                if exc.code in (404, 405):
                    break
                break
            except (URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_other_error = exc
                log.append(f"data_pipeline_invoker_agent: request failed at {dag_url}: {exc}")
                _emit_status(state, f"data_pipeline_invoker_agent request failed at {dag_url}: {exc}")
                break

    if last_http_error is not None:
        return {
            "data_pipeline_invoked": True,
            "dag_name": dag_name,
            "airflow_bearer_token": airflow_bearer_token,
            "dag_trigger_last_url": dag_urls[-1] if dag_urls else None,
            "dag_trigger_attempt_urls": dag_urls,
            "dag_invocation_ok": False,
            "dag_invocation_status_code": last_http_error.code,
            "error": f"DAG API failed with HTTP {last_http_error.code}",
            "log": log,
        }

    return {
        "data_pipeline_invoked": True,
        "dag_name": dag_name,
        "airflow_bearer_token": airflow_bearer_token,
        "dag_trigger_last_url": dag_urls[-1] if dag_urls else None,
        "dag_trigger_attempt_urls": dag_urls,
        "dag_invocation_ok": False,
        "error": f"DAG API request failed: {last_other_error}",
        "log": log,
    }


def data_pipeline_monitor_agent(state: PipelineState) -> dict[str, Any]:
    """Poll the DAG status every minute until it reports SUCCESS.

    Gives up after DAG_MONITOR_MAX_RETRIES attempts (default 5) and returns a
    non-OK result, which routes the graph to hil_agent.
    """
    log = list(state.get("log", []))
    dag_name = state.get("dag_name") or "random_number_check_dag"
    dag_run_id = state.get("dag_run_id")
    airflow_bearer_token = state.get("airflow_bearer_token")
    auth_headers = {"Authorization": f"Bearer {airflow_bearer_token}"} if airflow_bearer_token else None
    status_urls = _dag_status_urls(dag_name, dag_run_id)
    if not status_urls:
        log.append("data_pipeline_monitor_agent: no dag_run_id from data_pipeline_invoker_agent")
        _emit_status(state, "data_pipeline_monitor_agent missing dag_run_id, routing to HIL")
        return {
            "dag_monitor_ok": False,
            "error": "DAG monitoring skipped: no dag_run_id from data_pipeline_invoker_agent",
            "log": log,
        }
    max_retries = int(os.environ.get("DAG_MONITOR_MAX_RETRIES", "5"))
    attempt = 0

    while True:
        attempt += 1
        last_http_error: HTTPError | None = None
        observed_status: str | None = None

        for status_url in status_urls:
            log.append(f"data_pipeline_monitor_agent: calling GET {status_url}")
            _emit_status(state, f"data_pipeline_monitor_agent calling GET {status_url}")
            try:
                status_code, payload = _http_json_request("GET", status_url, extra_headers=auth_headers)
                dag_status = None
                if isinstance(payload, dict):
                    dag_status = payload.get("status") or payload.get("state")

                if isinstance(payload, list) and payload:
                    latest = payload[-1]
                    if isinstance(latest, dict):
                        dag_status = latest.get("state") or latest.get("status")
                        dag_run_id = dag_run_id or latest.get("dag_run_id") or latest.get("run_id")
                        if dag_run_id:
                            status_urls = _dag_status_urls(dag_name, dag_run_id)

                if isinstance(payload, dict) and dag_status is None and isinstance(payload.get("dag_runs"), list) and payload["dag_runs"]:
                    latest = payload["dag_runs"][0]
                    if isinstance(latest, dict):
                        dag_status = latest.get("state") or latest.get("status")
                        dag_run_id = dag_run_id or latest.get("dag_run_id") or latest.get("run_id")
                        if dag_run_id:
                            status_urls = _dag_status_urls(dag_name, dag_run_id)
                observed_status = dag_status

                log.append(
                    "data_pipeline_monitor_agent: "
                    f"status_code={status_code}, dag_status={dag_status!r}"
                )
                _emit_status(
                    state,
                    f"data_pipeline_monitor_agent received HTTP {status_code}; status={dag_status}",
                )

                normalized_status = str(dag_status).upper() if dag_status is not None else ""
                if status_code == 200 and normalized_status == "SUCCESS":
                    _emit_status(state, "data_pipeline_monitor_agent completed with SUCCESS")
                    return {
                        "dag_monitor_ok": True,
                        "dag_monitor_status": normalized_status,
                        "dag_monitor_status_code": status_code,
                        "dag_run_id": dag_run_id,
                        "log": log,
                    }

                if normalized_status in _DAG_TERMINAL_FAILURE_STATES:
                    log.append(
                        f"data_pipeline_monitor_agent: DAG run reported {normalized_status}, routing to HIL"
                    )
                    _emit_status(
                        state,
                        f"data_pipeline_monitor_agent DAG reported {normalized_status}, routing to HIL",
                    )
                    return {
                        "dag_monitor_ok": False,
                        "dag_monitor_status": normalized_status,
                        "dag_monitor_status_code": status_code,
                        "dag_run_id": dag_run_id,
                        "error": f"DAG {dag_name} run {dag_run_id} ended as {normalized_status}",
                        "log": log,
                    }

                break
            except HTTPError as exc:
                last_http_error = exc
                log.append(f"data_pipeline_monitor_agent: HTTPError status_code={exc.code} at {status_url}")
                _emit_status(state, f"data_pipeline_monitor_agent received HTTP {exc.code} at {status_url}")
                if exc.code != 404:
                    return {
                        "dag_monitor_ok": False,
                        "dag_monitor_status_code": exc.code,
                        "error": f"DAG status API failed with HTTP {exc.code}",
                        "log": log,
                    }
            except (URLError, TimeoutError, json.JSONDecodeError) as exc:
                log.append(f"data_pipeline_monitor_agent: request failed at {status_url}: {exc}")
                _emit_status(state, f"data_pipeline_monitor_agent request failed at {status_url}: {exc}")
                return {
                    "dag_monitor_ok": False,
                    "error": f"DAG status API request failed: {exc}",
                    "log": log,
                }

        if last_http_error is not None and observed_status is None:
            log.append("data_pipeline_monitor_agent: all status URLs returned 404, retrying in 60 seconds")
            _emit_status(state, "data_pipeline_monitor_agent all status URLs returned 404, retrying")

        if attempt >= max_retries:
            log.append(
                f"data_pipeline_monitor_agent: gave up after {attempt} attempts without SUCCESS"
            )
            _emit_status(
                state, f"data_pipeline_monitor_agent gave up after {attempt} attempts, routing to HIL"
            )
            return {
                "dag_monitor_ok": False,
                "dag_monitor_status": str(observed_status).upper() if observed_status else None,
                "dag_monitor_status_code": last_http_error.code if last_http_error else None,
                "error": f"DAG monitoring did not reach SUCCESS after {attempt} attempts",
                "log": log,
            }

        log.append("data_pipeline_monitor_agent: status not SUCCESS, waiting 60 seconds before retry")
        _emit_status(state, "data_pipeline_monitor_agent status not SUCCESS, waiting 60 seconds")
        time.sleep(60)


def hil_agent(state: PipelineState) -> dict[str, Any]:
    """Human-in-the-loop fallback when DAG invocation does not return HTTP 200."""
    log = list(state.get("log", []))
    status_code = state.get("dag_invocation_status_code")
    dag_name = state.get("dag_name") or "random_number_check_dag"
    job_id = state.get("job_id")
    last_url = state.get("dag_trigger_last_url")
    attempted_urls = state.get("dag_trigger_attempt_urls") or []
    endpoint_hint = ""
    if status_code == 405:
        endpoint_hint = (
            " Hint: HTTP 405 usually means the URL is not a trigger endpoint. "
            "Use POST /api/v2/dags/<dag_id>/dagRuns (or legacy /api/experimental/.../dag_runs)."
        )
    hil_message = (
        "HIL required: DAG invocation needs manual review. "
        f"dag={dag_name}, job_id={job_id}, status_code={status_code}, last_url={last_url}."
        f"{endpoint_hint}"
    )

    log.append(f"hil_agent: triggered due to dag invocation non-200 status={status_code!r}")
    log.append(f"hil_agent: {hil_message}")
    _emit_status(state, f"hil_agent triggered due to dag invocation status={status_code}")
    _emit_status(state, "hil_agent requesting human action: verify DAG API, then retry pipeline")

    return {
        "hil_required": True,
        "hil_message": hil_message,
        "dag_trigger_last_url": last_url,
        "dag_trigger_attempt_urls": attempted_urls,
        "error": state.get("error") or "HIL required: DAG invocation did not return HTTP 200",
        "log": log,
    }


def _build_execution_summary(state: PipelineState, memory: list[str]) -> str:
    """Turn the run's state + log into a short human-readable summary."""
    lines: list[str] = []
    lines.append(f'Pipeline run for input {state.get("input")!r}.')

    submit_code = state.get("api_submit_status_code")
    if state.get("api_submit_ok"):
        lines.append(
            f"- submitJob accepted the job (HTTP {submit_code}); "
            f"job_id={state.get('job_id')}, status={state.get('job_submit_status')}."
        )
    elif submit_code is not None or "api_invoker_agent" in " ".join(memory):
        lines.append(f"- submitJob did not succeed (HTTP {submit_code}).")

    if state.get("api_status_ok"):
        lines.append(f"- Job status polling reached {state.get('api_job_status')}.")
    elif state.get("job_id") and state.get("api_status_ok") is False:
        lines.append("- Job status polling did not reach SUCCESS.")

    if state.get("data_pipeline_invoked"):
        if state.get("dag_invocation_ok"):
            lines.append(
                f"- DAG {state.get('dag_name')} triggered "
                f"(HTTP {state.get('dag_invocation_status_code')}); "
                f"dag_run_id={state.get('dag_run_id')}."
            )
        else:
            lines.append(
                f"- DAG {state.get('dag_name')} trigger failed "
                f"(HTTP {state.get('dag_invocation_status_code')})."
            )

    if state.get("dag_monitor_ok"):
        lines.append(f"- DAG monitoring finished as {state.get('dag_monitor_status')}.")
    elif state.get("dag_monitor_ok") is False:
        lines.append("- DAG monitoring did not reach SUCCESS.")

    if state.get("hil_required"):
        lines.append(f"- HUMAN-IN-THE-LOOP required: {state.get('hil_message')}")

    if state.get("error"):
        lines.append(f"- Ended with an error: {state.get('error')}")
        lines.append("Outcome: FAILED — see the error above and the log for details.")
    else:
        lines.append(f"Outcome: SUCCESS — final output is {state.get('input')!r}.")

    lines.append(f"({len(memory)} steps recorded in execution memory.)")
    return "\n".join(lines)


def run_summarizer_memorizer_agent(state: PipelineState) -> dict[str, Any]:
    """Assemble the final output, keep a short-term memory of the run, and
    write a human-readable summary of what happened."""
    data = state.get("input")
    log = list(state.get("log", []))
    log.append(f"run_summarizer_memorizer_agent: finalizing {data!r}")

    # Short-term memory: a snapshot of every log line the graph produced.
    execution_memory = list(log)
    summary = _build_execution_summary(state, execution_memory)
    log.append("run_summarizer_memorizer_agent: summary generated")

    # Long-term memory: a one-line summary of this run, dated, kept for 10 runs.
    outcome = "FAILED" if state.get("error") else "SUCCESS"
    note = state.get("error") or state.get("hil_message") or f"final output {data!r}"
    entry = {
        "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
        "input": data,
        "outcome": outcome,
        "summary": f"{outcome}: {str(note)[:200]}",
    }
    long_term_memory = _append_long_term_memory(entry)
    log.append(
        f"run_summarizer_memorizer_agent: long-term memory updated ({len(long_term_memory)} runs)"
    )

    return {
        "final_output": data,
        "execution_memory": execution_memory,
        "summary": summary,
        "long_term_memory": long_term_memory,
        "log": log,
    }
