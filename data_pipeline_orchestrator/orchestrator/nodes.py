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
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from orchestrator.state import PipelineState


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


def step1_ingest(state: PipelineState) -> dict[str, Any]:
    """Placeholder: take the raw input and normalize/validate it."""
    raw = state.get("input")
    log = list(state.get("log", []))
    log.append(f"step1_ingest: received input={raw!r}")

    # TODO: replace with real ingestion logic (e.g. load a file, hit an API).
    output = raw

    return {"step1_output": output, "log": log}


def step2_process(state: PipelineState) -> dict[str, Any]:
    """Placeholder: transform the ingested data."""
    data = state.get("step1_output")
    log = list(state.get("log", []))
    log.append(f"step2_process: processing {data!r}")

    # Example of how you'd swap this for a real LLM call:
    #
    #   llm = get_llm()
    #   response = llm.invoke(f"Summarize this: {data}")
    #   output = response.content
    #
    # For the scaffold we just pass the data through unchanged.
    output = data

    return {"step2_output": output, "log": log}


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
    """Build DAG status URL candidates for Airflow REST API endpoints."""
    configured = os.environ.get("DAGS_STATUS_URL_TEMPLATE")
    if configured:
        return [configured.format(dag_name=dag_name, dag_run_id=dag_run_id or "")]

    base = _airflow_api_base_url()
    split = urlsplit(base)
    origin = f"{split.scheme}://{split.netloc}"
    if dag_run_id:
        return [
            f"{base}/dags/{dag_name}/dagRuns/{dag_run_id}",
            f"{base}/dags/{dag_name}/dagRuns",
            f"{origin}/api/experimental/dags/{dag_name}/dag_runs/{dag_run_id}",
            f"{origin}/api/experimental/dags/{dag_name}/dag_runs",
        ]
    return [
        f"{base}/dags/{dag_name}/dagRuns",
        f"{origin}/api/experimental/dags/{dag_name}/dag_runs",
    ]


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
    """Call submitJob API and persist status/job metadata in state."""
    submit_urls = _submit_job_urls()
    log = list(state.get("log", []))
    last_http_error: HTTPError | None = None
    last_other_error: Exception | None = None

    for submit_url in submit_urls:
        log.append(f"api_invoker_agent: calling POST {submit_url}")
        _emit_status(state, f"api_invoker_agent calling POST {submit_url}")
        try:
            status_code, payload = _http_json_request("POST", submit_url)
            job_id = payload.get("jobId")
            job_submit_status = payload.get("status")

            log.append(f"api_invoker_agent: status_code={status_code}, job_id={job_id!r}")
            _emit_status(state, f"api_invoker_agent received HTTP {status_code}; job_id={job_id}")
            return {
                "api_submit_ok": status_code == 200,
                "api_submit_status_code": status_code,
                "job_id": job_id,
                "job_submit_status": job_submit_status,
                "log": log,
            }
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

    if last_http_error is not None:
        return {
            "api_submit_ok": False,
            "api_submit_status_code": last_http_error.code,
            "error": f"submitJob API failed with HTTP {last_http_error.code}",
            "log": log,
        }

    return {
        "api_submit_ok": False,
        "error": f"submitJob API request failed: {last_other_error}",
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
    """Poll the DAG status every minute until it reports SUCCESS."""
    log = list(state.get("log", []))
    dag_name = state.get("dag_name") or "random_number_check_dag"
    dag_run_id = state.get("dag_run_id")
    airflow_bearer_token = state.get("airflow_bearer_token")
    auth_headers = {"Authorization": f"Bearer {airflow_bearer_token}"} if airflow_bearer_token else None
    status_urls = _dag_status_urls(dag_name, dag_run_id)

    while True:
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


def step3_validate(state: PipelineState) -> dict[str, Any]:
    """Placeholder: validate the processed data before final output."""
    data = state.get("step2_output")
    log = list(state.get("log", []))
    log.append(f"step3_validate: validating {data!r}")

    # TODO: replace with real validation logic; set state["error"] on failure.
    output = data

    return {"step3_output": output, "log": log}


def step4_finalize(state: PipelineState) -> dict[str, Any]:
    """Placeholder: assemble the final output of the pipeline."""
    data = state.get("step3_output")
    log = list(state.get("log", []))
    log.append(f"step4_finalize: finalizing {data!r}")

    return {"final_output": data, "log": log}
