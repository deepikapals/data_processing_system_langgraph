"""Smoke test for the pipeline graph. Runs entirely without an LLM/API key,
since the default scaffold steps are plain pass-through placeholders.
"""
from urllib.error import HTTPError

from orchestrator.graph import run_pipeline
from orchestrator import nodes


def test_pipeline_runs_end_to_end():
    result = run_pipeline("hello")

    assert result["final_output"] == "hello"
    assert result["step1_output"] == "hello"
    assert result["step2_output"] == "hello"
    assert result["step3_output"] == "hello"
    assert len(result["log"]) >= 5
    assert "step1_ingest" in result["log"][0]
    assert any("api_invoker_agent" in line for line in result["log"])
    assert "step4_finalize" in result["log"][-1]


def test_calls_api_status_validator_after_submit_200(monkeypatch):
    def fake_http_json_request(method: str, url: str, payload_obj=None, extra_headers=None):
        if method == "POST" and url == "http://airflow.localhost:6563/auth/token":
            return 200, {"access_token": "token-123"}
        if method == "POST" and url == "http://localhost:8080/api/submitJob/":
            return 200, {"jobId": "job-123", "status": "PENDING"}
        if method == "GET" and url.rstrip("/") == "http://localhost:8080/api/jobStatus/job-123":
            return 200, {"status": "SUCCESS"}
        if method == "POST" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns":
            return 200, {"dag_run_id": "manual__1", "state": "queued"}
        if method == "GET" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns/manual__1":
            return 200, {"dag_run_id": "manual__1", "state": "success"}
        raise AssertionError(f"Unexpected HTTP call: {method} {url}")

    monkeypatch.setattr(nodes, "_http_json_request", fake_http_json_request)

    result = run_pipeline("hello")

    assert result["api_submit_ok"] is True
    assert result["job_id"] == "job-123"
    assert result["api_status_ok"] is True
    assert result["api_job_status"] == "SUCCESS"
    assert result["data_pipeline_invoked"] is True
    assert result["dag_name"] == "random_number_check_dag"
    assert result["dag_run_id"] == "manual__1"
    assert result["dag_invocation_ok"] is True
    assert result["dag_invocation_status_code"] == 200
    assert result["dag_monitor_ok"] is True
    assert result["dag_monitor_status"] == "SUCCESS"
    assert result["dag_monitor_status_code"] == 200
    assert any("api_status_validator_agent" in line for line in result["log"])
    assert any("data_pipeline_invoker_agent" in line for line in result["log"])
    assert any("data_pipeline_monitor_agent" in line for line in result["log"])


def test_status_validator_polls_until_success(monkeypatch):
    calls = {"get": 0, "sleep": 0}

    def fake_http_json_request(method: str, url: str, payload_obj=None, extra_headers=None):
        if method == "POST" and url == "http://airflow.localhost:6563/auth/token":
            return 200, {"access_token": "token-123"}
        if method == "POST" and url == "http://localhost:8080/api/submitJob/":
            return 200, {"jobId": "job-123", "status": "PENDING"}
        if method == "GET" and url.rstrip("/") == "http://localhost:8080/api/jobStatus/job-123":
            calls["get"] += 1
            if calls["get"] == 1:
                return 200, {"status": "PENDING"}
            return 200, {"status": "SUCCESS"}
        if method == "POST" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns":
            return 200, {"dag_run_id": "manual__1", "state": "queued"}
        if method == "GET" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns/manual__1":
            return 200, {"dag_run_id": "manual__1", "state": "success"}
        raise AssertionError(f"Unexpected HTTP call: {method} {url}")

    def fake_sleep(seconds: int):
        assert seconds == 60
        calls["sleep"] += 1

    monkeypatch.setattr(nodes, "_http_json_request", fake_http_json_request)
    monkeypatch.setattr(nodes.time, "sleep", fake_sleep)

    result = run_pipeline("hello")

    assert calls["get"] == 2
    assert calls["sleep"] == 1
    assert result["api_status_ok"] is True
    assert result["api_job_status"] == "SUCCESS"
    assert result["data_pipeline_invoked"] is True
    assert result["dag_invocation_ok"] is True
    assert result["dag_monitor_ok"] is True


def test_run_pipeline_emits_live_status_messages(monkeypatch):
    calls = {"get": 0}

    def fake_http_json_request(method: str, url: str, payload_obj=None, extra_headers=None):
        if method == "POST" and url == "http://airflow.localhost:6563/auth/token":
            return 200, {"access_token": "token-123"}
        if method == "POST" and url == "http://localhost:8080/api/submitJob/":
            return 200, {"jobId": "job-123", "status": "PENDING"}
        if method == "GET" and url.rstrip("/") == "http://localhost:8080/api/jobStatus/job-123":
            calls["get"] += 1
            if calls["get"] == 1:
                return 200, {"status": "PENDING"}
            return 200, {"status": "SUCCESS"}
        if method == "POST" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns":
            return 200, {"dag_run_id": "manual__1", "state": "queued"}
        if method == "GET" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns/manual__1":
            return 200, {"dag_run_id": "manual__1", "state": "success"}
        raise AssertionError(f"Unexpected HTTP call: {method} {url}")

    status_messages: list[str] = []

    def status_callback(message: str):
        status_messages.append(message)

    monkeypatch.setattr(nodes, "_http_json_request", fake_http_json_request)
    monkeypatch.setattr(nodes.time, "sleep", lambda _: None)

    result = run_pipeline("hello", status_callback=status_callback)

    assert result["api_status_ok"] is True
    assert any("api_invoker_agent calling POST" in msg for msg in status_messages)
    assert any("waiting 60 seconds" in msg for msg in status_messages)
    assert any("completed with SUCCESS" in msg for msg in status_messages)
    assert any("data_pipeline_invoker_agent calling POST" in msg for msg in status_messages)
    assert any("data_pipeline_monitor_agent calling GET" in msg for msg in status_messages)


def test_status_validator_retries_on_404_until_success(monkeypatch):
    calls = {"job_get": 0, "sleep": 0}

    def fake_http_json_request(method: str, url: str, payload_obj=None, extra_headers=None):
        if method == "POST" and url == "http://airflow.localhost:6563/auth/token":
            return 200, {"access_token": "token-123"}
        if method == "POST" and url == "http://localhost:8080/api/submitJob/":
            return 200, {"jobId": "job-123", "status": "PENDING"}
        if method == "GET" and "jobStatus/job-123" in url:
            calls["job_get"] += 1
            if calls["job_get"] <= 4:
                raise HTTPError(url=url, code=404, msg="Not Found", hdrs=None, fp=None)
            return 200, {"status": "SUCCESS"}
        if method == "POST" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns":
            return 200, {"dag_run_id": "manual__1", "state": "queued"}
        if method == "GET" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns/manual__1":
            return 200, {"dag_run_id": "manual__1", "state": "success"}
        raise AssertionError(f"Unexpected HTTP call: {method} {url}")

    def fake_sleep(seconds: int):
        assert seconds == 60
        calls["sleep"] += 1

    monkeypatch.setattr(nodes, "_http_json_request", fake_http_json_request)
    monkeypatch.setattr(nodes.time, "sleep", fake_sleep)

    result = run_pipeline("hello")

    assert calls["job_get"] == 5
    assert calls["sleep"] == 1
    assert result["api_status_ok"] is True
    assert result["api_job_status"] == "SUCCESS"
    assert result["data_pipeline_invoked"] is True
    assert result["dag_monitor_ok"] is True


def test_dag_non_200_routes_to_hil(monkeypatch):
    def fake_http_json_request(method: str, url: str, payload_obj=None, extra_headers=None):
        if method == "POST" and url == "http://airflow.localhost:6563/auth/token":
            return 200, {"access_token": "token-123"}
        if method == "POST" and url == "http://localhost:8080/api/submitJob/":
            return 200, {"jobId": "job-123", "status": "PENDING"}
        if method == "GET" and url.rstrip("/") == "http://localhost:8080/api/jobStatus/job-123":
            return 200, {"status": "SUCCESS"}
        if method == "POST" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns":
            return 500, {"status": "FAILED"}
        raise AssertionError(f"Unexpected HTTP call: {method} {url}")

    monkeypatch.setattr(nodes, "_http_json_request", fake_http_json_request)

    result = run_pipeline("hello")

    assert result["data_pipeline_invoked"] is True
    assert result["dag_invocation_ok"] is False
    assert result["dag_invocation_status_code"] == 500
    assert result["hil_required"] is True
    assert "manual review" in result["hil_message"].lower()
    assert any("hil_agent" in line for line in result["log"])


def test_data_pipeline_monitor_polls_until_success(monkeypatch):
    calls = {"monitor_get": 0, "sleep": 0}

    def fake_http_json_request(method: str, url: str, payload_obj=None, extra_headers=None):
        if method == "POST" and url == "http://airflow.localhost:6563/auth/token":
            return 200, {"access_token": "token-123"}
        if method == "POST" and url == "http://localhost:8080/api/submitJob/":
            return 200, {"jobId": "job-123", "status": "PENDING"}
        if method == "GET" and url.rstrip("/") == "http://localhost:8080/api/jobStatus/job-123":
            return 200, {"status": "SUCCESS"}
        if method == "POST" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns":
            return 200, {"dag_run_id": "manual__1", "state": "queued"}
        if method == "GET" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns/manual__1":
            calls["monitor_get"] += 1
            if calls["monitor_get"] == 1:
                return 200, {"dag_run_id": "manual__1", "state": "running"}
            return 200, {"dag_run_id": "manual__1", "state": "success"}
        raise AssertionError(f"Unexpected HTTP call: {method} {url}")

    def fake_sleep(seconds: int):
        assert seconds == 60
        calls["sleep"] += 1

    monkeypatch.setattr(nodes, "_http_json_request", fake_http_json_request)
    monkeypatch.setattr(nodes.time, "sleep", fake_sleep)

    result = run_pipeline("hello")

    assert calls["monitor_get"] == 2
    assert calls["sleep"] == 1
    assert result["dag_monitor_ok"] is True
    assert result["dag_monitor_status"] == "SUCCESS"


def test_data_pipeline_monitor_non_200_routes_to_hil(monkeypatch):
    def fake_http_json_request(method: str, url: str, payload_obj=None, extra_headers=None):
        if method == "POST" and url == "http://airflow.localhost:6563/auth/token":
            return 200, {"access_token": "token-123"}
        if method == "POST" and url == "http://localhost:8080/api/submitJob/":
            return 200, {"jobId": "job-123", "status": "PENDING"}
        if method == "GET" and url.rstrip("/") == "http://localhost:8080/api/jobStatus/job-123":
            return 200, {"status": "SUCCESS"}
        if method == "POST" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns":
            return 200, {"dag_run_id": "manual__1", "state": "queued"}
        if method == "GET" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns/manual__1":
            return 500, {"status": "FAILED"}
        raise AssertionError(f"Unexpected HTTP call: {method} {url}")

    monkeypatch.setattr(nodes, "_http_json_request", fake_http_json_request)

    result = run_pipeline("hello")

    assert result["dag_monitor_ok"] is False
    assert result["dag_monitor_status_code"] == 500
    assert result["hil_required"] is True
    assert any("hil_agent" in line for line in result["log"])


def test_dag_invoker_falls_back_to_experimental_trigger(monkeypatch):
    def fake_http_json_request(method: str, url: str, payload_obj=None, extra_headers=None):
        if method == "POST" and url == "http://airflow.localhost:6563/auth/token":
            return 200, {"access_token": "token-123"}
        if method == "POST" and url == "http://localhost:8080/api/submitJob/":
            return 200, {"jobId": "job-123", "status": "PENDING"}
        if method == "GET" and url.rstrip("/") == "http://localhost:8080/api/jobStatus/job-123":
            return 200, {"status": "SUCCESS"}
        if method == "POST" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns":
            raise HTTPError(url=url, code=405, msg="Method Not Allowed", hdrs=None, fp=None)
        if method == "POST" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns/":
            raise HTTPError(url=url, code=405, msg="Method Not Allowed", hdrs=None, fp=None)
        if method == "POST" and url == "http://airflow.localhost:6563/api/experimental/dags/random_number_check_dag/dag_runs":
            return 200, {"message": "Created"}
        if method == "GET" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns":
            return 200, {"dag_runs": [{"dag_run_id": "manual__1", "state": "success"}]}
        raise AssertionError(f"Unexpected HTTP call: {method} {url}")

    monkeypatch.setattr(nodes, "_http_json_request", fake_http_json_request)

    result = run_pipeline("hello")

    assert result["dag_invocation_ok"] is True
    assert result["dag_monitor_ok"] is True


def test_dag_invoker_retries_payload_on_422_and_succeeds(monkeypatch):
    attempts = {"dag_post": 0}

    def fake_http_json_request(method: str, url: str, payload_obj=None, extra_headers=None):
        if method == "POST" and url == "http://airflow.localhost:6563/auth/token":
            return 200, {"access_token": "token-123"}
        if method == "POST" and url == "http://localhost:8080/api/submitJob/":
            return 200, {"jobId": "job-123", "status": "PENDING"}
        if method == "GET" and "jobStatus/job-123" in url:
            return 200, {"status": "SUCCESS"}
        if method == "POST" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns":
            attempts["dag_post"] += 1
            if attempts["dag_post"] == 1:
                raise HTTPError(url=url, code=422, msg="Unprocessable Entity", hdrs=None, fp=None)
            return 200, {"dag_run_id": "manual__2", "state": "queued"}
        if method == "GET" and url == "http://airflow.localhost:6563/api/v2/dags/random_number_check_dag/dagRuns/manual__2":
            return 200, {"dag_run_id": "manual__2", "state": "success"}
        raise AssertionError(f"Unexpected HTTP call: {method} {url}")

    monkeypatch.setattr(nodes, "_http_json_request", fake_http_json_request)

    result = run_pipeline("hello")

    assert attempts["dag_post"] == 2
    assert result["dag_invocation_ok"] is True
    assert result["dag_monitor_ok"] is True
