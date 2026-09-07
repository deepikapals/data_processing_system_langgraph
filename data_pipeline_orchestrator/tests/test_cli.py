from main import _parse_cli_command, _parse_hil_choice


def test_parse_exit_command():
    action, payload = _parse_cli_command("exit")
    assert action == "exit"
    assert payload is None


def test_parse_help_command():
    action, payload = _parse_cli_command("help")
    assert action == "help"
    assert payload is None


def test_parse_run_pipeline_command():
    action, payload = _parse_cli_command("run pipeline with input hello world")
    assert action == "run"
    assert payload == "hello world"


def test_parse_run_short_command():
    action, payload = _parse_cli_command("run hello")
    assert action == "run"
    assert payload == "hello"


def test_parse_run_datapipeline_job_default_input():
    action, payload = _parse_cli_command("Run datapipeline job")
    assert action == "run"
    assert payload == "example input"


def test_parse_run_datapipeline_job_with_input():
    action, payload = _parse_cli_command("Run datapipeline job customer orders")
    assert action == "run"
    assert payload == "customer orders"


def test_parse_unknown_command():
    action, payload = _parse_cli_command("status")
    assert action == "unknown"
    assert payload is None


def test_parse_hil_choice_retry_variants():
    assert _parse_hil_choice("1") == "retry"
    assert _parse_hil_choice("retry") == "retry"


def test_parse_hil_choice_diagnostics_variants():
    assert _parse_hil_choice("2") == "diagnostics"
    assert _parse_hil_choice("diag") == "diagnostics"


def test_parse_hil_choice_exit_variants():
    assert _parse_hil_choice("3") == "exit"
    assert _parse_hil_choice("quit") == "exit"


def test_parse_hil_choice_unknown():
    assert _parse_hil_choice("abc") == "unknown"
