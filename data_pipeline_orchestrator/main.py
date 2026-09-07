"""Entry point for the data pipeline orchestrator.

Run one shot:
    python main.py "some input value"

Run interactive CLI:
    python main.py --interactive
"""
from __future__ import annotations

import argparse
import re
import sys
import warnings
from typing import Optional
from pathlib import Path

from dotenv import load_dotenv

from langchain_core._api.deprecation import LangChainPendingDeprecationWarning

warnings.filterwarnings(
    "ignore",
    message=r"urllib3 v2 only supports OpenSSL 1.1.1\+.*",
)
warnings.filterwarnings(
    "ignore",
    category=LangChainPendingDeprecationWarning,
)

from orchestrator.graph import run_pipeline


def _print_result(result: dict) -> int:
    print("=== Pipeline log ===")
    for line in result.get("log", []):
        print(f"- {line}")

    if result.get("hil_required"):
        print("\n=== Human In The Loop Required ===")
        print(result.get("hil_message") or "Manual intervention is required before continuing.")
        print("Next actions:")
        print("1. Check Airflow REST API availability at http://airflow.localhost:6563/api/v2/dags/")
        print("2. Verify the DAG name random_number_check_dag exists and is deployable")
        print("3. Retry from CLI using: Run datapipeline job")

    if result.get("error"):
        print("\n=== Error ===")
        print(result["error"])
        return 1
    return 0


def _print_live_status(message: str) -> None:
    print(f"[status] {message}", flush=True)


def _run_once(input_data: str) -> tuple[int, dict]:
    result = run_pipeline(input_data, status_callback=_print_live_status)
    return _print_result(result), result


def _parse_hil_choice(choice: str) -> str:
    normalized = choice.strip().lower()
    if normalized in {"1", "retry", "r"}:
        return "retry"
    if normalized in {"2", "diagnostics", "diag", "d"}:
        return "diagnostics"
    if normalized in {"3", "exit", "e", "q", "quit"}:
        return "exit"
    return "unknown"


def _run_hil_interactive_loop(input_data: str) -> int:
    while True:
        exit_code, result = _run_once(input_data)
        if not result.get("hil_required"):
            if exit_code != 0:
                print("Pipeline run finished with errors.")
            return exit_code

        while True:
            print("\nHIL options:")
            print("1. Retry now")
            print("2. Show diagnostics")
            print("3. Exit")
            choice = input("Select an option [1/2/3]: ").strip()
            action = _parse_hil_choice(choice)

            if action == "retry":
                print("Retrying pipeline...\n")
                break
            if action == "diagnostics":
                print("\n=== HIL Diagnostics ===")
                print(f"job_id: {result.get('job_id')}")
                print(f"dag_name: {result.get('dag_name')}")
                print(f"dag_invocation_status_code: {result.get('dag_invocation_status_code')}")
                print(f"dag_trigger_last_url: {result.get('dag_trigger_last_url')}")
                print(f"dag_trigger_attempt_urls: {result.get('dag_trigger_attempt_urls')}")
                print(f"error: {result.get('error')}")
                continue
            if action == "exit":
                print("Exiting interactive HIL flow. You can retry later with: Run datapipeline job")
                return 1

            print("Invalid option. Please choose 1, 2, or 3.")


def _parse_cli_command(command: str) -> tuple[str, Optional[str]]:
    text = command.strip()
    lowered = text.lower()

    if lowered in {"exit", "quit"}:
        return "exit", None
    if lowered in {"help", "?"}:
        return "help", None

    match = re.match(r"^run\s+datapipeline\s+job\s*(.*)$", text, flags=re.IGNORECASE)
    if match:
        payload = match.group(1).strip()
        return "run", payload if payload else "example input"

    match = re.match(r"^run\s+pipeline(?:\s+with\s+input)?\s*(.*)$", text, flags=re.IGNORECASE)
    if match:
        payload = match.group(1).strip()
        return "run", payload if payload else None

    if lowered.startswith("run "):
        payload = text[4:].strip()
        return "run", payload if payload else None

    return "unknown", None


def _interactive_cli() -> int:
    print("Data Pipeline Orchestrator CLI")
    print("Type: Run datapipeline job")
    print("No additional text is required after this command.")
    print("This command sequentially triggers agents configured in the LangGraph state graph.")
    print("Type: run pipeline with input <text>")
    print("Also supported: run <text>, help, exit")

    while True:
        try:
            user_text = input("\ncli> ").strip()
        except EOFError:
            print()
            return 0

        if not user_text:
            continue

        action, payload = _parse_cli_command(user_text)
        if action == "exit":
            return 0
        if action == "help":
            print("Commands:")
            print("- Run datapipeline job")
            print("  No additional text is required after this command.")
            print("  Sequentially triggers agents configured in the LangGraph state graph.")
            print("- run pipeline with input <text>")
            print("- run <text>")
            print("- exit")
            continue
        if action == "run":
            if not payload:
                print("Please provide input text after the run command.")
                continue
            _run_hil_interactive_loop(payload)
            continue

        print("Unknown command. Type 'help' for available commands.")


def main() -> None:
    dotenv_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(dotenv_path=dotenv_path, override=False)

    parser = argparse.ArgumentParser(description="Run the LangGraph data pipeline.")
    parser.add_argument("input", nargs="*", help="Input text for a one-shot pipeline run")
    parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Start an interactive CLI that accepts run commands",
    )
    args = parser.parse_args()

    if args.interactive or not args.input:
        sys.exit(_interactive_cli())

    input_data = " ".join(args.input)
    exit_code, _ = _run_once(input_data)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
