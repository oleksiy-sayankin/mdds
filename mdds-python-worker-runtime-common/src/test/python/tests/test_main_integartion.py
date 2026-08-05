# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.fixtures.worker_runtime_main import (
    TEST_CONTEXT_PATH_ENV,
    TEST_MANIFEST_PATH_ENV,
    TEST_RESULT_PATH_ENV,
)

HANGING_HANDLER_IMPORT_PATH = (
    "tests.fixtures.hanging_worker_handler:HangingWorkerHandler"
)
TWO_NUMBERS_SUM_HANDLER_IMPORT_PATH = (
    "tests.fixtures.worker_handlers:TwoNumbersSumWorkerHandler"
)
HANDLER_START_TIMEOUT_SECONDS = 10.0
RUNTIME_COMPLETION_TIMEOUT_SECONDS = 10.0
RUNTIME_TERMINATION_TIMEOUT_SECONDS = 5.0
STATE_POLL_INTERVAL_SECONDS = 0.05
SIGTERM_EXIT_CODE = 128 + signal.SIGTERM


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Worker Runtime process contracts are Linux-specific.",
)
def test_worker_runtime_common_two_numbers_sum_integration(tmp_path: Path) -> None:
    number_a = 37
    number_b = -12
    input_directory = tmp_path / "inputs"
    number_a_path = input_directory / "number_a.json"
    number_b_path = input_directory / "number_b.json"
    output_path = tmp_path / "outputs" / "sum.json"
    manifest_path = tmp_path / "worker-manifest.json"
    context_path = tmp_path / "context-snapshot.json"
    result_path = tmp_path / "result.json"

    input_directory.mkdir()
    number_a_path.write_text(f"  {number_a}\n", encoding="utf-8")
    number_b_path.write_text(f"{number_b}\n", encoding="utf-8")
    _write_two_numbers_sum_worker_manifest(
        manifest_path=manifest_path,
        number_a_path=number_a_path,
        number_b_path=number_b_path,
        output_path=output_path,
    )

    runtime_process = _start_worker_runtime(
        handler_import_path=TWO_NUMBERS_SUM_HANDLER_IMPORT_PATH,
        manifest_path=manifest_path,
        context_path=context_path,
        result_path=result_path,
    )

    output = ""
    try:
        try:
            output, _ = runtime_process.communicate(
                timeout=RUNTIME_COMPLETION_TIMEOUT_SECONDS
            )
        except subprocess.TimeoutExpired:
            pytest.fail(
                "Worker Runtime did not complete TwoNumbersSumWorkerHandler "
                f"within {RUNTIME_COMPLETION_TIMEOUT_SECONDS} seconds."
            )

        assert runtime_process.returncode == 0, output
        assert int(output_path.read_text(encoding="utf-8")) == number_a + number_b
        assert json.loads(result_path.read_text(encoding="utf-8")) == {
            "exitCode": 0,
            "message": "WorkerHandler execution completed.",
        }
    finally:
        _kill_test_process_group(runtime_process)


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Worker Runtime process and signal contracts are Linux-specific.",
)
def test_worker_runtime_common_sigterm_integration(tmp_path: Path) -> None:
    manifest_path = tmp_path / "worker-manifest.json"
    context_path = tmp_path / "context-snapshot.json"
    result_path = tmp_path / "result.json"
    handler_started_path = tmp_path / "handler-started"
    _write_worker_manifest(manifest_path, handler_started_path)

    runtime_process = _start_worker_runtime(
        handler_import_path=HANGING_HANDLER_IMPORT_PATH,
        manifest_path=manifest_path,
        context_path=context_path,
        result_path=result_path,
    )

    output = ""
    try:
        handler_process_id = _wait_for_handler_start(
            runtime_process,
            handler_started_path,
        )

        runtime_process.send_signal(signal.SIGTERM)
        try:
            output, _ = runtime_process.communicate(
                timeout=RUNTIME_TERMINATION_TIMEOUT_SECONDS
            )
        except subprocess.TimeoutExpired:
            pytest.fail(
                "Worker Runtime did not terminate after SIGTERM within "
                f"{RUNTIME_TERMINATION_TIMEOUT_SECONDS} seconds."
            )

        assert runtime_process.returncode == SIGTERM_EXIT_CODE, output
        assert json.loads(result_path.read_text(encoding="utf-8")) == {
            "exitCode": SIGTERM_EXIT_CODE,
            "message": "Terminated by SIGTERM.",
        }
        assert not _process_exists(handler_process_id)
    finally:
        _kill_test_process_group(runtime_process)


def _write_worker_manifest(
    manifest_path: Path,
    handler_started_path: Path,
) -> None:
    manifest = {
        "apiVersion": "mdds/v1",
        "kind": "WorkerManifest",
        "execution": {
            "userId": 12345,
            "dagRunId": "sigterm-integration-test",
            "nodeId": "hanging-worker",
        },
        "inputs": {},
        "params": {"startedPath": str(handler_started_path)},
        "outputs": {},
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_two_numbers_sum_worker_manifest(
    *,
    manifest_path: Path,
    number_a_path: Path,
    number_b_path: Path,
    output_path: Path,
) -> None:
    manifest = {
        "apiVersion": "mdds/v1",
        "kind": "WorkerManifest",
        "execution": {
            "userId": 12345,
            "dagRunId": "two-numbers-sum-integration-test",
            "nodeId": "two-numbers-sum-worker",
        },
        "inputs": {
            "number_a": {
                "path": str(number_a_path),
                "format": "json",
            },
            "number_b": {
                "path": str(number_b_path),
                "format": "json",
            },
        },
        "params": {},
        "outputs": {
            "sum": {
                "path": str(output_path),
                "format": "json",
            }
        },
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )


def _worker_environment(
    *,
    handler_import_path: str,
    manifest_path: Path,
    context_path: Path,
    result_path: Path,
) -> dict[str, str]:
    environment = os.environ.copy()
    python_paths = [
        str(_module_root() / "src" / "main" / "python"),
        str(_module_root() / "src" / "test" / "python"),
    ]
    existing_python_path = environment.get("PYTHONPATH")
    if existing_python_path:
        python_paths.append(existing_python_path)

    environment.update(
        {
            "PYTHONPATH": os.pathsep.join(python_paths),
            "MDDS_WORKER_NAME": "integration-test-worker",
            "MDDS_WORKER_VERSION": "test",
            "MDDS_WORKER_HANDLER": handler_import_path,
            "MDDS_WORKER_POLL_INTERVAL_SECONDS": "1",
            "MDDS_ARGO_RETRY_INDEX": "0",
            TEST_MANIFEST_PATH_ENV: str(manifest_path),
            TEST_CONTEXT_PATH_ENV: str(context_path),
            TEST_RESULT_PATH_ENV: str(result_path),
        }
    )
    return environment


def _start_worker_runtime(
    *,
    handler_import_path: str,
    manifest_path: Path,
    context_path: Path,
    result_path: Path,
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-m", "tests.fixtures.worker_runtime_main"],
        cwd=_module_root(),
        env=_worker_environment(
            handler_import_path=handler_import_path,
            manifest_path=manifest_path,
            context_path=context_path,
            result_path=result_path,
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )


def _wait_for_handler_start(
    runtime_process: subprocess.Popen[str],
    handler_started_path: Path,
) -> int:
    deadline = time.monotonic() + HANDLER_START_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        if handler_started_path.is_file():
            return int(handler_started_path.read_text(encoding="utf-8"))

        return_code = runtime_process.poll()
        if return_code is not None:
            output, _ = runtime_process.communicate()
            pytest.fail(
                "Worker Runtime exited before HangingWorkerHandler started "
                f"with code {return_code}.\n{output}"
            )

        time.sleep(STATE_POLL_INTERVAL_SECONDS)

    pytest.fail(
        "HangingWorkerHandler did not start within "
        f"{HANDLER_START_TIMEOUT_SECONDS} seconds."
    )


def _process_exists(process_id: int) -> bool:
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    return True


def _kill_test_process_group(runtime_process: subprocess.Popen[str]) -> None:
    try:
        os.killpg(runtime_process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass

    try:
        runtime_process.wait(timeout=1.0)
    except subprocess.TimeoutExpired:
        runtime_process.kill()
        runtime_process.wait(timeout=1.0)


def _module_root() -> Path:
    return Path(__file__).resolve().parents[4]
