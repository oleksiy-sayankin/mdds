# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""SIGTERM integration test for the Python Vector Sum Worker Docker image."""

from __future__ import annotations

import base64
import json
import signal
import time

import pytest
from testcontainers.core.container import DockerContainer

_VECTOR_SUM_WORKER_IMAGE = "mddsproject/python-worker-vector-sum:0.1.0"
_HANGING_HANDLER_IMPORT_PATH = (
    "mdds_python_worker_vector_sum.testing_handlers:HangingWorkerHandler"
)

_WORKER_BINARY = "/opt/mdds/bin/mdds-worker"
_WORKER_MANIFEST_PATH = "/opt/mdds/config/worker-manifest.json"
_WORKER_RESULT_PATH = "/opt/mdds/result/result.json"
_RUNTIME_PID_PATH = "/opt/mdds/tmp/sigterm-test-runtime.pid"
_RUNTIME_LOG_PATH = "/opt/mdds/tmp/sigterm-test-runtime.log"

_HANDLER_START_TIMEOUT_SECONDS = 10.0
_HANDLER_STABILITY_SECONDS = 0.25
_RUNTIME_TERMINATION_TIMEOUT_SECONDS = 5.0
_STATE_POLL_INTERVAL_SECONDS = 0.05
_SIGTERM_EXIT_CODE = 128 + signal.SIGTERM


def test_vector_sum_sigterm_worker_docker_image_integration() -> None:
    """Terminate a hanging Worker attempt and verify its diagnostic result."""
    with _new_hanging_vector_sum_worker_container() as worker_container:
        _write_worker_manifest(worker_container)

        worker_exec_id = _start_worker_runtime(worker_container)
        runtime_process_id = _wait_for_runtime_process(
            worker_container,
            worker_exec_id,
        )
        handler_process_id = _wait_for_hanging_handler(
            worker_container,
            worker_exec_id,
            runtime_process_id,
        )

        _send_sigterm(worker_container, runtime_process_id)
        worker_exit_code = _wait_for_worker_exit(
            worker_container,
            worker_exec_id,
        )
        runtime_log = _read_optional_text_file(
            worker_container,
            _RUNTIME_LOG_PATH,
        )

        assert worker_exit_code == _SIGTERM_EXIT_CODE, (
            "Worker Runtime returned an unexpected exit code after SIGTERM.\n"
            f"Runtime log:\n{runtime_log}"
        )

        assert _read_json_file(worker_container, _WORKER_RESULT_PATH) == {
            "exitCode": _SIGTERM_EXIT_CODE,
            "message": "Terminated by SIGTERM.",
        }
        assert not _process_exists(worker_container, handler_process_id), (
            "HangingWorkerHandler process is still alive after Worker Runtime "
            "completed its SIGTERM path."
        )


def _new_hanging_vector_sum_worker_container() -> DockerContainer:
    """Keep the container alive while one Worker Runtime attempt is exercised."""
    return (
        DockerContainer(_VECTOR_SUM_WORKER_IMAGE)
        .with_env("MDDS_ARGO_RETRY_INDEX", "0")
        .with_env("MDDS_WORKER_HANDLER", _HANGING_HANDLER_IMPORT_PATH)
        .with_command(["sleep", "infinity"])
    )


def _write_worker_manifest(worker_container: DockerContainer) -> None:
    manifest = {
        "apiVersion": "mdds/v1",
        "kind": "WorkerManifest",
        "execution": {
            "userId": 12345,
            "dagRunId": "vector-sum-sigterm-integration-test",
            "nodeId": "hanging-worker",
        },
        "inputs": {},
        "params": {},
        "outputs": {},
    }
    manifest_bytes = (
        json.dumps(manifest, ensure_ascii=False, allow_nan=False, indent=2) + "\n"
    ).encode("utf-8")

    _write_file_in_container(
        worker_container,
        _WORKER_MANIFEST_PATH,
        manifest_bytes,
    )


def _start_worker_runtime(worker_container: DockerContainer) -> str:
    """Start mdds-worker asynchronously and retain its Docker exec identifier."""
    command = (
        f"printf '%s\\n' \"$$\" > {_RUNTIME_PID_PATH} && "
        f"exec {_WORKER_BINARY} > {_RUNTIME_LOG_PATH} 2>&1"
    )
    wrapped_container = worker_container.get_wrapped_container()
    exec_instance = wrapped_container.client.api.exec_create(
        wrapped_container.id,
        ["sh", "-c", command],
        user="mdds",
    )
    exec_id = exec_instance["Id"]
    wrapped_container.client.api.exec_start(exec_id, detach=True)
    return str(exec_id)


def _wait_for_runtime_process(
    worker_container: DockerContainer,
    worker_exec_id: str,
) -> int:
    deadline = time.monotonic() + _HANDLER_START_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        _fail_if_worker_exited(worker_container, worker_exec_id)
        runtime_process_id = _read_optional_process_id(
            worker_container,
            _RUNTIME_PID_PATH,
        )
        if runtime_process_id is not None:
            return runtime_process_id

        time.sleep(_STATE_POLL_INTERVAL_SECONDS)

    pytest.fail(
        "Worker Runtime process did not start within "
        f"{_HANDLER_START_TIMEOUT_SECONDS} seconds.\n"
        f"Runtime log:\n{_read_optional_text_file(worker_container, _RUNTIME_LOG_PATH)}"
    )


def _wait_for_hanging_handler(
    worker_container: DockerContainer,
    worker_exec_id: str,
    runtime_process_id: int,
) -> int:
    """Wait until the same supervised process remains alive long enough to hang."""
    deadline = time.monotonic() + _HANDLER_START_TIMEOUT_SECONDS
    candidate_process_id: int | None = None
    candidate_first_seen_at: float | None = None

    while time.monotonic() < deadline:
        _fail_if_worker_exited(worker_container, worker_exec_id)
        current_process_id = _find_spawned_handler_process(
            worker_container,
            runtime_process_id,
        )
        now = time.monotonic()

        if current_process_id is None:
            candidate_process_id = None
            candidate_first_seen_at = None
        elif current_process_id != candidate_process_id:
            candidate_process_id = current_process_id
            candidate_first_seen_at = now
        elif (
            candidate_first_seen_at is not None
            and now - candidate_first_seen_at >= _HANDLER_STABILITY_SECONDS
        ):
            return current_process_id

        time.sleep(_STATE_POLL_INTERVAL_SECONDS)

    pytest.fail(
        "HangingWorkerHandler did not start within "
        f"{_HANDLER_START_TIMEOUT_SECONDS} seconds.\n"
        f"Runtime log:\n{_read_optional_text_file(worker_container, _RUNTIME_LOG_PATH)}"
    )


def _find_spawned_handler_process(
    worker_container: DockerContainer,
    runtime_process_id: int,
) -> int | None:
    script = (
        "from pathlib import Path\n"
        f"runtime_pid = {runtime_process_id}\n"
        "children_path = Path(f'/proc/{runtime_pid}/task/{runtime_pid}/children')\n"
        "try:\n"
        "    child_process_ids = children_path.read_text().split()\n"
        "except FileNotFoundError:\n"
        "    raise SystemExit(1)\n"
        "for child_process_id in child_process_ids:\n"
        "    try:\n"
        "        command_line = Path(\n"
        "            f'/proc/{child_process_id}/cmdline'\n"
        "        ).read_bytes()\n"
        "    except FileNotFoundError:\n"
        "        continue\n"
        "    if b'spawn_main' in command_line:\n"
        "        print(child_process_id)\n"
        "        raise SystemExit(0)\n"
        "raise SystemExit(1)\n"
    )
    exit_code, output = _exec_in_container(
        worker_container,
        ["python", "-c", script],
    )

    if exit_code != 0:
        return None

    try:
        return int(_decode_output(output).strip())
    except ValueError as error:
        raise AssertionError(
            "Expected HangingWorkerHandler process identifier to be an integer, "
            f"got: {_decode_output(output)!r}."
        ) from error


def _send_sigterm(
    worker_container: DockerContainer,
    runtime_process_id: int,
) -> None:
    script = (
        "import os\n"
        "import signal\n"
        f"os.kill({runtime_process_id}, signal.SIGTERM)\n"
    )
    exit_code, output = _exec_in_container(
        worker_container,
        ["python", "-c", script],
    )

    assert exit_code == 0, (
        f"Cannot send SIGTERM to Worker Runtime process {runtime_process_id}.\n"
        f"Container output:\n{_decode_output(output)}"
    )


def _wait_for_worker_exit(
    worker_container: DockerContainer,
    worker_exec_id: str,
) -> int:
    deadline = time.monotonic() + _RUNTIME_TERMINATION_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        exec_state = _inspect_exec(worker_container, worker_exec_id)
        if not exec_state["Running"]:
            exit_code = exec_state["ExitCode"]
            if exit_code is None:
                raise AssertionError("Completed Worker Runtime has no exit code.")
            return int(exit_code)

        time.sleep(_STATE_POLL_INTERVAL_SECONDS)

    pytest.fail(
        "Worker Runtime did not terminate after SIGTERM within "
        f"{_RUNTIME_TERMINATION_TIMEOUT_SECONDS} seconds.\n"
        f"Runtime log:\n{_read_optional_text_file(worker_container, _RUNTIME_LOG_PATH)}"
    )


def _fail_if_worker_exited(
    worker_container: DockerContainer,
    worker_exec_id: str,
) -> None:
    exec_state = _inspect_exec(worker_container, worker_exec_id)
    if exec_state["Running"]:
        return

    pytest.fail(
        "Worker Runtime exited before HangingWorkerHandler was running "
        f"with code {exec_state['ExitCode']}.\n"
        f"Runtime log:\n{_read_optional_text_file(worker_container, _RUNTIME_LOG_PATH)}"
    )


def _inspect_exec(
    worker_container: DockerContainer,
    worker_exec_id: str,
) -> dict[str, object]:
    wrapped_container = worker_container.get_wrapped_container()
    return dict(wrapped_container.client.api.exec_inspect(worker_exec_id))


def _process_exists(
    worker_container: DockerContainer,
    process_id: int,
) -> bool:
    script = (
        "from pathlib import Path\n"
        f"raise SystemExit(0 if Path('/proc/{process_id}').exists() else 1)\n"
    )
    exit_code, _ = _exec_in_container(
        worker_container,
        ["python", "-c", script],
    )
    return exit_code == 0


def _read_optional_process_id(
    worker_container: DockerContainer,
    path: str,
) -> int | None:
    exit_code, output = _exec_in_container(
        worker_container,
        ["cat", path],
    )
    if exit_code != 0:
        return None

    try:
        return int(_decode_output(output).strip())
    except ValueError as error:
        raise AssertionError(
            f"Expected process identifier in {path}, got: {_decode_output(output)!r}."
        ) from error


def _read_json_file(
    worker_container: DockerContainer,
    path: str,
) -> object:
    exit_code, output = _exec_in_container(
        worker_container,
        ["cat", path],
    )
    assert exit_code == 0, (
        f"Cannot read expected JSON file: {path}.\n"
        f"Container output:\n{_decode_output(output)}"
    )

    return json.loads(output)


def _read_optional_text_file(
    worker_container: DockerContainer,
    path: str,
) -> str:
    exit_code, output = _exec_in_container(
        worker_container,
        ["cat", path],
    )
    if exit_code != 0:
        return f"<unavailable: {path}>"
    return _decode_output(output)


def _write_file_in_container(
    worker_container: DockerContainer,
    path: str,
    content: bytes,
) -> None:
    """Materialize an Argo-provided file as root inside the running container."""
    encoded_content = base64.b64encode(content).decode("ascii")
    script = (
        "import base64\n"
        "from pathlib import Path\n"
        f"path = Path({path!r})\n"
        "path.parent.mkdir(parents=True, exist_ok=True)\n"
        f"path.write_bytes(base64.b64decode({encoded_content!r}))\n"
    )
    exit_code, output = _exec_in_container(
        worker_container,
        ["python", "-c", script],
        user="root",
    )

    assert exit_code == 0, (
        f"Cannot create file inside container: {path}.\n"
        f"Container output:\n{_decode_output(output)}"
    )


def _exec_in_container(
    worker_container: DockerContainer,
    command: list[str],
    *,
    user: str = "mdds",
) -> tuple[int, bytes]:
    result = worker_container.get_wrapped_container().exec_run(
        command,
        user=user,
    )

    if not isinstance(result.output, bytes):
        raise AssertionError(
            "Expected combined container command output to be bytes, "
            f"got {type(result.output).__name__}."
        )

    return int(result.exit_code), result.output


def _decode_output(output: bytes) -> str:
    return output.decode("utf-8", errors="replace")
