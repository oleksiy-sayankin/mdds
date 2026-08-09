# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""Error-result integration test for the Python Vector Sum Worker image."""

from __future__ import annotations

import base64
import json

from testcontainers.core.container import DockerContainer

_VECTOR_SUM_WORKER_IMAGE = "mddsproject/python-worker-vector-sum:0.1.0"
_ERROR_HANDLER_IMPORT_PATH = (
    "mdds_python_worker_vector_sum.testing_handlers:ErrorWorkerHandler"
)

_WORKER_BINARY = "/opt/mdds/bin/mdds-worker"
_WORKER_MANIFEST_PATH = "/opt/mdds/config/worker-manifest.json"
_WORKER_RESULT_PATH = "/opt/mdds/result/result.json"

_EXECUTION_FAILED_EXIT_CODE = 1
_EXPECTED_ERROR_MESSAGE = "Synthetic division by zero execution failure."


def test_vector_sum_error_worker_docker_image_integration() -> None:
    """Verify that a handler exception is persisted as an execution failure."""
    with _new_error_vector_sum_worker_container() as worker_container:
        _write_worker_manifest(worker_container)

        worker_exit_code, worker_output = _exec_in_container(
            worker_container,
            [_WORKER_BINARY],
        )
        result_exit_code, result_output = _exec_in_container(
            worker_container,
            ["cat", _WORKER_RESULT_PATH],
        )
        result_diagnostic = _decode_output(result_output)

        assert worker_exit_code == _EXECUTION_FAILED_EXIT_CODE, (
            "Worker Runtime returned an unexpected exit code for "
            "ErrorWorkerHandler.\n"
            f"Container output:\n{_decode_output(worker_output)}\n"
            f"Runtime result:\n{result_diagnostic}"
        )
        assert result_exit_code == 0, (
            f"Cannot read expected JSON file: {_WORKER_RESULT_PATH}.\n"
            f"Container output:\n{result_diagnostic}\n"
            f"Worker output:\n{_decode_output(worker_output)}"
        )
        assert json.loads(result_output) == {
            "exitCode": _EXECUTION_FAILED_EXIT_CODE,
            "message": _EXPECTED_ERROR_MESSAGE,
        }


def _new_error_vector_sum_worker_container() -> DockerContainer:
    """Keep the container alive while one failing attempt is exercised."""
    return (
        DockerContainer(_VECTOR_SUM_WORKER_IMAGE)
        .with_env("MDDS_ARGO_RETRY_INDEX", "0")
        .with_env("MDDS_WORKER_HANDLER", _ERROR_HANDLER_IMPORT_PATH)
        .with_command(["sleep", "infinity"])
    )


def _write_worker_manifest(worker_container: DockerContainer) -> None:
    manifest = {
        "apiVersion": "mdds/v1",
        "kind": "WorkerManifest",
        "execution": {
            "userId": 12345,
            "dagRunId": "vector-sum-error-integration-test",
            "nodeId": "error-worker",
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


def _write_file_in_container(
    worker_container: DockerContainer,
    path: str,
    content: bytes,
) -> None:
    """Materialize an Argo-provided file as root inside the container."""
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
